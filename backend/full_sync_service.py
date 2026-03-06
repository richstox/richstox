"""
Full Sync Service
=================
One-time (or periodic) full data download for all VISIBLE tickers.

Two jobs:
  - run_full_price_history_sync: downloads complete EOD history from IPO for all visible tickers
  - run_full_fundamentals_sync:  downloads complete fundamentals for all visible tickers

Also handles cleanup: removes price + fundamentals data for tickers no longer visible.

Both jobs:
  - Read cancel flag from ops_config between tickers (Stop button support)
  - Log every API credit to credit_logs
  - Track completion in tracked_tickers (price_history_complete, fundamentals_complete)
  - Dynamic concurrency: start at 10, scale up to MAX_CONCURRENCY on success
  - Safety stops: error rate > 5%, runtime > 5h
"""

import os
import time
import asyncio
import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional

import httpx
from pymongo import UpdateOne

logger = logging.getLogger("richstox.full_sync")

EODHD_BASE_URL = "https://eodhd.com/api"
EODHD_API_KEY = os.getenv("EODHD_API_KEY", "")

MAX_CONCURRENCY = 50
START_CONCURRENCY = 10
MAX_RUNTIME_SECONDS = 5 * 3600   # 5 hours safety stop
MAX_ERROR_RATE_PCT = 5
BULK_CHUNK = 5000
PROGRESS_INTERVAL = 200


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _is_cancelled(db, job_name: str) -> bool:
    doc = await db.ops_config.find_one({"key": f"cancel_job_{job_name}"})
    if doc:
        await db.ops_config.delete_one({"key": f"cancel_job_{job_name}"})
        return True
    return False


async def _log_credit(db, *, job_name: str, operation: str, ticker: Optional[str],
                      api_endpoint: str, credits_used: int,
                      http_status: int, status: str, duration_ms: int) -> None:
    try:
        from credit_log_service import log_api_credit
        await log_api_credit(
            db,
            job_name=job_name,
            operation=operation,
            ticker=ticker,
            api_endpoint=api_endpoint,
            credits_used=credits_used,
            http_status=http_status,
            status=status,
            duration_ms=duration_ms,
        )
    except Exception:
        pass


async def _fetch_one(url: str, params: Dict) -> tuple:
    """Single EODHD HTTP call. Returns (data, http_status, duration_ms, ok)."""
    t0 = time.monotonic()
    try:
        async with httpx.AsyncClient(timeout=60) as client:
            resp = await client.get(url, params=params)
        ms = int((time.monotonic() - t0) * 1000)
        if resp.status_code == 429:
            return None, 429, ms, False
        resp.raise_for_status()
        data = resp.json()
        return data, resp.status_code, ms, True
    except Exception as exc:
        ms = int((time.monotonic() - t0) * 1000)
        logger.warning(f"Request failed [{url}]: {exc}")
        return None, 0, ms, False


# ---------------------------------------------------------------------------
# Cleanup: remove data for tickers no longer in visible universe
# ---------------------------------------------------------------------------

async def cleanup_invisible_ticker_data(db) -> Dict[str, Any]:
    """
    Delete price and fundamentals data for tickers that are no longer visible.
    Called at the start of both full sync jobs.
    """
    invisible = await db.tracked_tickers.distinct(
        "ticker",
        {"$or": [{"is_visible": {"$ne": True}}, {"is_delisted": True}]},
    )
    if not invisible:
        return {"deleted_tickers": 0}

    invisible_us = [t if t.endswith(".US") else f"{t}.US" for t in invisible]
    invisible_plain = [t.replace(".US", "") for t in invisible_us]
    all_variants = list(set(invisible_us + invisible_plain))

    collections = [
        "stock_prices",
        "company_fundamentals_cache",
        "company_financials",
        "company_earnings_history",
        "insider_activity",
    ]
    deleted: Dict[str, int] = {}
    for col in collections:
        res = await db[col].delete_many({"ticker": {"$in": all_variants}})
        deleted[col] = res.deleted_count

    logger.info(f"Cleanup removed data for {len(invisible)} invisible tickers: {deleted}")
    return {"deleted_tickers": len(invisible), "by_collection": deleted}


# ---------------------------------------------------------------------------
# Job A: Full Price History
# ---------------------------------------------------------------------------

async def _process_price_ticker(db, ticker: str, job_name: str,
                                 needs_redownload: bool) -> Dict[str, Any]:
    """Fetch + store complete EOD history for one ticker."""
    ticker_us = ticker if ticker.endswith(".US") else f"{ticker}.US"
    ticker_api = ticker_us  # EODHD accepts AAPL.US format

    # If split: delete old prices first (atomic re-download)
    if needs_redownload:
        await db.stock_prices.delete_many({"ticker": {"$in": [ticker_us, ticker_us.replace(".US", "")]}})

    url = f"{EODHD_BASE_URL}/eod/{ticker_api}"
    params = {"api_token": EODHD_API_KEY, "fmt": "json"}
    data, http_status, duration_ms, ok = await _fetch_one(url, params)

    await _log_credit(
        db, job_name=job_name, operation="price_history",
        ticker=ticker_us, api_endpoint=url,
        credits_used=1, http_status=http_status,
        status="success" if ok else ("429" if http_status == 429 else "error"),
        duration_ms=duration_ms,
    )

    if not ok or not isinstance(data, list) or not data:
        return {"ticker": ticker_us, "success": False, "records": 0, "rate_limited": http_status == 429}

    # Bulk upsert
    ops = []
    for rec in data:
        date = rec.get("date")
        if not date:
            continue
        ops.append(UpdateOne(
            {"ticker": ticker_us, "date": date},
            {"$set": {
                "ticker": ticker_us,
                "date": date,
                "open": float(rec["open"]) if rec.get("open") else None,
                "high": float(rec["high"]) if rec.get("high") else None,
                "low": float(rec["low"]) if rec.get("low") else None,
                "close": float(rec["close"]) if rec.get("close") else None,
                "adjusted_close": float(rec["adjusted_close"]) if rec.get("adjusted_close") else None,
                "volume": int(rec["volume"]) if rec.get("volume") else None,
            }},
            upsert=True,
        ))

    for i in range(0, len(ops), BULK_CHUNK):
        await db.stock_prices.bulk_write(ops[i:i + BULK_CHUNK], ordered=False)

    # Compute max date actually in DB for this ticker
    latest = await db.stock_prices.find_one(
        {"ticker": ticker_us},
        {"date": 1, "_id": 0},
        sort=[("date", -1)],
    )
    complete_as_of = latest["date"] if latest else None

    await db.tracked_tickers.update_one(
        {"ticker": ticker_us},
        {"$set": {
            "price_history_complete": True,
            "price_history_complete_as_of": complete_as_of,
            "price_history_status": "complete",
            "needs_price_redownload": False,
        }},
    )

    return {"ticker": ticker_us, "success": True, "records": len(ops), "rate_limited": False}


async def run_full_price_history_sync(db, ignore_kill_switch: bool = False) -> Dict[str, Any]:
    """
    Download complete EOD price history (IPO → today) for all visible tickers.
    Also re-downloads tickers flagged needs_price_redownload (splits).
    Runs cleanup first.
    """
    job_name = "full_price_history_sync"
    started_at = datetime.now(timezone.utc)

    if not EODHD_API_KEY:
        return {"job_name": job_name, "status": "skipped", "reason": "no_api_key",
                "started_at": started_at.isoformat()}

    # 1. Cleanup invisible tickers
    cleanup = await cleanup_invisible_ticker_data(db)

    # 2. Get tickers needing download
    docs = await db.tracked_tickers.find(
        {
            "is_visible": True,
            "$or": [
                {"price_history_complete": {"$ne": True}},
                {"needs_price_redownload": True},
            ],
        },
        {"ticker": 1, "needs_price_redownload": 1, "_id": 0},
    ).to_list(None)

    tickers = [(d["ticker"], bool(d.get("needs_price_redownload"))) for d in docs]
    total = len(tickers)
    logger.info(f"{job_name}: {total} tickers to process")

    if not tickers:
        return {
            "job_name": job_name, "status": "completed",
            "total": 0, "success": 0, "failed": 0,
            "cleanup": cleanup, "started_at": started_at.isoformat(),
            "finished_at": datetime.now(timezone.utc).isoformat(),
        }

    # 3. Dynamic concurrency parallel download
    semaphore = asyncio.Semaphore(START_CONCURRENCY)
    concurrency_holder = [START_CONCURRENCY]
    success_count = 0
    failed_count = 0
    rate_limited_count = 0
    processed = 0
    job_start = time.monotonic()

    async def worker(ticker: str, needs_redownload: bool) -> Dict:
        async with semaphore:
            return await _process_price_ticker(db, ticker, job_name, needs_redownload)

    tasks = [asyncio.create_task(worker(t, r)) for t, r in tickers]

    for i, coro in enumerate(asyncio.as_completed(tasks)):
        # Runtime safety stop
        if time.monotonic() - job_start > MAX_RUNTIME_SECONDS:
            logger.error(f"{job_name}: max runtime exceeded, stopping")
            for t in tasks:
                t.cancel()
            break

        # Cancel check every 50 tickers
        if i % 50 == 0 and await _is_cancelled(db, job_name):
            logger.info(f"{job_name}: cancelled by user")
            for t in tasks:
                t.cancel()
            break

        result = await coro
        processed += 1
        if result.get("rate_limited"):
            rate_limited_count += 1
            await asyncio.sleep(5)
            # Reduce concurrency on rate limit
            concurrency_holder[0] = max(5, concurrency_holder[0] // 2)
            semaphore._value = concurrency_holder[0]
        elif result.get("success"):
            success_count += 1
            # Scale up concurrency on sustained success
            if success_count % 100 == 0 and concurrency_holder[0] < MAX_CONCURRENCY:
                concurrency_holder[0] = min(MAX_CONCURRENCY, concurrency_holder[0] + 5)
                semaphore._value = concurrency_holder[0]
        else:
            failed_count += 1

        if processed % PROGRESS_INTERVAL == 0:
            logger.info(f"{job_name}: {processed}/{total} done, "
                        f"ok={success_count} fail={failed_count} concurrency={concurrency_holder[0]}")

        # Error rate safety stop
        if processed > 50 and (failed_count / processed * 100) > MAX_ERROR_RATE_PCT:
            logger.error(f"{job_name}: error rate {failed_count/processed*100:.1f}% exceeded, stopping")
            for t in tasks:
                t.cancel()
            break

    finished_at = datetime.now(timezone.utc)
    summary = {
        "job_name": job_name,
        "status": "completed",
        "total": total,
        "processed": processed,
        "success": success_count,
        "failed": failed_count,
        "rate_limited": rate_limited_count,
        "cleanup": cleanup,
        "credits_used": success_count,
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "duration_seconds": (finished_at - started_at).total_seconds(),
    }

    await db.ops_job_runs.insert_one({
        "job_name": job_name,
        "status": summary["status"],
        "started_at": started_at,
        "finished_at": finished_at,
        "details": summary,
    })

    logger.info(f"{job_name} done: {success_count}/{total} ok, {failed_count} failed")
    return summary


# ---------------------------------------------------------------------------
# Job B: Full Fundamentals
# ---------------------------------------------------------------------------

async def _process_fundamentals_ticker(db, ticker: str, job_name: str) -> Dict[str, Any]:
    """Fetch + store complete fundamentals for one ticker."""
    ticker_us = ticker if ticker.endswith(".US") else f"{ticker}.US"

    url = f"{EODHD_BASE_URL}/fundamentals/{ticker_us}"
    params = {"api_token": EODHD_API_KEY, "fmt": "json"}
    data, http_status, duration_ms, ok = await _fetch_one(url, params)

    await _log_credit(
        db, job_name=job_name, operation="fundamentals",
        ticker=ticker_us, api_endpoint=url,
        credits_used=10, http_status=http_status,
        status="success" if ok else ("429" if http_status == 429 else "error"),
        duration_ms=duration_ms,
    )

    if not ok or not data:
        return {"ticker": ticker_us, "success": False, "rate_limited": http_status == 429}

    # Parse + store using existing service
    from fundamentals_service import (
        parse_company_fundamentals,
        parse_financials,
        parse_earnings_history,
        parse_insider_activity,
    )
    ticker_plain = ticker_us.replace(".US", "")
    now = datetime.now(timezone.utc)

    company_doc = parse_company_fundamentals(ticker_plain, data)
    company_doc["updated_at"] = now
    await db.company_fundamentals_cache.update_one(
        {"ticker": ticker_us},
        {"$set": company_doc},
        upsert=True,
    )

    fin_rows = parse_financials(ticker_plain, data)
    if fin_rows:
        fin_ops = [
            UpdateOne(
                {"ticker": r["ticker"], "period_type": r["period_type"], "period_date": r["period_date"]},
                {"$set": r}, upsert=True,
            )
            for r in fin_rows
        ]
        await db.company_financials.bulk_write(fin_ops, ordered=False)

    earn_rows = parse_earnings_history(ticker_plain, data)
    if earn_rows:
        earn_ops = [
            UpdateOne(
                {"ticker": r["ticker"], "quarter_date": r["quarter_date"]},
                {"$set": r}, upsert=True,
            )
            for r in earn_rows
        ]
        await db.company_earnings_history.bulk_write(earn_ops, ordered=False)

    insider_doc = parse_insider_activity(ticker_plain, data)
    if insider_doc:
        await db.insider_activity.update_one(
            {"ticker": ticker_us}, {"$set": insider_doc}, upsert=True
        )

    await db.tracked_tickers.update_one(
        {"ticker": ticker_us},
        {"$set": {
            "fundamentals_complete": True,
            "fundamentals_at": now,
            "fundamentals_status": "complete",
            "needs_fundamentals_refresh": False,
            "fundamentals_updated_at": now,
            "sector": company_doc.get("sector"),
            "industry": company_doc.get("industry"),
            "has_classification": bool(company_doc.get("sector") and company_doc.get("industry")),
        }},
    )

    return {"ticker": ticker_us, "success": True, "rate_limited": False}


async def run_full_fundamentals_sync(db, ignore_kill_switch: bool = False) -> Dict[str, Any]:
    """
    Download complete fundamentals for all visible tickers.
    Prioritizes: needs_fundamentals_refresh=true, then fundamentals_complete=false.
    Runs cleanup first.
    """
    job_name = "full_fundamentals_sync"
    started_at = datetime.now(timezone.utc)

    if not EODHD_API_KEY:
        return {"job_name": job_name, "status": "skipped", "reason": "no_api_key",
                "started_at": started_at.isoformat()}

    # 1. Cleanup
    cleanup = await cleanup_invisible_ticker_data(db)

    # 2. Tickers needing fundamentals (refresh first, then missing)
    refresh_docs = await db.tracked_tickers.find(
        {"is_visible": True, "needs_fundamentals_refresh": True},
        {"ticker": 1, "_id": 0},
    ).to_list(None)
    missing_docs = await db.tracked_tickers.find(
        {"is_visible": True, "fundamentals_complete": {"$ne": True},
         "needs_fundamentals_refresh": {"$ne": True}},
        {"ticker": 1, "_id": 0},
    ).to_list(None)

    refresh_tickers = [d["ticker"] for d in refresh_docs]
    missing_tickers = [d["ticker"] for d in missing_docs]
    tickers = refresh_tickers + missing_tickers
    total = len(tickers)
    logger.info(f"{job_name}: {total} tickers ({len(refresh_tickers)} refresh + {len(missing_tickers)} missing)")

    if not tickers:
        return {
            "job_name": job_name, "status": "completed",
            "total": 0, "success": 0, "failed": 0,
            "cleanup": cleanup, "started_at": started_at.isoformat(),
            "finished_at": datetime.now(timezone.utc).isoformat(),
        }

    # 3. Parallel download — start at 10, scale to MAX_CONCURRENCY
    semaphore = asyncio.Semaphore(START_CONCURRENCY)
    concurrency_holder = [START_CONCURRENCY]
    success_count = 0
    failed_count = 0
    rate_limited_count = 0
    processed = 0
    job_start = time.monotonic()

    async def worker(ticker: str) -> Dict:
        async with semaphore:
            return await _process_fundamentals_ticker(db, ticker, job_name)

    tasks = [asyncio.create_task(worker(t)) for t in tickers]

    for i, coro in enumerate(asyncio.as_completed(tasks)):
        if time.monotonic() - job_start > MAX_RUNTIME_SECONDS:
            logger.error(f"{job_name}: max runtime exceeded, stopping")
            for t in tasks:
                t.cancel()
            break

        if i % 50 == 0 and await _is_cancelled(db, job_name):
            logger.info(f"{job_name}: cancelled by user")
            for t in tasks:
                t.cancel()
            break

        result = await coro
        processed += 1
        if result.get("rate_limited"):
            rate_limited_count += 1
            await asyncio.sleep(10)
            concurrency_holder[0] = max(5, concurrency_holder[0] // 2)
            semaphore._value = concurrency_holder[0]
        elif result.get("success"):
            success_count += 1
            if success_count % 50 == 0 and concurrency_holder[0] < MAX_CONCURRENCY:
                concurrency_holder[0] = min(MAX_CONCURRENCY, concurrency_holder[0] + 2)
                semaphore._value = concurrency_holder[0]
        else:
            failed_count += 1

        if processed % PROGRESS_INTERVAL == 0:
            logger.info(f"{job_name}: {processed}/{total} done, "
                        f"ok={success_count} fail={failed_count} concurrency={concurrency_holder[0]}")

        if processed > 50 and (failed_count / processed * 100) > MAX_ERROR_RATE_PCT:
            logger.error(f"{job_name}: error rate exceeded, stopping")
            for t in tasks:
                t.cancel()
            break

    finished_at = datetime.now(timezone.utc)
    summary = {
        "job_name": job_name,
        "status": "completed",
        "total": total,
        "processed": processed,
        "success": success_count,
        "failed": failed_count,
        "rate_limited": rate_limited_count,
        "cleanup": cleanup,
        "credits_used": success_count * 10,
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "duration_seconds": (finished_at - started_at).total_seconds(),
    }

    await db.ops_job_runs.insert_one({
        "job_name": job_name,
        "status": summary["status"],
        "started_at": started_at,
        "finished_at": finished_at,
        "details": summary,
    })

    logger.info(f"{job_name} done: {success_count}/{total} ok, credits={success_count * 10}")
    return summary
