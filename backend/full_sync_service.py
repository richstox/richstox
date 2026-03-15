"""
Full Sync Service
=================
One-time (or periodic) full data download for all VISIBLE tickers.

Jobs:
  - run_full_fundamentals_sync:  downloads complete fundamentals for all visible tickers

Also handles cleanup: removes price + fundamentals data for tickers no longer visible.

  - Read cancel flag from ops_config between tickers (Stop button support)
  - Log every API credit to credit_logs
  - Track completion in tracked_tickers (fundamentals_complete)
  - Dynamic concurrency: start at 10, scale up to MAX_CONCURRENCY on success
  - Safety stops: error rate > 5%, runtime > 5h
"""

import os
import time
import uuid
import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional

import httpx
from pymongo import UpdateOne, ReturnDocument

logger = logging.getLogger("richstox.full_sync")

EODHD_BASE_URL = "https://eodhd.com/api"
EODHD_API_KEY = os.getenv("EODHD_API_KEY", "")

MAX_CONCURRENCY = 50
START_CONCURRENCY = 10
MAX_RUNTIME_SECONDS = 5 * 3600   # 5 hours safety stop
MAX_ERROR_RATE_PCT = 5
BULK_CHUNK = 5000
PROGRESS_INTERVAL = 200

# Tickers stuck in "processing" longer than this are treated as zombie locks
# and re-claimed by the next available worker.
ZOMBIE_THRESHOLD_MINUTES = 15

# The set of tracked_tickers that belong to the fundamentals sync queue:
# every ticker that is not yet complete OR is flagged for refresh.
_QUEUED_FILTER = {
    "$or": [
        {"fundamentals_complete": {"$ne": True}},
        {"needs_fundamentals_refresh": True},
    ]
}


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


# ---------------------------------------------------------------------------
# Job B: Full Fundamentals  (run_id state-machine, atomic claiming)
# ---------------------------------------------------------------------------

OPS_CONFIG_RUN_KEY = "active_fundamentals_run"


def _build_claim_filter(current_run_id: str) -> Dict[str, Any]:
    """
    Build the MongoDB filter used to atomically claim the next ticker for
    a specific run.  The run_id scope replaces the old _QUEUED_FILTER guard —
    only tickers tagged for this run are eligible.

    Claimable status values:
      - null / missing / "pending" / "error"  → ready to process
      - "processing" AND started_at < stale_cutoff  → zombie recovery
    """
    stale_cutoff = datetime.now(timezone.utc) - timedelta(minutes=ZOMBIE_THRESHOLD_MINUTES)
    return {
        "$and": [
            {"fundamentals_run_id": current_run_id},
            {
                "$or": [
                    {"fundamentals_status": {"$in": [None, "pending", "error"]}},
                    {
                        "fundamentals_status": "processing",
                        "fundamentals_processing_started_at": {"$lt": stale_cutoff},
                    },
                ]
            },
        ]
    }


async def _claim_next_ticker(db, current_run_id: str) -> Optional[str]:
    """
    Atomically claim the next available ticker from the current run's queue.
    Returns the ticker string, or None when the queue is exhausted.
    The `fundamentals_processing_started_at` timestamp is always overwritten
    with UTC now — this resets the zombie clock for re-claimed stuck tickers.
    """
    now = datetime.now(timezone.utc)
    doc = await db.tracked_tickers.find_one_and_update(
        _build_claim_filter(current_run_id),
        {"$set": {
            "fundamentals_status": "processing",
            "fundamentals_processing_started_at": now,
        }},
        projection={"ticker": 1},
        return_document=ReturnDocument.AFTER,
    )
    return doc["ticker"] if doc else None


async def _process_fundamentals_ticker(
    db, ticker: str, job_name: str, cancel_event: asyncio.Event
) -> Dict[str, Any]:
    """
    Fetch, parse, and persist complete fundamentals for one ticker.
    cancel_event is set by the runner-level monitor (never deleted here).
    On success  → fundamentals_status = "complete",  fundamentals_complete = True.
    On failure  → fundamentals_status = "error".
    On cancel   → returns {"cancelled": True} without writing partial data.
    fundamentals_run_id is intentionally NOT cleared so the progress endpoint
    can keep counting this ticker in the run's aggregation.
    """
    ticker_us = ticker if ticker.endswith(".US") else f"{ticker}.US"

    url = f"{EODHD_BASE_URL}/fundamentals/{ticker_us}"
    params = {"api_token": EODHD_API_KEY, "fmt": "json"}
    data, http_status, duration_ms, ok = await _fetch_one(url, params)

    # Cancel check 1: immediately after API fetch, before any writes
    if cancel_event.is_set():
        logger.info(f"[{job_name}] Cancel signalled after fetch — {ticker_us}")
        return {"ticker": ticker_us, "success": False, "rate_limited": False, "cancelled": True}

    await _log_credit(
        db, job_name=job_name, operation="fundamentals",
        ticker=ticker_us, api_endpoint=url,
        credits_used=10, http_status=http_status,
        status="success" if ok else ("429" if http_status == 429 else "error"),
        duration_ms=duration_ms,
    )

    if not ok or not data:
        await db.tracked_tickers.update_one(
            {"ticker": ticker_us},
            {"$set": {"fundamentals_status": "error"}},
        )
        return {"ticker": ticker_us, "success": False, "rate_limited": http_status == 429}

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
        # Cancel check 2: before financials bulk_write
        if cancel_event.is_set():
            logger.info(f"[{job_name}] Cancel signalled before financials write — {ticker_us}")
            return {"ticker": ticker_us, "success": False, "rate_limited": False, "cancelled": True}
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
        # Cancel check 3: before earnings bulk_write
        if cancel_event.is_set():
            logger.info(f"[{job_name}] Cancel signalled before earnings write — {ticker_us}")
            return {"ticker": ticker_us, "success": False, "rate_limited": False, "cancelled": True}
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

    sector = (company_doc.get("sector") or "").strip()
    industry = (company_doc.get("industry") or "").strip()
    has_classification = bool(sector and industry)

    logger.critical(
        f"PARSER DEBUG: Ticker {ticker_us} -> "
        f"Sector: '{sector}', Industry: '{industry}', "
        f"HasClass: {has_classification}"
    )

    # Pull shares_outstanding directly from the raw EODHD payload (source of truth)
    shares_stats = data.get("SharesStats") or {}
    shares_outstanding = shares_stats.get("SharesOutstanding")

    from utils.currency_utils import extract_statement_currency
    financial_currency = extract_statement_currency(data)

    await db.tracked_tickers.update_one(
        {"ticker": ticker_us},
        {"$set": {
            "fundamentals_complete": True,
            "needs_fundamentals_refresh": False,
            "fundamentals_status": "complete",
            "fundamentals_updated_at": now,
            "sector":             sector   or None,
            "industry":           industry or None,
            "has_classification": has_classification,
            "shares_outstanding": shares_outstanding,
            "financial_currency": financial_currency,
        }},
    )

    return {"ticker": ticker_us, "success": True, "rate_limited": False}


async def run_full_fundamentals_sync(db, ignore_kill_switch: bool = False) -> Dict[str, Any]:
    """
    Download complete fundamentals for all queued tickers.

    Queue definition: tracked_tickers where
      fundamentals_complete != true  OR  needs_fundamentals_refresh == true

    Run-ID architecture:
      1. Concurrency guard — abort if another non-stale run is already active.
      2. Stamp all queued tickers with fundamentals_run_id = current_run_id.
         Only set fundamentals_status = "pending" where it is currently null/missing
         (preserves existing "error" history for diagnostics).
      3. Workers atomically claim tickers belonging to this run_id only.
      4. Progress endpoint aggregates on fundamentals_run_id — total_queued is
         stable because tagged tickers never leave the run's aggregation set.
      5. On job end (success / cancel / error) clear ops_config run marker.
    """
    job_name = "full_fundamentals_sync"
    started_at = datetime.now(timezone.utc)

    if not EODHD_API_KEY:
        return {"job_name": job_name, "status": "skipped", "reason": "no_api_key",
                "started_at": started_at.isoformat()}

    # ── 1. Concurrency guard ─────────────────────────────────────────────────
    existing_run = await db.ops_config.find_one({"key": OPS_CONFIG_RUN_KEY})
    if existing_run:
        age_seconds = (started_at - existing_run["started_at"].replace(tzinfo=timezone.utc)
                       if existing_run["started_at"].tzinfo is None
                       else (started_at - existing_run["started_at"]).total_seconds())
        if not isinstance(age_seconds, float):
            age_seconds = age_seconds.total_seconds() if hasattr(age_seconds, 'total_seconds') else MAX_RUNTIME_SECONDS
        if age_seconds < MAX_RUNTIME_SECONDS:
            logger.warning(f"{job_name}: another run is active ({existing_run['run_id']}), aborting")
            return {
                "job_name": job_name, "status": "skipped",
                "reason": "another_run_active",
                "active_run_id": existing_run["run_id"],
                "started_at": started_at.isoformat(),
            }

    # ── 2. Early-exit check ──────────────────────────────────────────────────
    total_to_process = await db.tracked_tickers.count_documents(_QUEUED_FILTER)
    if total_to_process == 0:
        logger.info(f"{job_name}: queue is empty, nothing to do")
        return {
            "job_name": job_name, "status": "completed",
            "total": 0, "success": 0, "failed": 0,
            "started_at": started_at.isoformat(),
            "finished_at": datetime.now(timezone.utc).isoformat(),
        }

    # ── 3. Generate run_id and register it in ops_config (zombies_reclaimed added after init)
    current_run_id = (
        f"frun_{started_at.strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    )
    await db.ops_config.replace_one(
        {"key": OPS_CONFIG_RUN_KEY},
        {
            "key": OPS_CONFIG_RUN_KEY,
            "run_id": current_run_id,
            "started_at": started_at,
            "zombies_reclaimed": 0,
        },
        upsert=True,
    )
    logger.info(f"{job_name}: started run_id={current_run_id}, queue={total_to_process}")

    # ── 4. Cleanup invisible tickers ────────────────────────────────────────
    cleanup = await cleanup_invisible_ticker_data(db)

    # ── 5. Initialize run: tag queued tickers + reset stale/zombie/error → pending
    now_init = datetime.now(timezone.utc)
    stale_processing_cutoff = now_init - timedelta(minutes=ZOMBIE_THRESHOLD_MINUTES)  # 15 min
    stale_pending_cutoff    = now_init - timedelta(minutes=60)                         # 60 min

    # Op A: stamp ALL queued tickers with current_run_id (status unchanged)
    await db.tracked_tickers.update_many(
        _QUEUED_FILTER,
        {"$set": {"fundamentals_run_id": current_run_id}},
    )

    # Op B: reset to "pending" + record enqueue timestamp for:
    #   - null / missing status  (never processed)
    #   - "error"                (failed in a previous run — retry)
    #   - zombie "processing"    (claimed > ZOMBIE_THRESHOLD_MINUTES ago without completing)
    #   - stale "pending"        (enqueued > 60 min ago without being claimed)
    # "complete" is never touched.
    reset_filter = {
        "$and": [
            {"fundamentals_run_id": current_run_id},
            {
                "$or": [
                    {"fundamentals_status": None},
                    {"fundamentals_status": "error"},
                    {
                        "fundamentals_status": "processing",
                        "fundamentals_processing_started_at": {"$lt": stale_processing_cutoff},
                    },
                    {
                        "fundamentals_status": "pending",
                        "fundamentals_processing_started_at": {"$lt": stale_pending_cutoff},
                    },
                ]
            },
        ]
    }
    reset_result = await db.tracked_tickers.update_many(
        reset_filter,
        {"$set": {
            "fundamentals_status": "pending",
            "fundamentals_processing_started_at": now_init,
        }},
    )
    zombies_reclaimed = reset_result.modified_count

    # Patch zombies_reclaimed into the ops_config record so the progress endpoint can expose it
    await db.ops_config.update_one(
        {"key": OPS_CONFIG_RUN_KEY},
        {"$set": {"zombies_reclaimed": zombies_reclaimed}},
    )
    logger.info(
        f"{job_name}: tagged={total_to_process}, "
        f"reset_to_pending={zombies_reclaimed} (errors+zombies+stale)"
    )

    # ── 6. Cancel event + counters + concurrency primitives ─────────────────
    cancel_event = asyncio.Event()
    counters: Dict[str, int] = {
        "success": 0, "failed": 0, "rate_limited": 0, "processed": 0
    }
    job_start = time.monotonic()
    cancelled = False

    semaphore = asyncio.Semaphore(START_CONCURRENCY)
    concurrency_holder = [START_CONCURRENCY]

    # ── 7. Background cancel monitor ─────────────────────────────────────────
    async def _cancel_monitor() -> None:
        """
        Poll ops_config every 2 s for the cancel flag.
        Consumes (deletes) the flag EXACTLY ONCE, then sets cancel_event
        so all workers stop at their next safe checkpoint.
        """
        while not cancel_event.is_set():
            await asyncio.sleep(2)
            doc = await db.ops_config.find_one({"key": f"cancel_job_{job_name}"})
            if doc:
                await db.ops_config.delete_one({"key": f"cancel_job_{job_name}"})
                logger.info(f"{job_name}: cancel flag consumed by monitor, setting cancel_event")
                cancel_event.set()
                return

    # ── 8. Drain-worker definition ───────────────────────────────────────────
    async def drain_worker() -> None:
        nonlocal cancelled
        while True:
            # Fast in-memory check — no DB round-trip
            if cancel_event.is_set():
                cancelled = True
                return

            if time.monotonic() - job_start > MAX_RUNTIME_SECONDS:
                logger.error(f"{job_name}: max runtime exceeded, stopping")
                cancelled = True
                return

            async with semaphore:
                ticker = await _claim_next_ticker(db, current_run_id)
                if ticker is None:
                    return  # queue for this run is exhausted

                result = await _process_fundamentals_ticker(
                    db, ticker, job_name, cancel_event
                )

            # Propagate cancellation detected inside the ticker processor
            if result.get("cancelled"):
                cancelled = True
                return

            counters["processed"] += 1

            if result.get("rate_limited"):
                counters["rate_limited"] += 1
                await asyncio.sleep(10)
                concurrency_holder[0] = max(5, concurrency_holder[0] // 2)
                semaphore._value = concurrency_holder[0]
            elif result.get("success"):
                counters["success"] += 1
                if counters["success"] % 50 == 0 and concurrency_holder[0] < MAX_CONCURRENCY:
                    concurrency_holder[0] = min(MAX_CONCURRENCY, concurrency_holder[0] + 2)
                    semaphore._value = concurrency_holder[0]
            else:
                counters["failed"] += 1

            if counters["processed"] % PROGRESS_INTERVAL == 0:
                logger.info(
                    f"{job_name}: {counters['processed']} done, "
                    f"ok={counters['success']} fail={counters['failed']} "
                    f"concurrency={concurrency_holder[0]}"
                )

            if counters["processed"] > 50:
                err_pct = counters["failed"] / counters["processed"] * 100
                if err_pct > MAX_ERROR_RATE_PCT:
                    logger.error(f"{job_name}: error rate {err_pct:.1f}% exceeded, stopping")
                    cancelled = True
                    return

    # ── 9. Run monitor + workers; always clean up in finally ─────────────────
    monitor_task = asyncio.create_task(_cancel_monitor())
    try:
        await asyncio.gather(*[drain_worker() for _ in range(START_CONCURRENCY)])
    finally:
        monitor_task.cancel()
        await db.ops_config.delete_one({"key": OPS_CONFIG_RUN_KEY})
        logger.info(f"{job_name}: cleared active run marker (run_id={current_run_id})")

    # ── 10. Persist summary ──────────────────────────────────────────────────
    finished_at = datetime.now(timezone.utc)
    from zoneinfo import ZoneInfo
    PRAGUE = ZoneInfo("Europe/Prague")
    final_status = "cancelled" if cancelled else "completed"

    summary = {
        "job_name": job_name,
        "run_id": current_run_id,
        "status": final_status,
        "total": total_to_process,
        "zombies_reclaimed": zombies_reclaimed,
        "processed": counters["processed"],
        "success": counters["success"],
        "failed": counters["failed"],
        "rate_limited": counters["rate_limited"],
        "cleanup": cleanup,
        "credits_used": counters["success"] * 10,
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "started_at_prague": started_at.astimezone(PRAGUE).isoformat(),
        "finished_at_prague": finished_at.astimezone(PRAGUE).isoformat(),
        "log_timezone": "Europe/Prague",
        "duration_seconds": (finished_at - started_at).total_seconds(),
        **({"cancelled_at": finished_at.isoformat()} if cancelled else {}),
    }

    await db.ops_job_runs.insert_one({
        "job_name": job_name,
        "status": final_status,
        "started_at": started_at,
        "finished_at": finished_at,
        "started_at_prague": summary["started_at_prague"],
        "finished_at_prague": summary["finished_at_prague"],
        "log_timezone": "Europe/Prague",
        "details": summary,
    })

    logger.info(
        f"{job_name} {final_status}: run_id={current_run_id}, "
        f"{counters['success']}/{total_to_process} ok, "
        f"credits={counters['success'] * 10}"
    )
    return summary
