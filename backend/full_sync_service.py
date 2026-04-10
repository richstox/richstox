"""
Full Sync Service
=================
Helpers for full data download jobs and cleanup of invisible ticker data.

  - cleanup_invisible_ticker_data: removes price + fundamentals data for tickers no longer visible
  - _process_price_ticker: fetch + store complete EOD price history for one ticker
  - _fetch_one / _log_credit: shared low-level helpers
"""

import os
import time
import logging
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, Optional

import httpx
from pymongo import UpdateOne

logger = logging.getLogger("richstox.full_sync")

EODHD_BASE_URL = "https://eodhd.com/api"
EODHD_API_KEY = os.getenv("EODHD_API_KEY", "")

BULK_CHUNK = 10000

# Minimum expected records for a full EOD history download.
# A legitimate US stock typically has 250+ trading days per year.
# Fewer than this threshold suggests truncated / incomplete data from the
# EODHD API — we still store what we got but do NOT mark the download as
# "complete" so Phase C retries on the next run.
_MIN_HISTORY_RECORDS = 10


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

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

    Benchmark tickers (from BENCHMARK_SYMBOLS) are explicitly excluded so
    that their price history is never removed by visibility-based cleanup.
    """
    from benchmark_service import BENCHMARK_SYMBOLS
    benchmark_tickers = set(BENCHMARK_SYMBOLS.values())

    invisible = await db.tracked_tickers.distinct(
        "ticker",
        {"$or": [{"is_visible": {"$ne": True}}, {"is_delisted": True}]},
    )
    if not invisible:
        return {"deleted_tickers": 0}

    invisible_us = [t if t.endswith(".US") else f"{t}.US" for t in invisible]
    invisible_plain = [t.replace(".US", "") for t in invisible_us]
    all_variants = list(set(invisible_us + invisible_plain) - benchmark_tickers)

    if not all_variants:
        return {"deleted_tickers": 0}

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

async def _process_price_ticker(
    db,
    ticker: str,
    job_name: str,
    needs_redownload: bool,
    cancel_check: Optional[Callable[[], Awaitable[bool]]] = None,
) -> Dict[str, Any]:
    """Fetch + store complete EOD history for one ticker."""
    ticker_us = ticker if ticker.endswith(".US") else f"{ticker}.US"
    ticker_api = ticker_us  # EODHD accepts AAPL.US format

    async def _should_cancel() -> bool:
        if cancel_check is None:
            return False
        return bool(await cancel_check())

    async def _cancel_result(records: int = 0) -> Dict[str, Any]:
        return {
            "ticker": ticker_us,
            "success": False,
            "records": records,
            "cancelled": True,
            "rate_limited": False,
        }

    # If split: delete old prices first (atomic re-download)
    if needs_redownload:
        if await _should_cancel():
            return await _cancel_result()
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
        # ── Record failure on the ticker so it's discoverable via DB query ──
        _fail_reason = "rate_limited" if http_status == 429 else "api_error"
        if ok and (not isinstance(data, list) or not data):
            _fail_reason = "api_returned_empty"
        _fail_ts = datetime.now(timezone.utc)
        await db.tracked_tickers.update_one(
            {"ticker": ticker_us},
            {"$set": {
                "price_history_status": "error",
                "history_download_failed_at": _fail_ts,
                "history_download_error": _fail_reason,
                "history_download_http_status": http_status,
            }},
        )
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

    processed_ops = 0
    for i in range(0, len(ops), BULK_CHUNK):
        if await _should_cancel():
            return await _cancel_result(records=processed_ops)
        chunk = ops[i:i + BULK_CHUNK]
        await db.stock_prices.bulk_write(chunk, ordered=False)
        processed_ops += len(chunk)

    # Compute max date actually in DB for this ticker
    latest = await db.stock_prices.find_one(
        {"ticker": ticker_us},
        {"date": 1, "_id": 0},
        sort=[("date", -1)],
    )
    complete_as_of = latest["date"] if latest else None

    if await _should_cancel():
        return await _cancel_result(records=processed_ops)

    # ── Suspect-incomplete guard ─────────────────────────────────────
    # A real full-history download returns hundreds/thousands of records.
    # If the API returned fewer than _MIN_HISTORY_RECORDS, the data is
    # likely truncated.  We still persist the records but do NOT mark the
    # ticker as "complete" so Phase C retries on the next run.
    # Without this guard, Phase C permanently seals the ticker out of all
    # retry paths (the broken link the debug report identified).
    if len(ops) < _MIN_HISTORY_RECORDS:
        logger.warning(
            "[Phase C] %s: only %d records returned (need >= %d). "
            "Data written but NOT marking price_history_complete=True — "
            "Phase C will retry on the next run.",
            ticker_us, len(ops), _MIN_HISTORY_RECORDS,
        )
        await db.tracked_tickers.update_one(
            {"ticker": ticker_us},
            {"$set": {
                "price_history_status": "suspect_incomplete",
                "price_history_complete_as_of": complete_as_of,
                "history_download_records": len(ops),
                "history_download_suspect_at": datetime.now(timezone.utc),
                "history_download_error": (
                    f"only_{len(ops)}_records_returned"
                ),
                "needs_price_redownload": False,
            }},
        )
        return {
            "ticker": ticker_us,
            "success": False,
            "records": len(ops),
            "rate_limited": False,
            "suspect_incomplete": True,
        }

    await db.tracked_tickers.update_one(
        {"ticker": ticker_us},
        {"$set": {
            "price_history_complete": True,
            "price_history_complete_as_of": complete_as_of,
            "price_history_status": "complete",
            "needs_price_redownload": False,
            # Strict proof marker — canonical source for history_download_completed
            "history_download_proven_at": datetime.now(timezone.utc),
            "history_download_proven_anchor": complete_as_of,
            # Full-history download provenance — used by returning-ticker
            # cooldown guard to prevent unnecessary re-download churn.
            "full_history_downloaded_at": datetime.now(timezone.utc),
            "full_history_source": "eodhd_eod_api",
            "full_history_version": 1,
            # Computed fields — kept in sync so dashboard facet reads work
            # without requiring a separate backfill run.
            "history_download_completed": True,
            "gap_free_since_history_download": True,
        }},
    )

    return {"ticker": ticker_us, "success": True, "records": len(ops), "rate_limited": False}
