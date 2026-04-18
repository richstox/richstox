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

from price_ingestion_service import validate_price_row

logger = logging.getLogger("richstox.full_sync")

EODHD_BASE_URL = "https://eodhd.com/api"
EODHD_API_KEY = os.getenv("EODHD_API_KEY", "")

BULK_CHUNK = 10000

# ── Range-proof tolerance (days) ────────────────────────────────────
# After Phase C writes EODHD history to stock_prices, we compare
# db_first_date / db_last_date against the provider payload's first/last
# dates.  Because some records may have close=0 and be legitimately
# skipped, we allow a small tolerance when checking coverage.
_RANGE_PROOF_TOLERANCE_DAYS = 5


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

    if needs_redownload:
        if await _should_cancel():
            return await _cancel_result()

    # ── Capture pre-write DB state for structured audit logging ──────
    _pre_agg = await db.stock_prices.aggregate([
        {"$match": {"ticker": ticker_us}},
        {"$group": {
            "_id": None,
            "count": {"$sum": 1},
            "earliest": {"$min": "$date"},
            "latest": {"$max": "$date"},
        }},
    ]).to_list(1)
    _pre_count = _pre_agg[0]["count"] if _pre_agg else 0
    _pre_earliest = _pre_agg[0]["earliest"] if _pre_agg else None
    _pre_latest = _pre_agg[0]["latest"] if _pre_agg else None

    # ── Fetch BEFORE any destructive delete ──────────────────────────
    # Previous code deleted existing rows first, then fetched.  If the
    # fetch failed (timeout / 429 / network), all historical price data
    # was permanently lost and subsequent daily bulk adds produced a
    # broken 2-point chart.
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

    _fetched_count = len(data) if isinstance(data, list) else 0

    if not ok or not isinstance(data, list) or not data:
        _fail_reason = "rate_limited" if http_status == 429 else "api_error"
        if ok and (not isinstance(data, list) or not data):
            _fail_reason = "api_returned_empty"
        _fail_ts = datetime.now(timezone.utc)
        # ── Re-flag for retry so Phase C picks this ticker up again ──
        # Without this, a failed fetch after a delete leaves the ticker
        # with no history and no retry path.
        _error_fields: Dict[str, Any] = {
            "price_history_status": "error",
            "history_download_failed_at": _fail_ts,
            "history_download_error": _fail_reason,
            "history_download_http_status": http_status,
        }
        if needs_redownload:
            _error_fields["needs_price_redownload"] = True
            _error_fields["price_history_complete"] = False
        await db.tracked_tickers.update_one(
            {"ticker": ticker_us},
            {"$set": _error_fields},
        )
        logger.warning(
            "[Phase C] %s: fetch FAILED (reason=%s, http=%s, "
            "needs_redownload=%s). Existing DB rows preserved "
            "(pre_count=%d). needs_price_redownload re-flagged=%s.",
            ticker_us, _fail_reason, http_status,
            needs_redownload, _pre_count, needs_redownload,
        )
        return {"ticker": ticker_us, "success": False, "records": 0, "rate_limited": http_status == 429}

    # ── Delete old rows ONLY after successful fetch ──────────────────
    # This is the critical ordering fix: we now have the replacement
    # data in memory before destroying existing rows.
    if needs_redownload:
        await db.stock_prices.delete_many({"ticker": {"$in": [ticker_us, ticker_us.replace(".US", "")]}})

    # ── Build and execute bulk upsert ────────────────────────────────
    ops = []
    _skipped_invalid = 0
    for rec in data:
        date = rec.get("date")
        if not date:
            continue
        row_doc = {
            "ticker": ticker_us,
            "date": date,
            "open": float(rec["open"]) if rec.get("open") else None,
            "high": float(rec["high"]) if rec.get("high") else None,
            "low": float(rec["low"]) if rec.get("low") else None,
            "close": float(rec["close"]) if rec.get("close") else None,
            "adjusted_close": float(rec["adjusted_close"]) if rec.get("adjusted_close") else None,
            "volume": int(rec["volume"]) if rec.get("volume") else None,
        }
        if not validate_price_row(row_doc):
            _skipped_invalid += 1
            continue
        ops.append(UpdateOne(
            {"ticker": ticker_us, "date": date},
            {"$set": row_doc},
            upsert=True,
        ))
    if _skipped_invalid:
        logger.warning(
            "[Phase C] %s: skipped %d invalid rows (missing ticker/date/close)",
            ticker_us, _skipped_invalid,
        )

    processed_ops = 0
    for i in range(0, len(ops), BULK_CHUNK):
        if await _should_cancel():
            return await _cancel_result(records=processed_ops)
        chunk = ops[i:i + BULK_CHUNK]
        await db.stock_prices.bulk_write(chunk, ordered=False)
        processed_ops += len(chunk)

    if await _should_cancel():
        return await _cancel_result(records=processed_ops)

    # ── Range-proof: extract provider date range from payload ────────
    payload_dates = sorted(rec["date"] for rec in data if rec.get("date"))
    provider_first_date = payload_dates[0] if payload_dates else None
    provider_last_date = payload_dates[-1] if payload_dates else None

    # Query actual DB state after write
    _agg_pipeline = [
        {"$match": {"ticker": ticker_us}},
        {"$group": {
            "_id": None,
            "db_first_date": {"$min": "$date"},
            "db_last_date": {"$max": "$date"},
            "db_row_count": {"$sum": 1},
        }},
    ]
    _agg_result = await db.stock_prices.aggregate(_agg_pipeline).to_list(1)
    if _agg_result:
        db_first_date = _agg_result[0]["db_first_date"]
        db_last_date = _agg_result[0]["db_last_date"]
        db_row_count = _agg_result[0]["db_row_count"]
    else:
        db_first_date = None
        db_last_date = None
        db_row_count = 0

    # ── Structured ingestion audit log ───────────────────────────────
    logger.info(
        "[Phase C] %s: ingestion_audit "
        "fetched_count=%d ops_count=%d "
        "pre_db=[count=%d, earliest=%s, latest=%s] "
        "post_db=[count=%d, earliest=%s, latest=%s] "
        "needs_redownload=%s",
        ticker_us, _fetched_count, len(ops),
        _pre_count, _pre_earliest, _pre_latest,
        db_row_count, db_first_date, db_last_date,
        needs_redownload,
    )

    # ── Row-count regression guard ───────────────────────────────────
    # A successful Phase C run must NEVER reduce row_count or move
    # earliest_date forward.  If this happened, it indicates a write-path
    # bug (e.g. the old delete-before-fetch issue).  Log a critical
    # warning and re-flag for retry rather than marking complete.
    _row_count_regressed = (
        _pre_count > 0
        and db_row_count < _pre_count
    )
    _earliest_regressed = (
        _pre_earliest is not None
        and db_first_date is not None
        and db_first_date > _pre_earliest
    )
    if _row_count_regressed or _earliest_regressed:
        logger.error(
            "[Phase C] %s: INGESTION REGRESSION DETECTED — "
            "pre=[count=%d, earliest=%s] → post=[count=%d, earliest=%s]. "
            "Re-flagging needs_price_redownload=True for retry.",
            ticker_us, _pre_count, _pre_earliest,
            db_row_count, db_first_date,
        )
        await db.tracked_tickers.update_one(
            {"ticker": ticker_us},
            {"$set": {
                "needs_price_redownload": True,
                "price_history_complete": False,
                "price_history_status": "ingestion_regression",
                "history_download_error": (
                    f"ingestion_regression:pre=[{_pre_count},{_pre_earliest}],"
                    f"post=[{db_row_count},{db_first_date}]"
                ),
                "updated_at": datetime.now(timezone.utc),
            }},
        )
        return {
            "ticker": ticker_us,
            "success": False,
            "records": len(ops),
            "rate_limited": False,
            "ingestion_regression": True,
        }

    # ── Range-proof validation ───────────────────────────────────────
    # Mark complete ONLY if the DB date range covers the provider's
    # date range within tolerance.  This replaces the old arbitrary
    # row-count threshold with a deterministic date-range proof.
    range_proof_pass = _check_range_proof(
        provider_first_date, provider_last_date,
        db_first_date, db_last_date,
    )

    # Common proof fields persisted on every download (pass or fail)
    _proof_fields = {
        "price_history_complete_as_of": db_last_date,
        "needs_price_redownload": False,
        "history_download_records": len(ops),
        # Range proof evidence — persisted so audits can verify later
        "range_proof": {
            "provider_first_date": provider_first_date,
            "provider_last_date": provider_last_date,
            "db_first_date": db_first_date,
            "db_last_date": db_last_date,
            "db_row_count": db_row_count,
            "tolerance_days": _RANGE_PROOF_TOLERANCE_DAYS,
            "pass": range_proof_pass,
            "checked_at": datetime.now(timezone.utc),
        },
        # Ingestion audit trail — proves what was fetched vs written
        "ingestion_audit": {
            "fetched_count": _fetched_count,
            "ops_count": len(ops),
            "pre_db_count": _pre_count,
            "pre_db_earliest": _pre_earliest,
            "pre_db_latest": _pre_latest,
            "post_db_count": db_row_count,
            "post_db_earliest": db_first_date,
            "post_db_latest": db_last_date,
            "needs_redownload": needs_redownload,
        },
    }

    if not range_proof_pass:
        logger.warning(
            "[Phase C] %s: range-proof FAILED — provider=[%s..%s], "
            "db=[%s..%s] (%d rows). Data written but NOT marking "
            "price_history_complete=True — Phase C will retry.",
            ticker_us, provider_first_date, provider_last_date,
            db_first_date, db_last_date, db_row_count,
        )
        await db.tracked_tickers.update_one(
            {"ticker": ticker_us},
            {"$set": {
                **_proof_fields,
                "price_history_status": "range_proof_failed",
                "history_download_error": (
                    f"range_proof_failed:provider=[{provider_first_date}..{provider_last_date}],"
                    f"db=[{db_first_date}..{db_last_date}]"
                ),
            }},
        )
        return {
            "ticker": ticker_us,
            "success": False,
            "records": len(ops),
            "rate_limited": False,
            "range_proof_failed": True,
        }

    await db.tracked_tickers.update_one(
        {"ticker": ticker_us},
        {"$set": {
            **_proof_fields,
            "price_history_complete": True,
            "price_history_status": "complete",
            # Strict proof marker — canonical source for history_download_completed
            "history_download_proven_at": datetime.now(timezone.utc),
            "history_download_proven_anchor": db_last_date,
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


def _check_range_proof(
    provider_first: Optional[str],
    provider_last: Optional[str],
    db_first: Optional[str],
    db_last: Optional[str],
) -> bool:
    """Return True if DB date range covers the provider's range within tolerance.

    Uses _RANGE_PROOF_TOLERANCE_DAYS to allow for close=0 records that are
    legitimately skipped on ingest.  Dates are YYYY-MM-DD strings.
    """
    if not all([provider_first, provider_last, db_first, db_last]):
        return False

    from datetime import timedelta
    try:
        _pf = datetime.strptime(provider_first, "%Y-%m-%d")
        _pl = datetime.strptime(provider_last, "%Y-%m-%d")
        _df = datetime.strptime(db_first, "%Y-%m-%d")
        _dl = datetime.strptime(db_last, "%Y-%m-%d")
    except (ValueError, TypeError):
        return False

    tolerance = timedelta(days=_RANGE_PROOF_TOLERANCE_DAYS)

    # DB first date must be at or before provider_first + tolerance
    if _df > _pf + tolerance:
        return False
    # DB last date must be at or after provider_last - tolerance
    if _dl < _pl - tolerance:
        return False

    return True
