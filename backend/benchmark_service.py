# ================================================================================
# BENCHMARK INGESTION SERVICE
# ================================================================================
# This file contains EODHD API calls for benchmark data updates.
# Called by: scheduler.py at 04:15 Prague time (daily incremental)
#            server.py admin endpoint (manual trigger with optional params)
#
# Allowed EODHD endpoint for benchmarks:
#   https://eodhd.com/api/eod/{SYMBOL}
# ================================================================================

"""
Benchmark Service
=================
Standalone benchmark ingestion — completely independent of the bulk
ticker pricing pipeline and its universe/ticker filters.

Each symbol listed in BENCHMARK_SYMBOLS is fetched via a dedicated
EODHD ``/eod/{symbol}`` API call, so benchmarks are never subject to
the normal seed/filter/bulk-ingest flow.

Sync state is tracked explicitly in the ``benchmark_sync_state``
MongoDB collection (one document per symbol).  This makes mode
selection deterministic:

  1. **Full history** — runs automatically when no ``last_full_backfill_at``
     exists for a symbol (first run / data loss), or when explicitly
     requested via ``full_history=True``.  Fetches from 1988-01-01.
  2. **Daily incremental** (default) — fetches from the day after the
     stored ``latest_date_in_db`` through today.  Runs only when a
     prior full backfill is recorded.
  3. **Date-range repair** — explicit ``date_from`` / ``date_to``
     for manual gap repair.

Called by: scheduler.py at 04:15 Europe/Prague (Mon-Sat)
           server.py admin endpoint (manual trigger)
"""

import os
import logging
import httpx
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Optional

logger = logging.getLogger("richstox.benchmark_service")

EODHD_API_KEY = os.environ.get("EODHD_API_KEY", "")
EODHD_BASE_URL = "https://eodhd.com/api"

# ==============================================================================
# BENCHMARK_SYMBOLS — extensible registry
# ==============================================================================
# Add new benchmarks here.  Each entry maps a human-readable key to the
# EODHD ticker used in the /eod/ endpoint.  The same ticker is used as
# the ``ticker`` field when persisting to ``stock_prices``.
#
# To add a new benchmark (e.g. VIX, DOW30):
#   1. Add an entry below
#   2. Add the ticker to PROTECTED_TICKERS in visibility_rules.py
#   3. No scheduler or pipeline changes needed — update_all_benchmarks
#      iterates this registry automatically.
# ==============================================================================
BENCHMARK_SYMBOLS: Dict[str, str] = {
    "SP500TR": "SP500TR.INDX",       # S&P 500 Total Return Index
    # "VIX":   "VIX.INDX",           # CBOE Volatility Index (future)
    # "DOW30": "DJI.INDX",           # Dow Jones Industrial Average (future)
}

# Earliest date for full history backfill (SP500TR data begins 1988-01-04)
_EARLIEST_BENCHMARK_DATE = "1988-01-01"

# Collection that stores per-symbol sync metadata.
_SYNC_STATE_COLLECTION = "benchmark_sync_state"


# ---------------------------------------------------------------------------
# Sync-state helpers  (benchmark_sync_state collection)
# ---------------------------------------------------------------------------
async def _get_sync_state(db, symbol: str) -> Optional[Dict[str, Any]]:
    """Return the sync-state document for *symbol*, or ``None``."""
    return await db[_SYNC_STATE_COLLECTION].find_one({"symbol": symbol})


async def _save_sync_state(db, symbol: str, update_fields: Dict[str, Any]) -> None:
    """Upsert sync-state fields for *symbol*."""
    await db[_SYNC_STATE_COLLECTION].update_one(
        {"symbol": symbol},
        {"$set": {**update_fields, "symbol": symbol}},
        upsert=True,
    )


async def _refresh_date_bounds(db, symbol: str) -> tuple:
    """Query earliest / latest dates for *symbol* from stock_prices.

    Returns ``(earliest_date_str | None, latest_date_str | None)``.
    """
    pipeline = [
        {"$match": {"ticker": symbol}},
        {"$group": {
            "_id": None,
            "earliest": {"$min": "$date"},
            "latest": {"$max": "$date"},
        }},
    ]
    cursor = db.stock_prices.aggregate(pipeline)
    doc = await cursor.to_list(length=1)
    if doc:
        return doc[0].get("earliest"), doc[0].get("latest")
    return None, None


async def get_benchmark_sync_states(db) -> List[Dict[str, Any]]:
    """Return sync-state documents for all known benchmarks (admin visibility)."""
    states: List[Dict[str, Any]] = []
    for _name, symbol in BENCHMARK_SYMBOLS.items():
        doc = await _get_sync_state(db, symbol)
        if doc:
            doc.pop("_id", None)
            states.append(doc)
        else:
            states.append({"symbol": symbol, "last_status": "never_synced"})
    return states


# ---------------------------------------------------------------------------
# Generic single-benchmark updater
# ---------------------------------------------------------------------------
async def update_benchmark(
    db,
    symbol: str,
    full_history: bool = False,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Fetch and persist price history for a single benchmark *symbol*.

    Mode selection (checked in priority order):
      1. **Explicit date range** — ``date_from`` / ``date_to`` given →
         fetch exactly that window.  Useful for emergency gap repair.
      2. **Full history** — ``full_history=True`` → fetch from 1988-01-01.
      3. **Auto full-history** — when no ``last_full_backfill_at`` exists
         in the sync state for this symbol (first run or after state
         reset).  Self-healing: the next scheduler tick after data loss
         will perform a full backfill automatically.
      4. **Daily incremental** — fetch from the day after
         ``latest_date_in_db`` through today.

    Sync state is updated atomically on success.

    Args:
        db:            Motor database handle.
        symbol:        EODHD ticker, e.g. ``"SP500TR.INDX"``.
        full_history:  When ``True`` fetch the entire available history
                       (from 1988-01-01).  Ignored when explicit dates
                       are provided.
        date_from:     Optional start date ``"YYYY-MM-DD"``.
        date_to:       Optional end date ``"YYYY-MM-DD"`` (defaults to today).

    Returns:
        dict with ``status``, ``ticker``, ``records_upserted``, ``date_range``,
        and ``mode``.
    """
    now_utc = datetime.now(timezone.utc)
    sync_state = await _get_sync_state(db, symbol)

    # ── mode selection ────────────────────────────────────────────────────
    mode = "incremental"
    if date_from:
        mode = "date_range"
    elif full_history:
        mode = "full_history"
    elif not sync_state or not sync_state.get("last_full_backfill_at"):
        mode = "full_history"
        full_history = True
        logger.warning(
            f"No full-backfill state for {symbol} — "
            f"auto-escalating to full_history mode."
        )
    logger.info(f"Starting benchmark update: {symbol} (mode={mode})")

    # ── date-range computation ────────────────────────────────────────────
    end_date = date_to or now_utc.strftime("%Y-%m-%d")
    if date_from:
        start_date = date_from
    elif full_history:
        start_date = _EARLIEST_BENCHMARK_DATE
    else:
        # Incremental: start from the day after the last known date in DB.
        last_known = (sync_state or {}).get("latest_date_in_db")
        if last_known:
            try:
                next_day = (
                    datetime.strptime(last_known, "%Y-%m-%d") + timedelta(days=1)
                ).strftime("%Y-%m-%d")
                start_date = next_day
            except (ValueError, TypeError):
                start_date = (now_utc - timedelta(days=7)).strftime("%Y-%m-%d")
        else:
            # Safety fallback — should not normally be reached because we
            # escalate to full_history when no sync state exists.
            start_date = (now_utc - timedelta(days=7)).strftime("%Y-%m-%d")

    url = (
        f"{EODHD_BASE_URL}/eod/{symbol}"
        f"?api_token={EODHD_API_KEY}&fmt=json&from={start_date}&to={end_date}"
    )

    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.get(url)
            response.raise_for_status()
            data = response.json()

        if not data:
            logger.info(f"No new data returned for {symbol} ({start_date}→{end_date})")
            # Still a success — no gap, just nothing new.
            no_data_state: Dict[str, Any] = {
                "last_status": "no_new_data",
                "last_error": None,
            }
            if mode == "incremental":
                no_data_state["last_incremental_sync_at"] = now_utc.isoformat()
            elif mode == "full_history":
                no_data_state["last_full_backfill_at"] = now_utc.isoformat()
            await _save_sync_state(db, symbol, no_data_state)
            return {
                "status": "no_data",
                "ticker": symbol,
                "records_upserted": 0,
                "date_range": f"{start_date} to {end_date}",
                "mode": mode,
            }

        # Upsert each record
        upserted = 0
        for row in data:
            await db.stock_prices.update_one(
                {"ticker": symbol, "date": row["date"]},
                {"$set": {
                    "ticker": symbol,
                    "date": row["date"],
                    "open": row.get("open"),
                    "high": row.get("high"),
                    "low": row.get("low"),
                    "close": row.get("close"),
                    "adjusted_close": row.get("close"),
                    "volume": row.get("volume", 0),
                }},
                upsert=True,
            )
            upserted += 1

        logger.info(f"{symbol} benchmark update complete: {upserted} records upserted")

        # ── update sync state ─────────────────────────────────────────────
        earliest_in_db, latest_in_db = await _refresh_date_bounds(db, symbol)

        state_update: Dict[str, Any] = {
            "last_status": "success",
            "last_error": None,
            "earliest_date_in_db": earliest_in_db,
            "latest_date_in_db": latest_in_db,
        }
        if mode == "full_history":
            state_update["last_full_backfill_at"] = now_utc.isoformat()
            state_update["last_full_backfill_through_date"] = end_date
        if mode == "incremental":
            state_update["last_incremental_sync_at"] = now_utc.isoformat()
            state_update["last_incremental_sync_through_date"] = end_date

        await _save_sync_state(db, symbol, state_update)

        return {
            "status": "success",
            "ticker": symbol,
            "records_upserted": upserted,
            "date_range": f"{start_date} to {end_date}",
            "mode": mode,
        }

    except httpx.HTTPError as e:
        logger.error(f"EODHD API error for {symbol}: {e}")
        await _save_sync_state(db, symbol, {
            "last_status": "error",
            "last_error": str(e),
        })
        return {"status": "error", "ticker": symbol, "error": str(e), "mode": mode}
    except Exception as e:
        logger.error(f"Unexpected error updating {symbol}: {e}")
        await _save_sync_state(db, symbol, {
            "last_status": "error",
            "last_error": str(e),
        })
        return {"status": "error", "ticker": symbol, "error": str(e), "mode": mode}


# ---------------------------------------------------------------------------
# Convenience wrappers
# ---------------------------------------------------------------------------

async def update_sp500tr_benchmark(db) -> Dict[str, Any]:
    """Update SP500TR.INDX only (backward-compatible entry point)."""
    return await update_benchmark(db, BENCHMARK_SYMBOLS["SP500TR"])


async def update_all_benchmarks(
    db,
    full_history: bool = False,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Update every benchmark in BENCHMARK_SYMBOLS.

    Called by scheduler.py and available as an admin-triggerable job.
    Each benchmark is fetched in its own dedicated API call so it is
    completely independent of the bulk ticker pipeline.

    Optional params ``full_history``, ``date_from``, ``date_to`` are
    forwarded to :func:`update_benchmark` — see its docstring for
    mode-selection rules.
    """
    results: List[Dict[str, Any]] = []
    for name, symbol in BENCHMARK_SYMBOLS.items():
        result = await update_benchmark(
            db, symbol,
            full_history=full_history,
            date_from=date_from,
            date_to=date_to,
        )
        result["benchmark_name"] = name
        results.append(result)

    succeeded = sum(1 for r in results if r.get("status") == "success")
    failed = sum(1 for r in results if r.get("status") == "error")
    total_records = sum(r.get("records_upserted", 0) for r in results)
    # Each benchmark symbol uses one API call
    total_api_calls = len(BENCHMARK_SYMBOLS)

    if failed == 0:
        overall = "success"
    elif succeeded == 0:
        overall = "error"
    else:
        overall = "partial"

    return {
        "status": overall,
        "benchmarks_updated": succeeded,
        "benchmarks_failed": failed,
        "tickers_updated": succeeded,       # standard field for finalize_job_audit_entry
        "api_calls": total_api_calls,       # standard field for finalize_job_audit_entry
        "records_upserted": total_records,
        "details": results,
    }
