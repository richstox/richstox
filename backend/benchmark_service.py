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

Four operational modes:
  1. **Daily incremental** (default) — scheduler at 04:15, last 30 days.
  2. **Full history** — ``full_history=True``, from 1988-01-01.
  3. **Date-range recovery** — explicit ``date_from`` / ``date_to``.
  4. **Auto-backfill** — when fewer than ``_MIN_RECORDS_FOR_INCREMENTAL``
     records exist in the DB for a benchmark, incremental mode is
     automatically escalated to full-history.  This ensures the first
     run (or recovery from data loss) is self-healing.

Called by: scheduler.py at 04:15 Europe/Prague (Mon-Sat)
           server.py admin endpoint (manual trigger)
"""

import os
import logging
import httpx
from datetime import datetime, timedelta
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

# Default rolling window for daily incremental updates (days)
_DEFAULT_LOOKBACK_DAYS = 30

# Earliest date for full history backfill (SP500TR data begins 1988-01-04)
_EARLIEST_BENCHMARK_DATE = "1988-01-01"

# Minimum records required before incremental mode is allowed.
# If the DB has fewer records for a benchmark, auto-escalate to full_history.
# ~252 trading days ≈ 1 year — a safe threshold to detect missing backfill.
_MIN_RECORDS_FOR_INCREMENTAL = 252


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

    Four modes (checked in priority order):
      1. **Explicit date range** — ``date_from`` / ``date_to`` given →
         fetch exactly that window.  Useful for emergency gap repair.
      2. **Full history** — ``full_history=True`` → fetch from 1988-01-01.
      3. **Auto-backfill** — when the DB has fewer than
         ``_MIN_RECORDS_FOR_INCREMENTAL`` records for this symbol the
         incremental default is automatically escalated to a full-history
         fetch.  This makes the first run (or any run after data loss)
         self-healing without manual intervention.
      4. **Daily incremental** (default) → last 30 calendar days.

    Args:
        db:            Motor database handle.
        symbol:        EODHD ticker, e.g. ``"SP500TR.INDX"``.
        full_history:  When ``True`` fetch the entire available history
                       (from 1988-01-01).  Ignored when explicit dates
                       are provided.
        date_from:     Optional start date ``"YYYY-MM-DD"``.
        date_to:       Optional end date ``"YYYY-MM-DD"`` (defaults to today).

    Returns:
        dict with ``status``, ``ticker``, ``records_upserted``, ``date_range``.
    """
    mode = "incremental"
    if date_from:
        mode = "date_range"
    elif full_history:
        mode = "full_history"
    else:
        # Auto-detect missing backfill: if the DB has very few records for
        # this benchmark, escalate to full_history automatically so the
        # scheduler self-heals without manual intervention.
        existing_count = await db.stock_prices.count_documents({"ticker": symbol})
        if existing_count < _MIN_RECORDS_FOR_INCREMENTAL:
            mode = "full_history"
            full_history = True
            logger.warning(
                f"Auto-backfill triggered for {symbol}: only {existing_count} "
                f"records in DB (threshold={_MIN_RECORDS_FOR_INCREMENTAL}). "
                f"Escalating to full_history mode."
            )
    logger.info(f"Starting benchmark update: {symbol} (mode={mode})")

    end_date = date_to or datetime.now().strftime("%Y-%m-%d")
    if date_from:
        start_date = date_from
    elif full_history:
        start_date = _EARLIEST_BENCHMARK_DATE
    else:
        start_date = (datetime.now() - timedelta(days=_DEFAULT_LOOKBACK_DAYS)).strftime("%Y-%m-%d")

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
            logger.warning(f"No data returned for {symbol}")
            return {"status": "no_data", "ticker": symbol}

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

        return {
            "status": "success",
            "ticker": symbol,
            "records_upserted": upserted,
            "date_range": f"{start_date} to {end_date}",
        }

    except httpx.HTTPError as e:
        logger.error(f"EODHD API error for {symbol}: {e}")
        return {"status": "error", "ticker": symbol, "error": str(e)}
    except Exception as e:
        logger.error(f"Unexpected error updating {symbol}: {e}")
        return {"status": "error", "ticker": symbol, "error": str(e)}


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
        "details": results,
    }
