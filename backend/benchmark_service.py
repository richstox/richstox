# ================================================================================
# SCHEDULER-ONLY SERVICE - DO NOT IMPORT IN RUNTIME ROUTES
# ================================================================================
# This file contains EODHD API calls for benchmark data updates.
# It is ONLY called by scheduler.py at 04:15 Prague time.
# It must NEVER be imported by server.py routes or any runtime code.
#
# Allowed EODHD endpoint for benchmarks:
#   https://eodhd.com/api/eod/{SYMBOL}
# ================================================================================

"""
Benchmark Service - Scheduler-Only
==================================
Standalone benchmark ingestion — completely independent of the bulk
ticker pricing pipeline and its universe/ticker filters.

Each symbol listed in BENCHMARK_SYMBOLS is fetched via a dedicated
EODHD ``/eod/{symbol}`` API call, so benchmarks are never subject to
the normal seed/filter/bulk-ingest flow.

Called by: scheduler.py at 04:15 Europe/Prague (Mon-Sat)
Never called by: runtime routes, startup events
"""

import os
import logging
import httpx
from datetime import datetime, timedelta
from typing import Dict, Any, List

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


# ---------------------------------------------------------------------------
# Generic single-benchmark updater
# ---------------------------------------------------------------------------
async def update_benchmark(
    db,
    symbol: str,
    full_history: bool = False,
) -> Dict[str, Any]:
    """
    Fetch and persist price history for a single benchmark *symbol*.

    Args:
        db:            Motor database handle.
        symbol:        EODHD ticker, e.g. ``"SP500TR.INDX"``.
        full_history:  When ``True`` fetch the entire available history
                       (from 1988-01-01).  Default ``False`` fetches only the
                       last 30 days to minimise API calls.

    Returns:
        dict with ``status``, ``ticker``, ``records_upserted``, ``date_range``.
    """
    logger.info(f"Starting benchmark update: {symbol} (full_history={full_history})")

    end_date = datetime.now().strftime("%Y-%m-%d")
    if full_history:
        start_date = "1988-01-01"
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


async def update_all_benchmarks(db) -> Dict[str, Any]:
    """
    Update every benchmark in BENCHMARK_SYMBOLS.

    Called by scheduler.py and available as an admin-triggerable job.
    Each benchmark is fetched in its own dedicated API call so it is
    completely independent of the bulk ticker pipeline.
    """
    results: List[Dict[str, Any]] = []
    for name, symbol in BENCHMARK_SYMBOLS.items():
        result = await update_benchmark(db, symbol)
        result["benchmark_name"] = name
        results.append(result)

    succeeded = sum(1 for r in results if r.get("status") == "success")
    failed = sum(1 for r in results if r.get("status") == "error")

    return {
        "status": "success" if failed == 0 else "partial",
        "benchmarks_updated": succeeded,
        "benchmarks_failed": failed,
        "details": results,
    }
