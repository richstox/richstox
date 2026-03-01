# ================================================================================
# SCHEDULER-ONLY SERVICE - DO NOT IMPORT IN RUNTIME ROUTES
# ================================================================================
# This file contains EODHD API calls for benchmark data updates.
# It is ONLY called by scheduler.py at 04:15 Prague time.
# It must NEVER be imported by server.py routes or any runtime code.
#
# Allowed EODHD endpoint for benchmarks:
#   https://eodhd.com/api/eod/{SP500TR.INDX}
# ================================================================================

"""
Benchmark Service - Scheduler-Only
==================================
Updates SP500TR.INDX (S&P 500 Total Return Index) price data.

Called by: scheduler.py at 04:15 Europe/Prague (Mon-Sat)
Never called by: runtime routes, startup events
"""

import os
import logging
import httpx
from datetime import datetime, timedelta

logger = logging.getLogger("richstox.benchmark_service")

EODHD_API_KEY = os.environ.get("EODHD_API_KEY", "")
EODHD_BASE_URL = "https://eodhd.com/api"


async def update_sp500tr_benchmark(db) -> dict:
    """
    Update SP500TR.INDX benchmark price data.
    
    SCHEDULER-ONLY: Called by scheduler.py at 04:15.
    
    Fetches latest prices from EODHD and upserts into stock_prices.
    Only fetches last 30 days to minimize API calls.
    """
    ticker = "SP500TR.INDX"
    
    logger.info(f"Starting SP500TR.INDX benchmark update")
    
    # Calculate date range (last 30 days)
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    
    # EODHD API call
    url = f"{EODHD_BASE_URL}/eod/{ticker}?api_token={EODHD_API_KEY}&fmt=json&from={start_date}&to={end_date}"
    
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(url)
            response.raise_for_status()
            data = response.json()
        
        if not data:
            logger.warning(f"No data returned for {ticker}")
            return {"status": "no_data", "ticker": ticker}
        
        # Upsert each record
        upserted = 0
        for row in data:
            await db.stock_prices.update_one(
                {"ticker": ticker, "date": row["date"]},
                {"$set": {
                    "ticker": ticker,
                    "date": row["date"],
                    "open": row.get("open"),
                    "high": row.get("high"),
                    "low": row.get("low"),
                    "close": row.get("close"),
                    "adjusted_close": row.get("close"),
                    "volume": row.get("volume", 0),
                }},
                upsert=True
            )
            upserted += 1
        
        logger.info(f"SP500TR.INDX update complete: {upserted} records upserted")
        
        return {
            "status": "success",
            "ticker": ticker,
            "records_upserted": upserted,
            "date_range": f"{start_date} to {end_date}"
        }
        
    except httpx.HTTPError as e:
        logger.error(f"EODHD API error for {ticker}: {e}")
        return {"status": "error", "ticker": ticker, "error": str(e)}
    except Exception as e:
        logger.error(f"Unexpected error updating {ticker}: {e}")
        return {"status": "error", "ticker": ticker, "error": str(e)}
