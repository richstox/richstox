# ==============================================================================
# RICHSTOX PER-TICKER HISTORICAL PRICE BACKFILL
# ==============================================================================
# BINDING: IPO-to-today history must be filled via per-ticker historical refill
# using /eod/{TICKER}.US?from={IPO_DATE}&to=TODAY
# Daily bulk sync is ONLY for last-day incremental updates after history is complete.
# ==============================================================================

import asyncio
import httpx
import logging
from datetime import datetime, timezone, date
from typing import Dict, Any, List, Optional
import os

logger = logging.getLogger("richstox.backfill_prices_historical")

EODHD_API_KEY = os.environ.get("EODHD_API_KEY", "")
EODHD_BASE_URL = "https://eodhd.com/api"

# Rate limiting
REQUEST_DELAY_SECONDS = 0.15  # ~6.5 requests per second
BATCH_SIZE = 50  # Progress reporting


async def fetch_historical_prices(ticker: str, from_date: str, to_date: str) -> Optional[List[Dict]]:
    """
    Fetch full historical prices for a ticker from EODHD.
    
    BINDING: This is the ONLY way to get IPO-to-today history.
    Daily bulk sync only provides last day's data.
    
    Args:
        ticker: Ticker symbol (e.g., "AAPL.US")
        from_date: Start date (YYYY-MM-DD), typically IPO date
        to_date: End date (YYYY-MM-DD), typically today
        
    Returns:
        List of price records or None if failed
    """
    url = f"{EODHD_BASE_URL}/eod/{ticker}"
    params = {
        "api_token": EODHD_API_KEY,
        "from": from_date,
        "to": to_date,
        "fmt": "json"
    }
    
    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            if isinstance(data, list):
                return data
            else:
                logger.warning(f"[{ticker}] Unexpected response format: {type(data)}")
                return None
    except Exception as e:
        logger.error(f"[{ticker}] Failed to fetch historical prices: {e}")
        return None


async def backfill_prices_historical_per_ticker(
    db,
    triggered_by: str = "manual",
    target_tickers: Optional[List[str]] = None,
    force_refill: bool = False
) -> Dict[str, Any]:
    """
    Backfill full price history (IPO to today) for each visible ticker.
    
    BINDING RULE:
    - IPO-to-today history MUST be filled via this per-ticker endpoint
    - Daily bulk sync is ONLY for last-day incremental updates
    
    Args:
        db: MongoDB database
        triggered_by: "admin_ui", "scheduler", "manual"
        target_tickers: Optional list of specific tickers
        force_refill: If True, refill even if ticker has data
        
    Returns:
        Job result dict for ops_job_runs
    """
    job_id = f"backfill_prices_historical_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    started_at = datetime.now(timezone.utc)
    today = date.today().isoformat()
    
    # Get target tickers with IPO dates
    if target_tickers:
        cursor = db.tracked_tickers.find(
            {"ticker": {"$in": target_tickers}, "is_visible": True},
            {"ticker": 1, "fundamentals.General.IPODate": 1}
        )
    else:
        # Get all visible tickers
        cursor = db.tracked_tickers.find(
            {"is_visible": True},
            {"ticker": 1, "fundamentals.General.IPODate": 1}
        )
    
    tickers_data = []
    async for doc in cursor:
        ticker = doc["ticker"]
        ipo_date = None
        
        # Extract IPO date from fundamentals
        if doc.get("fundamentals") and doc["fundamentals"].get("General"):
            ipo_date = doc["fundamentals"]["General"].get("IPODate")
        
        # Default to 1970-01-01 if no IPO date
        if not ipo_date:
            ipo_date = "1970-01-01"
        
        # Check current price count if not forcing refill
        if not force_refill:
            current_count = await db.stock_prices.count_documents({"ticker": ticker})
            # Skip if already has substantial data (>5000 records = ~20 years)
            if current_count > 5000:
                continue
        
        tickers_data.append({"ticker": ticker, "ipo_date": ipo_date})
    
    total_tickers = len(tickers_data)
    logger.info(f"[{job_id}] Starting historical price backfill for {total_tickers} tickers")
    
    # Stats
    stats = {
        "tickers_targeted": total_tickers,
        "tickers_updated": 0,
        "tickers_skipped": 0,
        "tickers_failed": 0,
        "api_calls": 0,
        "total_records_added": 0,
        "errors": []
    }
    
    # Inventory snapshot before
    inventory_before = {
        "total_price_records": await db.stock_prices.count_documents({})
    }
    
    # Process tickers
    for i, ticker_info in enumerate(tickers_data):
        ticker = ticker_info["ticker"]
        ipo_date = ticker_info["ipo_date"]
        
        try:
            # Fetch historical prices from IPO to today
            prices = await fetch_historical_prices(ticker, ipo_date, today)
            stats["api_calls"] += 1
            
            if not prices:
                stats["tickers_failed"] += 1
                stats["errors"].append({"ticker": ticker, "error": "No data returned"})
                continue
            
            if len(prices) == 0:
                stats["tickers_skipped"] += 1
                continue
            
            # Prepare bulk upsert
            operations = []
            for price in prices:
                operations.append({
                    "filter": {"ticker": ticker, "date": price["date"]},
                    "update": {
                        "$set": {
                            "ticker": ticker,
                            "date": price["date"],
                            "open": price.get("open"),
                            "high": price.get("high"),
                            "low": price.get("low"),
                            "close": price.get("close"),
                            "adjusted_close": price.get("adjusted_close"),
                            "volume": price.get("volume")
                        }
                    },
                    "upsert": True
                })
            
            # Execute bulk upsert
            if operations:
                # Use bulk_write for efficiency
                from pymongo import UpdateOne
                bulk_ops = [
                    UpdateOne(op["filter"], op["update"], upsert=True)
                    for op in operations
                ]
                result = await db.stock_prices.bulk_write(bulk_ops, ordered=False)
                
                records_added = result.upserted_count + result.modified_count
                stats["total_records_added"] += records_added
                stats["tickers_updated"] += 1
                
                logger.info(f"[{ticker}] Added/updated {records_added} records (IPO: {ipo_date})")
            
            # Rate limiting
            await asyncio.sleep(REQUEST_DELAY_SECONDS)
            
            # Progress log
            if (i + 1) % BATCH_SIZE == 0:
                progress = (i + 1) / total_tickers * 100
                logger.info(f"[{job_id}] Progress: {progress:.1f}% ({i + 1}/{total_tickers})")
                
        except Exception as e:
            logger.error(f"[{job_id}] Error processing {ticker}: {e}")
            stats["tickers_failed"] += 1
            stats["errors"].append({"ticker": ticker, "error": str(e)})
    
    finished_at = datetime.now(timezone.utc)
    
    # Inventory snapshot after
    inventory_after = {
        "total_price_records": await db.stock_prices.count_documents({})
    }
    
    # Build result
    result = {
        "job_id": job_id,
        "job_name": "backfill_prices_historical_per_ticker",
        "status": "completed" if stats["tickers_failed"] == 0 else "completed_with_errors",
        "triggered_by": triggered_by,
        "started_at": started_at,
        "finished_at": finished_at,
        "duration_seconds": (finished_at - started_at).total_seconds(),
        "binding_rule": "IPO-to-today via /eod/{TICKER}?from=IPO (NOT daily bulk)",
        "tickers_targeted": stats["tickers_targeted"],
        "tickers_updated": stats["tickers_updated"],
        "tickers_skipped": stats["tickers_skipped"],
        "tickers_failed": stats["tickers_failed"],
        "api_calls": stats["api_calls"],
        "total_records_added": stats["total_records_added"],
        "errors": stats["errors"][:50],
        "inventory_snapshot_before": inventory_before,
        "inventory_snapshot_after": inventory_after
    }
    
    # Log to ops_job_runs
    await db.ops_job_runs.insert_one(result)
    
    logger.info(
        f"[{job_id}] Completed: {stats['tickers_updated']} updated, "
        f"{stats['tickers_failed']} failed, {stats['total_records_added']} records added"
    )
    
    return result


async def get_tickers_missing_history(db, min_expected_records: int = 1000) -> List[Dict]:
    """
    Get list of visible tickers that may be missing historical data.
    
    Args:
        db: MongoDB database
        min_expected_records: Minimum expected records for a "complete" ticker
        
    Returns:
        List of tickers with their current record count and IPO date
    """
    # Aggregate to find tickers with low record counts
    pipeline = [
        {"$match": {"is_visible": True}},
        {"$lookup": {
            "from": "stock_prices",
            "localField": "ticker",
            "foreignField": "ticker",
            "as": "prices"
        }},
        {"$project": {
            "ticker": 1,
            "ipo_date": "$fundamentals.General.IPODate",
            "price_count": {"$size": "$prices"},
            "first_price": {"$min": "$prices.date"},
            "last_price": {"$max": "$prices.date"}
        }},
        {"$match": {"price_count": {"$lt": min_expected_records}}},
        {"$sort": {"price_count": 1}},
        {"$limit": 100}
    ]
    
    result = []
    async for doc in db.tracked_tickers.aggregate(pipeline):
        result.append({
            "ticker": doc["ticker"],
            "ipo_date": doc.get("ipo_date"),
            "price_count": doc["price_count"],
            "first_price": doc.get("first_price"),
            "last_price": doc.get("last_price")
        })
    
    return result
