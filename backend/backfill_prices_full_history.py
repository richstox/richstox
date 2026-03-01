# ==============================================================================
# RICHSTOX FULL HISTORY PRICE BACKFILL
# ==============================================================================
# BINDING: Prices Historical Backfill = FULL HISTORY (NO DATES, NO IPO LOGIC)
#
# For each visible ticker:
#   GET https://eodhd.com/api/eod/{TICKER}.US?api_token=...&fmt=json
#   (NO from, NO to parameters)
#
# Skip logic:
#   - Only skip if API returns empty list or hard error
#   - Do NOT use record-count thresholds
#   - Do NOT use IPO date
# ==============================================================================

import asyncio
import httpx
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
import os

logger = logging.getLogger("richstox.backfill_prices_full_history")

EODHD_API_KEY = os.environ.get("EODHD_API_KEY", "")
EODHD_BASE_URL = "https://eodhd.com/api"

# Rate limiting
REQUEST_DELAY_SECONDS = 0.15  # ~6.5 requests per second
BATCH_SIZE = 50  # Progress reporting every 50 tickers


async def fetch_full_history(ticker: str) -> Dict[str, Any]:
    """
    Fetch FULL historical prices for a ticker from EODHD.
    
    BINDING: NO from, NO to parameters - get ALL available history.
    
    Args:
        ticker: Ticker symbol (e.g., "AAPL.US")
        
    Returns:
        Dict with 'data' (list of prices), 'error' (if any), 'http_code'
    """
    url = f"{EODHD_BASE_URL}/eod/{ticker}"
    params = {
        "api_token": EODHD_API_KEY,
        "fmt": "json"
        # NO from, NO to - get full history
    }
    
    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.get(url, params=params)
            http_code = response.status_code
            
            if http_code != 200:
                return {
                    'data': None,
                    'error': f"HTTP {http_code}: {response.text[:200]}",
                    'http_code': http_code
                }
            
            data = response.json()
            
            if isinstance(data, list):
                return {'data': data, 'error': None, 'http_code': 200}
            else:
                return {
                    'data': None,
                    'error': f"Unexpected response format: {type(data)}",
                    'http_code': 200
                }
                
    except httpx.TimeoutException:
        return {'data': None, 'error': "Request timeout (60s)", 'http_code': None}
    except Exception as e:
        return {'data': None, 'error': str(e), 'http_code': None}


async def backfill_prices_full_history(
    db,
    triggered_by: str = "manual"
) -> Dict[str, Any]:
    """
    Backfill FULL price history for ALL visible tickers.
    
    BINDING RULES:
    - NO from/to date parameters
    - NO IPO-based logic
    - NO record-count thresholds
    - Only skip if API returns empty or hard error
    
    Args:
        db: MongoDB database
        triggered_by: "admin_ui", "scheduler", "manual"
        
    Returns:
        Job result dict for ops_job_runs
    """
    job_id = f"backfill_prices_full_history_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    started_at = datetime.now(timezone.utc)
    
    logger.info(f"[{job_id}] Starting FULL HISTORY backfill (NO dates, NO IPO logic)")
    
    # Get ALL visible tickers - no filtering
    cursor = db.tracked_tickers.find(
        {"is_visible": True},
        {"ticker": 1}
    )
    
    tickers = [doc["ticker"] async for doc in cursor]
    total_tickers = len(tickers)
    
    logger.info(f"[{job_id}] Processing {total_tickers} visible tickers")
    
    # Stats
    stats = {
        "tickers_processed": 0,
        "tickers_with_data": 0,
        "tickers_empty": 0,
        "tickers_failed": 0,
        "api_calls": 0,
        "total_records_returned": 0,
        "total_records_inserted": 0,
        "total_records_updated": 0
    }
    
    # Per-ticker log
    ticker_logs = []
    failed_tickers = []
    
    # Inventory snapshot before
    inventory_before = await db.stock_prices.count_documents({})
    
    # Process tickers
    for i, ticker in enumerate(tickers):
        ticker_log = {
            "ticker": ticker,
            "records_returned": 0,
            "records_inserted": 0,
            "records_updated": 0,
            "error": None
        }
        
        try:
            # Fetch full history (NO from, NO to)
            result = await fetch_full_history(ticker)
            stats["api_calls"] += 1
            
            if result['error']:
                ticker_log["error"] = result['error']
                stats["tickers_failed"] += 1
                failed_tickers.append({"ticker": ticker, "error": result['error']})
                logger.warning(f"[{ticker}] API error: {result['error']}")
                
            elif result['data'] is None or len(result['data']) == 0:
                # Empty response - skip but not an error
                stats["tickers_empty"] += 1
                ticker_log["records_returned"] = 0
                logger.info(f"[{ticker}] Empty response - no data available")
                
            else:
                # Has data - upsert all rows
                prices = result['data']
                ticker_log["records_returned"] = len(prices)
                stats["total_records_returned"] += len(prices)
                
                # Prepare bulk upsert
                from pymongo import UpdateOne
                bulk_ops = []
                
                for price in prices:
                    bulk_ops.append(
                        UpdateOne(
                            {"ticker": ticker, "date": price["date"]},
                            {"$set": {
                                "ticker": ticker,
                                "date": price["date"],
                                "open": price.get("open"),
                                "high": price.get("high"),
                                "low": price.get("low"),
                                "close": price.get("close"),
                                "adjusted_close": price.get("adjusted_close"),
                                "volume": price.get("volume")
                            }},
                            upsert=True
                        )
                    )
                
                # Execute bulk upsert
                if bulk_ops:
                    result = await db.stock_prices.bulk_write(bulk_ops, ordered=False)
                    ticker_log["records_inserted"] = result.upserted_count
                    ticker_log["records_updated"] = result.modified_count
                    stats["total_records_inserted"] += result.upserted_count
                    stats["total_records_updated"] += result.modified_count
                    stats["tickers_with_data"] += 1
                    
                    logger.info(f"[{ticker}] {len(prices)} returned, {result.upserted_count} inserted, {result.modified_count} updated")
            
            stats["tickers_processed"] += 1
            ticker_logs.append(ticker_log)
            
            # Rate limiting
            await asyncio.sleep(REQUEST_DELAY_SECONDS)
            
            # Progress log
            if (i + 1) % BATCH_SIZE == 0:
                progress = (i + 1) / total_tickers * 100
                logger.info(f"[{job_id}] Progress: {progress:.1f}% ({i + 1}/{total_tickers})")
                
        except Exception as e:
            logger.error(f"[{job_id}] Error processing {ticker}: {e}")
            ticker_log["error"] = str(e)
            stats["tickers_failed"] += 1
            failed_tickers.append({"ticker": ticker, "error": str(e)})
            ticker_logs.append(ticker_log)
    
    finished_at = datetime.now(timezone.utc)
    
    # Inventory snapshot after
    inventory_after = await db.stock_prices.count_documents({})
    
    # Build result
    result = {
        "job_id": job_id,
        "job_name": "backfill_prices_full_history",
        "status": "completed" if stats["tickers_failed"] == 0 else "completed_with_errors",
        "triggered_by": triggered_by,
        "started_at": started_at,
        "finished_at": finished_at,
        "duration_seconds": (finished_at - started_at).total_seconds(),
        "binding_rule": "FULL HISTORY - NO from/to dates, NO IPO logic, NO record thresholds",
        
        # Summary stats
        "total_tickers": total_tickers,
        "tickers_processed": stats["tickers_processed"],
        "tickers_with_data": stats["tickers_with_data"],
        "tickers_empty": stats["tickers_empty"],
        "tickers_failed": stats["tickers_failed"],
        "api_calls": stats["api_calls"],
        "total_records_returned": stats["total_records_returned"],
        "total_records_inserted": stats["total_records_inserted"],
        "total_records_updated": stats["total_records_updated"],
        
        # Inventory
        "inventory_before": inventory_before,
        "inventory_after": inventory_after,
        "net_records_added": inventory_after - inventory_before,
        
        # Failed tickers list
        "failed_tickers": failed_tickers[:100],  # Limit to 100
        "failed_tickers_count": len(failed_tickers)
    }
    
    # Log to ops_job_runs
    await db.ops_job_runs.insert_one(result)
    
    # Final report
    logger.info("=" * 80)
    logger.info(f"[{job_id}] FINAL REPORT")
    logger.info("=" * 80)
    logger.info(f"Total tickers: {total_tickers}")
    logger.info(f"Processed: {stats['tickers_processed']}")
    logger.info(f"With data: {stats['tickers_with_data']}")
    logger.info(f"Empty: {stats['tickers_empty']}")
    logger.info(f"Failed: {stats['tickers_failed']}")
    logger.info(f"Records returned: {stats['total_records_returned']:,}")
    logger.info(f"Records inserted: {stats['total_records_inserted']:,}")
    logger.info(f"Records updated: {stats['total_records_updated']:,}")
    logger.info(f"Net DB change: {inventory_after - inventory_before:,}")
    logger.info("=" * 80)
    
    return result
