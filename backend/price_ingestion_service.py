# ==============================================================================
# 🛡️ SCHEDULER-ONLY FILE - EXTERNAL API CALLS ALLOWED
# ==============================================================================
# This file is in ALLOWLIST: /app/scripts/audit_external_calls.py
# May call: eodhd.com/api/* (via httpx)
# Context: ONLY from scheduler.py jobs or admin backfill endpoints
# Runtime API endpoints MUST NOT import from this file
# ==============================================================================
#
# ================================================================================
# UNIVERSE SYSTEM — PERMANENT & BINDING FOR ALL FUTURE INSTANCES
# ================================================================================
# ALLOWED EODHD API ENDPOINTS (ONLY THESE 3):
# 1. SEED:         https://eodhd.com/api/exchange-symbol-list/{NYSE|NASDAQ}
# 2. PRICES:       https://eodhd.com/api/eod-bulk-last-day/US  <-- THIS SERVICE
# 3. FUNDAMENTALS: https://eodhd.com/api/fundamentals/{TICKER}.US
#
# VISIBLE UNIVERSE RULE:
# is_visible = is_seeded && has_price_data && has_classification
#
# APP RUNTIME NEVER CALLS EODHD. All data comes from MongoDB only.
# This service is ONLY called by scheduler/backfill jobs.
#
# Any deviation requires explicit written approval from Richard (kurtarichard@gmail.com).
# ================================================================================

"""
RICHSTOX Price Ingestion Service
=================================
Fetches and stores historical EOD price data from EODHD.

EODHD ENDPOINT: https://eodhd.com/api/eod-bulk-last-day/US

Features:
1. Backfill job: Full IPO-to-present history for each ticker
2. Daily sync: Bulk last day update
3. 52W High/Low computed on-demand from stock_prices

Collection: stock_prices
- ticker: string (e.g., "AAPL.US")
- date: string (YYYY-MM-DD)
- open: float
- high: float
- low: float
- close: float
- adjusted_close: float
- volume: int
"""

import os
import logging
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional
import httpx
import asyncio

logger = logging.getLogger("richstox.prices")

EODHD_BASE_URL = "https://eodhd.com/api"
EODHD_API_KEY = os.getenv("EODHD_API_KEY", "")


async def fetch_eod_history(ticker: str, from_date: str = None, to_date: str = None) -> List[Dict[str, Any]]:
    """
    Fetch EOD price history from EODHD.
    
    If no date range specified, fetches full history (IPO to present).
    
    API: https://eodhd.com/api/eod/{ticker}?api_token=XXX&fmt=json
    Cost: 1 credit per request
    
    Args:
        ticker: Stock ticker (e.g., "AAPL" or "AAPL.US")
        from_date: Start date (YYYY-MM-DD), optional
        to_date: End date (YYYY-MM-DD), optional
    
    Returns:
        List of EOD records
    """
    if not EODHD_API_KEY:
        logger.error("EODHD_API_KEY not configured")
        return []
    
    ticker_full = ticker if ticker.endswith(".US") else f"{ticker}.US"
    
    url = f"{EODHD_BASE_URL}/eod/{ticker_full}"
    params = {
        "api_token": EODHD_API_KEY,
        "fmt": "json",
    }
    
    if from_date:
        params["from"] = from_date
    if to_date:
        params["to"] = to_date
    
    try:
        async with httpx.AsyncClient(timeout=60) as client:
            response = await client.get(url, params=params)
            
            if response.status_code == 404:
                logger.warning(f"No price data for {ticker_full}")
                return []
            
            response.raise_for_status()
            data = response.json()
            
            if not isinstance(data, list):
                logger.warning(f"Unexpected response for {ticker_full}: {type(data)}")
                return []
            
            return data
    except Exception as e:
        logger.error(f"Failed to fetch EOD for {ticker_full}: {e}")
        return []


async def fetch_bulk_eod_latest(exchange: str = "US") -> List[Dict[str, Any]]:
    """
    Fetch bulk EOD data for latest trading day.
    
    API: https://eodhd.com/api/eod-bulk-last-day/{exchange}?api_token=XXX&fmt=json
    Cost: 1 credit (covers entire exchange)
    
    Returns:
        List of EOD records for all tickers
    """
    if not EODHD_API_KEY:
        logger.error("EODHD_API_KEY not configured")
        return []
    
    url = f"{EODHD_BASE_URL}/eod-bulk-last-day/{exchange}"
    params = {
        "api_token": EODHD_API_KEY,
        "fmt": "json",
    }
    
    try:
        async with httpx.AsyncClient(timeout=120) as client:
            response = await client.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            if not isinstance(data, list):
                return []
            
            return data
    except Exception as e:
        logger.error(f"Failed to fetch bulk EOD: {e}")
        return []


def parse_eod_record(ticker: str, record: Dict) -> Dict[str, Any]:
    """Parse EODHD EOD record into normalized format."""
    ticker_full = ticker if ticker.endswith(".US") else f"{ticker}.US"
    
    return {
        "ticker": ticker_full,
        "date": record.get("date"),
        "open": float(record.get("open", 0)) if record.get("open") else None,
        "high": float(record.get("high", 0)) if record.get("high") else None,
        "low": float(record.get("low", 0)) if record.get("low") else None,
        "close": float(record.get("close", 0)) if record.get("close") else None,
        "adjusted_close": float(record.get("adjusted_close", 0)) if record.get("adjusted_close") else None,
        "volume": int(record.get("volume", 0)) if record.get("volume") else None,
    }


async def backfill_ticker_prices(db, ticker: str) -> Dict[str, Any]:
    """
    Backfill full price history for a single ticker.
    
    Fetches IPO-to-present data and upserts into stock_prices.
    Deduplicates on ticker+date.
    
    Returns:
        Summary of backfill operation
    """
    ticker_full = ticker if ticker.endswith(".US") else f"{ticker}.US"
    
    result = {
        "ticker": ticker_full,
        "success": False,
        "records_fetched": 0,
        "records_upserted": 0,
        "date_range": None,
        "error": None,
    }
    
    try:
        # Fetch full history (no date range = IPO to present)
        eod_data = await fetch_eod_history(ticker_full)
        
        if not eod_data:
            result["message"] = "No price data available"
            result["success"] = True  # Not an error
            return result
        
        result["records_fetched"] = len(eod_data)
        
        # Parse and upsert records
        upserted = 0
        for record in eod_data:
            parsed = parse_eod_record(ticker_full, record)
            
            if not parsed["date"]:
                continue
            
            # Upsert (dedupe on ticker+date)
            await db.stock_prices.update_one(
                {"ticker": ticker_full, "date": parsed["date"]},
                {"$set": parsed},
                upsert=True
            )
            upserted += 1
        
        result["records_upserted"] = upserted
        result["success"] = True
        
        if eod_data:
            dates = [r.get("date") for r in eod_data if r.get("date")]
            if dates:
                result["date_range"] = {"from": min(dates), "to": max(dates)}
        
        # CRITICAL: Set has_price_data=true and is_active=true
        # This is what makes the ticker visible in the app
        if upserted > 0:
            await db.tracked_tickers.update_one(
                {"ticker": ticker_full},
                {"$set": {
                    "has_price_data": True,
                    "is_active": True,  # Visible in app = has price data
                    "last_price_date": max(dates) if dates else None,
                    "updated_at": datetime.now(timezone.utc),
                }}
            )
            logger.info(f"Activated {ticker_full}: has_price_data=true, is_active=true")
        
        logger.info(f"Backfilled {upserted} prices for {ticker_full}")
        
    except Exception as e:
        result["error"] = str(e)
        logger.error(f"Error backfilling prices for {ticker_full}: {e}")
    
    return result


async def backfill_batch_prices(
    db,
    tickers: List[str],
    delay_between_requests: float = 0.3
) -> Dict[str, Any]:
    """
    Backfill price history for multiple tickers.
    
    Args:
        db: MongoDB database
        tickers: List of tickers to backfill
        delay_between_requests: Delay between API calls (rate limiting)
    
    Returns:
        Summary of batch operation
    """
    result = {
        "started_at": datetime.now(timezone.utc).isoformat(),
        "total_tickers": len(tickers),
        "success": 0,
        "failed": 0,
        "total_records": 0,
        "api_calls": 0,
    }
    
    for i, ticker in enumerate(tickers):
        ticker_result = await backfill_ticker_prices(db, ticker)
        result["api_calls"] += 1
        
        if ticker_result["success"]:
            result["success"] += 1
            result["total_records"] += ticker_result.get("records_upserted", 0)
        else:
            result["failed"] += 1
        
        # Progress logging every 10 tickers
        if (i + 1) % 10 == 0:
            logger.info(f"Price backfill progress: {i+1}/{len(tickers)}")
        
        await asyncio.sleep(delay_between_requests)
    
    result["finished_at"] = datetime.now(timezone.utc).isoformat()
    
    # Create indexes
    await db.stock_prices.create_index([("ticker", 1), ("date", -1)])
    await db.stock_prices.create_index([("ticker", 1), ("date", 1)], unique=True)
    await db.stock_prices.create_index("date")
    
    return result


async def sync_daily_prices(db) -> Dict[str, Any]:
    """
    Sync latest day prices using EODHD bulk endpoint.
    
    Cost-efficient: 1 API call for entire exchange.
    Logs run to ops_job_runs for tracking.
    
    Returns:
        Summary of sync operation
    """
    started_at = datetime.now(timezone.utc)
    
    result = {
        "job_type": "daily_price_sync",
        "started_at": started_at.isoformat(),
        "api_calls": 1,
        "records_fetched": 0,
        "records_upserted": 0,
        "active_tickers_matched": 0,
        "date": None,
        "status": "running",
    }
    
    try:
        # Fetch bulk data for US exchange
        bulk_data = await fetch_bulk_eod_latest("US")
        
        if not bulk_data:
            result["status"] = "failed"
            result["error"] = "No bulk data returned from EODHD"
            result["finished_at"] = datetime.now(timezone.utc).isoformat()
            await db.ops_job_runs.insert_one(result)
            return result
        
        result["records_fetched"] = len(bulk_data)
        
        # Get list of ACTIVE tickers only (status='active')
        active_tickers = await db.company_fundamentals_cache.distinct("ticker")
        active_set = set(active_tickers)
        result["active_tickers_count"] = len(active_set)
        
        # Upsert only tickers we track
        upserted = 0
        matched = 0
        for record in bulk_data:
            code = record.get("code", "")
            ticker_full = f"{code}.US" if not code.endswith(".US") else code
            
            if ticker_full not in active_set:
                continue
            
            matched += 1
            date = record.get("date")
            if not date:
                continue
            
            result["date"] = date  # Track the date
            
            parsed = {
                "ticker": ticker_full,
                "date": date,
                "open": float(record.get("open", 0)) if record.get("open") else None,
                "high": float(record.get("high", 0)) if record.get("high") else None,
                "low": float(record.get("low", 0)) if record.get("low") else None,
                "close": float(record.get("close", 0)) if record.get("close") else None,
                "adjusted_close": float(record.get("adjusted_close", 0)) if record.get("adjusted_close") else None,
                "volume": int(record.get("volume", 0)) if record.get("volume") else None,
            }
            
            await db.stock_prices.update_one(
                {"ticker": ticker_full, "date": date},
                {"$set": parsed},
                upsert=True
            )
            upserted += 1
        
        result["active_tickers_matched"] = matched
        result["records_upserted"] = upserted
        result["status"] = "completed"
        result["finished_at"] = datetime.now(timezone.utc).isoformat()
        
        logger.info(f"Daily sync: upserted {upserted} price records for {result['date']}")
        
    except Exception as e:
        result["status"] = "failed"
        result["error"] = str(e)
        result["finished_at"] = datetime.now(timezone.utc).isoformat()
        logger.error(f"Error in daily price sync: {e}")
    
    # Log to ops_job_runs
    await db.ops_job_runs.insert_one({
        "job_type": result["job_type"],
        "started_at": started_at,
        "finished_at": datetime.now(timezone.utc),
        "status": result["status"],
        "api_calls": result["api_calls"],
        "records_fetched": result["records_fetched"],
        "records_upserted": result["records_upserted"],
        "active_tickers_matched": result.get("active_tickers_matched", 0),
        "date": result.get("date"),
        "error": result.get("error"),
    })
    
    return result


async def compute_52w_high_low(db, ticker: str) -> Dict[str, Any]:
    """
    Compute 52-week high and low from stock_prices using ADJUSTED CLOSE prices.
    
    ⚠️ DEFAULT RULE - DO NOT CHANGE WITHOUT USER APPROVAL ⚠️
    
    Uses last 252 trading days (approx 1 year).
    
    MUST use adjusted_close for 52W high/low because:
    - Stock splits would show irrelevant historical prices with raw close
    - Dividend adjustments ensure consistency with chart data
    - This is the industry standard for historical comparisons
    
    52w_high = MAX(adjusted_close) WHERE date >= (today - 252 trading days)
    52w_low  = MIN(adjusted_close) WHERE date >= (today - 252 trading days)
    
    Args:
        db: MongoDB database
        ticker: Stock ticker
    
    Returns:
        {
            "fifty_two_week_high": float,
            "fifty_two_week_low": float,
            "high_date": str,
            "low_date": str,
            "days_of_data": int
        }
    """
    ticker_full = ticker if ticker.endswith(".US") else f"{ticker}.US"
    
    # Get last 252 trading days
    cursor = db.stock_prices.find(
        {"ticker": ticker_full},
        {"_id": 0, "date": 1, "adjusted_close": 1}
    ).sort("date", -1).limit(252)
    
    prices = await cursor.to_list(length=252)
    
    if not prices:
        return {
            "fifty_two_week_high": None,
            "fifty_two_week_low": None,
            "high_date": None,
            "low_date": None,
            "days_of_data": 0,
            "source": "stock_prices",
            "computation": "MAX/MIN(adjusted_close) over 252 trading days [DEFAULT]"
        }
    
    # Find MAX and MIN of ADJUSTED CLOSE prices (DEFAULT - DO NOT CHANGE)
    max_adj = None
    max_adj_date = None
    min_adj = None
    min_adj_date = None
    
    for p in prices:
        adj_close = p.get("adjusted_close")
        date = p.get("date")
        
        if adj_close is None:
            continue
        
        if max_adj is None or adj_close > max_adj:
            max_adj = adj_close
            max_adj_date = date
        
        if min_adj is None or adj_close < min_adj:
            min_adj = adj_close
            min_adj_date = date
    
    return {
        "fifty_two_week_high": round(max_adj, 2) if max_adj else None,
        "fifty_two_week_low": round(min_adj, 2) if min_adj else None,
        "high_date": max_adj_date,
        "low_date": min_adj_date,
        "days_of_data": len(prices),
        "source": "stock_prices",
        "computation": "MAX/MIN(adjusted_close) over 252 trading days [DEFAULT]"
    }


async def get_latest_price(db, ticker: str) -> Dict[str, Any]:
    """
    Get latest price for a ticker from stock_prices.
    
    Returns:
        Latest price record or None
    """
    ticker_full = ticker if ticker.endswith(".US") else f"{ticker}.US"
    
    latest = await db.stock_prices.find_one(
        {"ticker": ticker_full},
        {"_id": 0},
        sort=[("date", -1)]
    )
    
    if not latest:
        return None
    
    # Get previous day for change calculation
    prev = await db.stock_prices.find_one(
        {"ticker": ticker_full, "date": {"$lt": latest["date"]}},
        {"_id": 0},
        sort=[("date", -1)]
    )
    
    close = latest.get("adjusted_close") or latest.get("close")
    prev_close = (prev.get("adjusted_close") or prev.get("close")) if prev else close
    
    change = close - prev_close if close and prev_close else 0
    change_pct = (change / prev_close * 100) if prev_close else 0
    
    return {
        "last_close": round(close, 2) if close else None,
        "previous_close": round(prev_close, 2) if prev_close else None,
        "change": round(change, 2) if change else 0,
        "change_pct": round(change_pct, 2) if change_pct else 0,
        "date": latest.get("date"),
        "volume": latest.get("volume"),
        "source": "stock_prices"
    }


async def get_price_stats(db) -> Dict[str, Any]:
    """Get statistics about stock_prices collection (optimized)."""
    # Use aggregation for faster unique count instead of distinct
    pipeline = [
        {"$group": {"_id": "$ticker"}},
        {"$count": "count"}
    ]
    
    # Run queries in parallel
    total_records_task = db.stock_prices.count_documents({})
    oldest_task = db.stock_prices.find_one({}, {"date": 1, "ticker": 1}, sort=[("date", 1)])
    newest_task = db.stock_prices.find_one({}, {"date": 1, "ticker": 1}, sort=[("date", -1)])
    unique_count_task = db.stock_prices.aggregate(pipeline).to_list(1)
    
    total_records, oldest, newest, unique_result = await asyncio.gather(
        total_records_task, oldest_task, newest_task, unique_count_task
    )
    
    unique_count = unique_result[0]["count"] if unique_result else 0
    
    return {
        "total_records": total_records,
        "unique_tickers": unique_count,
        "date_range": {
            "oldest": oldest.get("date") if oldest else None,
            "newest": newest.get("date") if newest else None,
        },
    }



# =============================================================================
# DELTA PRICE FETCHING (Smart sync - avoid redundant downloads)
# =============================================================================

async def get_price_fetch_range(db, ticker: str) -> tuple:
    """
    Determine the date range for fetching prices (delta logic).
    
    Returns: (from_date: str, to_date: str, skip: bool, reason: str)
    
    Logic:
    - If no existing data: fetch from "1990-01-01"
    - If MAX(date) >= yesterday: skip (already up-to-date)
    - Otherwise: fetch from MAX(date) + 1 day
    """
    ticker_full = ticker if ticker.endswith(".US") else f"{ticker}.US"
    
    # Get last known price date for this ticker
    last_price = await db.stock_prices.find_one(
        {"ticker": ticker_full},
        {"_id": 0, "date": 1},
        sort=[("date", -1)]
    )
    
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
    
    if not last_price or not last_price.get("date"):
        # No existing data - fetch full history
        return ("1990-01-01", yesterday, False, "no_existing_data")
    
    last_date = last_price["date"]
    
    if last_date >= yesterday:
        # Already up-to-date
        return (None, None, True, f"up_to_date:{last_date}")
    
    # Calculate from_date = last_date + 1 day
    last_dt = datetime.strptime(last_date, "%Y-%m-%d")
    from_date = (last_dt + timedelta(days=1)).strftime("%Y-%m-%d")
    
    return (from_date, yesterday, False, f"delta_from:{last_date}")


async def sync_ticker_prices_delta(db, ticker: str) -> Dict[str, Any]:
    """
    Fetch only NEW prices for a ticker (delta sync).
    
    This is the smart delta fetching logic:
    1. Check MAX(date) in stock_prices for this ticker
    2. If up-to-date: skip API call entirely
    3. If gap: fetch only from MAX(date)+1 to yesterday
    4. If no data: fetch full history
    
    Returns:
        Summary of sync operation including skip reason if applicable
    """
    ticker_full = ticker if ticker.endswith(".US") else f"{ticker}.US"
    
    from_date, to_date, skip, reason = await get_price_fetch_range(db, ticker_full)
    
    if skip:
        logger.info(f"[DELTA SKIP] {ticker_full}: {reason}")
        return {
            "status": "skipped",
            "ticker": ticker_full,
            "reason": reason,
            "records_fetched": 0,
            "api_calls": 0
        }
    
    logger.info(f"[DELTA FETCH] {ticker_full}: from={from_date} to={to_date} ({reason})")
    
    # Fetch from EODHD API with date range
    eod_data = await fetch_eod_history(ticker_full, from_date, to_date)
    
    if not eod_data:
        return {
            "status": "no_new_data",
            "ticker": ticker_full,
            "from_date": from_date,
            "to_date": to_date,
            "records_fetched": 0,
            "api_calls": 1
        }
    
    # Parse and insert records
    records = []
    for record in eod_data:
        parsed = parse_eod_record(ticker_full, record)
        if parsed.get("date"):
            records.append(parsed)
    
    inserted = 0
    if records:
        # Bulk upsert
        for rec in records:
            await db.stock_prices.update_one(
                {"ticker": ticker_full, "date": rec["date"]},
                {"$set": rec},
                upsert=True
            )
            inserted += 1
        
        # Update tracked_tickers
        dates = [r["date"] for r in records]
        await db.tracked_tickers.update_one(
            {"ticker": ticker_full},
            {"$set": {
                "has_price_data": True,
                "is_active": True,
                "last_price_date": max(dates),
                "updated_at": datetime.now(timezone.utc)
            }}
        )
    
    return {
        "status": "success",
        "ticker": ticker_full,
        "from_date": from_date,
        "to_date": to_date,
        "records_fetched": len(eod_data),
        "records_inserted": inserted,
        "api_calls": 1
    }


async def sync_batch_prices_delta(db, tickers: List[str], delay: float = 0.2) -> Dict[str, Any]:
    """
    Delta sync prices for multiple tickers.
    
    Smart batching: skips up-to-date tickers, fetches only missing data.
    """
    result = {
        "started_at": datetime.now(timezone.utc).isoformat(),
        "total_tickers": len(tickers),
        "skipped": 0,
        "fetched": 0,
        "failed": 0,
        "total_records": 0,
        "api_calls": 0,
        "skip_reasons": {}
    }
    
    for i, ticker in enumerate(tickers):
        try:
            sync_result = await sync_ticker_prices_delta(db, ticker)
            
            if sync_result.get("status") == "skipped":
                result["skipped"] += 1
                reason = sync_result.get("reason", "unknown")
                result["skip_reasons"][reason] = result["skip_reasons"].get(reason, 0) + 1
            elif sync_result.get("status") == "success":
                result["fetched"] += 1
                result["total_records"] += sync_result.get("records_inserted", 0)
            else:
                result["fetched"] += 1  # Attempted but no new data
            
            result["api_calls"] += sync_result.get("api_calls", 0)
            
            # Progress logging
            if (i + 1) % 50 == 0:
                logger.info(f"[DELTA SYNC] Progress: {i+1}/{len(tickers)} "
                           f"(skipped={result['skipped']}, fetched={result['fetched']})")
            
            if sync_result.get("api_calls", 0) > 0:
                await asyncio.sleep(delay)
                
        except Exception as e:
            result["failed"] += 1
            logger.error(f"[DELTA SYNC] Error for {ticker}: {e}")
    
    result["finished_at"] = datetime.now(timezone.utc).isoformat()
    
    logger.info(f"[DELTA SYNC] Complete: {result['fetched']} fetched, "
               f"{result['skipped']} skipped, {result['api_calls']} API calls")
    
    return result



# =============================================================================
# IMPROVED GAP DETECTION + BULK CATCHUP (coverage-based, configurable)
# =============================================================================
# GUARDRAIL: Thresholds from ops_config collection, not hardcoded
# GUARDRAIL: Uses bulk_write for performance
# =============================================================================

# Default values (used only if ops_config not set)
DEFAULT_GAP_LOOKBACK_DAYS = 30
DEFAULT_GAP_COVERAGE_THRESHOLD = 0.80


async def get_gap_detection_config(db) -> Dict[str, Any]:
    """
    Fetch gap detection configuration from ops_config collection.
    
    Returns defaults if not configured.
    """
    config = await db.ops_config.find_one({"key": "gap_detection_config"})
    
    if config and config.get("value"):
        return {
            "lookback_days": config["value"].get("lookback_days", DEFAULT_GAP_LOOKBACK_DAYS),
            "coverage_threshold": config["value"].get("coverage_threshold", DEFAULT_GAP_COVERAGE_THRESHOLD)
        }
    
    return {
        "lookback_days": DEFAULT_GAP_LOOKBACK_DAYS,
        "coverage_threshold": DEFAULT_GAP_COVERAGE_THRESHOLD
    }


def get_trading_days_in_range(start_date: str, end_date: str) -> List[str]:
    """
    Get list of trading days (weekdays) between start_date and end_date.
    Excludes weekends. Does not account for holidays (EODHD returns empty for holidays).
    """
    trading_days = []
    current = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    
    while current <= end:
        if current.weekday() < 5:  # Monday=0 to Friday=4
            trading_days.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)
    
    return trading_days


async def compute_date_coverage(db, dates: List[str], visible_ticker_count: int) -> Dict[str, Dict]:
    """
    Compute price data coverage for each date.
    
    Returns:
        {
            "2026-02-25": {"count": 5500, "coverage": 0.97, "needs_backfill": False},
            "2026-02-24": {"count": 5400, "coverage": 0.95, "needs_backfill": False},
            "2026-02-23": {"count": 100, "coverage": 0.02, "needs_backfill": True},
            ...
        }
    """
    config = await get_gap_detection_config(db)
    coverage_threshold = config["coverage_threshold"]
    
    # Aggregate price counts by date
    pipeline = [
        {"$match": {"date": {"$in": dates}}},
        {"$group": {"_id": "$date", "count": {"$sum": 1}}}
    ]
    
    counts_by_date = {}
    async for doc in db.stock_prices.aggregate(pipeline):
        counts_by_date[doc["_id"]] = doc["count"]
    
    result = {}
    for date in dates:
        count = counts_by_date.get(date, 0)
        coverage = count / visible_ticker_count if visible_ticker_count > 0 else 0
        needs_backfill = coverage < coverage_threshold
        
        result[date] = {
            "count": count,
            "coverage": round(coverage, 4),
            "needs_backfill": needs_backfill
        }
    
    return result


async def detect_price_gaps(db) -> Dict[str, Any]:
    """
    Detect price data gaps in the last N trading days.
    
    A gap is defined as a date where coverage < threshold (configurable).
    
    GUARDRAIL: Thresholds read from ops_config collection.
    
    Returns:
        {
            "config": {"lookback_days": 30, "coverage_threshold": 0.80},
            "visible_ticker_count": 5662,
            "dates_checked": 22,
            "dates_with_gaps": ["2026-02-20", "2026-02-21"],
            "coverage_by_date": {...},
            "fully_missing_dates": [...],
            "partial_coverage_dates": [...]
        }
    """
    # Get config from ops_config
    config = await get_gap_detection_config(db)
    lookback_days = config["lookback_days"]
    coverage_threshold = config["coverage_threshold"]
    
    logger.info(f"[GAP DETECT] Config: lookback_days={lookback_days}, coverage_threshold={coverage_threshold}")
    
    # Get visible ticker count
    visible_ticker_count = await db.tracked_tickers.count_documents({"is_whitelisted": True})
    
    # Calculate date range
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
    start_date = (datetime.now(timezone.utc) - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    
    # Get trading days in range
    trading_days = get_trading_days_in_range(start_date, yesterday)
    
    # Compute coverage for each date
    coverage = await compute_date_coverage(db, trading_days, visible_ticker_count)
    
    # Identify gaps
    fully_missing = []
    partial_coverage = []
    dates_with_gaps = []
    
    for date in sorted(coverage.keys()):
        info = coverage[date]
        if info["needs_backfill"]:
            dates_with_gaps.append(date)
            if info["count"] == 0:
                fully_missing.append(date)
            else:
                partial_coverage.append({
                    "date": date,
                    "count": info["count"],
                    "coverage": info["coverage"]
                })
    
    return {
        "config": {
            "lookback_days": lookback_days,
            "coverage_threshold": coverage_threshold
        },
        "visible_ticker_count": visible_ticker_count,
        "dates_checked": len(trading_days),
        "dates_with_gaps": dates_with_gaps,
        "fully_missing_dates": fully_missing,
        "partial_coverage_dates": partial_coverage,
        "coverage_by_date": coverage
    }


async def fetch_bulk_prices_for_date(date: str) -> List[dict]:
    """
    Fetch bulk prices for a specific date from EODHD.
    
    API: https://eodhd.com/api/eod-bulk-last-day/US?api_token=XXX&date=YYYY-MM-DD&fmt=json
    """
    import aiohttp
    
    # Use module-level constant (loaded at import time from .env)
    api_token = EODHD_API_KEY
    if not api_token:
        logger.error("EODHD_API_KEY not configured")
        return []
    
    url = f"https://eodhd.com/api/eod-bulk-last-day/US"
    params = {
        "api_token": api_token,
        "date": date,
        "fmt": "json"
    }
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=60) as response:
                if response.status != 200:
                    logger.error(f"EODHD bulk API error: {response.status}")
                    return []
                data = await response.json()
                return data if isinstance(data, list) else []
    except Exception as e:
        logger.error(f"EODHD bulk fetch error for {date}: {e}")
        return []


async def run_daily_bulk_catchup(db, job_name: str = "price_sync") -> Dict[str, Any]:
    """
    Fetch the EODHD latest-day bulk file once and upsert into stock_prices.

    Cancel-flag (cancel_job_{job_name} in ops_config) is checked at two points:
      a) Immediately after the EODHD API call returns.
      b) Before each bulk_write batch, so an in-flight write is never interrupted.
    On cancel: flag is deleted, job returns cleanly with status="cancelled".
    """
    from pymongo import UpdateOne

    BULK_WRITE_BATCH_SIZE = 1000
    cancel_key = f"cancel_job_{job_name}"

    async def _cancelled() -> bool:
        doc = await db.ops_config.find_one({"key": cancel_key})
        if doc:
            await db.ops_config.delete_one({"key": cancel_key})
            logger.info("[BULK CATCHUP] Cancel flag consumed, stopping cleanly")
            return True
        return False

    logger.info("[BULK CATCHUP] Fetching latest-day bulk prices from EODHD...")

    # Single API call — no date param = EODHD returns the latest available trading day
    bulk_data = await fetch_bulk_eod_latest("US")

    # ── Cancel check 2a: immediately after the API call returns ──────────────
    if await _cancelled():
        return {
            "status": "cancelled",
            "message": "Cancelled after API fetch, before any writes",
            "dates_processed": 0,
            "records_upserted": 0,
            "api_calls": 1,
            "bulk_writes": 0,
        }

    if not bulk_data:
        logger.warning("[BULK CATCHUP] No data returned from EODHD (possible holiday or API issue)")
        return {
            "status": "success",
            "message": "No data returned from EODHD",
            "dates_processed": 0,
            "records_upserted": 0,
            "api_calls": 1,
            "bulk_writes": 0,
        }

    # Load seeded tickers for filtering (use has_price_data or is_whitelisted per universe)
    visible_tickers: set = set()
    async for doc in db.tracked_tickers.find(
        {"is_whitelisted": True}, {"_id": 0, "ticker": 1}
    ):
        visible_tickers.add(doc["ticker"])

    logger.info(f"[BULK CATCHUP] {len(bulk_data)} raw records, {len(visible_tickers)} tracked tickers")

    # Build bulk operations — filter to tracked tickers only
    bulk_operations = []
    date_seen: Optional[str] = None

    for record in bulk_data:
        code = record.get("code", "")
        ticker = f"{code}.US" if code else None
        if not ticker or ticker not in visible_tickers:
            continue
        date = record.get("date")
        if date:
            date_seen = date
        bulk_operations.append(
            UpdateOne(
                {"ticker": ticker, "date": date},
                {"$set": {
                    "ticker": ticker,
                    "date": date,
                    "open": record.get("open"),
                    "high": record.get("high"),
                    "low": record.get("low"),
                    "close": record.get("close"),
                    "adjusted_close": record.get("adjusted_close"),
                    "volume": record.get("volume"),
                }},
                upsert=True,
            )
        )

    if not bulk_operations:
        logger.info("[BULK CATCHUP] No matching tickers in bulk response")
        return {
            "status": "success",
            "message": "No tracked tickers matched in bulk response",
            "date": date_seen,
            "dates_processed": 0,
            "records_upserted": 0,
            "api_calls": 1,
            "bulk_writes": 0,
        }

    # Execute in batches — cancel check before each batch (soft stop)
    records_upserted = 0
    bulk_writes = 0

    for i in range(0, len(bulk_operations), BULK_WRITE_BATCH_SIZE):
        # ── Cancel check 2b: before each bulk_write batch ────────────────────
        if await _cancelled():
            logger.info(
                f"[BULK CATCHUP] Cancelled before batch {i // BULK_WRITE_BATCH_SIZE + 1} "
                f"({records_upserted} records already written)"
            )
            return {
                "status": "cancelled",
                "message": f"Cancelled during write phase after {records_upserted} records",
                "date": date_seen,
                "dates_processed": 1 if records_upserted > 0 else 0,
                "records_upserted": records_upserted,
                "api_calls": 1,
                "bulk_writes": bulk_writes,
            }

        batch = bulk_operations[i:i + BULK_WRITE_BATCH_SIZE]
        write_result = await db.stock_prices.bulk_write(batch, ordered=False)
        records_upserted += write_result.upserted_count + write_result.modified_count
        bulk_writes += 1

    logger.info(
        f"[BULK CATCHUP] Complete: date={date_seen}, "
        f"{records_upserted} records, {bulk_writes} bulk_write batches"
    )

    return {
        "status": "success",
        "date": date_seen,
        "dates_processed": 1,
        "records_upserted": records_upserted,
        "api_calls": 1,
        "bulk_writes": bulk_writes,
    }
