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
from typing import List, Dict, Any, Optional, Callable, Awaitable, Set, Tuple, Union
import httpx
import asyncio
from zoneinfo import ZoneInfo

logger = logging.getLogger("richstox.prices")

EODHD_BASE_URL = "https://eodhd.com/api"
EODHD_API_KEY = os.getenv("EODHD_API_KEY", "")
PRAGUE_TZ = ZoneInfo("Europe/Prague")


def _to_prague_iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(PRAGUE_TZ).isoformat()


def _normalize_step2_ticker(value: Any) -> Optional[str]:
    """
    Canonical Step 2 ticker normalization used by both bulk rows and seeded universe.
    Rules:
    - uppercase
    - trim whitespace
    - normalize optional .US suffix to a single canonical form (always include .US)
    """
    if value is None:
        return None
    text = str(value).strip().upper()
    if not text:
        return None
    if text.endswith(".US"):
        text = text[:-3]
    return f"{text}.US"


def _is_zero_or_missing_close(value: Any) -> bool:
    """Return True if ``value`` is zero, None, or non-numeric (e.g. malformed API data)."""
    if value is None:
        return True
    try:
        return float(value) == 0
    except (ValueError, TypeError):
        return True


async def _read_price_bulk_state(db) -> Optional[Dict[str, Any]]:
    return await db.pipeline_state.find_one({"_id": "price_bulk"})


async def _write_price_bulk_state(db, bulk_date_str: str, now_utc: datetime) -> None:
    await db.pipeline_state.update_one(
        {"_id": "price_bulk"},
        {"$set": {
            "_id": "price_bulk",
            "global_last_bulk_date_processed": bulk_date_str,
            "updated_at": now_utc,
            "updated_at_prague": _to_prague_iso(now_utc),
        }},
        upsert=True,
    )


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
    
    # Remediation URL uses ticker_full constructed above.
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
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(connect=10, read=30, write=10, pool=10)
        ) as client:
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


async def fetch_bulk_eod_latest(
    exchange: str = "US",
    include_meta: bool = False,
    *,
    for_date: str,
) -> Union[List[Dict[str, Any]], Tuple[List[Dict[str, Any]], bool]]:
    """
    Fetch bulk EOD data for a **specific** trading day.

    RULE: ``for_date`` is **mandatory**.  We NEVER call the EODHD bulk
    endpoint without an explicit date because EODHD's "latest" behaviour
    is non-deterministic and can return data for a date we did not intend.
    Every caller must resolve the target date from market_calendar first.

    API: https://eodhd.com/api/eod-bulk-last-day/{exchange}?date=YYYY-MM-DD&api_token=XXX&fmt=json
    Cost: 1 credit (covers entire exchange)

    Args:
        for_date: **Required** YYYY-MM-DD date string.  The ``date``
            query-param is always sent so EODHD returns data for that
            exact trading day.

    Returns:
        List of EOD records for all tickers

    Raises:
        ValueError: if ``for_date`` is empty or None.
    """
    if not for_date:
        raise ValueError(
            "fetch_bulk_eod_latest: for_date is required — "
            "NEVER call bulk EODHD without an explicit date"
        )
    if not EODHD_API_KEY:
        logger.error("EODHD_API_KEY not configured")
        return ([], False) if include_meta else []

    url = f"{EODHD_BASE_URL}/eod-bulk-last-day/{exchange}"
    params: Dict[str, str] = {
        "api_token": EODHD_API_KEY,
        "fmt": "json",
        "date": for_date,
    }
    response_received = False
    try:
        async with httpx.AsyncClient(timeout=120) as client:
            response = await client.get(url, params=params)
            response_received = True
            response.raise_for_status()
            data = response.json()
            
            if not isinstance(data, list):
                return ([], response_received) if include_meta else []

            return (data, response_received) if include_meta else data
    except Exception as e:
        logger.error(f"Failed to fetch bulk EOD: {e}")
        return ([], response_received) if include_meta else []


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

    IMPORTANT: This is a **legacy** convenience endpoint.  It writes price rows
    to ``stock_prices`` but does NOT update ``pipeline_state.price_bulk`` or
    write ``details.price_bulk_gapfill.days`` to ``ops_job_runs``.  As a result,
    the admin "Completed Trading Days" health metric
    (``completed_trading_days_health``) will not reflect runs made through this
    path.  Use "Run Full Pipeline Now"
    (``scheduler_service.run_daily_price_sync``) for canonical price ingestion
    that updates all metric state.  See DASHBOARD_METRIC_AUDIT.md §5.
    
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
        # Resolve the target date from market_calendar — NEVER call bulk
        # without an explicit date.
        from services.market_calendar_service import get_last_closing_day

        last_closing_day = await get_last_closing_day(db, "US")
        if not last_closing_day:
            result["status"] = "failed"
            result["error"] = "Cannot resolve last closing day from market_calendar"
            result["finished_at"] = datetime.now(timezone.utc).isoformat()
            await db.ops_job_runs.insert_one(result)
            return result

        # Fetch bulk data for US exchange with explicit date
        bulk_data = await fetch_bulk_eod_latest("US", for_date=last_closing_day)
        
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


async def run_daily_bulk_catchup(
    db,
    job_name: str = "price_sync",
    progress_cb: Optional[Callable[[int, int, str], Awaitable[None]]] = None,
    seeded_tickers_override: Optional[Set[str]] = None,
    *,
    latest_trading_day: str,
    bulk_data_override: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """
    Fetch the EODHD latest-day bulk file once and upsert into stock_prices.

    RULE: ``latest_trading_day`` is **mandatory**.  We NEVER call the
    EODHD bulk endpoint without an explicit date.  The caller must resolve
    the target date from market_calendar before calling this function.

    Cancel-flag (cancel_job_{job_name} in ops_config) is checked at two points:
      a) Immediately after the EODHD API call returns.
      b) Before each bulk_write batch, so an in-flight write is never interrupted.
    On cancel: flag is deleted, job returns cleanly with status="cancelled".

    Optional progress_cb receives (processed_tickers, expected_tickers_count, "2.1 bulk price sync")
    after each bulk_write batch to stream Step 2 progress.

    latest_trading_day: **Required** YYYY-MM-DD.  The bulk URL always
    includes ``?date={latest_trading_day}`` so EODHD returns data for
    that exact date.  ``processed_date`` is set to this value.

    bulk_data_override: when provided, skip the EODHD fetch and use this data directly.
    Used by the LCD validation loop to avoid double-fetching the same bulk payload.

    Raises:
        ValueError: if ``latest_trading_day`` is empty or None.
    """
    if not latest_trading_day:
        raise ValueError(
            "run_daily_bulk_catchup: latest_trading_day is required — "
            "NEVER call bulk EODHD without an explicit date"
        )
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

    bulk_url_used = f"{EODHD_BASE_URL}/eod-bulk-last-day/US?date={latest_trading_day}"

    # Use pre-fetched bulk data when available (LCD validation loop already
    # fetched it); otherwise make a fresh EODHD API call.
    if bulk_data_override is not None:
        bulk_data = bulk_data_override
        bulk_fetch_executed = True
        logger.info("[BULK CATCHUP] Using pre-fetched bulk data (date=%s, rows=%d)", latest_trading_day, len(bulk_data))
    else:
        logger.info("[BULK CATCHUP] Fetching bulk prices from EODHD (date=%s)", latest_trading_day)
        # Single API call — date param ensures EODHD returns data for the exact trading day
        bulk_data, bulk_fetch_executed = await fetch_bulk_eod_latest(
            "US", include_meta=True, for_date=latest_trading_day,
        )
    raw_row_count = len(bulk_data)

    # ── Cancel check 2a: immediately after the API call returns ──────────────
    if await _cancelled():
        return {
            "status": "cancelled",
            "message": "Cancelled after API fetch, before any writes",
            "dates_processed": 0,
            "records_upserted": 0,
            "api_calls": 1 if bulk_fetch_executed else 0,
            "bulk_fetch_executed": bulk_fetch_executed,
            "raw_row_count": raw_row_count,
            "bulk_writes": 0,
            "bulk_url_used": bulk_url_used,
        }

    if not bulk_data:
        logger.warning("[BULK CATCHUP] No data returned from EODHD (possible holiday or API issue)")
        return {
            "status": "success",
            "message": "No data returned from EODHD",
            "dates_processed": 0,
            "records_upserted": 0,
            "api_calls": 1 if bulk_fetch_executed else 0,
            "bulk_fetch_executed": bulk_fetch_executed,
            "raw_row_count": raw_row_count,
            "bulk_writes": 0,
            "bulk_url_used": bulk_url_used,
        }

    # Load Step 2 universe tickers for filtering bulk_data rows.
    # When seeded_tickers_override is provided (chained pipeline call), use it
    # directly so the ticker set and count match the exact Step 1 output.
    # Otherwise fall back to querying tracked_tickers (standalone / backward compat).
    _STEP2_QUERY: Dict[str, Any] = {
        "exchange": {"$in": ["NYSE", "NASDAQ"]},
        "asset_type": "Common Stock",
    }
    if seeded_tickers_override is not None:
        step2_tickers: set = seeded_tickers_override
        expected_tickers_count = len(step2_tickers)
    else:
        step2_tickers = set(
            await db.tracked_tickers.distinct("ticker", _STEP2_QUERY)
        )
        expected_tickers_count = len(step2_tickers)
    logger.info(f"[BULK CATCHUP] {len(bulk_data)} raw records, {expected_tickers_count} Step 2 universe tickers")

    # Determine provider ticker field explicitly from parsed payload shape.
    sample_row = bulk_data[0] if bulk_data else {}
    bulk_ticker_field: Optional[str] = None
    if isinstance(sample_row, dict):
        if "code" in sample_row:
            bulk_ticker_field = "code"
        elif "symbol" in sample_row:
            bulk_ticker_field = "symbol"
    logger.info(f"[BULK CATCHUP] Using bulk ticker field: {bulk_ticker_field}")
    if not bulk_ticker_field:
        return {
            "status": "error",
            "message": "Bulk payload is missing ticker field (expected code or symbol)",
            "date": None,
            "processed_date": None,
            "unique_dates": [],
            "dates_processed": 0,
            "records_upserted": 0,
            "rows_written": 0,
            "matched_price_tickers_raw": 0,
            "tickers_with_price_data": 0,
            "api_calls": 1 if bulk_fetch_executed else 0,
            "bulk_fetch_executed": bulk_fetch_executed,
            "raw_row_count": raw_row_count,
            "bulk_writes": 0,
            "bulk_url_used": bulk_url_used,
            "tickers_with_price": [],
            "ticker_samples": {
                "bulk_rows_sample": [],
                "bulk_rows_normalized_sample": [],
                "seeded_tickers_sample": [],
                "seeded_tickers_normalized_sample": [],
            },
        }

    # Canonical seeded ticker map by normalized key.
    seeded_tickers_sample: List[str] = []
    seeded_tickers_normalized_sample: List[str] = []
    normalized_seeded_to_canonical: Dict[str, str] = {}
    for seeded_ticker in step2_tickers:
        if not seeded_ticker:
            continue
        seeded_text = str(seeded_ticker)
        if len(seeded_tickers_sample) < 10:
            seeded_tickers_sample.append(seeded_text)
        normalized_seeded = _normalize_step2_ticker(seeded_text)
        if normalized_seeded and len(seeded_tickers_normalized_sample) < 10:
            seeded_tickers_normalized_sample.append(normalized_seeded)
        if normalized_seeded and normalized_seeded not in normalized_seeded_to_canonical:
            normalized_seeded_to_canonical[normalized_seeded] = seeded_text

    # Build normalized bulk lookup (normalized ticker -> rows)
    bulk_rows_sample: List[str] = []
    bulk_rows_normalized_sample: List[str] = []
    normalized_bulk_rows: Dict[str, List[Dict[str, Any]]] = {}
    for record in bulk_data:
        raw_bulk_ticker = record.get(bulk_ticker_field)
        if raw_bulk_ticker is None:
            continue
        raw_bulk_text = str(raw_bulk_ticker)
        if len(bulk_rows_sample) < 10:
            bulk_rows_sample.append(raw_bulk_text)
        normalized_bulk_ticker = _normalize_step2_ticker(raw_bulk_text)
        if not normalized_bulk_ticker:
            continue
        if len(bulk_rows_normalized_sample) < 10:
            bulk_rows_normalized_sample.append(normalized_bulk_ticker)
        normalized_bulk_rows.setdefault(normalized_bulk_ticker, []).append(record)

    ticker_samples = {
        "bulk_rows_sample": bulk_rows_sample,
        "bulk_rows_normalized_sample": bulk_rows_normalized_sample,
        "seeded_tickers_sample": seeded_tickers_sample,
        "seeded_tickers_normalized_sample": seeded_tickers_normalized_sample,
    }

    # Build bulk operations — filter to tracked tickers only after normalization.
    # Group UpdateOne operations with their unique tickers for progress tracking
    batched_operations_with_tickers: List[Tuple[List[UpdateOne], Set[str]]] = []
    current_batch_ops: List[UpdateOne] = []
    current_batch_tickers: Set[str] = set()
    parsed_rows: List[Dict[str, Any]] = []
    matched_seeded_tickers: Set[str] = set()

    # Tickers that appear in bulk with close=0/None are tracked separately.
    # They are NOT written to stock_prices (the UI says "Close price = 0 or
    # missing" is an exclusion filter).  They *are* counted in the overlap
    # (matched_seeded_tickers) but do NOT generate UpdateOne ops.
    zero_price_tickers: Set[str] = set()

    normalized_overlap = (
        set(normalized_seeded_to_canonical.keys()) & set(normalized_bulk_rows.keys())
    )
    for normalized_ticker in normalized_overlap:
        canonical_ticker = normalized_seeded_to_canonical[normalized_ticker]
        for record in normalized_bulk_rows.get(normalized_ticker, []):
            date = record.get("date")
            if not date:
                continue

            # ── Close-price sanity filter ────────────────────────────
            # Skip rows where close is 0, None, or missing.  EODHD
            # sometimes includes halted / delisted tickers with all-zero
            # prices.  Writing them creates garbage stock_prices rows.
            raw_close = record.get("close")
            if _is_zero_or_missing_close(raw_close):
                zero_price_tickers.add(canonical_ticker)
                continue

            parsed_rows.append({"ticker": canonical_ticker, "date": date})
            current_batch_ops.append(
                UpdateOne(
                    {"ticker": canonical_ticker, "date": date},
                    {"$set": {
                        "ticker": canonical_ticker,
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
            current_batch_tickers.add(canonical_ticker)
            matched_seeded_tickers.add(canonical_ticker)

            if len(current_batch_ops) == BULK_WRITE_BATCH_SIZE:
                batched_operations_with_tickers.append(
                    (current_batch_ops, current_batch_tickers)
                )
                current_batch_ops = []
                current_batch_tickers = set()

    unique_dates = sorted(set(row["date"] for row in parsed_rows))
    if len(unique_dates) != 1:
        _error_detail = (
            f"bulk payload has no dates (empty payload) for date={latest_trading_day}"
            if len(unique_dates) == 0
            else f"bulk payload contains multiple dates ({unique_dates}) for date={latest_trading_day}"
        )
        logger.error("[BULK CATCHUP] %s", _error_detail)
        return {
            "status": "error",
            "message": "Bulk payload contains zero or multiple dates",
            "date": None,
            "processed_date": None,
            "unique_dates": unique_dates,
            "dates_processed": 0,
            "records_upserted": 0,
            "rows_written": 0,
            "matched_price_tickers_raw": len(matched_seeded_tickers),
            "tickers_with_price_data": len(matched_seeded_tickers),
            "api_calls": 1 if bulk_fetch_executed else 0,
            "bulk_fetch_executed": bulk_fetch_executed,
            "raw_row_count": raw_row_count,
            "bulk_writes": 0,
            "bulk_url_used": bulk_url_used,
            "tickers_with_price": [],
            "ticker_samples": ticker_samples,
        }
    date_seen = unique_dates[0]

    # ── Market calendar guard: reject data for non-trading days ──────────
    # EODHD may return stale data dated on a holiday (exchange closed).
    # Validate against market_calendar to prevent writing garbage prices.
    #
    # FAIL-OPEN: only skip when a calendar row EXPLICITLY marks the date
    # as non-trading.  If no calendar row exists (e.g. calendar not yet
    # populated), proceed with the write — never silently block syncs.
    try:
        # Delayed import: market_calendar_service is optional here.
        # If the import fails, the except block lets the write proceed.
        from services.market_calendar_service import COLLECTION as _MC_COLLECTION
        _cal_doc = await db[_MC_COLLECTION].find_one(
            {"market": "US", "date": date_seen},
            {"is_trading_day": 1, "holiday_name": 1, "_id": 0},
        )
        if _cal_doc is not None and not _cal_doc.get("is_trading_day", True):
            _holiday_label = _cal_doc.get("holiday_name") or "weekend/holiday"
            logger.warning(
                "[BULK CATCHUP] EODHD returned data for %s which is NOT a trading day "
                "(%s per market_calendar) — skipping write",
                date_seen, _holiday_label,
            )
            return {
                "status": "skipped",
                "message": (
                    f"Bulk data date {date_seen} is not a US trading day "
                    f"({_holiday_label} per market_calendar) — no prices written"
                ),
                "date": date_seen,
                "processed_date": date_seen,
                "unique_dates": unique_dates,
                "dates_processed": 0,
                "records_upserted": 0,
                "rows_written": 0,
                "matched_price_tickers_raw": len(matched_seeded_tickers),
                "tickers_with_price_data": len(matched_seeded_tickers),
                "api_calls": 1 if bulk_fetch_executed else 0,
                "bulk_fetch_executed": bulk_fetch_executed,
                "raw_row_count": raw_row_count,
                "bulk_writes": 0,
                "bulk_url_used": bulk_url_used,
                "tickers_with_price": [],
                "ticker_samples": ticker_samples,
                "skipped_reason": "non_trading_day",
                "holiday_name": _holiday_label,
            }
        if _cal_doc is None:
            logger.info(
                "[BULK CATCHUP] No market_calendar row for %s — proceeding with write (fail-open)",
                date_seen,
            )
    except Exception as _cal_exc:
        # Calendar lookup failure is non-fatal — proceed with write.
        # The calendar may not be populated yet on first run.
        logger.warning(
            "[BULK CATCHUP] Market calendar check failed for %s (%s) — proceeding with write",
            date_seen, _cal_exc,
        )

    if current_batch_ops:
        batched_operations_with_tickers.append(
            (current_batch_ops, current_batch_tickers)
        )

    if zero_price_tickers:
        logger.info(
            "[BULK CATCHUP] Skipped %d ticker(s) with close=0: %s",
            len(zero_price_tickers),
            sorted(zero_price_tickers)[:20],
        )

    if not batched_operations_with_tickers:
        logger.info("[BULK CATCHUP] No matching tickers in bulk response")
        return {
            "status": "success",
            "message": "No tracked tickers matched in bulk response",
            "date": date_seen,
            "dates_processed": 0,
            "records_upserted": 0,
            "rows_written": 0,
            "matched_price_tickers_raw": 0,
            "tickers_with_price_data": 0,
            "api_calls": 1 if bulk_fetch_executed else 0,
            "bulk_fetch_executed": bulk_fetch_executed,
            "raw_row_count": raw_row_count,
            "bulk_writes": 0,
            "bulk_url_used": bulk_url_used,
            "tickers_with_price": [],
            "ticker_samples": ticker_samples,
        }

    # Execute in batches — cancel check before each batch (soft stop)
    records_upserted = 0
    bulk_writes = 0
    processed_ticker_set: Set[str] = set()
    # Pre-compute matched ticker count across all batches for accurate progress total
    matched_ticker_total = len(set().union(*(tks for _, tks in batched_operations_with_tickers)))

    for batch_index, (batch, batch_unique_tickers) in enumerate(batched_operations_with_tickers):
        # ── Cancel check 2b: before each bulk_write batch ────────────────────
        if await _cancelled():
            logger.info(
                f"[BULK CATCHUP] Cancelled before batch {batch_index + 1} "
                f"({records_upserted} records already written)"
            )
            return {
                "status": "cancelled",
                "message": f"Cancelled during write phase after {records_upserted} records",
                "date": date_seen,
                "dates_processed": 1 if records_upserted > 0 else 0,
                "records_upserted": records_upserted,
                "rows_written": records_upserted,
                "matched_price_tickers_raw": len(matched_seeded_tickers),
                "tickers_with_price_data": len(matched_seeded_tickers),
                "api_calls": 1 if bulk_fetch_executed else 0,
                "bulk_fetch_executed": bulk_fetch_executed,
                "raw_row_count": raw_row_count,
                "bulk_writes": bulk_writes,
                "bulk_url_used": bulk_url_used,
                "tickers_with_price": sorted(processed_ticker_set),
                "ticker_samples": ticker_samples,
            }

        write_result = await db.stock_prices.bulk_write(batch, ordered=False)
        records_upserted += write_result.upserted_count + write_result.modified_count
        bulk_writes += 1

        processed_ticker_set.update(batch_unique_tickers)
        if progress_cb:
            await progress_cb(
                len(processed_ticker_set),
                matched_ticker_total,
                "2.1 bulk price sync",
            )

    logger.info(
        f"[BULK CATCHUP] Complete: date={date_seen}, "
        f"{records_upserted} records, {bulk_writes} bulk_write batches"
    )

    return {
        "status": "success",
        "date": date_seen,
        "processed_date": date_seen,
        "unique_dates": unique_dates,
        "dates_processed": 1,
        "records_upserted": records_upserted,
        "rows_written": records_upserted,
        "matched_price_tickers_raw": len(matched_seeded_tickers),
        "tickers_with_price_data": len(matched_seeded_tickers),
        "zero_price_tickers_count": len(zero_price_tickers),
        "api_calls": 1 if bulk_fetch_executed else 0,
        "bulk_fetch_executed": bulk_fetch_executed,
        "raw_row_count": raw_row_count,
        "bulk_writes": bulk_writes,
        "bulk_url_used": bulk_url_used,
        "tickers_with_price": sorted(processed_ticker_set),
        "ticker_samples": ticker_samples,
    }
