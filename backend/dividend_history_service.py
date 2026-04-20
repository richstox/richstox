# ==============================================================================
# 🛡️ SCHEDULER-ONLY FILE - EXTERNAL API CALLS ALLOWED
# ==============================================================================
# This file is in ALLOWLIST: /app/scripts/audit_external_calls.py
# May call: eodhd.com/api/* (via httpx)
# Context: ONLY from scheduler.py jobs or admin backfill endpoints
# Runtime API endpoints MUST NOT import from this file
# ==============================================================================
"""
RICHSTOX Dividend History Service
=================================
Fetches and stores historical dividend data from EODHD.

Collection:
- dividend_history: Stores all dividend payments per ticker

Used for:
- Calculating Dividend Yield TTM
- Dividends tab visualization (annual chart, YoY growth)
"""

import os
import logging
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional
import httpx

logger = logging.getLogger("richstox.dividends")

EODHD_BASE_URL = "https://eodhd.com/api"
EODHD_API_KEY = os.getenv("EODHD_API_KEY", "")


async def fetch_dividends_from_eodhd(ticker: str, from_date: str = None) -> List[Dict[str, Any]]:
    """
    Fetch dividend history from EODHD API.
    Cost: 1 credit per request.
    
    Args:
        ticker: Stock ticker (e.g., "AAPL" or "AAPL.US")
        from_date: Start date (YYYY-MM-DD), defaults to 10 years ago
    
    Returns:
        List of dividend records
    """
    if not EODHD_API_KEY:
        logger.error("EODHD_API_KEY not configured")
        return []
    
    ticker_full = ticker if ticker.endswith(".US") else f"{ticker}.US"
    
    if not from_date:
        from_date = (datetime.now() - timedelta(days=365 * 10)).strftime("%Y-%m-%d")
    
    url = f"{EODHD_BASE_URL}/div/{ticker_full}"
    params = {
        "api_token": EODHD_API_KEY,
        "fmt": "json",
        "from": from_date,
    }
    
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.get(url, params=params)
            
            if response.status_code == 404:
                logger.info(f"No dividends for {ticker}")
                return []
            
            response.raise_for_status()
            data = response.json()
            
            if not isinstance(data, list):
                return []
            
            return data
    except Exception as e:
        logger.error(f"Failed to fetch dividends for {ticker}: {e}")
        return []


def parse_dividend_records(ticker: str, dividends: List[Dict]) -> List[Dict[str, Any]]:
    """
    Parse EODHD dividend data into normalized records.
    """
    now = datetime.now(timezone.utc)
    ticker_full = ticker if ticker.endswith(".US") else f"{ticker}.US"
    
    records = []
    for div in dividends:
        ex_date = div.get("date")
        if not ex_date:
            continue
        
        amount = div.get("value") or div.get("dividend") or 0
        if amount <= 0:
            continue
        
        records.append({
            "ticker": ticker_full,
            "ex_date": ex_date,
            "payment_date": div.get("paymentDate"),
            "record_date": div.get("recordDate"),
            "declaration_date": div.get("declarationDate"),
            "amount": amount,
            "unadjusted_amount": div.get("unadjustedValue"),
            "currency": div.get("currency", "USD"),
            "created_at": now,
        })
    
    return records


async def sync_ticker_dividends(db, ticker: str) -> Dict[str, Any]:
    """
    Sync dividend history for a single ticker.
    
    Returns:
        Summary of sync operation.
    """
    ticker_upper = ticker.upper()
    ticker_full = ticker_upper if ticker_upper.endswith(".US") else f"{ticker_upper}.US"
    
    result = {
        "ticker": ticker_full,
        "success": False,
        "dividends_synced": 0,
        "error": None,
    }
    
    # Fetch from EODHD
    dividends = await fetch_dividends_from_eodhd(ticker_upper)
    
    if not dividends:
        result["message"] = "No dividend data (stock may not pay dividends)"
        result["success"] = True  # Not an error, just no dividends
        return result
    
    try:
        records = parse_dividend_records(ticker_upper, dividends)
        
        if records:
            # Delete old records and insert new
            await db.dividend_history.delete_many({"ticker": ticker_full})
            await db.dividend_history.insert_many(records)
            result["dividends_synced"] = len(records)
        
        result["success"] = True
        logger.info(f"Synced {len(records)} dividends for {ticker_full}")
        
    except Exception as e:
        result["error"] = str(e)
        logger.error(f"Error syncing dividends for {ticker_full}: {e}")
    
    return result


async def sync_batch_dividends(
    db,
    tickers: List[str],
    delay_between_requests: float = 0.2
) -> Dict[str, Any]:
    """
    Sync dividend history for multiple tickers.
    
    Args:
        db: MongoDB database
        tickers: List of tickers to sync
        delay_between_requests: Delay in seconds between API calls
    
    Returns:
        Summary of batch operation.
    """
    import asyncio
    
    result = {
        "started_at": datetime.now(timezone.utc).isoformat(),
        "total_tickers": len(tickers),
        "success": 0,
        "failed": 0,
        "total_dividends": 0,
        "api_calls": 0,
    }
    
    for ticker in tickers:
        ticker_result = await sync_ticker_dividends(db, ticker)
        result["api_calls"] += 1
        
        if ticker_result["success"]:
            result["success"] += 1
            result["total_dividends"] += ticker_result.get("dividends_synced", 0)
        else:
            result["failed"] += 1
        
        await asyncio.sleep(delay_between_requests)
    
    result["finished_at"] = datetime.now(timezone.utc).isoformat()
    
    # Create index
    await db.dividend_history.create_index([("ticker", 1), ("ex_date", -1)])
    await db.dividend_history.create_index("ex_date")
    
    return result


async def calculate_dividend_yield_ttm(db, ticker: str, current_price: float) -> Optional[float]:
    """
    Calculate trailing 12-month dividend yield.
    
    Formula: sum(dividends_last_365_days) / current_price * 100
    
    Supports both field formats:
    - Legacy: ex_date, amount
    - Backfill: date, value
    
    Returns:
        Dividend yield as percentage, or None if no data.
    """
    ticker_full = ticker if ticker.endswith(".US") else f"{ticker}.US"
    
    one_year_ago = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
    
    # Query using $or to support both field formats
    cursor = db.dividend_history.find({
        "ticker": ticker_full,
        "$or": [
            {"ex_date": {"$gte": one_year_ago}},
            {"date": {"$gte": one_year_ago}}
        ]
    })
    
    dividends = await cursor.to_list(length=100)
    
    if not dividends:
        return None
    
    # Sum amounts using either field name
    total_dividends = sum(
        d.get("amount") or d.get("value") or 0 
        for d in dividends
    )
    
    if current_price <= 0 or total_dividends <= 0:
        return None
    
    dividend_yield_ttm = (total_dividends / current_price) * 100
    return round(dividend_yield_ttm, 4)


async def get_dividend_history_for_ticker(
    db,
    ticker: str,
    years: int = 10
) -> Dict[str, Any]:
    """
    Get dividend history with annual aggregation for UI.
    
    Supports both field formats:
    - Legacy: ex_date, amount (from sync_ticker_dividends)
    - Backfill: date, value (from backfill_dividends.py)
    
    Returns:
        {
            "ticker": str,
            "annual_dividends": [...],  # For bar chart
            "history": [...],           # All dividend records
            "recent_payments": [...],   # Last 8 payments
            "yoy_growth": float,        # Year-over-year growth %
            "status": "growing" | "stable" | "declining" | "no_dividends"
        }
    """
    ticker_full = ticker if ticker.endswith(".US") else f"{ticker}.US"
    
    from_date = (datetime.now() - timedelta(days=365 * years)).strftime("%Y-%m-%d")
    
    # Query using $or to support both field formats
    cursor = db.dividend_history.find(
        {
            "ticker": ticker_full,
            "$or": [
                {"ex_date": {"$gte": from_date}},
                {"date": {"$gte": from_date}}
            ]
        },
        {"_id": 0}
    ).sort([("ex_date", -1), ("date", -1)])
    
    dividends = await cursor.to_list(length=500)
    
    if not dividends:
        return {
            "ticker": ticker_full,
            "annual_dividends": [],
            "history": [],
            "recent_payments": [],
            "yoy_growth": None,
            "status": "no_dividends",
            "total_years": 0,
        }
    
    # Normalize records to consistent format
    normalized = []
    for div in dividends:
        # Get date from either field
        div_date = div.get("ex_date") or div.get("date")
        # Get amount from either field
        amount = div.get("amount") or div.get("value") or 0
        
        if div_date and amount > 0:
            normalized.append({
                "ex_date": div_date,
                "amount": amount,
                "payment_date": div.get("payment_date") or div.get("paymentDate"),
                "currency": div.get("currency", "USD"),
            })
    
    # Sort by date descending
    normalized.sort(key=lambda x: x["ex_date"], reverse=True)
    
    if not normalized:
        return {
            "ticker": ticker_full,
            "annual_dividends": [],
            "history": [],
            "recent_payments": [],
            "yoy_growth": None,
            "status": "no_dividends",
            "total_years": 0,
        }
    
    # Group by year
    by_year = {}
    current_year = datetime.now().year
    
    for div in normalized:
        year = div["ex_date"][:4]
        year_int = int(year)
        if year_int not in by_year:
            by_year[year_int] = []
        by_year[year_int].append(div)
    
    # Build annual totals
    annual_dividends = []
    for year in sorted(by_year.keys()):
        year_divs = by_year[year]
        total = sum(d["amount"] for d in year_divs)
        count = len(year_divs)
        
        annual_dividends.append({
            "year": year,
            "total": round(total, 4),
            "payment_count": count,
            "is_partial": year == current_year,
        })
    
    # Calculate YoY growth (compare last complete year to previous)
    yoy_growth = None
    if len(annual_dividends) >= 2:
        # Find last two complete years
        complete_years = [a for a in annual_dividends if not a["is_partial"]]
        if len(complete_years) >= 2:
            latest = complete_years[-1]["total"]
            previous = complete_years[-2]["total"]
            if previous > 0:
                yoy_growth = round(((latest - previous) / previous) * 100, 2)
    
    # Determine status
    if not annual_dividends:
        status = "no_dividends"
    elif yoy_growth is not None:
        if yoy_growth > 5:
            status = "growing"
        elif yoy_growth < -5:
            status = "declining"
        else:
            status = "stable"
    else:
        status = "stable"
    
    return {
        "ticker": ticker_full,
        "annual_dividends": annual_dividends,
        "history": normalized,
        "recent_payments": normalized[:8],  # Last 8 payments
        "yoy_growth": yoy_growth,
        "status": status,
        "total_years": len(annual_dividends),
    }


async def get_dividend_stats(db) -> Dict[str, Any]:
    """Get statistics about dividend_history collection."""
    total_records = await db.dividend_history.count_documents({})
    unique_tickers = await db.dividend_history.distinct("ticker")
    
    return {
        "total_records": total_records,
        "unique_tickers": len(unique_tickers),
        "sample_tickers": unique_tickers[:10] if unique_tickers else [],
    }


# ---------------------------------------------------------------------------
# Daily automated sync — called from scheduler.py
# ---------------------------------------------------------------------------
DIVIDEND_RESYNC_DAYS = 7  # Re-fetch dividend history every 7 days


async def sync_dividends_for_visible_tickers(db) -> Dict[str, Any]:
    """
    Daily job: sync dividend_history for all visible tickers.

    Logic:
    1. Get all visible tickers from tracked_tickers.
    2. Skip tickers whose dividends_synced_at is < DIVIDEND_RESYNC_DAYS old.
    3. For remaining tickers, call sync_ticker_dividends and stamp
       dividends_synced_at + dividends_sync_status on tracked_tickers.

    Returns summary dict suitable for ops logging.
    """
    import asyncio

    now = datetime.now(timezone.utc)
    resync_cutoff = now - timedelta(days=DIVIDEND_RESYNC_DAYS)

    # All visible tickers
    cursor = db.tracked_tickers.find(
        {"is_visible": True},
        {"_id": 0, "ticker": 1, "dividends_synced_at": 1},
    )
    all_visible = await cursor.to_list(length=10_000)

    # Filter to those needing (re-)sync
    pending = []
    for doc in all_visible:
        last_sync = doc.get("dividends_synced_at")
        if last_sync is not None and getattr(last_sync, "tzinfo", None) is None:
            # MongoDB returns naive datetimes — treat as UTC to match resync_cutoff.
            last_sync = last_sync.replace(tzinfo=timezone.utc)
        if last_sync is None or last_sync < resync_cutoff:
            pending.append(doc["ticker"])

    summary: Dict[str, Any] = {
        "started_at": now.isoformat(),
        "total_visible": len(all_visible),
        "pending_sync": len(pending),
        "synced_ok": 0,
        "synced_fail": 0,
        "total_dividends_written": 0,
    }

    logger.info(
        f"[dividend_sync] Starting: {len(pending)} of {len(all_visible)} "
        f"visible tickers need sync (resync_days={DIVIDEND_RESYNC_DAYS})"
    )

    for ticker in pending:
        try:
            result = await sync_ticker_dividends(db, ticker)
            status = "ok" if result["success"] else "error"
            divs_written = result.get("dividends_synced", 0)

            await db.tracked_tickers.update_one(
                {"ticker": ticker},
                {"$set": {
                    "dividends_synced_at": now,
                    "dividends_sync_status": status,
                }},
            )

            if result["success"]:
                summary["synced_ok"] += 1
                summary["total_dividends_written"] += divs_written
            else:
                summary["synced_fail"] += 1
                logger.warning(f"[dividend_sync] {ticker}: {result.get('error')}")

        except Exception as exc:
            summary["synced_fail"] += 1
            logger.error(f"[dividend_sync] {ticker} unhandled error: {exc}")

        # Rate-limit: 0.25 s between EODHD calls (≈4 req/s)
        await asyncio.sleep(0.25)

    summary["finished_at"] = datetime.now(timezone.utc).isoformat()
    logger.info(
        f"[dividend_sync] Done: ok={summary['synced_ok']}, "
        f"fail={summary['synced_fail']}, dividends={summary['total_dividends_written']}"
    )
    return summary
