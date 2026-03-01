#!/usr/bin/env python3
"""
Backfill ALL fundamentals for pending tickers.
Concurrency=10, logs every 500, safety stops.
"""
import asyncio
import httpx
import os
import sys
from datetime import datetime, timezone, timedelta
from motor.motor_asyncio import AsyncIOMotorClient

# Load env
with open('/app/backend/.env') as f:
    for line in f:
        if '=' in line and not line.startswith('#'):
            key, val = line.strip().split('=', 1)
            os.environ[key] = val.strip('"')

EODHD_API_KEY = os.environ.get('EODHD_API_KEY', '')
EODHD_BASE_URL = "https://eodhd.com/api"
CONCURRENCY = 10
LOG_EVERY = 500
MAX_RUNTIME_HOURS = 4
MAX_ERROR_RATE = 0.05
MAX_RATE_LIMIT_BACKOFF = 30

# Stats
stats = {
    "started_at": None,
    "processed": 0,
    "success": 0,
    "failed": 0,
    "api_calls": 0,
    "rows_inserted": 0,
    "errors": [],
    "rate_limit_backoffs": 0,
}

async def fetch_fundamentals(client: httpx.AsyncClient, ticker: str) -> dict:
    """Fetch fundamentals from EODHD."""
    ticker_api = ticker.replace(".US", "").replace(".", "-") + ".US"
    url = f"{EODHD_BASE_URL}/fundamentals/{ticker_api}"
    params = {"api_token": EODHD_API_KEY, "fmt": "json"}
    
    try:
        stats["api_calls"] += 1
        response = await client.get(url, params=params, timeout=30)
        
        if response.status_code == 429:
            return {"ticker": ticker, "rate_limited": True}
        if response.status_code == 404:
            return {"ticker": ticker, "not_found": True}
        
        response.raise_for_status()
        data = response.json()
        return {"ticker": ticker, "data": data}
    except Exception as e:
        return {"ticker": ticker, "error": str(e)[:100]}

async def process_ticker(db, client: httpx.AsyncClient, ticker: str, semaphore: asyncio.Semaphore):
    """Process single ticker."""
    async with semaphore:
        result = await fetch_fundamentals(client, ticker)
        
        if result.get("rate_limited"):
            stats["rate_limit_backoffs"] += 1
            await asyncio.sleep(5)
            result = await fetch_fundamentals(client, ticker)
        
        if result.get("error") or result.get("not_found"):
            stats["failed"] += 1
            if result.get("error"):
                stats["errors"].append({"ticker": ticker, "error": result.get("error")})
            # Mark as no_fundamentals
            await db.tracked_tickers.update_one(
                {"ticker": ticker},
                {"$set": {"status": "no_fundamentals"}}
            )
            return
        
        data = result.get("data", {})
        general = data.get("General", {})
        highlights = data.get("Highlights", {})
        
        if not general:
            stats["failed"] += 1
            return
        
        # Build document
        doc = {
            "ticker": ticker,
            "code": general.get("Code"),
            "name": general.get("Name"),
            "exchange": general.get("Exchange"),
            "sector": general.get("Sector"),
            "industry": general.get("Industry"),
            "description": general.get("Description"),
            "website": general.get("WebURL"),
            "logo_url": general.get("LogoURL"),
            "full_time_employees": general.get("FullTimeEmployees"),
            "ipo_date": general.get("IPODate"),
            "address": general.get("Address"),
            "city": general.get("AddressData", {}).get("City") if general.get("AddressData") else None,
            "state": general.get("AddressData", {}).get("State") if general.get("AddressData") else None,
            "country_name": general.get("CountryName"),
            "asset_type": general.get("Type"),
            "market_cap": highlights.get("MarketCapitalization"),
            "ebitda": highlights.get("EBITDA"),
            "pe_ratio": highlights.get("PERatio"),
            "eps_ttm": highlights.get("EarningsShare"),
            "dividend_yield": highlights.get("DividendYield"),
            "revenue_ttm": highlights.get("RevenueTTM"),
            "profit_margin": highlights.get("ProfitMargin"),
            "beta": data.get("Technicals", {}).get("Beta"),
            "fifty_two_week_high": data.get("Technicals", {}).get("52WeekHigh"),
            "fifty_two_week_low": data.get("Technicals", {}).get("52WeekLow"),
            "updated_at": datetime.now(timezone.utc),
        }
        
        # Upsert
        await db.company_fundamentals_cache.update_one(
            {"ticker": ticker},
            {"$set": doc},
            upsert=True
        )
        
        # Update tracked_tickers
        await db.tracked_tickers.update_one(
            {"ticker": ticker},
            {"$set": {"status": "active", "name": general.get("Name")}}
        )
        
        stats["success"] += 1
        stats["rows_inserted"] += 1

async def main():
    client = AsyncIOMotorClient("mongodb://localhost:27017")
    db = client["test_database"]
    
    # Get pending tickers
    lovable_filter = {
        "is_active": True,
        "exchange": {"$in": ["NYSE", "NASDAQ"]},
        "asset_type": "Common Stock",
        "status": "pending_fundamentals"
    }
    
    tickers = await db.tracked_tickers.distinct("ticker", lovable_filter)
    total = len(tickers)
    
    print(f"[FUNDAMENTALS] Starting backfill for {total} tickers", flush=True)
    stats["started_at"] = datetime.now(timezone.utc)
    
    semaphore = asyncio.Semaphore(CONCURRENCY)
    
    async with httpx.AsyncClient() as http_client:
        tasks = []
        for i, ticker in enumerate(tickers):
            # Safety: check runtime
            elapsed = (datetime.now(timezone.utc) - stats["started_at"]).total_seconds() / 3600
            if elapsed > MAX_RUNTIME_HOURS:
                print(f"[FUNDAMENTALS] SAFETY STOP: Max runtime {MAX_RUNTIME_HOURS}h exceeded", flush=True)
                break
            
            # Safety: check error rate
            if stats["processed"] > 100:
                error_rate = stats["failed"] / stats["processed"]
                if error_rate > MAX_ERROR_RATE:
                    print(f"[FUNDAMENTALS] SAFETY STOP: Error rate {error_rate:.1%} > {MAX_ERROR_RATE:.0%}", flush=True)
                    break
            
            tasks.append(process_ticker(db, http_client, ticker, semaphore))
            stats["processed"] += 1
            
            # Process in batches
            if len(tasks) >= CONCURRENCY * 5:
                await asyncio.gather(*tasks)
                tasks = []
                
                # Log every 500
                if stats["processed"] % LOG_EVERY == 0:
                    elapsed_mins = (datetime.now(timezone.utc) - stats["started_at"]).total_seconds() / 60
                    print(f"[FUNDAMENTALS] Progress: {stats['processed']}/{total} | Success: {stats['success']} | Failed: {stats['failed']} | Time: {elapsed_mins:.1f}m", flush=True)
                
                await asyncio.sleep(0.1)  # Rate limit protection
        
        # Process remaining
        if tasks:
            await asyncio.gather(*tasks)
    
    # Final report
    ended_at = datetime.now(timezone.utc)
    duration = (ended_at - stats["started_at"]).total_seconds()
    
    print(f"\n[FUNDAMENTALS] === COMPLETED ===", flush=True)
    print(f"Duration: {duration/60:.1f} minutes", flush=True)
    print(f"Processed: {stats['processed']}", flush=True)
    print(f"Success: {stats['success']}", flush=True)
    print(f"Failed: {stats['failed']}", flush=True)
    print(f"API Calls: {stats['api_calls']}", flush=True)
    print(f"Rows Inserted: {stats['rows_inserted']}", flush=True)
    
    # Log to DB
    await db.ops_job_runs.insert_one({
        "job_type": "backfill_all_fundamentals",
        "started_at": stats["started_at"],
        "ended_at": ended_at,
        "status": "completed",
        "details": {
            "total_tickers": total,
            "processed": stats["processed"],
            "success": stats["success"],
            "failed": stats["failed"],
            "api_calls": stats["api_calls"],
            "rows_inserted": stats["rows_inserted"],
            "duration_seconds": duration,
            "errors": stats["errors"][:50],
        }
    })

if __name__ == "__main__":
    asyncio.run(main())
