#!/usr/bin/env python3
"""
Backfill ALL dividends for visible tickers.
Concurrency=10, logs every 500, safety stops.
"""
import asyncio
import httpx
import os
from datetime import datetime, timezone
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

stats = {
    "started_at": None,
    "processed": 0,
    "success": 0,
    "failed": 0,
    "api_calls": 0,
    "rows_inserted": 0,
    "errors": [],
}

async def fetch_dividends(client: httpx.AsyncClient, ticker: str) -> dict:
    """Fetch dividend history from EODHD."""
    ticker_api = ticker.replace(".US", "").replace(".", "-") + ".US"
    url = f"{EODHD_BASE_URL}/div/{ticker_api}"
    params = {"api_token": EODHD_API_KEY, "fmt": "json"}
    
    try:
        stats["api_calls"] += 1
        response = await client.get(url, params=params, timeout=30)
        
        if response.status_code == 429:
            return {"ticker": ticker, "rate_limited": True}
        if response.status_code == 404:
            return {"ticker": ticker, "not_found": True, "data": []}
        
        response.raise_for_status()
        data = response.json()
        return {"ticker": ticker, "data": data if isinstance(data, list) else []}
    except Exception as e:
        return {"ticker": ticker, "error": str(e)[:100], "data": []}

async def process_ticker(db, client: httpx.AsyncClient, ticker: str, semaphore: asyncio.Semaphore):
    """Process single ticker."""
    async with semaphore:
        result = await fetch_dividends(client, ticker)
        
        if result.get("rate_limited"):
            await asyncio.sleep(5)
            result = await fetch_dividends(client, ticker)
        
        if result.get("error"):
            stats["failed"] += 1
            stats["errors"].append({"ticker": ticker, "error": result.get("error")})
            return
        
        dividends = result.get("data", [])
        
        if not dividends:
            stats["success"] += 1  # No dividends is valid
            return
        
        # Prepare bulk operations
        from pymongo import UpdateOne
        operations = []
        
        for div in dividends:
            doc = {
                "ticker": ticker,
                "date": div.get("date"),
                "value": float(div.get("value", 0)) if div.get("value") else 0,
                "unadjusted_value": float(div.get("unadjustedValue", 0)) if div.get("unadjustedValue") else None,
                "currency": div.get("currency", "USD"),
                "declaration_date": div.get("declarationDate"),
                "record_date": div.get("recordDate"),
                "payment_date": div.get("paymentDate"),
            }
            
            operations.append(UpdateOne(
                {"ticker": ticker, "date": div.get("date")},
                {"$set": doc},
                upsert=True
            ))
        
        if operations:
            await db.dividend_history.bulk_write(operations, ordered=False)
            stats["rows_inserted"] += len(operations)
        
        stats["success"] += 1

async def main():
    client = AsyncIOMotorClient("mongodb://localhost:27017")
    db = client["test_database"]
    
    # Index already exists - skip creation
    # await db.dividend_history.create_index([("ticker", 1), ("date", 1)], unique=True)
    
    # Get visible tickers
    lovable_filter = {
        "is_active": True,
        "exchange": {"$in": ["NYSE", "NASDAQ"]},
        "asset_type": "Common Stock"
    }
    
    tickers = await db.tracked_tickers.distinct("ticker", lovable_filter)
    total = len(tickers)
    
    print(f"[DIVIDENDS] Starting backfill for {total} tickers", flush=True)
    stats["started_at"] = datetime.now(timezone.utc)
    
    semaphore = asyncio.Semaphore(CONCURRENCY)
    
    async with httpx.AsyncClient() as http_client:
        tasks = []
        for i, ticker in enumerate(tickers):
            # Safety checks
            elapsed = (datetime.now(timezone.utc) - stats["started_at"]).total_seconds() / 3600
            if elapsed > MAX_RUNTIME_HOURS:
                print(f"[DIVIDENDS] SAFETY STOP: Max runtime exceeded", flush=True)
                break
            
            if stats["processed"] > 100:
                error_rate = stats["failed"] / stats["processed"]
                if error_rate > MAX_ERROR_RATE:
                    print(f"[DIVIDENDS] SAFETY STOP: Error rate {error_rate:.1%}", flush=True)
                    break
            
            tasks.append(process_ticker(db, http_client, ticker, semaphore))
            stats["processed"] += 1
            
            if len(tasks) >= CONCURRENCY * 5:
                await asyncio.gather(*tasks)
                tasks = []
                
                if stats["processed"] % LOG_EVERY == 0:
                    elapsed_mins = (datetime.now(timezone.utc) - stats["started_at"]).total_seconds() / 60
                    print(f"[DIVIDENDS] Progress: {stats['processed']}/{total} | Success: {stats['success']} | Failed: {stats['failed']} | Rows: {stats['rows_inserted']} | Time: {elapsed_mins:.1f}m", flush=True)
                
                await asyncio.sleep(0.1)
        
        if tasks:
            await asyncio.gather(*tasks)
    
    ended_at = datetime.now(timezone.utc)
    duration = (ended_at - stats["started_at"]).total_seconds()
    
    print(f"\n[DIVIDENDS] === COMPLETED ===", flush=True)
    print(f"Duration: {duration/60:.1f} minutes", flush=True)
    print(f"Processed: {stats['processed']}", flush=True)
    print(f"Success: {stats['success']}", flush=True)
    print(f"Failed: {stats['failed']}", flush=True)
    print(f"API Calls: {stats['api_calls']}", flush=True)
    print(f"Rows Inserted: {stats['rows_inserted']}", flush=True)
    
    await db.ops_job_runs.insert_one({
        "job_type": "backfill_all_dividends",
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
        }
    })

if __name__ == "__main__":
    asyncio.run(main())
