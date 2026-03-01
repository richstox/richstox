"""
RICHSTOX Fundamentals Backfill Job
===================================
One-time backfill to fetch sector/industry for all universe tickers.

Usage:
    python backfill_fundamentals.py [--limit 100] [--dry-run]

This job:
1. Iterates over all universe tickers (is_active=true, NYSE/NASDAQ, Common Stock)
2. Calls EODHD fundamentals endpoint for each ticker
3. Updates tracked_tickers with sector/industry
4. Logs progress to ops_job_runs
"""

import asyncio
import os
import sys
import argparse
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List
import httpx
from motor.motor_asyncio import AsyncIOMotorClient

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("backfill_fundamentals")

MONGO_URL = os.environ.get("MONGO_URL", "mongodb://localhost:27017")
DB_NAME = os.environ.get("DB_NAME", "richstox")
EODHD_API_KEY = os.environ.get("EODHD_API_KEY", "")

# Rate limiting
REQUESTS_PER_SECOND = 5  # EODHD limit
BATCH_SIZE = 50


async def fetch_fundamentals(client: httpx.AsyncClient, ticker: str) -> Dict[str, Any]:
    """Fetch fundamentals from EODHD API."""
    url = f"https://eodhd.com/api/fundamentals/{ticker}"
    params = {
        "api_token": EODHD_API_KEY,
        "fmt": "json"
    }
    
    try:
        response = await client.get(url, params=params, timeout=30)
        if response.status_code == 200:
            return response.json()
        else:
            logger.warning(f"API error for {ticker}: {response.status_code}")
            return None
    except Exception as e:
        logger.error(f"Request failed for {ticker}: {e}")
        return None


def extract_fields(fundamentals: Dict[str, Any]) -> Dict[str, Any]:
    """Extract required fields from fundamentals response."""
    general = fundamentals.get("General", {})
    
    return {
        "sector": general.get("Sector", ""),
        "industry": general.get("Industry", ""),
        "exchange": general.get("Exchange", ""),
        "asset_type": general.get("Type", ""),
        "country": general.get("CountryISO", "US"),
        "primary_ticker": general.get("PrimaryTicker", ""),
        "symbol_canonical": general.get("Code", ""),
        "name": general.get("Name", ""),
        "logo_url": general.get("LogoURL", ""),
        "is_delisted": general.get("IsDelisted", False),
    }


async def run_backfill(limit: int = None, dry_run: bool = False):
    """
    Run fundamentals backfill for all universe tickers missing sector/industry.
    """
    if not EODHD_API_KEY:
        logger.error("EODHD_API_KEY not set!")
        return
    
    client = AsyncIOMotorClient(MONGO_URL)
    db = client[DB_NAME]
    
    job_started = datetime.now(timezone.utc)
    job_id = f"backfill_fundamentals_{job_started.strftime('%Y%m%d_%H%M%S')}"
    
    logger.info(f"Starting backfill job: {job_id}")
    logger.info(f"Dry run: {dry_run}, Limit: {limit or 'ALL'}")
    
    # Find tickers missing sector/industry
    query = {
        "is_active": True,
        "exchange": {"$in": ["NYSE", "NASDAQ"]},
        "asset_type": "Common Stock",
        "$or": [
            {"sector": None},
            {"sector": ""},
            {"sector": {"$exists": False}}
        ]
    }
    
    total_missing = await db.tracked_tickers.count_documents(query)
    logger.info(f"Found {total_missing} tickers missing sector/industry")
    
    if total_missing == 0:
        logger.info("Nothing to backfill!")
        return
    
    # Get tickers to process
    cursor = db.tracked_tickers.find(
        query,
        {"ticker": 1, "_id": 0}
    ).sort("ticker", 1)
    
    if limit:
        cursor = cursor.limit(limit)
    
    tickers = [doc["ticker"] for doc in await cursor.to_list(length=limit or total_missing)]
    logger.info(f"Processing {len(tickers)} tickers")
    
    # Stats
    stats = {
        "total": len(tickers),
        "processed": 0,
        "updated": 0,
        "no_data": 0,
        "errors": 0,
        "skipped_not_common_stock": 0,
    }
    
    # Process in batches with rate limiting
    async with httpx.AsyncClient() as http_client:
        for i, ticker in enumerate(tickers):
            try:
                # Rate limiting
                if i > 0 and i % REQUESTS_PER_SECOND == 0:
                    await asyncio.sleep(1)
                
                # Progress log every 100
                if i > 0 and i % 100 == 0:
                    logger.info(f"Progress: {i}/{len(tickers)} ({100*i/len(tickers):.1f}%)")
                
                # Fetch fundamentals
                fundamentals = await fetch_fundamentals(http_client, ticker)
                stats["processed"] += 1
                
                if not fundamentals or "General" not in fundamentals:
                    stats["no_data"] += 1
                    logger.debug(f"No fundamentals for {ticker}")
                    continue
                
                # Extract fields
                fields = extract_fields(fundamentals)
                
                # Skip if not Common Stock
                if fields["asset_type"] != "Common Stock":
                    stats["skipped_not_common_stock"] += 1
                    logger.info(f"Skipping {ticker}: asset_type={fields['asset_type']}")
                    
                    # Mark as not common stock (deactivate from universe)
                    if not dry_run:
                        await db.tracked_tickers.update_one(
                            {"ticker": ticker},
                            {"$set": {
                                "asset_type": fields["asset_type"],
                                "is_active": False,  # Remove from universe
                                "fundamentals_status": "ok",
                                "updated_at": datetime.now(timezone.utc),
                            }}
                        )
                    continue
                
                # Update tracked_tickers
                if not dry_run:
                    await db.tracked_tickers.update_one(
                        {"ticker": ticker},
                        {"$set": {
                            "sector": fields["sector"],
                            "industry": fields["industry"],
                            "exchange": fields["exchange"],
                            "asset_type": fields["asset_type"],
                            "country": fields["country"],
                            "symbol_canonical": fields["symbol_canonical"],
                            "name": fields["name"],
                            "logo_url": fields["logo_url"],
                            "fundamentals_status": "ok",
                            "updated_at": datetime.now(timezone.utc),
                        }}
                    )
                    
                    # Also update company_fundamentals_cache
                    await db.company_fundamentals_cache.update_one(
                        {"ticker": ticker},
                        {"$set": {
                            "ticker": ticker,
                            "code": fields["symbol_canonical"],
                            "sector": fields["sector"],
                            "industry": fields["industry"],
                            "exchange": fields["exchange"],
                            "name": fields["name"],
                            "logo_url": fields["logo_url"],
                            "updated_at": datetime.now(timezone.utc),
                        }},
                        upsert=True
                    )
                
                stats["updated"] += 1
                logger.debug(f"Updated {ticker}: sector={fields['sector']}, industry={fields['industry']}")
                
            except Exception as e:
                stats["errors"] += 1
                logger.error(f"Error processing {ticker}: {e}")
    
    # Log job completion
    job_completed = datetime.now(timezone.utc)
    duration_seconds = (job_completed - job_started).total_seconds()
    
    logger.info("=" * 60)
    logger.info(f"Backfill complete: {job_id}")
    logger.info(f"Duration: {duration_seconds:.1f}s")
    logger.info(f"Stats: {stats}")
    logger.info("=" * 60)
    
    # Save to ops_job_runs
    if not dry_run:
        await db.ops_job_runs.insert_one({
            "job_type": "backfill_fundamentals",
            "job_id": job_id,
            "started_at": job_started,
            "completed_at": job_completed,
            "duration_seconds": duration_seconds,
            "status": "completed",
            "stats": stats,
        })
    
    # Verify results
    remaining_missing = await db.tracked_tickers.count_documents(query)
    logger.info(f"Remaining missing sector: {remaining_missing}")
    
    client.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill fundamentals for universe tickers")
    parser.add_argument("--limit", type=int, help="Limit number of tickers to process")
    parser.add_argument("--dry-run", action="store_true", help="Don't write to DB")
    args = parser.parse_args()
    
    asyncio.run(run_backfill(limit=args.limit, dry_run=args.dry_run))
