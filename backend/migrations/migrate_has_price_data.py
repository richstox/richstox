"""
Migration: Backfill has_price_data and fundamentals_status
============================================================
This migration:
1. Adds has_price_data field to tracked_tickers based on stock_prices existence
2. Adds fundamentals_status field based on current status
3. Recalculates is_active based on has_price_data (not fundamentals)

Run this ONCE after deploying the new logic.
"""

import asyncio
import os
import logging
from datetime import datetime, timezone
from motor.motor_asyncio import AsyncIOMotorClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("migration")

MONGO_URL = os.environ.get("MONGO_URL", "mongodb://localhost:27017")
DB_NAME = os.environ.get("DB_NAME", "richstox")


async def run_migration():
    """
    Backfill has_price_data and fundamentals_status for all tracked_tickers.
    """
    client = AsyncIOMotorClient(MONGO_URL)
    db = client[DB_NAME]
    
    logger.info("Starting migration: backfill has_price_data and fundamentals_status")
    
    # Get all tickers with price data
    tickers_with_prices = await db.stock_prices.distinct("ticker")
    tickers_with_prices_set = set(tickers_with_prices)
    logger.info(f"Found {len(tickers_with_prices_set)} tickers with price data")
    
    # Process all tracked_tickers
    total = await db.tracked_tickers.count_documents({})
    logger.info(f"Processing {total} tracked_tickers")
    
    updated_active = 0
    updated_inactive = 0
    
    async for doc in db.tracked_tickers.find({}):
        ticker = doc.get("ticker")
        old_status = doc.get("status", "unknown")
        
        # Determine fundamentals_status from old status field
        if old_status == "active":
            fundamentals_status = "ok"
        elif old_status == "no_fundamentals":
            fundamentals_status = "missing"
        elif old_status == "pending_fundamentals":
            fundamentals_status = "pending"
        else:
            fundamentals_status = "pending"
        
        # Check if ticker has price data
        has_price_data = ticker in tickers_with_prices_set
        is_active = has_price_data  # NEW: is_active = has price data
        
        # Update the document
        await db.tracked_tickers.update_one(
            {"_id": doc["_id"]},
            {"$set": {
                "has_price_data": has_price_data,
                "fundamentals_status": fundamentals_status,
                "is_active": is_active,
                "migrated_at": datetime.now(timezone.utc),
            }}
        )
        
        if is_active:
            updated_active += 1
        else:
            updated_inactive += 1
    
    logger.info(f"Migration complete:")
    logger.info(f"  - Activated (has_price_data=true): {updated_active}")
    logger.info(f"  - Inactive (no price data): {updated_inactive}")
    
    # Verify counts
    final_active = await db.tracked_tickers.count_documents({"is_active": True})
    final_with_prices = await db.tracked_tickers.count_documents({"has_price_data": True})
    
    logger.info(f"Final counts:")
    logger.info(f"  - is_active=true: {final_active}")
    logger.info(f"  - has_price_data=true: {final_with_prices}")
    
    # Check for fundamentals completeness in active tickers
    active_no_sector = await db.tracked_tickers.count_documents({
        "is_active": True,
        "$or": [{"sector": None}, {"sector": ""}, {"sector": {"$exists": False}}]
    })
    active_no_industry = await db.tracked_tickers.count_documents({
        "is_active": True,
        "$or": [{"industry": None}, {"industry": ""}, {"industry": {"$exists": False}}]
    })
    
    if active_no_sector > 0 or active_no_industry > 0:
        logger.warning(f"AUDIT: Active tickers missing fundamentals:")
        logger.warning(f"  - Missing sector: {active_no_sector}")
        logger.warning(f"  - Missing industry: {active_no_industry}")
        
        # Sample tickers missing sector
        sample = await db.tracked_tickers.find({
            "is_active": True,
            "$or": [{"sector": None}, {"sector": ""}, {"sector": {"$exists": False}}]
        }).limit(20).to_list(20)
        
        logger.warning(f"Sample tickers missing sector:")
        for t in sample:
            logger.warning(f"  - {t.get('ticker')} ({t.get('exchange')}) - fundamentals_status: {t.get('fundamentals_status')}")
    else:
        logger.info("All active tickers have sector and industry data")
    
    client.close()


if __name__ == "__main__":
    asyncio.run(run_migration())
