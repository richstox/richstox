"""
Migration: Backfill is_whitelisted field
==========================================
This migration sets is_whitelisted=true for all existing Common Stock tickers
that are NYSE/NASDAQ.

Run this ONCE after deploying the Universe System.
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
    Backfill is_whitelisted for all existing tickers.
    """
    client = AsyncIOMotorClient(MONGO_URL)
    db = client[DB_NAME]
    
    logger.info("Starting migration: backfill is_whitelisted")
    
    # Update all NYSE/NASDAQ Common Stock tickers to is_whitelisted=true
    result = await db.tracked_tickers.update_many(
        {
            "exchange": {"$in": ["NYSE", "NASDAQ"]},
            "asset_type": "Common Stock",
        },
        {
            "$set": {
                "is_whitelisted": True,
                "updated_at": datetime.now(timezone.utc),
            }
        }
    )
    
    logger.info(f"Updated {result.modified_count} tickers with is_whitelisted=true")
    
    # Set is_whitelisted=false for non-Common Stock or other exchanges
    result2 = await db.tracked_tickers.update_many(
        {
            "$or": [
                {"exchange": {"$nin": ["NYSE", "NASDAQ"]}},
                {"asset_type": {"$ne": "Common Stock"}},
            ]
        },
        {
            "$set": {
                "is_whitelisted": False,
                "updated_at": datetime.now(timezone.utc),
            }
        }
    )
    
    logger.info(f"Updated {result2.modified_count} tickers with is_whitelisted=false")
    
    # Verify
    whitelisted = await db.tracked_tickers.count_documents({"is_whitelisted": True})
    not_whitelisted = await db.tracked_tickers.count_documents({"is_whitelisted": False})
    
    logger.info(f"Final counts:")
    logger.info(f"  is_whitelisted=true: {whitelisted}")
    logger.info(f"  is_whitelisted=false: {not_whitelisted}")
    
    client.close()


if __name__ == "__main__":
    asyncio.run(run_migration())
