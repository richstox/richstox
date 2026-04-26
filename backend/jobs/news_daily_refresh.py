#!/usr/bin/env python3
"""
RICHSTOX Daily News Refresh Job
=================================
Runs daily at 13:00 Europe/Prague via supervisor.

This job:
1. Gets HOT tickers (top by followers + recently viewed)
2. Fetches news for each HOT ticker (limit=50, offset=0)
3. Fetches Market Digest (general news)
4. Stores articles with deduplication

Cost rules:
- Only limit=50, offset=0 per ticker
- No pagination in batch job (offset only on user "Load more")
"""

import asyncio
import os
import sys
import logging
from datetime import datetime, timezone

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv

# Load environment
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env'))

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("richstox.news_job")


async def run_daily_refresh():
    """
    Main job function - fetches news for HOT tickers and Market Digest.
    """
    logger.info("=" * 60)
    logger.info("Starting Daily News Refresh Job")
    logger.info(f"Time: {datetime.now(timezone.utc).isoformat()}")
    logger.info("=" * 60)
    
    # Connect to MongoDB
    mongo_url = os.environ.get('MONGO_URL', 'mongodb://localhost:27017')
    db_name = os.environ.get('DB_NAME', 'test_database')
    
    client = AsyncIOMotorClient(mongo_url)
    db = client[db_name]
    
    try:
        # Import services
        from services.news_service import (
            refresh_full_news,
        )

        logger.info("Running full news refresh...")
        result = await refresh_full_news(db)
        total_inserted = result.get("new_articles_stored", 0)
        total_skipped = 0
        hot_tickers = result.get("sample_tickers", [])
        logger.info(f"HOT tickers sample: {hot_tickers}")
        logger.info(
            "  -> Inserted: %s (ticker + market digest)",
            total_inserted,
        )
        
        # 4. Summary
        logger.info("=" * 60)
        logger.info("Daily News Refresh Complete")
        logger.info(f"Total articles inserted: {total_inserted}")
        logger.info(f"Total articles skipped (duplicates): {total_skipped}")
        logger.info(f"HOT tickers processed: {len(hot_tickers)}")
        logger.info("=" * 60)
        
        # Log to DB for monitoring
        await db.job_logs.insert_one({
            "job_name": "news_daily_refresh",
            "status": "completed",
            "started_at": datetime.now(timezone.utc),
            "stats": {
                "hot_tickers_count": len(hot_tickers),
                "articles_inserted": total_inserted,
                "articles_skipped": total_skipped,
            }
        })
        
    except Exception as e:
        logger.error(f"Job failed: {e}")
        await db.job_logs.insert_one({
            "job_name": "news_daily_refresh",
            "status": "failed",
            "started_at": datetime.now(timezone.utc),
            "error": str(e),
        })
        raise
    finally:
        client.close()


if __name__ == "__main__":
    asyncio.run(run_daily_refresh())
