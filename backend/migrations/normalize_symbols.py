"""
Migration: Normalize Symbol Subscriptions
==========================================
One-time migration to:
1. Convert all *.US suffixed symbols to canonical format (AAPL.US -> AAPL)
2. Remove duplicates in talk_subscriptions by unique (user_id, type, value)

Run this script once to cleanup existing data.
After migration, no more .US suffixes will be stored in talk_subscriptions.

Usage:
    python -m migrations.normalize_symbols
"""

import asyncio
import os
import logging
from datetime import datetime, timezone
from motor.motor_asyncio import AsyncIOMotorClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("migration.normalize_symbols")

# Suffixes to remove
EXCHANGE_SUFFIXES = [".US", ".NYSE", ".NASDAQ", ".AMEX"]


def normalize_symbol(symbol: str) -> str:
    """Normalize a symbol to canonical format."""
    if not symbol:
        return symbol
    
    result = symbol.strip().upper()
    for suffix in EXCHANGE_SUFFIXES:
        if result.endswith(suffix.upper()):
            result = result[:-len(suffix)]
            break
    return result


async def migrate_subscriptions(db):
    """
    Migrate talk_subscriptions to use canonical symbols.
    """
    logger.info("Starting subscription migration...")
    
    # Get all symbol-type subscriptions
    cursor = db.talk_subscriptions.find({"type": "symbol"})
    subscriptions = await cursor.to_list(None)
    
    logger.info(f"Found {len(subscriptions)} symbol subscriptions")
    
    # Track stats
    stats = {
        "total": len(subscriptions),
        "normalized": 0,
        "duplicates_removed": 0,
        "unchanged": 0,
    }
    
    # Group by user_id to detect duplicates
    user_symbols = {}
    
    for sub in subscriptions:
        user_id = sub["user_id"]
        original_value = sub.get("value", "")
        normalized_value = normalize_symbol(original_value)
        
        key = (user_id, normalized_value)
        
        if key in user_symbols:
            # Duplicate - mark for deletion
            logger.info(f"  Duplicate found: user={user_id}, original={original_value}, normalized={normalized_value}")
            await db.talk_subscriptions.delete_one({"_id": sub["_id"]})
            stats["duplicates_removed"] += 1
        else:
            user_symbols[key] = True
            
            if original_value != normalized_value:
                # Update to normalized value
                await db.talk_subscriptions.update_one(
                    {"_id": sub["_id"]},
                    {"$set": {"value": normalized_value}}
                )
                logger.info(f"  Normalized: user={user_id}, {original_value} -> {normalized_value}")
                stats["normalized"] += 1
            else:
                stats["unchanged"] += 1
    
    logger.info(f"Migration complete: {stats}")
    return stats


async def migrate_posts(db):
    """
    Migrate talk_posts to use canonical symbols.
    """
    logger.info("Starting post migration...")
    
    # Get all posts with symbols
    cursor = db.talk_posts.find({
        "$or": [
            {"symbol": {"$exists": True, "$ne": None}},
            {"symbols": {"$exists": True, "$ne": []}}
        ]
    })
    posts = await cursor.to_list(None)
    
    logger.info(f"Found {len(posts)} posts with symbols")
    
    stats = {
        "total": len(posts),
        "normalized": 0,
        "unchanged": 0,
    }
    
    for post in posts:
        post_id = post.get("post_id")
        updates = {}
        changed = False
        
        # Normalize primary symbol
        if post.get("symbol"):
            original = post["symbol"]
            normalized = normalize_symbol(original)
            if original != normalized:
                updates["symbol"] = normalized
                changed = True
        
        # Normalize symbols array
        if post.get("symbols"):
            original_symbols = post["symbols"]
            normalized_symbols = [normalize_symbol(s) for s in original_symbols]
            if original_symbols != normalized_symbols:
                updates["symbols"] = normalized_symbols
                changed = True
        
        if changed:
            await db.talk_posts.update_one(
                {"_id": post["_id"]},
                {"$set": updates}
            )
            logger.info(f"  Normalized post {post_id}: {updates}")
            stats["normalized"] += 1
        else:
            stats["unchanged"] += 1
    
    logger.info(f"Post migration complete: {stats}")
    return stats


async def main():
    """Run the migration."""
    # Connect to MongoDB
    mongo_url = os.environ.get("MONGO_URL", "mongodb://localhost:27017")
    db_name = os.environ.get("DB_NAME", "richstox")
    
    logger.info(f"Connecting to MongoDB: {db_name}")
    client = AsyncIOMotorClient(mongo_url)
    db = client[db_name]
    
    try:
        # Run migrations
        sub_stats = await migrate_subscriptions(db)
        post_stats = await migrate_posts(db)
        
        logger.info("=" * 50)
        logger.info("MIGRATION SUMMARY")
        logger.info("=" * 50)
        logger.info(f"Subscriptions: {sub_stats}")
        logger.info(f"Posts: {post_stats}")
        logger.info("=" * 50)
        
        # Verify no .US suffixes remain
        remaining = await db.talk_subscriptions.count_documents({
            "type": "symbol",
            "value": {"$regex": r"\.US$"}
        })
        
        if remaining > 0:
            logger.warning(f"WARNING: {remaining} subscriptions still have .US suffix!")
        else:
            logger.info("SUCCESS: No .US suffixes remain in talk_subscriptions")
        
        return True
        
    finally:
        client.close()


if __name__ == "__main__":
    asyncio.run(main())
