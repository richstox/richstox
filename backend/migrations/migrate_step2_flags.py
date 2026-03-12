"""
Migration: Step 2 flags initialisation.

One-time migration that sets needs_fundamentals_refresh=True for all
tracked_tickers where:
  - has_price_data = True (ticker passed Step 2 filter)
  - last_fundamentals_fetched_at is null/missing (fundamentals not yet fetched
    or were fetched before this field was introduced)

This ensures Step 3 will catch up and fetch fundamentals for those tickers
on the next run, without requiring a full re-seed.

Run this script once after deploying the Step 2 systemic pipeline changes.
"""

import asyncio
import os
import sys
import logging
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/richstox")
DB_NAME   = os.getenv("MONGO_DB_NAME", "richstox")


async def run_migration(db) -> None:
    """Apply one-time Step 2 flag migration."""

    now = datetime.now(timezone.utc)

    # ── 1. Count scope ────────────────────────────────────────────────────────
    total_with_price = await db.tracked_tickers.count_documents(
        {"has_price_data": True}
    )
    logger.info(f"Tickers with has_price_data=True: {total_with_price}")

    # Count tickers that need the flag set (field absent, null, or empty string)
    missing_fundamentals_fetched = await db.tracked_tickers.count_documents(
        {
            "has_price_data": True,
            "$or": [
                {"last_fundamentals_fetched_at": {"$exists": False}},
                {"last_fundamentals_fetched_at": None},
                {"last_fundamentals_fetched_at": ""},
            ],
        }
    )
    logger.info(
        f"Tickers needing fundamentals refresh (has_price_data=True, "
        f"last_fundamentals_fetched_at missing/null): {missing_fundamentals_fetched}"
    )

    # ── 2. Set needs_fundamentals_refresh=True for unsynced tickers ───────────
    result = await db.tracked_tickers.update_many(
        {
            "has_price_data": True,
            "$or": [
                {"last_fundamentals_fetched_at": {"$exists": False}},
                {"last_fundamentals_fetched_at": None},
                {"last_fundamentals_fetched_at": ""},
            ],
            # Don't overwrite tickers that already have the flag set
            "needs_fundamentals_refresh": {"$ne": True},
        },
        {
            "$set": {
                "needs_fundamentals_refresh": True,
                "migration_step2_flags_at": now,
            }
        },
    )
    logger.info(f"Set needs_fundamentals_refresh=True on {result.modified_count} tickers")

    # ── 3. Summary ────────────────────────────────────────────────────────────
    needs_refresh_total = await db.tracked_tickers.count_documents(
        {"needs_fundamentals_refresh": True}
    )
    logger.info(f"Total tickers with needs_fundamentals_refresh=True: {needs_refresh_total}")
    logger.info("Migration complete.")


async def main() -> None:
    try:
        from motor.motor_asyncio import AsyncIOMotorClient
    except ImportError:
        logger.error("motor not installed. Run: pip install motor")
        sys.exit(1)

    client = AsyncIOMotorClient(MONGO_URI)
    db = client[DB_NAME]
    try:
        await run_migration(db)
    finally:
        client.close()


if __name__ == "__main__":
    asyncio.run(main())
