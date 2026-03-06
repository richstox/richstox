#!/usr/bin/env python3
"""
Idempotent migration: user_watchlist.user_email → user_id
==========================================================

Converts legacy `user_email` field to `user_id` by looking up
each email in the `users` collection.

Safe to re-run:
  - Skips documents that already have a non-empty `user_id`
  - Logs and skips emails that have no matching user
  - Creates unique compound index {user_id: 1, ticker: 1} only after
    all documents are clean (no nulls in user_id)

Usage:
    python scripts/migrate_watchlist_user_id.py

Environment:
    MONGO_URL  (default: mongodb://localhost:27017)
    DB_NAME    (required)
"""

import asyncio
import os
import sys
import logging

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
logger = logging.getLogger("migrate_watchlist")

# Allow imports from parent directory
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


async def main():
    from motor.motor_asyncio import AsyncIOMotorClient

    mongo_url = os.environ.get("MONGO_URL", "mongodb://localhost:27017")
    db_name = os.environ.get("DB_NAME")
    if not db_name:
        logger.error("DB_NAME environment variable is required")
        sys.exit(1)

    client = AsyncIOMotorClient(mongo_url)
    db = client[db_name]

    # ── Step 1: Build email → user_id lookup ────────────────────────────
    users = await db.users.find({}, {"email": 1, "user_id": 1, "_id": 0}).to_list(None)
    email_to_uid: dict[str, str] = {}
    for u in users:
        email = u.get("email")
        uid = u.get("user_id")
        if email and uid:
            email_to_uid[email] = uid
    logger.info("Loaded %d users for email→user_id lookup", len(email_to_uid))

    # ── Step 2: Migrate documents that need it ──────────────────────────
    docs = await db.user_watchlist.find(
        {"$or": [
            {"user_id": {"$exists": False}},
            {"user_id": None},
            {"user_id": ""},
        ]},
    ).to_list(None)

    migrated = 0
    skipped_no_user = 0
    already_ok = 0

    for doc in docs:
        email = doc.get("user_email")
        uid = email_to_uid.get(email) if email else None
        if not uid:
            logger.warning("No user found for email=%s (doc _id=%s) — skipped", email, doc["_id"])
            skipped_no_user += 1
            continue

        await db.user_watchlist.update_one(
            {"_id": doc["_id"]},
            {"$set": {"user_id": uid}},
        )
        migrated += 1

    # Count already-OK docs
    already_ok = await db.user_watchlist.count_documents(
        {"user_id": {"$exists": True, "$nin": [None, ""]}}
    )

    logger.info(
        "Migration done: migrated=%d, skipped_no_user=%d, total_with_user_id=%d",
        migrated, skipped_no_user, already_ok,
    )

    # ── Step 3: Verify no nulls remain before index creation ────────────
    null_count = await db.user_watchlist.count_documents(
        {"$or": [
            {"user_id": {"$exists": False}},
            {"user_id": None},
            {"user_id": ""},
        ]}
    )
    if null_count > 0:
        logger.error(
            "%d documents still lack user_id — NOT creating unique index. "
            "Fix these manually and re-run.", null_count,
        )
        client.close()
        return

    # ── Step 4: Create unique compound index ────────────────────────────
    existing_indexes = await db.user_watchlist.index_information()
    idx_name = "user_id_1_ticker_1"
    if idx_name in existing_indexes:
        logger.info("Index %s already exists — skipping creation", idx_name)
    else:
        await db.user_watchlist.create_index(
            [("user_id", 1), ("ticker", 1)],
            unique=True,
            name=idx_name,
        )
        logger.info("Created unique index %s", idx_name)

    # ── Step 5: Drop legacy user_email field (optional cleanup) ─────────
    cleaned = await db.user_watchlist.update_many(
        {"user_email": {"$exists": True}},
        {"$unset": {"user_email": ""}},
    )
    if cleaned.modified_count:
        logger.info("Removed legacy user_email field from %d documents", cleaned.modified_count)

    logger.info("✅ Migration complete")
    client.close()


if __name__ == "__main__":
    asyncio.run(main())
