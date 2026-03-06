#!/usr/bin/env python3
"""
Idempotent migration: user_watchlist.user_email → user_id
==========================================================

Converts legacy `user_email` field to `user_id` by looking up each email
in the `users` collection.  Handles duplicates, writes rollback log,
and supports --dry-run.

Usage:
    python scripts/migrate_watchlist_user_id.py --dry-run   # report only
    python scripts/migrate_watchlist_user_id.py              # execute

Environment:
    MONGO_URL  (default: mongodb://localhost:27017)
    DB_NAME    (required)
"""

import argparse
import asyncio
import json
import os
import sys
import logging
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-5s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("migrate_watchlist")

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

BATCH_SIZE = 500


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _rollback_path() -> Path:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return Path(f"rollback_watchlist_migration_{ts}.jsonl")


def _serialize(doc: dict) -> dict:
    """Make a doc JSON-serialisable (ObjectId → str, datetime → iso)."""
    out = {}
    for k, v in doc.items():
        if hasattr(v, "isoformat"):
            out[k] = v.isoformat()
        else:
            out[k] = str(v) if k == "_id" else v
    return out


# ──────────────────────────────────────────────────────────────────────────────
# Core logic
# ──────────────────────────────────────────────────────────────────────────────

async def _build_email_lookup(db) -> dict[str, str]:
    users = await db.users.find({}, {"email": 1, "user_id": 1, "_id": 0}).to_list(None)
    return {u["email"]: u["user_id"] for u in users if u.get("email") and u.get("user_id")}


async def _get_docs_needing_migration(db) -> list[dict]:
    return await db.user_watchlist.find(
        {"$or": [
            {"user_id": {"$exists": False}},
            {"user_id": None},
            {"user_id": ""},
        ]},
    ).to_list(None)


async def _find_duplicates(db) -> dict[tuple[str, str], list[dict]]:
    """
    Return {(user_id, ticker): [docs]} where len(docs) > 1.
    Covers both already-migrated docs and docs that *will* get a user_id.
    """
    all_docs = await db.user_watchlist.find(
        {"user_id": {"$exists": True, "$nin": [None, ""]}},
    ).to_list(None)

    groups: dict[tuple, list] = defaultdict(list)
    for d in all_docs:
        key = (d["user_id"], (d.get("ticker") or "").upper())
        groups[key].append(d)

    return {k: v for k, v in groups.items() if len(v) > 1}


def _pick_keeper(docs: list[dict]) -> tuple[dict, list[dict]]:
    """Keep the most recently created/updated doc; rest are losers."""
    def sort_key(d):
        for field in ("updated_at", "created_at", "followed_at"):
            val = d.get(field)
            if val:
                if isinstance(val, str):
                    return val
                if hasattr(val, "isoformat"):
                    return val.isoformat()
        return ""

    ordered = sorted(docs, key=sort_key, reverse=True)
    return ordered[0], ordered[1:]


# ──────────────────────────────────────────────────────────────────────────────
# Dry-run report
# ──────────────────────────────────────────────────────────────────────────────

async def dry_run_report(db, email_map: dict[str, str]):
    docs = await _get_docs_needing_migration(db)

    matched = 0
    orphans = 0
    orphan_emails: list[str] = []

    for d in docs:
        email = d.get("user_email")
        if email and email in email_map:
            matched += 1
        else:
            orphans += 1
            if email:
                orphan_emails.append(email)

    # Simulate post-migration duplicates
    all_existing = await db.user_watchlist.find(
        {"user_id": {"$exists": True, "$nin": [None, ""]}},
    ).to_list(None)

    simulated: dict[tuple, int] = defaultdict(int)
    for d in all_existing:
        simulated[(d["user_id"], (d.get("ticker") or "").upper())] += 1
    for d in docs:
        email = d.get("user_email")
        uid = email_map.get(email) if email else None
        if uid:
            simulated[(uid, (d.get("ticker") or "").upper())] += 1

    dup_count = sum(v - 1 for v in simulated.values() if v > 1)
    dup_keys = {k: v for k, v in simulated.items() if v > 1}

    total = await db.user_watchlist.count_documents({})

    print()
    print("=" * 60)
    print("  WATCHLIST MIGRATION — DRY-RUN REPORT")
    print("=" * 60)
    print(f"  Total user_watchlist documents:        {total}")
    print(f"  Documents needing migration:           {len(docs)}")
    print(f"    ├─ Matched (email → user_id found):  {matched}")
    print(f"    └─ Orphans (no matching user):        {orphans}")
    if orphan_emails:
        for e in sorted(set(orphan_emails)):
            print(f"         • {e}")
    print(f"  Duplicate (user_id, ticker) pairs:     {len(dup_keys)}")
    print(f"  Duplicate records to delete:           {dup_count}")
    if dup_keys:
        for (uid, ticker), cnt in sorted(dup_keys.items()):
            print(f"         • ({uid}, {ticker}) × {cnt}")
    print()
    print("  ⚠️  NO CHANGES WERE MADE (--dry-run)")
    print("=" * 60)
    print()


# ──────────────────────────────────────────────────────────────────────────────
# Execute migration
# ──────────────────────────────────────────────────────────────────────────────

async def execute_migration(db, email_map: dict[str, str]):
    from pymongo import UpdateOne, DeleteOne

    rollback_file = _rollback_path()
    logger.info("Rollback log → %s", rollback_file)

    docs = await _get_docs_needing_migration(db)
    logger.info("Documents needing migration: %d", len(docs))

    if not docs:
        logger.info("Nothing to migrate.")
    else:
        # ── Phase 1: Batch update user_email → user_id ──────────────────
        ops: list = []
        skipped = 0

        with open(rollback_file, "a") as f:
            for d in docs:
                email = d.get("user_email")
                uid = email_map.get(email) if email else None
                if not uid:
                    skipped += 1
                    logger.warning("No user for email=%s (_id=%s) — skipped", email, d["_id"])
                    continue

                f.write(json.dumps({"action": "set_user_id", "before": _serialize(d)}) + "\n")
                ops.append(UpdateOne({"_id": d["_id"]}, {"$set": {"user_id": uid}}))

                if len(ops) >= BATCH_SIZE:
                    result = await db.user_watchlist.bulk_write(ops, ordered=False)
                    logger.info("Batch update: modified=%d", result.modified_count)
                    ops.clear()

            if ops:
                result = await db.user_watchlist.bulk_write(ops, ordered=False)
                logger.info("Batch update (final): modified=%d", result.modified_count)

        logger.info("Phase 1 done: updated=%d, skipped=%d", len(docs) - skipped, skipped)

    # ── Phase 2: Deduplicate ────────────────────────────────────────────
    dups = await _find_duplicates(db)
    if dups:
        logger.info("Resolving %d duplicate (user_id, ticker) groups", len(dups))
        delete_ops: list = []

        with open(rollback_file, "a") as f:
            for (_uid, _ticker), group in dups.items():
                keeper, losers = _pick_keeper(group)
                for loser in losers:
                    f.write(json.dumps({"action": "dedup_delete", "before": _serialize(loser)}) + "\n")
                    delete_ops.append(DeleteOne({"_id": loser["_id"]}))

                    if len(delete_ops) >= BATCH_SIZE:
                        result = await db.user_watchlist.bulk_write(delete_ops, ordered=False)
                        logger.info("Batch dedup delete: deleted=%d", result.deleted_count)
                        delete_ops.clear()

            if delete_ops:
                result = await db.user_watchlist.bulk_write(delete_ops, ordered=False)
                logger.info("Batch dedup delete (final): deleted=%d", result.deleted_count)
    else:
        logger.info("No duplicates found.")

    # ── Phase 3: Verify no nulls before index creation ──────────────────
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
            "Fix orphans manually and re-run.",
            null_count,
        )
        return

    # ── Phase 4: Create unique compound index ───────────────────────────
    existing_indexes = await db.user_watchlist.index_information()
    idx_name = "user_id_1_ticker_1"
    if idx_name in existing_indexes:
        logger.info("Index %s already exists — skipping.", idx_name)
    else:
        await db.user_watchlist.create_index(
            [("user_id", 1), ("ticker", 1)],
            unique=True,
            name=idx_name,
        )
        logger.info("Created unique index %s", idx_name)

    # ── Phase 5: Drop legacy user_email field ───────────────────────────
    cleaned = await db.user_watchlist.update_many(
        {"user_email": {"$exists": True}},
        {"$unset": {"user_email": ""}},
    )
    if cleaned.modified_count:
        logger.info("Removed legacy user_email from %d documents.", cleaned.modified_count)

    logger.info("✅  Migration complete.  Rollback log: %s", rollback_file)


# ──────────────────────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────────────────────

async def main():
    parser = argparse.ArgumentParser(description="Migrate user_watchlist: user_email → user_id")
    parser.add_argument("--dry-run", action="store_true", help="Report only, no writes")
    args = parser.parse_args()

    from motor.motor_asyncio import AsyncIOMotorClient

    mongo_url = os.environ.get("MONGO_URL", "mongodb://localhost:27017")
    db_name = os.environ.get("DB_NAME")
    if not db_name:
        logger.error("DB_NAME environment variable is required")
        sys.exit(1)

    client = AsyncIOMotorClient(mongo_url)
    db = client[db_name]

    email_map = await _build_email_lookup(db)
    logger.info("Loaded %d users for email → user_id lookup", len(email_map))

    if args.dry_run:
        await dry_run_report(db, email_map)
    else:
        await execute_migration(db, email_map)

    client.close()


if __name__ == "__main__":
    asyncio.run(main())
