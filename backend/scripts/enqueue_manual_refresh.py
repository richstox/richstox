#!/usr/bin/env python3
"""
One-off script: enqueue manual fundamentals refresh for 28 stuck tickers.
=========================================================================

For each ticker:
  1. Upserts a fundamentals_events row:
       event_type="manual_refresh", status="pending"
       (idempotent — skips if a pending/processing event already exists)
  2. Sets tracked_tickers.needs_fundamentals_refresh=True so the skip-gate
     in run_fundamentals_changes_sync lets the ticker through.

The normal scheduled Step 3 run (run_fundamentals_changes_sync) will pick
these up on its next execution without requiring a Full Sync.

Usage:
    cd backend
    DB_NAME=richstox_dev python scripts/enqueue_manual_refresh.py --dry-run
    DB_NAME=richstox_dev python scripts/enqueue_manual_refresh.py

Environment:
    MONGO_URL  (default: mongodb://localhost:27017)
    DB_NAME    (required)
"""

import argparse
import asyncio
import os
import sys
import logging
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger("enqueue_manual_refresh")

PRAGUE_TZ = ZoneInfo("Europe/Prague")

# 28 tickers confirmed "Fundamentals not synced" from Step 3 exclusion report.
TICKERS = [
    "AME.US", "AMP.US", "AMRC.US", "AMX.US", "AON.US", "APAM.US",
    "APLE.US", "APTV.US", "AR.US", "AREN.US", "ARL.US", "AROC.US",
    "AS.US", "ASC.US", "ASIX.US", "ASX.US", "ATNM.US", "AVA.US",
    "AVD.US", "AWX.US", "AXIA-P.US", "AXS.US", "AZTR.US", "BABA.US",
    "BALL.US", "BB.US", "BBU.US", "BCC.US",
]


def _to_prague_iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(PRAGUE_TZ).isoformat()


async def run(dry_run: bool) -> None:
    mongo_url = os.environ.get("MONGO_URL", "mongodb://localhost:27017")
    db_name   = os.environ.get("DB_NAME", "")
    if not db_name:
        logger.error("DB_NAME environment variable is required")
        sys.exit(1)

    from motor.motor_asyncio import AsyncIOMotorClient
    client = AsyncIOMotorClient(mongo_url)
    db = client[db_name]

    now = datetime.now(timezone.utc)
    event_type = "manual_refresh"
    source_job = "enqueue_manual_refresh_script"

    logger.info(f"DB: {db_name}  dry_run={dry_run}  tickers={len(TICKERS)}")

    events_inserted = 0
    events_already_pending = 0
    flags_set = 0

    for ticker in TICKERS:
        if dry_run:
            logger.info(f"[DRY-RUN] Would enqueue {ticker} event_type={event_type} + set needs_fundamentals_refresh=True")
            continue

        # 1. Idempotent event upsert — only insert if no pending/processing exists.
        result = await db.fundamentals_events.update_one(
            {
                "ticker": ticker,
                "event_type": event_type,
                "status": {"$in": ["pending", "processing"]},
            },
            {
                "$setOnInsert": {
                    "ticker":        ticker,
                    "event_type":    event_type,
                    "status":        "pending",
                    "source_job":    source_job,
                    "detector_step": "manual",
                    "created_at":    now,
                },
                "$set": {
                    "updated_at": now,
                },
            },
            upsert=True,
        )
        if result.upserted_id is not None:
            events_inserted += 1
            logger.info(f"  INSERTED event for {ticker}")
        else:
            events_already_pending += 1
            logger.info(f"  SKIPPED  event for {ticker} (already pending/processing)")

        # 2. Set needs_fundamentals_refresh=True so skip-gate does not block this ticker.
        flag_result = await db.tracked_tickers.update_one(
            {"ticker": ticker},
            {"$set": {
                "needs_fundamentals_refresh": True,
                "updated_at": now,
            }},
        )
        if flag_result.modified_count:
            flags_set += 1

    if not dry_run:
        logger.info(
            f"\nDone — events_inserted={events_inserted}, "
            f"events_already_pending={events_already_pending}, "
            f"needs_fundamentals_refresh flags set={flags_set}"
        )
        logger.info(
            "Next Step 3 run will process these tickers via run_fundamentals_changes_sync."
        )
    else:
        logger.info(f"\n[DRY-RUN] Would process {len(TICKERS)} tickers — no changes made.")

    client.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Enqueue manual fundamentals refresh for stuck tickers.")
    parser.add_argument("--dry-run", action="store_true", help="Report only — make no DB changes.")
    args = parser.parse_args()
    asyncio.run(run(dry_run=args.dry_run))


if __name__ == "__main__":
    main()
