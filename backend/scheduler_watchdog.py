"""Scheduler watchdog — fail-fast if the in-process scheduler stops heartbeating.

Runs as an asyncio background task next to the scheduler daemon.  Queries
MongoDB for the latest ``scheduler_heartbeat`` document and terminates the
whole process (non-zero exit) when heartbeats are stale for too long, letting
Railway restart the service automatically.

Default OFF — enable with ``ENABLE_SCHEDULER_WATCHDOG=true``.
"""

import asyncio
import logging
import os
import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from motor.motor_asyncio import AsyncIOMotorDatabase

logger = logging.getLogger("richstox.scheduler_watchdog")

TIMEZONE = ZoneInfo("Europe/Prague")


def _env_int(name: str, default: int) -> int:
    return int(os.environ.get(name, str(default)))


async def watchdog_loop(db: AsyncIOMotorDatabase) -> None:  # noqa: C901
    """Monitor scheduler heartbeats and kill the process if they go stale.

    Parameters
    ----------
    db : motor AsyncIOMotorDatabase
        The application MongoDB database handle (already connected).
    """
    max_staleness_minutes = _env_int("SCHEDULER_WATCHDOG_MAX_STALENESS_MINUTES", 30)
    check_interval_seconds = _env_int("SCHEDULER_WATCHDOG_CHECK_INTERVAL_SECONDS", 60)
    startup_grace_minutes = _env_int("SCHEDULER_WATCHDOG_STARTUP_GRACE_MINUTES", 10)
    consecutive_failures_to_exit = _env_int("SCHEDULER_WATCHDOG_CONSECUTIVE_FAILURES_TO_EXIT", 2)

    process_start_time = time.monotonic()
    consecutive_unhealthy = 0

    logger.info(
        "Scheduler watchdog started — max_staleness=%dm, check_interval=%ds, "
        "startup_grace=%dm, consecutive_to_exit=%d",
        max_staleness_minutes,
        check_interval_seconds,
        startup_grace_minutes,
        consecutive_failures_to_exit,
    )

    while True:
        await asyncio.sleep(check_interval_seconds)

        now_prague = datetime.now(TIMEZONE)
        elapsed_since_start = (time.monotonic() - process_start_time) / 60.0

        # Fetch latest heartbeat
        doc = await db.ops_job_runs.find_one(
            {"job_name": "scheduler_heartbeat"},
            sort=[("started_at", -1)],
        )

        if doc is None:
            if elapsed_since_start < startup_grace_minutes:
                logger.info(
                    "Watchdog: no heartbeat yet (startup grace %.1f/%.0f min elapsed)",
                    elapsed_since_start,
                    startup_grace_minutes,
                )
                continue
            # Grace period exhausted with no heartbeat — treat as unhealthy
            logger.warning(
                "Watchdog: no heartbeat found after startup grace of %d min",
                startup_grace_minutes,
            )
            staleness_minutes = elapsed_since_start
            last_heartbeat_prague = None
        else:
            # started_at is stored as UTC by MongoDB regardless of input tz
            started_at = doc["started_at"]
            if started_at.tzinfo is None:
                started_at = started_at.replace(tzinfo=timezone.utc)
            else:
                started_at = started_at.astimezone(timezone.utc)

            last_heartbeat_prague = started_at.astimezone(TIMEZONE)
            now_utc = datetime.now(timezone.utc)
            staleness_minutes = (now_utc - started_at).total_seconds() / 60.0

        if staleness_minutes <= max_staleness_minutes:
            logger.debug(
                "Watchdog HEALTHY: staleness=%.1f min (threshold=%d min) | now_prague=%s",
                staleness_minutes,
                max_staleness_minutes,
                now_prague.isoformat(),
            )

        if staleness_minutes > max_staleness_minutes:
            consecutive_unhealthy += 1
            logger.warning(
                "Watchdog UNHEALTHY [%d/%d]: staleness=%.1f min (threshold=%d min) | "
                "now_prague=%s | last_heartbeat_prague=%s",
                consecutive_unhealthy,
                consecutive_failures_to_exit,
                staleness_minutes,
                max_staleness_minutes,
                now_prague.isoformat(),
                last_heartbeat_prague.isoformat() if last_heartbeat_prague else "N/A",
            )
        else:
            if consecutive_unhealthy > 0:
                logger.info(
                    "Watchdog RECOVERED: staleness=%.1f min (threshold=%d min), "
                    "resetting counter from %d",
                    staleness_minutes,
                    max_staleness_minutes,
                    consecutive_unhealthy,
                )
            consecutive_unhealthy = 0

        if consecutive_unhealthy >= consecutive_failures_to_exit:
            logger.critical(
                "Scheduler watchdog terminating process! "
                "now_prague=%s | last_heartbeat_prague=%s | "
                "staleness_minutes=%.1f | threshold_minutes=%d | "
                "consecutive_unhealthy=%d",
                now_prague.isoformat(),
                last_heartbeat_prague.isoformat() if last_heartbeat_prague else "N/A",
                staleness_minutes,
                max_staleness_minutes,
                consecutive_unhealthy,
            )
            # Hard kill — os._exit bypasses asyncio cleanup and ensures Railway sees non-zero exit
            os._exit(1)
