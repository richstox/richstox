# ==============================================================================
# 🛡️ SCHEDULER ORCHESTRATOR - EXTERNAL API CALLS ALLOWED
# ==============================================================================
# This file is in ALLOWLIST: /app/scripts/audit_external_calls.py
# Orchestrates ALL scheduler jobs that call eodhd.com/api/*
# Runtime API endpoints MUST NOT trigger jobs from this file directly
# ==============================================================================
#
# ================================================================================
# RICHSTOX SCHEDULER — BINDING SPECIFICATION
# ================================================================================
# BINDING: Do not change schedule, endpoints, or conditions without Richard's
# explicit approval (kurtarichard@gmail.com).
#
# Full specification: /app/docs/SCHEDULER_JOBS.md
# Audit script: /app/scripts/audit_scheduler.py
# ================================================================================
#
# UNIVERSE SYSTEM — PERMANENT & BINDING FOR ALL FUTURE INSTANCES
# ================================================================================
# This scheduler/data pipeline is IMMUTABLE.
#
# This is the ONLY way the app defines its ticker universe. No exceptions.
# No agent, fork, or future instance may deviate from this.
#
# ALLOWED EODHD API ENDPOINTS:
# 1. SEED:         https://eodhd.com/api/exchange-symbol-list/{NYSE|NASDAQ}
# 2. PRICES:       https://eodhd.com/api/eod-bulk-last-day/US
# 3. FUNDAMENTALS: https://eodhd.com/api/fundamentals/{TICKER}.US
# 4. NEWS:         https://eodhd.com/api/news?s={TICKER}.US
# 5. EOD:          https://eodhd.com/api/eod/{TICKER}.US (backfill)
#
# VISIBLE UNIVERSE RULE:
# is_visible = is_seeded && has_price_data && has_classification
# Where:
#   - is_seeded: NYSE/NASDAQ + Type == "Common Stock"
#   - has_price_data: appears in daily bulk prices
#   - has_classification: sector AND industry are non-empty
#
# APP RUNTIME NEVER CALLS EODHD. All data comes from MongoDB only.
#
# Any deviation requires explicit written approval from Richard (kurtarichard@gmail.com).
# ================================================================================

"""
RICHSTOX Scheduler Daemon
=========================
DO NOT CHANGE WITHOUT RICHARD APPROVAL (kurtarichard@gmail.com)
This scheduler/data pipeline is IMMUTABLE.

Standalone scheduler process that runs scheduled jobs.

UNIVERSE SYSTEM - Single Source of Truth
=========================================
Step 1: Weekly Universe Seed (Sunday 04:00 Prague)
  - Fetches NYSE + NASDAQ exchange-symbol-list
  - ONLY Common Stock (no ETF/funds/warrants/preferred)
  - Sets is_whitelisted=true, is_active=false

Step 2: Daily Bulk Prices (04:00 Prague, Mon-Sat)
  - Fetches eod-bulk-last-day/US
  - Sets has_price_data=true, is_active=true for tickers with prices
  - DETECTS splits/dividends -> triggers backfill + fundamentals for those tickers

Step 3: Daily Fundamentals (04:30 Prague, Mon-Sat)
  - ONLY for has_price_data=true tickers + corporate action tickers
  - Stores sector/industry (does NOT block visibility)

Schedule (Europe/Prague timezone):
- SUNDAY 04:00: Weekly universe seed (NYSE + NASDAQ Common Stock)
- MON-SAT 04:00: Daily price sync (bulk API) + split/dividend detection
- MON-SAT 04:15: SP500TR benchmark update
- MON-SAT 04:30: Fundamentals sync (changes + corporate actions)
- MON-SAT 04:45: Price backfill (gaps + corporate actions)
- MON-SAT 05:00: PAIN cache refresh (max drawdown from full series)
- MON-SAT 05:00: Parallel backfill ALL (1,000 tickers/day)
- MON-SAT 05:30: Key metrics + peer medians
- MON-SAT 13:00: News & sentiment refresh (followed/watchlisted tickers)

Run with: python scheduler.py
"""

import os
import sys
import asyncio
import logging
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

# Add backend directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv
load_dotenv()

from motor.motor_asyncio import AsyncIOMotorClient

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("richstox.scheduler_daemon")

# Configuration
TIMEZONE = ZoneInfo("Europe/Prague")

# SUNDAY ONLY - Universe seed
UNIVERSE_SEED_DAY = 6  # Sunday
UNIVERSE_SEED_HOUR = 23
UNIVERSE_SEED_MINUTE = 0

# MON-SAT - Daily jobs
DAILY_SCHEDULE_DAYS = [0, 1, 2, 3, 4, 5]  # Mon=0, Sat=5 (excludes Sunday=6)

PRICE_SYNC_HOUR = 4
PRICE_SYNC_MINUTE = 0

FUNDAMENTALS_SYNC_HOUR = 4
FUNDAMENTALS_SYNC_MINUTE = 30

BACKFILL_HOUR = 4
BACKFILL_MINUTE = 45

# NEW: Parallel backfill all prices at 05:00
BACKFILL_ALL_HOUR = 5
BACKFILL_ALL_MINUTE = 0

# NEWS: Daily news refresh at 13:00
NEWS_REFRESH_HOUR = 13
NEWS_REFRESH_MINUTE = 0

# SP500TR.INDX: Daily benchmark update at 04:15
SP500TR_UPDATE_HOUR = 4
SP500TR_UPDATE_MINUTE = 15

# KEY METRICS: Job A - compute per-ticker metrics at 05:00
KEY_METRICS_HOUR = 5
KEY_METRICS_MINUTE = 0

# PEER MEDIANS: Job B - compute peer medians at 05:30 (after Job A)
PEER_MEDIANS_HOUR = 5
PEER_MEDIANS_MINUTE = 30

# PAIN CACHE: Refresh max drawdown cache at 05:00
PAIN_CACHE_HOUR = 5
PAIN_CACHE_MINUTE = 0

# ADMIN REPORT: Daily admin report at 06:00 (after all morning jobs)
ADMIN_REPORT_HOUR = 6
ADMIN_REPORT_MINUTE = 0

# MongoDB connection
mongo_url = os.environ.get('MONGO_URL', 'mongodb://localhost:27017')
db_name = os.environ.get('DB_NAME', 'richstox')


def get_prague_time() -> datetime:
    """Get current time in Europe/Prague timezone."""
    return datetime.now(TIMEZONE)


def is_sunday() -> bool:
    """Check if today is Sunday (universe seed day)."""
    return get_prague_time().weekday() == UNIVERSE_SEED_DAY


def is_daily_job_day() -> bool:
    """Check if today is a daily job day (Mon-Sat)."""
    return get_prague_time().weekday() in DAILY_SCHEDULE_DAYS


def time_until_next_run(target_hour: int, target_minute: int) -> float:
    """
    Calculate seconds until next scheduled run.
    
    Returns:
        Seconds until next run (0 if should run now)
    """
    now = get_prague_time()
    
    # Target time today
    target_today = now.replace(hour=target_hour, minute=target_minute, second=0, microsecond=0)
    
    if now < target_today:
        # Haven't passed target time today
        return (target_today - now).total_seconds()
    
    # Already passed today, calculate for tomorrow
    from datetime import timedelta
    target_tomorrow = target_today + timedelta(days=1)
    return (target_tomorrow - now).total_seconds()


async def log_job_execution(db, job_name: str, status: str, start_time: datetime, 
                            end_time: datetime = None, records_processed: int = 0,
                            error_message: str = None, details: dict = None):
    """
    Log job execution to system_job_logs collection (NEW observability layer).
    
    This is the primary job execution log for Admin Panel visibility.
    """
    if end_time is None:
        end_time = datetime.now(timezone.utc)
    
    duration = (end_time - start_time).total_seconds()
    
    await db.system_job_logs.insert_one({
        "job_name": job_name,
        "status": status,
        "start_time": start_time,
        "end_time": end_time,
        "duration_seconds": round(duration, 2),
        "records_processed": records_processed,
        "error_message": error_message,
        "details": details or {}
    })
    
    logger.info(f"[system_job_logs] {job_name}: {status} ({round(duration, 1)}s, {records_processed} records)")


async def run_job_with_retry(job_name: str, job_func, db, max_retries: int = 3):
    """
    Run a job with retry logic and log to BOTH ops_job_runs (legacy) and system_job_logs (new).
    
    Args:
        job_name: Name for logging
        job_func: Async function to call
        db: MongoDB database
        max_retries: Maximum retry attempts
    """
    started_at = datetime.now(timezone.utc)
    
    for attempt in range(max_retries):
        try:
            logger.info(f"Running {job_name} (attempt {attempt + 1}/{max_retries})")
            result = await job_func(db)
            logger.info(f"{job_name} completed: {result.get('status', 'unknown')}")
            
            # Extract records count from result
            records_processed = 0
            if isinstance(result, dict):
                records_processed = result.get("tickers_processed", 
                                   result.get("records_processed",
                                   result.get("processed",
                                   result.get("records_updated",
                                   result.get("tickers_updated",
                                   result.get("count", 0))))))
            
            completed_at = datetime.now(timezone.utc)
            
            # NEW: Log to system_job_logs (primary observability)
            await log_job_execution(
                db, job_name, "success", started_at, completed_at,
                records_processed=records_processed,
                details=result if isinstance(result, dict) else {}
            )
            
            # LEGACY: Log to ops_job_runs (backward compatibility)
            await db.ops_job_runs.insert_one({
                "job_name": job_name,
                "status": result.get("status", "completed"),
                "started_at": started_at,
                "completed_at": completed_at,
                "duration_seconds": (completed_at - started_at).total_seconds(),
                "result": result if isinstance(result, dict) else {"value": str(result)},
                "details": {
                    "api_calls": result.get("api_calls"),
                    "records_updated": records_processed,
                    "result_summary": str(result)[:500] if result else None
                }
            })
            
            return result
        except Exception as e:
            logger.error(f"{job_name} failed (attempt {attempt + 1}): {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(30)  # Wait before retry
    
    # Log failed job to BOTH collections
    completed_at = datetime.now(timezone.utc)
    error_msg = f"Max retries ({max_retries}) exceeded"
    
    # NEW: Log failure to system_job_logs
    await log_job_execution(
        db, job_name, "error", started_at, completed_at,
        error_message=error_msg
    )
    
    # LEGACY: Log to ops_job_runs
    await db.ops_job_runs.insert_one({
        "job_name": job_name,
        "status": "failed",
        "started_at": started_at,
        "completed_at": completed_at,
        "duration_seconds": (completed_at - started_at).total_seconds(),
        "details": {"error": error_msg}
    })
    
    return {"status": "failed", "error": error_msg}


async def scheduler_loop():
    """
    Main scheduler loop with PERSISTENT STATE and CATCH-UP logic.
    
    Runs indefinitely, checking every minute if a job should run.
    
    GUARDRAIL: Uses ops_config collection for state persistence (survives restarts).
    GUARDRAIL: Catch-up logic runs missed jobs if scheduler restarts after scheduled time.
    GUARDRAIL: Heartbeat logged every 15 minutes for observability.
    """
    # Connect to MongoDB
    client = AsyncIOMotorClient(mongo_url)
    db = client[db_name]
    
    logger.info(f"Scheduler started - Timezone: Europe/Prague")
    logger.info(f"Schedule: Mon-Sat")
    logger.info(f"  {PRICE_SYNC_HOUR:02d}:{PRICE_SYNC_MINUTE:02d} - Daily price sync (bulk)")
    logger.info(f"  {SP500TR_UPDATE_HOUR:02d}:{SP500TR_UPDATE_MINUTE:02d} - SP500TR benchmark update")
    logger.info(f"  {FUNDAMENTALS_SYNC_HOUR:02d}:{FUNDAMENTALS_SYNC_MINUTE:02d} - Fundamentals sync")
    logger.info(f"  {BACKFILL_HOUR:02d}:{BACKFILL_MINUTE:02d} - Price backfill (gaps)")
    logger.info(f"  {KEY_METRICS_HOUR:02d}:{KEY_METRICS_MINUTE:02d} - Key Metrics + Peer Medians")
    logger.info(f"  {PAIN_CACHE_HOUR:02d}:{PAIN_CACHE_MINUTE:02d} - PAIN cache refresh")
    logger.info(f"  {ADMIN_REPORT_HOUR:02d}:{ADMIN_REPORT_MINUTE:02d} - Admin Report")
    logger.info(f"  {NEWS_REFRESH_HOUR:02d}:{NEWS_REFRESH_MINUTE:02d} - Daily news refresh")
    
    # ==========================================================================
    # PERSISTENT STATE FUNCTIONS (reuse ops_config collection)
    # ==========================================================================
    
    async def get_last_run_state() -> dict:
        """Get persistent last_run state from ops_config collection."""
        doc = await db.ops_config.find_one({"key": "scheduler_last_run"})
        if doc and doc.get("value"):
            return doc["value"]
        return {}
    
    async def set_last_run_state(state: dict):
        """Persist last_run state to ops_config collection."""
        await db.ops_config.update_one(
            {"key": "scheduler_last_run"},
            {"$set": {
                "key": "scheduler_last_run",
                "value": state,
                "updated_at": datetime.now(timezone.utc)
            }},
            upsert=True
        )
    
    async def log_heartbeat(last_run: dict):
        """Log heartbeat to system_job_logs for Admin Panel visibility."""
        await db.system_job_logs.insert_one({
            "job_name": "scheduler_heartbeat",
            "status": "success",
            "start_time": datetime.now(timezone.utc),
            "end_time": datetime.now(timezone.utc),
            "duration_seconds": 0,
            "records_processed": 0,
            "details": {"last_run_state": last_run, "prague_time": get_prague_time().isoformat()}
        })
    
    def should_run(job_name: str, scheduled_hour: int, scheduled_minute: int, last_run: dict, today_str: str, current_hour: int, current_minute: int) -> bool:
        """
        Check if job should run (with catch-up logic).
        
        Returns True if:
        - Job hasn't run today AND
        - Current time is >= scheduled time
        """
        if last_run.get(job_name) == today_str:
            return False
        if current_hour > scheduled_hour:
            return True
        if current_hour == scheduled_hour and current_minute >= scheduled_minute:
            return True
        return False

    def should_run_after_dependency(job_name: str, dependency_job: str, last_run: dict, today_str: str) -> bool:
        """
        Check if dependent job should run immediately after dependency completes.
        """
        if last_run.get(job_name) == today_str:
            return False
        return last_run.get(dependency_job) == today_str
    
    # Load persistent state from DB (survives restarts)
    last_run = await get_last_run_state()
    logger.info(f"Loaded persistent last_run state: {last_run}")
    
    # Track last heartbeat minute
    last_heartbeat_minute = -1
    
    # Import job functions
    from scheduler_service import (
        run_daily_price_sync,
        run_fundamentals_changes_sync,
        run_price_backfill_gaps,
        get_scheduler_enabled,
    )
    from parallel_batch_service import run_scheduled_backfill_all_prices
    
    try:
        while True:
            now = get_prague_time()
            today_str = now.strftime("%Y-%m-%d")
            current_hour = now.hour
            current_minute = now.minute
            
            # =================================================================
            # HEARTBEAT (every 15 minutes)
            # =================================================================
            if current_minute % 15 == 0 and current_minute != last_heartbeat_minute:
                last_heartbeat_minute = current_minute
                logger.info(f"[HEARTBEAT] Scheduler alive at {now.strftime('%Y-%m-%d %H:%M')} Prague")
                await log_heartbeat(last_run)
            
            # =================================================================
            # KILL SWITCH CHECK
            # =================================================================
            scheduler_enabled = await get_scheduler_enabled(db)
            
            if not scheduler_enabled:
                logger.debug("Scheduler disabled (kill switch engaged)")
                await asyncio.sleep(60)
                continue
            
            # =================================================================
            # SUNDAY: News refresh only, then skip all other jobs
            # Universe Seed runs Mon-Sat only (markets closed on Sunday)
            # =================================================================
            if is_sunday():
                # News refresh at 13:00 Sunday (catch-up enabled)
                if should_run("news_refresh", NEWS_REFRESH_HOUR, NEWS_REFRESH_MINUTE, last_run, today_str, current_hour, current_minute):
                    logger.info(f"Triggering news_refresh (hour={current_hour}, scheduled={NEWS_REFRESH_HOUR}:{NEWS_REFRESH_MINUTE:02d})")
                    from services.news_service import news_daily_refresh
                    await run_job_with_retry("news_refresh", news_daily_refresh, db)
                    last_run["news_refresh"] = today_str
                    await set_last_run_state(last_run)
                
                await asyncio.sleep(60)
                continue
            
            # =================================================================
            # MON-SAT JOBS (with catch-up logic)
            # =================================================================
            if not is_daily_job_day():
                await asyncio.sleep(60)
                continue
            
            # STEP 1: Universe Seed at 23:00 Mon-Sat
            if should_run("universe_seed", UNIVERSE_SEED_HOUR, UNIVERSE_SEED_MINUTE, last_run, today_str, current_hour, current_minute):
                logger.info(f"Triggering universe_seed STEP 1 (hour={current_hour}, scheduled={UNIVERSE_SEED_HOUR}:{UNIVERSE_SEED_MINUTE:02d})")
                from whitelist_service import sync_ticker_whitelist
                await run_job_with_retry("universe_seed", sync_ticker_whitelist, db)
                last_run["universe_seed"] = today_str
                await set_last_run_state(last_run)

            # STEP 2: Price sync immediately after Step 1 completes
            if should_run_after_dependency("price_sync", "universe_seed", last_run, today_str):
                logger.info("Triggering price_sync (dependency: universe_seed completed)")
                await run_job_with_retry("price_sync", run_daily_price_sync, db)
                last_run["price_sync"] = today_str
                await set_last_run_state(last_run)
            
            # SP500TR benchmark at 04:15 (catch-up enabled)
            if should_run("sp500tr_update", SP500TR_UPDATE_HOUR, SP500TR_UPDATE_MINUTE, last_run, today_str, current_hour, current_minute):
                logger.info(f"Triggering sp500tr_update (hour={current_hour}, scheduled={SP500TR_UPDATE_HOUR}:{SP500TR_UPDATE_MINUTE:02d})")
                from benchmark_service import update_sp500tr_benchmark
                await run_job_with_retry("sp500tr_update", update_sp500tr_benchmark, db)
                last_run["sp500tr_update"] = today_str
                await set_last_run_state(last_run)
            
            # STEP 3: Fundamentals sync immediately after Step 2 completes
            if should_run_after_dependency("fundamentals_sync", "price_sync", last_run, today_str):
                logger.info("Triggering fundamentals_sync (dependency: price_sync completed)")
                await run_job_with_retry("fundamentals_sync", run_fundamentals_changes_sync, db)
                last_run["fundamentals_sync"] = today_str
                await set_last_run_state(last_run)
            
            # Backfill gaps at 04:45 (catch-up enabled)
            if should_run("backfill", BACKFILL_HOUR, BACKFILL_MINUTE, last_run, today_str, current_hour, current_minute):
                logger.info(f"Triggering backfill_gaps (hour={current_hour}, scheduled={BACKFILL_HOUR}:{BACKFILL_MINUTE:02d})")
                await run_job_with_retry("backfill_gaps", run_price_backfill_gaps, db)
                last_run["backfill"] = today_str
                await set_last_run_state(last_run)
            
            # BACKFILL_ALL: MANUAL ONLY by default
            backfill_all_enabled = await db.ops_config.find_one({"key": "job_backfill_all_enabled"})
            backfill_all_auto = backfill_all_enabled.get("value", False) if backfill_all_enabled else False
            
            if backfill_all_auto and should_run("backfill_all", BACKFILL_ALL_HOUR, BACKFILL_ALL_MINUTE, last_run, today_str, current_hour, current_minute):
                logger.info(f"Triggering backfill_all (hour={current_hour}, scheduled={BACKFILL_ALL_HOUR}:{BACKFILL_ALL_MINUTE:02d})")
                await run_job_with_retry("backfill_all", run_scheduled_backfill_all_prices, db)
                last_run["backfill_all"] = today_str
                await set_last_run_state(last_run)
            
            # KEY METRICS at 05:00 (catch-up enabled)
            if should_run("key_metrics", KEY_METRICS_HOUR, KEY_METRICS_MINUTE, last_run, today_str, current_hour, current_minute):
                logger.info(f"Triggering key_metrics (hour={current_hour}, scheduled={KEY_METRICS_HOUR}:{KEY_METRICS_MINUTE:02d})")
                from key_metrics_service import compute_daily_key_metrics
                await run_job_with_retry("key_metrics", compute_daily_key_metrics, db)
                last_run["key_metrics"] = today_str
                await set_last_run_state(last_run)
            
            # PEER MEDIANS at 05:30 (catch-up enabled)
            if should_run("peer_medians", PEER_MEDIANS_HOUR, PEER_MEDIANS_MINUTE, last_run, today_str, current_hour, current_minute):
                logger.info(f"Triggering peer_medians (hour={current_hour}, scheduled={PEER_MEDIANS_HOUR}:{PEER_MEDIANS_MINUTE:02d})")
                from key_metrics_service import compute_peer_benchmarks_v3
                await run_job_with_retry("peer_medians", compute_peer_benchmarks_v3, db)
                last_run["peer_medians"] = today_str
                await set_last_run_state(last_run)
            
            # PAIN CACHE at 05:00 (catch-up enabled)
            if should_run("pain_cache", PAIN_CACHE_HOUR, PAIN_CACHE_MINUTE, last_run, today_str, current_hour, current_minute):
                logger.info(f"Triggering pain_cache (hour={current_hour}, scheduled={PAIN_CACHE_HOUR}:{PAIN_CACHE_MINUTE:02d})")
                from server import run_pain_cache_refresh
                await run_job_with_retry("pain_cache", run_pain_cache_refresh, db)
                last_run["pain_cache"] = today_str
                await set_last_run_state(last_run)
            
            # ADMIN REPORT at 06:00 (catch-up enabled)
            if should_run("admin_report", ADMIN_REPORT_HOUR, ADMIN_REPORT_MINUTE, last_run, today_str, current_hour, current_minute):
                logger.info(f"Triggering admin_report (hour={current_hour}, scheduled={ADMIN_REPORT_HOUR}:{ADMIN_REPORT_MINUTE:02d})")
                from services.admin_report_service import run_admin_report_job
                await run_job_with_retry("admin_report", run_admin_report_job, db)
                last_run["admin_report"] = today_str
                await set_last_run_state(last_run)
            
            # NEWS at 13:00 (catch-up enabled)
            if should_run("news_refresh", NEWS_REFRESH_HOUR, NEWS_REFRESH_MINUTE, last_run, today_str, current_hour, current_minute):
                logger.info(f"Triggering news_refresh (hour={current_hour}, scheduled={NEWS_REFRESH_HOUR}:{NEWS_REFRESH_MINUTE:02d})")
                from services.news_service import news_daily_refresh
                await run_job_with_retry("news_refresh", news_daily_refresh, db)
                last_run["news_refresh"] = today_str
                await set_last_run_state(last_run)
            
            # Sleep until next minute
            await asyncio.sleep(60)
            
    except KeyboardInterrupt:
        logger.info("Scheduler stopped by user")
    except Exception as e:
        logger.error(f"Scheduler error: {e}")
        raise
    finally:
        client.close()


def main():
    """Entry point for scheduler daemon."""
    logger.info("Starting RICHSTOX Scheduler Daemon")
    asyncio.run(scheduler_loop())


if __name__ == "__main__":
    main()
