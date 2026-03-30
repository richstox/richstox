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
Step 1: Universe Seed (Mon-Sat 03:00 Prague)
  - Fetches NYSE + NASDAQ exchange-symbol-list
  - ONLY Common Stock (no ETF/funds/warrants/preferred)
  - Sets is_whitelisted=true, is_active=false

Step 2: Price Sync (auto after Step 1 completion)
  - Fetches eod-bulk-last-day/US
  - Sets has_price_data=true, is_active=true for tickers with prices
  - DETECTS splits/dividends -> triggers backfill + fundamentals for those tickers

Step 3: Fundamentals Sync (auto after Step 2 completion)
  - ONLY for has_price_data=true tickers + corporate action tickers
  - Stores sector/industry (does NOT block visibility)

Schedule (Europe/Prague timezone):
- MON-SAT 02:00: Market calendar refresh (EODHD exchange-details)
- MON-SAT 03:00: Universe seed (NYSE + NASDAQ Common Stock)
- MON-SAT after Step 1 completion: price sync (bulk API) + split/dividend detection
- MON-SAT after Step 2 completion: fundamentals sync (changes + corporate actions)
- MON-SAT 04:15: SP500TR benchmark update
- MON-SAT 05:00: PAIN cache refresh (max drawdown from full series)
- MON-SAT 05:00: Parallel backfill ALL (1,000 tickers/day, manual toggle)
- MON-SAT 05:30: Key metrics + peer medians
- MON-SAT 06:00: Admin report
- DAILY  13:00: News & sentiment refresh (followed/watchlisted tickers)

Run with: python scheduler.py
"""

import os
import sys
import asyncio
import logging
from datetime import datetime, timedelta, timezone
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

# SUNDAY EXCLUSION — Universe Seed runs Mon-Sat; Sunday is news-only
UNIVERSE_SEED_DAY = 6  # Sunday (used to identify the news-only day)
UNIVERSE_SEED_HOUR = 3
UNIVERSE_SEED_MINUTE = 0

# MON-SAT - Daily jobs
DAILY_SCHEDULE_DAYS = [0, 1, 2, 3, 4, 5]  # Mon=0, Sat=5 (excludes Sunday=6)

PRICE_SYNC_HOUR = 4
PRICE_SYNC_MINUTE = 0

FUNDAMENTALS_SYNC_HOUR = 4
FUNDAMENTALS_SYNC_MINUTE = 30

# NEW: Parallel backfill all prices at 05:00
BACKFILL_ALL_HOUR = 5
BACKFILL_ALL_MINUTE = 0

# NEWS: Daily news refresh at 13:00
NEWS_REFRESH_HOUR = 13
NEWS_REFRESH_MINUTE = 0

# SP500TR.INDX: Daily benchmark update at 04:15
BENCHMARK_UPDATE_HOUR = 4
BENCHMARK_UPDATE_MINUTE = 15

# MARKET CALENDAR: Daily refresh at 02:00 (before pipeline at 03:00)
MARKET_CALENDAR_HOUR = 2
MARKET_CALENDAR_MINUTE = 0

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


def to_prague_iso(dt: datetime | None) -> str | None:
    """Format datetime as Prague ISO string for audit logs."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(TIMEZONE).isoformat()


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
        "start_time_prague": to_prague_iso(start_time),
        "end_time_prague": to_prague_iso(end_time),
        "log_timezone": "Europe/Prague",
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
                "started_at_prague": to_prague_iso(started_at),
                "completed_at_prague": to_prague_iso(completed_at),
                "log_timezone": "Europe/Prague",
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
    
    # Log failed job to BOTH collections — best-effort so that a logging
    # failure cannot crash the caller (e.g. the scheduler daemon).
    completed_at = datetime.now(timezone.utc)
    error_msg = f"Max retries ({max_retries}) exceeded"
    
    try:
        await log_job_execution(
            db, job_name, "error", started_at, completed_at,
            error_message=error_msg
        )
    except Exception:
        logger.error(f"{job_name}: failed to log failure to system_job_logs")
    
    try:
        await db.ops_job_runs.insert_one({
            "job_name": job_name,
            "status": "failed",
            "started_at": started_at,
            "completed_at": completed_at,
            "started_at_prague": to_prague_iso(started_at),
            "completed_at_prague": to_prague_iso(completed_at),
            "log_timezone": "Europe/Prague",
            "duration_seconds": (completed_at - started_at).total_seconds(),
            "details": {"error": error_msg}
        })
    except Exception:
        logger.error(f"{job_name}: failed to log failure to ops_job_runs")
    
    return {"status": "failed", "error": error_msg}


async def _run_universe_seed_scheduled(db):
    """
    Run Universe Seed for the nightly scheduler with full sentinel + live progress logging.

    Mirrors the sentinel+progress logic used by the admin manual endpoint
    (_run_universe_seed_bg in server.py) so that scheduled runs also show
    a live progress bar and correct Prague finish time in the Admin UI.

    Unlike run_job_with_retry, this function:
    - Inserts a "running" sentinel BEFORE calling sync_ticker_whitelist.
    - Passes a progress_callback so UI polls show "N / total tickers" live.
    - Updates the sentinel in-place on completion/failure (no duplicate doc).
    - Still logs to system_job_logs for observability parity.
    """
    import uuid
    from whitelist_service import sync_ticker_whitelist

    job_id = f"universe_seed_{uuid.uuid4().hex[:8]}"
    chain_run_id = f"chain_sched_{uuid.uuid4().hex[:12]}"
    started_at = datetime.now(timezone.utc)
    logger.info(f"[scheduler] Universe Seed started (job_id={job_id})")

    # Create pipeline_chain_runs document so the canonical chain registry
    # tracks this scheduled pipeline execution from the very start.
    await db.pipeline_chain_runs.insert_one({
        "chain_run_id": chain_run_id,
        "status": "running",
        "current_step": 1,
        "steps_done": [],
        "started_at": started_at,
        "source": "scheduled",
        "step_run_ids": {},
    })

    # Insert running sentinel so Admin UI shows "running" immediately.
    _sentinel = await db.ops_job_runs.insert_one({
        "job_id": job_id,
        "job_name": "universe_seed",
        "status": "running",
        "source": "scheduled",
        "triggered_by": "scheduler",
        "started_at": started_at,
        "started_at_prague": to_prague_iso(started_at),
        "log_timezone": "Europe/Prague",
        "progress": "Fetching symbols from EODHD…",
        "progress_pct": 0,
        "details": {"chain_run_id": chain_run_id},
    })
    _doc_id = _sentinel.inserted_id

    async def _progress(processed: int, total: int) -> None:
        pct = round(100.0 * processed / total) if total else 0
        await db.ops_job_runs.update_one(
            {"_id": _doc_id},
            {"$set": {
                "progress": f"Seeding… {processed:,} / {total:,} tickers",
                "progress_processed": processed,
                "progress_total": total,
                "progress_pct": pct,
            }},
        )

    async def _raw_total(raw_rows_total: int) -> None:
        """Write raw total to sentinel as soon as all exchange symbols are fetched."""
        await db.ops_job_runs.update_one(
            {"_id": _doc_id},
            {"$set": {
                "raw_rows_total": raw_rows_total,
                "details.raw_rows_total": raw_rows_total,
            }},
        )

    status = "failed"
    result: dict = {}
    try:
        result = await sync_ticker_whitelist(
            db, dry_run=False, job_run_id=job_id,
            progress_callback=_progress,
            raw_total_callback=_raw_total,
        )
        status = "completed"
        logger.info(f"[scheduler] Universe Seed completed: {result.get('seeded_total', 0)} seeded")
    except Exception as exc:
        result = {"error": str(exc)}
        logger.error(f"[scheduler] Universe Seed failed: {exc}")

    finished_at = datetime.now(timezone.utc)
    duration = (finished_at - started_at).total_seconds()

    if status == "completed":
        _s1_run_id = result.get("raw_run_id") or job_id
        await db.ops_job_runs.update_one(
            {"_id": _doc_id},
            {"$set": {
                "status": "completed",
                "finished_at": finished_at,
                "finished_at_prague": to_prague_iso(finished_at),
                "duration_seconds": duration,
                "result": result,
                "details": {
                    "chain_run_id": chain_run_id,
                    "exclusion_report_run_id": result.get("exclusion_report_run_id"),
                    "fetched": result.get("fetched") or 0,
                    "raw_rows_total": result.get("raw_rows_total") or 0,
                    "seeded_total": result.get("seeded_total") or 0,
                    "filtered_out_total_step1": result.get("filtered_out_total_step1") or 0,
                    "fetched_raw_per_exchange": result.get("fetched_raw_per_exchange") or {},
                },
                "progress": f"Completed: {result.get('seeded_total', 0):,} seeded",
                "progress_pct": 100,
            }},
        )
        # Update pipeline_chain_runs: Step 1 done, advance to Step 2.
        await db.pipeline_chain_runs.update_one(
            {"chain_run_id": chain_run_id},
            {"$set": {
                "step_run_ids.step1": _s1_run_id,
                "current_step": 2,
                "steps_done": [1],
            }},
        )
    else:
        await db.ops_job_runs.update_one(
            {"_id": _doc_id},
            {"$set": {
                "status": "failed",
                "finished_at": finished_at,
                "finished_at_prague": to_prague_iso(finished_at),
                "duration_seconds": duration,
                "error": result.get("error", "unknown error"),
                "progress": "Failed",
            }},
        )
        # Mark pipeline_chain_runs as failed since Step 1 failed.
        await db.pipeline_chain_runs.update_one(
            {"chain_run_id": chain_run_id},
            {"$set": {
                "status": "failed",
                "failed_step": 1,
                "error": result.get("error", "unknown error"),
                "finished_at": finished_at,
                "finished_at_prague": to_prague_iso(finished_at),
            }},
        )

    # Also log to system_job_logs for observability parity with other jobs.
    records_processed = result.get("seeded_total", 0) if status == "completed" else 0
    await log_job_execution(
        db, "universe_seed",
        "success" if status == "completed" else "error",
        started_at, finished_at,
        records_processed=records_processed,
        details=result if isinstance(result, dict) else {},
    )

    return result


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
    logger.info(f"  {BENCHMARK_UPDATE_HOUR:02d}:{BENCHMARK_UPDATE_MINUTE:02d} - Benchmark update (SP500TR + future benchmarks)")
    logger.info(f"  {FUNDAMENTALS_SYNC_HOUR:02d}:{FUNDAMENTALS_SYNC_MINUTE:02d} - Fundamentals sync")
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
                try:
                    await log_heartbeat(last_run)
                except Exception as hb_exc:
                    logger.error(f"[HEARTBEAT] Failed to write heartbeat (non-fatal): {hb_exc}")
            
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
            #
            # GUARDRAIL: Use the already-captured `now.weekday()` instead of
            # the standalone day-check helpers which invoke get_prague_time()
            # independently.  Near midnight the two calls could straddle the
            # day boundary (TOCTOU) and cause the loop to skip an entire
            # iteration — neither Sunday nor weekday code would execute.
            # =================================================================
            weekday = now.weekday()

            if weekday == UNIVERSE_SEED_DAY:
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
            if weekday not in DAILY_SCHEDULE_DAYS:
                await asyncio.sleep(60)
                continue
            
            # STEP 1: Universe Seed at 03:00 Mon-Sat
            if should_run("universe_seed", UNIVERSE_SEED_HOUR, UNIVERSE_SEED_MINUTE, last_run, today_str, current_hour, current_minute):
                logger.info(f"Triggering universe_seed STEP 1 (hour={current_hour}, scheduled={UNIVERSE_SEED_HOUR}:{UNIVERSE_SEED_MINUTE:02d})")
                try:
                    _s1_result = await _run_universe_seed_scheduled(db)
                    # _run_universe_seed_scheduled catches internal errors and
                    # returns {"error": "..."} instead of raising.  Only mark
                    # the job as "ran today" when it actually succeeded;
                    # otherwise the next tick will retry and Steps 2/3 won't
                    # be triggered on a failed Step 1.
                    if isinstance(_s1_result, dict) and _s1_result.get("error"):
                        logger.warning(
                            f"[scheduler] universe_seed STEP 1 failed internally: "
                            f"{_s1_result.get('error')} – will retry next tick"
                        )
                    else:
                        last_run["universe_seed"] = today_str
                        await set_last_run_state(last_run)
                except Exception as exc:
                    # _run_universe_seed_scheduled has its own internal try/except
                    # for the actual sync work, but early failures (import, DB
                    # connection) can propagate here.  Log and let the next
                    # iteration retry instead of crashing the whole daemon.
                    logger.error(
                        f"[scheduler] universe_seed STEP 1 unhandled error "
                        f"(will retry next minute): {exc}"
                    )
                    try:
                        await log_job_execution(
                            db, "universe_seed", "error",
                            datetime.now(timezone.utc), datetime.now(timezone.utc),
                            error_message=f"Scheduler unhandled: {exc}",
                        )
                    except Exception:
                        pass  # best-effort observability

            # STEP 2: Price sync immediately after Step 1 completes
            if should_run_after_dependency("price_sync", "universe_seed", last_run, today_str):
                logger.info("Triggering price_sync (dependency: universe_seed completed)")
                try:
                    # Read the latest completed universe_seed run to get both
                    # parent_run_id (exclusion_report_run_id) and chain_run_id.
                    _s1_doc = await db.ops_job_runs.find_one(
                        {
                            "job_name": "universe_seed",
                            "status": "completed",
                            "details.exclusion_report_run_id": {"$exists": True, "$ne": None},
                            "details.chain_run_id": {"$exists": True, "$ne": None},
                        },
                        {"details.exclusion_report_run_id": 1, "details.chain_run_id": 1},
                        sort=[("started_at", -1)],
                    )
                    _s1_excl_run_id = (_s1_doc or {}).get("details", {}).get("exclusion_report_run_id")
                    _s1_chain_run_id = (_s1_doc or {}).get("details", {}).get("chain_run_id")
                    _s2_result = await run_job_with_retry(
                        "price_sync",
                        lambda _db, _pid=_s1_excl_run_id, _cid=_s1_chain_run_id: run_daily_price_sync(
                            _db, parent_run_id=_pid, chain_run_id=_cid
                        ),
                        db,
                    )
                    # Only advance last_run on success so the step retries
                    # next tick on failure (matching Step 1 pattern).
                    _s2_failed = (
                        isinstance(_s2_result, dict)
                        and (_s2_result.get("error") or _s2_result.get("status") == "failed")
                    )
                    if _s2_failed:
                        logger.warning(
                            f"[scheduler] price_sync STEP 2 failed: "
                            f"{_s2_result} – will retry next tick"
                        )
                    else:
                        last_run["price_sync"] = today_str
                        await set_last_run_state(last_run)
                        # Update pipeline_chain_runs: Step 2 done, advance to Step 3.
                        if _s1_chain_run_id:
                            try:
                                _s2_excl_run_id = (_s2_result or {}).get("exclusion_report_run_id") if isinstance(_s2_result, dict) else None
                                await db.pipeline_chain_runs.update_one(
                                    {"chain_run_id": _s1_chain_run_id},
                                    {"$set": {
                                        "step_run_ids.step2": _s2_excl_run_id,
                                        "current_step": 3,
                                        "steps_done": [1, 2],
                                    }},
                                )
                            except Exception as _chain_exc:
                                logger.error(
                                    f"[scheduler] pipeline_chain_runs Step 2 "
                                    f"update failed (non-fatal): {_chain_exc}"
                                )
                except Exception as exc:
                    logger.error(
                        f"[scheduler] price_sync STEP 2 unhandled error "
                        f"(will retry next minute): {exc}"
                    )
                    try:
                        await log_job_execution(
                            db, "price_sync", "error",
                            datetime.now(timezone.utc), datetime.now(timezone.utc),
                            error_message=f"Scheduler unhandled: {exc}",
                        )
                    except Exception:
                        pass  # best-effort observability
            
            # STEP 3: Fundamentals sync immediately after Step 2 completes
            if should_run_after_dependency("fundamentals_sync", "price_sync", last_run, today_str):
                logger.info("Triggering fundamentals_sync (dependency: price_sync completed)")
                try:
                    # Exact Step 2 parent: the exclusion_report_run_id from the just-completed Step 2.
                    _s2_doc = await db.ops_job_runs.find_one(
                        {"job_name": "price_sync",
                         "details.exclusion_report_run_id": {"$exists": True, "$ne": None}},
                        {"details.exclusion_report_run_id": 1, "details.chain_run_id": 1},
                        sort=[("started_at", -1)],
                    )
                    _s2_run_id_for_s3 = (
                        (_s2_doc or {}).get("details", {}).get("exclusion_report_run_id")
                    )
                    _s2_chain_run_id = (_s2_doc or {}).get("details", {}).get("chain_run_id")
                    _s3_result = await run_job_with_retry(
                        "fundamentals_sync",
                        lambda _db, _pid=_s2_run_id_for_s3, _cid=_s2_chain_run_id: run_fundamentals_changes_sync(
                            _db, parent_run_id=_pid, chain_run_id=_cid
                        ),
                        db,
                    )
                    # Only advance last_run on success so the step retries
                    # next tick on failure (matching Step 1 pattern).
                    _s3_failed = (
                        isinstance(_s3_result, dict)
                        and (_s3_result.get("error") or _s3_result.get("status") == "failed")
                    )
                    if _s3_failed:
                        logger.warning(
                            f"[scheduler] fundamentals_sync STEP 3 failed: "
                            f"{_s3_result} – will retry next tick"
                        )
                    else:
                        last_run["fundamentals_sync"] = today_str
                        await set_last_run_state(last_run)
                        # Update pipeline_chain_runs: terminal status.
                        if _s2_chain_run_id:
                            try:
                                _s3_excl_run_id = (_s3_result or {}).get("exclusion_report_run_id") if isinstance(_s3_result, dict) else None
                                _s3_fin = datetime.now(timezone.utc)
                                await db.pipeline_chain_runs.update_one(
                                    {"chain_run_id": _s2_chain_run_id},
                                    {"$set": {
                                        "status": "completed",
                                        "step_run_ids.step3": _s3_excl_run_id,
                                        "steps_done": [1, 2, 3],
                                        "current_step": None,
                                        "finished_at": _s3_fin,
                                        "finished_at_prague": to_prague_iso(_s3_fin),
                                    }},
                                )
                            except Exception as _chain_exc:
                                logger.error(
                                    f"[scheduler] pipeline_chain_runs Step 3 "
                                    f"update failed (non-fatal): {_chain_exc}"
                                )
                except Exception as exc:
                    logger.error(
                        f"[scheduler] fundamentals_sync STEP 3 unhandled error "
                        f"(will retry next minute): {exc}"
                    )
                    try:
                        await log_job_execution(
                            db, "fundamentals_sync", "error",
                            datetime.now(timezone.utc), datetime.now(timezone.utc),
                            error_message=f"Scheduler unhandled: {exc}",
                        )
                    except Exception:
                        pass  # best-effort observability
            
            # ==================================================================
            # MARKET CALENDAR REFRESH at 02:00 — idempotent, runs daily
            # ==================================================================
            # Fetches EODHD exchange-details/US and regenerates calendar rows.
            # Upsert-based, safe to run daily.  Ensures indexes on first run.
            # Wrapped in try/except so a failure here cannot crash the daemon
            # and block Steps 1-2-3 at 03:00.
            if should_run("market_calendar", MARKET_CALENDAR_HOUR, MARKET_CALENDAR_MINUTE, last_run, today_str, current_hour, current_minute):
                logger.info(f"Triggering market_calendar (hour={current_hour}, scheduled={MARKET_CALENDAR_HOUR}:{MARKET_CALENDAR_MINUTE:02d})")
                _mc_started = datetime.now(timezone.utc)
                try:
                    from services.market_calendar_service import refresh_market_calendar, ensure_indexes as _mc_ensure_indexes
                    async def _market_calendar_job(_db):
                        await _mc_ensure_indexes(_db)
                        return await refresh_market_calendar(_db)
                    await run_job_with_retry("market_calendar", _market_calendar_job, db)
                    last_run["market_calendar"] = today_str
                    await set_last_run_state(last_run)
                except Exception as exc:
                    logger.error(
                        f"[scheduler] market_calendar unhandled error "
                        f"(will retry next minute): {exc}"
                    )
                    try:
                        await log_job_execution(
                            db, "market_calendar", "error",
                            _mc_started, datetime.now(timezone.utc),
                            error_message=f"Scheduler unhandled: {exc}",
                        )
                    except Exception:
                        pass  # best-effort observability

            # ==================================================================
            # BENCHMARK UPDATE at 04:15 — standalone, NOT part of Steps 1-2-3
            # ==================================================================
            # Runs after the main pricing pipeline but is completely independent
            # of the bulk ticker flow.  Uses its own dedicated EODHD /eod/ calls
            # and is never filtered by universe/seed/visibility rules.
            # Extensible: iterates BENCHMARK_SYMBOLS registry automatically.
            if should_run("benchmark_update", BENCHMARK_UPDATE_HOUR, BENCHMARK_UPDATE_MINUTE, last_run, today_str, current_hour, current_minute):
                logger.info(f"Triggering benchmark_update (hour={current_hour}, scheduled={BENCHMARK_UPDATE_HOUR}:{BENCHMARK_UPDATE_MINUTE:02d})")
                _bm_started = datetime.now(timezone.utc)
                try:
                    from benchmark_service import update_all_benchmarks
                    await run_job_with_retry("benchmark_update", update_all_benchmarks, db)
                    last_run["benchmark_update"] = today_str
                    await set_last_run_state(last_run)
                except Exception as exc:
                    logger.error(
                        f"[scheduler] benchmark_update unhandled error "
                        f"(will retry next minute): {exc}"
                    )
                    try:
                        await log_job_execution(
                            db, "benchmark_update", "error",
                            _bm_started, datetime.now(timezone.utc),
                            error_message=f"Scheduler unhandled: {exc}",
                        )
                    except Exception:
                        pass  # best-effort observability
            
            # BACKFILL_ALL: MANUAL ONLY by default
            try:
                backfill_all_enabled = await db.ops_config.find_one({"key": "job_backfill_all_enabled"})
                backfill_all_auto = backfill_all_enabled.get("value", False) if backfill_all_enabled else False
            except Exception:
                backfill_all_auto = False
            
            if backfill_all_auto and should_run("backfill_all", BACKFILL_ALL_HOUR, BACKFILL_ALL_MINUTE, last_run, today_str, current_hour, current_minute):
                logger.info(f"Triggering backfill_all (hour={current_hour}, scheduled={BACKFILL_ALL_HOUR}:{BACKFILL_ALL_MINUTE:02d})")
                try:
                    await run_job_with_retry("backfill_all", run_scheduled_backfill_all_prices, db)
                    last_run["backfill_all"] = today_str
                    await set_last_run_state(last_run)
                except Exception as exc:
                    logger.error(f"[scheduler] backfill_all unhandled error (will retry next minute): {exc}")
            
            # KEY METRICS at 05:00 (catch-up enabled)
            if should_run("key_metrics", KEY_METRICS_HOUR, KEY_METRICS_MINUTE, last_run, today_str, current_hour, current_minute):
                logger.info(f"Triggering key_metrics (hour={current_hour}, scheduled={KEY_METRICS_HOUR}:{KEY_METRICS_MINUTE:02d})")
                try:
                    from key_metrics_service import compute_daily_key_metrics
                    await run_job_with_retry("key_metrics", compute_daily_key_metrics, db)
                    last_run["key_metrics"] = today_str
                    await set_last_run_state(last_run)
                except Exception as exc:
                    logger.error(f"[scheduler] key_metrics unhandled error (will retry next minute): {exc}")
            
            # PEER MEDIANS at 05:30 (catch-up enabled)
            if should_run("peer_medians", PEER_MEDIANS_HOUR, PEER_MEDIANS_MINUTE, last_run, today_str, current_hour, current_minute):
                logger.info(f"Triggering peer_medians (hour={current_hour}, scheduled={PEER_MEDIANS_HOUR}:{PEER_MEDIANS_MINUTE:02d})")
                try:
                    from key_metrics_service import compute_peer_benchmarks_v3
                    await run_job_with_retry("peer_medians", compute_peer_benchmarks_v3, db)
                    last_run["peer_medians"] = today_str
                    await set_last_run_state(last_run)
                except Exception as exc:
                    logger.error(f"[scheduler] peer_medians unhandled error (will retry next minute): {exc}")
            
            # PAIN CACHE at 05:00 (catch-up enabled)
            if should_run("pain_cache", PAIN_CACHE_HOUR, PAIN_CACHE_MINUTE, last_run, today_str, current_hour, current_minute):
                logger.info(f"Triggering pain_cache (hour={current_hour}, scheduled={PAIN_CACHE_HOUR}:{PAIN_CACHE_MINUTE:02d})")
                try:
                    from server import run_pain_cache_refresh
                    await run_job_with_retry("pain_cache", run_pain_cache_refresh, db)
                    last_run["pain_cache"] = today_str
                    await set_last_run_state(last_run)
                except Exception as exc:
                    logger.error(f"[scheduler] pain_cache unhandled error (will retry next minute): {exc}")
            
            # ADMIN REPORT at 06:00 (catch-up enabled)
            if should_run("admin_report", ADMIN_REPORT_HOUR, ADMIN_REPORT_MINUTE, last_run, today_str, current_hour, current_minute):
                logger.info(f"Triggering admin_report (hour={current_hour}, scheduled={ADMIN_REPORT_HOUR}:{ADMIN_REPORT_MINUTE:02d})")
                try:
                    from services.admin_report_service import run_admin_report_job
                    await run_job_with_retry("admin_report", run_admin_report_job, db)
                    last_run["admin_report"] = today_str
                    await set_last_run_state(last_run)
                except Exception as exc:
                    logger.error(f"[scheduler] admin_report unhandled error (will retry next minute): {exc}")
            
            # NEWS at 13:00 (catch-up enabled)
            if should_run("news_refresh", NEWS_REFRESH_HOUR, NEWS_REFRESH_MINUTE, last_run, today_str, current_hour, current_minute):
                logger.info(f"Triggering news_refresh (hour={current_hour}, scheduled={NEWS_REFRESH_HOUR}:{NEWS_REFRESH_MINUTE:02d})")
                try:
                    from services.news_service import news_daily_refresh
                    await run_job_with_retry("news_refresh", news_daily_refresh, db)
                    last_run["news_refresh"] = today_str
                    await set_last_run_state(last_run)
                except Exception as exc:
                    logger.error(f"[scheduler] news_refresh unhandled error (will retry next minute): {exc}")
            
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
