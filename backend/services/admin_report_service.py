"""
RICHSTOX Admin Report Service (P45)

Generates daily admin reports with job status, API costs, data ingested, and warnings.
Runs at 06:00 daily (after all morning scheduler jobs).

Collection: admin_daily_reports
Retention: 90 days

Audit fields (P45 FINAL POLISH):
- generated_at: Europe/Prague local time, ISO string
- generation_source: "scheduled" or "manual"
- generation_duration_ms: time taken to generate report

BINDING: Do not change without Richard's approval (kurtarichard@gmail.com).
"""

import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional, Literal
from zoneinfo import ZoneInfo

logger = logging.getLogger("richstox.admin_report")

PRAGUE_TZ = ZoneInfo("Europe/Prague")
RETENTION_DAYS = 90

# Type for generation source
GenerationSource = Literal["scheduled", "manual"]


async def generate_daily_report(db, source: GenerationSource = "manual") -> Dict[str, Any]:
    """
    Generate the daily admin report.
    
    Args:
        db: MongoDB database instance
        source: "scheduled" (from scheduler) or "manual" (from API call)
    
    Collects:
    - Jobs status (last run times, success/failure)
    - API costs (EODHD calls from sync_state)
    - Data ingested (counts from collections)
    - Warnings (missing data, gaps, errors)
    
    Returns:
        Report dictionary with audit fields
    """
    start_time = time.perf_counter()
    
    now_utc = datetime.now(timezone.utc)
    now_prague = datetime.now(PRAGUE_TZ)
    today_str = now_prague.strftime("%Y-%m-%d")
    day_of_week = now_prague.weekday()  # 0=Mon, 6=Sun
    
    report = {
        "report_id": f"report_{today_str}",
        "report_date": today_str,
        # Audit fields (P45 FINAL POLISH)
        "generated_at": now_prague.isoformat(),  # Europe/Prague local time
        "generated_at_utc": now_utc.isoformat(),
        "generation_source": source,
        "generation_duration_ms": 0,  # Will be set at the end
        # Report data
        "jobs": {},
        "api_costs": {},
        "data_summary": {},
        "warnings": [],
    }
    
    # =========================================================================
    # JOBS STATUS - Query ops_job_runs for actual execution data (P46)
    # =========================================================================
    # Map scheduler job names to ops_job_runs job_type patterns
    job_mapping = {
        "universe_seed": {"patterns": ["universe_seed", "whitelist_seed", "sync_ticker_whitelist"], "sunday_only": False},
        "price_sync": {"patterns": ["daily_price_sync", "scheduled_price_sync", "price_sync"], "sunday_only": False},
        "fundamentals_sync": {"patterns": ["scheduled_fundamentals_sync", "fundamentals_sync", "fundamentals_batch"], "sunday_only": False},
        "backfill_gaps": {"patterns": ["backfill_gaps", "scheduled_backfill_gaps"], "sunday_only": False},
        "backfill_all": {"patterns": ["backfill_all", "scheduled_backfill_all", "parallel_backfill"], "sunday_only": False},
        "news_refresh": {"patterns": ["news_refresh", "news_daily_refresh", "news_sync"], "sunday_only": False},
        "sp500tr_update": {"patterns": ["sp500tr_update", "sp500tr_sync"], "sunday_only": False},
        "key_metrics": {"patterns": ["key_metrics", "metrics_calc"], "sunday_only": False},
        "peer_medians": {"patterns": ["peer_medians", "medians_calc"], "sunday_only": False},
        "pain_cache": {"patterns": ["pain_cache", "pain_refresh"], "sunday_only": False},
        "admin_report": {"patterns": ["admin_report"], "sunday_only": False},
    }
    
    # Get today's start timestamp for filtering (Europe/Prague timezone)
    # Line 91: This ensures "today" boundary is Prague midnight, not UTC
    today_start = now_prague.replace(hour=0, minute=0, second=0, microsecond=0)
    today_start_utc = today_start.astimezone(timezone.utc)
    current_hour = now_prague.hour
    current_minute = now_prague.minute
    
    # Scheduled times (Europe/Prague) for expected_by_now calculation.
    # Step 1 is hard scheduled at 23:00 (Mon-Sat). Step 2/3 are dependency based
    # and become expected once their dependency should already have run.
    job_schedule = {
        "universe_seed": {"hour": 23, "minute": 0},      # Mon-Sat 23:00
        "sp500tr_update": {"hour": 4, "minute": 15},     # 04:15
        "backfill_gaps": {"hour": 4, "minute": 45},      # 04:45
        "backfill_all": {"hour": 5, "minute": 0},        # 05:00
        "key_metrics": {"hour": 5, "minute": 0},         # 05:00
        "pain_cache": {"hour": 5, "minute": 0},          # 05:00
        "peer_medians": {"hour": 5, "minute": 30},       # 05:30
        "admin_report": {"hour": 6, "minute": 0},        # 06:00
        "news_refresh": {"hour": 13, "minute": 0},       # 13:00
    }
    dependency_expected = {
        "price_sync": "universe_seed",
        "fundamentals_sync": "price_sync",
    }
    
    def is_expected_by_now(job_name: str) -> bool:
        """Check if job should have run by now based on Europe/Prague time."""
        dep = dependency_expected.get(job_name)
        if dep:
            return is_expected_by_now(dep)
        schedule = job_schedule.get(job_name)
        if not schedule:
            return False
        sched_hour = schedule["hour"]
        sched_minute = schedule["minute"]
        # Job is expected if current time is past scheduled time
        if current_hour > sched_hour:
            return True
        if current_hour == sched_hour and current_minute >= sched_minute:
            return True
        return False
    
    # Fetch all job runs from today
    all_patterns = []
    for job_info in job_mapping.values():
        all_patterns.extend(job_info["patterns"])
    
    # Query ops_job_runs for today's jobs
    today_runs = await db.ops_job_runs.find({
        "$or": [
            {"job_name": {"$in": all_patterns}},
            {"job_type": {"$in": all_patterns}},
        ],
        "started_at": {"$gte": today_start_utc}
    }).sort("started_at", -1).to_list(100)
    
    # Also check news_sync_state for jobs that track there
    sync_docs = await db.news_sync_state.find({}).to_list(50)
    sync_map = {d.get("job_name", ""): d for d in sync_docs}
    
    # Build job status for each job
    for job_name, job_info in job_mapping.items():
        patterns = job_info["patterns"]
        is_sunday_only = job_info["sunday_only"]
        
        # Find matching run for this job
        matching_run = None
        for run in today_runs:
            run_name = run.get("job_name") or run.get("job_type") or ""
            if any(p in run_name.lower() for p in [p.lower() for p in patterns]):
                matching_run = run
                break
        
        # Also check sync_state as fallback
        sync_doc = None
        for pattern in patterns:
            if pattern in sync_map:
                sync_doc = sync_map[pattern]
                break
        
        if matching_run:
            # Job ran today - extract details
            status = matching_run.get("status", "unknown")
            if status in ["completed", "success"]:
                status = "success"
            elif status in ["failed", "error"]:
                status = "error"
            
            started_at = matching_run.get("started_at")
            finished_at = matching_run.get("finished_at")
            details = matching_run.get("details", {})
            
            # Calculate duration
            duration_ms = 0
            if started_at and finished_at:
                try:
                    duration_ms = int((finished_at - started_at).total_seconds() * 1000)
                except:
                    pass
            elif details.get("duration_seconds"):
                duration_ms = int(details["duration_seconds"] * 1000)
            
            # Extract metrics from details
            api_calls = details.get("api_calls_used") or details.get("api_calls") or 0
            records = details.get("processed") or details.get("records_upserted") or details.get("total_tickers") or 0
            
            report["jobs"][job_name] = {
                "status": status,
                "ran_today": True,
                "expected_by_now": is_expected_by_now(job_name),
                "last_run": started_at.isoformat() if started_at else None,
                "duration_ms": duration_ms,
                "api_calls": api_calls,
                "records_updated": records,
            }
            
            # Add error message if failed
            if status == "error":
                errors = details.get("errors", [])
                report["jobs"][job_name]["error_count"] = len(errors) if isinstance(errors, list) else 0
                
        elif sync_doc and sync_doc.get("last_successful_run"):
            # Check sync_state for last run
            last_run = sync_doc.get("last_successful_run")
            ran_today = False
            try:
                ran_today = last_run.strftime("%Y-%m-%d") == today_str
            except:
                pass
            
            report["jobs"][job_name] = {
                "status": "success" if ran_today else "not_run_yet",
                "ran_today": ran_today,
                "expected_by_now": is_expected_by_now(job_name),
                "last_run": last_run.isoformat() if last_run else None,
            }
        else:
            # Job not run today
            expected = is_expected_by_now(job_name)
            if is_sunday_only and day_of_week != 6:
                report["jobs"][job_name] = {
                    "status": "not_scheduled",
                    "ran_today": False,
                    "expected_by_now": False,  # Not scheduled today
                    "message": "Runs on Sunday only"
                }
            else:
                report["jobs"][job_name] = {
                    "status": "not_run_yet",
                    "ran_today": False,
                    "expected_by_now": expected,
                    "message": "Not run yet today" if not expected else "Overdue"
                }
    
    # Warnings for missed jobs (only if expected_by_now=True and not run)
    for job_name, job_status in report["jobs"].items():
        if job_status.get("expected_by_now") and not job_status.get("ran_today") and job_status.get("status") != "not_scheduled":
            report["warnings"].append({"type": "job_overdue", "job": job_name, "message": f"Job '{job_name}' is overdue"})
    
    # =========================================================================
    # API COSTS + DATA SUMMARY (fast counts only)
    # =========================================================================
    watchlist_count = await db.user_watchlist.count_documents({})
    visible_tracked = await db.tracked_tickers.count_documents({"is_visible": True})
    total_articles = await db.news_articles.count_documents({})
    fundamentals_count = await db.company_fundamentals_cache.count_documents({})
    
    report["api_costs"] = {
        "news_api_calls": watchlist_count,
        "price_bulk_calls": 1 if day_of_week != 6 else 0,
        "estimated_total": watchlist_count + (2 if day_of_week != 6 else 1),
    }
    
    report["data_summary"] = {
        "visible_tickers": visible_tracked,
        "news_articles": total_articles,
        "watchlist": watchlist_count,
        "fundamentals": fundamentals_count,
    }
    
    # Warnings
    if visible_tracked < 100:
        report["warnings"].append({"type": "low_visible_tickers", "count": visible_tracked})
    
    # Calculate generation duration (audit field)
    end_time = time.perf_counter()
    report["generation_duration_ms"] = round((end_time - start_time) * 1000)
    
    return report


async def save_daily_report(db, report: Dict[str, Any]) -> Dict[str, Any]:
    """
    Save the daily report to admin_daily_reports collection.
    Uses upsert to avoid duplicates for the same date.
    """
    result = await db.admin_daily_reports.update_one(
        {"report_date": report["report_date"]},
        {"$set": report},
        upsert=True
    )
    
    return {
        "status": "saved",
        "report_date": report["report_date"],
        "upserted": result.upserted_id is not None,
        "modified": result.modified_count > 0,
    }


async def cleanup_old_reports(db) -> Dict[str, int]:
    """
    Delete reports older than RETENTION_DAYS (90 days).
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=RETENTION_DAYS)
    cutoff_str = cutoff.strftime("%Y-%m-%d")
    
    result = await db.admin_daily_reports.delete_many({
        "report_date": {"$lt": cutoff_str}
    })
    
    return {"deleted_count": result.deleted_count}


async def run_admin_report_job(db) -> Dict[str, Any]:
    """
    Main entry point for scheduled job at 06:00.
    Generates report, saves it, and cleans up old reports.
    Sets generation_source = "scheduled"
    """
    logger.info("Starting daily admin report generation (scheduled)")
    
    # Generate report with source="scheduled"
    report = await generate_daily_report(db, source="scheduled")
    
    # Save to DB
    save_result = await save_daily_report(db, report)
    
    # Cleanup old reports
    cleanup_result = await cleanup_old_reports(db)
    
    logger.info(f"Admin report generated for {report['report_date']} in {report.get('generation_duration_ms', 0)}ms, {cleanup_result['deleted_count']} old reports cleaned")
    
    return {
        "status": "completed",
        "report_date": report["report_date"],
        "generation_source": report.get("generation_source"),
        "generation_duration_ms": report.get("generation_duration_ms"),
        "warnings_count": len(report["warnings"]),
        "cleanup": cleanup_result,
    }


async def get_today_report(db) -> Optional[Dict[str, Any]]:
    """
    Get today's admin report.
    """
    now_prague = datetime.now(PRAGUE_TZ)
    today_str = now_prague.strftime("%Y-%m-%d")
    
    report = await db.admin_daily_reports.find_one(
        {"report_date": today_str},
        {"_id": 0}
    )
    
    return report


async def get_report_by_date(db, date_str: str) -> Optional[Dict[str, Any]]:
    """
    Get admin report for a specific date.
    
    Args:
        date_str: Date in YYYY-MM-DD format
    """
    report = await db.admin_daily_reports.find_one(
        {"report_date": date_str},
        {"_id": 0}
    )
    
    return report


async def get_recent_reports(db, days: int = 7) -> list:
    """
    Get the most recent admin reports.
    
    Args:
        days: Number of days to look back (default 7)
    """
    reports = await db.admin_daily_reports.find(
        {},
        {"_id": 0}
    ).sort("report_date", -1).limit(days).to_list(days)
    
    return reports
