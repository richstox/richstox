"""
Admin Overview Service (P48 - Single Source of Truth)
=====================================================
Single aggregated endpoint for Admin Panel v2.

P48 REQUIREMENTS:
0) Uses same header as end-user app
1) Health = Unknown when no runs today (not "10% Critical")
2) API calls = Unknown if not logged (not 0)
3) Universe Funnel uses shared universe_counts_service
4) Jobs sorted by next run time

BINDING: Do not change without Richard's approval.
"""

import asyncio
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional
from zoneinfo import ZoneInfo

from services.universe_counts_service import get_universe_counts
from credit_log_service import get_pipeline_sync_status

PRAGUE_TZ = ZoneInfo("Europe/Prague")


async def get_job_last_runs(db) -> Dict[str, Any]:
    """
    Get last run status for all scheduled jobs from ops_job_runs.
    Uses single aggregation instead of N sequential find_one calls — O(1) latency.

    PERF: 1 MongoDB round-trip regardless of job count.
    Requires index: { job_name: 1, started_at: -1 } on ops_job_runs.
    """
    pipeline = [
        {"$sort": {"started_at": -1}},
        {"$group": {
            "_id": "$job_name",
            "status":             {"$first": "$status"},
            "started_at":         {"$first": "$started_at"},
            "finished_at":        {"$first": "$finished_at"},
            "completed_at":       {"$first": "$completed_at"},
            "duration_sec":       {"$first": "$duration_sec"},
            "duration_seconds":   {"$first": "$duration_seconds"},
            "result":             {"$first": "$result"},
            "details":            {"$first": "$details"},
            "triggered_by":       {"$first": "$triggered_by"},
            "raw_rows_total":     {"$first": "$raw_rows_total"},
            "progress":           {"$first": "$progress"},
            "progress_total":     {"$first": "$progress_total"},
            "progress_processed": {"$first": "$progress_processed"},
            "progress_pct":       {"$first": "$progress_pct"},
        }},
    ]
    docs = await db.ops_job_runs.aggregate(pipeline).to_list(None)

    def _to_iso_utc(dt) -> Optional[str]:
        if dt is None:
            return None
        if not hasattr(dt, "isoformat"):
            return str(dt)
        if getattr(dt, "tzinfo", None) is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.isoformat()

    last_runs = {}
    for doc in docs:
        job_name = doc.get("_id")
        if not job_name:
            continue
        started = doc.get("started_at")
        finished = doc.get("finished_at") or doc.get("completed_at")
        result = doc.get("result") or doc.get("details") or {}
        doc["start_time"] = _to_iso_utc(started)
        doc["end_time"] = _to_iso_utc(finished)
        doc["duration_seconds"] = doc.get("duration_sec") or doc.get("duration_seconds")
        doc["records_processed"] = (
            result.get("tickers_with_price_data") or
            result.get("tickers_processed") or
            result.get("added_pending") or
            result.get("processed") or
            result.get("records_upserted") or 0
        )
        doc["error_message"] = result.get("error") if doc.get("status") == "failed" else None
        # Raw symbols fetched from EODHD before filtering (universe_seed only).
        # Prefer the top-level raw_rows_total written by raw_total_callback during
        # a running job; fall back to result.raw_rows_total (details sub-doc) or
        # result.fetched from a completed run.
        doc["raw_symbols_fetched"] = (
            doc.get("raw_rows_total") or        # top-level field set early during run
            result.get("raw_rows_total") or     # details sub-doc (also set early)
            result.get("fetched") or            # completed result fallback
            None
        )
        # Canonical Step 1 filtered_out = deduped exclusion rows written (universe_seed only)
        doc["filtered_out_total_step1"] = result.get("filtered_out_total_step1") or None
        # Per-exchange raw counts before distinct deduplication (universe_seed only)
        doc["fetched_raw_per_exchange"] = result.get("fetched_raw_per_exchange") or None
        # Seeded total from result or details — used by frontend for seededFromRun.
        details = doc.get("details") or {}
        if isinstance(details, dict):
            _seeded = result.get("seeded_total")
            if _seeded is None:
                _seeded = details.get("seeded_total")
            doc["seeded_total"] = _seeded
        last_runs[job_name] = doc

    return last_runs

async def compute_system_health(db) -> Dict[str, Any]:
    """
    Compute system health from ops_job_runs (last 24h).
    PERF: Reads from ops_job_runs — same collection already fetched by get_admin_overview.
    No longer reads system_job_logs (separate collection, likely empty).
    """
    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)

    recent_runs = await db.ops_job_runs.find(
        {
            "started_at": {"$gte": cutoff},
            "job_name": {"$ne": "scheduler_heartbeat"}
        },
        {"_id": 0, "job_name": 1, "status": 1, "result": 1}
    ).sort("started_at", -1).to_list(50)

    if not recent_runs:
        return {
            "status": "Unknown",
            "message": "No job runs in last 24h",
            "failed_jobs": [],
            "successful_jobs": 0,
            "total_runs": 0
        }

    job_latest: Dict[str, Any] = {}
    for run in recent_runs:
        jn = run.get("job_name", "")
        if jn and jn not in job_latest:
            job_latest[jn] = run

    failed_jobs = []
    successful_count = 0
    for jn, run in job_latest.items():
        st = run.get("status", "")
        if st in ("failed", "error"):
            err = (run.get("result") or {}).get("error", "Unknown error")
            failed_jobs.append({"job_name": jn, "error": str(err)[:100]})
        elif st in ("completed", "success"):
            successful_count += 1

    if failed_jobs:
        return {
            "status": "Degraded",
            "message": f"{len(failed_jobs)} job(s) failed",
            "failed_jobs": failed_jobs,
            "successful_jobs": successful_count,
            "total_runs": len(recent_runs)
        }

    return {
        "status": "Healthy",
        "message": f"All {successful_count} jobs succeeded",
        "failed_jobs": [],
        "successful_jobs": successful_count,
        "total_runs": len(recent_runs)
    }

# A4: Environment info for Admin Panel
def get_env_info() -> Dict[str, str]:
    """Get environment info for Admin Panel display."""
    mongo_url = os.environ.get('MONGO_URL', 'unknown')
    # Extract host from URL
    try:
        if '@' in mongo_url:
            host_part = mongo_url.split('@')[-1]
        else:
            host_part = mongo_url.replace('mongodb://', '')
        db_host = host_part.split('/')[0]
    except Exception:
        db_host = 'unknown'
    
    return {
        "environment": os.environ.get('ENV', 'development'),
        "db_name": os.environ.get('DB_NAME', 'unknown'),
        "db_host": db_host,
    }

# =============================================================================
# JOB REGISTRY - Single source of truth from SCHEDULER_JOBS.md
# =============================================================================
JOB_REGISTRY = {
    "universe_seed": {
        "hour": 23, "minute": 0, "sunday_only": False, "has_api_calls": True,
        "api_endpoint": "https://eodhd.com/api/exchange-symbol-list/NYSE + /NASDAQ",
    },
    "price_sync": {
        "hour": 4, "minute": 0, "sunday_only": False, "has_api_calls": True,
        "dependency_on": "universe_seed",
        "api_endpoint": "https://eodhd.com/api/eod/{SYMBOL}.US?period=d&fmt=json"
    },
    "sp500tr_update": {
        "hour": 4, "minute": 15, "sunday_only": False, "has_api_calls": True,
        "api_endpoint": "https://eodhd.com/api/eod/SP500TR.INDX?period=d&fmt=json"
    },
    "fundamentals_sync": {
        "hour": 4, "minute": 30, "sunday_only": False, "has_api_calls": True,
        "dependency_on": "price_sync",
        "api_endpoint": "https://eodhd.com/api/fundamentals/{SYMBOL}.US?fmt=json"
    },
    "backfill_gaps": {
        "hour": 4, "minute": 45, "sunday_only": False, "has_api_calls": True,
        "api_endpoint": "https://eodhd.com/api/eod/{SYMBOL}.US?from={date}&fmt=json"
    },
    "backfill_all": {
        "hour": 5, "minute": 0, "sunday_only": False, "has_api_calls": True,
        "api_endpoint": "https://eodhd.com/api/eod/{SYMBOL}.US?from={date}&fmt=json",
        "schedule_type": "manual",  # Manual-only - Run Now button in Admin Panel
        "description": "Full price history backfill - use sparingly (API credits)",
    },
    # C1: Manual-only fundamentals backfill
    "backfill_fundamentals_complete": {
        "hour": 0, "minute": 0, "sunday_only": False, "has_api_calls": True,
        "api_endpoint": "https://eodhd.com/api/fundamentals/{SYMBOL}.US?fmt=json",
        "schedule_type": "manual",  # Manual-only - ~60K API credits for full run
        "description": "Full fundamentals backfill - ~60K API credits",
    },
    "key_metrics": {
        "hour": 5, "minute": 0, "sunday_only": False, "has_api_calls": False,
        "api_endpoint": None  # Internal calculation
    },
    "pain_cache": {
        "hour": 5, "minute": 0, "sunday_only": False, "has_api_calls": False,
        "api_endpoint": None  # Internal calculation
    },
    "peer_medians": {
        "hour": 5, "minute": 30, "sunday_only": False, "has_api_calls": False,
        "api_endpoint": None  # Internal calculation
    },
    "admin_report": {
        "hour": 6, "minute": 0, "sunday_only": False, "has_api_calls": False,
        "api_endpoint": None  # Internal generation
    },
    "news_refresh": {
        "hour": 13, "minute": 0, "sunday_only": False, "has_api_calls": True,
        "api_endpoint": "https://eodhd.com/api/news?s={SYMBOL}.US&from={from_date}&to={to_date}&limit=10&offset=0&fmt=json",
        "note": "10 articles per ticker fetched, stored up to 100 per ticker for detail view",
    },
}

JOB_PATTERNS = {
    "universe_seed": ["universe_seed", "whitelist_seed", "sync_ticker_whitelist"],
    "news_refresh": ["news_refresh", "news_daily_refresh", "news_sync"],
    "price_sync": ["daily_price_sync", "scheduled_price_sync", "price_sync"],
    "sp500tr_update": ["sp500tr_update", "sp500tr_sync", "sp500tr"],
    "fundamentals_sync": ["scheduled_fundamentals_sync", "fundamentals_sync", "fundamentals_batch"],
    "backfill_gaps": ["backfill_gaps", "scheduled_backfill_gaps"],
    "backfill_all": ["backfill_all", "scheduled_backfill_all", "parallel_backfill"],
    "key_metrics": ["key_metrics", "metrics_calc"],
    "peer_medians": ["peer_medians", "medians_calc"],
    "pain_cache": ["pain_cache", "pain_refresh"],
    "admin_report": ["admin_report"],
}


def to_prague_str(dt) -> Optional[str]:
    if dt is None:
        return None
    try:
        if hasattr(dt, 'astimezone'):
            if getattr(dt, "tzinfo", None) is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(PRAGUE_TZ).strftime("%Y-%m-%d %H:%M:%S")
        return str(dt)
    except Exception:
        return str(dt)


def get_next_run_datetime(job_name: str, now_prague: datetime) -> datetime:
    """Calculate next run datetime for a job in Prague timezone."""
    reg = JOB_REGISTRY.get(job_name)
    if not reg:
        return now_prague + timedelta(days=365)  # Far future for unknown jobs
    
    is_sunday_only = reg.get("sunday_only", False)
    sched_hour = reg["hour"]
    sched_minute = reg["minute"]
    
    # Start with today at scheduled time
    next_run = now_prague.replace(hour=sched_hour, minute=sched_minute, second=0, microsecond=0)
    
    # If already past today's scheduled time, move to tomorrow
    if now_prague >= next_run:
        next_run += timedelta(days=1)
    
    # If Sunday-only, find next Sunday
    if is_sunday_only:
        while next_run.weekday() != 6:  # 6 = Sunday
            next_run += timedelta(days=1)
    else:
        # Skip Sunday for non-Sunday jobs
        if next_run.weekday() == 6:
            next_run += timedelta(days=1)
    
    return next_run


async def get_admin_overview(db) -> Dict[str, Any]:
    """
    Single aggregated response for Admin Panel.
    P48: Single source of truth, Talk-aligned funnel.
    """
    start_time = time.perf_counter()
    
    now_utc = datetime.now(timezone.utc)
    now_prague = datetime.now(PRAGUE_TZ)
    day_of_week = now_prague.weekday()
    current_hour = now_prague.hour
    current_minute = now_prague.minute
    
    today_start_prague = now_prague.replace(hour=0, minute=0, second=0, microsecond=0)
    today_start_utc = today_start_prague.astimezone(timezone.utc)
    
    # =========================================================================
    # PARALLEL QUERIES
    # =========================================================================
    
    # Universe counts (shared with Talk)
    universe_counts_task = get_universe_counts(db)
    
    # Scheduler config
    scheduler_config_task = db.ops_config.find_one({"key": "scheduler_enabled"}, {"_id": 0})
    
    # Latest price
    latest_price_task = db.stock_prices.find_one({}, {"date": 1, "_id": 0}, sort=[("date", -1)])
    
    # Today's job runs
    job_runs_task = db.ops_job_runs.find(
        {"started_at": {"$gte": today_start_utc}},
        {"_id": 0}
    ).sort("started_at", -1).to_list(200)
    
    # Last universe seed (any time) — exact match, no regex full-scan
    last_universe_seed_task = db.ops_job_runs.find_one(
        {"job_name": "universe_seed"},
        {"_id": 0},
        sort=[("started_at", -1)]
    )
    
    # LAYER 6: API Call Guard result from startup
    api_guard_task = db.ops_audit_runs.find_one(
        {"audit_type": "api_call_guard"},
        {"_id": 0}
    )
    
    # NEW: Job last runs from system_job_logs (observability layer)
    job_last_runs_task = get_job_last_runs(db)

    # NEW: System health from system_job_logs
    system_health_task = compute_system_health(db)

    # Pipeline sync status (price history + fundamentals completion + credits)
    pipeline_sync_task = get_pipeline_sync_status(db)

    results = await asyncio.gather(
        universe_counts_task, scheduler_config_task, latest_price_task,
        job_runs_task, last_universe_seed_task, api_guard_task, job_last_runs_task,
        system_health_task, pipeline_sync_task
    )

    universe_data, scheduler_config, latest_price, job_runs, last_universe_seed, api_guard_result, job_last_runs, system_health, pipeline_sync_status = results
    
    scheduler_enabled = scheduler_config.get("value", True) if scheduler_config else True
    latest_date = latest_price.get("date") if latest_price else None
    
    # =========================================================================
    # PROCESS JOBS
    # =========================================================================
    
    def is_expected_by_now(job_name: str) -> bool:
        reg = JOB_REGISTRY.get(job_name)
        if not reg:
            return False
        dependency_job = reg.get("dependency_on")
        if dependency_job:
            # Dependency jobs become expected only after predecessor runs today.
            return find_matching_run(dependency_job) is not None
        if reg.get("sunday_only") and day_of_week != 6:
            return False
        if current_hour > reg["hour"]:
            return True
        if current_hour == reg["hour"] and current_minute >= reg["minute"]:
            return True
        return False
    
    def find_matching_run(job_name: str) -> Optional[Dict]:
        patterns = JOB_PATTERNS.get(job_name, [job_name])
        for run in job_runs:
            run_name = (run.get("job_name") or run.get("job_type") or "").lower()
            for pattern in patterns:
                if pattern.lower() in run_name:
                    return run
        return None
    
    # Count job runs today
    has_any_runs_today = len(job_runs) > 0
    
    jobs_list = []
    completed_count = 0
    failed_count = 0
    overdue_jobs = []
    api_calls_total = 0
    api_breakdown = {}
    
    for job_name, reg in JOB_REGISTRY.items():
        is_sunday_only = reg.get("sunday_only", False)
        is_manual = reg.get("schedule_type") == "manual" or reg.get("is_manual") is True
        dependency_job = reg.get("dependency_on")
        expected = is_expected_by_now(job_name) if not is_manual else False  # Manual jobs never "expected"
        dependency_text = None
        if dependency_job == "universe_seed":
            dependency_text = "After Step 1 completion"
        elif dependency_job == "price_sync":
            dependency_text = "After Step 2 completion"
        else:
            dependency_text = f"After {dependency_job}" if dependency_job else None

        sched_time = "Manual" if is_manual else (dependency_text or f"{reg['hour']:02d}:{reg['minute']:02d}")
        next_run = None if dependency_job else get_next_run_datetime(job_name, now_prague)
        
        matching_run = find_matching_run(job_name)
        
        job_data = {
            "name": job_name,
            "scheduled_time": sched_time,
            "next_run": "On demand" if is_manual else (dependency_text or next_run.strftime("%Y-%m-%d %H:%M")),
            "next_run_iso": next_run.isoformat() if (next_run and not is_manual) else None,
            "has_api_calls": reg.get("has_api_calls", False),
            "sunday_only": is_sunday_only,
            "dependency_on": dependency_job,
            "api_endpoint": reg.get("api_endpoint"),  # P53: API endpoint template
            "schedule_type": reg.get("schedule_type", "auto"),  # "auto" | "manual"
            "is_manual": is_manual,
        }
        
        if matching_run:
            status_raw = matching_run.get("status", "unknown")
            if status_raw in ["completed", "success"]:
                job_data["status"] = "success"
                completed_count += 1
            elif status_raw in ["failed", "error"]:
                job_data["status"] = "error"
                failed_count += 1
            else:
                job_data["status"] = status_raw
            
            details = matching_run.get("details", {})
            started_at = matching_run.get("started_at")
            finished_at = matching_run.get("finished_at")
            
            # API calls - ONLY if explicitly logged
            api_calls = details.get("api_calls_used") or details.get("api_calls") or matching_run.get("api_calls")
            if api_calls is not None and isinstance(api_calls, int):
                job_data["api_calls"] = api_calls
                api_calls_total += api_calls
                if job_name not in api_breakdown:
                    api_breakdown[job_name] = 0
                api_breakdown[job_name] += api_calls
            elif reg.get("has_api_calls"):
                job_data["api_calls"] = "unknown"  # Not 0!
            
            # Duration
            if started_at and finished_at:
                try:
                    job_data["duration_ms"] = int((finished_at - started_at).total_seconds() * 1000)
                except Exception:
                    pass
            
            # Records
            records = (
                details.get("tickers_with_price_data") or
                details.get("tickers_processed") or
                details.get("processed") or
                details.get("records_upserted") or
                details.get("total_tickers")
            )
            if records:
                job_data["records_updated"] = records
            
            job_data["ran_today"] = True
            job_data["expected_by_now"] = expected
            job_data["last_run_started"] = to_prague_str(started_at)
            job_data["last_run_finished"] = to_prague_str(finished_at)
            
            if job_data["status"] == "error":
                job_data["error_summary"] = str(details.get("error") or details.get("errors") or "Unknown")[:200]
        else:
            # Not run today
            job_data["ran_today"] = False
            job_data["expected_by_now"] = expected
            
            if is_sunday_only and day_of_week != 6:
                job_data["status"] = "not_scheduled"
                job_data["message"] = "Sunday only"
            elif expected:
                job_data["status"] = "overdue"
                job_data["message"] = "Overdue"
                overdue_jobs.append(job_name)
            else:
                job_data["status"] = "pending"
                job_data["message"] = "Pending"
        
        jobs_list.append(job_data)
    
    # Sort jobs by next_run ascending (manual jobs last with "9999")
    jobs_list.sort(key=lambda x: x.get("next_run_iso") or "9999-12-31T23:59:59")
    
    # Total expected jobs today (exclude manual jobs)
    total_expected = sum(1 for j, r in JOB_REGISTRY.items() 
                        if r.get("schedule_type") != "manual" and
                        (not r.get("sunday_only") or day_of_week == 6))
    
    # =========================================================================
    # HEALTH SCORE
    # P48: If no runs today, show "Unknown" (not "10% Critical")
    # =========================================================================
    
    if not has_any_runs_today:
        health_score = None
        health_status = "Unknown"
        health_deductions = ["No job runs recorded today - cannot compute health score"]
    else:
        health_score = 100
        health_deductions = []
        
        for job in overdue_jobs:
            health_score -= 10
            health_deductions.append(f"-10: {job} overdue")
        
        for job in jobs_list:
            if job.get("status") == "error":
                health_score -= 25
                health_deductions.append(f"-25: {job['name']} failed")
        
        health_score = max(0, min(100, health_score))
        
        if health_score >= 80:
            health_status = "Good"
        elif health_score >= 50:
            health_status = "Warning"
        else:
            health_status = "Critical"
    
    # =========================================================================
    # BUILD RESPONSE
    # =========================================================================
    
    end_time = time.perf_counter()
    load_time_ms = round((end_time - start_time) * 1000)
    
    # Categorize jobs
    jobs_overdue = [j for j in jobs_list if j.get("status") == "overdue"]
    jobs_failed = [j for j in jobs_list if j.get("status") == "error"]
    jobs_completed = [j for j in jobs_list if j.get("status") == "success"]
    jobs_pending = [j for j in jobs_list if j.get("status") == "pending"]
    jobs_not_scheduled = [j for j in jobs_list if j.get("status") == "not_scheduled"]
    
    # A4: Get environment info for Admin Panel
    env_info = get_env_info()
    
    return {
        "generated_at": to_prague_str(now_utc),
        "load_time_ms": load_time_ms,
        "today_boundary_prague": today_start_prague.strftime("%Y-%m-%d 00:00:00"),
        
        # A4: Environment info
        "environment": env_info,
        
        # Health
        "health": {
            "score_pct": health_score,  # None if unknown
            "status": health_status,
            "deductions": health_deductions,
            "scheduler_active": scheduler_enabled,
            "jobs_completed": completed_count,
            "jobs_failed": failed_count,
            "jobs_total": total_expected,
            "overdue_count": len(overdue_jobs),
            "has_runs_today": has_any_runs_today,
            "latest_data_date": latest_date,
            # API calls - None if no data (not 0!)
            "api_calls_today": api_calls_total if api_calls_total > 0 else None,
            "api_breakdown": api_breakdown if api_breakdown else None,
            # LAYER 6: API Call Guard status
            "api_guard": {
                "passed": api_guard_result.get("passed") if api_guard_result else None,
                "last_check": to_prague_str(api_guard_result.get("ran_at")) if api_guard_result else None,
                "status": "OK" if (api_guard_result and api_guard_result.get("passed")) else ("FAIL" if api_guard_result else "Unknown"),
            },
        },
        
        # Jobs (sorted by next run)
        "jobs": {
            "registry_count": len(JOB_REGISTRY),
            "all_sorted": jobs_list,  # Sorted by next_run
            "overdue": jobs_overdue,
            "failed": jobs_failed,
            "completed": jobs_completed,
            "pending": jobs_pending,
            "not_scheduled": jobs_not_scheduled,
        },
        
        # Universe Funnel (from shared service)
        "universe_funnel": universe_data,
        
        # NEW: Job last runs from system_job_logs (observability layer)
        "job_last_runs": job_last_runs,
        
        # NEW: System health computed from system_job_logs (last 24h)
        "system_health": system_health,

        # Pipeline sync status: price history + fundamentals completion + credit usage
        "pipeline_sync_status": pipeline_sync_status,

        # For Talk compatibility
        "visible_universe_count": universe_data["visible_universe_count"],
    }
