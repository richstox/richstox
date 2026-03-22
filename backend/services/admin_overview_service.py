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
import logging
import os
import time
from datetime import date, datetime, timedelta, timezone
from typing import Dict, Any, List, Optional
from zoneinfo import ZoneInfo

from services.universe_counts_service import get_universe_counts
from credit_log_service import get_pipeline_sync_status

logger = logging.getLogger(__name__)

PRAGUE_TZ = ZoneInfo("Europe/Prague")


def _to_prague_iso(dt) -> Optional[str]:
    if dt is None:
        return None
    if not hasattr(dt, "astimezone"):
        return str(dt)
    if getattr(dt, "tzinfo", None) is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(PRAGUE_TZ).isoformat()


def _empty_step3_phase(name: str) -> Dict[str, Any]:
    return {
        "name": name,
        "status": "idle",
        "processed": 0,
        "total": None,
        "pct": None,
        "message": None,
    }


def _default_step3_telemetry_response() -> Dict[str, Any]:
    return {
        "job_name": "fundamentals_sync",
        "run_id": None,
        "status": "idle",
        "started_at_prague": None,
        "updated_at_prague": None,
        "pending_refresh_flags": 0,
        "pending_events_audit": 0,
        "phases": {
            "A": _empty_step3_phase("Fundamentals"),
            "B": _empty_step3_phase("Visibility"),
            "C": _empty_step3_phase("PriceHistory"),
        },
    }


def _normalize_run_status(raw_status: Optional[str]) -> str:
    if raw_status == "failed":
        return "error"
    allowed = {"idle", "running", "success", "completed", "cancelled", "error"}
    return raw_status if raw_status in allowed else "idle"


def _sanitize_phase_payload(raw: Any, *, fallback_name: str) -> Dict[str, Any]:
    phase = _empty_step3_phase(fallback_name)
    if not isinstance(raw, dict):
        return phase
    status = raw.get("status")
    if status in {"idle", "running", "done", "error"}:
        phase["status"] = status
    processed = raw.get("processed")
    if isinstance(processed, (int, float)):
        phase["processed"] = int(processed)
    total = raw.get("total")
    if isinstance(total, (int, float)):
        phase["total"] = int(total)
    elif total is None:
        phase["total"] = None
    pct = raw.get("pct")
    if isinstance(pct, (int, float)):
        phase["pct"] = float(pct)
    elif pct is None:
        phase["pct"] = None
    message = raw.get("message")
    if isinstance(message, str):
        phase["message"] = message.splitlines()[0]
    elif message is None:
        phase["message"] = None
    return phase


async def get_step3_live_telemetry(db) -> Dict[str, Any]:
    response = _default_step3_telemetry_response()
    pending_refresh_flags, pending_events_audit = await asyncio.gather(
        db.tracked_tickers.count_documents({"needs_fundamentals_refresh": True}),
        db.fundamentals_events.count_documents({"status": "pending"}),
    )
    response["pending_refresh_flags"] = pending_refresh_flags
    response["pending_events_audit"] = pending_events_audit

    projection = {
        "_id": 1,
        "status": 1,
        "started_at": 1,
        "finished_at": 1,
        "started_at_prague": 1,
        "finished_at_prague": 1,
        "details.step3_telemetry": 1,
    }
    run = await db.ops_job_runs.find_one(
        {"job_name": "fundamentals_sync", "status": "running"},
        projection,
        sort=[("started_at", -1)],
    )
    if run is None:
        run = await db.ops_job_runs.find_one(
            {"job_name": "fundamentals_sync", "status": {"$in": ["success", "completed", "cancelled", "error", "failed"]}},
            projection,
            sort=[("started_at", -1)],
        )
    if run is None:
        return response

    details = run.get("details") or {}
    telemetry = details.get("step3_telemetry") if isinstance(details, dict) else {}
    raw_phases = telemetry.get("phases") if isinstance(telemetry, dict) else {}

    response["run_id"] = str(run.get("_id")) if run.get("_id") is not None else None
    response["status"] = _normalize_run_status(run.get("status"))
    response["started_at_prague"] = run.get("started_at_prague") or _to_prague_iso(run.get("started_at"))
    response["updated_at_prague"] = (
        telemetry.get("updated_at_prague") if isinstance(telemetry, dict) else None
    ) or run.get("finished_at_prague") or _to_prague_iso(run.get("finished_at")) or response["started_at_prague"]
    response["phases"] = {
        "A": _sanitize_phase_payload((raw_phases or {}).get("A"), fallback_name="Fundamentals"),
        "B": _sanitize_phase_payload((raw_phases or {}).get("B"), fallback_name="Visibility"),
        "C": _sanitize_phase_payload((raw_phases or {}).get("C"), fallback_name="PriceHistory"),
    }
    return response


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


# =============================================================================
# PRICE INTEGRITY METRICS
# =============================================================================

# ── Heuristic depth thresholds (secondary/informational) ───────────────────
# full_price_history = True iff:
#   row_count >= FULL_HISTORY_MIN_ROWS (approx 1 year of US trading days)
#   AND min_date <= today - FULL_HISTORY_MIN_DAYS
# Kept as a lightweight depth indicator; NOT the canonical truth model.
FULL_HISTORY_MIN_ROWS = 252
FULL_HISTORY_MIN_DAYS = 365

# ── Canonical process-truth model ──────────────────────────────────────────
# The canonical truth for "price-complete" is:
#
#   history_download_completed = True
#     Evidence: tracked_tickers.history_download_proven_at IS NOT NULL
#     Set by: full_sync_service.py (after successful full historical download)
#             scheduler_service.py (after successful split/dividend remediation)
#     This is a STRICT proof marker: legacy price_history_complete alone is
#     NOT sufficient.  The system must have explicitly completed a full
#     price download (or re-download) and recorded the proof marker.
#
#   history_download_completed_at = tracked_tickers.history_download_proven_anchor
#     A date string (same type as stock_prices.date, "YYYY-MM-DD") representing
#     the latest price date covered by the historical download.
#     This is the CONTINUITY ANCHOR: bulk dates after this anchor must exist.
#     A ticker with history_download_proven_at but NULL anchor is treated as
#     NOT completed (anchor is required for gap-checking).
#
#   missing_bulk_dates_since_history_download = count of canonical successful
#     bulk processed dates D where D > anchor AND ticker has no price row on D.
#     Canonical bulk dates source: _get_bulk_processed_dates() from
#     ops_job_runs.details.price_bulk_gapfill.days[].processed_date
#     where status == "success".
#
#   gap_free_since_history_download = True iff:
#     history_download_completed == True
#     AND missing_bulk_dates_since_history_download == 0
#
# This is STRONGER than the old heuristic model because:
# 1. It requires an EXPLICIT proof marker (not legacy operational flags)
# 2. It requires ZERO gaps against canonical bulk ingestion truth (not depth)
# 3. Existing tickers without the proof marker are NOT completed and must
#    enter the full history download flow.

_SAFE_INTEGRITY = {
    "today_visible": 0,
    "today_visible_source": None,
    "last_bulk_trading_date": None,
    "needs_price_redownload": 0,
    "price_history_incomplete": 0,
    "full_price_history_count": 0,
    "history_download_completed_count": 0,
    "gap_free_since_history_download_count": 0,
    "missing_expected_dates": 0,
    "coverage_checkpoints": {},
}

_CHECKPOINT_KIND = {
    "latest_trading_day": "recent",
    "1_week_ago": "recent",
    "1_month_ago": "historical",
    "1_year_ago": "historical",
}


async def _find_nearest_price_date(db, target_date_str: str) -> Optional[str]:
    """Find the nearest date <= target_date_str that has price data."""
    doc = await db.stock_prices.find_one(
        {"date": {"$lte": target_date_str}},
        {"date": 1, "_id": 0},
        sort=[("date", -1)],
    )
    return doc["date"] if doc else None


async def _resolve_today_visible(db) -> tuple:
    """
    Derive TODAY_VISIBLE from the latest successful main pipeline report.

    Returns (visible_tickers: List[str], today_visible: int, source: dict).

    Strategy:
      1. Find the latest *completed* pipeline_chain_runs document.
      2. Use its chain_run_id + finished_at as the stable snapshot reference.
      3. Read the actual visible ticker list using the same query the
         canonical report uses (is_visible=True on tracked_tickers) — this is
         identical to build_canonical_pipeline_report's "visible" query and
         remains stable until the next pipeline run mutates is_visible flags.
      4. Expose provenance so the dashboard can display which run the count
         comes from.

    Fallback: if no completed chain exists, use live is_visible=True (same
    result but with source.chain_run_id = None).
    """
    chain_doc = await db.pipeline_chain_runs.find_one(
        {"status": "completed"},
        {"chain_run_id": 1, "finished_at": 1, "_id": 0},
        sort=[("finished_at", -1)],
    )

    chain_run_id: Optional[str] = None
    generated_at_prague: Optional[str] = None

    if chain_doc:
        chain_run_id = chain_doc.get("chain_run_id")
        finished = chain_doc.get("finished_at")
        if finished is not None:
            if getattr(finished, "tzinfo", None) is None:
                finished = finished.replace(tzinfo=timezone.utc)
            generated_at_prague = finished.astimezone(PRAGUE_TZ).isoformat()

    # The visible ticker list is the snapshot produced by the last pipeline
    # run (Step 3 sets is_visible flags).  Between pipeline runs this set
    # does NOT change, so it is inherently stable.
    visible_tickers: List[str] = await db.tracked_tickers.distinct(
        "ticker", {"is_visible": True}
    )

    source = {
        "chain_run_id": chain_run_id,
        "generated_at_prague": generated_at_prague,
    }

    return visible_tickers, len(visible_tickers), source


async def _get_bulk_processed_dates(db) -> List[str]:
    """
    Return the set of dates for which bulk ingestion succeeded, derived from
    ops_job_runs.details.price_bulk_gapfill.days[] entries with status=success.

    This is the *canonical* source of expected_dates — no rolling-date
    heuristic or calendar approximation.
    """
    pipeline = [
        {"$match": {
            "job_name": "price_sync",
            "status": {"$in": ["success", "completed"]},
            "details.price_bulk_gapfill.days": {"$exists": True},
        }},
        {"$sort": {"finished_at": -1}},
        # Limit to the most recent successful runs to bound the query.
        # MAX_BULK_GAPFILL_DAYS_HISTORY in scheduler_service is 60;
        # 10 runs is generous (each run can gap-fill multiple days).
        {"$limit": 10},
        {"$project": {"_id": 0, "details.price_bulk_gapfill.days": 1}},
    ]

    dates: set = set()
    async for doc in db.ops_job_runs.aggregate(pipeline):
        details = doc.get("details") or {}
        gapfill = details.get("price_bulk_gapfill") or {}
        for day in gapfill.get("days", []):
            if day.get("status") == "success" and day.get("processed_date"):
                dates.add(day["processed_date"])

    return sorted(dates)


async def get_price_integrity_metrics(db) -> Dict[str, Any]:
    """
    Price integrity / coverage metrics for the admin dashboard.

    TODAY_VISIBLE = visible tickers from the latest successful main pipeline
                    report (stable snapshot tied to a chain_run_id).
    expected_dates = dates for which we successfully processed an EODHD daily
                     bulk payload (from ops_job_runs bulk gapfill history).
    A gap = an expected date D where have_price_count(D, TODAY_VISIBLE) < TODAY_VISIBLE.

    Coverage checkpoints resolve the nearest available trading date at or before
    the target calendar date (latest bulk date, 1 week, 1 month, 1 year ago)
    and report how many of TODAY_VISIBLE tickers have a price record on that date.
    """
    try:
        today_str = date.today().isoformat()

        # ── 1. TODAY_VISIBLE (stable, from latest completed pipeline run) ───
        visible_tickers, today_visible, today_visible_source = (
            await _resolve_today_visible(db)
        )

        if today_visible == 0:
            return {**_SAFE_INTEGRITY, "today_visible_source": today_visible_source}

        # ── 2. Last bulk trading date ───────────────────────────────────────
        bulk_state = await db.pipeline_state.find_one({"_id": "price_bulk"})
        last_bulk_date: Optional[str] = (
            (bulk_state or {}).get("global_last_bulk_date_processed")
        )

        # ── 3. Ticker-level flags (visible only) ───────────────────────────
        flag_facet = await db.tracked_tickers.aggregate([
            {"$match": {"is_visible": True}},
            {"$facet": {
                "needs_redownload": [
                    {"$match": {"needs_price_redownload": True}},
                    {"$count": "n"},
                ],
                "incomplete_history": [
                    {"$match": {"price_history_complete": {"$ne": True}}},
                    {"$count": "n"},
                ],
                "full_price_history": [
                    {"$match": {"full_price_history": True}},
                    {"$count": "n"},
                ],
                "history_download_completed": [
                    {"$match": {"history_download_completed": True}},
                    {"$count": "n"},
                ],
                "gap_free_since_history_download": [
                    {"$match": {"gap_free_since_history_download": True}},
                    {"$count": "n"},
                ],
            }},
        ]).to_list(1)
        ff = flag_facet[0] if flag_facet else {}

        def _n(key: str) -> int:
            return (ff.get(key) or [{}])[0].get("n", 0)

        needs_redownload = _n("needs_redownload")
        incomplete_history = _n("incomplete_history")
        full_price_history_count = _n("full_price_history")
        history_download_completed_count = _n("history_download_completed")
        gap_free_count = _n("gap_free_since_history_download")

        # ── 4. Coverage checkpoints ─────────────────────────────────────────
        target_offsets = {
            "latest_trading_day": last_bulk_date or today_str,
            "1_week_ago": (date.today() - timedelta(days=7)).isoformat(),
            "1_month_ago": (date.today() - timedelta(days=30)).isoformat(),
            "1_year_ago": (date.today() - timedelta(days=365)).isoformat(),
        }

        # Resolve nearest actual dates in parallel
        nearest_tasks = {
            label: _find_nearest_price_date(db, target)
            for label, target in target_offsets.items()
        }
        nearest_results = await asyncio.gather(*nearest_tasks.values())
        nearest_dates = dict(zip(nearest_tasks.keys(), nearest_results))

        # Batch-count prices for all checkpoint dates in one aggregation
        checkpoint_date_values = [d for d in nearest_dates.values() if d]
        price_counts_by_date: Dict[str, int] = {}
        if checkpoint_date_values:
            count_cursor = db.stock_prices.aggregate([
                {"$match": {
                    "date": {"$in": checkpoint_date_values},
                    "ticker": {"$in": visible_tickers},
                }},
                {"$group": {"_id": "$date", "count": {"$sum": 1}}},
            ])
            async for doc in count_cursor:
                price_counts_by_date[doc["_id"]] = doc["count"]

        checkpoints: Dict[str, Any] = {}
        for label in target_offsets:
            actual_date = nearest_dates.get(label)
            have_price = price_counts_by_date.get(actual_date, 0) if actual_date else 0
            checkpoints[label] = {
                "date": actual_date,
                "have_price_count": have_price,
                "today_visible": today_visible,
                "kind": _CHECKPOINT_KIND.get(label, "historical"),
            }

        # ── 5. Missing expected dates (from canonical bulk ingestion truth) ─
        # expected_dates = dates where bulk ingestion succeeded
        expected_dates = await _get_bulk_processed_dates(db)

        if expected_dates:
            # Count distinct expected dates where coverage is incomplete
            gap_pipeline = [
                {"$match": {
                    "date": {"$in": expected_dates},
                    "ticker": {"$in": visible_tickers},
                }},
                {"$group": {"_id": "$date", "count": {"$sum": 1}}},
                {"$match": {"count": {"$lt": today_visible}}},
                {"$count": "n"},
            ]
            gap_result = await db.stock_prices.aggregate(gap_pipeline).to_list(1)
            gap_count = gap_result[0]["n"] if gap_result else 0

            # Also count dates with zero coverage (no rows at all)
            dates_with_any_data_pipeline = [
                {"$match": {
                    "date": {"$in": expected_dates},
                    "ticker": {"$in": visible_tickers},
                }},
                {"$group": {"_id": "$date"}},
            ]
            dates_with_data: set = set()
            async for doc in db.stock_prices.aggregate(dates_with_any_data_pipeline):
                dates_with_data.add(doc["_id"])
            dates_with_zero_coverage = sum(
                1 for d in expected_dates if d not in dates_with_data
            )
            missing_expected_dates = gap_count + dates_with_zero_coverage
        else:
            missing_expected_dates = 0

        return {
            "today_visible": today_visible,
            "today_visible_source": today_visible_source,
            "last_bulk_trading_date": last_bulk_date,
            "needs_price_redownload": needs_redownload,
            "price_history_incomplete": incomplete_history,
            "full_price_history_count": full_price_history_count,
            "history_download_completed_count": history_download_completed_count,
            "gap_free_since_history_download_count": gap_free_count,
            "missing_expected_dates": missing_expected_dates,
            "coverage_checkpoints": checkpoints,
        }
    except Exception as exc:
        logger.warning("get_price_integrity_metrics failed: %s", exc)
        return {**_SAFE_INTEGRITY}


async def backfill_full_price_history(db) -> Dict[str, Any]:
    """
    Admin-only backfill: recompute process-truth price completeness fields
    for all visible tickers.

    CANONICAL TRUTH MODEL (see module-level constants/comments for full docs):

      history_download_completed = tracked_tickers.history_download_proven_at IS NOT NULL
        AND tracked_tickers.history_download_proven_anchor IS NOT NULL
        Evidence: strict proof marker written ONLY by full_sync_service.py
        after successful full historical download, and by scheduler_service.py
        after split/dividend remediation re-download.
        Legacy price_history_complete alone is NOT sufficient proof.

      history_download_completed_at = tracked_tickers.history_download_proven_anchor
        A date string ("YYYY-MM-DD"), same type as stock_prices.date.
        Represents the latest price date covered by the historical download.
        This is the CONTINUITY ANCHOR: we check for gaps after this date.

      history_download_min_date = min(stock_prices.date) for the ticker

      missing_bulk_dates_since_history_download = count of canonical
        successful bulk processed dates D where D > anchor AND ticker has
        no stock_prices row on date D.
        Canonical bulk dates: _get_bulk_processed_dates() from
        ops_job_runs.details.price_bulk_gapfill.days[].processed_date
        where status == "success".

      gap_free_since_history_download = True iff:
        history_download_completed AND missing_bulk_dates == 0

    Also preserves legacy heuristic fields (full_price_history*) for
    backward compatibility and secondary informational display.

    Idempotent: safe to re-run; same inputs always produce same outputs.
    Writes audit summary to ops_job_runs.
    """
    started_at = datetime.now(timezone.utc)
    cutoff_date = (date.today() - timedelta(days=FULL_HISTORY_MIN_DAYS)).isoformat()

    # ── 1. Get all visible tickers with remediation fields ──────────────
    visible_docs = await db.tracked_tickers.find(
        {"is_visible": True},
        {
            "ticker": 1,
            "price_history_complete": 1,
            "price_history_complete_as_of": 1,
            "history_download_proven_at": 1,
            "history_download_proven_anchor": 1,
            "_id": 0,
        },
    ).to_list(None)
    visible_tickers = [d["ticker"] for d in visible_docs]
    ticker_info = {d["ticker"]: d for d in visible_docs}
    total_visible = len(visible_tickers)

    if total_visible == 0:
        return {
            "status": "no_work",
            "total_visible_tickers": 0,
            "history_download_completed_count": 0,
            "gap_free_since_history_download_count": 0,
            "tickers_with_missing_bulk_dates_count": 0,
            "total_missing_bulk_ticker_date_pairs": 0,
            "full_history_heuristic_count": 0,
        }

    # ── 2. Aggregate min_date and row_count per ticker from stock_prices ─
    agg_pipeline = [
        {"$match": {"ticker": {"$in": visible_tickers}}},
        {"$group": {
            "_id": "$ticker",
            "min_date": {"$min": "$date"},
            "row_count": {"$sum": 1},
        }},
    ]
    stats_by_ticker: Dict[str, Dict[str, Any]] = {}
    async for doc in db.stock_prices.aggregate(agg_pipeline):
        stats_by_ticker[doc["_id"]] = {
            "min_date": doc["min_date"],
            "row_count": doc["row_count"],
        }

    # ── 3. Get canonical bulk processed dates ────────────────────────────
    bulk_dates = await _get_bulk_processed_dates(db)
    bulk_dates_set = set(bulk_dates)

    # ── 4. Get existing bulk-date coverage per ticker (one query) ────────
    dates_by_ticker: Dict[str, set] = {}
    if bulk_dates:
        dates_pipeline = [
            {"$match": {
                "ticker": {"$in": visible_tickers},
                "date": {"$in": bulk_dates},
            }},
            {"$group": {"_id": "$ticker", "dates": {"$addToSet": "$date"}}},
        ]
        async for doc in db.stock_prices.aggregate(dates_pipeline):
            dates_by_ticker[doc["_id"]] = set(doc["dates"])

    # ── 5. Compute and write fields per ticker ───────────────────────────
    now = datetime.now(timezone.utc)
    history_completed_count = 0
    gap_free_count = 0
    tickers_with_gaps_count = 0
    total_missing_pairs = 0
    heuristic_full_count = 0
    no_data_count = 0
    sample_gap_free: List[str] = []
    sample_with_gaps: List[str] = []
    sample_no_history_download: List[str] = []

    for ticker in visible_tickers:
        info = ticker_info.get(ticker, {})
        stats = stats_by_ticker.get(ticker)
        min_date_val = stats["min_date"] if stats else None
        row_count = stats["row_count"] if stats else 0

        # Strict proof: history download completed only if explicit proof marker
        # AND anchor exist.  Legacy price_history_complete alone is NOT sufficient.
        has_proof = info.get("history_download_proven_at") is not None
        anchor_date = info.get("history_download_proven_anchor") if has_proof else None
        history_completed = has_proof and anchor_date is not None

        # Missing bulk dates since anchor
        missing_count = 0
        if history_completed and anchor_date:
            relevant_bulk = [d for d in bulk_dates if d > anchor_date]
            actual_dates = dates_by_ticker.get(ticker, set())
            missing_count = sum(1 for d in relevant_bulk if d not in actual_dates)

        gap_free = history_completed and missing_count == 0

        # Heuristic (legacy, kept for backward compat)
        is_full_heuristic = (
            row_count >= FULL_HISTORY_MIN_ROWS
            and min_date_val is not None
            and min_date_val <= cutoff_date
        )

        await db.tracked_tickers.update_one(
            {"ticker": ticker},
            {"$set": {
                # Process-truth fields (canonical)
                "history_download_completed": history_completed,
                "history_download_completed_at": anchor_date,
                "history_download_min_date": min_date_val,
                "missing_bulk_dates_since_history_download": missing_count,
                "gap_free_since_history_download": gap_free,
                # Legacy heuristic fields (informational)
                "full_price_history": is_full_heuristic,
                "full_price_history_verified_at": now,
                "full_price_history_min_date": min_date_val,
                "full_price_history_row_count": row_count,
            }},
        )

        # Counting
        if history_completed:
            history_completed_count += 1
            if gap_free:
                gap_free_count += 1
                if len(sample_gap_free) < 5:
                    sample_gap_free.append(ticker)
            else:
                tickers_with_gaps_count += 1
                total_missing_pairs += missing_count
                if len(sample_with_gaps) < 5:
                    sample_with_gaps.append(ticker)
        else:
            if len(sample_no_history_download) < 5:
                sample_no_history_download.append(ticker)

        if is_full_heuristic:
            heuristic_full_count += 1
        if not min_date_val:
            no_data_count += 1

    finished_at = datetime.now(timezone.utc)
    duration_s = round((finished_at - started_at).total_seconds(), 1)

    # ── 6. Write audit summary to ops_job_runs ───────────────────────────
    audit_details = {
        "total_visible_tickers": total_visible,
        "history_download_completed_count": history_completed_count,
        "gap_free_since_history_download_count": gap_free_count,
        "tickers_with_missing_bulk_dates_count": tickers_with_gaps_count,
        "total_missing_bulk_ticker_date_pairs": total_missing_pairs,
        "canonical_bulk_dates_count": len(bulk_dates),
        "no_price_data_count": no_data_count,
        "full_history_heuristic_count": heuristic_full_count,
        "heuristic_threshold": {
            "min_row_count": FULL_HISTORY_MIN_ROWS,
            "min_days": FULL_HISTORY_MIN_DAYS,
            "cutoff_date": cutoff_date,
        },
        "sample_gap_free": sample_gap_free,
        "sample_with_gaps": sample_with_gaps,
        "sample_no_history_download": sample_no_history_download,
    }
    await db.ops_job_runs.insert_one({
        "job_name": "backfill_full_price_history",
        "status": "completed",
        "source": "admin_manual",
        "triggered_by": "admin_manual",
        "started_at": started_at,
        "finished_at": finished_at,
        "duration_seconds": duration_s,
        "details": audit_details,
    })

    return {
        "status": "completed",
        "duration_seconds": duration_s,
        **audit_details,
    }


async def get_pipeline_last_success_age(db) -> Dict[str, Any]:
    """
    Return hours since last successful full pipeline run (Step 3 = fundamentals_sync)
    and last successful price_sync, for Ops Health thresholds.
    """
    try:
        now_utc = datetime.now(timezone.utc)

        pipeline_run = await db.ops_job_runs.find_one(
            {"job_name": "fundamentals_sync", "status": {"$in": ["success", "completed"]}},
            {"finished_at": 1, "_id": 0},
            sort=[("finished_at", -1)],
        )
        price_run = await db.ops_job_runs.find_one(
            {"job_name": "price_sync", "status": {"$in": ["success", "completed"]}},
            {"finished_at": 1, "_id": 0},
            sort=[("finished_at", -1)],
        )

        def _hours_since(run_doc):
            if not run_doc or not run_doc.get("finished_at"):
                return None
            finished = run_doc["finished_at"]
            if getattr(finished, "tzinfo", None) is None:
                finished = finished.replace(tzinfo=timezone.utc)
            return round((now_utc - finished).total_seconds() / 3600, 1)

        pipeline_hours = _hours_since(pipeline_run)
        price_hours = _hours_since(price_run)

        def _status(hours):
            if hours is None:
                return "unknown"
            if hours < 25:
                return "green"
            if hours <= 48:
                return "yellow"
            return "red"

        return {
            "pipeline_hours_since_success": pipeline_hours,
            "pipeline_status": _status(pipeline_hours),
            "morning_refresh_hours_since_success": price_hours,
            "morning_refresh_status": _status(price_hours),
        }
    except Exception as exc:
        logger.warning("get_pipeline_last_success_age failed: %s", exc)
        return {
            "pipeline_hours_since_success": None,
            "pipeline_status": "unknown",
            "morning_refresh_hours_since_success": None,
            "morning_refresh_status": "unknown",
        }


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

    # Price integrity metrics for dashboard coverage section
    price_integrity_task = get_price_integrity_metrics(db)

    # Pipeline / morning refresh last-success age for Ops Health thresholds
    pipeline_age_task = get_pipeline_last_success_age(db)

    results = await asyncio.gather(
        universe_counts_task, scheduler_config_task, latest_price_task,
        job_runs_task, last_universe_seed_task, api_guard_task, job_last_runs_task,
        system_health_task, pipeline_sync_task, price_integrity_task, pipeline_age_task
    )

    universe_data, scheduler_config, latest_price, job_runs, last_universe_seed, api_guard_result, job_last_runs, system_health, pipeline_sync_status, price_integrity, pipeline_age = results
    
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

        # Price integrity / coverage metrics for dashboard
        "price_integrity": price_integrity,

        # Pipeline & morning-refresh last-success age (hours) for Ops Health
        "pipeline_age": pipeline_age,

        # For Talk compatibility
        "visible_universe_count": universe_data["visible_universe_count"],
    }
