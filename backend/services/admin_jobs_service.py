from datetime import datetime, timezone
from typing import Any, Dict, Optional
from zoneinfo import ZoneInfo


VALID_CANCEL_RUNNING_JOB_NAMES = {"fundamentals_sync", "price_sync", "compute_visible_universe"}
PRAGUE_TZ = ZoneInfo("Europe/Prague")
JOB_NAME_DB_MAP = {
    "recompute_visibility_all": "compute_visible_universe",
}
JOB_MAX_RUNTIME_MINUTES = {
    "peer_medians": 20,
    "compute_visible_universe": 20,
    "dividend_upcoming_calendar": 90,
    "earnings_upcoming_calendar": 15,
    "splits_upcoming_calendar": 15,
    "ipos_upcoming_calendar": 15,
}
DEFAULT_MAX_RUNTIME_MINUTES = 120


def is_valid_cancel_running_job_name(job_name: str) -> bool:
    return job_name in VALID_CANCEL_RUNNING_JOB_NAMES


def resolve_db_job_name(api_job_name: str) -> str:
    return JOB_NAME_DB_MAP.get(api_job_name, api_job_name)


async def is_cancel_requested(db: Any, run_id: Any) -> bool:
    """Return True when the given run is marked cancel_requested."""
    run = await db.ops_job_runs.find_one({"_id": run_id}, {"status": 1})
    return bool(run and run.get("status") == "cancel_requested")


async def cancel_latest_running_job(
    db: Any,
    job_name: str,
    *,
    now: Optional[datetime] = None,
) -> Optional[Dict[str, Any]]:
    if not is_valid_cancel_running_job_name(job_name):
        raise ValueError(f"Invalid job_name: {job_name}")

    if now is None:
        now = datetime.now(timezone.utc)

    running_doc = await db.ops_job_runs.find_one(
        {"job_name": job_name, "status": "running"},
        {"_id": 1},
        sort=[("started_at", -1)],
    )
    if not running_doc:
        return None

    await db.ops_job_runs.update_one(
        {"_id": running_doc["_id"], "status": "running"},
        {"$set": {
            "status": "cancel_requested",
            "updated_at": now,
            "updated_at_prague": now.astimezone(PRAGUE_TZ).isoformat(),
        }},
    )

    return {
        "run_id": str(running_doc["_id"]),
        "status": "cancel_requested",
    }


async def recover_stale_job_run(database: Any, job_name: str) -> Optional[Dict[str, Any]]:
    """Auto-finalize a stale running job so Admin does not show ghost runs."""
    db_job_name = resolve_db_job_name(job_name)
    max_minutes = JOB_MAX_RUNTIME_MINUTES.get(
        db_job_name,
        JOB_MAX_RUNTIME_MINUTES.get(job_name, DEFAULT_MAX_RUNTIME_MINUTES),
    )

    existing = await database.ops_job_runs.find_one(
        {"job_name": db_job_name, "status": "running"},
        sort=[("started_at", -1)],
    )
    if not existing:
        return None

    def _ensure_utc(dt):
        if isinstance(dt, datetime) and dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt

    started = _ensure_utc(existing.get("started_at"))
    updated = _ensure_utc(existing.get("updated_at"))
    freshness_ref = updated or started

    try:
        stale_minutes = (
            (datetime.now(timezone.utc) - freshness_ref).total_seconds() / 60.0
            if isinstance(freshness_ref, datetime)
            else float("inf")
        )
    except Exception:
        stale_minutes = float("inf")

    if stale_minutes <= max_minutes:
        return None

    expire_at = datetime.now(timezone.utc)
    try:
        duration_seconds = (expire_at - started).total_seconds() if isinstance(started, datetime) else None
    except (TypeError, AttributeError):
        duration_seconds = None

    started_iso = started.isoformat() if isinstance(started, datetime) else str(started)
    updated_iso = updated.isoformat() if isinstance(updated, datetime) else str(updated)
    update_fields: Dict[str, Any] = {
        "status": "error",
        "error_code": "timeout",
        "finished_at": expire_at,
        "finished_at_prague": expire_at.astimezone(PRAGUE_TZ).isoformat(),
        "error_message": (
            f"Stale run auto-finalized (timeout). No heartbeat for {int(stale_minutes)} min "
            f"(threshold={max_minutes} min). started_at={started_iso}, last updated_at={updated_iso}."
        ),
        "error_traceback": "Timeout recovery by recover_stale_job_run",
        "details.timeout_recovery": True,
        "details.timeout_recovered_at": expire_at.isoformat(),
        "details.stale_auto_finalized": True,
    }
    if duration_seconds is not None:
        update_fields["duration_seconds"] = round(duration_seconds, 1)

    await database.ops_job_runs.update_one(
        {"_id": existing["_id"]},
        {"$set": update_fields},
    )
    existing.update(update_fields)
    existing["recovered"] = True
    return existing
