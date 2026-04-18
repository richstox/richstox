from datetime import datetime, timezone
from typing import Any, Dict, Optional
from zoneinfo import ZoneInfo


VALID_CANCEL_RUNNING_JOB_NAMES = {"fundamentals_sync", "price_sync", "compute_visible_universe"}
PRAGUE_TZ = ZoneInfo("Europe/Prague")


def is_valid_cancel_running_job_name(job_name: str) -> bool:
    return job_name in VALID_CANCEL_RUNNING_JOB_NAMES


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
