from datetime import datetime, timezone
from typing import Any, Dict, Optional
from zoneinfo import ZoneInfo


VALID_CANCEL_RUNNING_JOB_NAMES = {"fundamentals_sync", "price_sync"}
PRAGUE_TZ = ZoneInfo("Europe/Prague")


def is_valid_cancel_running_job_name(job_name: str) -> bool:
    return job_name in VALID_CANCEL_RUNNING_JOB_NAMES


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
            "status": "cancelled",
            "cancelled_at": now,
            "finished_at": now,
            "finished_at_prague": now.astimezone(PRAGUE_TZ).isoformat(),
            "log_timezone": "Europe/Prague",
            "details.cancelled_by": "admin_cancel_running_endpoint",
        }},
    )

    await db.ops_config.update_one(
        {"key": f"cancel_job_{job_name}"},
        {"$set": {
            "key": f"cancel_job_{job_name}",
            "value": True,
            "requested_at": now,
            "requested_by": "admin_cancel_running_endpoint",
        }},
        upsert=True,
    )

    return {
        "job_name": job_name,
        "run_id": str(running_doc["_id"]),
        "cancel_requested": True,
        "requested_at": now.isoformat(),
    }
