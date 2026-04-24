from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from services.admin_jobs_service import recover_stale_job_run
from services.admin_overview_service import get_job_last_runs


def _set_nested(doc, dotted_key, value):
    parts = dotted_key.split(".")
    target = doc
    for part in parts[:-1]:
        target = target.setdefault(part, {})
    target[parts[-1]] = value


def _matches(doc, query):
    for key, expected in query.items():
        actual = doc.get(key)
        if isinstance(expected, dict):
            for op, value in expected.items():
                if op == "$in" and actual not in value:
                    return False
        elif actual != expected:
            return False
    return True


class FakeAggCursor:
    def __init__(self, docs):
        self.docs = docs

    async def to_list(self, length=None):
        return self.docs if length is None else self.docs[:length]


class FakeOpsJobRuns:
    def __init__(self, docs):
        self.docs = docs

    async def find_one(self, query, projection=None, sort=None):
        matches = [doc for doc in self.docs if _matches(doc, query)]
        if sort:
            key, direction = sort[0]
            matches.sort(key=lambda doc: doc.get(key, datetime.min), reverse=direction == -1)
        if not matches:
            return None
        return matches[0]

    async def update_one(self, query, update):
        for doc in self.docs:
            if _matches(doc, query):
                for key, value in (update.get("$set") or {}).items():
                    _set_nested(doc, key, value)
                return SimpleNamespace(modified_count=1)
        return SimpleNamespace(modified_count=0)

    def aggregate(self, pipeline):
        docs = list(self.docs)
        for stage in pipeline:
            if "$match" in stage:
                docs = [doc for doc in docs if _matches(doc, stage["$match"])]
            elif "$sort" in stage:
                key, direction = next(iter(stage["$sort"].items()))
                docs.sort(key=lambda doc: doc.get(key, datetime.min), reverse=direction == -1)
            elif "$group" in stage:
                grouped = {}
                group_spec = stage["$group"]
                group_key = group_spec["_id"].lstrip("$")
                for doc in docs:
                    key = doc.get(group_key)
                    if key in grouped:
                        continue
                    grouped[key] = {"_id": key}
                    for field, expr in group_spec.items():
                        if field == "_id":
                            continue
                        if isinstance(expr, dict) and "$first" in expr:
                            grouped[key][field] = doc.get(expr["$first"].lstrip("$"))
                docs = list(grouped.values())
        return FakeAggCursor(docs)


@pytest.mark.asyncio
async def test_recover_stale_calendar_job_uses_shorter_timeout():
    started_at = datetime.now(timezone.utc) - timedelta(minutes=16)
    doc = {
        "_id": "run-1",
        "job_name": "earnings_upcoming_calendar",
        "status": "running",
        "started_at": started_at,
    }
    db = SimpleNamespace(ops_job_runs=FakeOpsJobRuns([doc]))

    recovered = await recover_stale_job_run(db, "earnings_upcoming_calendar")

    assert recovered is not None
    assert doc["status"] == "error"
    assert doc["error_code"] == "timeout"
    assert doc["details"]["timeout_recovery"] is True
    assert doc["duration_seconds"] >= 15 * 60


@pytest.mark.asyncio
async def test_get_job_last_runs_keeps_latest_completed_result_for_running_job():
    now = datetime.now(timezone.utc)
    docs = [
        {
            "_id": "completed-1",
            "job_name": "earnings_upcoming_calendar",
            "status": "completed",
            "started_at": now - timedelta(minutes=30),
            "finished_at": now - timedelta(minutes=29, seconds=20),
            "finished_at_prague": "2026-04-24T04:55:40+02:00",
            "duration_seconds": 40,
            "result": {
                "requested_days_count": 91,
                "days_fetched_ok_count": 91,
                "days_failed_count": 0,
            },
        },
        {
            "_id": "running-1",
            "job_name": "earnings_upcoming_calendar",
            "status": "running",
            "started_at": now - timedelta(minutes=2),
            "started_at_prague": "2026-04-24T23:07:00+02:00",
        },
    ]
    db = SimpleNamespace(ops_job_runs=FakeOpsJobRuns(docs))

    last_runs = await get_job_last_runs(db)
    job = last_runs["earnings_upcoming_calendar"]

    assert job["status"] == "running"
    assert job["result"] is None
    assert job["latest_completed_result"]["requested_days_count"] == 91
    assert job["latest_completed_duration_seconds"] == 40
    assert job["latest_completed_finished_at_prague"] == "2026-04-24T04:55:40+02:00"
