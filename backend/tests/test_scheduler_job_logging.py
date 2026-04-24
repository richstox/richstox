import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import scheduler


class _InsertOneResult:
    def __init__(self, inserted_id):
        self.inserted_id = inserted_id


class _InsertCollection:
    def __init__(self):
        self.docs = []

    async def insert_one(self, doc):
        self.docs.append(dict(doc))
        return _InsertOneResult(len(self.docs))


class _Db:
    def __init__(self):
        self.ops_job_runs = _InsertCollection()
        self.system_job_logs = _InsertCollection()


@pytest.mark.asyncio
async def test_run_job_with_retry_persists_finished_fields_and_split_counters():
    db = _Db()

    async def _job(_db):
        return {
            "status": "success",
            "tickers_targeted": 4,
            "tickers_updated": 1,
            "tickers_skipped_invalid": 1,
            "tickers_skipped_not_in_universe": 2,
            "api_calls": 1,
            "api_credits_estimated": 1,
        }

    result = await scheduler.run_job_with_retry(
        "splits_upcoming_calendar",
        _job,
        db,
        max_retries=1,
    )

    assert result["status"] == "success"
    assert len(db.ops_job_runs.docs) == 1
    run_doc = db.ops_job_runs.docs[0]
    assert run_doc["status"] == "success"
    assert run_doc["tickers_targeted"] == 4
    assert run_doc["tickers_updated"] == 1
    assert run_doc["tickers_skipped_invalid"] == 1
    assert run_doc["tickers_skipped_not_in_universe"] == 2
    assert run_doc["api_calls"] == 1
    assert run_doc["api_credits_estimated"] == 1
    assert run_doc["finished_at"] == run_doc["completed_at"]
    assert run_doc["finished_at_prague"] == run_doc["completed_at_prague"]
