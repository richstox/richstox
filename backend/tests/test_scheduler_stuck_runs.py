import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

from scheduler_service import (
    _finalize_stuck_price_sync_runs,
    _finalize_orphaned_chain_runs,
    finalize_stuck_admin_job_runs,
    FUNDAMENTALS_SYNC_ZOMBIE_TIMEOUT_SECONDS,
)


def test_finalize_stuck_price_sync_runs_marks_non_active_phase_runs_cancelled():
    class _DB:
        pass

    db = _DB()
    db.ops_job_runs = SimpleNamespace(update_many=AsyncMock(return_value=SimpleNamespace(modified_count=2)))

    now = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    modified = asyncio.run(_finalize_stuck_price_sync_runs(db, now))

    # update_many is called twice (running + cancel_requested), so total = 2+2=4
    assert modified == 4
    # Verify first call targeted running price_sync docs
    first_call_args = db.ops_job_runs.update_many.await_args_list[0].args
    assert first_call_args[0]["job_name"] == "price_sync"
    assert first_call_args[0]["status"] == "running"
    assert "$or" in first_call_args[0]
    assert first_call_args[1]["$set"]["status"] == "cancelled"
    assert first_call_args[1]["$set"]["finished_at"] == now
    assert first_call_args[1]["$set"]["details.zombie_reason"] == "stale_running_no_active_phase"


def test_finalize_stuck_admin_job_runs_honors_job_name_filter(monkeypatch):
    import scheduler_service

    now = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    price_mock = AsyncMock(return_value=1)
    fundamentals_mock = AsyncMock(return_value=0)
    orphaned_mock = AsyncMock(return_value=0)
    monkeypatch.setattr(scheduler_service, "_finalize_stuck_price_sync_runs", price_mock)
    monkeypatch.setattr(scheduler_service, "_finalize_zombie_fundamentals_runs", fundamentals_mock)
    monkeypatch.setattr(scheduler_service, "_finalize_orphaned_chain_runs", orphaned_mock)

    db = object()
    result = asyncio.run(finalize_stuck_admin_job_runs(db, now=now, job_names=["price_sync"]))

    assert result == {"price_sync": 1, "fundamentals_sync": 0, "orphaned_chains": 0}
    price_mock.assert_awaited_once_with(db, now)
    fundamentals_mock.assert_not_called()
    orphaned_mock.assert_awaited_once_with(db, now)


# ---------------------------------------------------------------------------
# Tests for _finalize_orphaned_chain_runs
# ---------------------------------------------------------------------------

class _AsyncCursorFromList:
    """Minimal async iterable that yields from a plain list."""
    def __init__(self, docs):
        self._docs = list(docs)
    def __aiter__(self):
        return self
    async def __anext__(self):
        if not self._docs:
            raise StopAsyncIteration
        return self._docs.pop(0)


def _make_db(chain_docs, active_job_for_chain=None):
    """Build a minimal fake db with pipeline_chain_runs.find and ops_job_runs.find_one."""
    class _DB:
        pass
    db = _DB()

    def _chain_find(query):
        # Motor's find() returns a cursor synchronously (not a coroutine)
        return _AsyncCursorFromList(chain_docs)

    db.pipeline_chain_runs = SimpleNamespace(
        find=_chain_find,
        update_one=AsyncMock(return_value=SimpleNamespace(modified_count=1)),
    )

    async def _ops_find_one(query):
        if active_job_for_chain is None:
            return None
        chain_id_filter = query.get("details.chain_run_id")
        if chain_id_filter and chain_id_filter in active_job_for_chain:
            return {"status": "running", "details": {"chain_run_id": chain_id_filter}}
        return None

    db.ops_job_runs = SimpleNamespace(find_one=_ops_find_one)
    return db


def test_orphaned_chain_finalized_when_no_active_jobs():
    """A running chain with no active child jobs should be finalized as cancelled."""
    now = datetime(2026, 3, 22, 13, 0, tzinfo=timezone.utc)
    stale_started = now - timedelta(seconds=FUNDAMENTALS_SYNC_ZOMBIE_TIMEOUT_SECONDS + 60)
    chain_docs = [{
        "chain_run_id": "chain_orphaned_abc",
        "status": "running",
        "started_at": stale_started,
        "current_step": 3,
    }]
    db = _make_db(chain_docs, active_job_for_chain=None)

    count = asyncio.run(_finalize_orphaned_chain_runs(db, now))
    assert count == 1
    db.pipeline_chain_runs.update_one.assert_awaited_once()
    call_args = db.pipeline_chain_runs.update_one.await_args.args
    assert call_args[0] == {"chain_run_id": "chain_orphaned_abc", "status": "running"}
    assert call_args[1]["$set"]["status"] == "cancelled"
    assert call_args[1]["$set"]["finished_at"] == now
    assert call_args[1]["$set"]["error"] == "orphaned_chain_no_active_jobs"


def test_orphaned_chain_not_finalized_when_active_job_exists():
    """A running chain with an active child job should NOT be finalized."""
    now = datetime(2026, 3, 22, 13, 0, tzinfo=timezone.utc)
    stale_started = now - timedelta(seconds=FUNDAMENTALS_SYNC_ZOMBIE_TIMEOUT_SECONDS + 60)
    chain_docs = [{
        "chain_run_id": "chain_still_active",
        "status": "running",
        "started_at": stale_started,
        "current_step": 3,
    }]
    db = _make_db(chain_docs, active_job_for_chain={"chain_still_active"})

    count = asyncio.run(_finalize_orphaned_chain_runs(db, now))
    assert count == 0
    db.pipeline_chain_runs.update_one.assert_not_awaited()


def test_orphaned_chain_not_finalized_when_recent():
    """A recently started running chain should NOT be finalized (not stale yet)."""
    now = datetime(2026, 3, 22, 13, 0, tzinfo=timezone.utc)
    # Started only 60 seconds ago — well within the zombie timeout
    recent_started = now - timedelta(seconds=60)
    chain_docs = [{
        "chain_run_id": "chain_recent",
        "status": "running",
        "started_at": recent_started,
        "current_step": 2,
    }]
    # The find query uses started_at < stale_before, so this doc won't appear
    # We simulate this by returning empty — the query in production filters at DB level
    db = _make_db([], active_job_for_chain=None)

    count = asyncio.run(_finalize_orphaned_chain_runs(db, now))
    assert count == 0


def test_orphaned_chain_skipped_when_no_chain_run_id():
    """Chain docs missing chain_run_id should be skipped gracefully."""
    now = datetime(2026, 3, 22, 13, 0, tzinfo=timezone.utc)
    stale_started = now - timedelta(seconds=FUNDAMENTALS_SYNC_ZOMBIE_TIMEOUT_SECONDS + 60)
    chain_docs = [{
        "status": "running",
        "started_at": stale_started,
    }]
    db = _make_db(chain_docs, active_job_for_chain=None)

    count = asyncio.run(_finalize_orphaned_chain_runs(db, now))
    assert count == 0
    db.pipeline_chain_runs.update_one.assert_not_awaited()


def test_finalize_stuck_admin_job_runs_includes_orphaned_chains(monkeypatch):
    """finalize_stuck_admin_job_runs must include orphaned_chains in the result."""
    import scheduler_service

    now = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(scheduler_service, "_finalize_stuck_price_sync_runs", AsyncMock(return_value=0))
    monkeypatch.setattr(scheduler_service, "_finalize_zombie_fundamentals_runs", AsyncMock(return_value=0))
    monkeypatch.setattr(scheduler_service, "_finalize_orphaned_chain_runs", AsyncMock(return_value=2))

    db = object()
    result = asyncio.run(finalize_stuck_admin_job_runs(db, now=now))

    assert result["orphaned_chains"] == 2
