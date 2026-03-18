import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

from scheduler_service import (
    _finalize_stuck_price_sync_runs,
    finalize_stuck_admin_job_runs,
)


def test_finalize_stuck_price_sync_runs_marks_non_active_phase_runs_cancelled():
    class _DB:
        pass

    db = _DB()
    db.ops_job_runs = SimpleNamespace(update_many=AsyncMock(return_value=SimpleNamespace(modified_count=2)))

    from datetime import datetime, timezone
    now = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    modified = asyncio.run(_finalize_stuck_price_sync_runs(db, now))

    assert modified == 2
    query, update = db.ops_job_runs.update_many.await_args.args
    assert query["job_name"] == "price_sync"
    assert query["status"] == "running"
    assert "$or" in query
    assert update["$set"]["status"] == "cancelled"
    assert update["$set"]["finished_at"] == now
    assert update["$set"]["details.zombie_reason"] == "stale_running_no_active_phase"


def test_finalize_stuck_admin_job_runs_honors_job_name_filter(monkeypatch):
    from datetime import datetime, timezone
    import scheduler_service

    now = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    price_mock = AsyncMock(return_value=1)
    fundamentals_mock = AsyncMock(return_value=0)
    monkeypatch.setattr(scheduler_service, "_finalize_stuck_price_sync_runs", price_mock)
    monkeypatch.setattr(scheduler_service, "_finalize_zombie_fundamentals_runs", fundamentals_mock)

    db = object()
    result = asyncio.run(finalize_stuck_admin_job_runs(db, now=now, job_names=["price_sync"]))

    assert result == {"price_sync": 1, "fundamentals_sync": 0}
    price_mock.assert_awaited_once_with(db, now)
    fundamentals_mock.assert_not_called()
