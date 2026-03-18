import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from services.admin_jobs_service import (
    cancel_latest_running_job,
    is_valid_cancel_running_job_name,
)


def test_cancel_running_job_name_validation():
    assert is_valid_cancel_running_job_name("fundamentals_sync")
    assert is_valid_cancel_running_job_name("price_sync")
    assert not is_valid_cancel_running_job_name("universe_seed")


def test_cancel_latest_running_job_invalid_name_raises():
    db = SimpleNamespace()
    with pytest.raises(ValueError):
        asyncio.run(cancel_latest_running_job(db, "universe_seed"))


def test_cancel_latest_running_job_returns_none_when_not_running():
    ops_job_runs = SimpleNamespace(
        find_one=AsyncMock(return_value=None),
        update_one=AsyncMock(),
    )
    ops_config = SimpleNamespace(update_one=AsyncMock())
    db = SimpleNamespace(ops_job_runs=ops_job_runs, ops_config=ops_config)

    result = asyncio.run(cancel_latest_running_job(db, "price_sync"))

    assert result is None
    ops_job_runs.update_one.assert_not_called()
    ops_config.update_one.assert_not_called()


def test_cancel_latest_running_job_updates_latest_running_doc():
    run_id = "run123"
    now = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)

    ops_job_runs = SimpleNamespace(
        find_one=AsyncMock(return_value={"_id": run_id}),
        update_one=AsyncMock(),
    )
    ops_config = SimpleNamespace(update_one=AsyncMock())
    db = SimpleNamespace(ops_job_runs=ops_job_runs, ops_config=ops_config)

    result = asyncio.run(cancel_latest_running_job(db, "fundamentals_sync", now=now))

    assert result == {
        "job_name": "fundamentals_sync",
        "run_id": run_id,
        "cancel_requested": True,
        "requested_at": now.isoformat(),
    }
    ops_job_runs.find_one.assert_awaited_once_with(
        {"job_name": "fundamentals_sync", "status": "running"},
        {"_id": 1},
        sort=[("started_at", -1)],
    )

    update_filter, update_doc = ops_job_runs.update_one.await_args.args
    assert update_filter == {"_id": run_id, "status": "running"}
    assert update_doc["$set"]["status"] == "cancelled"
    assert update_doc["$set"]["cancelled_at"] == now
    assert update_doc["$set"]["finished_at"] == now
    assert update_doc["$set"]["details.cancelled_by"] == "admin_cancel_running_endpoint"

    ops_config.update_one.assert_awaited_once()
    config_filter, config_update = ops_config.update_one.await_args.args
    assert config_filter == {"key": "cancel_job_fundamentals_sync"}
    assert config_update["$set"]["value"] is True
    assert config_update["$set"]["requested_at"] == now
    assert ops_config.update_one.await_args.kwargs["upsert"] is True
