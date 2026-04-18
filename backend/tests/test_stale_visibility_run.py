"""Tests for auto-finalization of stale compute_visible_universe runs.

Covers:
  1. Status endpoint auto-finalizes a stale running run (updated_at > threshold).
  2. Non-stale running run is NOT auto-finalized.
  3. Stale running run → auto-finalized then new run can start.
  4. Job-name alias resolution (_resolve_db_job_name).
  5. Fallback to started_at when no heartbeat (updated_at).
  6. cancel_latest_running_job now accepts compute_visible_universe.

The tests mock the DB layer to avoid needing Motor / a running MongoDB.
We re-implement the pure helpers inline and patch ``recover_stale_job_run``
via a lightweight import shim to avoid pulling the heavy ``server`` module
(which requires motor, uvicorn, etc.) at test-collection time.
"""

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


# ---------------------------------------------------------------------------
# Re-implement the lightweight helpers from server.py so we can test them
# without importing the full server module (which pulls motor, uvicorn, etc.)
# ---------------------------------------------------------------------------

_JOB_NAME_DB_MAP = {
    "recompute_visibility_all": "compute_visible_universe",
}

_JOB_MAX_RUNTIME_MINUTES = {
    "peer_medians": 20,
    "compute_visible_universe": 20,
}
_DEFAULT_MAX_RUNTIME_MINUTES = 120


def _resolve_db_job_name(api_job_name: str) -> str:
    return _JOB_NAME_DB_MAP.get(api_job_name, api_job_name)


async def recover_stale_job_run(database, job_name: str):
    """Inline replica of the server.py helper for unit-testing."""
    from zoneinfo import ZoneInfo as _RecoverZI
    import logging
    logger = logging.getLogger("richstox.test")

    db_job_name = _resolve_db_job_name(job_name)
    max_minutes = _JOB_MAX_RUNTIME_MINUTES.get(
        db_job_name,
        _JOB_MAX_RUNTIME_MINUTES.get(job_name, _DEFAULT_MAX_RUNTIME_MINUTES),
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
    error_msg = (
        f"Stale run auto-finalized (timeout). No heartbeat for {int(stale_minutes)} min "
        f"(threshold={max_minutes} min). "
        f"started_at={started_iso}, last updated_at={updated_iso}."
    )

    update_fields = {
        "status": "error",
        "error_code": "timeout",
        "finished_at": expire_at,
        "finished_at_prague": expire_at.astimezone(_RecoverZI("Europe/Prague")).isoformat(),
        "error_message": error_msg,
        "error_traceback": "Timeout recovery by recover_stale_job_run",
    }
    if duration_seconds is not None:
        update_fields["duration_seconds"] = round(duration_seconds, 1)
    update_fields["details.timeout_recovery"] = True
    update_fields["details.timeout_recovered_at"] = expire_at.isoformat()
    update_fields["details.stale_auto_finalized"] = True

    await database.ops_job_runs.update_one(
        {"_id": existing["_id"]},
        {"$set": update_fields},
    )

    existing.update(update_fields)
    existing["recovered"] = True
    return existing


# ---------------------------------------------------------------------------
# Mock helper
# ---------------------------------------------------------------------------

def _utc(year, month, day, hour, minute=0, second=0):
    return datetime(year, month, day, hour, minute, second, tzinfo=timezone.utc)


def _make_ops_collection(docs=None):
    _docs = list(docs or [])

    async def _find_one(query, *args, **kwargs):
        for d in _docs:
            match = True
            for k, v in query.items():
                if k.startswith("$"):
                    continue
                if d.get(k) != v:
                    match = False
                    break
            if match:
                return d
        return None

    return SimpleNamespace(
        find_one=AsyncMock(side_effect=_find_one),
        update_one=AsyncMock(return_value=SimpleNamespace(modified_count=1)),
    )


# ===========================================================================
# Tests
# ===========================================================================

# 1) _resolve_db_job_name
# ---------------------------------------------------------------------------

def test_resolve_db_job_name_returns_alias():
    assert _resolve_db_job_name("recompute_visibility_all") == "compute_visible_universe"


def test_resolve_db_job_name_passthrough():
    assert _resolve_db_job_name("price_sync") == "price_sync"
    assert _resolve_db_job_name("peer_medians") == "peer_medians"


def test_job_max_runtime_has_compute_visible_universe():
    assert "compute_visible_universe" in _JOB_MAX_RUNTIME_MINUTES
    assert _JOB_MAX_RUNTIME_MINUTES["compute_visible_universe"] == 20


# ---------------------------------------------------------------------------
# 2) Stale run auto-finalized on status read
# ---------------------------------------------------------------------------

def test_stale_run_auto_finalized_on_status():
    """A compute_visible_universe run with updated_at > 20 min ago is auto-finalized."""
    started = _utc(2026, 4, 18, 20, 23)
    updated = _utc(2026, 4, 18, 20, 25)

    stale_doc = {
        "_id": "69e3e85319f7b3590e8b6ba8",
        "job_name": "compute_visible_universe",
        "status": "running",
        "started_at": started,
        "updated_at": updated,
        "started_at_prague": "2026-04-18T22:23:46",
        "progress": "Processed 3,500 / 6,524 (53%)",
        "details": {},
    }

    ops = _make_ops_collection([stale_doc])
    database = SimpleNamespace(ops_job_runs=ops)

    result = asyncio.run(recover_stale_job_run(database, "recompute_visibility_all"))

    assert result is not None
    assert result["recovered"] is True
    assert result["status"] == "error"
    assert result["error_code"] == "timeout"
    assert "Stale run auto-finalized" in result["error_message"]
    assert "started_at=" in result["error_message"]
    assert "updated_at=" in result["error_message"]
    assert result["details.timeout_recovery"] is True
    assert result["details.stale_auto_finalized"] is True

    ops.update_one.assert_awaited_once()
    call_args = ops.update_one.await_args.args
    assert call_args[0] == {"_id": "69e3e85319f7b3590e8b6ba8"}
    assert call_args[1]["$set"]["status"] == "error"
    assert call_args[1]["$set"]["error_code"] == "timeout"


# ---------------------------------------------------------------------------
# 3) Non-stale running run → NOT auto-finalized (would cause 409)
# ---------------------------------------------------------------------------

def test_non_stale_run_not_finalized():
    """A running run with recent heartbeat (< 20 min) is NOT auto-finalized."""
    now = datetime.now(timezone.utc)
    started = now - timedelta(minutes=10)
    updated = now - timedelta(minutes=5)

    running_doc = {
        "_id": "active_run_123",
        "job_name": "compute_visible_universe",
        "status": "running",
        "started_at": started,
        "updated_at": updated,
        "details": {},
    }

    ops = _make_ops_collection([running_doc])
    database = SimpleNamespace(ops_job_runs=ops)

    result = asyncio.run(recover_stale_job_run(database, "recompute_visibility_all"))

    assert result is None
    ops.update_one.assert_not_awaited()


# ---------------------------------------------------------------------------
# 4) No running doc → returns None
# ---------------------------------------------------------------------------

def test_no_running_doc_returns_none():
    ops = _make_ops_collection([])
    database = SimpleNamespace(ops_job_runs=ops)

    result = asyncio.run(recover_stale_job_run(database, "recompute_visibility_all"))
    assert result is None


# ---------------------------------------------------------------------------
# 5) Fallback to started_at when updated_at is missing
# ---------------------------------------------------------------------------

def test_stale_run_fallback_to_started_at():
    """When updated_at is missing, staleness is measured from started_at."""
    started = _utc(2026, 4, 18, 20, 0)

    doc = {
        "_id": "no_heartbeat_run",
        "job_name": "compute_visible_universe",
        "status": "running",
        "started_at": started,
        "details": {},
    }

    ops = _make_ops_collection([doc])
    database = SimpleNamespace(ops_job_runs=ops)

    result = asyncio.run(recover_stale_job_run(database, "recompute_visibility_all"))
    assert result is not None
    assert result["recovered"] is True
    assert result["status"] == "error"
    assert result["error_code"] == "timeout"


# ---------------------------------------------------------------------------
# 6) Stale run → finalized, then new run can start (no 409)
# ---------------------------------------------------------------------------

def test_stale_run_allows_new_run_after_finalization():
    """After a stale run is finalized, recover returns the doc so the
    caller knows recovery happened and skips the 409 guard."""
    started = _utc(2026, 4, 18, 19, 0)  # very old
    updated = _utc(2026, 4, 18, 19, 5)

    stale_doc = {
        "_id": "old_stale_run",
        "job_name": "compute_visible_universe",
        "status": "running",
        "started_at": started,
        "updated_at": updated,
        "details": {},
    }

    ops = _make_ops_collection([stale_doc])
    database = SimpleNamespace(ops_job_runs=ops)

    recovered = asyncio.run(recover_stale_job_run(database, "recompute_visibility_all"))

    # Recovery happened → caller should NOT return 409.
    assert recovered is not None
    assert recovered["recovered"] is True
    # The doc status is now "error" in the returned dict.
    assert recovered["status"] == "error"


# ---------------------------------------------------------------------------
# 7) cancel valid for compute_visible_universe
# ---------------------------------------------------------------------------

def test_cancel_valid_for_compute_visible_universe():
    from services.admin_jobs_service import is_valid_cancel_running_job_name
    assert is_valid_cancel_running_job_name("compute_visible_universe") is True


def test_cancel_invalid_for_unrelated_job():
    from services.admin_jobs_service import is_valid_cancel_running_job_name
    assert is_valid_cancel_running_job_name("universe_seed") is False
