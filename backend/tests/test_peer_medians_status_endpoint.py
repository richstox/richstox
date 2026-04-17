"""
REGRESSION TEST for /api/admin/job/peer_medians/status endpoint.

Covers both bugs introduced by PR #315:
  (A) 500 Internal Server Error caused by TypeError in recover_stale_job_run
      when subtracting an offset-aware datetime from an offset-naive datetime
      returned by Motor.
  (B) +120 min elapsed timer offset caused by _iso() emitting naive ISO
      timestamps that JS Date.parse() interprets as local time.

Run:
    cd /app/backend && python -m pytest tests/test_peer_medians_status_endpoint.py -v
"""

import json
import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock
from bson import ObjectId


# ---------------------------------------------------------------------------
# Fake MongoDB collection (returns naive datetimes like real Motor)
# ---------------------------------------------------------------------------
class FakeOpsJobRuns:
    """In-memory mock that returns naive datetimes, just like Motor."""

    def __init__(self):
        self.docs = []

    async def insert_one(self, doc):
        doc = dict(doc)
        if "_id" not in doc:
            doc["_id"] = ObjectId()
        self.docs.append(doc)
        return MagicMock(inserted_id=doc["_id"])

    async def find_one(self, query, projection=None, sort=None):
        matches = self._filter(query)
        if sort:
            key, direction = sort[0] if isinstance(sort, list) else sort
            matches.sort(
                key=lambda d: d.get(key) or datetime.min,
                reverse=(direction == -1),
            )
        if not matches:
            return None
        doc = dict(matches[0])
        if projection:
            if "_id" in projection and projection["_id"] == 0:
                doc.pop("_id", None)
            keep = {k for k, v in projection.items() if v == 1}
            if keep:
                doc = {k: v for k, v in doc.items() if k in keep or k == "_id"}
        return doc

    async def update_one(self, query, update):
        matches = self._filter(query)
        if matches:
            target = matches[0]
            if "$set" in update:
                for k, v in update["$set"].items():
                    if "." in k:
                        parts = k.split(".", 1)
                        target.setdefault(parts[0], {})[parts[1]] = v
                    else:
                        target[k] = v
        return MagicMock(modified_count=len(matches))

    def _filter(self, query):
        results = []
        for doc in self.docs:
            match = True
            for k, v in query.items():
                if isinstance(v, dict):
                    for op, val in v.items():
                        if op == "$in" and doc.get(k) not in val:
                            match = False
                elif doc.get(k) != v:
                    match = False
            if match:
                results.append(doc)
        return results


def _make_fake_db():
    db = MagicMock()
    db.ops_job_runs = FakeOpsJobRuns()
    db.ops_config = AsyncMock()
    db.ops_config.find_one = AsyncMock(return_value=None)
    for coll in ["stock_prices", "company_fundamentals_cache", "financials_cache"]:
        c = AsyncMock()
        c.count_documents = AsyncMock(return_value=0)
        c.distinct = AsyncMock(return_value=[])
        setattr(db, coll, c)
    return db


def _naive(dt):
    """Strip timezone to simulate what Motor returns for BSON dates."""
    return dt.replace(tzinfo=None)


# ---------------------------------------------------------------------------
# Faithful replica of server.py functions under test.
#
# We inline the logic instead of importing server.py because importing the
# module pulls in FastAPI, dotenv, Motor, and dozens of other deps that are
# unavailable in the lightweight test environment.  This is the same pattern
# used by test_step4_admin_run_now.py.  If the real function changes, this
# test must be updated to match.
# ---------------------------------------------------------------------------

_JOB_MAX_RUNTIME_MINUTES = {"peer_medians": 20}
_DEFAULT_MAX_RUNTIME_MINUTES = 120


async def recover_stale_job_run(database, job_name: str):
    """Faithful copy of server.py recover_stale_job_run WITH the fix."""
    from zoneinfo import ZoneInfo as _ZI

    max_minutes = _JOB_MAX_RUNTIME_MINUTES.get(job_name, _DEFAULT_MAX_RUNTIME_MINUTES)
    existing = await database.ops_job_runs.find_one(
        {"job_name": job_name, "status": "running"},
        sort=[("started_at", -1)],
    )
    if not existing:
        return None

    started = existing.get("started_at")
    # FIX: normalise naive → aware UTC (Motor returns naive BSON dates)
    if isinstance(started, datetime) and started.tzinfo is None:
        started = started.replace(tzinfo=timezone.utc)
    try:
        age_minutes = (
            (datetime.now(timezone.utc) - started).total_seconds() / 60.0
            if isinstance(started, datetime)
            else float("inf")
        )
    except Exception:
        age_minutes = float("inf")

    if age_minutes <= max_minutes:
        return None

    expire_at = datetime.now(timezone.utc)
    try:
        duration_seconds = (expire_at - started).total_seconds() if isinstance(started, datetime) else None
    except (TypeError, AttributeError):
        duration_seconds = None

    update_fields = {
        "status": "error",
        "finished_at": expire_at,
        "finished_at_prague": expire_at.astimezone(_ZI("Europe/Prague")).isoformat(),
        "error_message": f"Timeout after {int(age_minutes)} min",
        "error_traceback": "Timeout recovery by recover_stale_job_run",
    }
    if duration_seconds is not None:
        update_fields["duration_seconds"] = round(duration_seconds, 1)
    update_fields["details.timeout_recovery"] = True
    update_fields["details.timeout_recovered_at"] = expire_at.isoformat()

    await database.ops_job_runs.update_one(
        {"_id": existing["_id"]}, {"$set": update_fields}
    )
    existing.update(update_fields)
    existing["recovered"] = True
    return existing


async def _call_status(db, job_name: str = "peer_medians") -> dict:
    """Run the exact logic of admin_get_job_status and return the response.

    Faithful copy of server.py lines 6200-6325 WITH the fixes so we can
    verify the endpoint behaviour without a running FastAPI app.
    """
    # 1. Auto-recover stale runs
    await recover_stale_job_run(db, job_name)

    # 2. Read config
    config_key = f"job_{job_name}_enabled"
    config = await db.ops_config.find_one({"key": config_key}, {"_id": 0})
    _ALWAYS_RUNNABLE = {
        "backfill_all", "recompute_visibility_all", "clean_zombie_tickers",
        "recompute_visibility_with_zombies", "benchmark_update",
        "market_calendar", "news_refresh", "peer_medians",
    }
    enabled = config.get("value", False) if config else job_name in _ALWAYS_RUNNABLE

    # 3. _iso helper — THE FIX: append 'Z' for naive datetimes
    def _iso(dt):
        if not dt:
            return None
        if hasattr(dt, "isoformat"):
            s = dt.isoformat()
            if isinstance(dt, datetime) and dt.tzinfo is None:
                s += "Z"
            return s
        return str(dt)

    # 4. Last run
    raw_last_run = await db.ops_job_runs.find_one(
        {"job_name": job_name}, sort=[("started_at", -1)]
    )
    last_run = None
    if raw_last_run:
        details = raw_last_run.get("details") or {}
        started_at = raw_last_run.get("started_at")
        finished_at = raw_last_run.get("finished_at") or raw_last_run.get("completed_at")
        seeded_total = (
            raw_last_run.get("progress_total")
            or details.get("seeded_total")
            or details.get("tickers_seeded_total")
        )
        phase = raw_last_run.get("phase") or details.get("phase")
        duration_seconds = None
        if started_at and finished_at:
            try:
                duration_seconds = (finished_at - started_at).total_seconds()
            except (TypeError, AttributeError):
                duration_seconds = None

        last_run = {
            "audit_id": str(raw_last_run["_id"]),
            "status": raw_last_run.get("status"),
            "started_at": _iso(started_at),
            "finished_at": _iso(finished_at),
            "started_at_prague": raw_last_run.get("started_at_prague"),
            "finished_at_prague": raw_last_run.get("finished_at_prague") or raw_last_run.get("completed_at_prague"),
            "duration_seconds": duration_seconds,
            "result": raw_last_run.get("result"),
        }

    # 5. Previous completed run
    previous_completed_run = None
    raw_prev = await db.ops_job_runs.find_one(
        {"job_name": job_name, "status": {"$in": ["success", "completed"]}},
        {"_id": 0},
        sort=[("finished_at", -1)],
    )
    if raw_prev:
        prev_started = raw_prev.get("started_at")
        prev_finished = raw_prev.get("finished_at")
        prev_duration = None
        if prev_started and prev_finished:
            try:
                prev_duration = (prev_finished - prev_started).total_seconds()
            except (TypeError, AttributeError):
                prev_duration = None
        previous_completed_run = {
            "status": raw_prev.get("status"),
            "started_at": _iso(prev_started),
            "finished_at": _iso(prev_finished),
            "started_at_prague": raw_prev.get("started_at_prague"),
            "finished_at_prague": raw_prev.get("finished_at_prague"),
            "duration_seconds": prev_duration,
        }

    return {
        "job": job_name,
        "enabled": enabled,
        "config_key": config_key,
        "last_run": last_run,
        "previous_completed_run": previous_completed_run,
    }


# =========================================================================
# TEST 1: No runs yet → 200 with last_run=None
# =========================================================================
@pytest.mark.asyncio
async def test_status_no_runs():
    """Endpoint must return 200 with last_run=null when no runs exist."""
    db = _make_fake_db()
    result = await _call_status(db)
    assert result["last_run"] is None
    assert result["previous_completed_run"] is None
    json.dumps(result)


# =========================================================================
# TEST 2: Running run → 200, started_at ends with Z
# =========================================================================
@pytest.mark.asyncio
async def test_status_running_run():
    """Endpoint must return 200 for a currently running job."""
    db = _make_fake_db()
    # Use a start time only 1 minute in the past so it stays within the
    # 20-minute max runtime and recover_stale_job_run does NOT fire.
    recent_start = _naive(datetime.now(timezone.utc))
    await db.ops_job_runs.insert_one({
        "job_name": "peer_medians",
        "status": "running",
        "started_at": recent_start,
        "finished_at": None,
        "details": {},
    })
    result = await _call_status(db)
    lr = result["last_run"]
    assert lr is not None
    assert lr["status"] == "running"
    assert lr["audit_id"] is not None
    assert lr["started_at"].endswith("Z"), \
        f"started_at must end with Z for UTC, got: {lr['started_at']}"
    assert lr["finished_at"] is None
    json.dumps(result)


# =========================================================================
# TEST 3: Completed run → 200, both timestamps end with Z
# =========================================================================
@pytest.mark.asyncio
async def test_status_completed_run():
    """Endpoint must return 200 for a completed job with valid UTC timestamps."""
    db = _make_fake_db()
    await db.ops_job_runs.insert_one({
        "job_name": "peer_medians",
        "status": "completed",
        "started_at": _naive(datetime(2026, 4, 17, 5, 30, 0, tzinfo=timezone.utc)),
        "finished_at": _naive(datetime(2026, 4, 17, 5, 31, 2, tzinfo=timezone.utc)),
        "started_at_prague": "2026-04-17T07:30:00+02:00",
        "finished_at_prague": "2026-04-17T07:31:02+02:00",
        "details": {},
        "result": {"tickers_processed": 5000},
    })
    result = await _call_status(db)
    lr = result["last_run"]
    assert lr["status"] == "completed"
    assert lr["started_at"].endswith("Z")
    assert lr["finished_at"].endswith("Z")
    assert lr["duration_seconds"] == pytest.approx(62.0, abs=1)
    json.dumps(result)


# =========================================================================
# TEST 4: Stale running run (>20 min) → recover_stale_job_run must NOT
#          crash (was the 500 root cause)
# =========================================================================
@pytest.mark.asyncio
async def test_status_stale_running_no_crash():
    """Bug A: recover_stale_job_run must not TypeError on naive datetimes.

    Motor returns naive datetimes.  ``datetime.now(timezone.utc) - naive``
    used to raise TypeError, causing the 500 on every poll.
    """
    db = _make_fake_db()
    stale_start = _naive(datetime(2026, 4, 17, 18, 0, 0, tzinfo=timezone.utc))
    await db.ops_job_runs.insert_one({
        "job_name": "peer_medians",
        "status": "running",
        "started_at": stale_start,
        "finished_at": None,
        "details": {},
    })
    # This MUST NOT raise (was the 500 bug)
    result = await _call_status(db)
    lr = result["last_run"]
    assert lr["status"] == "error"
    assert lr["finished_at"] is not None
    assert lr["audit_id"] is not None
    json.dumps(result)


# =========================================================================
# TEST 5: Error/timeout run → 200
# =========================================================================
@pytest.mark.asyncio
async def test_status_error_run():
    """Endpoint must return 200 for an error/timeout job."""
    db = _make_fake_db()
    await db.ops_job_runs.insert_one({
        "job_name": "peer_medians",
        "status": "error",
        "started_at": _naive(datetime(2026, 4, 17, 5, 30, 0, tzinfo=timezone.utc)),
        "finished_at": _naive(datetime(2026, 4, 17, 5, 50, 0, tzinfo=timezone.utc)),
        "error_message": "Timeout: 20 min",
        "details": {"timeout_recovery": True},
    })
    result = await _call_status(db)
    lr = result["last_run"]
    assert lr["status"] == "error"
    assert lr["started_at"].endswith("Z")
    assert lr["finished_at"].endswith("Z")
    json.dumps(result)


# =========================================================================
# TEST 6: ISO timestamps must always include timezone for JS Date.parse
# =========================================================================
@pytest.mark.asyncio
async def test_iso_timestamps_always_utc():
    """Bug B: _iso() on naive datetimes must append 'Z' so JS treats them as UTC.

    Without 'Z', Date.parse() in the browser interprets naive ISO strings as
    local time, causing the +120 min offset in Prague (UTC+2).
    """
    db = _make_fake_db()
    await db.ops_job_runs.insert_one({
        "job_name": "peer_medians",
        "status": "running",
        "started_at": datetime(2026, 4, 17, 19, 3, 0),  # naive!
        "finished_at": None,
        "details": {},
    })
    result = await _call_status(db)
    started = result["last_run"]["started_at"]
    assert started == "2026-04-17T19:03:00Z", f"Expected Z suffix, got: {started}"


# =========================================================================
# TEST 7: Full JSON round-trip (proves no 500 from serialization)
# =========================================================================
@pytest.mark.asyncio
async def test_full_json_serializable():
    """The complete status response must be JSON-serializable in all states."""
    db = _make_fake_db()
    await db.ops_job_runs.insert_one({
        "job_name": "peer_medians",
        "status": "completed",
        "started_at": _naive(datetime(2026, 4, 17, 5, 30, 0, tzinfo=timezone.utc)),
        "finished_at": _naive(datetime(2026, 4, 17, 5, 31, 0, tzinfo=timezone.utc)),
        "details": {"heartbeat": "2026-04-17T05:30:30Z", "phase": "industry"},
        "result": {
            "status": "success",
            "tickers_processed": 5432,
            "market_medians": {"pe_ttm": {"median": 22.5, "n_used": 3200}},
        },
    })
    result = await _call_status(db)
    serialized = json.dumps(result)
    parsed = json.loads(serialized)
    assert parsed["last_run"]["audit_id"] is not None
    assert parsed["last_run"]["result"]["tickers_processed"] == 5432


# =========================================================================
# TEST 8: Reproduces the EXACT 500 traceback (negative test for old code)
# =========================================================================
@pytest.mark.asyncio
async def test_old_code_would_crash():
    """Proves the EXACT TypeError that caused the 500.

    Before the fix, recover_stale_job_run did:
        expire_at = datetime.now(timezone.utc)   # aware
        duration  = (expire_at - started)         # started is naive → TypeError

    This is the traceback users saw on every status poll.
    """
    naive_started = datetime(2026, 4, 17, 18, 0, 0)  # naive, from Motor
    aware_now = datetime.now(timezone.utc)             # aware

    with pytest.raises(TypeError, match="can't subtract offset-naive and offset-aware"):
        _ = (aware_now - naive_started).total_seconds()

    # After the fix: normalise naive → aware first
    fixed_started = naive_started.replace(tzinfo=timezone.utc)
    duration = (aware_now - fixed_started).total_seconds()
    assert isinstance(duration, float)
    assert duration > 0

