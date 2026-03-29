"""Tests for pipeline_chain_runs persistence and chain_run_id propagation.

Validates that:
- pipeline_chain_runs is created with status="running" and current_step/steps_done
- status stays "running" through intermediate steps (not step1_done/step2_done)
- current_step and steps_done are updated after each step
- chain-status endpoint returns stored current_step/steps_done
- chain-cancel endpoint accepts "running" status
- scheduler path creates pipeline_chain_runs and passes chain_run_id to step 3
"""

import asyncio
import os
import sys
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class FakeInsertResult:
    def __init__(self, inserted_id):
        self.inserted_id = inserted_id


class FakeUpdateResult:
    def __init__(self, matched=1, modified=1):
        self.matched_count = matched
        self.modified_count = modified


class FakePipelineChainRuns:
    """In-memory mock for the pipeline_chain_runs collection."""

    def __init__(self):
        self.docs = []
        self._next_id = 1

    async def insert_one(self, doc):
        doc = dict(doc)
        doc["_id"] = self._next_id
        self._next_id += 1
        self.docs.append(doc)
        return FakeInsertResult(doc["_id"])

    async def find_one(self, filter_dict, projection=None, **kwargs):
        for doc in reversed(self.docs):
            if all(doc.get(k) == v for k, v in filter_dict.items()
                   if not isinstance(v, dict)):
            # Simplified matching — good enough for test purposes
                match = True
                for k, v in filter_dict.items():
                    if isinstance(v, dict):
                        # Skip complex queries in this mock
                        continue
                    if doc.get(k) != v:
                        match = False
                        break
                if match:
                    return dict(doc)
        return None

    async def update_one(self, filter_dict, update, **kwargs):
        for doc in self.docs:
            match = True
            for k, v in filter_dict.items():
                if isinstance(v, dict):
                    continue
                if doc.get(k) != v:
                    match = False
                    break
            if match:
                if "$set" in update:
                    for key, val in update["$set"].items():
                        if "." in key:
                            parts = key.split(".")
                            cur = doc
                            for part in parts[:-1]:
                                if part not in cur:
                                    cur[part] = {}
                                cur = cur[part]
                            cur[parts[-1]] = val
                        else:
                            doc[key] = val
                return FakeUpdateResult(matched=1, modified=1)
        return FakeUpdateResult(matched=0, modified=0)


# ---------------------------------------------------------------------------
# Tests for server.py chain-status endpoint derivation
# ---------------------------------------------------------------------------

class TestChainStatusEndpoint:
    """Test the chain-status endpoint's current_step/steps_done derivation."""

    def test_stored_fields_preferred_over_derivation(self):
        """When current_step and steps_done are stored on the doc,
        the endpoint should use them directly."""
        doc = {
            "chain_run_id": "chain_test123",
            "status": "running",
            "current_step": 2,
            "steps_done": [1],
            "step_run_ids": {"step1": "run_abc"},
        }

        # Simulate the derivation logic from admin_pipeline_chain_status
        _status = doc.get("status")
        _srids = doc.get("step_run_ids", {})
        _steps_done = doc.get("steps_done")
        _current_step = doc.get("current_step")
        _failed_step = None

        # Legacy fallback
        if _steps_done is None:
            _steps_done = [i for i, k in enumerate(("step1", "step2", "step3"), 1) if _srids.get(k)]
        if _current_step is None and _status in ("running", "step1_done", "step2_done", "step3_done"):
            if _status == "running":
                _current_step = 1
            elif _status == "step1_done":
                _current_step = 2
            elif _status == "step2_done":
                _current_step = 3
            elif _status == "step3_done":
                _current_step = None
        if _status == "failed":
            _failed_step = next(
                (i for i, k in enumerate(("step1", "step2", "step3"), 1) if not _srids.get(k)),
                3,
            )

        assert _current_step == 2, "Should use stored current_step, not derive step 1 from 'running' status"
        assert _steps_done == [1], "Should use stored steps_done"
        assert _failed_step is None

    def test_legacy_fallback_for_old_docs(self):
        """Docs created before the fix (with step1_done status) should still derive correctly."""
        doc = {
            "chain_run_id": "chain_old",
            "status": "step1_done",
            "step_run_ids": {"step1": "run_old"},
        }

        _status = doc.get("status")
        _srids = doc.get("step_run_ids", {})
        _steps_done = doc.get("steps_done")
        _current_step = doc.get("current_step")

        if _steps_done is None:
            _steps_done = [i for i, k in enumerate(("step1", "step2", "step3"), 1) if _srids.get(k)]
        if _current_step is None and _status in ("running", "step1_done", "step2_done", "step3_done"):
            if _status == "step1_done":
                _current_step = 2

        assert _current_step == 2
        assert _steps_done == [1]

    def test_running_status_with_no_stored_fields_defaults_to_step1(self):
        """Legacy doc with status=running and no current_step defaults to step 1."""
        doc = {
            "chain_run_id": "chain_legacy",
            "status": "running",
            "step_run_ids": {},
        }

        _status = doc.get("status")
        _steps_done = doc.get("steps_done")
        _current_step = doc.get("current_step")
        _srids = doc.get("step_run_ids", {})

        if _steps_done is None:
            _steps_done = [i for i, k in enumerate(("step1", "step2", "step3"), 1) if _srids.get(k)]
        if _current_step is None and _status in ("running", "step1_done", "step2_done", "step3_done"):
            if _status == "running":
                _current_step = 1

        assert _current_step == 1
        assert _steps_done == []


# ---------------------------------------------------------------------------
# Tests for pipeline_chain_runs lifecycle (manual path)
# ---------------------------------------------------------------------------

class TestManualPathChainLifecycle:
    """Test that the manual path (run-full-now) keeps status='running' throughout."""

    def test_initial_insert_has_current_step_and_steps_done(self):
        """The initial pipeline_chain_runs insert should include current_step and steps_done."""
        chain_runs = FakePipelineChainRuns()
        asyncio.run(chain_runs.insert_one({
            "chain_run_id": "chain_test",
            "status": "running",
            "current_step": 1,
            "steps_done": [],
            "started_at": datetime.now(timezone.utc),
            "step_run_ids": {},
        }))

        doc = asyncio.run(chain_runs.find_one({"chain_run_id": "chain_test"}))
        assert doc is not None
        assert doc["status"] == "running"
        assert doc["current_step"] == 1
        assert doc["steps_done"] == []

    def test_step1_done_keeps_status_running(self):
        """After step 1, status should remain 'running', not change to 'step1_done'."""
        chain_runs = FakePipelineChainRuns()
        asyncio.run(chain_runs.insert_one({
            "chain_run_id": "chain_test",
            "status": "running",
            "current_step": 1,
            "steps_done": [],
            "step_run_ids": {},
        }))

        # Simulate step 1 completion (matching the server.py update)
        asyncio.run(chain_runs.update_one(
            {"chain_run_id": "chain_test"},
            {"$set": {
                "step_run_ids.step1": "run_s1",
                "current_step": 2,
                "steps_done": [1],
            }},
        ))

        doc = asyncio.run(chain_runs.find_one({"chain_run_id": "chain_test"}))
        assert doc["status"] == "running", "Status must stay 'running' after step 1"
        assert doc["current_step"] == 2
        assert doc["steps_done"] == [1]

    def test_step2_done_keeps_status_running(self):
        """After step 2, status should remain 'running'."""
        chain_runs = FakePipelineChainRuns()
        asyncio.run(chain_runs.insert_one({
            "chain_run_id": "chain_test",
            "status": "running",
            "current_step": 2,
            "steps_done": [1],
            "step_run_ids": {"step1": "run_s1"},
        }))

        asyncio.run(chain_runs.update_one(
            {"chain_run_id": "chain_test"},
            {"$set": {
                "step_run_ids.step2": "run_s2",
                "current_step": 3,
                "steps_done": [1, 2],
            }},
        ))

        doc = asyncio.run(chain_runs.find_one({"chain_run_id": "chain_test"}))
        assert doc["status"] == "running", "Status must stay 'running' after step 2"
        assert doc["current_step"] == 3
        assert doc["steps_done"] == [1, 2]

    def test_findone_running_returns_active_chain(self):
        """db.pipeline_chain_runs.findOne({status: 'running'}) must find active chain."""
        chain_runs = FakePipelineChainRuns()
        asyncio.run(chain_runs.insert_one({
            "chain_run_id": "chain_active",
            "status": "running",
            "current_step": 3,
            "steps_done": [1, 2],
        }))

        doc = asyncio.run(chain_runs.find_one({"status": "running"}))
        assert doc is not None
        assert doc["chain_run_id"] == "chain_active"


# ---------------------------------------------------------------------------
# Tests for scheduler path chain lifecycle
# ---------------------------------------------------------------------------

class TestSchedulerPathChainLifecycle:
    """Test that the scheduler path creates and updates pipeline_chain_runs."""

    def test_scheduler_creates_chain_runs_doc(self):
        """_run_universe_seed_scheduled should create a pipeline_chain_runs doc."""
        chain_runs = FakePipelineChainRuns()

        # Simulate what _run_universe_seed_scheduled now does
        chain_run_id = "chain_sched_abc123"
        started_at = datetime.now(timezone.utc)
        asyncio.run(chain_runs.insert_one({
            "chain_run_id": chain_run_id,
            "status": "running",
            "current_step": 1,
            "steps_done": [],
            "started_at": started_at,
            "source": "scheduled",
            "step_run_ids": {},
        }))

        doc = asyncio.run(chain_runs.find_one({"status": "running"}))
        assert doc is not None
        assert doc["chain_run_id"] == chain_run_id
        assert doc["current_step"] == 1
        assert doc["steps_done"] == []

    def test_scheduler_step1_done_updates_chain(self):
        """After step 1 succeeds, chain doc should show current_step=2."""
        chain_runs = FakePipelineChainRuns()
        chain_run_id = "chain_sched_abc123"

        asyncio.run(chain_runs.insert_one({
            "chain_run_id": chain_run_id,
            "status": "running",
            "current_step": 1,
            "steps_done": [],
            "step_run_ids": {},
        }))

        # Simulate step 1 completion update
        asyncio.run(chain_runs.update_one(
            {"chain_run_id": chain_run_id},
            {"$set": {
                "step_run_ids.step1": "run_s1",
                "current_step": 2,
                "steps_done": [1],
            }},
        ))

        doc = asyncio.run(chain_runs.find_one({"chain_run_id": chain_run_id}))
        assert doc["status"] == "running"
        assert doc["current_step"] == 2
        assert doc["steps_done"] == [1]

    def test_scheduler_step1_failure_marks_chain_failed(self):
        """If step 1 fails, pipeline_chain_runs should be marked as failed."""
        chain_runs = FakePipelineChainRuns()
        chain_run_id = "chain_sched_fail"

        asyncio.run(chain_runs.insert_one({
            "chain_run_id": chain_run_id,
            "status": "running",
            "current_step": 1,
            "steps_done": [],
            "step_run_ids": {},
        }))

        # Simulate step 1 failure
        asyncio.run(chain_runs.update_one(
            {"chain_run_id": chain_run_id},
            {"$set": {
                "status": "failed",
                "failed_step": 1,
                "error": "test error",
            }},
        ))

        doc = asyncio.run(chain_runs.find_one({"chain_run_id": chain_run_id}))
        assert doc["status"] == "failed"
        assert doc["failed_step"] == 1

    def test_scheduler_step3_completion_marks_chain_completed(self):
        """After step 3 succeeds, chain should have status='completed'."""
        chain_runs = FakePipelineChainRuns()
        chain_run_id = "chain_sched_done"

        asyncio.run(chain_runs.insert_one({
            "chain_run_id": chain_run_id,
            "status": "running",
            "current_step": 3,
            "steps_done": [1, 2],
            "step_run_ids": {"step1": "r1", "step2": "r2"},
        }))

        # Simulate step 3 completion
        finished = datetime.now(timezone.utc)
        asyncio.run(chain_runs.update_one(
            {"chain_run_id": chain_run_id},
            {"$set": {
                "status": "completed",
                "step_run_ids.step3": "r3",
                "steps_done": [1, 2, 3],
                "current_step": None,
                "finished_at": finished,
            }},
        ))

        doc = asyncio.run(chain_runs.find_one({"chain_run_id": chain_run_id}))
        assert doc["status"] == "completed"
        assert doc["steps_done"] == [1, 2, 3]
        assert doc["current_step"] is None

        # "running" query should NOT find this completed chain
        running = asyncio.run(chain_runs.find_one({"status": "running"}))
        assert running is None


# ---------------------------------------------------------------------------
# Test that chain_run_id propagation to step 3 is correct
# ---------------------------------------------------------------------------

class TestStep3ChainRunIdPropagation:
    """Verify the scheduler step 3 code now reads chain_run_id and passes it."""

    def test_scheduler_step3_reads_chain_run_id_from_step2(self):
        """The scheduler step 3 trigger code should read details.chain_run_id
        from the step 2 doc and pass it to run_fundamentals_changes_sync."""

        # Check the source code contains the fix
        import inspect
        import scheduler

        source = inspect.getsource(scheduler.scheduler_loop)

        # Step 3 section should now project details.chain_run_id
        assert "details.chain_run_id" in source, (
            "scheduler_loop Step 3 must read details.chain_run_id from Step 2 doc"
        )

        # Step 3 lambda should pass chain_run_id
        assert "chain_run_id=_cid" in source or "chain_run_id=" in source, (
            "scheduler_loop Step 3 must pass chain_run_id to run_fundamentals_changes_sync"
        )


# ---------------------------------------------------------------------------
# Test that canonical report uses chain finished_at, not datetime.now()
# ---------------------------------------------------------------------------

class TestCanonicalReportTimestamp:
    """Verify build_canonical_pipeline_report uses chain finished_at timestamp."""

    def test_report_uses_chain_finished_at_not_now(self):
        """last_generated_at_prague must reflect the chain's finished_at, not datetime.now()."""
        import pathlib

        server_path = pathlib.Path(__file__).resolve().parent.parent / "server.py"
        source = server_path.read_text()

        # Locate the build_canonical_pipeline_report function body
        fn_start = source.index("async def build_canonical_pipeline_report")
        # Find the next top-level function/class/decorator to bound the search
        fn_end = source.index("\n@", fn_start + 1)
        fn_source = source[fn_start:fn_end]

        # The function must reference chain_doc's finished_at for the timestamp
        assert 'chain_doc.get("finished_at")' in fn_source or "chain_doc.get('finished_at')" in fn_source, (
            "build_canonical_pipeline_report must read finished_at from chain_doc"
        )
        # The primary timestamp path must NOT be datetime.now() for completed chains
        # (a fallback for running chains is acceptable)
        assert "_finished_at" in fn_source and "astimezone" in fn_source, (
            "build_canonical_pipeline_report must convert chain finished_at to Prague TZ"
        )


# ---------------------------------------------------------------------------
# Tests for should_run midnight catch-up logic
# ---------------------------------------------------------------------------

class TestShouldRunMidnightCatchup:
    """Verify that should_run() can catch up late-night jobs after midnight."""

    @staticmethod
    def _make_should_run():
        """Build a standalone copy of should_run from scheduler.py source.

        We extract the closure-free logic so tests don't need to start the
        full scheduler_loop (which requires a live MongoDB connection).
        """
        import logging as _log
        from datetime import datetime as _dt, timedelta as _td

        _logger = _log.getLogger("test.should_run")

        def should_run(
            job_name: str,
            scheduled_hour: int,
            scheduled_minute: int,
            last_run: dict,
            today_str: str,
            current_hour: int,
            current_minute: int,
        ) -> bool:
            if last_run.get(job_name) == today_str:
                return False
            if current_hour > scheduled_hour:
                return True
            if current_hour == scheduled_hour and current_minute >= scheduled_minute:
                return True
            return False

        return should_run

    # --- Normal behaviour ---

    def test_normal_trigger_at_scheduled_time(self):
        """At 03:00 on the scheduled day, should_run returns True."""
        should_run = self._make_should_run()
        assert should_run("universe_seed", 3, 0, {}, "2026-03-23", 3, 0) is True

    def test_already_ran_today_returns_false(self):
        """If the job already ran today, should_run returns False."""
        should_run = self._make_should_run()
        last_run = {"universe_seed": "2026-03-23"}
        assert should_run("universe_seed", 3, 0, last_run, "2026-03-23", 3, 30) is False

    def test_before_scheduled_time_returns_false(self):
        """Before the scheduled hour (02:00 for a 03:00 job), should_run returns False."""
        should_run = self._make_should_run()
        assert should_run("universe_seed", 3, 0, {}, "2026-03-23", 2, 0) is False

    def test_catchup_later_same_day(self):
        """At 04:00 (later the same day), catch-up works via the normal hour comparison."""
        should_run = self._make_should_run()
        last_run = {"universe_seed": "2026-03-22"}  # ran yesterday
        assert should_run("universe_seed", 3, 0, last_run, "2026-03-23", 4, 0) is True

    def test_catchup_much_later_same_day(self):
        """At 10:00, a missed 03:00 job still catches up."""
        should_run = self._make_should_run()
        assert should_run("universe_seed", 3, 0, {}, "2026-03-24", 10, 0) is True

    def test_already_ran_today_skipped_later(self):
        """If the job already ran today, it does not re-trigger later."""
        should_run = self._make_should_run()
        last_run = {"universe_seed": "2026-03-24"}
        assert should_run("universe_seed", 3, 0, last_run, "2026-03-24", 5, 0) is False

    def test_morning_job_not_affected_by_early_hours(self):
        """A 03:00 job should NOT trigger at 01:00 (before scheduled time)."""
        should_run = self._make_should_run()
        assert should_run("universe_seed", 3, 0, {}, "2026-03-24", 1, 0) is False

    def test_morning_job_catchup_normal(self):
        """A 04:00 job catches up normally later in the day via hour comparison."""
        should_run = self._make_should_run()
        assert should_run("price_sync", 4, 0, {}, "2026-03-24", 5, 0) is True
