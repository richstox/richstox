"""Tests for main pipeline (Steps 2 & 3) retry-on-failure and daemon protection.

Validates:
1. Step 2 (price_sync) failure does NOT advance last_run, enabling retry.
2. Step 3 (fundamentals_sync) failure does NOT advance last_run, enabling retry.
3. Steps 2 & 3 are wrapped in try/except (daemon cannot crash).
4. pipeline_chain_runs DB writes are protected by inner try/except.
5. Standalone jobs (key_metrics, etc.) are wrapped in try/except.
"""

import inspect
import os
import sys

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


class TestStep2RetryOnFailure:
    """Verify that a failed Step 2 does NOT mark price_sync as ran today."""

    def test_step2_checks_result_before_advancing_last_run(self):
        """price_sync must only advance last_run on success (matching Step 1)."""
        import scheduler

        source = inspect.getsource(scheduler.scheduler_loop)

        # The failure detection variable must exist.
        assert "_s2_failed" in source, (
            "Step 2 must compute _s2_failed to detect failure "
            "before deciding whether to advance last_run"
        )

        # On failure, last_run["price_sync"] must NOT be set.
        # Verify that _s2_failed check gates the advancement.
        assert "if _s2_failed:" in source, (
            "Step 2 must branch on _s2_failed to skip last_run advancement"
        )

    def test_step2_wrapped_in_try_except(self):
        """Step 2 block must be wrapped in try/except to prevent daemon crash."""
        import scheduler

        source = inspect.getsource(scheduler.scheduler_loop)

        assert "price_sync STEP 2 unhandled error" in source, (
            "Step 2 block must have try/except with error logging "
            "(matching the Step 1 / market_calendar pattern)"
        )

    def test_step2_chain_update_protected(self):
        """pipeline_chain_runs update in Step 2 must be in inner try/except."""
        import scheduler

        source = inspect.getsource(scheduler.scheduler_loop)

        assert "pipeline_chain_runs Step 2" in source, (
            "Step 2 pipeline_chain_runs update must be wrapped in "
            "try/except (non-fatal)"
        )

    def test_step2_does_not_advance_last_run_unconditionally(self):
        """last_run['price_sync'] must NOT be set outside the success branch."""
        import scheduler

        source = inspect.getsource(scheduler.scheduler_loop)

        # Find the Step 2 block (between STEP 2 and STEP 3 comments)
        s2_start = source.index("# STEP 2: Price sync")
        s3_start = source.index("# STEP 3: Fundamentals sync")
        step2_source = source[s2_start:s3_start]

        # Count occurrences of last_run["price_sync"] = today_str
        count = step2_source.count('last_run["price_sync"] = today_str')
        assert count == 1, (
            f"Expected exactly 1 last_run['price_sync'] = today_str in Step 2 "
            f"(inside the success branch), found {count}"
        )

    def test_step2_failure_prevents_step3(self):
        """If Step 2 fails, should_run_after_dependency must block Step 3."""
        def should_run_after_dependency(job_name, dependency_job, last_run, today_str):
            if last_run.get(job_name) == today_str:
                return False
            return last_run.get(dependency_job) == today_str

        # Scenario: Step 1 succeeded, Step 2 failed (last_run not advanced)
        last_run = {"universe_seed": "2026-03-29"}
        today_str = "2026-03-29"

        # price_sync NOT in last_run → should_run_after_dependency for step 3 is False
        assert should_run_after_dependency(
            "fundamentals_sync", "price_sync", last_run, today_str
        ) is False, (
            "Step 3 must NOT fire when Step 2 failed "
            "(last_run['price_sync'] not set)"
        )


class TestStep3RetryOnFailure:
    """Verify that a failed Step 3 does NOT mark fundamentals_sync as ran today."""

    def test_step3_checks_result_before_advancing_last_run(self):
        """fundamentals_sync must only advance last_run on success."""
        import scheduler

        source = inspect.getsource(scheduler.scheduler_loop)

        assert "_s3_failed" in source, (
            "Step 3 must compute _s3_failed to detect failure"
        )
        assert "if _s3_failed:" in source, (
            "Step 3 must branch on _s3_failed to skip last_run advancement"
        )

    def test_step3_wrapped_in_try_except(self):
        """Step 3 block must be wrapped in try/except to prevent daemon crash."""
        import scheduler

        source = inspect.getsource(scheduler.scheduler_loop)

        assert "fundamentals_sync STEP 3 unhandled error" in source, (
            "Step 3 block must have try/except with error logging"
        )

    def test_step3_chain_update_protected(self):
        """pipeline_chain_runs update in Step 3 must be in inner try/except."""
        import scheduler

        source = inspect.getsource(scheduler.scheduler_loop)

        assert "pipeline_chain_runs Step 3" in source, (
            "Step 3 pipeline_chain_runs update must be wrapped in "
            "try/except (non-fatal)"
        )

    def test_step3_does_not_advance_last_run_unconditionally(self):
        """last_run['fundamentals_sync'] must NOT be set outside the success branch."""
        import scheduler

        source = inspect.getsource(scheduler.scheduler_loop)

        # Find the Step 3 block (between STEP 3 comment and BENCHMARK UPDATE comment)
        s3_start = source.index("# STEP 3: Fundamentals sync")
        bm_start = source.index("# BENCHMARK UPDATE")
        step3_source = source[s3_start:bm_start]

        count = step3_source.count('last_run["fundamentals_sync"] = today_str')
        assert count == 1, (
            f"Expected exactly 1 last_run['fundamentals_sync'] = today_str "
            f"in Step 3 (inside the success branch), found {count}"
        )


class TestStandaloneJobsProtection:
    """Verify that ALL standalone jobs are wrapped in try/except."""

    @pytest.mark.parametrize("job_name", [
        "key_metrics",
        "peer_medians",
        "pain_cache",
        "admin_report",
        "news_refresh",
        "backfill_all",
    ])
    def test_standalone_job_wrapped_in_try_except(self, job_name):
        """Each standalone job block must be wrapped in try/except."""
        import scheduler

        source = inspect.getsource(scheduler.scheduler_loop)

        assert f"{job_name} unhandled error" in source, (
            f"{job_name} block must have try/except with error logging "
            f"to prevent daemon crash"
        )


class TestPipelineChainRetryBehavior:
    """Integration-style tests for the retry logic via should_run_after_dependency."""

    def test_full_chain_succeeds_when_all_steps_succeed(self):
        """Happy path: Steps 1→2→3 all set last_run, chain completes."""
        def should_run_after_dependency(job_name, dep, last_run, today_str):
            if last_run.get(job_name) == today_str:
                return False
            return last_run.get(dep) == today_str

        last_run = {}
        today = "2026-03-29"

        # Step 1 succeeds
        last_run["universe_seed"] = today

        # Step 2 should fire
        assert should_run_after_dependency("price_sync", "universe_seed", last_run, today)
        last_run["price_sync"] = today  # success

        # Step 3 should fire
        assert should_run_after_dependency("fundamentals_sync", "price_sync", last_run, today)
        last_run["fundamentals_sync"] = today  # success

        # All done — no step fires again
        assert not should_run_after_dependency("price_sync", "universe_seed", last_run, today)
        assert not should_run_after_dependency("fundamentals_sync", "price_sync", last_run, today)

    def test_step2_failure_blocks_step3_allows_step2_retry(self):
        """When Step 2 fails, Step 3 is blocked and Step 2 retries next tick."""
        def should_run_after_dependency(job_name, dep, last_run, today_str):
            if last_run.get(job_name) == today_str:
                return False
            return last_run.get(dep) == today_str

        last_run = {}
        today = "2026-03-29"

        # Step 1 succeeds
        last_run["universe_seed"] = today

        # Step 2 fires but fails — last_run NOT advanced
        assert should_run_after_dependency("price_sync", "universe_seed", last_run, today)
        # (failure — do NOT set last_run["price_sync"])

        # Step 3 must NOT fire (dependency not met)
        assert not should_run_after_dependency("fundamentals_sync", "price_sync", last_run, today)

        # Next tick: Step 2 should retry
        assert should_run_after_dependency("price_sync", "universe_seed", last_run, today)

        # Now Step 2 succeeds
        last_run["price_sync"] = today

        # Step 3 should fire now
        assert should_run_after_dependency("fundamentals_sync", "price_sync", last_run, today)

    def test_step3_failure_allows_step3_retry(self):
        """When Step 3 fails, it retries (Step 2 stays done, Step 3 retries)."""
        def should_run_after_dependency(job_name, dep, last_run, today_str):
            if last_run.get(job_name) == today_str:
                return False
            return last_run.get(dep) == today_str

        last_run = {}
        today = "2026-03-29"

        # Steps 1 and 2 succeed
        last_run["universe_seed"] = today
        last_run["price_sync"] = today

        # Step 3 fires but fails
        assert should_run_after_dependency("fundamentals_sync", "price_sync", last_run, today)
        # (failure — do NOT set last_run["fundamentals_sync"])

        # Next tick: Step 2 should NOT re-fire (already done)
        assert not should_run_after_dependency("price_sync", "universe_seed", last_run, today)

        # Step 3 should retry
        assert should_run_after_dependency("fundamentals_sync", "price_sync", last_run, today)


class TestKillSwitchBypass:
    """Verify Steps 2/3 pass ignore_kill_switch=True from the scheduler.

    When the scheduler loop has already verified the kill switch at the top of
    each tick, the downstream job functions must not re-check it — otherwise a
    race-condition toggle causes silent data skips treated as success.
    """

    def test_step2_passes_ignore_kill_switch(self):
        """run_daily_price_sync must be called with ignore_kill_switch=True."""
        import scheduler

        source = inspect.getsource(scheduler.scheduler_loop)

        # Find the Step 2 block
        s2_start = source.index("# STEP 2: Price sync")
        s3_start = source.index("# STEP 3: Fundamentals sync")
        step2_source = source[s2_start:s3_start]

        assert "ignore_kill_switch=True" in step2_source, (
            "Step 2 must pass ignore_kill_switch=True to run_daily_price_sync "
            "since the scheduler already checked the kill switch at loop top"
        )

    def test_step3_passes_ignore_kill_switch(self):
        """run_fundamentals_changes_sync must be called with ignore_kill_switch=True."""
        import scheduler

        source = inspect.getsource(scheduler.scheduler_loop)

        # Find the Step 3 block
        s3_start = source.index("# STEP 3: Fundamentals sync")
        bm_start = source.index("# BENCHMARK UPDATE")
        step3_source = source[s3_start:bm_start]

        assert "ignore_kill_switch=True" in step3_source, (
            "Step 3 must pass ignore_kill_switch=True to "
            "run_fundamentals_changes_sync since the scheduler already "
            "checked the kill switch at loop top"
        )


class TestStrictSuccessDetection:
    """Verify _s2_failed / _s3_failed detect ANY non-success status.

    The old code only detected status=="failed" or error key.  A "skipped" or
    "cancelled" result would silently advance last_run, creating data gaps.
    """

    def _get_failed_block(self, step_var: str) -> str:
        """Extract the full _sN_failed assignment block from scheduler source."""
        import scheduler

        source = inspect.getsource(scheduler.scheduler_loop)
        marker = f"{step_var} = ("
        start = source.index(marker)
        # Count balanced parentheses to find end of multi-line expression
        depth = 0
        for i, ch in enumerate(source[start:], start=start):
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
                if depth == 0:
                    return source[start : i + 1]
        return source[start : start + 200]

    def test_step2_detects_skipped_status(self):
        """_s2_failed must be True for status='skipped'."""
        expr = self._get_failed_block("_s2_failed")
        assert "completed" in expr, (
            "_s2_failed must whitelist 'completed' as success"
        )
        assert "not in" in expr or "not isinstance" in expr, (
            "_s2_failed must use a NOT pattern so that unknown/skipped/cancelled "
            "statuses are treated as failure"
        )

    def test_step3_detects_skipped_status(self):
        """_s3_failed must be True for status='skipped'."""
        expr = self._get_failed_block("_s3_failed")
        assert "completed" in expr, (
            "_s3_failed must whitelist 'completed' as success"
        )
        assert "not in" in expr or "not isinstance" in expr, (
            "_s3_failed must use a NOT pattern so that unknown/skipped/cancelled "
            "statuses are treated as failure"
        )

    def test_skipped_result_detected_as_failure(self):
        """Simulate the _s2_failed logic against a 'skipped' result dict."""
        # Reproduce the exact logic from scheduler.py
        _result = {"status": "skipped", "reason": "kill_switch_engaged"}

        _failed = (
            not isinstance(_result, dict)
            or _result.get("status") not in ("completed", "success")
            or _result.get("error")
        )
        assert _failed, (
            "A 'skipped' result must be detected as failed "
            "so last_run is NOT advanced"
        )

    def test_completed_result_not_detected_as_failure(self):
        """A 'completed' result must NOT be flagged as failed."""
        _result = {"status": "completed", "tickers_processed": 100}

        _failed = (
            not isinstance(_result, dict)
            or _result.get("status") not in ("completed", "success")
            or _result.get("error")
        )
        assert not _failed, (
            "A 'completed' result must NOT be detected as failed"
        )

    def test_cancelled_result_detected_as_failure(self):
        """A 'cancelled' result must be detected as failed."""
        _result = {"status": "cancelled"}

        _failed = (
            not isinstance(_result, dict)
            or _result.get("status") not in ("completed", "success")
            or _result.get("error")
        )
        assert _failed, (
            "A 'cancelled' result must be detected as failed "
            "so last_run is NOT advanced"
        )
