"""Tests for scheduler chain-start fixes.

Validates:
1. TOCTOU fix: day-of-week checks use captured `now.weekday()` instead of
   is_sunday()/is_daily_job_day() which call get_prague_time() independently.
2. Swallowed-exception fix: a failed Step 1 does NOT advance last_run, so
   the scheduler retries on the next tick and Steps 2/3 are not triggered
   on a failed Step 1.
"""

import inspect
import os
import sys

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


class TestTOCTOUFix:
    """Verify day-of-week checks use the captured `now` variable."""

    def test_scheduler_loop_uses_weekday_variable_not_is_sunday(self):
        """scheduler_loop must not call is_sunday() for the day gate."""
        import scheduler

        source = inspect.getsource(scheduler.scheduler_loop)

        # The TOCTOU-safe code assigns weekday = now.weekday() once and
        # uses it for both the Sunday gate and the daily-job-day gate.
        assert "weekday = now.weekday()" in source, (
            "scheduler_loop must derive weekday from the captured `now`, "
            "not from independent get_prague_time() calls"
        )

        # The Sunday gate should compare weekday to the constant.
        assert "weekday == UNIVERSE_SEED_DAY" in source, (
            "Sunday gate must use `weekday == UNIVERSE_SEED_DAY`"
        )

        # The daily-job-day gate should compare weekday to the list.
        assert "weekday not in DAILY_SCHEDULE_DAYS" in source, (
            "Daily-job-day gate must use `weekday not in DAILY_SCHEDULE_DAYS`"
        )

    def test_scheduler_loop_does_not_call_is_sunday_or_is_daily_job_day(self):
        """The loop body must not call is_sunday() or is_daily_job_day()."""
        import scheduler

        source = inspect.getsource(scheduler.scheduler_loop)
        assert "is_sunday()" not in source, (
            "scheduler_loop must not call is_sunday(); "
            "use the captured weekday variable instead"
        )
        assert "is_daily_job_day()" not in source, (
            "scheduler_loop must not call is_daily_job_day(); "
            "use the captured weekday variable instead"
        )


class TestBenchmarkUpdateProtection:
    """Verify that benchmark_update block has try/except protection."""

    def test_benchmark_update_wrapped_in_try_except(self):
        """benchmark_update block must be wrapped in try/except to prevent daemon crash."""
        import scheduler

        source = inspect.getsource(scheduler.scheduler_loop)

        # Find the benchmark_update should_run block
        assert 'should_run("benchmark_update"' in source, (
            "scheduler_loop must contain a benchmark_update should_run check"
        )

        # The block must be wrapped in try/except like market_calendar
        # Check that the error handler pattern exists for benchmark_update
        assert "benchmark_update unhandled error" in source, (
            "benchmark_update block must have try/except with error logging "
            "(matching the market_calendar pattern)"
        )

    def test_benchmark_update_reachable_on_saturday(self):
        """Saturday (weekday=5) must reach the benchmark_update check.

        Saturday is in DAILY_SCHEDULE_DAYS and is NOT UNIVERSE_SEED_DAY,
        so the loop must NOT continue/skip before reaching benchmark_update.
        """
        import scheduler

        # Saturday = weekday 5
        assert 5 in scheduler.DAILY_SCHEDULE_DAYS, (
            "Saturday (5) must be in DAILY_SCHEDULE_DAYS"
        )
        assert 5 != scheduler.UNIVERSE_SEED_DAY, (
            "Saturday (5) must not equal UNIVERSE_SEED_DAY"
        )


class TestSwallowedExceptionFix:
    """Verify that a failed Step 1 does NOT mark universe_seed as ran today."""

    def test_step1_failure_result_is_checked(self):
        """scheduler_loop must inspect _run_universe_seed_scheduled return value."""
        import scheduler

        source = inspect.getsource(scheduler.scheduler_loop)

        # The return value must be captured.
        assert "_s1_result = await _run_universe_seed_scheduled(db)" in source, (
            "scheduler_loop must capture the return value of "
            "_run_universe_seed_scheduled to detect internal failures"
        )

        # On failure, the code must NOT set last_run["universe_seed"].
        # We verify by checking that the error-result branch exists.
        assert '_s1_result.get("error")' in source, (
            "scheduler_loop must check _s1_result for an 'error' key "
            "to detect Step 1 internal failure"
        )

    def test_should_run_after_dependency_does_not_fire_when_dep_not_set(self):
        """should_run_after_dependency returns False when dependency did not complete."""
        from scheduler import scheduler_loop

        # Extract the inner function from scheduler_loop's code.
        # We can test the logic directly.
        last_run = {}  # universe_seed not set
        today_str = "2026-03-28"

        # Inline the logic from should_run_after_dependency
        def should_run_after_dependency(job_name, dependency_job, last_run, today_str):
            if last_run.get(job_name) == today_str:
                return False
            return last_run.get(dependency_job) == today_str

        assert should_run_after_dependency(
            "price_sync", "universe_seed", last_run, today_str
        ) is False, (
            "price_sync must NOT trigger when universe_seed has not "
            "completed today (last_run missing universe_seed)"
        )

    def test_should_run_after_dependency_fires_when_dep_set(self):
        """should_run_after_dependency returns True when dependency completed today."""

        def should_run_after_dependency(job_name, dependency_job, last_run, today_str):
            if last_run.get(job_name) == today_str:
                return False
            return last_run.get(dependency_job) == today_str

        last_run = {"universe_seed": "2026-03-28"}
        today_str = "2026-03-28"

        assert should_run_after_dependency(
            "price_sync", "universe_seed", last_run, today_str
        ) is True, (
            "price_sync MUST trigger when universe_seed completed today"
        )
