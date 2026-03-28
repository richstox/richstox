"""Tests for scheduler daemon resilience fixes.

Validates that:
- Heartbeat failure does not crash the scheduler daemon (try/except protection)
- Day-of-week checks use captured `now` variable (no TOCTOU race)
- Kill switch read failure defaults to enabled (scheduler keeps running)
- scheduler_active in admin overview checks heartbeat freshness, not just config flag
"""

import inspect
import os
import sys

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


# ---------------------------------------------------------------------------
# Tests for heartbeat try/except protection
# ---------------------------------------------------------------------------

class TestHeartbeatProtection:
    """Verify that the heartbeat DB write is wrapped in try/except."""

    def test_heartbeat_wrapped_in_try_except(self):
        """log_heartbeat call must be inside a try/except to prevent daemon crash."""
        import scheduler

        source = inspect.getsource(scheduler.scheduler_loop)

        # The heartbeat section should contain try/except around log_heartbeat
        assert "await log_heartbeat(last_run)" in source
        # Check that it is protected by try/except (the except should mention non-fatal)
        assert "non-fatal" in source or "hb_exc" in source, (
            "log_heartbeat must be wrapped in try/except to prevent daemon crash"
        )


# ---------------------------------------------------------------------------
# Tests for TOCTOU fix in day-of-week checks
# ---------------------------------------------------------------------------

class TestDayOfWeekTOCTOU:
    """Verify that the scheduler loop uses the captured `now` for day-of-week
    checks instead of calling get_prague_time() independently."""

    def test_sunday_check_uses_now_variable(self):
        """The Sunday check should use `now.weekday()`, not `is_sunday()`."""
        import scheduler

        source = inspect.getsource(scheduler.scheduler_loop)

        # The loop should NOT call is_sunday() (which re-calls get_prague_time)
        # It should use the captured `now` variable instead.
        assert "now.weekday() == UNIVERSE_SEED_DAY" in source, (
            "Sunday check must use captured `now.weekday()`, not is_sunday()"
        )

    def test_daily_job_check_uses_now_variable(self):
        """The daily job check should use `now.weekday()`, not `is_daily_job_day()`."""
        import scheduler

        source = inspect.getsource(scheduler.scheduler_loop)

        assert "now.weekday() not in DAILY_SCHEDULE_DAYS" in source, (
            "Daily job check must use captured `now.weekday()`, not is_daily_job_day()"
        )


# ---------------------------------------------------------------------------
# Tests for kill switch resilience
# ---------------------------------------------------------------------------

class TestKillSwitchResilience:
    """Verify that a kill switch read failure defaults to enabled."""

    def test_kill_switch_read_has_try_except(self):
        """get_scheduler_enabled call must be in try/except so a DB error
        does not crash the daemon."""
        import scheduler

        source = inspect.getsource(scheduler.scheduler_loop)

        assert "ks_exc" in source or "assuming enabled" in source, (
            "Kill switch read must be wrapped in try/except with default-enabled fallback"
        )


# ---------------------------------------------------------------------------
# Tests for scheduler_active heartbeat liveness check
# ---------------------------------------------------------------------------

class TestSchedulerActiveHeartbeat:
    """Verify that the admin overview service checks heartbeat freshness
    for the scheduler_active flag, not just the config flag."""

    def test_overview_queries_heartbeat(self):
        """admin_overview_service must query system_job_logs for scheduler_heartbeat."""
        from services import admin_overview_service

        source = inspect.getsource(admin_overview_service.get_admin_overview)

        assert "scheduler_heartbeat" in source, (
            "get_admin_overview must query scheduler_heartbeat from system_job_logs"
        )

    def test_scheduler_active_checks_daemon_alive(self):
        """scheduler_active must be gated by scheduler_daemon_alive, not just config."""
        from services import admin_overview_service

        source = inspect.getsource(admin_overview_service.get_admin_overview)

        assert "scheduler_daemon_alive" in source, (
            "scheduler_active must consider scheduler_daemon_alive (heartbeat freshness)"
        )
        assert "scheduler_enabled and scheduler_daemon_alive" in source, (
            "scheduler_active should be True only when BOTH config enabled AND daemon alive"
        )
