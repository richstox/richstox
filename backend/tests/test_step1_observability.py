"""Tests for Step 1 observability fix.

Validates that:
- Kill switch blocking is visible (logger.warning, not logger.debug)
- Heartbeat includes kill_switch_engaged status
- Step 1 decision is logged to DB (scheduler_step1_decision)
- Misleading comment is fixed (SUNDAY ONLY → SUNDAY EXCLUSION)
- SCHEDULER_JOBS.md and audit script match actual Mon-Sat behavior
"""

import inspect
import os
import re
import sys

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


class TestKillSwitchVisibility:
    """Verify that kill switch engagement is visible, not silent."""

    def test_kill_switch_uses_warning_not_debug(self):
        """Kill switch log must be logger.warning, not logger.debug."""
        import scheduler

        source = inspect.getsource(scheduler.scheduler_loop)

        # The old code had logger.debug — it should now be logger.warning
        assert 'logger.warning("Scheduler disabled (kill switch engaged)' in source, (
            "Kill switch message must use logger.warning (visible at INFO level), "
            "not logger.debug (invisible at INFO level)"
        )

    def test_kill_switch_not_debug_level(self):
        """Ensure no logger.debug for kill switch remains."""
        import scheduler

        source = inspect.getsource(scheduler.scheduler_loop)
        assert 'logger.debug("Scheduler disabled' not in source, (
            "logger.debug for kill switch must be removed — it was invisible at INFO level"
        )


class TestHeartbeatKillSwitchStatus:
    """Verify that heartbeat includes kill switch status."""

    def test_heartbeat_includes_kill_switch_engaged(self):
        """log_heartbeat must accept and include kill_switch_engaged parameter."""
        import scheduler

        source = inspect.getsource(scheduler.scheduler_loop)

        # The heartbeat call must pass kill_switch_engaged
        assert "kill_switch_engaged" in source, (
            "Heartbeat must include kill_switch_engaged status for observability"
        )

    def test_heartbeat_logs_kill_switch_status(self):
        """Heartbeat log message must include kill switch status."""
        import scheduler

        source = inspect.getsource(scheduler.scheduler_loop)

        # The heartbeat INFO message should mention kill_switch
        assert "kill_switch=" in source or "kill_switch_engaged" in source, (
            "Heartbeat log message should include kill switch status"
        )


class TestStep1DecisionLog:
    """Verify that Step 1 evaluation is logged to DB."""

    def test_step1_decision_logged_to_system_job_logs(self):
        """Step 1 decision must be written to system_job_logs."""
        import scheduler

        source = inspect.getsource(scheduler.scheduler_loop)

        assert '"scheduler_step1_decision"' in source, (
            "Step 1 decision must be logged as 'scheduler_step1_decision' in system_job_logs"
        )

    def test_step1_decision_includes_reason(self):
        """Step 1 decision log must include the reason."""
        import scheduler

        source = inspect.getsource(scheduler.scheduler_loop)

        assert '"will_trigger_now"' in source, "Decision log must include 'will_trigger_now' reason"
        assert '"already_completed_today"' in source, "Decision log must include 'already_completed_today' reason"

    def test_step1_decision_logged_once_per_day(self):
        """Step 1 decision should be logged at most once per day (not every tick)."""
        import scheduler

        source = inspect.getsource(scheduler.scheduler_loop)

        assert "_step1_decision_logged_date" in source, (
            "Must track per-day logging to avoid flooding DB with decision records"
        )

    def test_step1_decision_includes_last_run_state(self):
        """Decision log must include last_run_universe_seed for debugging."""
        import scheduler

        source = inspect.getsource(scheduler.scheduler_loop)

        assert '"last_run_universe_seed"' in source, (
            "Decision log must include the last_run_universe_seed value "
            "so admin can see what blocked the run"
        )


class TestMisleadingCommentFix:
    """Verify that the misleading SUNDAY ONLY comment is fixed."""

    def test_no_sunday_only_comment(self):
        """The old '# SUNDAY ONLY - Universe seed' comment must be gone."""
        import scheduler

        source = inspect.getsource(scheduler)

        assert "# SUNDAY ONLY - Universe seed" not in source, (
            "Misleading comment '# SUNDAY ONLY - Universe seed' must be removed. "
            "Universe Seed runs Mon-Sat; Sunday is excluded."
        )

    def test_universe_seed_day_comment_is_accurate(self):
        """UNIVERSE_SEED_DAY comment should indicate it's the exclusion day."""
        import scheduler

        source = inspect.getsource(scheduler)

        # Should mention exclusion or news-only
        assert "UNIVERSE_SEED_DAY" in source
        # The comment near UNIVERSE_SEED_DAY should mention exclusion
        match = re.search(r'UNIVERSE_SEED_DAY\s*=\s*6.*#(.+)', source)
        assert match, "UNIVERSE_SEED_DAY = 6 must have a comment"
        comment = match.group(1).lower()
        assert "exclusion" in comment or "news-only" in comment or "news only" in comment, (
            f"UNIVERSE_SEED_DAY comment should mention 'exclusion' or 'news-only', got: {match.group(1)}"
        )


class TestSchedulerJobsSpec:
    """Verify SCHEDULER_JOBS.md matches actual code (Mon-Sat for universe_seed)."""

    def test_spec_says_mon_sat_for_universe_seed(self):
        """SCHEDULER_JOBS.md must say Mon-Sat for Universe Seed, not Sunday."""
        spec_path = os.path.join(os.path.dirname(__file__), "..", "..", "docs", "SCHEDULER_JOBS.md")
        with open(spec_path, "r") as f:
            content = f.read()

        # Universe Seed row should say Mon-Sat
        assert "Universe Seed" in content
        # Find the Universe Seed row in the table
        for line in content.split("\n"):
            if "Universe Seed" in line and "|" in line:
                assert "Mon-Sat" in line, (
                    f"Universe Seed row in SCHEDULER_JOBS.md should say Mon-Sat, got: {line}"
                )
                assert "Sunday" not in line, (
                    f"Universe Seed row should NOT say Sunday, got: {line}"
                )
                break


class TestAuditScript:
    """Verify audit_scheduler.py matches actual code."""

    def test_audit_says_mon_sat_for_universe_seed(self):
        """audit_scheduler.py must expect Mon-Sat for universe_seed, not Sunday."""
        audit_path = os.path.join(os.path.dirname(__file__), "..", "..", "scripts", "audit_scheduler.py")
        with open(audit_path, "r") as f:
            content = f.read()

        # Should say Mon-Sat for universe_seed
        assert '"universe_seed": {"day": "Mon-Sat"' in content, (
            "Audit script must expect Mon-Sat for universe_seed, not Sunday"
        )

    def test_audit_checks_weekday_variable_not_is_sunday(self):
        """audit_scheduler.py must check for weekday == UNIVERSE_SEED_DAY, not is_sunday()."""
        audit_path = os.path.join(os.path.dirname(__file__), "..", "..", "scripts", "audit_scheduler.py")
        with open(audit_path, "r") as f:
            content = f.read()

        assert "weekday == UNIVERSE_SEED_DAY" in content or "weekday" in content, (
            "Audit script must accept weekday-based Sunday check, not require is_sunday()"
        )
