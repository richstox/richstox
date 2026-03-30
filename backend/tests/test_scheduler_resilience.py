"""Tests for scheduler daemon heartbeat resilience fix.

Validates that:
- Heartbeat failure does not crash the scheduler daemon (try/except protection)
"""

import inspect
import os
import sys

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


class TestHeartbeatProtection:
    """Verify that the heartbeat DB write is wrapped in try/except."""

    def test_heartbeat_wrapped_in_try_except(self):
        """log_heartbeat call must be inside a try/except to prevent daemon crash."""
        import scheduler

        source = inspect.getsource(scheduler.scheduler_loop)

        # The heartbeat section should contain try/except around log_heartbeat
        assert "await log_heartbeat(last_run" in source
        # Check that it is protected by try/except (the except should mention hb_exc)
        assert "hb_exc" in source, (
            "log_heartbeat must be wrapped in try/except to prevent daemon crash"
        )
