"""Tests for scheduler daemon launch from server startup.

Validates that:
- server.py has a startup event that launches scheduler_loop as a background task
- The startup handler imports from scheduler (not scheduler_service)
- The startup handler requires ENABLE_SCHEDULER_DAEMON env var (default OFF)
- The shutdown handler cancels the scheduler task and closes the MongoDB client
- _scheduler_task variable is defined at module level
"""

import asyncio
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


class TestSchedulerDaemonStartup:
    """Verify that server.py launches the scheduler daemon on startup."""

    def test_server_has_startup_scheduler_daemon_function(self):
        """server.py must define startup_scheduler_daemon as a startup event."""
        source = open(
            os.path.join(os.path.dirname(__file__), "..", "server.py")
        ).read()
        assert "async def startup_scheduler_daemon" in source, (
            "server.py must define startup_scheduler_daemon startup handler"
        )

    def test_startup_imports_scheduler_loop(self):
        """The startup handler must import scheduler_loop from scheduler module."""
        source = open(
            os.path.join(os.path.dirname(__file__), "..", "server.py")
        ).read()
        assert "from scheduler import scheduler_loop" in source, (
            "startup_scheduler_daemon must import scheduler_loop from scheduler"
        )

    def test_startup_creates_asyncio_task(self):
        """The startup handler must use asyncio.create_task to launch the scheduler."""
        source = open(
            os.path.join(os.path.dirname(__file__), "..", "server.py")
        ).read()
        assert "asyncio.create_task(scheduler_loop()" in source, (
            "startup must create an asyncio task for scheduler_loop"
        )

    def test_scheduler_task_variable_exists(self):
        """_scheduler_task module-level variable must be defined."""
        source = open(
            os.path.join(os.path.dirname(__file__), "..", "server.py")
        ).read()
        assert "_scheduler_task" in source, (
            "_scheduler_task variable must exist for task lifecycle management"
        )

    def test_shutdown_cancels_scheduler_task(self):
        """Shutdown handler must cancel the scheduler task."""
        source = open(
            os.path.join(os.path.dirname(__file__), "..", "server.py")
        ).read()
        assert "_scheduler_task.cancel()" in source, (
            "Shutdown must cancel the scheduler task"
        )

    def test_shutdown_closes_mongo_client(self):
        """Shutdown handler must still close the MongoDB client."""
        source = open(
            os.path.join(os.path.dirname(__file__), "..", "server.py")
        ).read()
        assert "client.close()" in source, (
            "Shutdown must close the MongoDB client"
        )

    def test_done_callback_logs_crash(self):
        """A done callback must be attached to surface task crashes."""
        source = open(
            os.path.join(os.path.dirname(__file__), "..", "server.py")
        ).read()
        assert "add_done_callback" in source, (
            "A done callback must be set to surface unexpected scheduler crashes"
        )

    def test_double_start_guard(self):
        """startup_scheduler_daemon must guard against starting twice."""
        source = open(
            os.path.join(os.path.dirname(__file__), "..", "server.py")
        ).read()
        assert "if _scheduler_task and not _scheduler_task.done():" in source, (
            "startup_scheduler_daemon must have a double-start guard"
        )


class TestSchedulerDaemonEnvGuard:
    """Verify the ENABLE_SCHEDULER_DAEMON env var guard."""

    def test_env_var_guard_exists(self):
        """startup_scheduler_daemon must check ENABLE_SCHEDULER_DAEMON env var."""
        source = open(
            os.path.join(os.path.dirname(__file__), "..", "server.py")
        ).read()
        assert "ENABLE_SCHEDULER_DAEMON" in source, (
            "startup_scheduler_daemon must check ENABLE_SCHEDULER_DAEMON env var"
        )

    def test_default_is_enabled(self):
        """Scheduler daemon must be ON by default (leader lock handles multi-replica)."""
        source = open(
            os.path.join(os.path.dirname(__file__), "..", "server.py")
        ).read()
        # The guard must use "true" as default so scheduler starts without explicit env var
        assert 'os.environ.get("ENABLE_SCHEDULER_DAEMON", "true")' in source, (
            'Must default to "true" so scheduler starts without explicit env var'
        )
        # Verify the import comes AFTER the env var check
        env_check_pos = source.index("ENABLE_SCHEDULER_DAEMON")
        import_pos = source.index("from scheduler import scheduler_loop")
        assert env_check_pos < import_pos, (
            "Env var check must come BEFORE the scheduler import"
        )

    def test_disabled_log_message(self):
        """When disabled, a clear log line must be emitted."""
        source = open(
            os.path.join(os.path.dirname(__file__), "..", "server.py")
        ).read()
        assert "Scheduler daemon DISABLED" in source, (
            "Must log 'Scheduler daemon DISABLED' when env var is not set"
        )

    def test_comment_documents_default_on(self):
        """The block comment must state that the daemon is ON by default."""
        source = open(
            os.path.join(os.path.dirname(__file__), "..", "server.py")
        ).read()
        # New default: ON (leader lock is the primary multi-replica guard)
        assert "Default is ON" in source or "default is on" in source.lower(), (
            "Block comment must document that daemon is ON by default"
        )


class TestSchedulerLoopImportable:
    """Verify that the scheduler module exports scheduler_loop correctly."""

    def test_scheduler_loop_is_async(self):
        """scheduler.scheduler_loop must be an async function."""
        import scheduler

        assert asyncio.iscoroutinefunction(scheduler.scheduler_loop), (
            "scheduler_loop must be an async coroutine function"
        )

    def test_scheduler_main_exists(self):
        """scheduler.main() must exist as standalone entry point."""
        import scheduler

        assert hasattr(scheduler, "main"), "scheduler.main must exist"
        assert callable(scheduler.main), "scheduler.main must be callable"


class TestSchedulerRuntimeProof:
    """Verify that scheduler_loop writes runtime evidence to ops_job_runs."""

    def _get_scheduler_source(self):
        return open(
            os.path.join(os.path.dirname(__file__), "..", "scheduler.py")
        ).read()

    def test_scheduler_started_entry(self):
        """scheduler_loop must write scheduler_started to ops_job_runs after lock."""
        source = self._get_scheduler_source()
        assert '"scheduler_started"' in source, (
            "scheduler_loop must insert scheduler_started into ops_job_runs"
        )
        assert "ops_job_runs" in source, (
            "Must write to ops_job_runs collection"
        )

    def test_scheduler_heartbeat_ops_job_runs(self):
        """log_heartbeat must write scheduler_heartbeat to ops_job_runs."""
        source = self._get_scheduler_source()
        # Find the heartbeat function and verify it writes to ops_job_runs
        hb_start = source.index("def log_heartbeat")
        hb_block = source[hb_start:hb_start + 2000]
        assert "ops_job_runs" in hb_block, (
            "log_heartbeat must mirror heartbeat to ops_job_runs"
        )
        assert '"scheduler_heartbeat"' in hb_block, (
            "ops_job_runs entry must use job_name scheduler_heartbeat"
        )

    def test_prague_timestamps_in_started(self):
        """scheduler_started entry must include Prague timestamps."""
        source = self._get_scheduler_source()
        # Find the scheduler_started block
        idx = source.index('"scheduler_started"')
        block = source[max(0, idx - 200):idx + 500]
        assert "prague" in block.lower(), (
            "scheduler_started entry must include Prague timestamp fields"
        )

    def test_prague_timestamps_in_heartbeat(self):
        """scheduler_heartbeat ops_job_runs entry must include Prague timestamps."""
        source = self._get_scheduler_source()
        hb_start = source.index("def log_heartbeat")
        hb_block = source[hb_start:hb_start + 2000]
        # Check for Prague timestamp fields in the ops_job_runs insert
        assert "started_at_prague" in hb_block, (
            "heartbeat ops_job_runs entry must include started_at_prague"
        )

    def test_best_effort_started_no_crash(self):
        """scheduler_started insert must be wrapped in try/except (best-effort)."""
        source = self._get_scheduler_source()
        idx = source.index('"scheduler_started"')
        # Look for try/except wrapping this insert
        preceding = source[max(0, idx - 300):idx]
        assert "try:" in preceding, (
            "scheduler_started insert must be inside try block (best-effort)"
        )

    def test_best_effort_heartbeat_no_crash(self):
        """heartbeat ops_job_runs insert must be wrapped in try/except (best-effort)."""
        source = self._get_scheduler_source()
        hb_start = source.index("def log_heartbeat")
        hb_block = source[hb_start:hb_start + 2000]
        # The ops_job_runs insert within heartbeat should be in try/except
        ops_idx = hb_block.index("db.ops_job_runs")
        preceding = hb_block[:ops_idx]
        assert "try:" in preceding, (
            "heartbeat ops_job_runs insert must be inside try block (best-effort)"
        )
