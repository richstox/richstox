"""Tests for scheduler daemon launch from server startup.

Validates that:
- server.py has a startup event that launches scheduler_loop as a background task
- The startup handler imports from scheduler (not scheduler_service)
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
