"""Tests for scheduler distributed leader lock (multi-replica safety).

Validates that:
- scheduler.py exports leader lock functions (try_acquire, renew, release)
- scheduler_loop acquires the leader lock before entering the main loop
- scheduler_loop releases the lock in finally block
- scheduler_loop exits gracefully when lock is lost during renewal
- Lock uses ops_locks collection with TTL
- Lock constants are correct (90s TTL, "scheduler_leader" ID)
"""

import asyncio
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

SCHEDULER_PY = os.path.join(os.path.dirname(__file__), "..", "scheduler.py")
SERVER_PY = os.path.join(os.path.dirname(__file__), "..", "server.py")


def _read(path: str) -> str:
    with open(path) as f:
        return f.read()


class TestLeaderLockFunctions:
    """Verify scheduler.py exports the required leader lock API."""

    def test_try_acquire_scheduler_leader_lock_exists(self):
        import scheduler
        assert hasattr(scheduler, "try_acquire_scheduler_leader_lock")
        assert asyncio.iscoroutinefunction(scheduler.try_acquire_scheduler_leader_lock)

    def test_renew_scheduler_leader_lock_exists(self):
        import scheduler
        assert hasattr(scheduler, "renew_scheduler_leader_lock")
        assert asyncio.iscoroutinefunction(scheduler.renew_scheduler_leader_lock)

    def test_release_scheduler_leader_lock_exists(self):
        import scheduler
        assert hasattr(scheduler, "release_scheduler_leader_lock")
        assert asyncio.iscoroutinefunction(scheduler.release_scheduler_leader_lock)


class TestLeaderLockConstants:
    """Verify lock configuration constants."""

    def test_lock_id_is_scheduler_leader(self):
        import scheduler
        assert scheduler.SCHEDULER_LEADER_LOCK_ID == "scheduler_leader"

    def test_ttl_is_90_seconds(self):
        import scheduler
        assert scheduler.SCHEDULER_LEADER_TTL_SECONDS == 90


class TestSchedulerLoopLeaderLockIntegration:
    """Verify scheduler_loop uses the leader lock correctly."""

    def test_loop_acquires_lock_before_main_loop(self):
        """scheduler_loop must call try_acquire_scheduler_leader_lock before entering while True."""
        source = _read(SCHEDULER_PY)
        # Find the function body
        func_start = source.index("async def scheduler_loop():")
        lock_acquire = source.index("try_acquire_scheduler_leader_lock(db)", func_start)
        while_loop = source.index("while True:", func_start)
        assert lock_acquire < while_loop, (
            "try_acquire_scheduler_leader_lock must be called BEFORE the while True loop"
        )

    def test_loop_returns_if_lock_not_acquired(self):
        """If lock not acquired, scheduler_loop must return (not raise)."""
        source = _read(SCHEDULER_PY)
        func_start = source.index("async def scheduler_loop():")
        # After lock acquisition there should be a check and return
        assert "if not acquired:" in source[func_start:], (
            "scheduler_loop must check if lock was acquired"
        )
        # Check that return follows the not-acquired path
        not_acquired_idx = source.index("if not acquired:", func_start)
        # Next 200 chars should contain return
        snippet = source[not_acquired_idx:not_acquired_idx + 300]
        assert "return" in snippet, (
            "scheduler_loop must return if leader lock not acquired"
        )

    def test_loop_renews_lock_on_each_tick(self):
        """scheduler_loop must call renew_scheduler_leader_lock on each tick."""
        source = _read(SCHEDULER_PY)
        func_start = source.index("async def scheduler_loop():")
        while_start = source.index("while True:", func_start)
        # renew must be inside the while loop
        assert "renew_scheduler_leader_lock(db)" in source[while_start:], (
            "scheduler_loop must renew leader lock inside the while True loop"
        )

    def test_loop_exits_if_lock_lost_during_renewal(self):
        """If renewal fails (lost lock), scheduler_loop must break out of the loop."""
        source = _read(SCHEDULER_PY)
        func_start = source.index("async def scheduler_loop():")
        while_start = source.index("while True:", func_start)
        loop_body = source[while_start:]
        assert "if not still_leader:" in loop_body or "if not renew" in loop_body, (
            "scheduler_loop must check renewal result and exit if lock was lost"
        )
        # After the check, there should be a break
        renew_check = loop_body.index("still_leader")
        snippet = loop_body[renew_check:renew_check + 300]
        assert "break" in snippet, (
            "scheduler_loop must break if leader lock renewal fails"
        )

    def test_loop_releases_lock_in_finally(self):
        """scheduler_loop must release the leader lock in its finally block."""
        source = _read(SCHEDULER_PY)
        func_start = source.index("async def scheduler_loop():")
        finally_idx = source.index("finally:", func_start)
        finally_block = source[finally_idx:finally_idx + 400]
        assert "release_scheduler_leader_lock" in finally_block, (
            "scheduler_loop must release leader lock in its finally block"
        )


class TestServerMultiReplicaComment:
    """Verify server.py documents multi-replica safety."""

    def test_server_comment_mentions_leader_lock(self):
        source = _read(SERVER_PY)
        assert "leader lock" in source.lower() or "LEADER LOCK" in source or "leader_lock" in source, (
            "server.py should document the distributed leader lock for multi-replica safety"
        )

    def test_server_done_callback_handles_normal_completion(self):
        """Done callback should handle normal completion (lock not acquired → returns)."""
        source = _read(SERVER_PY)
        startup_idx = source.index("async def startup_scheduler_daemon")
        snippet = source[startup_idx:startup_idx + 2000]
        # The else branch (no exception, no cancellation) should exist
        assert "else:" in snippet or "task completed" in snippet.lower(), (
            "Done callback should handle normal completion when scheduler returns without lock"
        )


class TestLeaderLockUsesOpsLocksCollection:
    """Verify the leader lock uses the same ops_locks collection and TTL pattern."""

    def test_uses_ops_locks_collection(self):
        source = _read(SCHEDULER_PY)
        assert "db.ops_locks" in source, (
            "Leader lock must use the ops_locks collection"
        )

    def test_creates_ttl_index(self):
        source = _read(SCHEDULER_PY)
        assert "expires_at" in source, (
            "Leader lock must use expires_at field for TTL"
        )
        assert "expireAfterSeconds" in source, (
            "Leader lock must create a TTL index"
        )

    def test_uses_duplicate_key_error_for_contention(self):
        source = _read(SCHEDULER_PY)
        assert "DuplicateKeyError" in source, (
            "Leader lock must handle DuplicateKeyError for concurrent acquire"
        )

    def test_imports_duplicate_key_error(self):
        source = _read(SCHEDULER_PY)
        assert "from pymongo.errors import DuplicateKeyError" in source, (
            "scheduler.py must import DuplicateKeyError from pymongo.errors"
        )
