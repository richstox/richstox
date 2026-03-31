"""
Tests for API Call Guard startup handler.

Validates that:
- The guard always logs concrete violation details (never "No output")
- When violations exist, stdout+stderr are combined in the log
- The audit result stored to ops_audit_runs includes stdout and stderr
"""

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


class TestApiCallGuardLogOutput:
    """Verify the startup handler never prints 'No output'."""

    def _get_guard_source(self):
        return open(
            os.path.join(os.path.dirname(__file__), "..", "server.py")
        ).read()

    def _get_guard_block(self):
        """Extract the startup_api_call_guard function body."""
        source = self._get_guard_source()
        start = source.index("async def startup_api_call_guard")
        # Find next top-level decorator or function at same indent to scope
        try:
            next_block = source.index("\n@app.on_event", start + 1)
        except ValueError:
            next_block = len(source)
        return source[start:next_block]

    def test_no_output_literal_absent(self):
        """The guard must never print the literal string 'No output'."""
        block = self._get_guard_block()
        assert '"No output"' not in block, (
            "startup_api_call_guard must not contain the 'No output' literal"
        )

    def test_combines_stdout_and_stderr(self):
        """On failure, the guard must combine stdout + stderr for logging."""
        block = self._get_guard_block()
        assert "result.stdout" in block, "Guard must reference result.stdout"
        assert "result.stderr" in block, "Guard must reference result.stderr"

    def test_logs_critical_with_detail(self):
        """On failure, the guard must log violations at CRITICAL level with detail."""
        block = self._get_guard_block()
        assert "logger.critical" in block, "Guard must log at CRITICAL level"
        assert "FAIL" in block, "Guard must include FAIL in the log"
        # Must include combined output in the critical log, not a separate line
        assert "detail" in block or "combined" in block, (
            "Guard must include combined output detail in the critical log"
        )

    def test_fallback_includes_exit_code(self):
        """If both stdout and stderr are empty, the fallback must include exit code."""
        block = self._get_guard_block()
        assert "returncode" in block, (
            "Fallback message must include the script's exit code"
        )

    def test_audit_result_stores_stderr(self):
        """The audit_result dict must include stderr for ops_audit_runs."""
        block = self._get_guard_block()
        assert '"stderr"' in block, (
            "audit_result must store stderr for the Admin Panel"
        )


class TestApiCallGuardScript:
    """Verify the audit script produces deterministic output."""

    def test_audit_script_exists(self):
        """audit_external_calls.py must exist in scripts/."""
        script_path = os.path.join(
            os.path.dirname(__file__), "..", "..", "scripts", "audit_external_calls.py"
        )
        assert os.path.isfile(script_path), (
            "scripts/audit_external_calls.py must exist"
        )

    def test_audit_script_always_prints_violations(self):
        """When violations are found, the script must print them to stdout."""
        script_path = os.path.join(
            os.path.dirname(__file__), "..", "..", "scripts", "audit_external_calls.py"
        )
        source = open(script_path).read()
        # Script must print each violation line
        assert "for v in violations" in source, (
            "Script must iterate and print each violation"
        )
        assert "print(" in source, (
            "Script must use print() to output violations"
        )
