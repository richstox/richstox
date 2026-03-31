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


class TestApiCallGuardRedactionInSource:
    """Verify that the guard source code applies redact_secrets."""

    def _get_guard_block(self):
        source = open(
            os.path.join(os.path.dirname(__file__), "..", "server.py")
        ).read()
        start = source.index("async def startup_api_call_guard")
        try:
            next_block = source.index("\n@app.on_event", start + 1)
        except ValueError:
            next_block = len(source)
        return source[start:next_block]

    def test_redact_secrets_called_before_logging(self):
        """redact_secrets must be applied in the guard before logger.critical."""
        block = self._get_guard_block()
        assert "redact_secrets" in block, (
            "startup_api_call_guard must call redact_secrets"
        )
        # redact_secrets import/call must appear before the critical log
        redact_pos = block.index("redact_secrets")
        critical_pos = block.index("logger.critical")
        assert redact_pos < critical_pos, (
            "redact_secrets must be applied before the CRITICAL log line"
        )

    def test_redact_applied_to_stored_stdout(self):
        """Stored stdout in audit_result must be redacted."""
        block = self._get_guard_block()
        # The stdout value should be wrapped with redact_secrets
        assert "redact_secrets(result.stdout" in block, (
            "audit_result stdout must be redacted before storage"
        )

    def test_redact_applied_to_stored_stderr(self):
        """Stored stderr in audit_result must be redacted."""
        block = self._get_guard_block()
        assert "redact_secrets(result.stderr" in block, (
            "audit_result stderr must be redacted before storage"
        )


class TestRedactSecretsFunction:
    """Unit tests for the redact_secrets helper."""

    @staticmethod
    def _redact(text):
        from utils.redact import redact_secrets
        return redact_secrets(text)

    # ---- URL query params ----

    def test_url_api_token_param(self):
        """api_token=... in a URL must be redacted."""
        url = "https://eodhd.com/api/eod/AAPL?api_token=abc123def456&fmt=json"
        result = self._redact(url)
        assert "abc123def456" not in result
        assert "api_token=" in result
        assert "[REDACTED]" in result
        # host + path preserved
        assert "eodhd.com/api/eod/AAPL" in result
        assert "fmt=json" in result

    def test_url_key_param(self):
        """key=... in a URL must be redacted."""
        url = "https://api.example.com/v1?key=MYSECRETKEY123&q=test"
        result = self._redact(url)
        assert "MYSECRETKEY123" not in result
        assert "key=" in result
        assert "[REDACTED]" in result

    def test_url_apiKey_param(self):
        """apiKey=... must be redacted."""
        url = "https://example.com?apiKey=sk_live_abcdef"
        result = self._redact(url)
        assert "sk_live_abcdef" not in result
        assert "apiKey=" in result

    def test_url_secret_param(self):
        """secret=... must be redacted."""
        text = "secret=verySecretValue123"
        result = self._redact(text)
        assert "verySecretValue123" not in result
        assert "secret=" in result

    def test_url_client_secret_param(self):
        """client_secret=... must be redacted."""
        text = "client_secret=cs_789xyz"
        result = self._redact(text)
        assert "cs_789xyz" not in result
        assert "client_secret=" in result

    def test_url_signature_param(self):
        """signature=... must be redacted."""
        text = "signature=abcdef1234567890"
        result = self._redact(text)
        assert "abcdef1234567890" not in result
        assert "signature=" in result

    # ---- Authorization headers ----

    def test_authorization_bearer(self):
        """Authorization: Bearer ... must be redacted."""
        text = "Authorization: Bearer eyJhbGciOiJIUzI1Ni"
        result = self._redact(text)
        assert "eyJhbGciOiJIUzI1Ni" not in result
        assert "Authorization: Bearer" in result
        assert "[REDACTED]" in result

    def test_authorization_token(self):
        """Authorization: token ... must be redacted."""
        text = "Authorization: token ghp_abc123def456"
        result = self._redact(text)
        assert "ghp_abc123def456" not in result
        assert "Authorization: token" in result

    # ---- Cookie headers ----

    def test_cookie_header(self):
        """Cookie: ... must be redacted."""
        text = "Cookie: session=abc123; user_id=42"
        result = self._redact(text)
        assert "session=abc123" not in result
        assert "Cookie:" in result
        assert "[REDACTED]" in result

    def test_set_cookie_header(self):
        """Set-Cookie: ... must be redacted."""
        text = "Set-Cookie: token=xyz789; Path=/"
        result = self._redact(text)
        assert "token=xyz789" not in result
        assert "Set-Cookie:" in result

    def test_x_api_key_header(self):
        """X-API-Key: ... must be redacted."""
        text = "X-API-Key: sk_live_abcdef123"
        result = self._redact(text)
        assert "sk_live_abcdef123" not in result
        assert "X-API-Key:" in result

    # ---- JWT tokens ----

    def test_jwt_like_token(self):
        """JWT-like three-segment token must be replaced with [REDACTED_JWT]."""
        jwt = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.dozjgNryP4J3jVmNHl0w5N_XgL0n3I9PlFUP0THsR8U"
        text = f"Token found: {jwt}"
        result = self._redact(text)
        assert jwt not in result
        assert "[REDACTED_JWT]" in result
        assert "Token found:" in result

    # ---- Preserving non-secret context ----

    def test_preserves_host_and_path(self):
        """Host + path must survive redaction."""
        text = "GET https://eodhd.com/api/eod/AAPL?api_token=secret123&fmt=json"
        result = self._redact(text)
        assert "eodhd.com/api/eod/AAPL" in result
        assert "GET" in result
        assert "fmt=json" in result

    def test_preserves_http_method(self):
        """HTTP method must survive."""
        text = "POST /api/auth token=secretval"
        result = self._redact(text)
        assert "POST" in result
        assert "/api/auth" in result
        assert "secretval" not in result

    def test_preserves_stack_trace(self):
        """Stack trace lines without secrets remain unchanged."""
        trace = '  File "/app/server.py", line 42, in handler\n    raise ValueError("bad")'
        result = self._redact(trace)
        assert result == trace

    def test_does_not_overredact_ordinary_words(self):
        """Ordinary prose containing 'token' or 'key' words is NOT redacted."""
        text = "The token endpoint returned an error. Check your key pair."
        result = self._redact(text)
        # No redaction markers should appear – these are ordinary prose
        assert "[REDACTED]" not in result
        assert "[REDACTED_JWT]" not in result

    def test_does_not_redact_version_numbers(self):
        """Dotted version strings like 1234.5678.9012 must NOT be redacted."""
        text = "Running version 1234.5678.9012 on server"
        result = self._redact(text)
        assert result == text

    # ---- Empty / None handling ----

    def test_empty_string(self):
        """Empty string returns empty string."""
        assert self._redact("") == ""

    def test_none_passthrough(self):
        """None is returned as-is (guard uses 'if result.stdout')."""
        from utils.redact import redact_secrets
        assert redact_secrets(None) is None

    # ---- Regression: combined stdout + stderr ----

    def test_redaction_after_combining_stdout_stderr(self):
        """Redaction must work on combined stdout + stderr content."""
        stdout = "Violation in server.py line 10: url=https://api.com?api_token=LEAKED\n"
        stderr = "Warning: Authorization: Bearer ghp_secret123\n"
        combined = stdout + stderr
        result = self._redact(combined)
        assert "LEAKED" not in result
        assert "ghp_secret123" not in result
        # Useful context preserved
        assert "server.py" in result
        assert "line 10" in result

    # ---- Regression: fallback exit code ----

    def test_fallback_exit_code_not_redacted(self):
        """Fallback with exit code must NOT be redacted (no secrets)."""
        fallback = "Script exited with code 1 but produced no output"
        result = self._redact(fallback)
        assert result == fallback

    # ---- Case insensitivity ----

    def test_case_insensitive_header(self):
        """Headers should match case-insensitively."""
        text = "authorization: bearer my_secret_tok"
        result = self._redact(text)
        assert "my_secret_tok" not in result

    def test_case_insensitive_param(self):
        """Param names should match case-insensitively."""
        text = "API_TOKEN=secret123"
        result = self._redact(text)
        assert "secret123" not in result


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
