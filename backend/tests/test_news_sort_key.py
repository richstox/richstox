"""
Regression test for _pub_sort_key sorting in /api/news.

Reproduces the bug where a local `from datetime import datetime` inside
get_news() shadowed the module-level import, making the nested
_pub_sort_key closure raise:

    NameError: cannot access free variable 'datetime' in enclosing scope

The fix removes the redundant local import so the module-level
`from datetime import datetime` is used everywhere in the function.
"""

import importlib
import types

import pytest


def _build_sort_key():
    """
    Import the actual sort-key logic from server.py by extracting the
    closure-free equivalent.  We replicate the function body here so
    the test stays self-contained and doesn't need a running FastAPI app.
    """
    from datetime import datetime, timezone  # module-level style, same as server.py:54

    _DT_MIN = datetime.min.replace(tzinfo=timezone.utc)

    def _pub_sort_key(art):
        pub = art.get("published_at") or art.get("date")
        if isinstance(pub, str):
            try:
                return datetime.fromisoformat(pub.replace("Z", "+00:00"))
            except (ValueError, TypeError):
                return _DT_MIN
        if isinstance(pub, datetime):
            return pub if pub.tzinfo else pub.replace(tzinfo=timezone.utc)
        return _DT_MIN

    return _pub_sort_key


class TestPubSortKey:
    """Unit tests for the _pub_sort_key helper used in GET /api/news."""

    def test_iso_string_no_exception(self):
        """Sorting articles with ISO datetime strings must not raise."""
        key_fn = _build_sort_key()
        articles = [
            {"published_at": "2025-04-10T14:30:00Z"},
            {"published_at": "2025-04-11T09:00:00+00:00"},
            {"published_at": "2025-04-09T22:15:00Z"},
        ]
        # Must not raise
        articles.sort(key=key_fn, reverse=True)
        # Newest first
        assert articles[0]["published_at"] == "2025-04-11T09:00:00+00:00"
        assert articles[-1]["published_at"] == "2025-04-09T22:15:00Z"

    def test_date_field_fallback(self):
        """Articles with 'date' instead of 'published_at' should sort."""
        key_fn = _build_sort_key()
        articles = [
            {"date": "2025-01-01T00:00:00Z"},
            {"date": "2025-06-15T12:00:00Z"},
        ]
        articles.sort(key=key_fn, reverse=True)
        assert articles[0]["date"] == "2025-06-15T12:00:00Z"

    def test_missing_date_sorts_to_end(self):
        """Articles with no date at all should sort to the end."""
        key_fn = _build_sort_key()
        articles = [
            {},
            {"published_at": "2025-04-10T14:30:00Z"},
        ]
        articles.sort(key=key_fn, reverse=True)
        assert articles[0]["published_at"] == "2025-04-10T14:30:00Z"

    def test_datetime_object_supported(self):
        """Articles where published_at is already a datetime object."""
        from datetime import datetime, timezone

        key_fn = _build_sort_key()
        dt_old = datetime(2025, 1, 1, tzinfo=timezone.utc)
        dt_new = datetime(2025, 6, 1, tzinfo=timezone.utc)
        articles = [
            {"published_at": dt_old},
            {"published_at": dt_new},
        ]
        articles.sort(key=key_fn, reverse=True)
        assert articles[0]["published_at"] == dt_new

    def test_malformed_string_no_crash(self):
        """Malformed date strings must not crash the sort."""
        key_fn = _build_sort_key()
        articles = [
            {"published_at": "not-a-date"},
            {"published_at": "2025-04-10T14:30:00Z"},
        ]
        articles.sort(key=key_fn, reverse=True)
        assert articles[0]["published_at"] == "2025-04-10T14:30:00Z"


class TestServerSortKeyImport:
    """
    Verify that server.py can be parsed and that the get_news function
    no longer contains a local datetime import that would shadow the
    module-level import and break the _pub_sort_key closure.
    """

    def test_no_local_datetime_import_in_get_news(self):
        """
        Scan the get_news function body in server.py for a local
        `from datetime import datetime` that would shadow the module
        import and break the nested _pub_sort_key closure.
        """
        import ast
        from pathlib import Path

        server_path = Path(__file__).resolve().parent.parent / "server.py"
        source = server_path.read_text()
        tree = ast.parse(source)

        for node in ast.walk(tree):
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            if node.name != "get_news":
                continue

            # Walk the function body looking for local datetime imports
            for child in ast.walk(node):
                if isinstance(child, ast.ImportFrom) and child.module == "datetime":
                    imported_names = [alias.name for alias in child.names]
                    assert "datetime" not in imported_names, (
                        f"server.py:{child.lineno} — get_news still has "
                        f"'from datetime import datetime' which shadows the "
                        f"module-level import and breaks _pub_sort_key"
                    )
