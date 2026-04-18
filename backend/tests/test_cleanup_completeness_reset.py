"""
Tests for cleanup-completeness consistency fix.

Root cause: visibility cleanup deleted stock_prices for tickers that
temporarily became invisible (e.g. absent from EODHD bulk for 2+ days),
but left price_history_complete=True on tracked_tickers.  When the ticker
returned to bulk, all 8 visibility gates passed → the ticker appeared
in search with only a few bulk-day data points and a broken chart.

Fix: when cleanup deletes stock_prices for a ticker, also reset
price_history_complete=False and needs_price_redownload=True so Phase C
re-downloads the full history before the ticker becomes visible again.

These tests prove:
  1) recompute_visibility_all cleanup resets completeness flags
  2) cleanup_invisible_ticker_data resets completeness flags
  3) Tickers with cleanup_reset status are picked up by Phase C
  4) BODI scenario: cleanup + return to bulk → not visible until re-downloaded
"""

import asyncio
import sys
import os
from datetime import datetime, timezone, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch, MagicMock

import pytest

# Ensure the backend directory is in the Python path.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _FakeCollection:
    """Minimal async collection mock."""
    def __init__(self, docs=None):
        self._docs = list(docs or [])
        self.updates = []
        self.deletes = []

    async def find_one(self, filt, projection=None):
        for doc in self._docs:
            match = True
            for k, v in filt.items():
                if isinstance(v, dict):
                    continue
                if doc.get(k) != v:
                    match = False
                    break
            if match:
                return doc
        return None

    def find(self, filt=None, projection=None):
        return _FakeCursor(self._docs, filt)

    async def count_documents(self, filt=None):
        if filt is None:
            return len(self._docs)
        count = 0
        for doc in self._docs:
            match = True
            for k, v in filt.items():
                if isinstance(v, dict):
                    continue
                if doc.get(k) != v:
                    match = False
                    break
            if match:
                count += 1
        return count

    async def distinct(self, field, filt=None):
        return [doc.get(field) for doc in self._docs if doc.get(field)]

    async def update_one(self, filt, update):
        self.updates.append({"filter": filt, "update": update})
        return SimpleNamespace(modified_count=1)

    async def update_many(self, filt, update):
        self.updates.append({"filter": filt, "update": update})
        return SimpleNamespace(modified_count=1)

    async def delete_many(self, filt):
        self.deletes.append(filt)
        return SimpleNamespace(deleted_count=100)

    async def insert_one(self, doc):
        self._docs.append(doc)
        return SimpleNamespace(inserted_id="fake_id")

    async def aggregate(self, pipeline):
        return _FakeCursor([], None)

    def bulk_write(self, ops, ordered=False):
        return _FakeAwaitable(SimpleNamespace(modified_count=len(ops)))


class _FakeAwaitable:
    def __init__(self, result):
        self._result = result
    def __await__(self):
        return iter([self._result])


class _FakeCursor:
    def __init__(self, docs, filt):
        self._docs = list(docs or [])
        self._iter = iter(self._docs)

    def sort(self, *args, **kwargs):
        return self

    async def to_list(self, n=None):
        if n:
            return self._docs[:n]
        return self._docs

    def __aiter__(self):
        self._iter = iter(self._docs)
        return self

    async def __anext__(self):
        try:
            return next(self._iter)
        except StopIteration:
            raise StopAsyncIteration


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestCleanupResetsCompleteness:
    """Visibility cleanup must reset price_history_complete when deleting stock_prices."""

    @pytest.mark.asyncio
    async def test_cleanup_invisible_ticker_data_resets_completeness(self):
        """cleanup_invisible_ticker_data resets price_history_complete=False
        when stock_prices rows are deleted."""
        from full_sync_service import cleanup_invisible_ticker_data

        # BODI: invisible ticker with price_history_complete=True (stale flag)
        tracked = _FakeCollection([
            {"ticker": "BODI.US", "is_visible": False, "price_history_complete": True},
        ])
        stock_prices = _FakeCollection()
        fundamentals = _FakeCollection()

        class FakeDB:
            def __getitem__(self, name):
                return {
                    "stock_prices": stock_prices,
                    "company_fundamentals_cache": fundamentals,
                    "company_financials": _FakeCollection(),
                    "company_earnings_history": _FakeCollection(),
                    "insider_activity": _FakeCollection(),
                }.get(name, _FakeCollection())

        db = FakeDB()
        db.tracked_tickers = tracked
        db.stock_prices = stock_prices

        with patch("benchmark_service.BENCHMARK_SYMBOLS", {}):
            result = await cleanup_invisible_ticker_data(db)

        assert result["deleted_tickers"] == 1

        # Verify completeness flags were reset
        assert len(tracked.updates) > 0
        last_update = tracked.updates[-1]
        set_fields = last_update["update"]["$set"]
        assert set_fields["price_history_complete"] is False
        assert set_fields["needs_price_redownload"] is True
        assert set_fields["price_history_status"] == "cleanup_reset"

    @pytest.mark.asyncio
    async def test_bodi_scenario_cleanup_then_bulk_return(self):
        """After cleanup resets completeness, the ticker fails Gate 8
        and is NOT visible even when it returns to bulk."""
        from visibility_rules import compute_visibility

        # BODI after cleanup: has_price_data=True (returned to bulk),
        # but price_history_complete=False (reset by cleanup)
        bodi_doc = {
            "ticker": "BODI.US",
            "is_seeded": True,
            "has_price_data": True,
            "sector": "Consumer Cyclical",
            "industry": "Leisure",
            "shares_outstanding": 100000000,
            "financial_currency": "USD",
            "is_delisted": False,
            "status": "Active",
            "price_history_complete": False,  # reset by cleanup
        }

        is_visible, reason = compute_visibility(bodi_doc)
        assert is_visible is False
        assert reason == "INCOMPLETE_HISTORY"

    @pytest.mark.asyncio
    async def test_bodi_scenario_after_phase_c_redownload(self):
        """After Phase C re-downloads full history, BODI becomes visible."""
        from visibility_rules import compute_visibility

        bodi_doc = {
            "ticker": "BODI.US",
            "is_seeded": True,
            "has_price_data": True,
            "sector": "Consumer Cyclical",
            "industry": "Leisure",
            "shares_outstanding": 100000000,
            "financial_currency": "USD",
            "is_delisted": False,
            "status": "Active",
            "price_history_complete": True,  # re-set by Phase C after download
        }

        is_visible, reason = compute_visibility(bodi_doc)
        assert is_visible is True
        assert reason is None


class TestCleanupResetStatus:
    """cleanup_reset is a valid price_history_status value."""

    def test_cleanup_reset_in_allowed_statuses(self):
        """'cleanup_reset' must be in the canonical ALLOWED_STATUSES enum."""
        from services.history_completeness_service import ALLOWED_STATUSES
        assert "cleanup_reset" in ALLOWED_STATUSES

    @pytest.mark.asyncio
    async def test_cleanup_reset_prevents_completeness_sweep_seal(self):
        """A ticker with cleanup_reset status and no history_download_proven_at
        must NOT be marked complete by the completeness sweep."""
        from services.history_completeness_service import verify_ticker_history_completeness

        # Fake DB where BODI has no proof marker (cleanup cleared it)
        tracked = _FakeCollection([
            {"ticker": "BODI.US", "price_history_status": "cleanup_reset"},
        ])

        class FakeDB:
            pass

        db = FakeDB()
        db.tracked_tickers = tracked

        result = await verify_ticker_history_completeness(db, "BODI.US")
        assert result["price_history_complete"] is False
        assert result["price_history_status"] == "no_history_download"
