"""
Tests for visibility circuit breaker in recompute_visibility_all().

Validates that when visibility recompute would result in 0 visible tickers
but there were previously visible tickers, the system restores previous
visibility to prevent an empty app.  Stale data is always better than no data.
"""

import asyncio
from copy import deepcopy
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock
import os
import sys

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from visibility_rules import recompute_visibility_all, compute_visibility


# ── Fake DB helpers ──────────────────────────────────────────────────────────

class _FakeCollection:
    """Minimal fake MongoDB collection for visibility tests."""
    def __init__(self, docs=None):
        self._docs = {d["ticker"]: deepcopy(d) for d in (docs or [])}
        self._next_id = 1

    async def count_documents(self, filt):
        count = 0
        for doc in self._docs.values():
            if self._matches(doc, filt):
                count += 1
        return count

    async def distinct(self, field, filt=None):
        results = set()
        for doc in self._docs.values():
            if filt is None or self._matches(doc, filt):
                val = doc.get(field)
                if val is not None:
                    results.add(val)
        return list(results)

    def find(self, filt=None):
        return _FakeAsyncCursor([
            deepcopy(d) for d in self._docs.values()
            if filt is None or self._matches(d, filt)
        ])

    async def bulk_write(self, ops, ordered=False):
        for op in ops:
            # UpdateOne-like
            filt = op._filter
            update = op._doc
            ticker = filt.get("ticker")
            if ticker and ticker in self._docs:
                for k, v in (update.get("$set") or {}).items():
                    self._docs[ticker][k] = v
        return SimpleNamespace(modified_count=len(ops))

    async def update_many(self, filt, update):
        count = 0
        for doc in self._docs.values():
            if self._matches(doc, filt):
                for k, v in (update.get("$set") or {}).items():
                    doc[k] = v
                count += 1
        return SimpleNamespace(modified_count=count)

    async def delete_many(self, filt):
        to_delete = [k for k, v in self._docs.items() if self._matches(v, filt)]
        for k in to_delete:
            del self._docs[k]
        return SimpleNamespace(deleted_count=len(to_delete))

    def _matches(self, doc, filt):
        for key, val in filt.items():
            doc_val = doc.get(key)
            if isinstance(val, dict):
                if "$in" in val and doc_val not in val["$in"]:
                    return False
                if "$nin" in val and doc_val in val["$nin"]:
                    return False
                if "$ne" in val and doc_val == val["$ne"]:
                    return False
            else:
                if doc_val != val:
                    return False
        return True


class _FakeAsyncCursor:
    def __init__(self, docs):
        self._docs = docs
        self._idx = 0

    def __aiter__(self):
        self._idx = 0
        return self

    async def __anext__(self):
        if self._idx >= len(self._docs):
            raise StopAsyncIteration
        doc = self._docs[self._idx]
        self._idx += 1
        return doc


class _FakeUpdateOne:
    """Mimics pymongo.UpdateOne for bulk_write."""
    def __init__(self, filt, update):
        self._filter = filt
        self._doc = update


class _FakeOpsJobRuns:
    def __init__(self):
        self.docs = {}
        self._next_id = 1

    def find(self, filt, **kwargs):
        return _FakeAsyncCursor([])

    async def find_one(self, filt, **kwargs):
        return None

    async def insert_one(self, doc):
        _id = self._next_id
        self._next_id += 1
        doc = deepcopy(doc)
        doc["_id"] = _id
        self.docs[_id] = doc
        return SimpleNamespace(inserted_id=_id)

    async def update_one(self, filt, update):
        _id = filt.get("_id")
        if _id in self.docs:
            for k, v in (update.get("$set") or {}).items():
                self.docs[_id][k] = v
        return SimpleNamespace(matched_count=1)


def _make_ticker(symbol, *, has_price=True, visible=True, sector="Tech",
                 industry="Software", shares=1000000, currency="USD"):
    """Create a ticker doc that passes or fails visibility gates."""
    return {
        "ticker": symbol,
        "name": symbol.replace(".US", ""),
        "is_seeded": True,
        "has_price_data": has_price,
        "is_visible": visible,
        "sector": sector,
        "industry": industry,
        "shares_outstanding": shares,
        "financial_currency": currency,
        "status": "active",
        "is_delisted": False,
    }


class _FakeDB:
    def __init__(self, tickers):
        self.tracked_tickers = _FakeCollection(tickers)
        self.stock_prices = _FakeCollection()
        self.financials_cache = _FakeCollection()
        self.ops_job_runs = _FakeOpsJobRuns()


# ── Monkey-patch pymongo.UpdateOne import inside visibility_rules ────────────

@pytest.fixture(autouse=True)
def patch_update_one(monkeypatch):
    """Replace pymongo.UpdateOne with our fake in the visibility module."""
    import visibility_rules
    # The function does `from pymongo import UpdateOne as _UpdateOne`
    # We can't intercept that import directly, so we patch pymongo.UpdateOne
    import pymongo
    monkeypatch.setattr(pymongo, "UpdateOne", _FakeUpdateOne)


# =============================================================================
# Tests
# =============================================================================


class TestVisibilityCircuitBreaker:
    """Tests for the circuit breaker that prevents 0-visible-ticker state."""

    def test_circuit_breaker_restores_visibility_when_all_lose_prices(self):
        """
        When all tickers lose has_price_data (e.g. holiday wipe), the circuit
        breaker should restore previously-visible tickers.
        """
        tickers = [
            # Previously visible, but has_price_data wiped → would become invisible
            _make_ticker("AAPL.US", has_price=False, visible=True),
            _make_ticker("MSFT.US", has_price=False, visible=True),
            _make_ticker("GOOGL.US", has_price=False, visible=True),
        ]
        db = _FakeDB(tickers)

        result = asyncio.run(recompute_visibility_all(db))

        # Circuit breaker should have triggered
        assert result["stats"].get("circuit_breaker_triggered") is True
        assert result["stats"].get("restored_tickers", 0) == 3

        # After restoration, visible count should match previous
        assert result["after"]["visible_count"] == 3

    def test_no_circuit_breaker_on_normal_run(self):
        """Normal run with visible tickers should not trigger circuit breaker."""
        tickers = [
            _make_ticker("AAPL.US", has_price=True, visible=True),
            _make_ticker("MSFT.US", has_price=True, visible=True),
            _make_ticker("GOOGL.US", has_price=True, visible=True),
        ]
        db = _FakeDB(tickers)

        result = asyncio.run(recompute_visibility_all(db))

        assert result["stats"].get("circuit_breaker_triggered") is not True
        assert result["stats"]["now_visible"] == 3

    def test_no_circuit_breaker_when_previously_zero(self):
        """If there were 0 visible before, circuit breaker should NOT trigger."""
        tickers = [
            _make_ticker("AAPL.US", has_price=False, visible=False),
            _make_ticker("MSFT.US", has_price=False, visible=False),
        ]
        db = _FakeDB(tickers)

        result = asyncio.run(recompute_visibility_all(db))

        # No circuit breaker — there were 0 visible before too
        assert result["stats"].get("circuit_breaker_triggered") is not True
        assert result["stats"]["now_visible"] == 0

    def test_partial_loss_does_not_trigger_circuit_breaker(self):
        """If some tickers lose visibility but not all, no circuit breaker."""
        tickers = [
            _make_ticker("AAPL.US", has_price=True, visible=True),
            _make_ticker("MSFT.US", has_price=False, visible=True),  # loses price
        ]
        db = _FakeDB(tickers)

        result = asyncio.run(recompute_visibility_all(db))

        # 1 visible remaining → no circuit breaker
        assert result["stats"].get("circuit_breaker_triggered") is not True
        assert result["stats"]["now_visible"] == 1

    def test_circuit_breaker_skips_cleanup(self):
        """When circuit breaker triggers, no data should be deleted."""
        tickers = [
            _make_ticker("AAPL.US", has_price=False, visible=True),
            _make_ticker("MSFT.US", has_price=False, visible=True),
        ]
        db = _FakeDB(tickers)

        result = asyncio.run(recompute_visibility_all(db))

        assert result["stats"].get("circuit_breaker_triggered") is True
        assert result["cleanup"]["stock_prices_deleted"] == 0
        assert result["cleanup"]["financials_cache_deleted"] == 0
