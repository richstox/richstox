"""
Regression test: offset-naive vs offset-aware datetime comparison in
sync_dividends_for_visible_tickers.

Root cause (PR339 follow-up):
    sync_dividends_for_visible_tickers creates ``resync_cutoff`` as a
    UTC-aware datetime (``datetime.now(timezone.utc) - timedelta(days=7)``),
    but ``dividends_synced_at`` read back from MongoDB is naive (Mongo strips
    tzinfo).  The comparison ``last_sync < resync_cutoff`` then raises:
        TypeError: can't compare offset-naive and offset-aware datetimes

Run:
    cd /app/backend && python -m pytest tests/test_dividend_sync_datetime.py -v
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import asyncio
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# ── Minimal async-MongoDB stub ──────────────────────────────────────────────


class _FakeCursor:
    """Mimics Motor cursor with .to_list()."""

    def __init__(self, docs):
        self._docs = docs

    async def to_list(self, length=None):
        return self._docs


class _FakeCollection:
    """Mimics motor collection with find / update_one."""

    def __init__(self, docs=None):
        self._docs = docs or []
        self.update_one = AsyncMock()

    def find(self, *args, **kwargs):
        return _FakeCursor(self._docs)


class _FakeDb:
    """Stub database with tracked_tickers + dividend_history collections."""

    def __init__(self, tracked_docs):
        self.tracked_tickers = _FakeCollection(tracked_docs)
        self.dividend_history = MagicMock()
        self.dividend_history.delete_many = AsyncMock()
        self.dividend_history.insert_many = AsyncMock()


# ── Tests ───────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_naive_synced_at_does_not_crash():
    """A naive ``dividends_synced_at`` (as returned by MongoDB) must NOT
    raise 'can't compare offset-naive and offset-aware datetimes'.
    """
    # Simulate a ticker whose dividends_synced_at was stored 3 days ago.
    # MongoDB strips tzinfo, so we pass a NAIVE datetime — this is the exact
    # condition that triggered the production crash.
    three_days_ago_naive = datetime.utcnow() - timedelta(days=3)
    tracked_docs = [
        {"ticker": "AAPL.US", "dividends_synced_at": three_days_ago_naive},
    ]

    db = _FakeDb(tracked_docs)

    # Patch the EODHD call so we don't do real HTTP
    with patch(
        "dividend_history_service.sync_ticker_dividends",
        new_callable=AsyncMock,
        return_value={"success": True, "dividends_synced": 0},
    ):
        from dividend_history_service import sync_dividends_for_visible_tickers

        # This must NOT raise TypeError
        summary = await sync_dividends_for_visible_tickers(db)

    # The ticker was synced 3 days ago, resync interval is 7 days →
    # it should NOT be pending (still fresh).
    assert summary["pending_sync"] == 0
    assert summary["synced_ok"] == 0


@pytest.mark.asyncio
async def test_naive_synced_at_old_triggers_resync():
    """A naive ``dividends_synced_at`` older than DIVIDEND_RESYNC_DAYS
    must correctly identify the ticker as pending resync.
    """
    ten_days_ago_naive = datetime.utcnow() - timedelta(days=10)
    tracked_docs = [
        {"ticker": "MSFT.US", "dividends_synced_at": ten_days_ago_naive},
    ]

    db = _FakeDb(tracked_docs)

    with patch(
        "dividend_history_service.sync_ticker_dividends",
        new_callable=AsyncMock,
        return_value={"success": True, "dividends_synced": 5},
    ):
        from dividend_history_service import sync_dividends_for_visible_tickers

        summary = await sync_dividends_for_visible_tickers(db)

    assert summary["pending_sync"] == 1
    assert summary["synced_ok"] == 1


@pytest.mark.asyncio
async def test_aware_synced_at_still_works():
    """If dividends_synced_at is already UTC-aware (e.g. set in the same
    process before a round-trip to Mongo), the comparison must still work.
    """
    two_days_ago_aware = datetime.now(timezone.utc) - timedelta(days=2)
    tracked_docs = [
        {"ticker": "GOOG.US", "dividends_synced_at": two_days_ago_aware},
    ]

    db = _FakeDb(tracked_docs)

    with patch(
        "dividend_history_service.sync_ticker_dividends",
        new_callable=AsyncMock,
        return_value={"success": True, "dividends_synced": 0},
    ):
        from dividend_history_service import sync_dividends_for_visible_tickers

        summary = await sync_dividends_for_visible_tickers(db)

    # 2 days < 7 days → not pending
    assert summary["pending_sync"] == 0


@pytest.mark.asyncio
async def test_none_synced_at_triggers_resync():
    """A ticker with dividends_synced_at=None (never synced) must be
    classified as pending.
    """
    tracked_docs = [
        {"ticker": "NVDA.US", "dividends_synced_at": None},
    ]

    db = _FakeDb(tracked_docs)

    with patch(
        "dividend_history_service.sync_ticker_dividends",
        new_callable=AsyncMock,
        return_value={"success": True, "dividends_synced": 3},
    ):
        from dividend_history_service import sync_dividends_for_visible_tickers

        summary = await sync_dividends_for_visible_tickers(db)

    assert summary["pending_sync"] == 1
    assert summary["synced_ok"] == 1
