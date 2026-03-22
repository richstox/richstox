"""
Tests for backfill_full_price_history in admin_overview_service.py

Covers:
  - Correct computation of full_price_history truth fields
  - Threshold constants (MIN_ROWS=252, MIN_DAYS=365)
  - Audit summary written to ops_job_runs
  - Idempotency: repeated execution produces same results
  - Edge cases: no visible tickers, no price data
"""

import asyncio
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from services.admin_overview_service import (
    backfill_full_price_history,
    FULL_HISTORY_MIN_ROWS,
    FULL_HISTORY_MIN_DAYS,
)


# ── Helpers ──────────────────────────────────────────────────────────────────


def _make_cursor(docs):
    """Create an async iterable cursor that also has to_list."""
    class _Cursor:
        def __init__(self, data):
            self._data = data
        async def to_list(self, length=None):
            return self._data
        def __aiter__(self):
            return _CursorIter(self._data)
    class _CursorIter:
        def __init__(self, data):
            self._iter = iter(data)
        async def __anext__(self):
            try:
                return next(self._iter)
            except StopIteration:
                raise StopAsyncIteration
    return _Cursor(docs)


def _mock_backfill_db(visible_tickers, price_stats):
    """
    Build a mock db for backfill_full_price_history.

    Args:
        visible_tickers: list of ticker strings returned by distinct()
        price_stats: list of dicts with keys _id, min_date, row_count
                     (result of stock_prices.aggregate group)
    """
    tracked_tickers = SimpleNamespace(
        distinct=AsyncMock(return_value=visible_tickers),
        update_one=AsyncMock(),
    )
    stock_prices = SimpleNamespace(
        aggregate=lambda pipeline: _make_cursor(price_stats),
    )
    ops_job_runs = SimpleNamespace(
        insert_one=AsyncMock(),
    )
    return SimpleNamespace(
        tracked_tickers=tracked_tickers,
        stock_prices=stock_prices,
        ops_job_runs=ops_job_runs,
    )


# ── Tests: Constants ─────────────────────────────────────────────────────────


def test_threshold_constants():
    """Verify canonical threshold values are documented as constants."""
    assert FULL_HISTORY_MIN_ROWS == 252
    assert FULL_HISTORY_MIN_DAYS == 365


# ── Tests: Backfill Job Logic ────────────────────────────────────────────────


def test_backfill_full_history_computed_correctly():
    """Ticker with >= 252 rows and min_date <= today-365d gets full_price_history=True."""
    old_date = (date.today() - timedelta(days=400)).isoformat()
    price_stats = [
        {"_id": "AAPL.US", "min_date": old_date, "row_count": 300},
    ]
    db = _mock_backfill_db(["AAPL.US"], price_stats)
    result = asyncio.run(backfill_full_price_history(db))

    assert result["status"] == "completed"
    assert result["full_history_count"] == 1
    assert result["partial_history_count"] == 0
    assert result["no_price_data_count"] == 0

    # Check the update_one call set full_price_history=True
    call_args = db.tracked_tickers.update_one.call_args
    update_doc = call_args[0][1]["$set"]
    assert update_doc["full_price_history"] is True
    assert update_doc["full_price_history_row_count"] == 300
    assert update_doc["full_price_history_min_date"] == old_date
    assert update_doc["full_price_history_verified_at"] is not None


def test_backfill_partial_history_low_rows():
    """Ticker with < 252 rows gets full_price_history=False."""
    old_date = (date.today() - timedelta(days=400)).isoformat()
    price_stats = [
        {"_id": "NEW.US", "min_date": old_date, "row_count": 100},
    ]
    db = _mock_backfill_db(["NEW.US"], price_stats)
    result = asyncio.run(backfill_full_price_history(db))

    assert result["partial_history_count"] == 1
    assert result["full_history_count"] == 0

    call_args = db.tracked_tickers.update_one.call_args
    update_doc = call_args[0][1]["$set"]
    assert update_doc["full_price_history"] is False


def test_backfill_partial_history_recent_min_date():
    """Ticker with enough rows but min_date too recent gets full_price_history=False."""
    recent_date = (date.today() - timedelta(days=100)).isoformat()
    price_stats = [
        {"_id": "SPAC.US", "min_date": recent_date, "row_count": 500},
    ]
    db = _mock_backfill_db(["SPAC.US"], price_stats)
    result = asyncio.run(backfill_full_price_history(db))

    assert result["partial_history_count"] == 1
    assert result["full_history_count"] == 0

    call_args = db.tracked_tickers.update_one.call_args
    update_doc = call_args[0][1]["$set"]
    assert update_doc["full_price_history"] is False


def test_backfill_no_price_data():
    """Ticker with no price data gets full_price_history=False and no_data count."""
    db = _mock_backfill_db(["EMPTY.US"], [])
    result = asyncio.run(backfill_full_price_history(db))

    assert result["no_price_data_count"] == 1
    assert result["full_history_count"] == 0
    assert result["partial_history_count"] == 0

    call_args = db.tracked_tickers.update_one.call_args
    update_doc = call_args[0][1]["$set"]
    assert update_doc["full_price_history"] is False
    assert update_doc["full_price_history_row_count"] == 0
    assert update_doc["full_price_history_min_date"] is None


def test_backfill_mixed_tickers():
    """Multiple tickers: some full, some partial, some no data."""
    old_date = (date.today() - timedelta(days=400)).isoformat()
    recent_date = (date.today() - timedelta(days=100)).isoformat()
    price_stats = [
        {"_id": "FULL.US", "min_date": old_date, "row_count": 300},
        {"_id": "PARTIAL.US", "min_date": recent_date, "row_count": 50},
    ]
    db = _mock_backfill_db(["FULL.US", "PARTIAL.US", "EMPTY.US"], price_stats)
    result = asyncio.run(backfill_full_price_history(db))

    assert result["total_visible_tickers"] == 3
    assert result["full_history_count"] == 1
    assert result["partial_history_count"] == 1
    assert result["no_price_data_count"] == 1


def test_backfill_no_visible_tickers():
    """No visible tickers returns no_work status."""
    db = _mock_backfill_db([], [])
    result = asyncio.run(backfill_full_price_history(db))

    assert result["status"] == "no_work"
    assert result["total_visible"] == 0


def test_backfill_writes_audit_to_ops_job_runs():
    """Audit summary is written to ops_job_runs."""
    old_date = (date.today() - timedelta(days=400)).isoformat()
    price_stats = [
        {"_id": "AAPL.US", "min_date": old_date, "row_count": 300},
    ]
    db = _mock_backfill_db(["AAPL.US"], price_stats)
    asyncio.run(backfill_full_price_history(db))

    db.ops_job_runs.insert_one.assert_called_once()
    call_args = db.ops_job_runs.insert_one.call_args[0][0]
    assert call_args["job_name"] == "backfill_full_price_history"
    assert call_args["status"] == "completed"
    details = call_args["details"]
    assert details["total_visible_tickers"] == 1
    assert details["full_history_count"] == 1
    assert details["threshold"]["min_row_count"] == 252
    assert details["threshold"]["min_days"] == 365


def test_backfill_idempotent():
    """Running backfill twice produces the same field values (idempotent)."""
    old_date = (date.today() - timedelta(days=400)).isoformat()
    price_stats = [
        {"_id": "AAPL.US", "min_date": old_date, "row_count": 300},
    ]

    # First run
    db1 = _mock_backfill_db(["AAPL.US"], price_stats)
    result1 = asyncio.run(backfill_full_price_history(db1))
    update1 = db1.tracked_tickers.update_one.call_args[0][1]["$set"]

    # Second run — same inputs
    db2 = _mock_backfill_db(["AAPL.US"], price_stats)
    result2 = asyncio.run(backfill_full_price_history(db2))
    update2 = db2.tracked_tickers.update_one.call_args[0][1]["$set"]

    # Same truth values (verified_at will differ but is a timestamp, not truth)
    assert update1["full_price_history"] == update2["full_price_history"]
    assert update1["full_price_history_row_count"] == update2["full_price_history_row_count"]
    assert update1["full_price_history_min_date"] == update2["full_price_history_min_date"]
    assert result1["full_history_count"] == result2["full_history_count"]


def test_backfill_boundary_exactly_252_rows():
    """Exactly 252 rows and min_date at boundary should qualify."""
    cutoff = (date.today() - timedelta(days=365)).isoformat()
    price_stats = [
        {"_id": "EDGE.US", "min_date": cutoff, "row_count": 252},
    ]
    db = _mock_backfill_db(["EDGE.US"], price_stats)
    result = asyncio.run(backfill_full_price_history(db))

    assert result["full_history_count"] == 1
    call_args = db.tracked_tickers.update_one.call_args
    assert call_args[0][1]["$set"]["full_price_history"] is True


def test_backfill_boundary_251_rows():
    """251 rows should NOT qualify even with old min_date."""
    old_date = (date.today() - timedelta(days=400)).isoformat()
    price_stats = [
        {"_id": "EDGE.US", "min_date": old_date, "row_count": 251},
    ]
    db = _mock_backfill_db(["EDGE.US"], price_stats)
    result = asyncio.run(backfill_full_price_history(db))

    assert result["full_history_count"] == 0
    assert result["partial_history_count"] == 1


def test_backfill_boundary_min_date_one_day_too_recent():
    """min_date exactly 364 days ago should NOT qualify."""
    almost_cutoff = (date.today() - timedelta(days=364)).isoformat()
    price_stats = [
        {"_id": "EDGE.US", "min_date": almost_cutoff, "row_count": 300},
    ]
    db = _mock_backfill_db(["EDGE.US"], price_stats)
    result = asyncio.run(backfill_full_price_history(db))

    assert result["full_history_count"] == 0
    assert result["partial_history_count"] == 1
