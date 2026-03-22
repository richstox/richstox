"""
Tests for backfill_full_price_history in admin_overview_service.py

Covers:
  - Process-truth model: history_download_completed, gap_free_since_history_download
  - Canonical derivation from price_history_complete + bulk processed dates
  - Legacy heuristic fields (full_price_history*) preserved
  - Audit summary written to ops_job_runs
  - Idempotency: repeated execution produces same results
  - Edge cases: no visible tickers, no price data, no bulk dates
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


def _mock_backfill_db(
    visible_ticker_docs,
    price_stats,
    bulk_days=None,
    bulk_date_coverage=None,
):
    """
    Build a mock db for backfill_full_price_history.

    Args:
        visible_ticker_docs: list of dicts with ticker, price_history_complete,
                             price_history_complete_as_of
        price_stats: list of dicts with _id, min_date, row_count
        bulk_days: list of dicts for ops_job_runs bulk gapfill days
        bulk_date_coverage: dict mapping ticker -> set of dates they have
    """
    # tracked_tickers.find → returns visible_ticker_docs
    class _FindResult:
        def __init__(self, docs):
            self._docs = docs
        async def to_list(self, length=None):
            return self._docs

    tracked_tickers = SimpleNamespace(
        find=lambda query, projection: _FindResult(visible_ticker_docs),
        update_one=AsyncMock(),
    )

    # stock_prices.aggregate → returns price_stats, then bulk date coverage
    bulk_days = bulk_days or []
    bulk_date_coverage = bulk_date_coverage or {}
    coverage_docs = [
        {"_id": ticker, "dates": list(dates)}
        for ticker, dates in bulk_date_coverage.items()
    ]

    sp_call_index = {"n": 0}
    def _sp_aggregate(pipeline):
        idx = sp_call_index["n"]
        sp_call_index["n"] += 1
        if idx == 0:
            # First call: min_date + row_count aggregation
            return _make_cursor(price_stats)
        if idx == 1:
            # Second call: bulk date coverage per ticker
            return _make_cursor(coverage_docs)
        return _make_cursor([])

    stock_prices = SimpleNamespace(
        aggregate=_sp_aggregate,
    )

    # ops_job_runs → for _get_bulk_processed_dates and audit insert
    bulk_runs = [{
        "details": {
            "price_bulk_gapfill": {
                "days": bulk_days,
            }
        }
    }] if bulk_days else []
    ops_job_runs = SimpleNamespace(
        aggregate=lambda pipeline: _make_cursor(bulk_runs),
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


# ── Tests: Process-Truth Model ───────────────────────────────────────────────


def test_history_download_completed_and_gap_free():
    """Ticker with successful history download and no missing bulk dates → gap_free=True."""
    anchor_date = "2026-03-10"
    bulk_days = [
        {"processed_date": "2026-03-11", "status": "success"},
        {"processed_date": "2026-03-12", "status": "success"},
    ]
    visible_docs = [{
        "ticker": "AAPL.US",
        "price_history_complete": True,
        "price_history_complete_as_of": anchor_date,
    }]
    price_stats = [{"_id": "AAPL.US", "min_date": "2020-01-02", "row_count": 1500}]
    # Ticker has price data for both bulk dates
    coverage = {"AAPL.US": {"2026-03-11", "2026-03-12"}}

    db = _mock_backfill_db(visible_docs, price_stats, bulk_days, coverage)
    result = asyncio.run(backfill_full_price_history(db))

    assert result["status"] == "completed"
    assert result["history_download_completed_count"] == 1
    assert result["gap_free_since_history_download_count"] == 1
    assert result["tickers_with_missing_bulk_dates_count"] == 0
    assert result["total_missing_bulk_ticker_date_pairs"] == 0

    # Check the update_one call
    call_args = db.tracked_tickers.update_one.call_args
    update_doc = call_args[0][1]["$set"]
    assert update_doc["history_download_completed"] is True
    assert update_doc["history_download_completed_at"] == anchor_date
    assert update_doc["gap_free_since_history_download"] is True
    assert update_doc["missing_bulk_dates_since_history_download"] == 0
    assert update_doc["history_download_min_date"] == "2020-01-02"


def test_history_download_completed_with_missing_bulk_date():
    """Ticker with successful history download but missing one bulk date → gap_free=False."""
    anchor_date = "2026-03-10"
    bulk_days = [
        {"processed_date": "2026-03-11", "status": "success"},
        {"processed_date": "2026-03-12", "status": "success"},
    ]
    visible_docs = [{
        "ticker": "AAPL.US",
        "price_history_complete": True,
        "price_history_complete_as_of": anchor_date,
    }]
    price_stats = [{"_id": "AAPL.US", "min_date": "2020-01-02", "row_count": 1500}]
    # Ticker only has data for one of the two bulk dates
    coverage = {"AAPL.US": {"2026-03-11"}}

    db = _mock_backfill_db(visible_docs, price_stats, bulk_days, coverage)
    result = asyncio.run(backfill_full_price_history(db))

    assert result["history_download_completed_count"] == 1
    assert result["gap_free_since_history_download_count"] == 0
    assert result["tickers_with_missing_bulk_dates_count"] == 1
    assert result["total_missing_bulk_ticker_date_pairs"] == 1

    call_args = db.tracked_tickers.update_one.call_args
    update_doc = call_args[0][1]["$set"]
    assert update_doc["history_download_completed"] is True
    assert update_doc["gap_free_since_history_download"] is False
    assert update_doc["missing_bulk_dates_since_history_download"] == 1


def test_no_proven_history_download():
    """Ticker without proven history download → history_download_completed=False."""
    bulk_days = [
        {"processed_date": "2026-03-11", "status": "success"},
    ]
    visible_docs = [{
        "ticker": "NEW.US",
        "price_history_complete": False,
    }]
    price_stats = [{"_id": "NEW.US", "min_date": "2026-03-01", "row_count": 10}]

    db = _mock_backfill_db(visible_docs, price_stats, bulk_days)
    result = asyncio.run(backfill_full_price_history(db))

    assert result["history_download_completed_count"] == 0
    assert result["gap_free_since_history_download_count"] == 0

    call_args = db.tracked_tickers.update_one.call_args
    update_doc = call_args[0][1]["$set"]
    assert update_doc["history_download_completed"] is False
    assert update_doc["gap_free_since_history_download"] is False
    assert update_doc["missing_bulk_dates_since_history_download"] == 0


def test_bulk_dates_before_anchor_are_ignored():
    """Bulk dates <= anchor should NOT count as missing gaps."""
    anchor_date = "2026-03-15"
    bulk_days = [
        {"processed_date": "2026-03-10", "status": "success"},  # before anchor
        {"processed_date": "2026-03-12", "status": "success"},  # before anchor
        {"processed_date": "2026-03-16", "status": "success"},  # after anchor
    ]
    visible_docs = [{
        "ticker": "AAPL.US",
        "price_history_complete": True,
        "price_history_complete_as_of": anchor_date,
    }]
    price_stats = [{"_id": "AAPL.US", "min_date": "2020-01-02", "row_count": 1500}]
    # Ticker has the post-anchor date
    coverage = {"AAPL.US": {"2026-03-16"}}

    db = _mock_backfill_db(visible_docs, price_stats, bulk_days, coverage)
    result = asyncio.run(backfill_full_price_history(db))

    assert result["gap_free_since_history_download_count"] == 1
    assert result["tickers_with_missing_bulk_dates_count"] == 0

    call_args = db.tracked_tickers.update_one.call_args
    update_doc = call_args[0][1]["$set"]
    assert update_doc["gap_free_since_history_download"] is True
    assert update_doc["missing_bulk_dates_since_history_download"] == 0


def test_no_bulk_dates_means_gap_free():
    """If there are no canonical bulk dates, a completed download is trivially gap-free."""
    visible_docs = [{
        "ticker": "AAPL.US",
        "price_history_complete": True,
        "price_history_complete_as_of": "2026-03-10",
    }]
    price_stats = [{"_id": "AAPL.US", "min_date": "2020-01-02", "row_count": 1500}]

    db = _mock_backfill_db(visible_docs, price_stats, bulk_days=[], bulk_date_coverage={})
    result = asyncio.run(backfill_full_price_history(db))

    assert result["history_download_completed_count"] == 1
    assert result["gap_free_since_history_download_count"] == 1


# ── Tests: Legacy Heuristic Fields Preserved ─────────────────────────────────


def test_legacy_heuristic_fields_preserved():
    """Legacy full_price_history* fields are still computed and written."""
    old_date = (date.today() - timedelta(days=400)).isoformat()
    visible_docs = [{
        "ticker": "AAPL.US",
        "price_history_complete": True,
        "price_history_complete_as_of": "2026-03-10",
    }]
    price_stats = [{"_id": "AAPL.US", "min_date": old_date, "row_count": 300}]

    db = _mock_backfill_db(visible_docs, price_stats)
    result = asyncio.run(backfill_full_price_history(db))

    call_args = db.tracked_tickers.update_one.call_args
    update_doc = call_args[0][1]["$set"]
    # Legacy heuristic: 300 rows >= 252 AND min_date old enough
    assert update_doc["full_price_history"] is True
    assert update_doc["full_price_history_verified_at"] is not None
    assert update_doc["full_price_history_min_date"] == old_date
    assert update_doc["full_price_history_row_count"] == 300
    assert result["full_history_heuristic_count"] == 1


def test_legacy_heuristic_false_when_too_few_rows():
    """Legacy heuristic: < 252 rows → full_price_history=False."""
    old_date = (date.today() - timedelta(days=400)).isoformat()
    visible_docs = [{
        "ticker": "NEW.US",
        "price_history_complete": False,
    }]
    price_stats = [{"_id": "NEW.US", "min_date": old_date, "row_count": 100}]

    db = _mock_backfill_db(visible_docs, price_stats)
    result = asyncio.run(backfill_full_price_history(db))

    call_args = db.tracked_tickers.update_one.call_args
    update_doc = call_args[0][1]["$set"]
    assert update_doc["full_price_history"] is False


# ── Tests: Edge Cases ────────────────────────────────────────────────────────


def test_no_visible_tickers():
    """No visible tickers returns no_work status."""
    db = _mock_backfill_db([], [])
    result = asyncio.run(backfill_full_price_history(db))

    assert result["status"] == "no_work"
    assert result["total_visible_tickers"] == 0


def test_no_price_data():
    """Ticker with no price data → min_date=None, all booleans False."""
    visible_docs = [{
        "ticker": "EMPTY.US",
        "price_history_complete": True,
        "price_history_complete_as_of": "2026-03-10",
    }]

    db = _mock_backfill_db(visible_docs, [])
    result = asyncio.run(backfill_full_price_history(db))

    call_args = db.tracked_tickers.update_one.call_args
    update_doc = call_args[0][1]["$set"]
    assert update_doc["history_download_min_date"] is None
    assert update_doc["full_price_history"] is False
    assert update_doc["full_price_history_row_count"] == 0


def test_mixed_tickers():
    """Multiple tickers with different states produce correct aggregate counts."""
    anchor = "2026-03-10"
    bulk_days = [
        {"processed_date": "2026-03-11", "status": "success"},
        {"processed_date": "2026-03-12", "status": "success"},
    ]
    visible_docs = [
        {"ticker": "FULL.US", "price_history_complete": True, "price_history_complete_as_of": anchor},
        {"ticker": "GAPPY.US", "price_history_complete": True, "price_history_complete_as_of": anchor},
        {"ticker": "NEW.US", "price_history_complete": False},
    ]
    old_date = (date.today() - timedelta(days=400)).isoformat()
    price_stats = [
        {"_id": "FULL.US", "min_date": old_date, "row_count": 300},
        {"_id": "GAPPY.US", "min_date": old_date, "row_count": 300},
        {"_id": "NEW.US", "min_date": "2026-03-01", "row_count": 5},
    ]
    coverage = {
        "FULL.US": {"2026-03-11", "2026-03-12"},  # has both
        "GAPPY.US": {"2026-03-11"},                 # missing 03-12
    }

    db = _mock_backfill_db(visible_docs, price_stats, bulk_days, coverage)
    result = asyncio.run(backfill_full_price_history(db))

    assert result["total_visible_tickers"] == 3
    assert result["history_download_completed_count"] == 2
    assert result["gap_free_since_history_download_count"] == 1
    assert result["tickers_with_missing_bulk_dates_count"] == 1
    assert result["total_missing_bulk_ticker_date_pairs"] == 1


# ── Tests: Audit & Idempotency ───────────────────────────────────────────────


def test_writes_audit_to_ops_job_runs():
    """Audit summary is written to ops_job_runs with process-truth counts."""
    visible_docs = [{
        "ticker": "AAPL.US",
        "price_history_complete": True,
        "price_history_complete_as_of": "2026-03-10",
    }]
    price_stats = [{"_id": "AAPL.US", "min_date": "2020-01-02", "row_count": 300}]

    db = _mock_backfill_db(visible_docs, price_stats)
    asyncio.run(backfill_full_price_history(db))

    db.ops_job_runs.insert_one.assert_called_once()
    call_args = db.ops_job_runs.insert_one.call_args[0][0]
    assert call_args["job_name"] == "backfill_full_price_history"
    assert call_args["status"] == "completed"
    details = call_args["details"]
    assert details["total_visible_tickers"] == 1
    assert details["history_download_completed_count"] == 1
    assert details["gap_free_since_history_download_count"] == 1
    assert "canonical_bulk_dates_count" in details
    assert "heuristic_threshold" in details
    assert details["heuristic_threshold"]["min_row_count"] == 252


def test_idempotent_recomputation():
    """Running backfill twice produces the same field values (idempotent)."""
    anchor = "2026-03-10"
    bulk_days = [{"processed_date": "2026-03-11", "status": "success"}]
    visible_docs = [{
        "ticker": "AAPL.US",
        "price_history_complete": True,
        "price_history_complete_as_of": anchor,
    }]
    old_date = (date.today() - timedelta(days=400)).isoformat()
    price_stats = [{"_id": "AAPL.US", "min_date": old_date, "row_count": 300}]
    coverage = {"AAPL.US": {"2026-03-11"}}

    # First run
    db1 = _mock_backfill_db(visible_docs, price_stats, bulk_days, coverage)
    result1 = asyncio.run(backfill_full_price_history(db1))
    update1 = db1.tracked_tickers.update_one.call_args[0][1]["$set"]

    # Second run — same inputs
    db2 = _mock_backfill_db(visible_docs, price_stats, bulk_days, coverage)
    result2 = asyncio.run(backfill_full_price_history(db2))
    update2 = db2.tracked_tickers.update_one.call_args[0][1]["$set"]

    # Truth fields must be identical
    assert update1["history_download_completed"] == update2["history_download_completed"]
    assert update1["history_download_completed_at"] == update2["history_download_completed_at"]
    assert update1["gap_free_since_history_download"] == update2["gap_free_since_history_download"]
    assert update1["missing_bulk_dates_since_history_download"] == update2["missing_bulk_dates_since_history_download"]
    assert update1["full_price_history"] == update2["full_price_history"]
    assert result1["gap_free_since_history_download_count"] == result2["gap_free_since_history_download_count"]
