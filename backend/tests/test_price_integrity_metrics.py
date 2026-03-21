"""
Tests for get_price_integrity_metrics and get_pipeline_last_success_age
in admin_overview_service.py
"""

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from services.admin_overview_service import (
    get_pipeline_last_success_age,
    get_price_integrity_metrics,
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


def _mock_db(
    visible_tickers=None,
    bulk_state=None,
    redownload_count=0,
    incomplete_count=0,
    checkpoint_counts=None,
    gap_count=0,
):
    """Build a mock db with the fields needed by get_price_integrity_metrics."""
    if visible_tickers is None:
        visible_tickers = ["AAPL.US", "MSFT.US", "GOOG.US"]

    # tracked_tickers.aggregate is sync (returns cursor), .to_list is async
    facet_data = [{
        "needs_redownload": [{"n": redownload_count}] if redownload_count else [],
        "incomplete_history": [{"n": incomplete_count}] if incomplete_count else [],
    }]
    tracked_tickers = SimpleNamespace(
        distinct=AsyncMock(return_value=visible_tickers),
        aggregate=lambda pipeline: _make_cursor(facet_data),
    )

    # pipeline_state
    pipeline_state = SimpleNamespace(
        find_one=AsyncMock(return_value=bulk_state),
    )

    # stock_prices — find_one for nearest date, aggregate for counts & gaps
    checkpoint_counts = checkpoint_counts or {}
    count_docs = [{"_id": d, "count": c} for d, c in checkpoint_counts.items()]
    gap_docs = [{"n": gap_count}] if gap_count else []

    call_index = {"n": 0}

    def _sp_aggregate(pipeline):
        # First aggregate call → checkpoint counts, second → gap count
        idx = call_index["n"]
        call_index["n"] += 1
        if idx == 0:
            return _make_cursor(count_docs)
        return _make_cursor(gap_docs)

    stock_prices = SimpleNamespace(
        find_one=AsyncMock(return_value={"date": "2026-03-20"}),
        aggregate=_sp_aggregate,
    )

    return SimpleNamespace(
        tracked_tickers=tracked_tickers,
        pipeline_state=pipeline_state,
        stock_prices=stock_prices,
    )


# ── Tests: get_price_integrity_metrics ───────────────────────────────────────


def test_returns_correct_keys():
    db = _mock_db(bulk_state={"global_last_bulk_date_processed": "2026-03-20"})
    result = asyncio.run(get_price_integrity_metrics(db))

    assert "today_visible" in result
    assert "last_bulk_trading_date" in result
    assert "needs_price_redownload" in result
    assert "price_history_incomplete" in result
    assert "missing_expected_dates" in result
    assert "coverage_checkpoints" in result


def test_today_visible_count():
    db = _mock_db(visible_tickers=["A.US", "B.US", "C.US", "D.US"])
    result = asyncio.run(get_price_integrity_metrics(db))
    assert result["today_visible"] == 4


def test_last_bulk_date_from_pipeline_state():
    db = _mock_db(bulk_state={"global_last_bulk_date_processed": "2026-03-19"})
    result = asyncio.run(get_price_integrity_metrics(db))
    assert result["last_bulk_trading_date"] == "2026-03-19"


def test_last_bulk_date_none_when_no_state():
    db = _mock_db(bulk_state=None)
    result = asyncio.run(get_price_integrity_metrics(db))
    assert result["last_bulk_trading_date"] is None


def test_needs_redownload_count():
    db = _mock_db(redownload_count=5)
    result = asyncio.run(get_price_integrity_metrics(db))
    assert result["needs_price_redownload"] == 5


def test_incomplete_history_count():
    db = _mock_db(incomplete_count=10)
    result = asyncio.run(get_price_integrity_metrics(db))
    assert result["price_history_incomplete"] == 10


def test_coverage_checkpoints_present():
    db = _mock_db(
        bulk_state={"global_last_bulk_date_processed": "2026-03-20"},
        checkpoint_counts={"2026-03-20": 3},
    )
    result = asyncio.run(get_price_integrity_metrics(db))
    cp = result["coverage_checkpoints"]
    assert "latest_trading_day" in cp
    assert "1_week_ago" in cp
    assert "1_month_ago" in cp
    assert "1_year_ago" in cp


def test_zero_visible_returns_safe_defaults():
    db = _mock_db(visible_tickers=[])
    result = asyncio.run(get_price_integrity_metrics(db))
    assert result["today_visible"] == 0
    assert result["needs_price_redownload"] == 0
    assert result["missing_expected_dates"] == 0
    assert result["coverage_checkpoints"] == {}


def test_missing_expected_dates_count():
    db = _mock_db(gap_count=7)
    result = asyncio.run(get_price_integrity_metrics(db))
    assert result["missing_expected_dates"] == 7


# ── Tests: get_pipeline_last_success_age ────────────────────────────────────


def test_pipeline_age_green():
    now = datetime.now(timezone.utc)
    recent = now - timedelta(hours=5)

    ops_job_runs = SimpleNamespace(
        find_one=AsyncMock(return_value={"finished_at": recent}),
    )
    db = SimpleNamespace(ops_job_runs=ops_job_runs)

    result = asyncio.run(get_pipeline_last_success_age(db))
    assert result["pipeline_status"] == "green"
    assert result["morning_refresh_status"] == "green"


def test_pipeline_age_yellow():
    now = datetime.now(timezone.utc)
    old = now - timedelta(hours=30)

    ops_job_runs = SimpleNamespace(
        find_one=AsyncMock(return_value={"finished_at": old}),
    )
    db = SimpleNamespace(ops_job_runs=ops_job_runs)

    result = asyncio.run(get_pipeline_last_success_age(db))
    assert result["pipeline_status"] == "yellow"


def test_pipeline_age_red():
    now = datetime.now(timezone.utc)
    very_old = now - timedelta(hours=50)

    ops_job_runs = SimpleNamespace(
        find_one=AsyncMock(return_value={"finished_at": very_old}),
    )
    db = SimpleNamespace(ops_job_runs=ops_job_runs)

    result = asyncio.run(get_pipeline_last_success_age(db))
    assert result["pipeline_status"] == "red"


def test_pipeline_age_unknown_no_runs():
    ops_job_runs = SimpleNamespace(
        find_one=AsyncMock(return_value=None),
    )
    db = SimpleNamespace(ops_job_runs=ops_job_runs)

    result = asyncio.run(get_pipeline_last_success_age(db))
    assert result["pipeline_status"] == "unknown"
    assert result["pipeline_hours_since_success"] is None
