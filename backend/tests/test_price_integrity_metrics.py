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
    """Create an async iterable cursor that also has to_list, sort, and limit."""
    class _Cursor:
        def __init__(self, data):
            self._data = list(data)
        def sort(self, *args, **kwargs):
            return self
        def limit(self, n):
            self._data = self._data[:n]
            return self
        async def to_list(self, length=None):
            if length is not None:
                return self._data[:length]
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
    full_price_history_count=0,
    history_download_completed_count=0,
    fundamentals_complete_count=0,
    checkpoint_counts=None,
    chain_doc="DEFAULT",
    bulk_days=None,
    proven_ticker_docs=None,
    gap_free_coverage_docs=None,
    **kwargs,
):
    """Build a mock db with the fields needed by get_price_integrity_metrics."""
    if visible_tickers is None:
        visible_tickers = ["AAPL.US", "MSFT.US", "GOOG.US"]

    # tracked_tickers.aggregate is sync (returns cursor), .to_list is async
    facet_data = [{
        "needs_redownload": [{"n": redownload_count}] if redownload_count else [],
        "incomplete_history": [{"n": incomplete_count}] if incomplete_count else [],
        "full_price_history": [{"n": full_price_history_count}] if full_price_history_count else [],
        "history_download_completed": [{"n": history_download_completed_count}] if history_download_completed_count else [],
        "fundamentals_complete": [{"n": fundamentals_complete_count}] if fundamentals_complete_count else [],
    }]

    if proven_ticker_docs is None:
        proven_ticker_docs = []
    if gap_free_coverage_docs is None:
        gap_free_coverage_docs = []

    tracked_tickers = SimpleNamespace(
        distinct=AsyncMock(return_value=visible_tickers),
        aggregate=lambda pipeline: _make_cursor(facet_data),
        find=lambda query, projection: _make_cursor(proven_ticker_docs),
    )

    # pipeline_state
    pipeline_state = SimpleNamespace(
        find_one=AsyncMock(return_value=bulk_state),
    )

    # pipeline_chain_runs — for _resolve_today_visible
    if chain_doc == "DEFAULT":
        chain_doc = {
            "chain_run_id": "chain_test_123",
            "finished_at": datetime(2026, 3, 20, 10, 0, tzinfo=timezone.utc),
        }
    pipeline_chain_runs = SimpleNamespace(
        find_one=AsyncMock(return_value=chain_doc),
    )

    # ops_job_runs — for _get_bulk_processed_dates
    if bulk_days is None:
        bulk_days = []
    bulk_runs = [{
        "details": {
            "price_bulk_gapfill": {
                "days": bulk_days,
            }
        }
    }] if bulk_days else []

    # Derive expected successful dates from bulk_days for mock alignment.
    # Must mirror the 3-stage filter in _get_bulk_processed_dates().
    expected_dates = [
        d["processed_date"] for d in bulk_days
        if d.get("status") == "success" and d.get("processed_date")
        and (d.get("rows_written") or 0) > 0
        and (d.get("matched_seeded_tickers_count") or 0) >= 4000
    ]

    # Derive trading-day dates: by default all expected_dates are trading days
    # unless the test explicitly provides calendar_trading_dates to override.
    calendar_trading_dates = kwargs.get("calendar_trading_dates")
    if calendar_trading_dates is None:
        # Default: all expected_dates are assumed trading days
        calendar_trading_dates = set(expected_dates)
    else:
        calendar_trading_dates = set(calendar_trading_dates)

    # stock_prices — find_one for nearest date, aggregate for counts & gap-free
    checkpoint_counts = checkpoint_counts or {}
    count_docs = [{"_id": d, "count": c} for d, c in checkpoint_counts.items()]

    call_index = {"n": 0}

    def _sp_aggregate(pipeline):
        idx = call_index["n"]
        call_index["n"] += 1
        if idx == 0:
            # First call: checkpoint counts
            return _make_cursor(count_docs)
        if idx == 1:
            # Second call: gap_free coverage per proven ticker
            return _make_cursor(gap_free_coverage_docs)
        return _make_cursor([])

    stock_prices = SimpleNamespace(
        find_one=AsyncMock(return_value={"date": "2026-03-20"}),
        aggregate=_sp_aggregate,
    )

    ops_job_runs = SimpleNamespace(
        aggregate=lambda pipeline: _make_cursor(bulk_runs),
        find_one=AsyncMock(return_value=None),
    )

    # market_calendar — for get_last_10_completed_trading_days_health AND
    # _get_bulk_processed_dates calendar filter.
    # The find() mock returns trading-day docs for dates in
    # calendar_trading_dates so the calendar filter works correctly.
    calendar_docs = [{"date": d} for d in sorted(calendar_trading_dates)]

    def _mc_find(query=None, projection=None, *a, **kw):
        if query and query.get("is_trading_day") is True and "$in" in (query.get("date") or {}):
            # Calendar filter query from _get_bulk_processed_dates
            requested = set(query["date"]["$in"])
            matching = [doc for doc in calendar_docs if doc["date"] in requested]
            return _make_cursor(matching)
        # Default: empty (for get_last_10_completed_trading_days_health etc.)
        return _make_cursor([])

    market_calendar = SimpleNamespace(
        find_one=AsyncMock(return_value=None),
        find=_mc_find,
        create_index=AsyncMock(),
    )

    class _DB:
        """Namespace that also supports __getitem__ for collection-name access."""
        def __init__(self, **collections):
            for k, v in collections.items():
                setattr(self, k, v)
        def __getitem__(self, key):
            return getattr(self, key, SimpleNamespace(
                find_one=AsyncMock(return_value=None),
                find=lambda *a, **kw: _make_cursor([]),
            ))

    return _DB(
        tracked_tickers=tracked_tickers,
        pipeline_state=pipeline_state,
        pipeline_chain_runs=pipeline_chain_runs,
        stock_prices=stock_prices,
        ops_job_runs=ops_job_runs,
        market_calendar=market_calendar,
    )


# ── Tests: get_price_integrity_metrics ───────────────────────────────────────


def test_returns_correct_keys():
    db = _mock_db(bulk_state={"global_last_bulk_date_processed": "2026-03-20"})
    result = asyncio.run(get_price_integrity_metrics(db))

    assert "today_visible" in result
    assert "today_visible_source" in result
    assert "last_bulk_trading_date" in result
    assert "needs_price_redownload" in result
    assert "price_history_incomplete" in result
    assert "full_price_history_count" in result
    assert "history_download_completed_count" in result
    assert "gap_free_since_history_download_count" in result
    assert "fundamentals_complete_count" in result
    assert "completed_trading_days_health" in result
    assert "coverage_checkpoints" in result


def test_today_visible_count():
    db = _mock_db(visible_tickers=["A.US", "B.US", "C.US", "D.US"])
    result = asyncio.run(get_price_integrity_metrics(db))
    assert result["today_visible"] == 4


def test_today_visible_source_from_chain():
    db = _mock_db(
        chain_doc={
            "chain_run_id": "chain_abc",
            "finished_at": datetime(2026, 3, 20, 10, 0, tzinfo=timezone.utc),
        }
    )
    result = asyncio.run(get_price_integrity_metrics(db))
    source = result["today_visible_source"]
    assert source is not None
    assert source["chain_run_id"] == "chain_abc"
    assert source["generated_at_prague"] is not None


def test_today_visible_source_none_when_no_chain():
    db = _mock_db(chain_doc=None)
    result = asyncio.run(get_price_integrity_metrics(db))
    source = result["today_visible_source"]
    assert source is not None
    assert source["chain_run_id"] is None


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


def test_full_price_history_count():
    db = _mock_db(full_price_history_count=42)
    result = asyncio.run(get_price_integrity_metrics(db))
    assert result["full_price_history_count"] == 42


def test_full_price_history_count_zero_default():
    db = _mock_db()
    result = asyncio.run(get_price_integrity_metrics(db))
    assert result["full_price_history_count"] == 0


def test_history_download_completed_count():
    db = _mock_db(history_download_completed_count=100)
    result = asyncio.run(get_price_integrity_metrics(db))
    assert result["history_download_completed_count"] == 100


def test_gap_free_since_history_download_count():
    """Gap-free count is computed inline from proven tickers + bulk coverage."""
    proven_docs = [
        {"ticker": "A.US", "history_download_proven_anchor": "2026-03-10"},
        {"ticker": "B.US", "history_download_proven_anchor": "2026-03-10"},
    ]
    coverage_docs = [
        {"_id": "A.US", "dates": ["2026-03-11"]},
        {"_id": "B.US", "dates": ["2026-03-11"]},
    ]
    db = _mock_db(
        visible_tickers=["A.US", "B.US", "C.US"],
        history_download_completed_count=2,
        bulk_days=[{"processed_date": "2026-03-11", "status": "success", "rows_written": 5000, "matched_seeded_tickers_count": 5000}],
        proven_ticker_docs=proven_docs,
        gap_free_coverage_docs=coverage_docs,
    )
    result = asyncio.run(get_price_integrity_metrics(db))
    assert result["gap_free_since_history_download_count"] == 2


def test_process_truth_counts_zero_default():
    db = _mock_db()
    result = asyncio.run(get_price_integrity_metrics(db))
    assert result["history_download_completed_count"] == 0
    assert result["gap_free_since_history_download_count"] == 0


def test_fundamentals_complete_count():
    db = _mock_db(fundamentals_complete_count=50)
    result = asyncio.run(get_price_integrity_metrics(db))
    assert result["fundamentals_complete_count"] == 50


def test_fundamentals_complete_count_zero_default():
    db = _mock_db()
    result = asyncio.run(get_price_integrity_metrics(db))
    assert result["fundamentals_complete_count"] == 0


def test_gap_free_with_missing_coverage():
    """Proven ticker missing a bulk date after anchor → not gap-free."""
    proven_docs = [
        {"ticker": "A.US", "history_download_proven_anchor": "2026-03-10"},
    ]
    # A.US has NO coverage for the bulk date → gap
    coverage_docs = []
    db = _mock_db(
        visible_tickers=["A.US", "B.US"],
        history_download_completed_count=1,
        bulk_days=[{"processed_date": "2026-03-11", "status": "success", "rows_written": 5000, "matched_seeded_tickers_count": 5000}],
        proven_ticker_docs=proven_docs,
        gap_free_coverage_docs=coverage_docs,
    )
    result = asyncio.run(get_price_integrity_metrics(db))
    assert result["gap_free_since_history_download_count"] == 0


def test_gap_free_no_bulk_dates():
    """Proven ticker with no bulk dates → trivially gap-free."""
    proven_docs = [
        {"ticker": "A.US", "history_download_proven_anchor": "2026-03-10"},
    ]
    db = _mock_db(
        visible_tickers=["A.US"],
        history_download_completed_count=1,
        proven_ticker_docs=proven_docs,
        bulk_days=[],
    )
    result = asyncio.run(get_price_integrity_metrics(db))
    assert result["gap_free_since_history_download_count"] == 1


def test_gap_free_ignores_zero_row_days():
    """Days with rows_written=0 must be excluded from expected_dates.

    This is the root cause of the 4/5661 gap-free bug: remediation runs that
    processed non-trading dates (or dates with no EODHD data) wrote
    status='success' with rows_written=0.  Those phantom dates had no
    stock_prices, so every ticker failed the gap-free check.
    """
    proven_docs = [
        {"ticker": "A.US", "history_download_proven_anchor": "2026-03-10"},
    ]
    coverage_docs = [
        {"_id": "A.US", "dates": ["2026-03-11"]},
    ]
    db = _mock_db(
        visible_tickers=["A.US"],
        history_download_completed_count=1,
        bulk_days=[
            # Real day with actual data → should count
            {"processed_date": "2026-03-11", "status": "success", "rows_written": 5000, "matched_seeded_tickers_count": 5000},
            # Phantom day with zero rows → must be excluded
            {"processed_date": "2026-03-12", "status": "success", "rows_written": 0, "matched_seeded_tickers_count": 0},
        ],
        proven_ticker_docs=proven_docs,
        gap_free_coverage_docs=coverage_docs,
    )
    result = asyncio.run(get_price_integrity_metrics(db))
    # A.US has coverage for 2026-03-11; 2026-03-12 is excluded (0 rows).
    # Without the fix this would be 0 because A.US has no price for 2026-03-12.
    assert result["gap_free_since_history_download_count"] == 1


def test_gap_free_ignores_missing_rows_written_field():
    """Days without a rows_written field at all must be excluded."""
    proven_docs = [
        {"ticker": "A.US", "history_download_proven_anchor": "2026-03-10"},
    ]
    coverage_docs = [
        {"_id": "A.US", "dates": ["2026-03-11"]},
    ]
    db = _mock_db(
        visible_tickers=["A.US"],
        history_download_completed_count=1,
        bulk_days=[
            {"processed_date": "2026-03-11", "status": "success", "rows_written": 5000, "matched_seeded_tickers_count": 5000},
            {"processed_date": "2026-03-13", "status": "success"},
        ],
        proven_ticker_docs=proven_docs,
        gap_free_coverage_docs=coverage_docs,
    )
    result = asyncio.run(get_price_integrity_metrics(db))
    assert result["gap_free_since_history_download_count"] == 1


def test_gap_free_excludes_non_trading_days_via_calendar():
    """Non-trading dates (weekends/holidays) with rows_written>0 must be
    excluded by the market-calendar filter in _get_bulk_processed_dates.

    This is the definitive fix for the 4/5661 gap-free bug: even if an
    ops_job_runs entry has status=success and rows_written>0 for a
    non-trading date, the calendar filter drops it so it never appears in
    expected_dates.
    """
    proven_docs = [
        {"ticker": "A.US", "history_download_proven_anchor": "2026-03-10"},
        {"ticker": "B.US", "history_download_proven_anchor": "2026-03-10"},
    ]
    coverage_docs = [
        {"_id": "A.US", "dates": ["2026-03-11"]},
        {"_id": "B.US", "dates": ["2026-03-11"]},
    ]
    db = _mock_db(
        visible_tickers=["A.US", "B.US"],
        history_download_completed_count=2,
        bulk_days=[
            # Real trading day with data
            {"processed_date": "2026-03-11", "status": "success", "rows_written": 5000, "matched_seeded_tickers_count": 5000},
            # Saturday — NOT a trading day, but leaked into ops_job_runs
            # with rows_written > 0 (data quirk / bad remediation run)
            {"processed_date": "2026-03-14", "status": "success", "rows_written": 3000, "matched_seeded_tickers_count": 4500},
        ],
        proven_ticker_docs=proven_docs,
        gap_free_coverage_docs=coverage_docs,
        # Only 2026-03-11 is a trading day; 2026-03-14 is NOT
        calendar_trading_dates=["2026-03-11"],
    )
    result = asyncio.run(get_price_integrity_metrics(db))
    # Both tickers have coverage for 2026-03-11.  2026-03-14 is dropped by
    # the calendar filter (non-trading day).  Without the fix, both would
    # fail because no ticker has stock_prices for a Saturday.
    assert result["gap_free_since_history_download_count"] == 2


def test_gap_free_all_non_trading_days_means_trivially_gap_free():
    """When ALL bulk dates are non-trading (filtered out by calendar),
    expected_dates becomes empty → proven tickers are trivially gap-free."""
    proven_docs = [
        {"ticker": "A.US", "history_download_proven_anchor": "2026-03-10"},
    ]
    db = _mock_db(
        visible_tickers=["A.US"],
        history_download_completed_count=1,
        bulk_days=[
            # Both are non-trading days (both will be filtered out by calendar)
            {"processed_date": "2026-03-14", "status": "success", "rows_written": 2000, "matched_seeded_tickers_count": 4500},
            {"processed_date": "2026-03-15", "status": "success", "rows_written": 1500, "matched_seeded_tickers_count": 4200},
        ],
        proven_ticker_docs=proven_docs,
        # Neither date is a trading day
        calendar_trading_dates=[],
    )
    result = asyncio.run(get_price_integrity_metrics(db))
    # expected_dates is empty → elif proven_anchors: gap_free = len(proven_anchors)
    assert result["gap_free_since_history_download_count"] == 1


def test_gap_free_excludes_partial_bulk_days():
    """Days with matched_seeded_tickers_count below sanity threshold must be
    excluded from expected_dates.

    This prevents partial / failed ingestion days (where EODHD returned
    data for only a handful of tickers) from becoming expected_dates.
    Without this filter, every ticker would fail gap-free because most
    tickers have no stock_prices for that date.
    """
    proven_docs = [
        {"ticker": "A.US", "history_download_proven_anchor": "2026-03-10"},
    ]
    coverage_docs = [
        {"_id": "A.US", "dates": ["2026-03-11"]},
    ]
    db = _mock_db(
        visible_tickers=["A.US"],
        history_download_completed_count=1,
        bulk_days=[
            # Full ingestion — passes all checks
            {"processed_date": "2026-03-11", "status": "success",
             "rows_written": 5000, "matched_seeded_tickers_count": 5000},
            # Partial ingestion — rows_written > 0 but matched count too low
            {"processed_date": "2026-03-12", "status": "success",
             "rows_written": 200, "matched_seeded_tickers_count": 150},
        ],
        proven_ticker_docs=proven_docs,
        gap_free_coverage_docs=coverage_docs,
    )
    result = asyncio.run(get_price_integrity_metrics(db))
    # A.US has coverage for 2026-03-11.  2026-03-12 is excluded (matched < 4000).
    assert result["gap_free_since_history_download_count"] == 1


def test_gap_free_excludes_day_missing_matched_count():
    """Days without matched_seeded_tickers_count field → excluded (fail-closed)."""
    proven_docs = [
        {"ticker": "A.US", "history_download_proven_anchor": "2026-03-10"},
    ]
    coverage_docs = [
        {"_id": "A.US", "dates": ["2026-03-11"]},
    ]
    db = _mock_db(
        visible_tickers=["A.US"],
        history_download_completed_count=1,
        bulk_days=[
            {"processed_date": "2026-03-11", "status": "success",
             "rows_written": 5000, "matched_seeded_tickers_count": 5000},
            # Has rows but no matched_seeded_tickers_count → excluded
            {"processed_date": "2026-03-12", "status": "success",
             "rows_written": 5000},
        ],
        proven_ticker_docs=proven_docs,
        gap_free_coverage_docs=coverage_docs,
    )
    result = asyncio.run(get_price_integrity_metrics(db))
    assert result["gap_free_since_history_download_count"] == 1
    """Coverage checkpoints present with correct 'kind' field: recent or historical."""
    db = _mock_db(
        bulk_state={"global_last_bulk_date_processed": "2026-03-20"},
        checkpoint_counts={"2026-03-20": 3},
    )
    result = asyncio.run(get_price_integrity_metrics(db))
    cp = result["coverage_checkpoints"]
    assert cp["last_closing_day"]["kind"] == "recent"
    assert cp["1_week_ago"]["kind"] == "recent"
    assert cp["1_month_ago"]["kind"] == "historical"
    assert cp["1_year_ago"]["kind"] == "historical"


def test_zero_visible_returns_safe_defaults():
    db = _mock_db(visible_tickers=[])
    result = asyncio.run(get_price_integrity_metrics(db))
    assert result["today_visible"] == 0
    assert result["needs_price_redownload"] == 0
    assert result["full_price_history_count"] == 0
    assert result["history_download_completed_count"] == 0
    assert result["gap_free_since_history_download_count"] == 0
    assert result["fundamentals_complete_count"] == 0
    assert result["completed_trading_days_health"] is None
    assert result["coverage_checkpoints"] == {}
    assert "today_visible_source" in result


def test_completed_trading_days_health_present():
    """New metric: completed_trading_days_health is present in result."""
    db = _mock_db()
    result = asyncio.run(get_price_integrity_metrics(db))
    # With empty market_calendar, should get a health dict with yellow status
    ctdh = result["completed_trading_days_health"]
    assert ctdh is not None
    assert "status" in ctdh
    assert "days" in ctdh
    assert "ok_count" in ctdh
    assert "missing_count" in ctdh


def test_completed_trading_days_health_with_no_calendar():
    """When market_calendar is empty, status should be yellow."""
    db = _mock_db(bulk_days=[])
    result = asyncio.run(get_price_integrity_metrics(db))
    ctdh = result["completed_trading_days_health"]
    assert ctdh["status"] == "yellow"
    assert ctdh["days"] == []


def test_completed_trading_days_health_key_replaces_missing_expected_dates():
    """Confirm missing_expected_dates is no longer in the result."""
    db = _mock_db()
    result = asyncio.run(get_price_integrity_metrics(db))
    assert "missing_expected_dates" not in result
    assert "completed_trading_days_health" in result


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


def test_morning_refresh_standalone_only():
    """Morning Refresh must query only standalone price_sync (no chain_run_id)."""
    now = datetime.now(timezone.utc)
    recent = now - timedelta(hours=3)

    call_args_list = []

    async def _mock_find_one(query, *args, **kwargs):
        call_args_list.append(query)
        if query.get("job_name") == "fundamentals_sync":
            return {"finished_at": recent}
        if query.get("job_name") == "price_sync":
            # Standalone: no chain_run_id → should match the $or filter
            return None  # no standalone runs exist
        return None

    ops_job_runs = SimpleNamespace(find_one=_mock_find_one)
    db = SimpleNamespace(ops_job_runs=ops_job_runs)

    result = asyncio.run(get_pipeline_last_success_age(db))
    assert result["pipeline_status"] == "green"
    # Morning refresh should be unknown when no standalone runs exist
    assert result["morning_refresh_hours_since_success"] is None
    assert result["morning_refresh_status"] == "unknown"
    # Verify the morning refresh query includes the chain_run_id exclusion
    mr_query = call_args_list[1]
    assert "$or" in mr_query, "Morning refresh query must filter out chain runs via $or"
