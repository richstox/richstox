"""
Tests for bulk completeness proof, baseline marker, and visible coverage.

Covers:
  A) Baseline marker written on successful full backfill only
     - Not written when killed / skipped / no tickers
     - Written when completed (both "already complete" and "ran to completion")
     - through_date derived from canonical sources (not "today")
     - No baseline if through_date cannot be canonically proven
  B) Completed trading day enumeration via market_calendar
  C) Missing bulk date detection since baseline (bounded query)
  D) Visible ticker coverage calculations
  E) Baseline-missing state
"""

import asyncio
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from zoneinfo import ZoneInfo

# ── Helpers ──────────────────────────────────────────────────────────────────

PRAGUE_TZ = ZoneInfo("Europe/Prague")
NY_TZ = ZoneInfo("America/New_York")


def _make_cursor(docs):
    """Create an async iterable cursor that also has .to_list() and .sort().limit()."""
    class _Cursor:
        def __init__(self, data):
            self._data = list(data)
        def sort(self, *args, **kwargs):
            if args and args[0] == "date":
                direction = args[1] if len(args) > 1 else -1
                reverse = direction == -1
                self._data = sorted(self._data, key=lambda d: d.get("date", ""), reverse=reverse)
            elif args:
                key_name = args[0]
                direction = args[1] if len(args) > 1 else -1
                reverse = direction == -1
                self._data = sorted(self._data, key=lambda d: d.get(key_name, ""), reverse=reverse)
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


def _build_calendar_docs(trading_dates, non_trading_dates=None, market="US"):
    """Build mock market_calendar documents."""
    docs = []
    for d in trading_dates:
        docs.append({
            "market": market,
            "date": d,
            "is_trading_day": True,
            "trading_hours": {"open": "09:30", "close": "16:00"},
        })
    for d in (non_trading_dates or []):
        docs.append({
            "market": market,
            "date": d,
            "is_trading_day": False,
        })
    return docs


def _build_ops_job_runs_with_bulk_days(processed_dates):
    """Build mock ops_job_runs docs with price_bulk_gapfill.days entries."""
    days = [
        {"processed_date": d, "status": "success", "rows_written": 100}
        for d in processed_dates
    ]
    return [{
        "job_name": "price_sync",
        "status": "success",
        "finished_at": datetime(2026, 3, 30, tzinfo=timezone.utc),
        "details": {
            "price_bulk_gapfill": {"days": days}
        },
    }]


# =============================================================================
# A) BASELINE MARKER TESTS
# =============================================================================

class TestBaselineMarker:
    """Test that the full backfill baseline is written correctly."""

    @pytest.fixture
    def mock_db(self):
        """Build a mock DB for run_scheduled_backfill_all_prices."""
        db = MagicMock()
        db.ops_job_runs.insert_one = AsyncMock(
            return_value=SimpleNamespace(inserted_id="abc123")
        )
        db.ops_job_runs.find_one = AsyncMock(
            return_value={"_id": "job_run_xyz"}
        )
        db.pipeline_state.find_one = AsyncMock(
            return_value={"_id": "price_bulk", "global_last_bulk_date_processed": "2026-03-28"}
        )
        db.pipeline_state.update_one = AsyncMock()
        db.ops_config.find_one = AsyncMock(
            return_value={"key": "scheduler_enabled", "value": True}
        )
        db.tracked_tickers.distinct = AsyncMock(return_value=[])
        db.stock_prices.aggregate = MagicMock(return_value=_make_cursor([]))
        return db

    @pytest.mark.asyncio
    async def test_baseline_written_when_all_tickers_complete(self, mock_db):
        """When no tickers need backfill and canonical through_date is provable → baseline is written."""
        from parallel_batch_service import run_scheduled_backfill_all_prices

        with patch("parallel_batch_service.get_tickers_without_full_prices",
                   new_callable=AsyncMock, return_value=[]):
            with patch("scheduler_service.get_scheduler_enabled",
                       new_callable=AsyncMock, return_value=True):
                # Patch _compute_canonical_through_date to return canonical value
                with patch("parallel_batch_service._compute_canonical_through_date",
                           new_callable=AsyncMock, return_value="2026-03-28"):
                    result = await run_scheduled_backfill_all_prices(mock_db)

        assert result["status"] == "completed"
        # Verify baseline was written via pipeline_state.update_one
        mock_db.pipeline_state.update_one.assert_called_once()
        call_args = mock_db.pipeline_state.update_one.call_args
        assert call_args[0][0] == {"_id": "full_backfill_baseline"}
        set_doc = call_args[0][1]["$set"]
        assert set_doc["_id"] == "full_backfill_baseline"
        assert set_doc["through_date"] == "2026-03-28"
        assert set_doc["job_run_id"] == "abc123"
        assert "completed_at" in set_doc
        # No completed_at_prague in persisted doc
        assert "completed_at_prague" not in set_doc

    @pytest.mark.asyncio
    async def test_baseline_not_written_when_killed(self, mock_db):
        """When backfill is killed → baseline must NOT be written."""
        from parallel_batch_service import (
            run_scheduled_backfill_all_prices,
            BatchResult,
        )

        killed_result = BatchResult(
            job_name="scheduled_backfill_all_prices",
            started_at=datetime.now(timezone.utc),
            finished_at=datetime.now(timezone.utc),
            killed=True,
            kill_reason="Kill switch engaged",
        )

        with patch("parallel_batch_service.get_tickers_without_full_prices",
                   new_callable=AsyncMock, return_value=["AAPL.US"]):
            with patch("scheduler_service.get_scheduler_enabled",
                       new_callable=AsyncMock, return_value=True):
                with patch("parallel_batch_service.run_parallel_price_backfill",
                           new_callable=AsyncMock, return_value=killed_result):
                    result = await run_scheduled_backfill_all_prices(mock_db)

        assert result["killed"] is True
        # Baseline should NOT have been written
        mock_db.pipeline_state.update_one.assert_not_called()

    @pytest.mark.asyncio
    async def test_baseline_not_written_when_skipped(self, mock_db):
        """When kill switch is engaged at start → skipped, no baseline."""
        from parallel_batch_service import run_scheduled_backfill_all_prices

        with patch("scheduler_service.get_scheduler_enabled",
                   new_callable=AsyncMock, return_value=False):
            result = await run_scheduled_backfill_all_prices(mock_db)

        assert result["status"] == "skipped"
        mock_db.pipeline_state.update_one.assert_not_called()

    @pytest.mark.asyncio
    async def test_baseline_written_on_successful_completion(self, mock_db):
        """When backfill runs to completion without being killed → baseline is written."""
        from parallel_batch_service import (
            run_scheduled_backfill_all_prices,
            BatchResult,
        )

        success_result = BatchResult(
            job_name="scheduled_backfill_all_prices",
            started_at=datetime.now(timezone.utc),
            finished_at=datetime.now(timezone.utc),
            killed=False,
            processed=10,
            success=10,
        )

        with patch("parallel_batch_service.get_tickers_without_full_prices",
                   new_callable=AsyncMock, return_value=["AAPL.US"]):
            with patch("scheduler_service.get_scheduler_enabled",
                       new_callable=AsyncMock, return_value=True):
                with patch("parallel_batch_service.run_parallel_price_backfill",
                           new_callable=AsyncMock, return_value=success_result):
                    with patch("parallel_batch_service._compute_canonical_through_date",
                               new_callable=AsyncMock, return_value="2026-03-28"):
                        result = await run_scheduled_backfill_all_prices(mock_db)

        assert result["killed"] is False
        # Baseline SHOULD have been written
        mock_db.pipeline_state.update_one.assert_called_once()
        call_args = mock_db.pipeline_state.update_one.call_args
        assert call_args[0][0] == {"_id": "full_backfill_baseline"}

    @pytest.mark.asyncio
    async def test_no_baseline_when_through_date_not_provable(self, mock_db):
        """When through_date cannot be canonically derived → no baseline written."""
        from parallel_batch_service import run_scheduled_backfill_all_prices

        with patch("parallel_batch_service.get_tickers_without_full_prices",
                   new_callable=AsyncMock, return_value=[]):
            with patch("scheduler_service.get_scheduler_enabled",
                       new_callable=AsyncMock, return_value=True):
                # _compute_canonical_through_date returns None → can't prove
                with patch("parallel_batch_service._compute_canonical_through_date",
                           new_callable=AsyncMock, return_value=None):
                    result = await run_scheduled_backfill_all_prices(mock_db)

        assert result["status"] == "completed"
        # Baseline should NOT have been written (through_date unproven)
        mock_db.pipeline_state.update_one.assert_not_called()


# =============================================================================
# B) BULK COMPLETENESS PROOF TESTS
# =============================================================================

class TestBulkCompletenessSinceBaseline:
    """Test bulk completeness proof since last full backfill."""

    def _mock_db_for_completeness(
        self,
        baseline_doc=None,
        calendar_trading_dates=None,
        bulk_processed_dates=None,
    ):
        """Build mock DB for get_bulk_completeness_since_baseline."""
        db = MagicMock()

        # pipeline_state: baseline
        db.pipeline_state.find_one = AsyncMock(return_value=baseline_doc)

        # market_calendar: trading days
        all_calendar = _build_calendar_docs(calendar_trading_dates or [])

        # Mock the market_calendar collection with find().sort().limit().to_list()
        def _mc_find(query, projection=None):
            market = query.get("market", "US")
            is_td = query.get("is_trading_day")
            date_filter = query.get("date", {})

            matching = [
                d for d in all_calendar
                if d["market"] == market
                and (is_td is None or d.get("is_trading_day") == is_td)
            ]

            if isinstance(date_filter, dict):
                if "$lte" in date_filter:
                    matching = [d for d in matching if d["date"] <= date_filter["$lte"]]
                if "$lt" in date_filter:
                    matching = [d for d in matching if d["date"] < date_filter["$lt"]]
                if "$gt" in date_filter:
                    matching = [d for d in matching if d["date"] > date_filter["$gt"]]
                if "$in" in date_filter:
                    matching = [d for d in matching if d["date"] in date_filter["$in"]]

            return _make_cursor(matching)

        db.market_calendar = MagicMock()
        db.market_calendar.find = MagicMock(side_effect=_mc_find)
        db.market_calendar.find_one = AsyncMock(return_value=None)

        # Override __getitem__ for db["market_calendar"]
        def _getitem(key):
            if key == "market_calendar":
                return db.market_calendar
            return MagicMock()
        db.__getitem__ = MagicMock(side_effect=_getitem)

        # ops_job_runs: bulk processed dates
        ops_runs = _build_ops_job_runs_with_bulk_days(bulk_processed_dates or [])
        db.ops_job_runs = MagicMock()
        db.ops_job_runs.aggregate = MagicMock(return_value=_make_cursor(ops_runs))

        return db

    @pytest.mark.asyncio
    async def test_no_baseline_returns_sentinel(self):
        """When no baseline exists → return no_baseline sentinel."""
        db = self._mock_db_for_completeness(baseline_doc=None)
        from services.admin_overview_service import get_bulk_completeness_since_baseline
        result = await get_bulk_completeness_since_baseline(db)
        assert result["has_baseline"] is False
        assert result["gap_free_since_baseline"] is None
        assert "No baseline yet" in result.get("message", "")

    @pytest.mark.asyncio
    async def test_gap_free_when_all_dates_processed(self):
        """All completed trading days since baseline have bulk data → gap-free."""
        baseline = {
            "_id": "full_backfill_baseline",
            "completed_at": datetime(2026, 3, 20, tzinfo=timezone.utc),
            "through_date": "2026-03-20",
            "job_run_id": "run123",
        }
        trading_dates = ["2026-03-20", "2026-03-23", "2026-03-24", "2026-03-25"]
        bulk_dates = ["2026-03-23", "2026-03-24", "2026-03-25"]

        db = self._mock_db_for_completeness(
            baseline_doc=baseline,
            calendar_trading_dates=trading_dates,
            bulk_processed_dates=bulk_dates,
        )

        from services.admin_overview_service import get_bulk_completeness_since_baseline

        # Patch last_n_completed_trading_days to avoid complex DB mock
        with patch(
            "services.admin_overview_service.last_n_completed_trading_days",
            new_callable=AsyncMock,
            return_value=["2026-03-25", "2026-03-24", "2026-03-23", "2026-03-20"],
        ):
            result = await get_bulk_completeness_since_baseline(db)

        assert result["has_baseline"] is True
        assert result["gap_free_since_baseline"] is True
        assert result["missing_count"] == 0
        assert result["missing_bulk_dates_since_baseline"] == []
        assert result["expected_days_count"] == 3  # 3 days after through_date

    @pytest.mark.asyncio
    async def test_gaps_detected_when_dates_missing(self):
        """Missing bulk dates are detected and reported."""
        baseline = {
            "_id": "full_backfill_baseline",
            "completed_at": datetime(2026, 3, 20, tzinfo=timezone.utc),
            "through_date": "2026-03-20",
            "job_run_id": "run123",
        }
        trading_dates = ["2026-03-20", "2026-03-23", "2026-03-24", "2026-03-25"]
        # Only 2026-03-23 processed, missing 24 and 25
        bulk_dates = ["2026-03-23"]

        db = self._mock_db_for_completeness(
            baseline_doc=baseline,
            calendar_trading_dates=trading_dates,
            bulk_processed_dates=bulk_dates,
        )

        from services.admin_overview_service import get_bulk_completeness_since_baseline

        with patch(
            "services.admin_overview_service.last_n_completed_trading_days",
            new_callable=AsyncMock,
            return_value=["2026-03-25", "2026-03-24", "2026-03-23", "2026-03-20"],
        ):
            result = await get_bulk_completeness_since_baseline(db)

        assert result["has_baseline"] is True
        assert result["gap_free_since_baseline"] is False
        assert result["missing_count"] == 2
        assert "2026-03-24" in result["missing_bulk_dates_since_baseline"]
        assert "2026-03-25" in result["missing_bulk_dates_since_baseline"]

    @pytest.mark.asyncio
    async def test_baseline_through_date_filtering(self):
        """Only days AFTER through_date are expected; through_date itself is not."""
        baseline = {
            "_id": "full_backfill_baseline",
            "completed_at": datetime(2026, 3, 25, tzinfo=timezone.utc),
            "through_date": "2026-03-25",
            "job_run_id": "run123",
        }
        # All completed days are at or before through_date
        all_completed = ["2026-03-25", "2026-03-24", "2026-03-23"]

        db = self._mock_db_for_completeness(
            baseline_doc=baseline,
            calendar_trading_dates=all_completed,
            bulk_processed_dates=[],
        )

        from services.admin_overview_service import get_bulk_completeness_since_baseline

        with patch(
            "services.admin_overview_service.last_n_completed_trading_days",
            new_callable=AsyncMock,
            return_value=all_completed,
        ):
            result = await get_bulk_completeness_since_baseline(db)

        # No expected days → gap_free = True, missing = 0
        assert result["gap_free_since_baseline"] is True
        assert result["missing_count"] == 0
        assert result["expected_days_count"] == 0

    @pytest.mark.asyncio
    async def test_baseline_info_returned(self):
        """Baseline info fields are properly returned."""
        baseline = {
            "_id": "full_backfill_baseline",
            "completed_at": datetime(2026, 3, 20, 10, 0, 0, tzinfo=timezone.utc),
            "through_date": "2026-03-20",
            "job_run_id": "abc123",
        }

        db = self._mock_db_for_completeness(
            baseline_doc=baseline,
            calendar_trading_dates=["2026-03-20"],
            bulk_processed_dates=[],
        )

        from services.admin_overview_service import get_bulk_completeness_since_baseline

        with patch(
            "services.admin_overview_service.last_n_completed_trading_days",
            new_callable=AsyncMock,
            return_value=["2026-03-20"],
        ):
            result = await get_bulk_completeness_since_baseline(db)

        assert result["has_baseline"] is True
        bl = result["baseline"]
        assert bl["through_date"] == "2026-03-20"
        assert bl["job_run_id"] == "abc123"
        # completed_at_prague is derived at read time (not persisted)
        assert bl["completed_at_prague"] is not None
        assert "2026-03-20" in bl["completed_at_prague"]

    @pytest.mark.asyncio
    async def test_no_through_date_in_baseline(self):
        """Baseline exists but through_date is missing → edge case."""
        baseline = {
            "_id": "full_backfill_baseline",
            "completed_at": datetime(2026, 3, 20, tzinfo=timezone.utc),
            "through_date": None,
            "job_run_id": "abc",
        }

        db = self._mock_db_for_completeness(baseline_doc=baseline)

        from services.admin_overview_service import get_bulk_completeness_since_baseline

        result = await get_bulk_completeness_since_baseline(db)
        assert result["has_baseline"] is True
        assert result["gap_free_since_baseline"] is True
        assert result["expected_days_count"] == 0


# =============================================================================
# C) VISIBLE COVERAGE TESTS
# =============================================================================

class TestVisibleCoverage:
    """Test visible ticker coverage analysis."""

    def _mock_db_for_coverage(
        self,
        visible_count=100,
        fundamentals_complete_count=80,
        last_bulk_date="2026-03-28",
        price_coverage_count=95,
    ):
        """Build mock DB for get_visible_coverage."""
        db = MagicMock()

        # tracked_tickers aggregate (faceted)
        facet_result = [{
            "total": [{"n": visible_count}] if visible_count > 0 else [],
            "fundamentals_complete": [{"n": fundamentals_complete_count}] if fundamentals_complete_count > 0 else [],
        }]
        db.tracked_tickers.aggregate = MagicMock(
            return_value=_make_cursor(facet_result)
        )

        # tracked_tickers.distinct (for visible tickers)
        db.tracked_tickers.distinct = AsyncMock(
            return_value=[f"T{i}.US" for i in range(visible_count)]
        )

        # pipeline_state for last bulk date
        bulk_state = {"_id": "price_bulk", "global_last_bulk_date_processed": last_bulk_date} if last_bulk_date else None
        db.pipeline_state.find_one = AsyncMock(return_value=bulk_state)

        # stock_prices aggregate (price coverage count)
        price_result = [{"n": price_coverage_count}] if price_coverage_count > 0 else []
        db.stock_prices.aggregate = MagicMock(
            return_value=_make_cursor(price_result)
        )

        return db

    @pytest.mark.asyncio
    async def test_full_coverage(self):
        """100% coverage for both prices and fundamentals."""
        db = self._mock_db_for_coverage(
            visible_count=100,
            fundamentals_complete_count=100,
            price_coverage_count=100,
        )
        from services.admin_overview_service import get_visible_coverage
        result = await get_visible_coverage(db)

        assert result["visible_total"] == 100
        assert result["price_coverage_count"] == 100
        assert result["price_coverage_pct"] == 100
        assert result["fundamentals_complete_count"] == 100
        assert result["fundamentals_complete_pct"] == 100

    @pytest.mark.asyncio
    async def test_partial_coverage(self):
        """Partial coverage returns correct percentages."""
        db = self._mock_db_for_coverage(
            visible_count=200,
            fundamentals_complete_count=150,
            price_coverage_count=180,
        )
        from services.admin_overview_service import get_visible_coverage
        result = await get_visible_coverage(db)

        assert result["visible_total"] == 200
        assert result["price_coverage_count"] == 180
        assert result["price_coverage_pct"] == 90
        assert result["fundamentals_complete_count"] == 150
        assert result["fundamentals_complete_pct"] == 75

    @pytest.mark.asyncio
    async def test_zero_visible_tickers(self):
        """Zero visible tickers → safe defaults."""
        db = self._mock_db_for_coverage(
            visible_count=0,
            fundamentals_complete_count=0,
            price_coverage_count=0,
        )
        from services.admin_overview_service import get_visible_coverage
        result = await get_visible_coverage(db)

        assert result["visible_total"] == 0
        assert result["price_coverage_pct"] == 0
        assert result["fundamentals_complete_pct"] == 0

    @pytest.mark.asyncio
    async def test_no_bulk_date(self):
        """No bulk date → price coverage is 0."""
        db = self._mock_db_for_coverage(
            visible_count=100,
            last_bulk_date=None,
            price_coverage_count=0,
        )
        from services.admin_overview_service import get_visible_coverage
        result = await get_visible_coverage(db)

        assert result["visible_total"] == 100
        assert result["latest_bulk_date"] is None
        assert result["price_coverage_count"] == 0


# =============================================================================
# D) FULL BACKFILL BASELINE WRITE FUNCTION
# =============================================================================

class TestWriteFullBackfillBaseline:
    """Test _write_full_backfill_baseline function directly."""

    @pytest.mark.asyncio
    async def test_writes_correct_document(self):
        """The baseline document is correctly structured."""
        from parallel_batch_service import _write_full_backfill_baseline

        db = MagicMock()
        db.pipeline_state.find_one = AsyncMock(
            return_value={"_id": "price_bulk", "global_last_bulk_date_processed": "2026-03-28"}
        )
        db.pipeline_state.update_one = AsyncMock()

        finished = datetime(2026, 3, 28, 15, 0, 0, tzinfo=timezone.utc)
        # Patch _compute_canonical_through_date to return canonical value
        with patch("parallel_batch_service._compute_canonical_through_date",
                   new_callable=AsyncMock, return_value="2026-03-28"):
            written = await _write_full_backfill_baseline(db, finished, "run_abc")

        assert written is True
        db.pipeline_state.update_one.assert_called_once()
        call_args = db.pipeline_state.update_one.call_args
        assert call_args[0][0] == {"_id": "full_backfill_baseline"}
        set_doc = call_args[0][1]["$set"]
        assert set_doc["through_date"] == "2026-03-28"
        assert set_doc["job_run_id"] == "run_abc"
        assert set_doc["completed_at"] == finished
        # No completed_at_prague in persisted doc (derived at read time)
        assert "completed_at_prague" not in set_doc
        # upsert=True
        assert call_args[1].get("upsert") is True or (len(call_args[0]) > 2 and call_args[0][2] is True)

    @pytest.mark.asyncio
    async def test_returns_false_when_through_date_unproven(self):
        """through_date cannot be proven → returns False, no write."""
        from parallel_batch_service import _write_full_backfill_baseline

        db = MagicMock()
        db.pipeline_state.update_one = AsyncMock()

        finished = datetime(2026, 3, 28, 15, 0, 0, tzinfo=timezone.utc)
        with patch("parallel_batch_service._compute_canonical_through_date",
                   new_callable=AsyncMock, return_value=None):
            written = await _write_full_backfill_baseline(db, finished, "run_xyz")

        assert written is False
        db.pipeline_state.update_one.assert_not_called()


# =============================================================================
# E) BOUNDED BULK PROCESSED DATES SET
# =============================================================================

class TestBoundedBulkProcessedDatesSet:
    """Test the bounded bulk processed dates set function."""

    @pytest.mark.asyncio
    async def test_collects_successful_dates_in_range(self):
        """Successfully processed dates within range are collected."""
        from services.admin_overview_service import _get_bounded_bulk_processed_dates_set

        ops_runs = [{
            "job_name": "price_sync",
            "status": "success",
            "details": {
                "price_bulk_gapfill": {
                    "days": [
                        {"processed_date": "2026-03-23", "status": "success", "rows_written": 100},
                        {"processed_date": "2026-03-24", "status": "success", "rows_written": 50},
                        {"processed_date": "2026-03-25", "status": "failed", "rows_written": 0},
                        # Outside range: should be excluded
                        {"processed_date": "2026-03-15", "status": "success", "rows_written": 100},
                    ]
                }
            },
        }]

        db = MagicMock()
        db.ops_job_runs.aggregate = MagicMock(return_value=_make_cursor(ops_runs))

        dates = await _get_bounded_bulk_processed_dates_set(
            db, after_date="2026-03-20", through_date="2026-03-25",
        )
        assert "2026-03-23" in dates
        assert "2026-03-24" in dates
        assert "2026-03-25" not in dates  # failed
        assert "2026-03-15" not in dates  # outside range

    @pytest.mark.asyncio
    async def test_zero_rows_written_not_included(self):
        """Dates with rows_written=0 are not included."""
        from services.admin_overview_service import _get_bounded_bulk_processed_dates_set

        ops_runs = [{
            "job_name": "price_sync",
            "status": "success",
            "details": {
                "price_bulk_gapfill": {
                    "days": [
                        {"processed_date": "2026-03-23", "status": "success", "rows_written": 0},
                    ]
                }
            },
        }]

        db = MagicMock()
        db.ops_job_runs.aggregate = MagicMock(return_value=_make_cursor(ops_runs))

        dates = await _get_bounded_bulk_processed_dates_set(
            db, after_date="2026-03-20", through_date="2026-03-25",
        )
        assert "2026-03-23" not in dates

    @pytest.mark.asyncio
    async def test_empty_runs_returns_empty(self):
        """No runs → empty set."""
        from services.admin_overview_service import _get_bounded_bulk_processed_dates_set

        db = MagicMock()
        db.ops_job_runs.aggregate = MagicMock(return_value=_make_cursor([]))

        dates = await _get_bounded_bulk_processed_dates_set(
            db, after_date="2026-03-20", through_date="2026-03-25",
        )
        assert len(dates) == 0

    @pytest.mark.asyncio
    async def test_dates_outside_range_excluded(self):
        """Dates before after_date and after through_date are excluded."""
        from services.admin_overview_service import _get_bounded_bulk_processed_dates_set

        ops_runs = [{
            "job_name": "price_sync",
            "status": "success",
            "details": {
                "price_bulk_gapfill": {
                    "days": [
                        {"processed_date": "2026-03-19", "status": "success", "rows_written": 100},
                        {"processed_date": "2026-03-20", "status": "success", "rows_written": 100},
                        {"processed_date": "2026-03-23", "status": "success", "rows_written": 100},
                        {"processed_date": "2026-03-26", "status": "success", "rows_written": 100},
                    ]
                }
            },
        }]

        db = MagicMock()
        db.ops_job_runs.aggregate = MagicMock(return_value=_make_cursor(ops_runs))

        dates = await _get_bounded_bulk_processed_dates_set(
            db, after_date="2026-03-20", through_date="2026-03-25",
        )
        # Only 2026-03-23 is in (2026-03-20, 2026-03-25]
        assert "2026-03-19" not in dates  # before range
        assert "2026-03-20" not in dates  # == after_date (exclusive)
        assert "2026-03-23" in dates      # in range
        assert "2026-03-26" not in dates  # after through_date


# =============================================================================
# F) COMPUTE CANONICAL THROUGH DATE
# =============================================================================

class TestComputeCanonicalThroughDate:
    """Test _compute_canonical_through_date."""

    @pytest.mark.asyncio
    async def test_returns_min_of_bulk_and_calendar(self):
        """through_date = min(latest_bulk, latest_completed_trading_day)."""
        from parallel_batch_service import _compute_canonical_through_date

        db = MagicMock()
        db.pipeline_state.find_one = AsyncMock(
            return_value={"_id": "price_bulk", "global_last_bulk_date_processed": "2026-03-25"}
        )

        with patch("services.market_calendar_service.last_n_completed_trading_days",
                   new_callable=AsyncMock, return_value=["2026-03-28"]):
            result = await _compute_canonical_through_date(db)

        # min("2026-03-25", "2026-03-28") = "2026-03-25"
        assert result == "2026-03-25"

    @pytest.mark.asyncio
    async def test_returns_none_when_no_bulk_date(self):
        """No bulk date → None (cannot prove)."""
        from parallel_batch_service import _compute_canonical_through_date

        db = MagicMock()
        db.pipeline_state.find_one = AsyncMock(return_value=None)

        with patch("services.market_calendar_service.last_n_completed_trading_days",
                   new_callable=AsyncMock, return_value=["2026-03-28"]):
            result = await _compute_canonical_through_date(db)

        assert result is None

    @pytest.mark.asyncio
    async def test_returns_none_when_no_calendar(self):
        """No calendar data → None (cannot prove)."""
        from parallel_batch_service import _compute_canonical_through_date

        db = MagicMock()
        db.pipeline_state.find_one = AsyncMock(
            return_value={"_id": "price_bulk", "global_last_bulk_date_processed": "2026-03-25"}
        )

        with patch("services.market_calendar_service.last_n_completed_trading_days",
                   new_callable=AsyncMock, return_value=[]):
            result = await _compute_canonical_through_date(db)

        assert result is None

    @pytest.mark.asyncio
    async def test_calendar_before_bulk(self):
        """Calendar latest < bulk latest → returns calendar value."""
        from parallel_batch_service import _compute_canonical_through_date

        db = MagicMock()
        db.pipeline_state.find_one = AsyncMock(
            return_value={"_id": "price_bulk", "global_last_bulk_date_processed": "2026-03-28"}
        )

        with patch("services.market_calendar_service.last_n_completed_trading_days",
                   new_callable=AsyncMock, return_value=["2026-03-25"]):
            result = await _compute_canonical_through_date(db)

        # min("2026-03-28", "2026-03-25") = "2026-03-25"
        assert result == "2026-03-25"


# =============================================================================
# G) BASELINE CONSTANT IMPORT
# =============================================================================

class TestBaselineConstantConsistency:
    """Verify the baseline ID constant is consistent across modules."""

    def test_baseline_id_in_parallel_batch(self):
        from parallel_batch_service import FULL_BACKFILL_BASELINE_ID
        assert FULL_BACKFILL_BASELINE_ID == "full_backfill_baseline"

    def test_baseline_id_in_admin_overview(self):
        from services.admin_overview_service import FULL_BACKFILL_BASELINE_ID
        assert FULL_BACKFILL_BASELINE_ID == "full_backfill_baseline"

    def test_ids_match(self):
        from parallel_batch_service import FULL_BACKFILL_BASELINE_ID as id1
        from services.admin_overview_service import FULL_BACKFILL_BASELINE_ID as id2
        assert id1 == id2
