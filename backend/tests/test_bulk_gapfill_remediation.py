"""
Tests for bulk_gapfill_remediation: detection, bounded remediation,
audit proof format, reader widening, and idempotency.
"""

import asyncio
import os
import sys
from copy import deepcopy
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import scheduler_service


# ── Helpers ──────────────────────────────────────────────────────────────────


def _set_path(doc: dict, dotted_key: str, value: Any) -> None:
    """Set a nested dotted-key path on a dict."""
    parts = dotted_key.split(".")
    cur = doc
    for part in parts[:-1]:
        if part not in cur or not isinstance(cur[part], dict):
            cur[part] = {}
        cur = cur[part]
    cur[parts[-1]] = value


class _FakeCursor:
    """Async cursor that supports to_list()."""

    def __init__(self, docs):
        self._docs = list(docs)

    async def to_list(self, _length=None):
        return deepcopy(self._docs)


class _FakeAggregateCursor:
    """Async iterable cursor returned by aggregate()."""

    def __init__(self, docs):
        self._docs = list(docs)

    def __aiter__(self):
        return _FakeAggregateCursorIter(self._docs)


class _FakeAggregateCursorIter:
    def __init__(self, docs):
        self._iter = iter(docs)

    async def __anext__(self):
        try:
            return next(self._iter)
        except StopIteration:
            raise StopAsyncIteration


class _FakeOpsJobRuns:
    """Minimal mock for db.ops_job_runs supporting insert_one, update_one, aggregate."""

    def __init__(self):
        self.docs: Dict[int, dict] = {}
        self._next_id = 1

    async def insert_one(self, doc):
        _id = self._next_id
        self._next_id += 1
        stored = deepcopy(doc)
        stored["_id"] = _id
        self.docs[_id] = stored
        return SimpleNamespace(inserted_id=_id)

    async def update_one(self, filt, update):
        _id = filt.get("_id")
        if _id not in self.docs:
            return SimpleNamespace(matched_count=0)
        for key, value in (update.get("$set") or {}).items():
            _set_path(self.docs[_id], key, deepcopy(value))
        return SimpleNamespace(matched_count=1)

    def aggregate(self, pipeline):
        """Return docs matching the $match stage for testing."""
        match_stage = pipeline[0].get("$match", {}) if pipeline else {}
        job_filter = match_stage.get("job_name")
        status_filter = match_stage.get("status", {}).get("$in", [])

        results = []
        for doc in self.docs.values():
            jn = doc.get("job_name")
            st = doc.get("status")
            has_days = bool(
                (doc.get("details") or {}).get("price_bulk_gapfill", {}).get("days")
            )
            # Check job_name filter
            if isinstance(job_filter, dict) and "$in" in job_filter:
                if jn not in job_filter["$in"]:
                    continue
            elif isinstance(job_filter, str):
                if jn != job_filter:
                    continue
            # Check status filter
            if status_filter and st not in status_filter:
                continue
            # Check days exists
            if match_stage.get("details.price_bulk_gapfill.days", {}).get("$exists"):
                if not has_days:
                    continue
            results.append(deepcopy(doc))

        # Sort by finished_at descending
        results.sort(key=lambda d: d.get("finished_at") or "", reverse=True)

        # Limit
        for stage in pipeline:
            if "$limit" in stage:
                results = results[: stage["$limit"]]

        return _FakeAggregateCursor(results)


class _FakeBulkWriteResult:
    def __init__(self, upserted=0, modified=0):
        self.upserted_count = upserted
        self.modified_count = modified


class _FakeStockPrices:
    """Minimal mock for db.stock_prices."""

    def __init__(self):
        self.ops: List[Any] = []

    async def bulk_write(self, ops, ordered=False):
        self.ops.extend(ops)
        return _FakeBulkWriteResult(upserted=len(ops), modified=0)


class _FakeTrackedTickers:
    """Minimal mock for db.tracked_tickers."""

    def __init__(self, tickers):
        self._tickers = tickers

    def find(self, query, projection=None):
        return _FakeCursor([{"ticker": t} for t in self._tickers])


def _build_db(
    tickers=None,
    existing_ops_docs=None,
):
    """Build a minimal fake db namespace."""
    db = SimpleNamespace()
    db.ops_job_runs = _FakeOpsJobRuns()
    db.stock_prices = _FakeStockPrices()
    db.tracked_tickers = _FakeTrackedTickers(tickers or ["AAPL.US", "MSFT.US"])

    # Pre-seed ops_job_runs docs if provided
    if existing_ops_docs:
        for doc in existing_ops_docs:
            _id = db.ops_job_runs._next_id
            db.ops_job_runs._next_id += 1
            stored = deepcopy(doc)
            stored["_id"] = _id
            db.ops_job_runs.docs[_id] = stored

    return db


# ── Tests ────────────────────────────────────────────────────────────────────


class TestDetection:
    """Missing-date detection produces correct oldest-first ordering."""

    @pytest.mark.asyncio
    async def test_missing_dates_oldest_first(self):
        """Completed days minus processed set should be sorted oldest-first."""
        completed = ["2026-03-25", "2026-03-26", "2026-03-27", "2026-03-28", "2026-03-31"]
        processed = {"2026-03-26", "2026-03-28"}

        missing = sorted([d for d in completed if d not in processed])
        assert missing == ["2026-03-25", "2026-03-27", "2026-03-31"]

    @pytest.mark.asyncio
    async def test_no_missing_dates(self):
        """When all completed days are processed, missing is empty."""
        completed = ["2026-03-25", "2026-03-26"]
        processed = {"2026-03-25", "2026-03-26"}

        missing = sorted([d for d in completed if d not in processed])
        assert missing == []

    @pytest.mark.asyncio
    async def test_all_dates_missing(self):
        """When no dates are processed, all completed days are missing."""
        completed = ["2026-03-25", "2026-03-26", "2026-03-27"]
        processed: set = set()

        missing = sorted([d for d in completed if d not in processed])
        assert missing == ["2026-03-25", "2026-03-26", "2026-03-27"]


class TestBoundedRemediation:
    """At most MAX_REMEDIATION_DAYS_PER_RUN dates are attempted."""

    @pytest.mark.asyncio
    async def test_max_3_dates_attempted(self):
        """Even if 5 dates are missing, only 3 are attempted."""
        db = _build_db(tickers=["AAPL.US"])

        completed = [
            "2026-03-20", "2026-03-21", "2026-03-24",
            "2026-03-25", "2026-03-26",
        ]

        async def _mock_remediate(db_, target_date):
            return {
                "status": "success",
                "gap_tickers_count": 1,
                "total_inserted": 1,
                "proof_table": [],
            }

        with patch(
            "services.market_calendar_service.last_n_completed_trading_days",
            new_callable=AsyncMock,
            return_value=completed,
        ), patch(
            "scheduler_service._get_remediation_processed_dates_set",
            new_callable=AsyncMock,
            return_value=set(),
        ), patch(
            "scheduler_service.remediate_gap_date",
            side_effect=_mock_remediate,
        ):
            result = await scheduler_service.run_bulk_gapfill_remediation(db)

        assert result["days_attempted"] == 3

        # Verify exactly 3 day entries in the audit proof
        run_doc = list(db.ops_job_runs.docs.values())[-1]
        gapfill_days = run_doc["details"]["price_bulk_gapfill"]["days"]
        assert len(gapfill_days) == 3
        # Oldest-first
        assert gapfill_days[0]["processed_date"] == "2026-03-20"
        assert gapfill_days[1]["processed_date"] == "2026-03-21"
        assert gapfill_days[2]["processed_date"] == "2026-03-24"


class TestAuditProofFormat:
    """days[] entries have correct structure: advanced_watermark=false, source, status."""

    @pytest.mark.asyncio
    async def test_day_entry_structure(self):
        """Each day entry has the required fields."""
        db = _build_db(tickers=["AAPL.US"])

        async def _mock_remediate(db_, target_date):
            return {
                "status": "success",
                "gap_tickers_count": 2,
                "total_inserted": 1,
                "proof_table": [
                    {"ticker": "AAPL.US", "inserted": True, "primary_reason": "resolved_from_bulk"},
                    {"ticker": "XYZ.US", "inserted": False, "primary_reason": "not_in_bulk_not_in_api"},
                ],
            }

        with patch(
            "services.market_calendar_service.last_n_completed_trading_days",
            new_callable=AsyncMock,
            return_value=["2026-03-31"],
        ), patch(
            "scheduler_service._get_remediation_processed_dates_set",
            new_callable=AsyncMock,
            return_value=set(),
        ), patch(
            "scheduler_service.remediate_gap_date",
            side_effect=_mock_remediate,
        ):
            result = await scheduler_service.run_bulk_gapfill_remediation(db)

        assert result["status"] == "success"

        # Find the run document
        run_doc = None
        for doc in db.ops_job_runs.docs.values():
            if doc.get("job_name") == "bulk_gapfill_remediation" and doc.get("finished_at"):
                run_doc = doc
                break
        assert run_doc is not None

        # Check finalized fields
        assert run_doc["status"] == "success"
        assert run_doc["finished_at"] is not None
        assert run_doc["finished_at_prague"] is not None
        assert run_doc["started_at_prague"] is not None

        day = run_doc["details"]["price_bulk_gapfill"]["days"][0]
        assert day["processed_date"] == "2026-03-31"
        assert day["status"] == "success"
        assert day["advanced_watermark"] is False
        assert day["source"] == "remediate_gap_date"
        assert day["rows_written"] == 1
        assert day["error"] is None
        assert day["gap_tickers_count"] == 2
        assert day["exclusions_written"] == 1  # XYZ.US not_in_bulk_not_in_api

    @pytest.mark.asyncio
    async def test_job_name_is_bulk_gapfill_remediation(self):
        """The ops_job_runs doc uses job_name=bulk_gapfill_remediation, not price_sync."""
        db = _build_db(tickers=[])

        with patch(
            "services.market_calendar_service.last_n_completed_trading_days",
            new_callable=AsyncMock,
            return_value=[],
        ), patch(
            "scheduler_service._get_remediation_processed_dates_set",
            new_callable=AsyncMock,
            return_value=set(),
        ):
            await scheduler_service.run_bulk_gapfill_remediation(db)

        run_docs = [
            d for d in db.ops_job_runs.docs.values()
            if d.get("job_name") == "bulk_gapfill_remediation"
        ]
        assert len(run_docs) == 1
        assert run_docs[0]["job_name"] == "bulk_gapfill_remediation"

    @pytest.mark.asyncio
    async def test_partial_failure_records_error_entry(self):
        """If one day fails, it gets status=error and overall is error, but other days proceed."""
        db = _build_db(tickers=["AAPL.US"])

        async def _mock_remediate(db_, target_date):
            if target_date == "2026-03-25":
                raise RuntimeError("API timeout")
            return {
                "status": "success",
                "gap_tickers_count": 1,
                "total_inserted": 1,
                "proof_table": [],
            }

        with patch(
            "services.market_calendar_service.last_n_completed_trading_days",
            new_callable=AsyncMock,
            return_value=["2026-03-25", "2026-03-26"],
        ), patch(
            "scheduler_service._get_remediation_processed_dates_set",
            new_callable=AsyncMock,
            return_value=set(),
        ), patch(
            "scheduler_service.remediate_gap_date",
            side_effect=_mock_remediate,
        ):
            result = await scheduler_service.run_bulk_gapfill_remediation(db)

        # The first day should have failed (exception from remediate_gap_date)
        # The second day should have succeeded
        assert result["days_attempted"] == 2
        run_doc = [d for d in db.ops_job_runs.docs.values() if d.get("finished_at")][0]
        assert run_doc["finished_at"] is not None

        days = run_doc["details"]["price_bulk_gapfill"]["days"]
        assert len(days) == 2
        # First day: error from remediate_gap_date exception
        assert days[0]["processed_date"] == "2026-03-25"
        assert days[0]["status"] == "error"
        assert "API timeout" in (days[0]["error"] or "")
        # Second day: should have succeeded
        assert days[1]["processed_date"] == "2026-03-26"
        assert days[1]["status"] == "success"


class TestReaderWidening:
    """Processed dates set includes records from both job names."""

    @pytest.mark.asyncio
    async def test_includes_both_job_names(self):
        """_get_remediation_processed_dates_set picks up both price_sync and remediation."""
        db = _build_db(existing_ops_docs=[
            {
                "job_name": "price_sync",
                "status": "success",
                "finished_at": datetime(2026, 3, 28, 12, 0, tzinfo=timezone.utc),
                "details": {
                    "price_bulk_gapfill": {
                        "days": [
                            {"processed_date": "2026-03-28", "status": "success", "rows_written": 5000},
                        ],
                    },
                },
            },
            {
                "job_name": "bulk_gapfill_remediation",
                "status": "success",
                "finished_at": datetime(2026, 3, 29, 5, 0, tzinfo=timezone.utc),
                "details": {
                    "price_bulk_gapfill": {
                        "days": [
                            {"processed_date": "2026-03-27", "status": "success", "rows_written": 4500},
                        ],
                    },
                },
            },
        ])

        result = await scheduler_service._get_remediation_processed_dates_set(db)
        assert "2026-03-28" in result  # from price_sync
        assert "2026-03-27" in result  # from bulk_gapfill_remediation

    @pytest.mark.asyncio
    async def test_excludes_failed_entries(self):
        """Entries with status != success or rows_written == 0 are excluded."""
        db = _build_db(existing_ops_docs=[
            {
                "job_name": "bulk_gapfill_remediation",
                "status": "success",
                "finished_at": datetime(2026, 3, 29, 5, 0, tzinfo=timezone.utc),
                "details": {
                    "price_bulk_gapfill": {
                        "days": [
                            {"processed_date": "2026-03-25", "status": "error", "rows_written": 0, "error": "fail"},
                            {"processed_date": "2026-03-26", "status": "success", "rows_written": 0},
                            {"processed_date": "2026-03-27", "status": "success", "rows_written": 4500},
                        ],
                    },
                },
            },
        ])

        result = await scheduler_service._get_remediation_processed_dates_set(db)
        assert "2026-03-25" not in result  # error status
        assert "2026-03-26" not in result  # rows_written == 0
        assert "2026-03-27" in result      # success with rows

    @pytest.mark.asyncio
    async def test_reader_query_format_matches_dashboard(self):
        """The $match in _get_remediation_processed_dates_set uses $in for both job names."""
        # This test verifies the source code structure
        import inspect
        source = inspect.getsource(scheduler_service._get_remediation_processed_dates_set)
        assert '"price_sync"' in source
        assert '"bulk_gapfill_remediation"' in source
        assert '"$in"' in source


class TestIdempotency:
    """If a date is already processed, it is not attempted again."""

    @pytest.mark.asyncio
    async def test_already_processed_dates_skipped(self):
        """Dates already in processed_set are not attempted."""
        db = _build_db(tickers=["AAPL.US"])

        completed = ["2026-03-25", "2026-03-26", "2026-03-27"]
        already_processed = {"2026-03-25", "2026-03-27"}

        remediate_calls = []

        async def _mock_remediate(db_, target_date):
            remediate_calls.append(target_date)
            return {
                "status": "success",
                "gap_tickers_count": 1,
                "total_inserted": 1,
                "proof_table": [],
            }

        with patch(
            "services.market_calendar_service.last_n_completed_trading_days",
            new_callable=AsyncMock,
            return_value=completed,
        ), patch(
            "scheduler_service._get_remediation_processed_dates_set",
            new_callable=AsyncMock,
            return_value=already_processed,
        ), patch(
            "scheduler_service.remediate_gap_date",
            side_effect=_mock_remediate,
        ):
            result = await scheduler_service.run_bulk_gapfill_remediation(db)

        # Only 2026-03-26 should have been attempted
        assert result["days_attempted"] == 1
        assert remediate_calls == ["2026-03-26"]

    @pytest.mark.asyncio
    async def test_no_work_when_all_processed(self):
        """If all dates are processed, no remediation work is done and result is success."""
        db = _build_db(tickers=["AAPL.US"])

        with patch(
            "services.market_calendar_service.last_n_completed_trading_days",
            new_callable=AsyncMock,
            return_value=["2026-03-25", "2026-03-26"],
        ), patch(
            "scheduler_service._get_remediation_processed_dates_set",
            new_callable=AsyncMock,
            return_value={"2026-03-25", "2026-03-26"},
        ):
            result = await scheduler_service.run_bulk_gapfill_remediation(db)

        assert result["status"] == "success"
        assert result["days_attempted"] == 0

        # Run doc should still be finalized with finished_at
        run_doc = [d for d in db.ops_job_runs.docs.values() if d.get("finished_at")][0]
        assert run_doc["finished_at"] is not None
        assert run_doc["status"] == "success"


class TestNoWatermarkAdvance:
    """Remediation must NOT write to pipeline_state._id='price_bulk'."""

    @pytest.mark.asyncio
    async def test_pipeline_state_not_touched(self):
        """Verify the source code of run_bulk_gapfill_remediation does not
        call _write_price_bulk_state."""
        import inspect
        source = inspect.getsource(scheduler_service.run_bulk_gapfill_remediation)
        assert "_write_price_bulk_state" not in source


class TestDelegatesToRemediateGapDate:
    """run_bulk_gapfill_remediation delegates to remediate_gap_date, which writes exclusions."""

    @pytest.mark.asyncio
    async def test_source_calls_remediate_gap_date(self):
        """Verify source code delegates to remediate_gap_date."""
        import inspect
        source = inspect.getsource(scheduler_service.run_bulk_gapfill_remediation)
        assert "remediate_gap_date" in source
        # Should NOT contain per-ticker API calls anymore
        assert "fetch_eod_history" not in source

    @pytest.mark.asyncio
    async def test_exclusions_counted_in_day_entry(self):
        """Exclusions from remediate_gap_date appear in the day_entry."""
        db = _build_db(tickers=["AAPL.US"])

        async def _mock_remediate(db_, target_date):
            return {
                "status": "success",
                "gap_tickers_count": 3,
                "total_inserted": 1,
                "proof_table": [
                    {"ticker": "AAPL.US", "inserted": True, "primary_reason": "resolved_from_bulk"},
                    {"ticker": "XYZ.US", "inserted": False, "primary_reason": "not_in_bulk_not_in_api"},
                    {"ticker": "HAL.US", "inserted": False, "primary_reason": "bulk_found_but_close_is_zero"},
                ],
            }

        with patch(
            "services.market_calendar_service.last_n_completed_trading_days",
            new_callable=AsyncMock,
            return_value=["2026-04-06"],
        ), patch(
            "scheduler_service._get_remediation_processed_dates_set",
            new_callable=AsyncMock,
            return_value=set(),
        ), patch(
            "scheduler_service.remediate_gap_date",
            side_effect=_mock_remediate,
        ):
            result = await scheduler_service.run_bulk_gapfill_remediation(db)

        assert result["status"] == "success"

        run_doc = [d for d in db.ops_job_runs.docs.values() if d.get("finished_at")][0]
        day = run_doc["details"]["price_bulk_gapfill"]["days"][0]
        assert day["gap_tickers_count"] == 3
        assert day["exclusions_written"] == 2  # XYZ.US + HAL.US
        assert day["rows_written"] == 1


class TestSchedulerIntegration:
    """Verify scheduler.py has the remediation block."""

    def test_scheduler_has_remediation_block(self):
        """scheduler.py should reference bulk_gapfill_remediation."""
        scheduler_path = os.path.join(
            os.path.dirname(__file__), "..", "scheduler.py"
        )
        with open(scheduler_path, "r") as f:
            source = f.read()

        assert "GAPFILL_REMEDIATION_HOUR" in source
        assert "GAPFILL_REMEDIATION_MINUTE" in source
        assert '"bulk_gapfill_remediation"' in source
        assert "run_bulk_gapfill_remediation" in source

    def test_scheduler_constants_are_05_00(self):
        """Remediation scheduled at 05:00."""
        scheduler_path = os.path.join(
            os.path.dirname(__file__), "..", "scheduler.py"
        )
        with open(scheduler_path, "r") as f:
            source = f.read()

        assert "GAPFILL_REMEDIATION_HOUR = 5" in source
        assert "GAPFILL_REMEDIATION_MINUTE = 0" in source


class TestReaderWideningSourceCode:
    """Verify the three dashboard readers now include bulk_gapfill_remediation."""

    def _read_file(self, relpath):
        full = os.path.join(os.path.dirname(__file__), "..", relpath)
        with open(full, "r") as f:
            return f.read()

    def test_admin_get_bulk_processed_dates_widened(self):
        src = self._read_file("services/admin_overview_service.py")
        # The function _get_bulk_processed_dates should now use $in
        assert '"bulk_gapfill_remediation"' in src
        # Original price_sync must still be present
        assert '"price_sync"' in src

    def test_market_calendar_get_bulk_processed_dates_set_widened(self):
        src = self._read_file("services/market_calendar_service.py")
        assert '"bulk_gapfill_remediation"' in src
        assert '"price_sync"' in src

    def test_admin_get_bounded_bulk_processed_dates_set_widened(self):
        src = self._read_file("services/admin_overview_service.py")
        # Should have at least two occurrences of bulk_gapfill_remediation
        # (one for _get_bulk_processed_dates, one for _get_bounded_bulk_processed_dates_set)
        count = src.count('"bulk_gapfill_remediation"')
        assert count >= 2, f"Expected >=2 occurrences, found {count}"
