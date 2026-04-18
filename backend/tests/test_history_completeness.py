"""
Tests for the canonical history-completeness verifier.

Tests verify:
  1. verify_ticker_history_completeness — per-ticker range-proof
  2. run_history_completeness_sweep — batch ops job
  3. ALLOWED_STATUSES — enum coverage
  4. Edge cases: no proof, no bulk dates, all excluded, mixed
"""

import asyncio
import sys
import os
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

# Backend must be on the path so imports resolve
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from services.history_completeness_service import (
    ALLOWED_STATUSES,
    verify_ticker_history_completeness,
    persist_ticker_completeness,
    run_history_completeness_sweep,
    _result,
)


# ── Helpers ────────────────────────────────────────────────────────────────────

class _Cursor:
    """Minimal async cursor mock that supports find/aggregate patterns."""
    def __init__(self, docs):
        self._docs = list(docs)

    def sort(self, *a, **kw):
        return self

    def limit(self, n):
        self._docs = self._docs[:n]
        return self

    async def to_list(self, length=None):
        if length is not None:
            return self._docs[:length]
        return self._docs

    def __aiter__(self):
        return _CursorIter(self._docs)


class _CursorIter:
    def __init__(self, docs):
        self._iter = iter(docs)

    async def __anext__(self):
        try:
            return next(self._iter)
        except StopIteration:
            raise StopAsyncIteration


def _make_db(
    *,
    tracked_ticker_doc=None,
    price_dates=None,
    first_last=None,
    exclusions=None,
    bulk_processed_dates=None,
    visible_docs=None,
):
    """Build a mock db with the collections needed by the verifier."""
    db = MagicMock()

    # tracked_tickers.find_one
    db.tracked_tickers.find_one = AsyncMock(return_value=tracked_ticker_doc)

    # tracked_tickers.find (for sweep)
    _vis = visible_docs or []
    db.tracked_tickers.find.return_value = _Cursor(_vis)

    # tracked_tickers.update_one
    db.tracked_tickers.update_one = AsyncMock()

    # tracked_tickers.bulk_write
    db.tracked_tickers.bulk_write = AsyncMock()

    # stock_prices.aggregate — returns different results depending on pipeline
    # We chain them: first call = relevant dates, second call = first/last
    _price_dates = price_dates or []
    _first_last = first_last  # {"first_date": "...", "last_date": "..."}

    call_count = {"n": 0}

    def _agg(pipeline):
        call_count["n"] += 1
        # Detect which aggregation this is by inspecting the pipeline
        for stage in pipeline:
            if "$group" in stage:
                group = stage["$group"]
                if "dates" in group:
                    # Per-ticker date aggregation
                    if isinstance(_price_dates, list) and _price_dates and isinstance(_price_dates[0], dict):
                        return _Cursor(_price_dates)
                    # Single ticker: return one doc with dates
                    return _Cursor([{"_id": None, "dates": _price_dates}] if _price_dates else [])
                if "first_date" in group:
                    if isinstance(_first_last, list):
                        return _Cursor(_first_last)
                    if _first_last:
                        return _Cursor([{"_id": None, **_first_last}])
                    return _Cursor([])
        return _Cursor([])

    db.stock_prices.aggregate = _agg

    # gap_free_exclusions.find
    _excls = exclusions or []
    db.gap_free_exclusions.find.return_value = _Cursor(_excls)

    # ops_job_runs — for _get_bulk_processed_dates
    # We mock via the bulk_processed_dates parameter
    _bpd = bulk_processed_dates or []
    db.ops_job_runs.aggregate.return_value = _Cursor([])
    db.ops_job_runs.insert_one = AsyncMock()

    # market_calendar.find
    db.market_calendar = MagicMock()
    db.market_calendar.find.return_value = _Cursor([])

    return db


# ── Test: ALLOWED_STATUSES ─────────────────────────────────────────────────────

class TestAllowedStatuses:
    def test_canonical_statuses_are_strings(self):
        for s in ALLOWED_STATUSES:
            assert isinstance(s, str)

    def test_complete_and_incomplete_present(self):
        assert "complete" in ALLOWED_STATUSES
        assert "incomplete" in ALLOWED_STATUSES

    def test_no_history_download_present(self):
        assert "no_history_download" in ALLOWED_STATUSES

    def test_no_bulk_dates_present(self):
        assert "no_bulk_dates" in ALLOWED_STATUSES


# ── Test: _result helper ───────────────────────────────────────────────────────

class TestResultHelper:
    def test_complete_result(self):
        now = datetime.now(timezone.utc)
        r = _result(status="complete", complete=True, first_date="2020-01-02",
                     last_date="2025-04-17", verified_at=now)
        assert r["price_history_complete"] is True
        assert r["price_history_status"] == "complete"
        assert r["price_history_first_date"] == "2020-01-02"
        assert r["price_history_last_date"] == "2025-04-17"
        assert r["price_history_missing_days_count"] == 0
        assert r["price_history_missing_days"] == []
        assert r["price_history_last_verified_at"] == now

    def test_incomplete_result(self):
        r = _result(status="incomplete", complete=False, missing_days_count=3,
                     missing_days=["2025-04-14", "2025-04-15", "2025-04-16"])
        assert r["price_history_complete"] is False
        assert r["price_history_missing_days_count"] == 3


# ── Test: verify_ticker_history_completeness ───────────────────────────────────

class TestVerifyTicker:
    def test_no_tracked_ticker_doc(self):
        """Ticker not found in tracked_tickers → no_history_download."""
        db = _make_db(tracked_ticker_doc=None)
        result = asyncio.get_event_loop().run_until_complete(
            verify_ticker_history_completeness(db, "GHOST.US", expected_dates=["2025-04-17"])
        )
        assert result["price_history_status"] == "no_history_download"
        assert result["price_history_complete"] is False

    def test_no_proof_marker(self):
        """Ticker exists but has no history_download_proven_at."""
        db = _make_db(tracked_ticker_doc={"ticker": "AAPL.US"})
        result = asyncio.get_event_loop().run_until_complete(
            verify_ticker_history_completeness(db, "AAPL.US", expected_dates=["2025-04-17"])
        )
        assert result["price_history_status"] == "no_history_download"
        assert result["price_history_complete"] is False

    def test_no_anchor(self):
        """Proof marker exists but anchor is None → not complete."""
        db = _make_db(tracked_ticker_doc={
            "history_download_proven_at": datetime(2025, 1, 1, tzinfo=timezone.utc),
            "history_download_proven_anchor": None,
        })
        result = asyncio.get_event_loop().run_until_complete(
            verify_ticker_history_completeness(db, "AAPL.US", expected_dates=["2025-04-17"])
        )
        assert result["price_history_status"] == "no_history_download"

    def test_no_expected_dates(self):
        """No bulk dates available → no_bulk_dates status."""
        db = _make_db(tracked_ticker_doc={
            "history_download_proven_at": datetime(2025, 1, 1, tzinfo=timezone.utc),
            "history_download_proven_anchor": "2025-01-01",
        })
        result = asyncio.get_event_loop().run_until_complete(
            verify_ticker_history_completeness(db, "AAPL.US", expected_dates=[])
        )
        assert result["price_history_status"] == "no_bulk_dates"
        assert result["price_history_complete"] is False

    def test_all_dates_present_complete(self):
        """All relevant dates have price data → complete."""
        expected = ["2025-04-14", "2025-04-15", "2025-04-16", "2025-04-17"]
        db = _make_db(
            tracked_ticker_doc={
                "history_download_proven_at": datetime(2025, 4, 11, tzinfo=timezone.utc),
                "history_download_proven_anchor": "2025-04-11",
            },
            price_dates=expected,
            first_last={"first_date": "2020-01-02", "last_date": "2025-04-17"},
        )
        result = asyncio.get_event_loop().run_until_complete(
            verify_ticker_history_completeness(db, "AAPL.US", expected_dates=expected)
        )
        assert result["price_history_complete"] is True
        assert result["price_history_status"] == "complete"
        assert result["price_history_missing_days_count"] == 0
        assert result["price_history_first_date"] == "2020-01-02"
        assert result["price_history_last_date"] == "2025-04-17"

    def test_missing_days_incomplete(self):
        """Some required dates missing → incomplete."""
        expected = ["2025-04-14", "2025-04-15", "2025-04-16", "2025-04-17"]
        db = _make_db(
            tracked_ticker_doc={
                "history_download_proven_at": datetime(2025, 4, 11, tzinfo=timezone.utc),
                "history_download_proven_anchor": "2025-04-11",
            },
            price_dates=["2025-04-14", "2025-04-17"],  # missing 15 and 16
            first_last={"first_date": "2020-01-02", "last_date": "2025-04-17"},
        )
        result = asyncio.get_event_loop().run_until_complete(
            verify_ticker_history_completeness(db, "AAPL.US", expected_dates=expected)
        )
        assert result["price_history_complete"] is False
        assert result["price_history_status"] == "incomplete"
        assert result["price_history_missing_days_count"] == 2
        assert "2025-04-15" in result["price_history_missing_days"]
        assert "2025-04-16" in result["price_history_missing_days"]

    def test_excluded_days_not_counted_as_missing(self):
        """Days where ticker is absent from bulk are NOT counted as missing."""
        expected = ["2025-04-14", "2025-04-15", "2025-04-16", "2025-04-17"]
        db = _make_db(
            tracked_ticker_doc={
                "history_download_proven_at": datetime(2025, 4, 11, tzinfo=timezone.utc),
                "history_download_proven_anchor": "2025-04-11",
            },
            price_dates=["2025-04-14", "2025-04-17"],
            first_last={"first_date": "2020-01-02", "last_date": "2025-04-17"},
            exclusions=[
                {"ticker": "AAPL.US", "date": "2025-04-15"},
                {"ticker": "AAPL.US", "date": "2025-04-16"},
            ],
        )
        result = asyncio.get_event_loop().run_until_complete(
            verify_ticker_history_completeness(db, "AAPL.US", expected_dates=expected)
        )
        assert result["price_history_complete"] is True
        assert result["price_history_missing_days_count"] == 0

    def test_dates_before_anchor_ignored(self):
        """Dates before the anchor are not in the verification range."""
        expected = ["2025-04-10", "2025-04-11", "2025-04-14", "2025-04-15"]
        db = _make_db(
            tracked_ticker_doc={
                "history_download_proven_at": datetime(2025, 4, 14, tzinfo=timezone.utc),
                "history_download_proven_anchor": "2025-04-14",
            },
            price_dates=["2025-04-15"],
            first_last={"first_date": "2020-01-02", "last_date": "2025-04-15"},
        )
        result = asyncio.get_event_loop().run_until_complete(
            verify_ticker_history_completeness(db, "AAPL.US", expected_dates=expected)
        )
        # Only 2025-04-15 is after anchor; 2025-04-10, 11, 14 are <= anchor
        assert result["price_history_complete"] is True
        assert result["price_history_missing_days_count"] == 0

    def test_mixed_present_excluded_missing(self):
        """Mix of present, excluded, and truly missing dates."""
        expected = ["2025-04-14", "2025-04-15", "2025-04-16", "2025-04-17"]
        db = _make_db(
            tracked_ticker_doc={
                "history_download_proven_at": datetime(2025, 4, 11, tzinfo=timezone.utc),
                "history_download_proven_anchor": "2025-04-11",
            },
            price_dates=["2025-04-14"],  # present
            first_last={"first_date": "2020-01-02", "last_date": "2025-04-14"},
            exclusions=[
                {"ticker": "AAPL.US", "date": "2025-04-15"},  # excluded
            ],
        )
        result = asyncio.get_event_loop().run_until_complete(
            verify_ticker_history_completeness(db, "AAPL.US", expected_dates=expected)
        )
        # 2025-04-16 and 2025-04-17 are truly missing
        assert result["price_history_complete"] is False
        assert result["price_history_missing_days_count"] == 2


# ── Test: persist_ticker_completeness ──────────────────────────────────────────

class TestPersist:
    def test_persist_writes_all_fields(self):
        db = _make_db()
        now = datetime.now(timezone.utc)
        result = _result(
            status="complete",
            complete=True,
            first_date="2020-01-02",
            last_date="2025-04-17",
            verified_at=now,
        )
        asyncio.get_event_loop().run_until_complete(
            persist_ticker_completeness(db, "AAPL.US", result)
        )
        db.tracked_tickers.update_one.assert_called_once()
        call_args = db.tracked_tickers.update_one.call_args
        filt = call_args[0][0]
        update = call_args[0][1]
        assert filt == {"ticker": "AAPL.US"}
        set_fields = update["$set"]
        assert set_fields["price_history_complete"] is True
        assert set_fields["price_history_status"] == "complete"
        assert set_fields["price_history_first_date"] == "2020-01-02"
        assert set_fields["price_history_last_date"] == "2025-04-17"
        assert set_fields["price_history_missing_days_count"] == 0
        assert set_fields["price_history_last_verified_at"] == now


# ── Test: run_history_completeness_sweep ───────────────────────────────────────

class TestSweep:
    def test_no_visible_tickers(self):
        """Empty visible set → no_work."""
        db = _make_db(visible_docs=[])
        result = asyncio.get_event_loop().run_until_complete(
            run_history_completeness_sweep(db)
        )
        assert result["status"] == "no_work"
        assert result["total"] == 0

    def test_sweep_with_complete_ticker(self):
        """One visible ticker with full coverage → complete."""
        now = datetime.now(timezone.utc)
        visible = [
            {
                "ticker": "AAPL.US",
                "history_download_proven_at": now,
                "history_download_proven_anchor": "2025-04-11",
            },
        ]
        expected = ["2025-04-14", "2025-04-15"]

        db = _make_db(visible_docs=visible)

        # Override _get_bulk_processed_dates for sweep
        import services.history_completeness_service as svc
        original = svc._get_bulk_processed_dates

        async def _mock_bpd(db):
            return expected
        svc._get_bulk_processed_dates = _mock_bpd

        # stock_prices aggregation for dates_by_ticker
        call_count = {"n": 0}
        def _mock_agg(pipeline):
            call_count["n"] += 1
            for stage in pipeline:
                if "$group" in stage:
                    group = stage["$group"]
                    if "dates" in group:
                        return _Cursor([{"_id": "AAPL.US", "dates": expected}])
                    if "first_date" in group:
                        return _Cursor([{"_id": "AAPL.US", "first_date": "2020-01-02", "last_date": "2025-04-15"}])
            return _Cursor([])
        db.stock_prices.aggregate = _mock_agg

        try:
            result = asyncio.get_event_loop().run_until_complete(
                run_history_completeness_sweep(db)
            )
            assert result["status"] == "success"
            assert result["total"] == 1
            assert result["complete"] == 1
            assert result["incomplete"] == 0
            # Verify bulk_write was called
            db.tracked_tickers.bulk_write.assert_called_once()
        finally:
            svc._get_bulk_processed_dates = original

    def test_sweep_with_incomplete_ticker(self):
        """One visible ticker with missing days → incomplete."""
        now = datetime.now(timezone.utc)
        visible = [
            {
                "ticker": "MSFT.US",
                "history_download_proven_at": now,
                "history_download_proven_anchor": "2025-04-11",
            },
        ]
        expected = ["2025-04-14", "2025-04-15", "2025-04-16"]

        db = _make_db(visible_docs=visible)

        import services.history_completeness_service as svc
        original = svc._get_bulk_processed_dates

        async def _mock_bpd(db):
            return expected
        svc._get_bulk_processed_dates = _mock_bpd

        def _mock_agg(pipeline):
            for stage in pipeline:
                if "$group" in stage:
                    group = stage["$group"]
                    if "dates" in group:
                        # Only has 2025-04-14 — missing 15 and 16
                        return _Cursor([{"_id": "MSFT.US", "dates": ["2025-04-14"]}])
                    if "first_date" in group:
                        return _Cursor([{"_id": "MSFT.US", "first_date": "2020-01-02", "last_date": "2025-04-14"}])
            return _Cursor([])
        db.stock_prices.aggregate = _mock_agg

        try:
            result = asyncio.get_event_loop().run_until_complete(
                run_history_completeness_sweep(db)
            )
            assert result["status"] == "success"
            assert result["total"] == 1
            assert result["complete"] == 0
            assert result["incomplete"] == 1
        finally:
            svc._get_bulk_processed_dates = original

    def test_sweep_no_proof_ticker(self):
        """Ticker without proof marker → no_proof count."""
        visible = [
            {
                "ticker": "NEW.US",
                # No history_download_proven_at
            },
        ]
        db = _make_db(visible_docs=visible)

        import services.history_completeness_service as svc
        original = svc._get_bulk_processed_dates

        async def _mock_bpd(db):
            return ["2025-04-17"]
        svc._get_bulk_processed_dates = _mock_bpd

        def _mock_agg(pipeline):
            for stage in pipeline:
                if "$group" in stage:
                    group = stage["$group"]
                    if "first_date" in group:
                        return _Cursor([{"_id": "NEW.US", "first_date": None, "last_date": None}])
            return _Cursor([])
        db.stock_prices.aggregate = _mock_agg

        try:
            result = asyncio.get_event_loop().run_until_complete(
                run_history_completeness_sweep(db)
            )
            assert result["no_proof"] == 1
            assert result["complete"] == 0
        finally:
            svc._get_bulk_processed_dates = original
