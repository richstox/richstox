"""
Tests for history data-loss detection.

Root cause (BODI bug): Phase C downloaded 1312 rows on 2026-04-10 and set
range_proof, history_download_proven_at/anchor, history_download_records.
Later, visibility cleanup deleted those rows but did NOT clear the
proof fields.  The completeness sweep then trusted the stale anchor and
only checked post-anchor dates → falsely declared "complete" while
stock_prices contained only 4 recent bulk rows.

Fix:
  1. _detect_data_loss() cross-checks live first_date vs stored
     range_proof.db_first_date.  If gap > 30 days, returns True.
  2. Sweep detects data-loss and clears stale proof fields + flags
     needs_price_redownload=True so Phase C re-downloads full history.
  3. Cleanup code now clears all proof fields when deleting stock_prices.

These tests prove:
  1. _detect_data_loss detects BODI-like scenario (5-year gap)
  2. _detect_data_loss ignores small gaps (< 30 days)
  3. _detect_data_loss handles None / missing fields gracefully
  4. verify_ticker_history_completeness returns history_data_lost
  5. run_history_completeness_sweep clears stale proof fields on data loss
  6. history_data_lost is in ALLOWED_STATUSES
  7. STALE_PROOF_RESET_FIELDS constant covers all required fields
  8. Cleanup code clears proof fields (visibility_rules + full_sync_service)
"""

import asyncio
import sys
import os
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from services.history_completeness_service import (
    ALLOWED_STATUSES,
    STALE_PROOF_RESET_FIELDS,
    _DATA_LOSS_THRESHOLD_DAYS,
    _detect_data_loss,
    _result,
    verify_ticker_history_completeness,
    run_history_completeness_sweep,
)


# ── Helpers ────────────────────────────────────────────────────────────────────

class _Cursor:
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
    """Build a mock db for the verifier / sweep."""
    db = MagicMock()

    db.tracked_tickers.find_one = AsyncMock(return_value=tracked_ticker_doc)

    _vis = visible_docs or []
    db.tracked_tickers.find.return_value = _Cursor(_vis)
    db.tracked_tickers.update_one = AsyncMock()
    db.tracked_tickers.bulk_write = AsyncMock()

    _price_dates = price_dates or []
    _first_last = first_last

    def _agg(pipeline):
        for stage in pipeline:
            if "$group" in stage:
                group = stage["$group"]
                if "dates" in group:
                    if isinstance(_price_dates, list) and _price_dates and isinstance(_price_dates[0], dict):
                        return _Cursor(_price_dates)
                    return _Cursor([{"_id": None, "dates": _price_dates}] if _price_dates else [])
                if "first_date" in group:
                    if isinstance(_first_last, list):
                        return _Cursor(_first_last)
                    if _first_last:
                        return _Cursor([{"_id": None, **_first_last}])
                    return _Cursor([])
        return _Cursor([])

    db.stock_prices.aggregate = _agg
    db.gap_free_exclusions.find.return_value = _Cursor(exclusions or [])
    db.ops_job_runs.aggregate.return_value = _Cursor([])
    db.ops_job_runs.insert_one = AsyncMock()
    db.market_calendar = MagicMock()
    db.market_calendar.find.return_value = _Cursor([])

    return db


# ── Test: _detect_data_loss helper ─────────────────────────────────────────────

class TestDetectDataLoss:
    """Unit tests for _detect_data_loss()."""

    def test_bodi_scenario_5_year_gap(self):
        """BODI: proof says 2021-01-15, live DB says 2026-04-13 → True."""
        rp = {"db_first_date": "2021-01-15", "db_last_date": "2026-04-09"}
        assert _detect_data_loss(rp, "2026-04-13") is True

    def test_no_gap_same_date(self):
        """No gap: proof and live match → False."""
        rp = {"db_first_date": "2020-01-02", "db_last_date": "2026-04-17"}
        assert _detect_data_loss(rp, "2020-01-02") is False

    def test_small_gap_within_threshold(self):
        """Gap of 10 days (< 30 threshold) → False."""
        rp = {"db_first_date": "2026-01-01", "db_last_date": "2026-04-17"}
        assert _detect_data_loss(rp, "2026-01-11") is False

    def test_gap_at_threshold_boundary(self):
        """Gap exactly at threshold (30 days) → False (> not >=)."""
        rp = {"db_first_date": "2026-01-01", "db_last_date": "2026-04-17"}
        assert _detect_data_loss(rp, "2026-01-31") is False

    def test_gap_just_over_threshold(self):
        """Gap of 31 days → True."""
        rp = {"db_first_date": "2026-01-01", "db_last_date": "2026-04-17"}
        assert _detect_data_loss(rp, "2026-02-01") is True

    def test_none_range_proof(self):
        """No range_proof → False (nothing to compare)."""
        assert _detect_data_loss(None, "2026-04-13") is False

    def test_none_live_first(self):
        """No live first_date (empty stock_prices) → False."""
        rp = {"db_first_date": "2021-01-15"}
        assert _detect_data_loss(rp, None) is False

    def test_missing_db_first_in_proof(self):
        """range_proof without db_first_date → False."""
        rp = {"db_last_date": "2026-04-09"}
        assert _detect_data_loss(rp, "2026-04-13") is False

    def test_empty_range_proof(self):
        """Empty range_proof dict → False."""
        assert _detect_data_loss({}, "2026-04-13") is False

    def test_malformed_dates(self):
        """Invalid date strings → False (no crash)."""
        rp = {"db_first_date": "not-a-date"}
        assert _detect_data_loss(rp, "2026-04-13") is False


# ── Test: ALLOWED_STATUSES ─────────────────────────────────────────────────────

class TestAllowedStatuses:
    def test_history_data_lost_in_allowed(self):
        """New status is registered."""
        assert "history_data_lost" in ALLOWED_STATUSES


# ── Test: STALE_PROOF_RESET_FIELDS constant ────────────────────────────────────

class TestStaleProofResetFields:
    def test_contains_required_fields(self):
        """All Phase-C proof fields are included."""
        required = {
            "range_proof", "history_download_records",
            "history_download_proven_at", "history_download_proven_anchor",
            "full_history_downloaded_at", "full_history_source",
            "full_history_version", "history_download_completed",
            "gap_free_since_history_download", "price_history_complete_as_of",
            "needs_price_redownload",
        }
        assert required.issubset(STALE_PROOF_RESET_FIELDS.keys())

    def test_needs_price_redownload_is_true(self):
        """needs_price_redownload must be True to trigger Phase C."""
        assert STALE_PROOF_RESET_FIELDS["needs_price_redownload"] is True

    def test_proof_fields_are_none(self):
        """Proof fields should be cleared to None."""
        assert STALE_PROOF_RESET_FIELDS["range_proof"] is None
        assert STALE_PROOF_RESET_FIELDS["history_download_proven_at"] is None
        assert STALE_PROOF_RESET_FIELDS["history_download_proven_anchor"] is None


# ── Test: verify_ticker_history_completeness with data loss ────────────────────

class TestVerifyTickerDataLoss:
    """verify_ticker_history_completeness must detect data loss."""

    def test_bodi_scenario_returns_history_data_lost(self):
        """BODI: proof says 2021-01-15, live DB has only recent rows."""
        expected = ["2026-04-14", "2026-04-15", "2026-04-16", "2026-04-17"]
        db = _make_db(
            tracked_ticker_doc={
                "history_download_proven_at": datetime(2026, 4, 10, tzinfo=timezone.utc),
                "history_download_proven_anchor": "2026-04-09",
                "range_proof": {
                    "db_first_date": "2021-01-15",
                    "db_last_date": "2026-04-09",
                    "db_row_count": 1312,
                    "pass": True,
                },
            },
            price_dates=expected,
            first_last={"first_date": "2026-04-13", "last_date": "2026-04-17"},
        )
        result = asyncio.get_event_loop().run_until_complete(
            verify_ticker_history_completeness(db, "BODI.US", expected_dates=expected)
        )
        assert result["price_history_complete"] is False
        assert result["price_history_status"] == "history_data_lost"
        assert result["price_history_first_date"] == "2026-04-13"
        assert result["price_history_last_date"] == "2026-04-17"

    def test_healthy_ticker_not_flagged(self):
        """Ticker with consistent proof + live data → complete (not data_lost)."""
        expected = ["2026-04-14", "2026-04-15", "2026-04-16", "2026-04-17"]
        db = _make_db(
            tracked_ticker_doc={
                "history_download_proven_at": datetime(2026, 4, 10, tzinfo=timezone.utc),
                "history_download_proven_anchor": "2026-04-09",
                "range_proof": {
                    "db_first_date": "2020-01-02",
                    "db_last_date": "2026-04-09",
                    "db_row_count": 1500,
                    "pass": True,
                },
            },
            price_dates=expected,
            first_last={"first_date": "2020-01-02", "last_date": "2026-04-17"},
        )
        result = asyncio.get_event_loop().run_until_complete(
            verify_ticker_history_completeness(db, "AAPL.US", expected_dates=expected)
        )
        assert result["price_history_complete"] is True
        assert result["price_history_status"] == "complete"

    def test_no_range_proof_not_data_lost(self):
        """Ticker with proof but no range_proof → not data_lost (treated normally)."""
        expected = ["2026-04-14", "2026-04-15", "2026-04-16", "2026-04-17"]
        db = _make_db(
            tracked_ticker_doc={
                "history_download_proven_at": datetime(2026, 4, 10, tzinfo=timezone.utc),
                "history_download_proven_anchor": "2026-04-09",
                # no range_proof field
            },
            price_dates=expected,
            first_last={"first_date": "2026-04-13", "last_date": "2026-04-17"},
        )
        result = asyncio.get_event_loop().run_until_complete(
            verify_ticker_history_completeness(db, "BODI.US", expected_dates=expected)
        )
        # Without range_proof, can't detect data loss — treated as normal
        assert result["price_history_status"] != "history_data_lost"


# ── Test: run_history_completeness_sweep with data loss ────────────────────────

class TestSweepDataLoss:
    """Sweep must detect data loss and clear stale proof fields."""

    def test_sweep_detects_bodi_data_loss(self):
        """Sweep marks BODI as history_data_lost and clears proof fields."""
        # BODI: visible, has stale proof
        visible_docs = [{
            "ticker": "BODI.US",
            "history_download_proven_at": datetime(2026, 4, 10, tzinfo=timezone.utc),
            "history_download_proven_anchor": "2026-04-09",
            "range_proof": {
                "db_first_date": "2021-01-15",
                "db_last_date": "2026-04-09",
                "db_row_count": 1312,
                "pass": True,
            },
        }]
        expected_dates = ["2026-04-14", "2026-04-15", "2026-04-16", "2026-04-17"]

        db = _make_db(
            visible_docs=visible_docs,
            # sweep pre-loads dates and first/last by ticker
            price_dates=[{"_id": "BODI.US", "dates": expected_dates}],
            first_last=[{"_id": "BODI.US", "first_date": "2026-04-13", "last_date": "2026-04-17"}],
            bulk_processed_dates=expected_dates,
        )

        # Mock _get_bulk_processed_dates
        with patch(
            "services.history_completeness_service._get_bulk_processed_dates",
            new=AsyncMock(return_value=expected_dates),
        ):
            result = asyncio.get_event_loop().run_until_complete(
                run_history_completeness_sweep(db)
            )

        assert result["data_lost"] == 1
        assert result["complete"] == 0

        # Verify bulk_write was called with STALE_PROOF_RESET_FIELDS
        db.tracked_tickers.bulk_write.assert_called_once()
        ops = db.tracked_tickers.bulk_write.call_args[0][0]
        assert len(ops) == 1

        # Extract the $set fields from the UpdateOne op
        op = ops[0]
        set_fields = op._doc["$set"]
        assert set_fields["price_history_complete"] is False
        assert set_fields["price_history_status"] == "history_data_lost"
        assert set_fields["needs_price_redownload"] is True
        assert set_fields["range_proof"] is None
        assert set_fields["history_download_proven_at"] is None
        assert set_fields["history_download_proven_anchor"] is None
        assert set_fields["history_download_records"] is None

    def test_sweep_healthy_ticker_not_flagged(self):
        """Healthy ticker with consistent proof is not marked as data_lost."""
        visible_docs = [{
            "ticker": "AAPL.US",
            "history_download_proven_at": datetime(2026, 4, 10, tzinfo=timezone.utc),
            "history_download_proven_anchor": "2026-04-09",
            "range_proof": {
                "db_first_date": "1990-01-02",
                "db_last_date": "2026-04-09",
                "db_row_count": 8000,
                "pass": True,
            },
        }]
        expected_dates = ["2026-04-14", "2026-04-15", "2026-04-16", "2026-04-17"]

        db = _make_db(
            visible_docs=visible_docs,
            price_dates=[{"_id": "AAPL.US", "dates": expected_dates}],
            first_last=[{"_id": "AAPL.US", "first_date": "1990-01-02", "last_date": "2026-04-17"}],
            bulk_processed_dates=expected_dates,
        )

        with patch(
            "services.history_completeness_service._get_bulk_processed_dates",
            new=AsyncMock(return_value=expected_dates),
        ):
            result = asyncio.get_event_loop().run_until_complete(
                run_history_completeness_sweep(db)
            )

        assert result["data_lost"] == 0
        assert result["complete"] == 1

        # Verify the op does NOT contain stale-proof reset fields
        ops = db.tracked_tickers.bulk_write.call_args[0][0]
        op = ops[0]
        set_fields = op._doc["$set"]
        assert "range_proof" not in set_fields
        assert set_fields["price_history_complete"] is True
        assert set_fields["price_history_status"] == "complete"

    def test_sweep_mixed_tickers(self):
        """Sweep handles a mix of data-lost and healthy tickers."""
        visible_docs = [
            {
                "ticker": "BODI.US",
                "history_download_proven_at": datetime(2026, 4, 10, tzinfo=timezone.utc),
                "history_download_proven_anchor": "2026-04-09",
                "range_proof": {
                    "db_first_date": "2021-01-15",
                    "db_last_date": "2026-04-09",
                    "db_row_count": 1312,
                    "pass": True,
                },
            },
            {
                "ticker": "AAPL.US",
                "history_download_proven_at": datetime(2026, 4, 10, tzinfo=timezone.utc),
                "history_download_proven_anchor": "2026-04-09",
                "range_proof": {
                    "db_first_date": "1990-01-02",
                    "db_last_date": "2026-04-09",
                    "db_row_count": 8000,
                    "pass": True,
                },
            },
        ]
        expected_dates = ["2026-04-14", "2026-04-15", "2026-04-16", "2026-04-17"]

        db = _make_db(
            visible_docs=visible_docs,
            price_dates=[
                {"_id": "BODI.US", "dates": expected_dates},
                {"_id": "AAPL.US", "dates": expected_dates},
            ],
            first_last=[
                {"_id": "BODI.US", "first_date": "2026-04-13", "last_date": "2026-04-17"},
                {"_id": "AAPL.US", "first_date": "1990-01-02", "last_date": "2026-04-17"},
            ],
            bulk_processed_dates=expected_dates,
        )

        with patch(
            "services.history_completeness_service._get_bulk_processed_dates",
            new=AsyncMock(return_value=expected_dates),
        ):
            result = asyncio.get_event_loop().run_until_complete(
                run_history_completeness_sweep(db)
            )

        assert result["data_lost"] == 1
        assert result["complete"] == 1
        assert result["total"] == 2


# ── Test: cleanup code clears proof fields ─────────────────────────────────────

class TestCleanupClearsProofFields:
    """Cleanup must now clear stale proof fields in addition to completeness flags."""

    @pytest.mark.asyncio
    async def test_cleanup_invisible_clears_proof_fields(self):
        """cleanup_invisible_ticker_data clears range_proof + proof fields."""
        from full_sync_service import cleanup_invisible_ticker_data

        class _FakeCollection:
            def __init__(self, docs=None):
                self._docs = list(docs or [])
                self.updates = []
                self.deletes = []

            async def find_one(self, f, p=None):
                return self._docs[0] if self._docs else None

            def find(self, f=None, p=None):
                return _FakeCursorSimple(self._docs)

            async def count_documents(self, f=None):
                return len(self._docs)

            async def distinct(self, field, f=None):
                return [d.get(field) for d in self._docs if d.get(field)]

            async def update_many(self, filt, update):
                self.updates.append({"filter": filt, "update": update})
                return SimpleNamespace(modified_count=1)

            async def delete_many(self, filt):
                self.deletes.append(filt)
                return SimpleNamespace(deleted_count=100)

            async def insert_one(self, doc):
                return SimpleNamespace(inserted_id="fake_id")

            async def aggregate(self, pipeline):
                return _FakeCursorSimple([])

            def bulk_write(self, ops, ordered=False):
                return _FakeAwaitable(SimpleNamespace(modified_count=len(ops)))

        class _FakeAwaitable:
            def __init__(self, result):
                self._result = result
            def __await__(self):
                return iter([self._result])

        class _FakeCursorSimple:
            def __init__(self, docs):
                self._docs = list(docs)
            def sort(self, *a, **kw):
                return self
            async def to_list(self, n=None):
                return self._docs[:n] if n else self._docs
            def __aiter__(self):
                self._iter = iter(self._docs)
                return self
            async def __anext__(self):
                try:
                    return next(self._iter)
                except StopIteration:
                    raise StopAsyncIteration

        tracked = _FakeCollection([
            {"ticker": "BODI.US", "is_visible": False,
             "price_history_complete": True,
             "range_proof": {"db_first_date": "2021-01-15"},
             "history_download_proven_at": datetime(2026, 4, 10, tzinfo=timezone.utc)},
        ])
        stock_prices = _FakeCollection()

        class FakeDB:
            def __getitem__(self, name):
                return {
                    "stock_prices": stock_prices,
                    "company_fundamentals_cache": _FakeCollection(),
                    "company_financials": _FakeCollection(),
                    "company_earnings_history": _FakeCollection(),
                    "insider_activity": _FakeCollection(),
                }.get(name, _FakeCollection())

        db = FakeDB()
        db.tracked_tickers = tracked
        db.stock_prices = stock_prices

        with patch("benchmark_service.BENCHMARK_SYMBOLS", {}):
            await cleanup_invisible_ticker_data(db)

        # Verify proof fields were cleared
        assert len(tracked.updates) > 0
        last_update = tracked.updates[-1]
        set_fields = last_update["update"]["$set"]
        assert set_fields["range_proof"] is None
        assert set_fields["history_download_proven_at"] is None
        assert set_fields["history_download_proven_anchor"] is None
        assert set_fields["history_download_records"] is None
        assert set_fields["needs_price_redownload"] is True
        assert set_fields["price_history_complete"] is False
