"""
Tests for Phase C non-destructive write path.

Root cause: _process_price_ticker previously deleted all existing rows
BEFORE fetching replacement data.  If the fetch failed (timeout / 429 /
network error), all historical price data was permanently lost.
Subsequent daily bulk adds produced a broken 2-point chart (GRTUF.US).

These tests prove:
  1) Phase C does NOT delete existing rows before a successful fetch.
  2) A failed fetch after needs_redownload=True preserves existing rows
     and re-flags needs_price_redownload=True for retry.
  3) A successful run cannot reduce row_count or move earliest_date
     forward (ingestion regression guard).
  4) Structured ingestion audit fields are persisted on success.
"""

import asyncio
import sys
import os
from types import SimpleNamespace
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest

# Ensure the backend directory is in the Python path.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


# ---------------------------------------------------------------------------
# Fake DB collections used by _process_price_ticker
# ---------------------------------------------------------------------------

class _FakeAggCursor:
    """Simulates a MongoDB aggregation cursor with to_list support."""
    def __init__(self, results):
        self._results = results

    async def to_list(self, n):
        return self._results[:n] if n else self._results


class _FakeStockPrices:
    def __init__(self):
        self.deleted = []
        self.written = []
        self._agg_results = []  # List of agg results — popped in order

    async def delete_many(self, filt):
        self.deleted.append(filt)
        return SimpleNamespace(deleted_count=0)

    async def bulk_write(self, ops, ordered=False):
        self.written.extend(ops)
        return SimpleNamespace(upserted_count=len(ops), modified_count=0)

    def aggregate(self, pipeline):
        # Return next queued agg result
        if self._agg_results:
            return _FakeAggCursor(self._agg_results.pop(0))
        return _FakeAggCursor([])


class _FakeTrackedTickers:
    def __init__(self):
        self.updates = []

    async def update_one(self, filt, update):
        self.updates.append({"filter": filt, "update": update})
        return SimpleNamespace(modified_count=1)


class _FakeApiCreditsLog:
    async def insert_one(self, doc):
        return SimpleNamespace(inserted_id="fake_id")


class _FakeDB:
    def __init__(self):
        self.stock_prices = _FakeStockPrices()
        self.tracked_tickers = _FakeTrackedTickers()
        self.api_credits_log = _FakeApiCreditsLog()
        self.credit_logs = _FakeApiCreditsLog()


def _make_eod_records_range(first_date, last_date, freq_days=1):
    """Helper: generate EOD records spanning first_date..last_date."""
    from datetime import timedelta
    start = datetime.strptime(first_date, "%Y-%m-%d")
    end = datetime.strptime(last_date, "%Y-%m-%d")
    records = []
    d = start
    while d <= end:
        records.append({
            "date": d.strftime("%Y-%m-%d"),
            "open": 10.0, "high": 11.0, "low": 9.0,
            "close": 10.0, "adjusted_close": 10.0, "volume": 100,
        })
        d += timedelta(days=freq_days)
    return records


# ===========================================================================
# TEST 1: Non-destructive ordering — fetch BEFORE delete
# ===========================================================================

class TestFetchBeforeDelete:
    """Phase C must fetch data BEFORE deleting existing rows."""

    def test_successful_redownload_deletes_after_fetch_succeeds(self):
        """When needs_redownload=True and fetch succeeds, delete happens
        AFTER the fetch, not before.  Then bulk_write inserts new data."""
        from full_sync_service import _process_price_ticker

        db = _FakeDB()
        records = _make_eod_records_range("2003-09-02", "2026-04-16", freq_days=7)

        # Queue TWO agg results: first for pre-write state, second for post-write state
        db.stock_prices._agg_results = [
            # Pre-write: DB has 2 legacy rows
            [{"_id": None, "count": 2, "earliest": "2026-04-15", "latest": "2026-04-16"}],
            # Post-write: DB now has full history
            [{"_id": None, "db_first_date": "2003-09-02", "db_last_date": "2026-04-16", "db_row_count": len(records)}],
        ]

        with patch("full_sync_service._fetch_one", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = (records, 200, 50, True)
            result = asyncio.run(
                _process_price_ticker(db, "GRTUF.US", job_name="test", needs_redownload=True)
            )

        assert result["success"] is True
        assert result["records"] == len(records)

        # Verify delete happened (since needs_redownload=True)
        assert len(db.stock_prices.deleted) == 1
        # Verify bulk_write also happened
        assert len(db.stock_prices.written) > 0

    def test_failed_fetch_with_redownload_preserves_existing_rows(self):
        """When needs_redownload=True but fetch FAILS, existing rows must
        NOT be deleted.  This is the exact GRTUF.US root cause fix."""
        from full_sync_service import _process_price_ticker

        db = _FakeDB()
        # Pre-write state: DB has 5000 rows of history
        db.stock_prices._agg_results = [
            [{"_id": None, "count": 5000, "earliest": "2003-09-02", "latest": "2026-04-16"}],
        ]

        with patch("full_sync_service._fetch_one", new_callable=AsyncMock) as mock_fetch:
            # Fetch fails (timeout / network error)
            mock_fetch.return_value = (None, 0, 60000, False)
            result = asyncio.run(
                _process_price_ticker(db, "GRTUF.US", job_name="test", needs_redownload=True)
            )

        assert result["success"] is False
        # CRITICAL: NO delete_many was called — existing rows preserved
        assert len(db.stock_prices.deleted) == 0, (
            "Failed fetch must NOT delete existing rows — "
            f"but {len(db.stock_prices.deleted)} delete(s) were issued"
        )
        # No writes either
        assert len(db.stock_prices.written) == 0

    def test_failed_fetch_with_redownload_reflags_for_retry(self):
        """When needs_redownload=True and fetch fails, the ticker must be
        re-flagged with needs_price_redownload=True so Phase C retries."""
        from full_sync_service import _process_price_ticker

        db = _FakeDB()
        db.stock_prices._agg_results = [
            [{"_id": None, "count": 5000, "earliest": "2003-09-02", "latest": "2026-04-16"}],
        ]

        with patch("full_sync_service._fetch_one", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = (None, 429, 500, False)
            result = asyncio.run(
                _process_price_ticker(db, "GRTUF.US", job_name="test", needs_redownload=True)
            )

        assert result["success"] is False
        assert result["rate_limited"] is True

        # Find the tracked_tickers update
        assert len(db.tracked_tickers.updates) >= 1
        last_update = db.tracked_tickers.updates[-1]
        set_fields = last_update["update"]["$set"]
        assert set_fields["needs_price_redownload"] is True, (
            "Failed fetch must re-flag needs_price_redownload=True"
        )
        assert set_fields["price_history_complete"] is False

    def test_normal_fetch_without_redownload_does_not_delete(self):
        """When needs_redownload=False, no delete happens regardless."""
        from full_sync_service import _process_price_ticker

        db = _FakeDB()
        records = _make_eod_records_range("2003-09-02", "2026-04-16", freq_days=7)
        db.stock_prices._agg_results = [
            [{"_id": None, "count": 0, "earliest": None, "latest": None}],
            [{"_id": None, "db_first_date": "2003-09-02", "db_last_date": "2026-04-16", "db_row_count": len(records)}],
        ]

        with patch("full_sync_service._fetch_one", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = (records, 200, 50, True)
            result = asyncio.run(
                _process_price_ticker(db, "GRTUF.US", job_name="test", needs_redownload=False)
            )

        assert result["success"] is True
        assert len(db.stock_prices.deleted) == 0


# ===========================================================================
# TEST 2: Ingestion regression guard
# ===========================================================================

class TestIngestionRegressionGuard:
    """A successful Phase C run cannot reduce row_count or move
    earliest_date forward."""

    def test_row_count_regression_detected(self):
        """If post-write DB has fewer rows than pre-write, the run must
        fail and re-flag for retry."""
        from full_sync_service import _process_price_ticker

        db = _FakeDB()
        records = _make_eod_records_range("2026-04-15", "2026-04-16")
        db.stock_prices._agg_results = [
            # Pre-write: DB had 5000 rows
            [{"_id": None, "count": 5000, "earliest": "2003-09-02", "latest": "2026-04-16"}],
            # Post-write: DB now has only 2 rows (regression!)
            [{"_id": None, "db_first_date": "2026-04-15", "db_last_date": "2026-04-16", "db_row_count": 2}],
        ]

        with patch("full_sync_service._fetch_one", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = (records, 200, 50, True)
            result = asyncio.run(
                _process_price_ticker(db, "GRTUF.US", job_name="test", needs_redownload=True)
            )

        assert result["success"] is False
        assert result.get("ingestion_regression") is True

        # Must re-flag for retry
        last_update = db.tracked_tickers.updates[-1]
        set_fields = last_update["update"]["$set"]
        assert set_fields["needs_price_redownload"] is True
        assert set_fields["price_history_complete"] is False
        assert set_fields["price_history_status"] == "ingestion_regression"

    def test_earliest_date_regression_detected(self):
        """If post-write earliest_date is later than pre-write (history
        truncated), the run must fail and re-flag."""
        from full_sync_service import _process_price_ticker

        db = _FakeDB()
        records = _make_eod_records_range("2025-01-01", "2026-04-16", freq_days=7)
        db.stock_prices._agg_results = [
            # Pre-write: DB started from 2003
            [{"_id": None, "count": 5000, "earliest": "2003-09-02", "latest": "2026-04-16"}],
            # Post-write: earliest moved forward to 2025 (regression!)
            [{"_id": None, "db_first_date": "2025-01-01", "db_last_date": "2026-04-16", "db_row_count": 5100}],
        ]

        with patch("full_sync_service._fetch_one", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = (records, 200, 50, True)
            result = asyncio.run(
                _process_price_ticker(db, "TEST.US", job_name="test", needs_redownload=True)
            )

        assert result["success"] is False
        assert result.get("ingestion_regression") is True

    def test_no_regression_when_row_count_increases(self):
        """Normal case: pre=0, post=5000 → no regression."""
        from full_sync_service import _process_price_ticker

        db = _FakeDB()
        records = _make_eod_records_range("2003-09-02", "2026-04-16", freq_days=7)
        db.stock_prices._agg_results = [
            # Pre-write: empty DB
            [{"_id": None, "count": 0, "earliest": None, "latest": None}],
            # Post-write: full history
            [{"_id": None, "db_first_date": "2003-09-02", "db_last_date": "2026-04-16", "db_row_count": len(records)}],
        ]

        with patch("full_sync_service._fetch_one", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = (records, 200, 50, True)
            result = asyncio.run(
                _process_price_ticker(db, "GRTUF.US", job_name="test", needs_redownload=False)
            )

        assert result["success"] is True
        assert result.get("ingestion_regression") is not True


# ===========================================================================
# TEST 3: Ingestion audit trail persisted
# ===========================================================================

class TestIngestionAuditTrail:
    """Structured ingestion audit fields are persisted on successful writes."""

    def test_audit_fields_persisted_on_success(self):
        """After a successful Phase C run, the tracked_tickers document
        must contain ingestion_audit with pre/post DB counts."""
        from full_sync_service import _process_price_ticker

        db = _FakeDB()
        records = _make_eod_records_range("2003-09-02", "2026-04-16", freq_days=7)
        db.stock_prices._agg_results = [
            [{"_id": None, "count": 2, "earliest": "2026-04-15", "latest": "2026-04-16"}],
            [{"_id": None, "db_first_date": "2003-09-02", "db_last_date": "2026-04-16", "db_row_count": len(records)}],
        ]

        with patch("full_sync_service._fetch_one", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = (records, 200, 50, True)
            result = asyncio.run(
                _process_price_ticker(db, "GRTUF.US", job_name="test", needs_redownload=True)
            )

        assert result["success"] is True

        # Find the final tracked_tickers update
        last_update = db.tracked_tickers.updates[-1]
        set_fields = last_update["update"]["$set"]

        # Must contain ingestion_audit
        audit = set_fields.get("ingestion_audit")
        assert audit is not None, "ingestion_audit field must be persisted"
        assert audit["fetched_count"] == len(records)
        assert audit["pre_db_count"] == 2
        assert audit["pre_db_earliest"] == "2026-04-15"
        assert audit["post_db_count"] == len(records)
        assert audit["post_db_earliest"] == "2003-09-02"
        assert audit["needs_redownload"] is True

    def test_audit_fields_persisted_on_range_proof_fail(self):
        """Even when range-proof fails, ingestion_audit is persisted."""
        from full_sync_service import _process_price_ticker

        db = _FakeDB()
        records = _make_eod_records_range("1984-11-07", "2026-04-08", freq_days=7)
        db.stock_prices._agg_results = [
            [{"_id": None, "count": 0, "earliest": None, "latest": None}],
            # Post-write: only 2 rows landed (truncated)
            [{"_id": None, "db_first_date": "2026-04-07", "db_last_date": "2026-04-08", "db_row_count": 2}],
        ]

        with patch("full_sync_service._fetch_one", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = (records, 200, 50, True)
            result = asyncio.run(
                _process_price_ticker(db, "BDL.US", job_name="test", needs_redownload=False)
            )

        assert result["success"] is False
        assert result.get("range_proof_failed") is True

        # Must still have ingestion_audit
        last_update = db.tracked_tickers.updates[-1]
        set_fields = last_update["update"]["$set"]
        audit = set_fields.get("ingestion_audit")
        assert audit is not None
        assert audit["fetched_count"] == len(records)
        assert audit["post_db_count"] == 2


# ===========================================================================
# TEST 4: Unique index is (ticker, date), not just ticker
# ===========================================================================

class TestUpsertKeyIsTickerDate:
    """The upsert key in bulk_write must be (ticker, date), ensuring
    multiple dates per ticker are stored as separate documents."""

    def test_upsert_filter_is_ticker_and_date(self):
        """Each UpdateOne in the bulk_write must filter on both ticker
        AND date, not just ticker (which would overwrite all rows)."""
        from full_sync_service import _process_price_ticker

        db = _FakeDB()
        records = _make_eod_records_range("2026-04-14", "2026-04-16")
        db.stock_prices._agg_results = [
            [{"_id": None, "count": 0, "earliest": None, "latest": None}],
            [{"_id": None, "db_first_date": "2026-04-14", "db_last_date": "2026-04-16", "db_row_count": 3}],
        ]

        with patch("full_sync_service._fetch_one", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = (records, 200, 50, True)
            asyncio.run(
                _process_price_ticker(db, "TEST.US", job_name="test", needs_redownload=False)
            )

        # Inspect the UpdateOne operations
        assert len(db.stock_prices.written) == 3
        # Each op is a pymongo.UpdateOne — extract the filter
        for op in db.stock_prices.written:
            filt = op._filter
            assert "ticker" in filt, "UpdateOne filter must include 'ticker'"
            assert "date" in filt, "UpdateOne filter must include 'date'"
            assert filt["ticker"] == "TEST.US"
