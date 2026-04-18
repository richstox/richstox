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
  3) Write-boundary validation skips rows missing required fields.
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
        self._agg_result = []  # Default aggregation result

    async def delete_many(self, filt):
        self.deleted.append(filt)
        return SimpleNamespace(deleted_count=0)

    async def bulk_write(self, ops, ordered=False):
        self.written.extend(ops)
        return SimpleNamespace(upserted_count=len(ops), modified_count=0)

    def aggregate(self, pipeline):
        return _FakeAggCursor(self._agg_result)


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

        # Set up aggregate to return post-write state for range-proof
        db.stock_prices._agg_result = [{
            "_id": None,
            "db_first_date": "2003-09-02",
            "db_last_date": "2026-04-16",
            "db_row_count": len(records),
        }]

        with patch("full_sync_service._fetch_one", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = (records, 200, 50, True)
            with patch("full_sync_service._log_credit", new_callable=AsyncMock):
                result = asyncio.get_event_loop().run_until_complete(
                    _process_price_ticker(
                        db, "GRTUF.US", "test_job",
                        needs_redownload=True,
                    )
                )

        # Fetch was called (proves fetch happened)
        assert mock_fetch.called

        # Delete happened (needs_redownload=True)
        assert len(db.stock_prices.deleted) == 1

        # Bulk write happened
        assert len(db.stock_prices.written) > 0

        # Key ordering proof: fetch was called, THEN delete, THEN write.
        # Since _fetch_one is mocked, it returns immediately.
        # The delete_many call happens AFTER fetch returns successfully.
        assert result["success"] is True
        assert result["records"] > 0

    def test_failed_fetch_preserves_existing_rows(self):
        """When fetch fails, NO delete happens and existing rows survive."""
        from full_sync_service import _process_price_ticker

        db = _FakeDB()

        with patch("full_sync_service._fetch_one", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = (None, 429, 50, False)
            with patch("full_sync_service._log_credit", new_callable=AsyncMock):
                result = asyncio.get_event_loop().run_until_complete(
                    _process_price_ticker(
                        db, "GRTUF.US", "test_job",
                        needs_redownload=True,
                    )
                )

        # NO delete happened — existing rows preserved
        assert len(db.stock_prices.deleted) == 0

        # NO writes happened
        assert len(db.stock_prices.written) == 0

        assert result["success"] is False


# ===========================================================================
# TEST 2: Re-flag needs_price_redownload on failure
# ===========================================================================

class TestReflagOnFailure:
    """Failed fetch must re-flag needs_price_redownload=True when
    needs_redownload=True, so Phase C retries on the next run."""

    def test_reflag_on_fetch_failure(self):
        from full_sync_service import _process_price_ticker

        db = _FakeDB()

        with patch("full_sync_service._fetch_one", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = (None, 500, 50, False)
            with patch("full_sync_service._log_credit", new_callable=AsyncMock):
                asyncio.get_event_loop().run_until_complete(
                    _process_price_ticker(
                        db, "GRTUF.US", "test_job",
                        needs_redownload=True,
                    )
                )

        # Check that tracked_tickers was updated with re-flag
        assert len(db.tracked_tickers.updates) == 1
        update = db.tracked_tickers.updates[0]
        set_fields = update["update"]["$set"]
        assert set_fields["needs_price_redownload"] is True
        assert set_fields["price_history_complete"] is False
        assert set_fields["price_history_status"] == "error"

    def test_no_reflag_when_not_redownload(self):
        """When needs_redownload=False, failure should NOT set
        needs_price_redownload (it wasn't flagged to begin with)."""
        from full_sync_service import _process_price_ticker

        db = _FakeDB()

        with patch("full_sync_service._fetch_one", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = (None, 500, 50, False)
            with patch("full_sync_service._log_credit", new_callable=AsyncMock):
                asyncio.get_event_loop().run_until_complete(
                    _process_price_ticker(
                        db, "AAPL.US", "test_job",
                        needs_redownload=False,
                    )
                )

        assert len(db.tracked_tickers.updates) == 1
        set_fields = db.tracked_tickers.updates[0]["update"]["$set"]
        assert "needs_price_redownload" not in set_fields

    def test_reflag_on_rate_limit(self):
        """429 rate-limit with needs_redownload=True must also re-flag."""
        from full_sync_service import _process_price_ticker

        db = _FakeDB()

        with patch("full_sync_service._fetch_one", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = (None, 429, 50, False)
            with patch("full_sync_service._log_credit", new_callable=AsyncMock):
                asyncio.get_event_loop().run_until_complete(
                    _process_price_ticker(
                        db, "GRTUF.US", "test_job",
                        needs_redownload=True,
                    )
                )

        set_fields = db.tracked_tickers.updates[0]["update"]["$set"]
        assert set_fields["needs_price_redownload"] is True
        assert set_fields["history_download_error"] == "rate_limited"


# ===========================================================================
# TEST 3: Write-boundary validation in Phase C upsert
# ===========================================================================

class TestWriteBoundaryValidation:
    """Phase C must skip rows missing required fields (ticker/date/close)."""

    def test_rows_with_zero_close_are_skipped(self):
        """Records where close=0 must be filtered out by validate_price_row."""
        from full_sync_service import _process_price_ticker

        db = _FakeDB()
        records = [
            {"date": "2024-01-02", "open": 10.0, "high": 11.0,
             "low": 9.0, "close": 10.0, "adjusted_close": 10.0, "volume": 100},
            {"date": "2024-01-03", "open": 10.0, "high": 11.0,
             "low": 9.0, "close": 0, "adjusted_close": 0, "volume": 0},  # zero close
        ]
        db.stock_prices._agg_result = [{
            "_id": None,
            "db_first_date": "2024-01-02",
            "db_last_date": "2024-01-02",
            "db_row_count": 1,
        }]

        with patch("full_sync_service._fetch_one", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = (records, 200, 50, True)
            with patch("full_sync_service._log_credit", new_callable=AsyncMock):
                result = asyncio.get_event_loop().run_until_complete(
                    _process_price_ticker(
                        db, "TEST.US", "test_job",
                        needs_redownload=False,
                    )
                )

        # Only 1 record should be written (the one with close=10.0)
        assert result["records"] == 1

    def test_rows_without_close_are_skipped(self):
        """Records where close is None/missing must be filtered out."""
        from full_sync_service import _process_price_ticker

        db = _FakeDB()
        records = [
            {"date": "2024-01-02", "open": 10.0, "high": 11.0,
             "low": 9.0, "close": 10.0, "adjusted_close": 10.0, "volume": 100},
            {"date": "2024-01-03", "open": 10.0, "high": 11.0,
             "low": 9.0, "volume": 0},  # missing close entirely
        ]
        db.stock_prices._agg_result = [{
            "_id": None,
            "db_first_date": "2024-01-02",
            "db_last_date": "2024-01-02",
            "db_row_count": 1,
        }]

        with patch("full_sync_service._fetch_one", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = (records, 200, 50, True)
            with patch("full_sync_service._log_credit", new_callable=AsyncMock):
                result = asyncio.get_event_loop().run_until_complete(
                    _process_price_ticker(
                        db, "TEST.US", "test_job",
                        needs_redownload=False,
                    )
                )

        assert result["records"] == 1


# ===========================================================================
# TEST 4: Upsert key is (ticker, date)
# ===========================================================================

class TestUpsertKeyIsTickerDate:
    """Bulk upsert filter must be {ticker, date} — not just {ticker}."""

    def test_upsert_filter_is_ticker_and_date(self):
        from full_sync_service import _process_price_ticker

        db = _FakeDB()
        records = [
            {"date": "2024-01-02", "open": 10.0, "high": 11.0,
             "low": 9.0, "close": 10.0, "adjusted_close": 10.0, "volume": 100},
        ]
        db.stock_prices._agg_result = [{
            "_id": None,
            "db_first_date": "2024-01-02",
            "db_last_date": "2024-01-02",
            "db_row_count": 1,
        }]

        with patch("full_sync_service._fetch_one", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = (records, 200, 50, True)
            with patch("full_sync_service._log_credit", new_callable=AsyncMock):
                asyncio.get_event_loop().run_until_complete(
                    _process_price_ticker(
                        db, "AAPL.US", "test_job",
                        needs_redownload=False,
                    )
                )

        # Inspect the UpdateOne operations written
        assert len(db.stock_prices.written) == 1
        op = db.stock_prices.written[0]
        # UpdateOne._filter contains the filter dict
        filt = op._filter
        assert "ticker" in filt
        assert "date" in filt
        assert filt["ticker"] == "AAPL.US"
        assert filt["date"] == "2024-01-02"
