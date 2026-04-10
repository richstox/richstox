"""Tests for the Phase C suspect-incomplete guard in full_sync_service.

The guard prevents _process_price_ticker from marking
price_history_complete=True when the EODHD API returns fewer than
_MIN_HISTORY_RECORDS records.  Without this guard, a ticker with
truncated data is permanently sealed out of all Phase C retry paths
(the "broken link" in the returning-to-bulk redownload pipeline).
"""

import asyncio
import sys
import os
from types import SimpleNamespace
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch, MagicMock

import pytest

# Ensure the backend directory is in the Python path.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


# ---------------------------------------------------------------------------
# Fake DB collections used by _process_price_ticker
# ---------------------------------------------------------------------------

class _FakeStockPrices:
    def __init__(self):
        self.deleted = []
        self.written = []
        self._records = []  # Simulated existing records

    async def delete_many(self, filt):
        self.deleted.append(filt)
        return SimpleNamespace(deleted_count=len(self._records))

    async def bulk_write(self, ops, ordered=False):
        self.written.extend(ops)
        return SimpleNamespace(upserted_count=len(ops), modified_count=0)

    async def find_one(self, filt, projection=None, sort=None):
        # Return the "latest" record
        return {"date": "2026-04-08"} if self.written else None


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


# ---------------------------------------------------------------------------
# Helper: build a fake EODHD response with N records
# ---------------------------------------------------------------------------

def _make_eod_records(n: int):
    """Return a list of N fake EOD records."""
    return [
        {
            "date": f"2026-01-{str(i + 1).zfill(2)}",
            "open": 100.0 + i,
            "high": 105.0 + i,
            "low": 95.0 + i,
            "close": 102.0 + i,
            "adjusted_close": 102.0 + i,
            "volume": 1000000,
        }
        for i in range(n)
    ]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestSuspectIncompleteGuard:
    """Verify the _MIN_HISTORY_RECORDS guard in _process_price_ticker."""

    def test_normal_download_marks_complete(self):
        """Download with >= _MIN_HISTORY_RECORDS → price_history_complete=True."""
        from full_sync_service import _process_price_ticker, _MIN_HISTORY_RECORDS

        db = _FakeDB()
        records = _make_eod_records(_MIN_HISTORY_RECORDS + 5)

        with patch("full_sync_service._fetch_one", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = (records, 200, 50, True)

            result = asyncio.run(
                _process_price_ticker(db, "AAPL.US", job_name="test", needs_redownload=False)
            )

        assert result["success"] is True
        assert result["records"] == len(records)

        # The final tracked_tickers update should set price_history_complete=True
        last_update = db.tracked_tickers.updates[-1]
        set_fields = last_update["update"]["$set"]
        assert set_fields["price_history_complete"] is True
        assert set_fields["price_history_status"] == "complete"
        assert set_fields["needs_price_redownload"] is False

    def test_suspect_download_does_not_mark_complete(self):
        """Download with < _MIN_HISTORY_RECORDS → price_history_complete NOT set to True."""
        from full_sync_service import _process_price_ticker, _MIN_HISTORY_RECORDS

        db = _FakeDB()
        records = _make_eod_records(3)  # Well below threshold

        with patch("full_sync_service._fetch_one", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = (records, 200, 50, True)

            result = asyncio.run(
                _process_price_ticker(db, "NYC.US", job_name="test", needs_redownload=False)
            )

        assert result["success"] is False, "Suspect download should return success=False"
        assert result.get("suspect_incomplete") is True
        assert result["records"] == 3

        # The tracked_tickers update should NOT set price_history_complete=True
        last_update = db.tracked_tickers.updates[-1]
        set_fields = last_update["update"]["$set"]
        assert "price_history_complete" not in set_fields, (
            "Suspect download must NOT set price_history_complete=True"
        )
        assert set_fields["price_history_status"] == "suspect_incomplete"
        assert set_fields["needs_price_redownload"] is False

    def test_suspect_download_still_writes_records(self):
        """Even with < _MIN_HISTORY_RECORDS, records ARE written to stock_prices."""
        from full_sync_service import _process_price_ticker

        db = _FakeDB()
        records = _make_eod_records(3)

        with patch("full_sync_service._fetch_one", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = (records, 200, 50, True)

            result = asyncio.run(
                _process_price_ticker(db, "NYC.US", job_name="test", needs_redownload=False)
            )

        # Records should have been written
        assert len(db.stock_prices.written) == 3
        assert result["records"] == 3

    def test_redownload_with_suspect_data_clears_flag(self):
        """needs_redownload=True + suspect result → needs_price_redownload=False.

        The ticker will still be retried on the next Phase C run because
        price_history_complete is not set to True.
        """
        from full_sync_service import _process_price_ticker

        db = _FakeDB()
        records = _make_eod_records(2)

        with patch("full_sync_service._fetch_one", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = (records, 200, 50, True)

            result = asyncio.run(
                _process_price_ticker(db, "NYC.US", job_name="test", needs_redownload=True)
            )

        # Old data should have been deleted (redownload path)
        assert len(db.stock_prices.deleted) == 1

        # needs_price_redownload should be cleared
        last_update = db.tracked_tickers.updates[-1]
        set_fields = last_update["update"]["$set"]
        assert set_fields["needs_price_redownload"] is False

        # But price_history_complete should NOT be True
        assert "price_history_complete" not in set_fields

    def test_exactly_threshold_marks_complete(self):
        """Download with exactly _MIN_HISTORY_RECORDS → marks complete."""
        from full_sync_service import _process_price_ticker, _MIN_HISTORY_RECORDS

        db = _FakeDB()
        records = _make_eod_records(_MIN_HISTORY_RECORDS)

        with patch("full_sync_service._fetch_one", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = (records, 200, 50, True)

            result = asyncio.run(
                _process_price_ticker(db, "AAPL.US", job_name="test", needs_redownload=False)
            )

        assert result["success"] is True
        last_update = db.tracked_tickers.updates[-1]
        set_fields = last_update["update"]["$set"]
        assert set_fields["price_history_complete"] is True

    def test_phase_c_retries_suspect_ticker(self):
        """A suspect ticker matches Phase C's {price_history_complete: {$ne: True}}.

        After a suspect download, the ticker has:
        - price_history_status: "suspect_incomplete"
        - price_history_complete: NOT True (either False or absent)
        - needs_price_redownload: False

        Phase C query uses: {"price_history_complete": {"$ne": True}}
        This should match, so the ticker is retried on the next run.
        """
        # This is a logical test — verify that the suspect path leaves
        # the ticker in a state that matches Phase C's selection query.
        from full_sync_service import _process_price_ticker

        db = _FakeDB()
        records = _make_eod_records(3)

        with patch("full_sync_service._fetch_one", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = (records, 200, 50, True)

            asyncio.run(
                _process_price_ticker(db, "NYC.US", job_name="test", needs_redownload=False)
            )

        # Simulate the ticker state after the suspect update
        last_update = db.tracked_tickers.updates[-1]
        set_fields = last_update["update"]["$set"]

        # Verify Phase C would pick this up
        ticker_state = {
            "ticker": "NYC.US",
            "is_seeded": True,
            "has_price_data": True,
            "price_history_complete": set_fields.get("price_history_complete"),
            "needs_price_redownload": set_fields.get("needs_price_redownload", False),
            "history_download_proven_at": set_fields.get("history_download_proven_at"),
        }

        # Phase C $or branch 1: price_history_complete != True
        assert ticker_state["price_history_complete"] is not True, (
            "Suspect ticker must match Phase C's {price_history_complete: {$ne: True}}"
        )
