"""
Test: Phase C (_process_price_ticker) suspicious-download guard.

When the EODHD API returns fewer than _MIN_HISTORY_RECORDS records, the
data IS written to stock_prices but the ticker must NOT be marked as
price_history_complete=True.  Instead, price_history_status should be
"suspect_incomplete" so Phase C retries on the next run.
"""

import asyncio
import os
import sys
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import full_sync_service


# ── Helpers ──────────────────────────────────────────────────────────────────


class _FakeBulkWriteResult:
    def __init__(self, upserted=0, modified=0):
        self.upserted_count = upserted
        self.modified_count = modified


class _FakeStockPrices:
    """Minimal mock for db.stock_prices."""

    def __init__(self):
        self.ops = []

    async def delete_many(self, _filter):
        return SimpleNamespace(deleted_count=0)

    async def bulk_write(self, ops, ordered=False):
        self.ops.extend(ops)
        return _FakeBulkWriteResult(upserted=len(ops))

    async def find_one(self, _filter, _projection=None, sort=None):
        """Return a fake latest-date doc."""
        return {"date": "2026-04-08"}


class _FakeTrackedTickers:
    """Minimal mock for db.tracked_tickers capturing $set updates."""

    def __init__(self):
        self.last_update_set: dict = {}

    async def update_one(self, _filter, update):
        self.last_update_set = update.get("$set", {})
        return SimpleNamespace(matched_count=1)


class _FakeCreditLog:
    """Minimal mock for db.credit_log."""

    async def insert_one(self, doc):
        return SimpleNamespace(inserted_id=1)


def _build_db():
    db = SimpleNamespace()
    db.stock_prices = _FakeStockPrices()
    db.tracked_tickers = _FakeTrackedTickers()
    db.credit_log = _FakeCreditLog()
    return db


# ── Tests ────────────────────────────────────────────────────────────────────


class TestSuspectIncompleteGuard:
    """Phase C should NOT mark a download as complete when record count is suspiciously low."""

    @pytest.mark.asyncio
    async def test_few_records_returns_suspect_incomplete(self):
        """With only 2 records, the function returns success=False and suspect_incomplete=True."""
        db = _build_db()

        # EODHD returns only 2 records
        fake_data = [
            {"date": "2026-04-07", "open": 8, "high": 8.5, "low": 7.5, "close": 8, "adjusted_close": 8, "volume": 100},
            {"date": "2026-04-08", "open": 8, "high": 8.2, "low": 7.8, "close": 8.06, "adjusted_close": 8.06, "volume": 200},
        ]

        with patch.object(
            full_sync_service, "_fetch_one",
            new_callable=AsyncMock,
            return_value=(fake_data, 200, 100, True),
        ), patch.object(
            full_sync_service, "_log_credit",
            new_callable=AsyncMock,
        ):
            result = await full_sync_service._process_price_ticker(
                db, "NYC.US", job_name="test", needs_redownload=False,
            )

        # Data WAS written to stock_prices
        assert len(db.stock_prices.ops) == 2

        # But download is NOT marked complete
        assert result["success"] is False
        assert result["suspect_incomplete"] is True
        assert result["records"] == 2

        # Tracked ticker updated with suspect status, NOT price_history_complete
        assert db.tracked_tickers.last_update_set["price_history_status"] == "suspect_incomplete"
        assert "price_history_complete" not in db.tracked_tickers.last_update_set

    @pytest.mark.asyncio
    async def test_enough_records_marks_complete(self):
        """With >= _MIN_HISTORY_RECORDS records, the download IS marked complete."""
        db = _build_db()

        fake_data = [
            {
                "date": f"2026-01-{i:02d}",
                "open": 10, "high": 11, "low": 9, "close": 10.5,
                "adjusted_close": 10.5, "volume": 1000,
            }
            for i in range(1, full_sync_service._MIN_HISTORY_RECORDS + 5)
        ]

        with patch.object(
            full_sync_service, "_fetch_one",
            new_callable=AsyncMock,
            return_value=(fake_data, 200, 100, True),
        ), patch.object(
            full_sync_service, "_log_credit",
            new_callable=AsyncMock,
        ):
            result = await full_sync_service._process_price_ticker(
                db, "AAPL.US", job_name="test", needs_redownload=False,
            )

        assert result["success"] is True
        assert result["records"] == len(fake_data)
        assert db.tracked_tickers.last_update_set["price_history_complete"] is True
        assert db.tracked_tickers.last_update_set["price_history_status"] == "complete"

    @pytest.mark.asyncio
    async def test_exactly_min_records_marks_complete(self):
        """With exactly _MIN_HISTORY_RECORDS records, the download IS marked complete."""
        db = _build_db()

        fake_data = [
            {
                "date": f"2026-01-{i:02d}",
                "open": 10, "high": 11, "low": 9, "close": 10.5,
                "adjusted_close": 10.5, "volume": 1000,
            }
            for i in range(1, full_sync_service._MIN_HISTORY_RECORDS + 1)
        ]

        with patch.object(
            full_sync_service, "_fetch_one",
            new_callable=AsyncMock,
            return_value=(fake_data, 200, 100, True),
        ), patch.object(
            full_sync_service, "_log_credit",
            new_callable=AsyncMock,
        ):
            result = await full_sync_service._process_price_ticker(
                db, "AAPL.US", job_name="test", needs_redownload=False,
            )

        assert result["success"] is True
        assert db.tracked_tickers.last_update_set["price_history_complete"] is True

    @pytest.mark.asyncio
    async def test_redownload_still_deletes_old_data(self):
        """When needs_redownload=True and data is suspect, old data is still deleted first."""
        db = _build_db()

        delete_called = False
        original_delete = db.stock_prices.delete_many

        async def _tracking_delete(_filter):
            nonlocal delete_called
            delete_called = True
            return await original_delete(_filter)

        db.stock_prices.delete_many = _tracking_delete

        fake_data = [
            {"date": "2026-04-08", "open": 8, "high": 8.2, "low": 7.8, "close": 8, "adjusted_close": 8, "volume": 100},
        ]

        with patch.object(
            full_sync_service, "_fetch_one",
            new_callable=AsyncMock,
            return_value=(fake_data, 200, 100, True),
        ), patch.object(
            full_sync_service, "_log_credit",
            new_callable=AsyncMock,
        ):
            result = await full_sync_service._process_price_ticker(
                db, "NYC.US", job_name="test", needs_redownload=True,
            )

        assert delete_called
        assert result["success"] is False
        assert result["suspect_incomplete"] is True
