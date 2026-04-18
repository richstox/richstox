"""
Tests for _remediate_price_redownload range-proof guard.

Root cause: _remediate_price_redownload (Step 2.7 split/dividend remediation)
set price_history_complete=True after backfill_ticker_prices returned
success=True with records_upserted > 0, WITHOUT validating the DB state.

Tickers like BODI where EODHD returned only a few rows got
price_history_complete=True → passed visibility Gate 8 → showed as visible
with a broken 3-point chart.

Phase C (full_sync_service._process_price_ticker) already has a range-proof
guard via _check_range_proof.  This fix aligns _remediate_price_redownload
with the same standard: post-write DB aggregation + range-proof check.

These tests prove:
  1) Range-proof pass → price_history_complete=True (normal case)
  2) Range-proof fail → price_history_complete=False, needs_price_redownload=True
  3) Provider date_range missing → range-proof fails safely
  4) DB empty after write → range-proof fails safely
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
# Fake DB collections
# ---------------------------------------------------------------------------

class _FakeAggCursor:
    def __init__(self, results):
        self._results = results

    async def to_list(self, n):
        return self._results[:n] if n else self._results


class _FakeStockPrices:
    def __init__(self, agg_result=None):
        self._agg_result = agg_result or []

    def aggregate(self, pipeline):
        return _FakeAggCursor(self._agg_result)


class _FakeTrackedTickers:
    def __init__(self):
        self.updates = []
        self._docs = []

    async def find_one(self, filt, projection=None):
        for doc in self._docs:
            if all(doc.get(k) == v for k, v in filt.items() if not isinstance(v, dict)):
                return doc
        return None

    def find(self, filt, projection=None):
        return _FakeAggCursor(self._docs)

    async def update_one(self, filt, update):
        self.updates.append({"filter": filt, "update": update})
        return SimpleNamespace(modified_count=1)


class _FakeOpsConfig:
    async def find_one(self, filt):
        return None

    async def delete_one(self, filt):
        pass


class _FakeDB:
    def __init__(self, agg_result=None):
        self.stock_prices = _FakeStockPrices(agg_result)
        self.tracked_tickers = _FakeTrackedTickers()
        self.tracked_tickers._docs = [
            {"ticker": "GOOD.US", "name": "Good Corp"},
            {"ticker": "BODI.US", "name": "Beachbody"},
        ]
        self.ops_config = _FakeOpsConfig()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestRemediateRangeProof:
    """_remediate_price_redownload must not set price_history_complete=True
    unless the post-write range-proof passes."""

    @pytest.mark.asyncio
    async def test_range_proof_pass_sets_complete(self):
        """Full history backfill with good range-proof → price_history_complete=True."""
        db = _FakeDB(agg_result=[{
            "_id": None,
            "db_first": "2020-01-02",
            "db_last": "2026-04-17",
            "db_count": 1500,
        }])

        backfill_result = {
            "ticker": "GOOD.US",
            "success": True,
            "records_upserted": 1500,
            "date_range": {"from": "2020-01-02", "to": "2026-04-17"},
        }

        with patch("price_ingestion_service.backfill_ticker_prices", return_value=backfill_result):
            from scheduler_service import _remediate_price_redownload
            result = await _remediate_price_redownload(
                db, ["GOOD.US"], datetime.now(timezone.utc),
            )

        assert result["succeeded"] == 1
        assert result["failed"] == 0

        # Verify the update set price_history_complete=True
        update = db.tracked_tickers.updates[-1]
        set_fields = update["update"]["$set"]
        assert set_fields["price_history_complete"] is True
        assert set_fields["price_history_status"] == "complete"
        assert set_fields["range_proof"]["pass"] is True

    @pytest.mark.asyncio
    async def test_range_proof_fail_blocks_complete(self):
        """Backfill returns 3 rows but DB only has 3 rows → range-proof fails.
        This is the exact BODI scenario."""
        db = _FakeDB(agg_result=[{
            "_id": None,
            "db_first": "2026-04-15",
            "db_last": "2026-04-17",
            "db_count": 3,
        }])

        # Provider says history from 2020..2026 but only 3 rows written
        backfill_result = {
            "ticker": "BODI.US",
            "success": True,
            "records_upserted": 3,
            "date_range": {"from": "2020-03-01", "to": "2026-04-17"},
        }

        with patch("price_ingestion_service.backfill_ticker_prices", return_value=backfill_result):
            from scheduler_service import _remediate_price_redownload
            result = await _remediate_price_redownload(
                db, ["BODI.US"], datetime.now(timezone.utc),
            )

        assert result["succeeded"] == 0
        assert result["failed"] == 1

        # Verify price_history_complete is NOT set to True
        update = db.tracked_tickers.updates[-1]
        set_fields = update["update"]["$set"]
        assert set_fields["price_history_complete"] is False
        assert set_fields["needs_price_redownload"] is True
        assert set_fields["price_history_status"] == "range_proof_failed"
        assert set_fields["range_proof"]["pass"] is False

    @pytest.mark.asyncio
    async def test_missing_provider_date_range_fails_safely(self):
        """backfill_ticker_prices returns no date_range → range-proof fails."""
        db = _FakeDB(agg_result=[{
            "_id": None,
            "db_first": "2026-04-17",
            "db_last": "2026-04-17",
            "db_count": 1,
        }])

        backfill_result = {
            "ticker": "BODI.US",
            "success": True,
            "records_upserted": 1,
            "date_range": None,  # provider returned no date info
        }

        with patch("price_ingestion_service.backfill_ticker_prices", return_value=backfill_result):
            from scheduler_service import _remediate_price_redownload
            result = await _remediate_price_redownload(
                db, ["BODI.US"], datetime.now(timezone.utc),
            )

        assert result["failed"] == 1

        update = db.tracked_tickers.updates[-1]
        set_fields = update["update"]["$set"]
        assert set_fields["price_history_complete"] is False

    @pytest.mark.asyncio
    async def test_empty_db_after_write_fails_safely(self):
        """DB aggregation returns empty (hypothetical race) → range-proof fails."""
        db = _FakeDB(agg_result=[])  # empty aggregation result

        backfill_result = {
            "ticker": "BODI.US",
            "success": True,
            "records_upserted": 3,
            "date_range": {"from": "2020-01-02", "to": "2026-04-17"},
        }

        with patch("price_ingestion_service.backfill_ticker_prices", return_value=backfill_result):
            from scheduler_service import _remediate_price_redownload
            result = await _remediate_price_redownload(
                db, ["BODI.US"], datetime.now(timezone.utc),
            )

        assert result["failed"] == 1

        update = db.tracked_tickers.updates[-1]
        set_fields = update["update"]["$set"]
        assert set_fields["price_history_complete"] is False
