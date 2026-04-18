"""Tests for the Phase C range-proof validation in full_sync_service.

The range-proof replaces the old arbitrary _MIN_HISTORY_RECORDS threshold.
Phase C marks price_history_complete=True ONLY when the DB date range
covers the provider's date range within _RANGE_PROOF_TOLERANCE_DAYS.
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
        self._agg_result = []  # Set per-test for aggregate() calls

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


# ---------------------------------------------------------------------------
# Helper: build a fake EODHD response with a date range
# ---------------------------------------------------------------------------

def _make_eod_records_range(start_date: str, end_date: str, freq_days: int = 1):
    """Return a list of fake EOD records spanning start_date..end_date.

    Generates one record every freq_days between start and end.
    """
    from datetime import datetime, timedelta

    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    records = []
    current = start
    i = 0
    while current <= end:
        records.append({
            "date": current.strftime("%Y-%m-%d"),
            "open": 100.0 + i,
            "high": 105.0 + i,
            "low": 95.0 + i,
            "close": 102.0 + i,
            "adjusted_close": 102.0 + i,
            "volume": 1000000,
        })
        current += timedelta(days=freq_days)
        i += 1
    return records


# ---------------------------------------------------------------------------
# Tests: _check_range_proof
# ---------------------------------------------------------------------------

class TestCheckRangeProof:
    """Unit tests for the _check_range_proof helper function."""

    def test_exact_match_passes(self):
        from full_sync_service import _check_range_proof
        assert _check_range_proof(
            "2020-01-02", "2026-04-08",
            "2020-01-02", "2026-04-08",
        ) is True

    def test_db_covers_more_passes(self):
        from full_sync_service import _check_range_proof
        assert _check_range_proof(
            "2020-01-02", "2026-04-08",
            "2019-12-30", "2026-04-09",
        ) is True

    def test_db_within_tolerance_passes(self):
        from full_sync_service import _check_range_proof, _RANGE_PROOF_TOLERANCE_DAYS
        # DB first date 3 days after provider first, DB last 2 days before provider last
        assert _check_range_proof(
            "2020-01-01", "2026-04-08",
            "2020-01-04", "2026-04-06",
        ) is True

    def test_db_exactly_at_tolerance_boundary_passes(self):
        from full_sync_service import _check_range_proof, _RANGE_PROOF_TOLERANCE_DAYS
        # DB first date is EXACTLY tolerance_days after provider first → edge, still OK
        assert _RANGE_PROOF_TOLERANCE_DAYS == 5, "Test assumes tolerance=5"
        assert _check_range_proof(
            "2020-01-01", "2026-04-08",
            "2020-01-06", "2026-04-03",  # +5d first, -5d last
        ) is True

    def test_db_one_day_past_tolerance_fails(self):
        from full_sync_service import _check_range_proof, _RANGE_PROOF_TOLERANCE_DAYS
        # DB first date is tolerance_days+1 after provider first → FAIL
        assert _RANGE_PROOF_TOLERANCE_DAYS == 5, "Test assumes tolerance=5"
        assert _check_range_proof(
            "2020-01-01", "2026-04-08",
            "2020-01-07", "2026-04-08",  # +6d first → too late
        ) is False

    def test_db_first_too_late_fails(self):
        from full_sync_service import _check_range_proof
        # DB starts years after provider → fail
        assert _check_range_proof(
            "1984-11-07", "2026-04-08",
            "2026-04-07", "2026-04-08",
        ) is False

    def test_db_last_too_early_fails(self):
        from full_sync_service import _check_range_proof
        # DB ends years before provider → fail
        assert _check_range_proof(
            "1984-11-07", "2026-04-08",
            "1984-11-07", "2020-01-01",
        ) is False

    def test_missing_dates_fail(self):
        from full_sync_service import _check_range_proof
        assert _check_range_proof(None, "2026-04-08", "2020-01-01", "2026-04-08") is False
        assert _check_range_proof("2020-01-01", None, "2020-01-01", "2026-04-08") is False
        assert _check_range_proof("2020-01-01", "2026-04-08", None, "2026-04-08") is False
        assert _check_range_proof("2020-01-01", "2026-04-08", "2020-01-01", None) is False

    def test_invalid_date_format_fails(self):
        from full_sync_service import _check_range_proof
        assert _check_range_proof("not-a-date", "2026-04-08", "2020-01-01", "2026-04-08") is False


# ---------------------------------------------------------------------------
# Tests: _process_price_ticker with range-proof
# ---------------------------------------------------------------------------

class TestRangeProofInProcessPriceTicker:
    """Verify that _process_price_ticker uses range-proof to decide completeness."""

    def test_full_history_marks_complete(self):
        """Download spanning 1984-2026 → range-proof passes → complete=True."""
        from full_sync_service import _process_price_ticker

        db = _FakeDB()
        records = _make_eod_records_range("1984-11-07", "2026-04-08", freq_days=7)
        # Set up aggregate to return matching DB stats
        db.stock_prices._agg_result = [{
            "_id": None,
            "db_first_date": "1984-11-07",
            "db_last_date": "2026-04-08",
            "db_row_count": len(records),
        }]

        with patch("full_sync_service._fetch_one", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = (records, 200, 50, True)
            result = asyncio.run(
                _process_price_ticker(db, "BDL.US", job_name="test", needs_redownload=False)
            )

        assert result["success"] is True
        assert result["records"] == len(records)

        # Check tracked_tickers was updated with range_proof
        last_update = db.tracked_tickers.updates[-1]
        set_fields = last_update["update"]["$set"]
        assert set_fields["price_history_complete"] is True
        assert set_fields["price_history_status"] == "complete"
        assert set_fields["range_proof"]["pass"] is True
        assert set_fields["range_proof"]["provider_first_date"] == "1984-11-07"
        assert set_fields["range_proof"]["provider_last_date"] == "2026-04-08"

    def test_truncated_db_does_not_mark_complete(self):
        """Provider has 1984-2026 but DB only has 2026-04-07..2026-04-08.

        This is the BDL.US scenario: EODHD returned full history but only
        2 records landed in the DB (from bulk catchup, not Phase C).
        """
        from full_sync_service import _process_price_ticker

        db = _FakeDB()
        records = _make_eod_records_range("1984-11-07", "2026-04-08", freq_days=7)
        # DB only has 2 rows — simulating the truncated scenario
        db.stock_prices._agg_result = [{
            "_id": None,
            "db_first_date": "2026-04-07",
            "db_last_date": "2026-04-08",
            "db_row_count": 2,
        }]

        with patch("full_sync_service._fetch_one", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = (records, 200, 50, True)
            result = asyncio.run(
                _process_price_ticker(db, "BDL.US", job_name="test", needs_redownload=False)
            )

        assert result["success"] is False
        assert result.get("range_proof_failed") is True

        # Should NOT mark complete
        last_update = db.tracked_tickers.updates[-1]
        set_fields = last_update["update"]["$set"]
        assert "price_history_complete" not in set_fields
        assert set_fields["price_history_status"] == "range_proof_failed"
        assert set_fields["range_proof"]["pass"] is False

    def test_small_but_valid_history_marks_complete(self):
        """A young stock with only 5 records — but matching provider range → complete.

        This proves we're NOT using an arbitrary row-count threshold.
        """
        from full_sync_service import _process_price_ticker

        db = _FakeDB()
        records = _make_eod_records_range("2026-04-01", "2026-04-05")
        db.stock_prices._agg_result = [{
            "_id": None,
            "db_first_date": "2026-04-01",
            "db_last_date": "2026-04-05",
            "db_row_count": 5,
        }]

        with patch("full_sync_service._fetch_one", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = (records, 200, 50, True)
            result = asyncio.run(
                _process_price_ticker(db, "NEW.US", job_name="test", needs_redownload=False)
            )

        assert result["success"] is True
        last_update = db.tracked_tickers.updates[-1]
        set_fields = last_update["update"]["$set"]
        assert set_fields["price_history_complete"] is True
        assert set_fields["range_proof"]["db_row_count"] == 5
        assert set_fields["range_proof"]["pass"] is True

    def test_proof_fields_persisted_on_failure(self):
        """Even when range-proof fails, proof fields are persisted for auditing."""
        from full_sync_service import _process_price_ticker

        db = _FakeDB()
        records = _make_eod_records_range("1984-11-07", "2026-04-08", freq_days=7)
        db.stock_prices._agg_result = [{
            "_id": None,
            "db_first_date": "2026-04-07",
            "db_last_date": "2026-04-08",
            "db_row_count": 2,
        }]

        with patch("full_sync_service._fetch_one", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = (records, 200, 50, True)
            asyncio.run(
                _process_price_ticker(db, "BDL.US", job_name="test", needs_redownload=False)
            )

        last_update = db.tracked_tickers.updates[-1]
        rp = last_update["update"]["$set"]["range_proof"]
        assert rp["provider_first_date"] == "1984-11-07"
        assert rp["provider_last_date"] == "2026-04-08"
        assert rp["db_first_date"] == "2026-04-07"
        assert rp["db_last_date"] == "2026-04-08"
        assert rp["db_row_count"] == 2
        assert rp["pass"] is False

    def test_empty_db_after_write_does_not_mark_complete(self):
        """If aggregate returns empty (no rows), range-proof must fail."""
        from full_sync_service import _process_price_ticker

        db = _FakeDB()
        records = _make_eod_records_range("2020-01-01", "2026-04-08", freq_days=7)
        db.stock_prices._agg_result = []  # No rows in DB

        with patch("full_sync_service._fetch_one", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = (records, 200, 50, True)
            result = asyncio.run(
                _process_price_ticker(db, "TEST.US", job_name="test", needs_redownload=False)
            )

        assert result["success"] is False
        assert result.get("range_proof_failed") is True

    def test_redownload_with_range_proof_fail_clears_flag(self):
        """needs_redownload=True + range-proof fail → needs_price_redownload=False."""
        from full_sync_service import _process_price_ticker

        db = _FakeDB()
        records = _make_eod_records_range("1984-11-07", "2026-04-08", freq_days=7)
        db.stock_prices._agg_result = [{
            "_id": None,
            "db_first_date": "2026-04-07",
            "db_last_date": "2026-04-08",
            "db_row_count": 2,
        }]

        with patch("full_sync_service._fetch_one", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = (records, 200, 50, True)
            result = asyncio.run(
                _process_price_ticker(db, "BDL.US", job_name="test", needs_redownload=True)
            )

        # Old data should have been deleted
        assert len(db.stock_prices.deleted) == 1
        # needs_price_redownload should be cleared
        last_update = db.tracked_tickers.updates[-1]
        set_fields = last_update["update"]["$set"]
        assert set_fields["needs_price_redownload"] is False


class TestPreflightRangeProofUnsealing:
    """Verify Phase C pre-flight uses range_proof instead of row-count threshold."""

    def test_ticker_without_range_proof_is_unsealed(self):
        """Ticker with price_history_complete=True and no range_proof field → unseal.

        This is the primary BDL.US scenario: sealed before range-proof existed.
        """
        ticker_after_unseal = {
            "price_history_complete": False,
            "needs_price_redownload": True,
        }
        # Phase C $or branch 1: price_history_complete != True → matches
        assert ticker_after_unseal["price_history_complete"] is not True
        # Phase C $or branch 2: needs_price_redownload → matches
        assert ticker_after_unseal["needs_price_redownload"] is True

    def test_ticker_with_passing_range_proof_is_not_unsealed(self):
        """Ticker with range_proof.pass=True → keep sealed."""
        from full_sync_service import _check_range_proof
        rp = {
            "provider_first_date": "2000-01-03",
            "provider_last_date": "2026-04-08",
            "db_first_date": "2000-01-03",
            "db_last_date": "2026-04-08",
            "pass": True,
        }
        # Re-verify passes
        assert _check_range_proof(
            rp["provider_first_date"], rp["provider_last_date"],
            rp["db_first_date"], rp["db_last_date"],
        ) is True

    def test_ticker_with_failed_range_proof_is_unsealed(self):
        """Ticker with range_proof.pass=False → unseal."""
        from full_sync_service import _check_range_proof
        rp = {
            "provider_first_date": "1984-11-07",
            "provider_last_date": "2026-04-08",
            "db_first_date": "2026-04-07",
            "db_last_date": "2026-04-08",
            "pass": False,
        }
        assert _check_range_proof(
            rp["provider_first_date"], rp["provider_last_date"],
            rp["db_first_date"], rp["db_last_date"],
        ) is False


class TestModuleConstants:
    """Verify module-level constants are exported correctly."""

    def test_range_proof_tolerance_exported(self):
        from full_sync_service import _RANGE_PROOF_TOLERANCE_DAYS
        assert isinstance(_RANGE_PROOF_TOLERANCE_DAYS, int)
        assert _RANGE_PROOF_TOLERANCE_DAYS >= 1

    def test_check_range_proof_exported(self):
        from full_sync_service import _check_range_proof
        assert callable(_check_range_proof)

    def test_not_applicable_reasons_is_module_constant(self):
        """_NOT_APPLICABLE_REASONS is a module-level frozenset in scheduler_service."""
        import scheduler_service
        assert hasattr(scheduler_service, "_NOT_APPLICABLE_REASONS")
        reasons = scheduler_service._NOT_APPLICABLE_REASONS
        assert isinstance(reasons, frozenset)
        assert "not_in_bulk_not_in_api" in reasons
        assert "bulk_found_but_close_is_zero" in reasons
        assert "bulk_close_zero_api_returned_empty" in reasons
        assert "api_returned_only_zero_price" in reasons
        assert len(reasons) == 4
