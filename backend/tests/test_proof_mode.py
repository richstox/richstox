"""
Tests for proof_mode_service.run_proof_mode().

All tests use mocked DB and bulk data — no external API calls.
"""

import os
import sys
import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from bson import ObjectId

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from services.proof_mode_service import run_proof_mode, _normalize_ticker


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _make_bulk_row(code: str, date: str, close: float = 100.0):
    return {
        "code": code,
        "date": date,
        "open": close - 1,
        "high": close + 1,
        "low": close - 2,
        "close": close,
        "adjusted_close": close,
        "volume": 50000,
    }


def _make_db(
    stock_prices_rows=None,
    tracked_tickers_doc=None,
    ops_job_runs_doc=None,
    market_calendar_doc=None,
):
    """Build a mock motor database with the collections proof_mode queries."""

    db = MagicMock()

    # stock_prices.find().limit().to_list()
    sp_cursor = AsyncMock()
    sp_cursor.to_list = AsyncMock(return_value=stock_prices_rows or [])
    sp_find = MagicMock(return_value=MagicMock(limit=MagicMock(return_value=sp_cursor)))
    db.stock_prices.find = sp_find

    # tracked_tickers.find_one()
    db.tracked_tickers.find_one = AsyncMock(return_value=tracked_tickers_doc)

    # ops_job_runs.find_one()
    db.ops_job_runs.find_one = AsyncMock(return_value=ops_job_runs_doc)

    # ops_job_runs.aggregate() — used by _get_bulk_processed_dates
    async def _empty_agg(*args, **kwargs):
        return
        yield  # make it an async generator that yields nothing

    db.ops_job_runs.aggregate = _empty_agg

    # market_calendar collection
    mc_collection = MagicMock()
    mc_collection.find_one = AsyncMock(return_value=market_calendar_doc)
    mc_find = MagicMock()
    mc_find.to_list = AsyncMock(return_value=[])
    mc_collection.find = MagicMock(return_value=mc_find)
    db.__getitem__ = MagicMock(return_value=mc_collection)

    return db


# ---------------------------------------------------------------------------
# Normalization tests
# ---------------------------------------------------------------------------
class TestNormalizeTicker:
    def test_plain_symbol(self):
        assert _normalize_ticker("AAPL") == "AAPL.US"

    def test_with_dot_us(self):
        assert _normalize_ticker("AHH.US") == "AHH.US"

    def test_lowercase(self):
        assert _normalize_ticker("ahh.us") == "AHH.US"

    def test_whitespace(self):
        assert _normalize_ticker("  AHH.US  ") == "AHH.US"

    def test_none(self):
        assert _normalize_ticker(None) is None

    def test_empty(self):
        assert _normalize_ticker("") is None


# ---------------------------------------------------------------------------
# Main reconciliation tests
# ---------------------------------------------------------------------------
@pytest.fixture
def bulk_data_ahh():
    """Bulk payload containing AHH for 2026-03-31."""
    return [
        _make_bulk_row("AHH", "2026-03-31", 12.50),
        _make_bulk_row("AAPL", "2026-03-31", 210.0),
        _make_bulk_row("MSFT", "2026-03-31", 390.0),
    ]


class TestProofModeConsistent:
    """Ticker present in both bulk and DB → CONSISTENT."""

    @pytest.mark.asyncio
    async def test_both_present(self, bulk_data_ahh):
        oid = ObjectId()
        db = _make_db(
            stock_prices_rows=[
                {"_id": oid, "ticker": "AHH.US", "date": "2026-03-31", "close": 12.5}
            ],
            tracked_tickers_doc={
                "ticker": "AHH.US", "exchange": "NYSE",
                "asset_type": "Common Stock", "is_seeded": True,
                "is_visible": True,
            },
        )

        result = await run_proof_mode(
            db, ticker="AHH.US", date="2026-03-31",
            bulk_data_override=bulk_data_ahh,
        )

        assert result["bulk_check"]["found"] is True
        assert result["bulk_check"]["matched_symbol"] == "AHH"
        assert result["db_check"]["found"] is True
        assert result["db_check"]["count"] == 1
        assert result["skip_reasons"] is None
        assert "CONSISTENT" in result["summary"]


class TestProofModeBothAbsent:
    """Ticker absent from both bulk and DB → CONSISTENT absent."""

    @pytest.mark.asyncio
    async def test_both_absent(self, bulk_data_ahh):
        db = _make_db()

        result = await run_proof_mode(
            db, ticker="ZZZZ.US", date="2026-03-31",
            bulk_data_override=bulk_data_ahh,
        )

        assert result["bulk_check"]["found"] is False
        assert result["db_check"]["found"] is False
        assert result["skip_reasons"] is None
        assert "CONSISTENT" in result["summary"]


class TestProofModeGapDetected:
    """Ticker in bulk but NOT in DB → GAP DETECTED with skip reasons."""

    @pytest.mark.asyncio
    async def test_gap_not_in_seeded(self, bulk_data_ahh):
        """Ticker is in bulk but has no tracked_tickers entry."""
        db = _make_db(
            tracked_tickers_doc=None,  # not seeded at all
        )
        # Override find_one for step2 query too
        db.tracked_tickers.find_one = AsyncMock(return_value=None)

        result = await run_proof_mode(
            db, ticker="AHH.US", date="2026-03-31",
            bulk_data_override=bulk_data_ahh,
        )

        assert result["bulk_check"]["found"] is True
        assert result["db_check"]["found"] is False
        assert result["skip_reasons"]["not_in_seeded"] is True
        assert result["skip_reasons"]["primary_reason"] == "not_in_seeded"
        assert "GAP DETECTED" in result["summary"]

    @pytest.mark.asyncio
    async def test_gap_non_trading_day(self, bulk_data_ahh):
        """Ticker is seeded but date is a non-trading day."""
        seeded = {
            "ticker": "AHH.US", "exchange": "NYSE",
            "asset_type": "Common Stock", "is_seeded": True,
        }

        db = _make_db(
            tracked_tickers_doc=seeded,
            market_calendar_doc={
                "is_trading_day": False,
                "holiday_name": "Good Friday",
            },
        )
        # Make the step2 query also return seeded
        call_count = [0]
        async def _find_one_side_effect(*args, **kwargs):
            call_count[0] += 1
            return seeded
        db.tracked_tickers.find_one = AsyncMock(side_effect=_find_one_side_effect)

        result = await run_proof_mode(
            db, ticker="AHH.US", date="2026-03-31",
            bulk_data_override=bulk_data_ahh,
        )

        assert result["bulk_check"]["found"] is True
        assert result["db_check"]["found"] is False
        assert result["skip_reasons"]["filtered_by_non_trading_day"] is True
        assert result["skip_reasons"]["holiday_name"] == "Good Friday"


class TestProofModeNormalization:
    """Normalization audit correctly reports raw/normalized forms."""

    @pytest.mark.asyncio
    async def test_normalization_all_match(self, bulk_data_ahh):
        oid = ObjectId()
        db = _make_db(
            stock_prices_rows=[
                {"_id": oid, "ticker": "AHH.US", "date": "2026-03-31", "close": 12.5}
            ],
            tracked_tickers_doc={
                "ticker": "AHH.US", "exchange": "NYSE",
                "asset_type": "Common Stock", "is_seeded": True,
            },
        )

        result = await run_proof_mode(
            db, ticker="AHH.US", date="2026-03-31",
            bulk_data_override=bulk_data_ahh,
        )

        norm = result["normalization"]
        assert norm["input_normalized"] == "AHH.US"
        assert norm["bulk_symbol_raw"] == "AHH"
        assert norm["bulk_symbol_normalized"] == "AHH.US"
        assert norm["seeded_ticker_raw"] == "AHH.US"
        assert norm["seeded_ticker_normalized"] == "AHH.US"
        assert norm["all_match"] is True


class TestProofModeGapCheckContext:
    """Gap-check context section returns expected_dates info."""

    @pytest.mark.asyncio
    async def test_gap_check_context_fields(self, bulk_data_ahh):
        db = _make_db(
            stock_prices_rows=[
                {"_id": ObjectId(), "ticker": "AHH.US", "date": "2026-03-31", "close": 12.5}
            ],
            tracked_tickers_doc={"ticker": "AHH.US"},
        )

        result = await run_proof_mode(
            db, ticker="AHH.US", date="2026-03-31",
            bulk_data_override=bulk_data_ahh,
        )

        ctx = result["gap_check_context"]
        assert "date_in_expected_dates" in ctx
        assert "expected_dates_count" in ctx
        assert "expected_dates_sample" in ctx


class TestProofModeEmptyBulk:
    """Empty bulk payload → bulk_found=False."""

    @pytest.mark.asyncio
    async def test_empty_bulk(self):
        db = _make_db()

        result = await run_proof_mode(
            db, ticker="AHH.US", date="2026-03-31",
            bulk_data_override=[],
        )

        assert result["bulk_check"]["found"] is False
        assert result["bulk_check"]["raw_row_count"] == 0
