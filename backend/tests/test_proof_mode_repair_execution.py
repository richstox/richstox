"""
Tests for proof-mode read-only behavior and Step 2 remediation persistence.

Proof-mode is strictly diagnostic/read-only — it NEVER writes to stock_prices
or tracked_tickers.  It reads persisted remediation outcomes from tracked_tickers
so it can report whether repair was done (by Step 2 or admin endpoint).

Tests:
  1. Proof-mode does NOT write to stock_prices or tracked_tickers (read-only).
  2. Step 2 direct repair persists last_remediation_action on tracked_tickers.
  3. After Step 2 repair, proof-mode reads persisted action (not null).
  4. Step 2 repair failure persists "gap_repair_failed" on tracked_tickers.
  5. Zero-close and not-seeded gaps remain unrepaired (correct behavior).
"""

import asyncio
import os
import sys
import pytest
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Set
from unittest.mock import AsyncMock, MagicMock, patch, call
from bson import ObjectId

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from services.proof_mode_service import run_proof_mode
import price_ingestion_service


# ---------------------------------------------------------------------------
# Helpers — proof-mode mock DB
# ---------------------------------------------------------------------------
DATE = "2026-04-15"
TICKER = "BODI.US"

SEEDED_DOC = {
    "ticker": "BODI.US",
    "exchange": "NASDAQ",
    "asset_type": "Common Stock",
    "is_seeded": True,
    "is_visible": True,
}

OPS_DOC = {
    "status": "success",
    "details": {
        "price_bulk_gapfill": {
            "days": [{
                "processed_date": DATE,
                "status": "success",
                "rows_written": 5000,
                "matched_seeded_tickers_count": 5000,
            }],
        },
    },
}


def _make_bulk_row(code: str, date: str = DATE, close: float = 11.0):
    return {
        "code": code,
        "date": date,
        "open": close - 0.5,
        "high": close + 0.5,
        "low": close - 1.0,
        "close": close,
        "adjusted_close": close,
        "volume": 10000,
    }


def _make_proof_db(
    *,
    stock_prices_rows=None,
    tracked_tickers_doc=None,
    ops_job_runs_doc=None,
    market_calendar_doc=None,
):
    """Build a mock motor database for proof-mode (read-only assertions)."""
    db = MagicMock()

    # stock_prices.find().limit().to_list()
    sp_cursor = AsyncMock()
    sp_cursor.to_list = AsyncMock(return_value=stock_prices_rows or [])
    sp_find = MagicMock(
        return_value=MagicMock(limit=MagicMock(return_value=sp_cursor))
    )
    db.stock_prices.find = sp_find
    # Explicitly mock write operations so we can assert they're never called
    db.stock_prices.update_one = AsyncMock()
    db.stock_prices.bulk_write = AsyncMock()
    db.stock_prices.insert_one = AsyncMock()

    # tracked_tickers.find_one()
    db.tracked_tickers.find_one = AsyncMock(return_value=tracked_tickers_doc)
    # Explicitly mock write operations
    db.tracked_tickers.update_one = AsyncMock()
    db.tracked_tickers.bulk_write = AsyncMock()

    # ops_job_runs.find_one()
    db.ops_job_runs.find_one = AsyncMock(return_value=ops_job_runs_doc)

    # ops_job_runs.aggregate() — used by _get_bulk_processed_dates
    async def _empty_agg(*args, **kwargs):
        if False:
            yield

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
# Helpers — Step 2 fake DB (from test_bulk_row_auto_remediation pattern)
# ---------------------------------------------------------------------------
class _FakeOpsConfig:
    async def find_one(self, filt):
        return None

    async def delete_one(self, filt):
        return SimpleNamespace(deleted_count=0)


class _FakeCursor:
    def __init__(self, rows: List[Dict[str, Any]]):
        self._rows = rows

    async def to_list(self, length=None):
        return list(self._rows)


class _FakeTrackedTickers:
    def __init__(self, step2_tickers: Set[str]):
        self._step2 = set(step2_tickers)
        self.writes: List[Any] = []

    async def distinct(self, field, query):
        return list(self._step2)

    async def bulk_write(self, ops, ordered=False):
        self.writes.extend(ops)
        return SimpleNamespace(upserted_count=0, modified_count=len(ops))


class _FakeStockPrices:
    def __init__(self, existing_rows: Optional[List[Dict[str, Any]]] = None):
        self.writes: List[Any] = []
        self._existing = existing_rows or []

    async def bulk_write(self, batch, ordered=False):
        self.writes.extend(batch)
        return SimpleNamespace(upserted_count=len(batch), modified_count=0)

    def find(self, query, projection=None):
        tickers = query.get("ticker", {}).get("$in", [])
        date_val = query.get("date")
        matched = [
            r for r in self._existing
            if r.get("ticker") in tickers and r.get("date") == date_val
        ]
        return _FakeCursor(matched)


class _FakeStep2DB:
    def __init__(
        self,
        step2_tickers: Set[str],
        existing_stock_prices: Optional[List[Dict[str, Any]]] = None,
    ):
        self.ops_config = _FakeOpsConfig()
        self.tracked_tickers = _FakeTrackedTickers(step2_tickers)
        self.stock_prices = _FakeStockPrices(existing_stock_prices)


def _run(coro):
    return asyncio.run(coro)


# ===========================================================================
# Test 1: Proof-mode is strictly read-only — no writes
# ===========================================================================
class TestProofModeReadOnly:
    """Proof-mode must never write to stock_prices or tracked_tickers."""

    @pytest.mark.asyncio
    async def test_proof_mode_does_not_write_for_proven_gap(self):
        """
        Scenario: proven true gap (bulk_found, !db_found, seeded, close>0).
        Proof-mode must NOT call any write operations.
        It reports the gap and awaiting_repair status.
        """
        bulk_data = [_make_bulk_row("BODI", close=11.0)]
        db = _make_proof_db(
            tracked_tickers_doc=SEEDED_DOC,
            ops_job_runs_doc=OPS_DOC,
        )

        result = await run_proof_mode(
            db, ticker=TICKER, date=DATE,
            bulk_data_override=bulk_data,
        )

        # Assert no write methods called on stock_prices
        db.stock_prices.update_one.assert_not_awaited()
        db.stock_prices.bulk_write.assert_not_awaited()
        db.stock_prices.insert_one.assert_not_awaited()

        # Assert no write methods called on tracked_tickers
        db.tracked_tickers.update_one.assert_not_awaited()
        db.tracked_tickers.bulk_write.assert_not_awaited()

        # Report should show gap awaiting repair (not performed by proof-mode)
        assert result["bulk_check"]["found"] is True
        assert result["db_check"]["found"] is False
        assert result["remediation_evaluated_for_date"] is True
        assert result["remediation_reason"] == "proven_true_gap_awaiting_repair"
        assert "repair_result" not in result

    @pytest.mark.asyncio
    async def test_proof_mode_does_not_write_when_both_present(self):
        """When bulk and DB both have the row, proof-mode doesn't write."""
        oid = ObjectId()
        bulk_data = [_make_bulk_row("BODI", close=11.0)]
        db = _make_proof_db(
            stock_prices_rows=[
                {"_id": oid, "ticker": TICKER, "date": DATE, "close": 11.0}
            ],
            tracked_tickers_doc=SEEDED_DOC,
        )

        result = await run_proof_mode(
            db, ticker=TICKER, date=DATE,
            bulk_data_override=bulk_data,
        )

        db.stock_prices.update_one.assert_not_awaited()
        db.stock_prices.bulk_write.assert_not_awaited()
        db.tracked_tickers.update_one.assert_not_awaited()
        db.tracked_tickers.bulk_write.assert_not_awaited()
        assert result["db_check"]["found"] is True
        assert "CONSISTENT" in result["summary"]
        assert "repair_result" not in result


# ===========================================================================
# Test 2: Step 2 direct repair persists outcome on tracked_tickers
# ===========================================================================
class TestStep2PersistsRemediationOutcome:
    """Step 2 bulk catchup must persist last_remediation_action on tracked_tickers."""

    def test_step2_repair_persists_gap_repaired(self):
        """
        BODI.US in bulk with close>0, not in Step 1 override (simulates skip).
        After Step 2, tracked_tickers must have
        last_remediation_action="gap_repaired_from_bulk_row".
        """
        bulk = [
            _make_bulk_row("AAPL", close=150.0),
            _make_bulk_row("BODI", close=3.25),
        ]
        db = _FakeStep2DB(step2_tickers={"AAPL.US"})

        result = _run(
            price_ingestion_service.run_daily_bulk_catchup(
                db,
                latest_trading_day=DATE,
                bulk_data_override=bulk,
                seeded_tickers_override={"AAPL.US"},
            )
        )

        assert result["status"] == "success"
        assert result["auto_remediated_tickers_count"] == 1
        assert "BODI.US" in result["auto_remediated_tickers"]

        # stock_prices must have the repair write
        repair_tickers = {
            op._filter["ticker"]
            for op in db.stock_prices.writes
            if op._filter.get("ticker") == "BODI.US"
        }
        assert "BODI.US" in repair_tickers

        # tracked_tickers must have the persistence write with remediation fields
        persist_writes = db.tracked_tickers.writes
        assert len(persist_writes) > 0
        # Find the persist op for BODI.US
        bodi_ops = [
            op for op in persist_writes
            if op._filter.get("ticker") == "BODI.US"
        ]
        assert len(bodi_ops) == 1, (
            f"Expected exactly 1 persist op for BODI.US, got {len(bodi_ops)}"
        )
        persisted_set = bodi_ops[0]._doc.get("$set", {})
        assert persisted_set.get("last_remediation_action") == "gap_repaired_from_bulk_row"
        assert persisted_set.get("last_remediation_reason") == "missing_row_written_from_bulk"
        assert persisted_set.get("last_remediation_date") == DATE


# ===========================================================================
# Test 3: After Step 2, proof-mode reads persisted action from tracked_tickers
# ===========================================================================
class TestProofModeReadsPersistedAction:
    """
    Integration: run Step 2 → then proof-mode.
    Proof-mode must show the persisted remediation_action without writing.
    """

    @pytest.mark.asyncio
    async def test_proof_mode_shows_persisted_action_after_step2(self):
        """
        After Step 2 repairs BODI.US, proof-mode for that ticker+date must
        show remediation_action="gap_repaired_from_bulk_row" (read from
        tracked_tickers) and db_check.found=True.
        """
        oid = ObjectId()
        bulk_data = [_make_bulk_row("BODI", close=11.0)]

        # Simulate post-repair state: DB row exists, tracked_tickers has
        # persisted remediation outcome from Step 2.
        seeded_with_remediation = {
            **SEEDED_DOC,
            "last_remediation_action": "gap_repaired_from_bulk_row",
            "last_remediation_reason": "missing_row_written_from_bulk",
            "last_remediation_date": DATE,
        }
        db = _make_proof_db(
            stock_prices_rows=[
                {"_id": oid, "ticker": TICKER, "date": DATE, "close": 11.0}
            ],
            tracked_tickers_doc=seeded_with_remediation,
        )

        result = await run_proof_mode(
            db, ticker=TICKER, date=DATE,
            bulk_data_override=bulk_data,
        )

        # No writes happened
        db.stock_prices.update_one.assert_not_awaited()
        db.stock_prices.bulk_write.assert_not_awaited()
        db.tracked_tickers.update_one.assert_not_awaited()

        # Acceptance criteria
        assert result["db_check"]["found"] is True
        assert result["remediation_evaluated_for_date"] is True
        assert result["remediation_action"] == "gap_repaired_from_bulk_row"
        assert "CONSISTENT" in result["summary"]


# ===========================================================================
# Test 4: Step 2 repair failure persists "gap_repair_failed"
# ===========================================================================
class TestStep2PersistsFailureOutcome:
    """When Step 2 direct repair fails, tracked_tickers must have
    last_remediation_action="gap_repair_failed"."""

    def test_step2_failure_persists_on_tracked_tickers(self):
        """
        Simulate stock_prices.bulk_write raising an exception during repair.
        Fallback reflag must include last_remediation_action="gap_repair_failed".
        """
        bulk = [
            _make_bulk_row("AAPL", close=150.0),
            _make_bulk_row("BODI", close=3.25),
        ]
        db = _FakeStep2DB(step2_tickers={"AAPL.US"})

        # Make the second bulk_write (repair) fail
        _call_count = [0]
        _orig_bulk_write = db.stock_prices.bulk_write

        async def _fail_on_repair(batch, ordered=False):
            _call_count[0] += 1
            if _call_count[0] == 1:
                # First call: normal batch write for AAPL
                return await _orig_bulk_write(batch, ordered=ordered)
            # Second call: repair write for BODI — fail
            raise Exception("simulated repair write failure")

        db.stock_prices.bulk_write = _fail_on_repair

        result = _run(
            price_ingestion_service.run_daily_bulk_catchup(
                db,
                latest_trading_day=DATE,
                bulk_data_override=bulk,
                seeded_tickers_override={"AAPL.US"},
            )
        )

        assert result["status"] == "success"
        # Repair failed → fell back to reflag
        assert result["auto_reflagged_tickers_count"] > 0

        # Verify tracked_tickers reflag ops include failure fields
        reflag_ops = db.tracked_tickers.writes
        bodi_ops = [
            op for op in reflag_ops
            if op._filter.get("ticker") == "BODI.US"
               or (op._filter.get("ticker", {}).get("$in") and "BODI.US" in op._filter["ticker"]["$in"])
        ]
        # The reflag op must have last_remediation_action
        found_failure = False
        for op in reflag_ops:
            doc_set = op._doc.get("$set", {})
            if doc_set.get("last_remediation_action") == "gap_repair_failed":
                found_failure = True
                assert "last_remediation_reason" in doc_set
                assert doc_set.get("last_remediation_date") == DATE
                break
        assert found_failure, (
            "Expected last_remediation_action='gap_repair_failed' in "
            f"tracked_tickers writes, got: {reflag_ops}"
        )


# ===========================================================================
# Test 5: Non-gap scenarios — existing behavior preserved
# ===========================================================================
class TestExistingBehaviorPreserved:

    @pytest.mark.asyncio
    async def test_zero_close_no_repair(self):
        """Bulk close=0 → expected gap, no repair, proof-mode read-only."""
        bulk_data = [_make_bulk_row("BODI", close=0)]
        db = _make_proof_db(
            tracked_tickers_doc=SEEDED_DOC,
            ops_job_runs_doc=OPS_DOC,
        )

        result = await run_proof_mode(
            db, ticker=TICKER, date=DATE,
            bulk_data_override=bulk_data,
        )

        db.stock_prices.update_one.assert_not_awaited()
        assert result["remediation_reason"] == "bulk_close_is_zero_no_repair"

    @pytest.mark.asyncio
    async def test_not_seeded_no_repair(self):
        """Ticker not seeded → no repair."""
        bulk_data = [_make_bulk_row("BODI", close=11.0)]
        db = _make_proof_db(
            tracked_tickers_doc=None,
            ops_job_runs_doc=OPS_DOC,
        )
        db.tracked_tickers.find_one = AsyncMock(return_value=None)

        result = await run_proof_mode(
            db, ticker=TICKER, date=DATE,
            bulk_data_override=bulk_data,
        )

        db.stock_prices.update_one.assert_not_awaited()
        assert result["remediation_reason"] == "ticker_not_currently_seeded"
