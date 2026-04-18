"""
Tests for auto-remediation of the proven true-gap case:
  bulk row exists with positive price, but DB row is still missing.

Scenario: A seeded ticker appears in the EODHD bulk payload with close > 0,
but after the normal write phase completes the stock_prices row is absent
(e.g. the ticker was not in the Step 1 override set).  The remediation
must flag it for Phase C redownload.

Cases:
  1. Positive bulk price + simulated write skip → remediation triggers
  2. Bulk price == 0 → NOT triggered
  3. Bulk row absent → NOT triggered
  4. DB row exists after write phase → NOT triggered
"""

import asyncio
import os
import sys
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Set

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import price_ingestion_service


# ---------------------------------------------------------------------------
# Fake DB layer
# ---------------------------------------------------------------------------
class _FakeOpsConfig:
    async def find_one(self, filt):
        return None

    async def delete_one(self, filt):
        return SimpleNamespace(deleted_count=0)


class _FakeCursor:
    """Async cursor returned by find()."""

    def __init__(self, rows: List[Dict[str, Any]]):
        self._rows = rows

    async def to_list(self, length=None):
        return list(self._rows)


class _FakeTrackedTickers:
    """Supports distinct() for Step 2 universe AND bulk_write() for remediation reflag."""

    def __init__(self, step2_tickers: Set[str]):
        # step2_tickers: passed via seeded_tickers_override (Step 1 set).
        self._step2 = set(step2_tickers)
        self.reflag_writes: List[Any] = []

    async def distinct(self, field, query):
        return list(self._step2)

    async def bulk_write(self, ops, ordered=False):
        self.reflag_writes.extend(ops)
        return SimpleNamespace(
            upserted_count=0,
            modified_count=len(ops),
        )


class _FakeStockPrices:
    """Tracks bulk_write calls and allows pre-seeding existing rows."""

    def __init__(self, existing_rows: Optional[List[Dict[str, Any]]] = None):
        self.writes: List[Any] = []
        self._existing = existing_rows or []

    async def bulk_write(self, batch, ordered=False):
        self.writes.extend(batch)
        return SimpleNamespace(upserted_count=len(batch), modified_count=0)

    def find(self, query, projection=None):
        """Return matching rows from pre-seeded data."""
        tickers = query.get("ticker", {}).get("$in", [])
        date_val = query.get("date")
        matched = [
            r for r in self._existing
            if r.get("ticker") in tickers and r.get("date") == date_val
        ]
        return _FakeCursor(matched)


class _FakeDB:
    def __init__(
        self,
        step2_tickers: Set[str],
        existing_stock_prices: Optional[List[Dict[str, Any]]] = None,
    ):
        self.ops_config = _FakeOpsConfig()
        self.tracked_tickers = _FakeTrackedTickers(step2_tickers)
        self.stock_prices = _FakeStockPrices(existing_stock_prices)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
DATE = "2026-04-15"


def _make_bulk_row(code: str, close: float = 5.0) -> Dict[str, Any]:
    return {
        "code": code,
        "date": DATE,
        "open": close - 0.5,
        "high": close + 0.5,
        "low": close - 1.0,
        "close": close,
        "adjusted_close": close,
        "volume": 10000,
    }


def _run(coro):
    return asyncio.run(coro)


# ---------------------------------------------------------------------------
# Test 1: Remediation triggers — positive bulk price + write skip
# ---------------------------------------------------------------------------
def test_remediation_triggers_for_positive_bulk_price_write_skip():
    """
    BODI.US is present in this run's bulk with close > 0, but NOT in the
    Step 1 override set → not written during normal write phase.
    After the write phase, the DB row is missing → remediation MUST trigger.
    Candidate set is derived from bulk data only (not a tracked_tickers sweep).
    """
    bulk = [
        _make_bulk_row("AAPL", close=150.0),
        _make_bulk_row("BODI", close=3.25),  # positive price, in bulk
    ]
    db = _FakeDB(
        step2_tickers={"AAPL.US"},          # Step 1 only has AAPL
    )

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

    # Direct repair writes the missing row to stock_prices (not reflag).
    # The stock_prices.writes list contains both the normal AAPL write and
    # the BODI repair write.
    repair_tickers = {
        op._filter["ticker"]
        for op in db.stock_prices.writes
        if op._filter.get("ticker") == "BODI.US"
    }
    assert "BODI.US" in repair_tickers

    # No reflag to tracked_tickers — direct repair succeeded
    assert len(db.tracked_tickers.reflag_writes) == 0


# ---------------------------------------------------------------------------
# Test 2: NOT triggered when bulk price == 0
# ---------------------------------------------------------------------------
def test_no_remediation_when_bulk_price_is_zero():
    """
    BODI.US is in this run's bulk but close=0 (halted/delisted).
    Remediation must NOT trigger — zero-price is an expected exclusion.
    """
    bulk = [
        _make_bulk_row("AAPL", close=150.0),
        _make_bulk_row("BODI", close=0),  # zero price
    ]
    db = _FakeDB(
        step2_tickers={"AAPL.US"},
    )

    result = _run(
        price_ingestion_service.run_daily_bulk_catchup(
            db,
            latest_trading_day=DATE,
            bulk_data_override=bulk,
            seeded_tickers_override={"AAPL.US"},
        )
    )

    assert result["status"] == "success"
    assert result["auto_remediated_tickers_count"] == 0
    assert result["auto_remediated_tickers"] == []
    assert len(db.tracked_tickers.reflag_writes) == 0


# ---------------------------------------------------------------------------
# Test 3: NOT triggered when bulk row is absent
# ---------------------------------------------------------------------------
def test_no_remediation_when_bulk_row_absent():
    """
    BODI.US is NOT present in this run's bulk payload at all.
    Remediation must NOT trigger — no bulk evidence means no gap.
    """
    bulk = [
        _make_bulk_row("AAPL", close=150.0),
        # BODI is NOT in bulk at all
    ]
    db = _FakeDB(
        step2_tickers={"AAPL.US"},
    )

    result = _run(
        price_ingestion_service.run_daily_bulk_catchup(
            db,
            latest_trading_day=DATE,
            bulk_data_override=bulk,
            seeded_tickers_override={"AAPL.US"},
        )
    )

    assert result["status"] == "success"
    assert result["auto_remediated_tickers_count"] == 0
    assert result["auto_remediated_tickers"] == []
    assert len(db.tracked_tickers.reflag_writes) == 0


# ---------------------------------------------------------------------------
# Test 4: NOT triggered when DB row already exists
# ---------------------------------------------------------------------------
def test_no_remediation_when_db_row_exists():
    """
    BODI.US is in this run's bulk with close > 0, was NOT in the Step 1
    override, BUT a stock_prices row already exists (from a prior backfill
    or Phase C run).  Remediation must NOT trigger.
    """
    bulk = [
        _make_bulk_row("AAPL", close=150.0),
        _make_bulk_row("BODI", close=3.25),
    ]
    db = _FakeDB(
        step2_tickers={"AAPL.US"},
        existing_stock_prices=[
            {"ticker": "BODI.US", "date": DATE, "close": 3.25},
        ],
    )

    result = _run(
        price_ingestion_service.run_daily_bulk_catchup(
            db,
            latest_trading_day=DATE,
            bulk_data_override=bulk,
            seeded_tickers_override={"AAPL.US"},
        )
    )

    assert result["status"] == "success"
    assert result["auto_remediated_tickers_count"] == 0
    assert result["auto_remediated_tickers"] == []
    assert len(db.tracked_tickers.reflag_writes) == 0
