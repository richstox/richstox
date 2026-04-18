"""
Tests for PR12 — deterministic repair of proven true-gap.

A proven true-gap exists when:
  - ticker is seeded (is_seeded=True in tracked_tickers)
  - bulk row exists for the date with canonical close > 0
  - stock_prices row for (ticker, date) is missing

The repair writes the missing stock_prices row directly from bulk data.

Cases:
  1. Proven true-gap → row is written, action = gap_repaired_from_bulk_row
  2. close == 0 → no repair
  3. bulk_found = false → no repair
  4. DB row already exists → no-op
  5. Repeated repair call remains idempotent
  6. Ticker not seeded → no repair
"""

import asyncio
import os
import sys
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Set
from unittest.mock import AsyncMock

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import price_ingestion_service


# ---------------------------------------------------------------------------
# Fake DB layer
# ---------------------------------------------------------------------------
class _FakeTrackedTickers:
    """Supports find_one and update_one."""

    def __init__(self, docs: Optional[List[Dict[str, Any]]] = None):
        self._docs = docs or []
        self.update_calls: List[Any] = []

    async def find_one(self, filt, projection=None):
        for doc in self._docs:
            match = True
            for k, v in filt.items():
                if doc.get(k) != v:
                    match = False
                    break
            if match:
                return dict(doc)
        return None

    async def update_one(self, filt, update, **kwargs):
        self.update_calls.append({"filter": filt, "update": update})
        return SimpleNamespace(modified_count=1)


class _FakeStockPrices:
    """Tracks writes and supports find_one for existence check."""

    def __init__(self, existing_rows: Optional[List[Dict[str, Any]]] = None):
        self._existing = list(existing_rows or [])
        self.upsert_calls: List[Dict[str, Any]] = []

    async def find_one(self, filt, projection=None):
        for row in self._existing:
            match = all(row.get(k) == v for k, v in filt.items() if k != "_id")
            if match:
                return row
        return None

    async def update_one(self, filt, update, upsert=False):
        self.upsert_calls.append({
            "filter": filt,
            "update": update,
            "upsert": upsert,
        })
        return SimpleNamespace(upserted_count=1 if upsert else 0, modified_count=0)


class _FakeDB:
    def __init__(
        self,
        tracked_tickers_docs=None,
        existing_stock_prices=None,
    ):
        self.tracked_tickers = _FakeTrackedTickers(tracked_tickers_docs)
        self.stock_prices = _FakeStockPrices(existing_stock_prices)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
DATE = "2026-04-15"


def _make_bulk_row(code: str, close: float = 11.0) -> Dict[str, Any]:
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
# Test 1: Proven true-gap → row is written
# ---------------------------------------------------------------------------
def test_proven_true_gap_repaired_from_bulk():
    """
    BODI.US: seeded=True, bulk has close=11 (>0), no DB row.
    Repair must write the row and return gap_repaired_from_bulk_row.
    """
    bulk = [_make_bulk_row("BODI", close=11.0)]
    db = _FakeDB(
        tracked_tickers_docs=[
            {"ticker": "BODI.US", "is_seeded": True},
        ],
    )

    result = _run(
        price_ingestion_service.repair_proven_true_gap(
            db, ticker="BODI.US", date=DATE,
            bulk_data_override=bulk,
        )
    )

    assert result["status"] == "repaired"
    assert result["remediation_action"] == "gap_repaired_from_bulk_row"
    assert result["remediation_evaluated_for_date"] is True
    assert result["bulk_found"] is True
    assert result["bulk_close"] == 11.0

    # Verify the stock_prices upsert was called
    assert len(db.stock_prices.upsert_calls) == 1
    written = db.stock_prices.upsert_calls[0]
    assert written["filter"]["ticker"] == "BODI.US"
    assert written["filter"]["date"] == DATE
    assert written["upsert"] is True
    assert written["update"]["$set"]["close"] == 11.0

    # No reflag to tracked_tickers (needs_price_redownload not set).
    # There IS a persistence write for last_remediation_action.
    reflag_calls = [
        c for c in db.tracked_tickers.update_calls
        if c["update"]["$set"].get("needs_price_redownload") is True
    ]
    assert len(reflag_calls) == 0

    # But we DO expect a persistence write for the remediation outcome
    persist_calls = [
        c for c in db.tracked_tickers.update_calls
        if c["update"]["$set"].get("last_remediation_action") == "gap_repaired_from_bulk_row"
    ]
    assert len(persist_calls) == 1


# ---------------------------------------------------------------------------
# Test 2: close == 0 → no repair
# ---------------------------------------------------------------------------
def test_no_repair_when_close_is_zero():
    """
    Bulk has close=0 → halted/delisted. Repair must NOT trigger.
    """
    bulk = [_make_bulk_row("BODI", close=0)]
    db = _FakeDB(
        tracked_tickers_docs=[
            {"ticker": "BODI.US", "is_seeded": True},
        ],
    )

    result = _run(
        price_ingestion_service.repair_proven_true_gap(
            db, ticker="BODI.US", date=DATE,
            bulk_data_override=bulk,
        )
    )

    assert result["status"] == "skipped"
    assert result["remediation_action"] is None
    assert result["reason"] == "bulk_close_is_zero_or_missing"
    assert len(db.stock_prices.upsert_calls) == 0
    assert len(db.tracked_tickers.update_calls) == 0


# ---------------------------------------------------------------------------
# Test 3: bulk_found = false → no repair
# ---------------------------------------------------------------------------
def test_no_repair_when_bulk_row_absent():
    """
    Ticker not in bulk payload → no evidence of gap. No repair.
    """
    bulk = [_make_bulk_row("AAPL", close=150.0)]  # BODI absent
    db = _FakeDB(
        tracked_tickers_docs=[
            {"ticker": "BODI.US", "is_seeded": True},
        ],
    )

    result = _run(
        price_ingestion_service.repair_proven_true_gap(
            db, ticker="BODI.US", date=DATE,
            bulk_data_override=bulk,
        )
    )

    assert result["status"] == "skipped"
    assert result["remediation_action"] is None
    assert result["reason"] == "bulk_row_not_found"
    assert result["bulk_found"] is False
    assert len(db.stock_prices.upsert_calls) == 0


# ---------------------------------------------------------------------------
# Test 4: DB row already exists → no-op
# ---------------------------------------------------------------------------
def test_no_op_when_db_row_exists():
    """
    stock_prices already has the row → idempotent no-op.
    """
    bulk = [_make_bulk_row("BODI", close=11.0)]
    db = _FakeDB(
        tracked_tickers_docs=[
            {"ticker": "BODI.US", "is_seeded": True},
        ],
        existing_stock_prices=[
            {"ticker": "BODI.US", "date": DATE, "close": 11.0},
        ],
    )

    result = _run(
        price_ingestion_service.repair_proven_true_gap(
            db, ticker="BODI.US", date=DATE,
            bulk_data_override=bulk,
        )
    )

    assert result["status"] == "no_op"
    assert result["remediation_action"] is None
    assert result["reason"] == "db_row_already_exists"
    assert result["db_found"] is True
    assert len(db.stock_prices.upsert_calls) == 0


# ---------------------------------------------------------------------------
# Test 5: Repeated repair call remains idempotent
# ---------------------------------------------------------------------------
def test_idempotent_repeated_repair():
    """
    First call writes the row. Second call should be a no-op because
    the DB row now exists (simulated by pre-seeding).
    """
    bulk = [_make_bulk_row("BODI", close=11.0)]

    # First call: no existing row → repair
    db1 = _FakeDB(
        tracked_tickers_docs=[
            {"ticker": "BODI.US", "is_seeded": True},
        ],
    )
    r1 = _run(
        price_ingestion_service.repair_proven_true_gap(
            db1, ticker="BODI.US", date=DATE,
            bulk_data_override=bulk,
        )
    )
    assert r1["status"] == "repaired"
    assert r1["remediation_action"] == "gap_repaired_from_bulk_row"

    # Second call: row exists → no-op
    db2 = _FakeDB(
        tracked_tickers_docs=[
            {"ticker": "BODI.US", "is_seeded": True},
        ],
        existing_stock_prices=[
            {"ticker": "BODI.US", "date": DATE, "close": 11.0},
        ],
    )
    r2 = _run(
        price_ingestion_service.repair_proven_true_gap(
            db2, ticker="BODI.US", date=DATE,
            bulk_data_override=bulk,
        )
    )
    assert r2["status"] == "no_op"
    assert r2["remediation_action"] is None
    assert len(db2.stock_prices.upsert_calls) == 0


# ---------------------------------------------------------------------------
# Test 6: Ticker not seeded → no repair
# ---------------------------------------------------------------------------
def test_no_repair_when_ticker_not_seeded():
    """
    Ticker exists in bulk but is_seeded=False (or not in tracked_tickers).
    Repair must NOT trigger.
    """
    bulk = [_make_bulk_row("BODI", close=11.0)]
    db = _FakeDB(
        tracked_tickers_docs=[
            {"ticker": "BODI.US", "is_seeded": False},
        ],
    )

    result = _run(
        price_ingestion_service.repair_proven_true_gap(
            db, ticker="BODI.US", date=DATE,
            bulk_data_override=bulk,
        )
    )

    assert result["status"] == "skipped"
    assert result["remediation_action"] is None
    assert result["reason"] == "ticker_not_seeded"
    assert len(db.stock_prices.upsert_calls) == 0


# ---------------------------------------------------------------------------
# Test 7: Direct repair in run_daily_bulk_catchup writes stock_prices
# ---------------------------------------------------------------------------
def test_bulk_catchup_direct_repair_writes_stock_prices():
    """
    Verify that the Step 2 auto-remediation now writes directly to
    stock_prices instead of just reflagging tracked_tickers.
    """
    from tests.test_bulk_row_auto_remediation import (
        _FakeDB as _BulkFakeDB,
        _make_bulk_row as _bulk_make_row,
        DATE as BULK_DATE,
    )

    bulk = [
        _bulk_make_row("AAPL", close=150.0),
        _bulk_make_row("BODI", close=3.25),
    ]
    db = _BulkFakeDB(step2_tickers={"AAPL.US"})

    result = _run(
        price_ingestion_service.run_daily_bulk_catchup(
            db,
            latest_trading_day=BULK_DATE,
            bulk_data_override=bulk,
            seeded_tickers_override={"AAPL.US"},
        )
    )

    assert result["status"] == "success"
    assert result["auto_remediated_tickers_count"] == 1
    assert "BODI.US" in result["auto_remediated_tickers"]
    # The auto_reflagged count should be 0 (direct repair, not fallback)
    assert result.get("auto_reflagged_tickers_count", 0) == 0
