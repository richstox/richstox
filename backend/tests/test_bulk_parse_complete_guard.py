"""
Tests for the bulk_parse_complete hard guard (PR15).

Validates that:
1. When bulk_parse_complete=False, no not_in_bulk_data exclusions are written.
2. When bulk_parse_complete=True (default), not_in_bulk_data exclusions work as before.
3. Zero-close exclusions are still written even when bulk_parse_complete=False.
4. run_daily_bulk_catchup returns bulk_parse_complete=True on successful parse
   and bulk_parse_complete=False on early-exit paths.
5. The summary dict includes bulk_parse_complete and skipped count.
"""

import asyncio
import os
import sys
from types import SimpleNamespace

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import scheduler_service


# ── Fake DB helpers (following project test patterns) ────────────────────────

class _FakeCursor:
    """Async cursor over a list of dicts."""
    def __init__(self, docs):
        self._docs = list(docs)
    async def to_list(self, _):
        return list(self._docs)
    def __aiter__(self):
        return _FakeCursorIter(self._docs)


class _FakeCursorIter:
    def __init__(self, docs):
        self._iter = iter(docs)
    async def __anext__(self):
        try:
            return next(self._iter)
        except StopIteration:
            raise StopAsyncIteration


class _FakeTrackedTickers:
    def __init__(self, tickers):
        self._tickers = list(tickers)
    def find(self, query, projection=None):
        return _FakeCursor([{"ticker": t, "name": t} for t in self._tickers])
    async def update_many(self, filt, update):
        return SimpleNamespace(modified_count=0)


class _FakeGapFreeExclusions:
    def __init__(self):
        self.ops = []
    async def bulk_write(self, ops, ordered=False):
        self.ops.extend(ops)
        return SimpleNamespace(upserted_count=len(ops))


class _FakeDB:
    def __init__(self, tickers):
        self.tracked_tickers = _FakeTrackedTickers(tickers)
        self.gap_free_exclusions = _FakeGapFreeExclusions()


def _get_excl_doc(op):
    """Extract $set doc from a pymongo UpdateOne operation."""
    return op._doc.get("$set", {})


# ── Tests ────────────────────────────────────────────────────────────────────

class TestBulkParseCompleteGuard:

    def test_incomplete_parse_blocks_not_in_bulk_exclusions(self):
        """When bulk_parse_complete=False, no not_in_bulk_data exclusions are written."""
        seeded = ["AAPL.US", "GHOST.US", "MISSING.US"]
        db = _FakeDB(seeded)

        summary = asyncio.run(
            scheduler_service.sync_has_price_data_flags(
                db,
                include_exclusions=False,
                tickers_with_price=["AAPL.US"],
                bulk_date="2026-04-15",
                bulk_zero_close_tickers=set(),
                bulk_zero_close_data={},
                bulk_parse_complete=False,
            )
        )

        # No exclusions should have been written at all
        assert len(db.gap_free_exclusions.ops) == 0
        # Summary reflects the guard
        assert summary["bulk_parse_complete"] is False
        assert summary["not_in_bulk_skipped_incomplete_parse"] == 2

    def test_complete_parse_writes_not_in_bulk_exclusions(self):
        """When bulk_parse_complete=True, not_in_bulk_data exclusions are written as before."""
        seeded = ["AAPL.US", "GHOST.US"]
        db = _FakeDB(seeded)

        summary = asyncio.run(
            scheduler_service.sync_has_price_data_flags(
                db,
                include_exclusions=False,
                tickers_with_price=["AAPL.US"],
                bulk_date="2026-04-15",
                bulk_zero_close_tickers=set(),
                bulk_zero_close_data={},
                bulk_parse_complete=True,
            )
        )

        assert len(db.gap_free_exclusions.ops) == 1
        doc = _get_excl_doc(db.gap_free_exclusions.ops[0])
        assert doc["ticker"] == "GHOST.US"
        assert doc["reason"] == "not_in_bulk_data"
        assert doc["bulk_found"] is False
        assert summary["bulk_parse_complete"] is True
        assert summary["not_in_bulk_skipped_incomplete_parse"] == 0

    def test_default_bulk_parse_complete_is_true(self):
        """Default value of bulk_parse_complete is True (backward compat)."""
        seeded = ["AAPL.US", "GONE.US"]
        db = _FakeDB(seeded)

        summary = asyncio.run(
            scheduler_service.sync_has_price_data_flags(
                db,
                include_exclusions=False,
                tickers_with_price=["AAPL.US"],
                bulk_date="2026-04-15",
            )
        )

        # Default = True → exclusions written
        assert len(db.gap_free_exclusions.ops) == 1
        assert summary["bulk_parse_complete"] is True

    def test_incomplete_parse_still_writes_zero_close_exclusions(self):
        """Zero-close exclusions are still written even when parse is incomplete
        because those tickers WERE seen in the partial parse."""
        seeded = ["AAPL.US", "BODI.US", "GHOST.US"]
        db = _FakeDB(seeded)

        summary = asyncio.run(
            scheduler_service.sync_has_price_data_flags(
                db,
                include_exclusions=False,
                tickers_with_price=["AAPL.US"],
                bulk_date="2026-04-15",
                bulk_zero_close_tickers={"BODI.US"},
                bulk_zero_close_data={
                    "BODI.US": {"close": 0, "adjusted_close": 0, "volume": 100},
                },
                bulk_parse_complete=False,
            )
        )

        # Only zero-close exclusion written, NOT the not_in_bulk for GHOST.US
        assert len(db.gap_free_exclusions.ops) == 1
        doc = _get_excl_doc(db.gap_free_exclusions.ops[0])
        assert doc["ticker"] == "BODI.US"
        assert doc["reason"] == "bulk_found_but_close_is_zero"
        assert doc["bulk_found"] is True
        # GHOST.US was suppressed
        assert summary["not_in_bulk_skipped_incomplete_parse"] == 1

    def test_complete_parse_writes_both_zero_close_and_not_in_bulk(self):
        """When parse is complete, both zero-close and not_in_bulk are written."""
        seeded = ["AAPL.US", "BODI.US", "GHOST.US"]
        db = _FakeDB(seeded)

        asyncio.run(
            scheduler_service.sync_has_price_data_flags(
                db,
                include_exclusions=False,
                tickers_with_price=["AAPL.US"],
                bulk_date="2026-04-15",
                bulk_zero_close_tickers={"BODI.US"},
                bulk_zero_close_data={
                    "BODI.US": {"close": 0, "adjusted_close": 0, "volume": 100},
                },
                bulk_parse_complete=True,
            )
        )

        assert len(db.gap_free_exclusions.ops) == 2
        docs_by_ticker = {
            _get_excl_doc(op)["ticker"]: _get_excl_doc(op)
            for op in db.gap_free_exclusions.ops
        }
        assert docs_by_ticker["BODI.US"]["reason"] == "bulk_found_but_close_is_zero"
        assert docs_by_ticker["GHOST.US"]["reason"] == "not_in_bulk_data"


class TestBulkParseCompleteInResult:
    """Verify that run_daily_bulk_catchup returns bulk_parse_complete field."""

    def test_cancelled_before_parse_returns_false(self):
        """Cancelled before parse → bulk_parse_complete=False."""
        import price_ingestion_service

        # Create a fake DB that cancels on first check
        class _FakeOpsConfig:
            def __init__(self):
                self._cancelled = False
            async def find_one(self, query):
                if not self._cancelled:
                    self._cancelled = True
                    return {"key": "cancel"}
                return None
            async def delete_one(self, query):
                pass

        class _MinimalDB:
            def __init__(self):
                self.ops_config = _FakeOpsConfig()

        db = _MinimalDB()
        result = asyncio.run(
            price_ingestion_service.run_daily_bulk_catchup(
                db, "test_job",
                latest_trading_day="2026-04-15",
                bulk_data_override=[{"code": "AAPL", "date": "2026-04-15", "close": 100}],
            )
        )
        assert result["status"] == "cancelled"
        assert result["bulk_parse_complete"] is False

    def test_no_data_returns_false(self):
        """Empty bulk data → bulk_parse_complete=False."""
        import price_ingestion_service

        class _NoCancelOpsConfig:
            async def find_one(self, query):
                return None
            async def delete_one(self, query):
                pass

        class _MinimalDB:
            def __init__(self):
                self.ops_config = _NoCancelOpsConfig()

        db = _MinimalDB()
        result = asyncio.run(
            price_ingestion_service.run_daily_bulk_catchup(
                db, "test_job",
                latest_trading_day="2026-04-15",
                bulk_data_override=[],
            )
        )
        assert result["bulk_parse_complete"] is False
        assert result.get("bulk_parse_incomplete_reason") == "no_data_returned"

    def test_success_returns_true(self):
        """Successful parse → bulk_parse_complete=True."""
        import price_ingestion_service

        class _NoCancelOpsConfig:
            async def find_one(self, query):
                return None
            async def delete_one(self, query):
                pass

        class _FakeBulkWriteResult:
            upserted_count = 1
            modified_count = 0

        class _FakeStockPrices:
            async def bulk_write(self, ops, ordered=False):
                return _FakeBulkWriteResult()
            async def find_one(self, query, *args, **kwargs):
                return None

        class _FakeTracked:
            def __init__(self):
                self._tickers = ["AAPL.US"]
            async def distinct(self, field, query):
                return self._tickers
            def find(self, query, projection=None):
                return _FakeCursor([{"ticker": t} for t in self._tickers])
            async def update_many(self, filt, update):
                return SimpleNamespace(modified_count=0)
            async def find_one(self, query, *args, **kwargs):
                return None

        class _MinimalDB:
            def __init__(self):
                self.ops_config = _NoCancelOpsConfig()
                self.stock_prices = _FakeStockPrices()
                self.tracked_tickers = _FakeTracked()
            def __getattr__(self, name):
                # Return a minimal mock for any other collection access
                return SimpleNamespace(
                    find_one=lambda *a, **kw: asyncio.coroutine(lambda: None)(),
                )

        db = _MinimalDB()
        result = asyncio.run(
            price_ingestion_service.run_daily_bulk_catchup(
                db, "test_job",
                seeded_tickers_override={"AAPL.US"},
                latest_trading_day="2026-04-15",
                bulk_data_override=[
                    {"code": "AAPL", "date": "2026-04-15", "close": 150.0,
                     "open": 148.0, "high": 151.0, "low": 147.0,
                     "adjusted_close": 150.0, "volume": 1000000},
                ],
            )
        )
        assert result["status"] == "success"
        assert result["bulk_parse_complete"] is True
