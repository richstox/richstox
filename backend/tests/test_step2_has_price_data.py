"""
Tests for sync_has_price_data_flags — verifies that the fast path (when
tickers_with_price is provided) also checks stock_prices for historical
price data of seeded tickers not in today's bulk feed.
"""

import asyncio
import os
import sys
from copy import deepcopy
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import scheduler_service


# ---------------------------------------------------------------------------
# Fake DB components
# ---------------------------------------------------------------------------

class _FakeCursor:
    def __init__(self, docs):
        self._docs = docs

    async def to_list(self, _):
        return deepcopy(self._docs)


class _FakeTrackedTickers:
    """Tracks find + update_many calls so we can assert has_price_data writes."""

    def __init__(self, docs):
        self._docs = list(docs)
        self.updates: list = []  # list of (filter, update)

    def find(self, query, projection):
        _ = query, projection
        return _FakeCursor(deepcopy(self._docs))

    async def update_many(self, filt, update):
        self.updates.append((deepcopy(filt), deepcopy(update)))
        return SimpleNamespace(modified_count=len(self._docs))


class _FakeStockPrices:
    """Return configured tickers from distinct() based on query filter."""

    def __init__(self, valid_tickers=None, any_tickers=None):
        self._valid_tickers = set(valid_tickers or [])
        self._any_tickers = set(any_tickers or []) | self._valid_tickers

    async def distinct(self, field, query):
        candidates = set(query.get("ticker", {}).get("$in", []))
        if "$or" in query:
            # Valid price query (close > 0 OR adjusted_close > 0)
            return sorted(candidates & self._valid_tickers)
        else:
            # "any price" query (all records regardless of close)
            return sorted(candidates & self._any_tickers)


class _FakeDB:
    def __init__(self, tracked_docs, valid_price_tickers=None, any_price_tickers=None):
        self.tracked_tickers = _FakeTrackedTickers(tracked_docs)
        self.stock_prices = _FakeStockPrices(
            valid_tickers=valid_price_tickers,
            any_tickers=any_price_tickers,
        )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _seeded_doc(ticker: str, name: str = "(unknown)") -> dict:
    return {
        "ticker": ticker,
        "name": name,
        "exchange": "NYSE",
        "asset_type": "Common Stock",
        "is_seeded": True,
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestSyncHasPriceDataFlagsHistoricalPrices:
    """
    Regression test: tickers with historical prices in stock_prices but NOT in
    today's bulk feed must still get has_price_data=True.
    """

    @pytest.mark.asyncio
    async def test_historical_prices_counted_when_not_in_bulk_feed(self):
        """
        BFLY.US has prices in stock_prices but is NOT in tickers_with_price
        (today's bulk feed).  It must still be counted as with_price.
        """
        docs = [
            _seeded_doc("AAPL.US", "Apple"),
            _seeded_doc("BFLY.US", "Butterfly Network"),
            _seeded_doc("MSFT.US", "Microsoft"),
        ]
        # BFLY has valid historical prices in stock_prices
        db = _FakeDB(
            tracked_docs=docs,
            valid_price_tickers={"BFLY.US", "BFLY"},
            any_price_tickers={"BFLY.US", "BFLY"},
        )

        # Only AAPL and MSFT are in today's bulk feed
        result = await scheduler_service.sync_has_price_data_flags(
            db,
            include_exclusions=True,
            tickers_with_price=["AAPL.US", "MSFT.US"],
        )

        assert result["seeded_total"] == 3
        assert result["with_price_data"] == 3  # all three have prices
        assert result["without_price_data"] == 0
        assert result.get("exclusions") == []

    @pytest.mark.asyncio
    async def test_no_historical_prices_still_excluded(self):
        """
        A ticker NOT in today's bulk feed and NOT in stock_prices must be
        excluded (has_price_data=False).
        """
        docs = [
            _seeded_doc("AAPL.US", "Apple"),
            _seeded_doc("NEWT.US", "New Ticker"),
        ]
        # NEWT has no price data at all
        db = _FakeDB(
            tracked_docs=docs,
            valid_price_tickers=set(),
            any_price_tickers=set(),
        )

        result = await scheduler_service.sync_has_price_data_flags(
            db,
            include_exclusions=True,
            tickers_with_price=["AAPL.US"],
        )

        assert result["seeded_total"] == 2
        assert result["with_price_data"] == 1
        assert result["without_price_data"] == 1
        exclusions = result["exclusions"]
        assert len(exclusions) == 1
        assert exclusions[0]["ticker"] == "NEWT"

    @pytest.mark.asyncio
    async def test_bulk_feed_only_still_works(self):
        """
        When all seeded tickers are in the bulk feed, no stock_prices query
        is needed (remaining set is empty).
        """
        docs = [
            _seeded_doc("AAPL.US", "Apple"),
            _seeded_doc("MSFT.US", "Microsoft"),
        ]
        db = _FakeDB(
            tracked_docs=docs,
            valid_price_tickers=set(),
        )

        result = await scheduler_service.sync_has_price_data_flags(
            db,
            include_exclusions=True,
            tickers_with_price=["AAPL.US", "MSFT.US"],
        )

        assert result["seeded_total"] == 2
        assert result["with_price_data"] == 2
        assert result["without_price_data"] == 0

    @pytest.mark.asyncio
    async def test_legacy_fallback_still_works(self):
        """
        When tickers_with_price is None, the legacy stock_prices path must
        still work correctly.
        """
        docs = [
            _seeded_doc("AAPL.US", "Apple"),
            _seeded_doc("BFLY.US", "Butterfly Network"),
        ]
        db = _FakeDB(
            tracked_docs=docs,
            valid_price_tickers={"AAPL.US", "AAPL", "BFLY.US", "BFLY"},
            any_price_tickers={"AAPL.US", "AAPL", "BFLY.US", "BFLY"},
        )

        result = await scheduler_service.sync_has_price_data_flags(
            db,
            include_exclusions=True,
            tickers_with_price=None,
        )

        assert result["seeded_total"] == 2
        assert result["with_price_data"] == 2
        assert result["without_price_data"] == 0

    @pytest.mark.asyncio
    async def test_exclusion_reason_historical_zero_close(self):
        """
        A ticker not in bulk feed with price rows but all close==0 must get
        the 'Close/adjusted_close missing or zero' exclusion reason.
        """
        docs = [
            _seeded_doc("AAPL.US", "Apple"),
            _seeded_doc("JUNK.US", "Junk Co"),
        ]
        # JUNK has rows in stock_prices but no valid close (close==0)
        db = _FakeDB(
            tracked_docs=docs,
            valid_price_tickers=set(),           # no valid close
            any_price_tickers={"JUNK.US", "JUNK"},  # has rows
        )

        result = await scheduler_service.sync_has_price_data_flags(
            db,
            include_exclusions=True,
            tickers_with_price=["AAPL.US"],
        )

        assert result["with_price_data"] == 1
        exclusions = result["exclusions"]
        assert len(exclusions) == 1
        assert exclusions[0]["ticker"] == "JUNK"
        assert "Close/adjusted_close" in exclusions[0]["reason"]
