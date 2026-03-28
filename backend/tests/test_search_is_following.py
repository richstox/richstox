#!/usr/bin/env python3
"""
Test: search_whitelist is_following annotation
==============================================

Verifies that search_whitelist correctly annotates results
with is_following when a followed_tickers set is provided,
and omits the field when it is not.
"""

import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
from unittest.mock import AsyncMock, MagicMock


# ---------------------------------------------------------------------------
# Helpers – lightweight fakes that satisfy the search_whitelist code path
# ---------------------------------------------------------------------------

class _FakeCursor:
    """Mimics Motor aggregate cursor with a canned result list."""

    def __init__(self, docs):
        self._docs = list(docs)

    async def to_list(self, length=None):
        return self._docs


def _make_fake_db(tracked_docs):
    """Return a fake db object whose tracked_tickers.aggregate returns *tracked_docs*."""
    db = MagicMock()
    db.tracked_tickers.aggregate = MagicMock(return_value=_FakeCursor(tracked_docs))
    return db


# Raw docs as the aggregation pipeline would return (after $project)
_RAW_DOCS = [
    {"ticker": "KO.US",   "name": "The Coca-Cola Company",     "exchange": "NYSE",   "sector": "Consumer Defensive", "industry": "Beverages", "asset_type": "Common Stock", "status": "active", "safety_type": "standard", "logo": None, "rank": 0},
    {"ticker": "KOD.US",  "name": "Kodiak Sciences Inc",       "exchange": "NASDAQ", "sector": "Healthcare",         "industry": "Biotech",   "asset_type": "Common Stock", "status": "active", "safety_type": "standard", "logo": None, "rank": 1},
    {"ticker": "KODK.US", "name": "Eastman Kodak Co",          "exchange": "NYSE",   "sector": "Technology",         "industry": "Imaging",   "asset_type": "Common Stock", "status": "active", "safety_type": "standard", "logo": None, "rank": 1},
]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_is_following_present_when_set_provided():
    """Results should include is_following=True/False when followed_tickers is given."""
    from whitelist_service import search_whitelist

    db = _make_fake_db(_RAW_DOCS)
    followed = {"KO", "KODK"}

    results = await search_whitelist(db, "KO", limit=20, followed_tickers=followed)

    assert len(results) == 3
    assert results[0]["ticker"] == "KO"
    assert results[0]["is_following"] is True
    assert results[1]["ticker"] == "KOD"
    assert results[1]["is_following"] is False
    assert results[2]["ticker"] == "KODK"
    assert results[2]["is_following"] is True


@pytest.mark.asyncio
async def test_is_following_absent_when_no_set():
    """Results should NOT include is_following when followed_tickers is None."""
    from whitelist_service import search_whitelist

    db = _make_fake_db(_RAW_DOCS)

    results = await search_whitelist(db, "KO", limit=20, followed_tickers=None)

    assert len(results) == 3
    for r in results:
        assert "is_following" not in r, f"is_following should be absent for {r['ticker']}"


@pytest.mark.asyncio
async def test_is_following_empty_set_all_false():
    """All results should have is_following=False when user follows nothing."""
    from whitelist_service import search_whitelist

    db = _make_fake_db(_RAW_DOCS)

    results = await search_whitelist(db, "KO", limit=20, followed_tickers=set())

    assert len(results) == 3
    for r in results:
        assert r["is_following"] is False, f"{r['ticker']} should not be followed"
