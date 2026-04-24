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


def _make_fake_db(tracked_docs, cache_docs=None):
    """Return a fake db object whose tracked_tickers.aggregate returns *tracked_docs*.

    If *cache_docs* is provided, ``company_fundamentals_cache.find()`` returns
    a cursor over those docs (used for the logo batch lookup).
    """
    db = MagicMock()
    db.tracked_tickers.aggregate = MagicMock(return_value=_FakeCursor(tracked_docs))
    # company_fundamentals_cache.find(...) returns a cursor-like object
    db.company_fundamentals_cache.find = MagicMock(return_value=_FakeCursor(cache_docs or []))
    return db


# Raw docs as the aggregation pipeline would return (after $project)
_RAW_DOCS = [
    {"ticker": "KO.US",   "name": "The Coca-Cola Company",     "exchange": "NYSE",   "sector": "Consumer Defensive", "industry": "Beverages", "asset_type": "Common Stock", "status": "active", "safety_type": "standard", "rank": 0},
    {"ticker": "KOD.US",  "name": "Kodiak Sciences Inc",       "exchange": "NASDAQ", "sector": "Healthcare",         "industry": "Biotech",   "asset_type": "Common Stock", "status": "active", "safety_type": "standard", "rank": 1},
    {"ticker": "KODK.US", "name": "Eastman Kodak Co",          "exchange": "NYSE",   "sector": "Technology",         "industry": "Imaging",   "asset_type": "Common Stock", "status": "active", "safety_type": "standard", "rank": 1},
]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_is_following_present_when_set_provided():
    """Results should include is_following=True/False when followed_tickers is given."""
    from whitelist_service import search_whitelist

    db = _make_fake_db(_RAW_DOCS)
    followed = {"KO", "KODK"}

    results = asyncio.run(search_whitelist(db, "KO", limit=20, followed_tickers=followed))

    assert len(results) == 3
    assert results[0]["ticker"] == "KO"
    assert results[0]["is_following"] is True
    assert results[1]["ticker"] == "KOD"
    assert results[1]["is_following"] is False
    assert results[2]["ticker"] == "KODK"
    assert results[2]["is_following"] is True


def test_is_following_absent_when_no_set():
    """Results should NOT include is_following when followed_tickers is None."""
    from whitelist_service import search_whitelist

    db = _make_fake_db(_RAW_DOCS)

    results = asyncio.run(search_whitelist(db, "KO", limit=20, followed_tickers=None))

    assert len(results) == 3
    for r in results:
        assert "is_following" not in r, f"is_following should be absent for {r['ticker']}"


def test_is_following_empty_set_all_false():
    """All results should have is_following=False when user follows nothing."""
    from whitelist_service import search_whitelist

    db = _make_fake_db(_RAW_DOCS)

    results = asyncio.run(search_whitelist(db, "KO", limit=20, followed_tickers=set()))

    assert len(results) == 3
    for r in results:
        assert r["is_following"] is False, f"{r['ticker']} should not be followed"


# ---------------------------------------------------------------------------
# Logo URL tests
# ---------------------------------------------------------------------------

def test_build_full_logo_url_relative_path():
    """Relative paths should be converted to internal /api/logo/ URLs."""
    from whitelist_service import _build_full_logo_url

    assert _build_full_logo_url("/img/logos/US/AAPL.png") == "/api/logo/AAPL"


def test_build_full_logo_url_absolute():
    """Absolute URLs should be converted to internal /api/logo/ URLs."""
    from whitelist_service import _build_full_logo_url

    assert _build_full_logo_url("https://example.com/logo.png") == "/api/logo/LOGO"


def test_build_full_logo_url_none():
    """None input should return None."""
    from whitelist_service import _build_full_logo_url

    assert _build_full_logo_url(None) is None


def test_build_full_logo_url_empty():
    """Empty string input should return None."""
    from whitelist_service import _build_full_logo_url

    assert _build_full_logo_url("") is None


def test_search_results_include_full_logo_url():
    """Search results should include fully-qualified logo URLs from company_fundamentals_cache."""
    from whitelist_service import search_whitelist

    tracked_docs = [
        {"ticker": "AAPL.US", "name": "Apple Inc", "exchange": "NASDAQ", "sector": "Technology", "industry": "Consumer Electronics", "asset_type": "Common Stock", "status": "active", "safety_type": "standard", "rank": 0},
        {"ticker": "MSFT.US", "name": "Microsoft Corp", "exchange": "NASDAQ", "sector": "Technology", "industry": "Software", "asset_type": "Common Stock", "status": "active", "safety_type": "standard", "rank": 1},
        {"ticker": "AMZN.US", "name": "Amazon.com Inc", "exchange": "NASDAQ", "sector": "Consumer Cyclical", "industry": "Internet Retail", "asset_type": "Common Stock", "status": "active", "safety_type": "standard", "rank": 2},
    ]
    cache_docs = [
        {"ticker": "AAPL.US", "logo_url": "/img/logos/US/AAPL.png"},
        {"ticker": "MSFT.US", "logo_url": "https://cdn.example.com/msft.png"},
        # AMZN missing from cache → logo should be None
    ]
    db = _make_fake_db(tracked_docs, cache_docs=cache_docs)

    results = asyncio.run(search_whitelist(db, "A", limit=20))

    assert results[0]["logo"] == "/api/logo/AAPL"
    assert results[1]["logo"] == "/api/logo/MSFT"
    assert results[2]["logo"] is None


def test_search_logo_from_fundamentals_cache():
    """Logos come from company_fundamentals_cache (same source as dashboard)."""
    from whitelist_service import search_whitelist

    # tracked_tickers docs (no logo field — logo comes from cache)
    tracked_docs = [
        {"ticker": "KO.US", "name": "The Coca-Cola Company", "exchange": "NYSE", "sector": "Consumer Defensive", "industry": "Beverages", "asset_type": "Common Stock", "status": "active", "safety_type": "standard", "rank": 0},
        {"ticker": "KOD.US", "name": "Kodiak Sciences Inc", "exchange": "NASDAQ", "sector": "Healthcare", "industry": "Biotech", "asset_type": "Common Stock", "status": "active", "safety_type": "standard", "rank": 1},
    ]

    # company_fundamentals_cache has the logo URLs
    cache_docs = [
        {"ticker": "KO.US", "logo_url": "/img/logos/US/KO.png"},
        {"ticker": "KOD.US", "logo_url": "https://cdn.example.com/kod.png"},
    ]

    db = _make_fake_db(tracked_docs, cache_docs=cache_docs)
    results = asyncio.run(search_whitelist(db, "KO", limit=20))

    assert results[0]["ticker"] == "KO"
    assert results[0]["logo"] == "/api/logo/KO"
    assert results[1]["ticker"] == "KOD"
    assert results[1]["logo"] == "/api/logo/KOD"
