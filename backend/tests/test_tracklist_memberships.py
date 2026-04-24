#!/usr/bin/env python3
"""
Regression tests for Tracklist memberships in whitelist search.
"""

import asyncio
import os
import sys
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class _FakeCursor:
    def __init__(self, docs):
        self._docs = list(docs)

    async def to_list(self, length=None):
        return self._docs


def _make_fake_db(tracked_docs, cache_docs=None):
    db = MagicMock()
    db.tracked_tickers.aggregate = MagicMock(return_value=_FakeCursor(tracked_docs))
    db.company_fundamentals_cache.find = MagicMock(return_value=_FakeCursor(cache_docs or []))
    return db


def test_search_includes_tracklist_memberships():
    from whitelist_service import search_whitelist

    tracked_docs = [
        {"ticker": "AAPL.US", "name": "Apple Inc", "exchange": "NASDAQ", "sector": "Technology", "industry": "Hardware", "asset_type": "Common Stock", "status": "active", "safety_type": "standard", "rank": 0},
        {"ticker": "MSFT.US", "name": "Microsoft Corp", "exchange": "NASDAQ", "sector": "Technology", "industry": "Software", "asset_type": "Common Stock", "status": "active", "safety_type": "standard", "rank": 1},
    ]
    db = _make_fake_db(tracked_docs)

    results = asyncio.run(search_whitelist(
        db,
        'A',
        followed_tickers={'AAPL'},
        tracklist_tickers={'MSFT'},
    ))

    assert results[0]['memberships'] == ['watchlist']
    assert results[1]['memberships'] == ['tracklist']


def test_server_declares_tracklist_endpoints():
    server_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'server.py')
    with open(server_path, 'r', encoding='utf-8') as handle:
        content = handle.read()

    assert '/v1/tracklist/add/{ticker}' in content
    assert '/v1/tracklist/replace' in content
    assert 'Tracklist is full (7). Manage it on the Tracklist page.' in content
