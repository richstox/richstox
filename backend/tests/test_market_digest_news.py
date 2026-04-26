#!/usr/bin/env python3
"""
Market digest + Tracklist news regressions.
"""

import os
import sys
from unittest.mock import AsyncMock, MagicMock

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services import news_service
from services.news_service import (
    fetch_market_digest_from_eodhd,
    get_hot_symbols,
    store_market_digest_articles,
)


class TestFetchMarketDigestFromEodhd:
    @pytest.mark.asyncio
    async def test_uses_markets_topic(self, monkeypatch):
        monkeypatch.setattr(news_service, "EODHD_API_KEY", "token")

        response = MagicMock()
        response.raise_for_status = MagicMock()
        response.json = MagicMock(return_value=[{"title": "Markets headline"}])

        client = MagicMock()
        client.get = AsyncMock(return_value=response)

        class _ClientCtx:
            async def __aenter__(self_inner):
                return client

            async def __aexit__(self_inner, exc_type, exc, tb):
                return False

        monkeypatch.setattr(news_service.httpx, "AsyncClient", MagicMock(return_value=_ClientCtx()))

        articles, api_url_template = await fetch_market_digest_from_eodhd()

        assert articles == [{"title": "Markets headline"}]
        assert "t=MARKETS" in api_url_template
        client.get.assert_awaited_once()
        _, kwargs = client.get.await_args
        assert kwargs["params"]["t"] == "MARKETS"
        assert kwargs["params"]["limit"] == news_service.MARKET_DIGEST_FETCH_LIMIT


class TestStoreMarketDigestArticles:
    @pytest.mark.asyncio
    async def test_stores_market_topic_and_sentiment_label(self):
        db = MagicMock()

        captured_updates = []

        async def capture_update(filter_doc, update_doc, upsert=False):
            captured_updates.append((filter_doc, update_doc, upsert))
            return MagicMock(upserted_id="market-1")

        db.market_news.update_one = AsyncMock(side_effect=capture_update)
        db.market_news.count_documents = AsyncMock(return_value=1)

        result = await store_market_digest_articles(db, [{
            "link": "https://example.com/markets/1",
            "title": "Macro outlook improves",
            "date": "2026-04-26T07:50:00+00:00",
            "content": "Market digest content",
            "tags": ["MARKETS", "MACRO"],
            "symbols": ["SPY.US"],
            "sentiment": {"pos": 0.3, "neg": 0.1},
        }])

        assert result == {"new_articles": 1}
        assert len(captured_updates) == 1
        _, update_doc, upsert = captured_updates[0]
        stored_doc = update_doc["$setOnInsert"]
        assert upsert is True
        assert stored_doc["topic"] == "MARKETS"
        assert stored_doc["sentiment_label"] == "positive"
        assert stored_doc["symbols"] == ["SPY.US"]


class TestGetHotSymbols:
    @pytest.mark.asyncio
    async def test_includes_tracklist_tickers(self):
        db = MagicMock()

        watchlist_cursor = MagicMock()
        watchlist_cursor.to_list = AsyncMock(return_value=[])
        positions_cursor = MagicMock()
        positions_cursor.to_list = AsyncMock(return_value=[])
        tracklist_cursor = MagicMock()
        tracklist_cursor.to_list = AsyncMock(return_value=[{
            "positions": [{"ticker": "PLTR.US"}, {"ticker": "NVDA"}],
            "draft_tickers": ["SOFI.US"],
        }])

        db.user_watchlist.aggregate = MagicMock(return_value=watchlist_cursor)
        db.positions.aggregate = MagicMock(return_value=positions_cursor)
        db.user_tracklists.find = MagicMock(return_value=tracklist_cursor)

        hot_symbols = await get_hot_symbols(db, n=10)

        assert {"PLTR", "NVDA", "SOFI"}.issubset(set(hot_symbols))
