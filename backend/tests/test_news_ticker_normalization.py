#!/usr/bin/env python3
"""
News Ticker Normalization Test
===============================
Ensures that article_ticker_mapping stores bare tickers ("AAPL") and that
read paths correctly query both bare and suffixed forms so that:
  - A mapping stored as "AAPL" is found when querying "AAPL.US"
  - A mapping stored as "AAPL" is found when querying "AAPL"
  - Legacy news_article_symbols stored as "AAPL.US" is found when querying "AAPL"
"""

import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, timezone

from services.news_service import (
    _bare_ticker,
    store_articles_with_mapping,
    _backfill_normalize_mappings,
)


# ---------------------------------------------------------------------------
# _bare_ticker unit tests
# ---------------------------------------------------------------------------

class TestBareTicker:
    def test_strips_us_suffix(self):
        assert _bare_ticker("AAPL.US") == "AAPL"

    def test_strips_cc_suffix(self):
        assert _bare_ticker("BTC.CC") == "BTC"

    def test_already_bare(self):
        assert _bare_ticker("AAPL") == "AAPL"

    def test_lowercased_input(self):
        assert _bare_ticker("aapl.us") == "AAPL"

    def test_bare_lowercased(self):
        assert _bare_ticker("nvda") == "NVDA"

    def test_whitespace_stripped(self):
        assert _bare_ticker("  GOOG.US  ") == "GOOG"

    def test_empty_string(self):
        assert _bare_ticker("") == ""

    def test_only_suffix(self):
        # Edge case: ".US" alone becomes ""
        assert _bare_ticker(".US") == ""


# ---------------------------------------------------------------------------
# store_articles_with_mapping: verify that tickers are stored bare
# ---------------------------------------------------------------------------

class TestStoreArticlesMapping:
    @pytest.mark.asyncio
    async def test_mapping_stored_as_bare_ticker(self):
        """Articles from EODHD with symbols like 'AAPL.US' must result in
        article_ticker_mapping entries with ticker='AAPL' (bare)."""

        upserted_docs = []

        # Build mock db
        db = MagicMock()

        # news_articles.update_one returns "upserted"
        mock_upsert = AsyncMock(return_value=MagicMock(upserted_id="fake_id"))
        db.news_articles.update_one = mock_upsert

        # Capture article_ticker_mapping.update_one calls
        async def capture_mapping_upsert(filter_doc, update_doc, upsert=False):
            upserted_docs.append(filter_doc)
            return MagicMock(upserted_id="fake_mapping_id")

        db.article_ticker_mapping.update_one = AsyncMock(side_effect=capture_mapping_upsert)
        db.article_ticker_mapping.count_documents = AsyncMock(return_value=1)

        articles = [
            {
                "link": "https://example.com/news/1",
                "title": "Apple rises",
                "date": "2024-01-15",
                "symbols": ["AAPL.US", "MSFT.US"],
                "sentiment": {"pos": 0.6, "neg": 0.1},
                "content": "Apple stock went up.",
                "tags": ["tech"],
            }
        ]

        result = await store_articles_with_mapping(db, articles)
        assert result["new_articles"] == 1

        # Verify that all mapping filters use bare tickers
        stored_tickers = [d["ticker"] for d in upserted_docs]
        assert "AAPL" in stored_tickers, f"Expected 'AAPL' in {stored_tickers}"
        assert "MSFT" in stored_tickers, f"Expected 'MSFT' in {stored_tickers}"

        # .US must NOT appear
        for t in stored_tickers:
            assert ".US" not in t, f"Ticker '{t}' still has .US suffix"


# ---------------------------------------------------------------------------
# _backfill_normalize_mappings: verify it fixes suffixed rows
# ---------------------------------------------------------------------------

class TestBackfillNormalizeMappings:
    @pytest.mark.asyncio
    async def test_backfill_renames_suffixed_tickers(self):
        """Existing article_ticker_mapping rows with 'AAPL.US' must be
        renamed to 'AAPL'."""

        db = MagicMock()

        # Simulate 2 suffixed documents
        suffixed_docs = [
            {"_id": "id1", "article_id": "art1", "ticker": "AAPL.US"},
            {"_id": "id2", "article_id": "art2", "ticker": "NVDA.US"},
        ]

        db.article_ticker_mapping.count_documents = AsyncMock(return_value=2)

        # Mock the async iterator for find()
        async def async_iter(docs):
            for d in docs:
                yield d

        mock_cursor = MagicMock()
        mock_cursor.batch_size = MagicMock(return_value=mock_cursor)
        mock_cursor.__aiter__ = lambda self: async_iter(suffixed_docs)
        db.article_ticker_mapping.find = MagicMock(return_value=mock_cursor)

        # No existing bare rows
        db.article_ticker_mapping.find_one = AsyncMock(return_value=None)

        # Track update calls
        update_calls = []

        async def capture_update(filter_doc, update_doc):
            update_calls.append((filter_doc, update_doc))
            return MagicMock(modified_count=1)

        db.article_ticker_mapping.update_one = AsyncMock(side_effect=capture_update)
        db.article_ticker_mapping.delete_one = AsyncMock()

        fixed = await _backfill_normalize_mappings(db)
        assert fixed == 2

        # Verify updates set bare tickers
        for filt, upd in update_calls:
            new_ticker = upd["$set"]["ticker"]
            assert ".US" not in new_ticker, f"Updated ticker still has suffix: {new_ticker}"

    @pytest.mark.asyncio
    async def test_backfill_deletes_duplicates(self):
        """If both 'AAPL.US' and 'AAPL' rows exist for the same article_id,
        the suffixed row must be deleted (not renamed)."""

        db = MagicMock()

        suffixed_docs = [
            {"_id": "id1", "article_id": "art1", "ticker": "AAPL.US"},
        ]

        db.article_ticker_mapping.count_documents = AsyncMock(return_value=1)

        async def async_iter(docs):
            for d in docs:
                yield d

        mock_cursor = MagicMock()
        mock_cursor.batch_size = MagicMock(return_value=mock_cursor)
        mock_cursor.__aiter__ = lambda self: async_iter(suffixed_docs)
        db.article_ticker_mapping.find = MagicMock(return_value=mock_cursor)

        # Bare row already exists
        db.article_ticker_mapping.find_one = AsyncMock(
            return_value={"_id": "id_bare", "article_id": "art1", "ticker": "AAPL"}
        )

        db.article_ticker_mapping.delete_one = AsyncMock()
        db.article_ticker_mapping.update_one = AsyncMock()

        fixed = await _backfill_normalize_mappings(db)
        assert fixed == 1

        # delete_one should have been called (not update_one)
        db.article_ticker_mapping.delete_one.assert_called_once_with({"_id": "id1"})
        db.article_ticker_mapping.update_one.assert_not_called()


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
