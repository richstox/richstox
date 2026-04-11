"""
Test: Searched-symbol mapping guarantee
========================================
Verifies that store_articles_with_mapping creates a mapping for the
*searched* ticker (the one the user follows), even when the EODHD
``symbols`` field is empty or doesn't include that ticker.

Also verifies the 24-hour recency guard in cleanup_orphaned_articles.
"""

import asyncio
import hashlib
import sys
import os
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.news_service import (
    store_articles_with_mapping,
    cleanup_orphaned_articles,
    fetch_news_batch_from_eodhd,
    generate_article_id,
    TICKER_DETAIL_LIMIT,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class FakeCursor:
    """Mimics an async Motor cursor."""

    def __init__(self, docs):
        self._docs = list(docs)

    def sort(self, *a, **kw):
        return self

    def limit(self, n):
        self._docs = self._docs[:n]
        return self

    async def to_list(self, length=None):
        return self._docs

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._docs:
            return self._docs.pop(0)
        raise StopAsyncIteration


class FakeCollection:
    """In-memory Mongo-like collection for tests."""

    def __init__(self, docs=None):
        self._docs = {doc["_id"]: dict(doc) for doc in (docs or [])}
        self._next_id = 1

    async def update_one(self, filter_q, update, upsert=False):
        match = self._find_one_sync(filter_q)
        if match:
            return MagicMock(upserted_id=None, matched_count=1)
        if upsert:
            new_doc = {}
            new_doc.update(filter_q)
            if "$setOnInsert" in update:
                new_doc.update(update["$setOnInsert"])
            oid = f"_oid_{self._next_id}"
            self._next_id += 1
            new_doc["_id"] = oid
            self._docs[oid] = new_doc
            return MagicMock(upserted_id=oid, matched_count=0)
        return MagicMock(upserted_id=None, matched_count=0)

    async def count_documents(self, filter_q, limit=None):
        n = sum(1 for d in self._docs.values() if self._matches(d, filter_q))
        if limit is not None:
            return min(n, limit)
        return n

    async def find_one(self, filter_q=None, projection=None):
        filter_q = filter_q or {}
        for d in self._docs.values():
            if self._matches(d, filter_q):
                return d
        return None

    def find(self, filter_q=None, projection=None):
        filter_q = filter_q or {}
        results = [d for d in self._docs.values() if self._matches(d, filter_q)]
        return FakeCursor(results)

    def aggregate(self, pipeline, maxTimeMS=None):
        # Simplified aggregation: only supports the orphan-cleanup pipeline shape
        return self._run_agg(pipeline)

    async def _run_agg_list(self, pipeline):
        result = []
        async for doc in self._run_agg(pipeline):
            result.append(doc)
        return result

    def _run_agg(self, pipeline):
        return _FakeAggCursor(self, pipeline)

    async def delete_many(self, filter_q):
        to_del = [k for k, v in self._docs.items() if self._matches(v, filter_q)]
        for k in to_del:
            del self._docs[k]
        return MagicMock(deleted_count=len(to_del))

    # helpers
    def _find_one_sync(self, fq):
        for d in self._docs.values():
            if self._matches(d, fq):
                return d
        return None

    @staticmethod
    def _matches(doc, fq):
        for k, v in fq.items():
            dv = doc.get(k)
            if isinstance(v, dict):
                for op, operand in v.items():
                    if op == "$in" and dv not in operand:
                        return False
                    if op == "$lt" and not (dv is not None and dv < operand):
                        return False
                    if op == "$size" and (not isinstance(dv, list) or len(dv) != operand):
                        return False
            else:
                if dv != v:
                    return False
        return True

    def all_docs(self):
        return list(self._docs.values())


class _FakeAggCursor:
    """Mimics async iteration over an aggregation result."""

    def __init__(self, coll, pipeline):
        self._coll = coll
        self._pipeline = pipeline
        self._results = None

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._results is None:
            self._results = list(self._execute())
        if self._results:
            return self._results.pop(0)
        raise StopAsyncIteration

    def _execute(self):
        docs = list(self._coll._docs.values())
        for stage in self._pipeline:
            docs = self._apply_stage(stage, docs)
        return docs

    def _apply_stage(self, stage, docs):
        if "$match" in stage:
            return [d for d in docs if self._match_doc(d, stage["$match"])]
        if "$lookup" in stage:
            lk = stage["$lookup"]
            from_coll = getattr(self._coll, f"_lookup_{lk['from']}", None)
            if from_coll is None:
                # cannot resolve — return docs unchanged but with empty arrays
                return [{**d, lk["as"]: []} for d in docs]
            out = []
            for d in docs:
                local_val = d.get(lk["localField"])
                matches = [
                    fd for fd in from_coll._docs.values()
                    if fd.get(lk["foreignField"]) == local_val
                ]
                out.append({**d, lk["as"]: matches})
            return out
        if "$project" in stage:
            proj = stage["$project"]
            return [{k: d.get(k) for k in proj} for d in docs]
        return docs

    @staticmethod
    def _match_doc(doc, fq):
        return FakeCollection._matches(doc, fq)


def _make_db(articles=None, mappings=None, legacy=None):
    """Create a fake DB with the collections used by news_service."""
    db = MagicMock()
    articles_coll = FakeCollection(articles or [])
    mappings_coll = FakeCollection(mappings or [])
    legacy_coll = FakeCollection(legacy or [])

    # Wire $lookup resolution so the aggregation can join
    articles_coll._lookup_article_ticker_mapping = mappings_coll
    articles_coll._lookup_news_article_symbols = legacy_coll
    mappings_coll._lookup_news_articles = articles_coll

    db.news_articles = articles_coll
    db.article_ticker_mapping = mappings_coll
    db.news_article_symbols = legacy_coll
    return db


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestSearchedSymbolTagging:
    """fetch_news_batch_from_eodhd should tag articles with _searched_symbol."""

    @pytest.mark.asyncio
    async def test_articles_tagged_with_searched_symbol(self):
        """Articles returned by EODHD should carry _searched_symbol."""
        fake_response = MagicMock()
        fake_response.status_code = 200
        fake_response.raise_for_status = MagicMock()
        fake_response.json.return_value = [
            {"title": "A1", "link": "http://a1", "symbols": ["TSLA.US"]},
        ]

        fake_client = AsyncMock()
        fake_client.get.return_value = fake_response
        fake_client.__aenter__ = AsyncMock(return_value=fake_client)
        fake_client.__aexit__ = AsyncMock(return_value=False)

        with patch("services.news_service.EODHD_API_KEY", "test-key"), \
             patch("services.news_service.httpx.AsyncClient", return_value=fake_client):
            articles, _ = await fetch_news_batch_from_eodhd(
                symbols=["AAPL.US"],
                from_date="2024-01-01",
                to_date="2024-01-02",
            )

        assert len(articles) == 1
        assert articles[0]["_searched_symbol"] == "AAPL"


class TestStoreArticlesWithMapping:
    """store_articles_with_mapping should always create a mapping for _searched_symbol."""

    @pytest.mark.asyncio
    async def test_mapping_created_for_searched_symbol(self):
        """Even with empty EODHD symbols, the searched ticker gets a mapping."""
        db = _make_db()
        articles = [
            {
                "link": "http://example.com/article1",
                "title": "Test Article",
                "date": "2024-04-11",
                "symbols": [],  # empty!
                "_searched_symbol": "AAPL",
                "sentiment": {"pos": 0.5, "neg": 0.1},
            }
        ]
        result = await store_articles_with_mapping(db, articles)
        assert result["new_articles"] == 1
        assert result["new_mappings"] == 1  # the searched ticker

        # Verify the mapping exists for AAPL
        mappings = db.article_ticker_mapping.all_docs()
        assert any(m["ticker"] == "AAPL" for m in mappings)

    @pytest.mark.asyncio
    async def test_mapping_for_searched_plus_eodhd_symbols(self):
        """Both the searched ticker and EODHD symbols get mappings."""
        db = _make_db()
        articles = [
            {
                "link": "http://example.com/article2",
                "title": "Multi-ticker Article",
                "date": "2024-04-11",
                "symbols": ["MSFT.US", "GOOGL.US"],
                "_searched_symbol": "AAPL",
                "sentiment": {},
            }
        ]
        result = await store_articles_with_mapping(db, articles)
        assert result["new_articles"] == 1
        assert result["new_mappings"] == 3  # AAPL + MSFT + GOOGL

        mapped_tickers = {m["ticker"] for m in db.article_ticker_mapping.all_docs()}
        assert mapped_tickers == {"AAPL", "MSFT", "GOOGL"}

    @pytest.mark.asyncio
    async def test_dedup_when_searched_in_eodhd_symbols(self):
        """No duplicate mapping when searched ticker is also in EODHD symbols."""
        db = _make_db()
        articles = [
            {
                "link": "http://example.com/article3",
                "title": "AAPL Article",
                "date": "2024-04-11",
                "symbols": ["AAPL.US", "MSFT.US"],
                "_searched_symbol": "AAPL",
                "sentiment": {},
            }
        ]
        result = await store_articles_with_mapping(db, articles)
        assert result["new_mappings"] == 2  # AAPL + MSFT (deduped)

    @pytest.mark.asyncio
    async def test_non_string_symbols_skipped(self):
        """Non-string items in the EODHD symbols list are safely skipped."""
        db = _make_db()
        articles = [
            {
                "link": "http://example.com/article4",
                "title": "Bad symbols",
                "date": "2024-04-11",
                "symbols": [None, 123, {"code": "X"}],
                "_searched_symbol": "NVDA",
                "sentiment": {},
            }
        ]
        result = await store_articles_with_mapping(db, articles)
        assert result["new_articles"] == 1
        assert result["new_mappings"] == 1  # only NVDA

    @pytest.mark.asyncio
    async def test_articles_without_searched_symbol(self):
        """Articles without _searched_symbol still work (backward compat)."""
        db = _make_db()
        articles = [
            {
                "link": "http://example.com/article5",
                "title": "Legacy",
                "date": "2024-04-11",
                "symbols": ["META.US"],
                "sentiment": {},
            }
        ]
        result = await store_articles_with_mapping(db, articles)
        assert result["new_mappings"] == 1
        mapped_tickers = {m["ticker"] for m in db.article_ticker_mapping.all_docs()}
        assert mapped_tickers == {"META"}


class TestCleanupRecencyGuard:
    """cleanup_orphaned_articles should NOT delete articles created < 24h ago."""

    @pytest.mark.asyncio
    async def test_recent_articles_spared(self):
        """Articles created less than 24h ago are never deleted as orphans."""
        now = datetime.now(timezone.utc)
        recent_article = {
            "_id": "recent",
            "article_id": "a1",
            "created_at": now - timedelta(hours=1),  # 1 hour ago
        }
        db = _make_db(
            articles=[recent_article],
            mappings=[{"_id": "m_other", "article_id": "other_article", "ticker": "X"}],
        )
        result = await cleanup_orphaned_articles(db)
        # Article a1 has zero mappings but is recent → should NOT be deleted
        assert result["orphans_deleted"] == 0
        assert len(db.news_articles.all_docs()) == 1

    @pytest.mark.asyncio
    async def test_old_orphans_deleted(self):
        """Articles older than 24h with zero mappings ARE deleted."""
        now = datetime.now(timezone.utc)
        old_article = {
            "_id": "old",
            "article_id": "a2",
            "created_at": now - timedelta(hours=48),  # 2 days ago
        }
        db = _make_db(
            articles=[old_article],
            mappings=[{"_id": "m_other", "article_id": "other_article", "ticker": "X"}],
        )
        result = await cleanup_orphaned_articles(db)
        assert result["orphans_deleted"] == 1
        assert len(db.news_articles.all_docs()) == 0

    @pytest.mark.asyncio
    async def test_old_article_with_mapping_preserved(self):
        """Articles older than 24h that still have mappings are NOT deleted."""
        now = datetime.now(timezone.utc)
        aid = "a3"
        old_article = {
            "_id": "old_mapped",
            "article_id": aid,
            "created_at": now - timedelta(hours=48),
        }
        mapping = {
            "_id": "m1",
            "article_id": aid,
            "ticker": "AAPL",
        }
        db = _make_db(articles=[old_article], mappings=[mapping])
        result = await cleanup_orphaned_articles(db)
        assert result["orphans_deleted"] == 0
        assert len(db.news_articles.all_docs()) == 1
