from pathlib import Path


def _server_source() -> str:
    return (Path(__file__).resolve().parent.parent / "server.py").read_text()


def _slice_between(source: str, start_marker: str, end_marker: str) -> str:
    start = source.index(start_marker)
    end = source.index(end_marker, start)
    return source[start:end]


def test_homepage_aggregate_sentiment_uses_full_news_corpus():
    server_source = _server_source()
    assert '"aggregate_sentiment": _build_aggregate_sentiment(final_articles)' in server_source


def test_homepage_news_is_user_scoped_to_watchlist_and_tracklist():
    server_source = _server_source()
    get_news_block = _slice_between(server_source, '@api_router.get("/news")', '@api_router.get("/v1/markets/news")')

    assert 'membership_state = await _get_membership_state(user["user_id"])' in get_news_block
    assert 'all_followed = membership_state["watchlist"] | membership_state["tracklist"]' in get_news_block
    assert 'watchlist_raw = await db.user_watchlist.distinct("ticker")' not in get_news_block
    assert "portfolio_docs = await db.positions.find(" not in get_news_block
    assert "tracklist_tickers = await _get_global_tracklist_tickers()" not in get_news_block


def test_markets_news_uses_full_global_corpus_and_full_sentiment():
    server_source = _server_source()
    markets_block = _slice_between(server_source, '@api_router.get("/v1/markets/news")', "async def refresh_news_cache_for_tickers")

    assert "GLOBAL_MARKETS_MARKET_NEWS_LIMIT = 100" in server_source
    assert "GLOBAL_MARKETS_MIN_TICKER_NEWS = 100" in server_source
    assert "GLOBAL_MARKETS_WATCHLIST_TICKER_LIMIT = 10" not in server_source
    assert "GLOBAL_MARKETS_TRACKLIST_TICKER_LIMIT = 10" not in server_source
    assert "watchlist_tickers = sorted(await _get_global_watchlist_tickers())" in markets_block
    assert "tracklist_tickers = sorted(await _get_global_tracklist_tickers())" in markets_block
    assert "_get_ranked_global_watchlist_tickers" not in markets_block
    assert "_get_ranked_global_tracklist_tickers" not in markets_block
    assert "async def _get_recent_global_ticker_mappings(" in server_source
    assert "fallback_ticker_mappings = await _get_recent_global_ticker_mappings(" in markets_block
    assert 'GLOBAL_MARKETS_RESPONSE_LIMIT = 1000' in server_source
    assert 'limit: int = Query(GLOBAL_MARKETS_RESPONSE_LIMIT, ge=1, le=GLOBAL_MARKETS_RESPONSE_LIMIT)' in markets_block
    assert '"total_news_count": total_count' in markets_block
    assert '"aggregate_sentiment": _build_aggregate_sentiment(merged_news)' in markets_block


def test_ticker_detail_news_returns_full_stored_content():
    server_source = _server_source()
    ticker_block = _slice_between(server_source, '@api_router.get("/news/ticker/{ticker}")', '@api_router.get("/news/{article_id}")')

    assert '"content": article.get("content", "")' in ticker_block
    assert '[:500]' not in ticker_block
