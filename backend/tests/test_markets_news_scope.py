from pathlib import Path


def test_homepage_aggregate_sentiment_uses_full_news_corpus():
    server_source = (Path(__file__).resolve().parent.parent / "server.py").read_text()
    assert '"aggregate_sentiment": _build_aggregate_sentiment(final_articles)' in server_source


def test_markets_news_uses_shared_global_corpus_and_full_sentiment():
    server_source = (Path(__file__).resolve().parent.parent / "server.py").read_text()
    assert "GLOBAL_MARKETS_WATCHLIST_TICKER_LIMIT = 10" in server_source
    assert "GLOBAL_MARKETS_TRACKLIST_TICKER_LIMIT = 10" in server_source
    assert "GLOBAL_MARKETS_MARKET_NEWS_LIMIT = 100" in server_source
    assert "limit: int = Query(GLOBAL_MARKETS_TOTAL_NEWS_LIMIT" in server_source
    assert '"total_news_count": total_count' in server_source
    assert '"aggregate_sentiment": _build_aggregate_sentiment(merged_news)' in server_source
