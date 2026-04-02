# ==============================================================================
# 🛡️ SCHEDULER-ONLY FILE - EXTERNAL API CALLS ALLOWED
# ==============================================================================
# This file is in ALLOWLIST: /app/scripts/audit_external_calls.py
# May call: eodhd.com/api/* (via httpx)
# Context: ONLY from scheduler.py jobs or admin backfill endpoints
# Runtime API endpoints MUST NOT import from this file
# ==============================================================================
"""
RICHSTOX News Service (P53 BINDING)
=====================================
Handles EODHD news fetching with batch API calls, delta sync, and proper dedup.

P53 BINDING RULES:
1. BATCH API CALLS: Multiple tickers in one request (s=AAPL.US,MSFT.US,TSLA.US)
2. DELTA SYNC: Use last_synced_at per ticker, fetch only &from={last_synced_at}
3. PAGINATION: limit=1000, offset=0 per batch (no pagination needed)
4. DEDUP: One article stored once, mapped to multiple tickers via article_ticker_mapping
5. LIMIT: Max 3 articles per ticker in mapping

Tables:
- news_articles: article_id, link, title, published_at, eodhd_symbols_raw
- article_ticker_mapping: article_id, ticker, rank, created_at
"""

import os
import hashlib
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional
import httpx

logger = logging.getLogger("richstox.news")

EODHD_API_KEY = os.environ.get("EODHD_API_KEY")
BATCH_SIZE = 50  # Tickers per API request

# P53: Different limits for different contexts
GLOBAL_FEED_LIMIT = 3  # Max articles per ticker in global feed (Talk, Markets)
TICKER_DETAIL_LIMIT = 100  # Max articles per ticker in detail view
EODHD_FETCH_LIMIT = 10  # Articles to fetch per ticker from EODHD (balance cost vs coverage)


def generate_article_id(source_link: str, title: str = "", published_at: str = "") -> str:
    """Generate unique article ID. Primary: source_link hash."""
    if source_link:
        return hashlib.md5(source_link.encode()).hexdigest()[:16]
    fallback = f"{title}|{published_at}"
    return hashlib.md5(fallback.encode()).hexdigest()[:16]


async def fetch_news_batch_from_eodhd(
    symbols: List[str],
    from_date: str,
    to_date: str,
    limit: int = 200,
    offset: int = 0
) -> tuple[List[Dict[str, Any]], str]:
    """
    P53: Fetch news for multiple tickers.
    
    NOTE: EODHD API does NOT support comma-separated symbols in single call.
    We must call separately for each ticker, but we batch the requests efficiently.
    
    Returns:
        tuple: (articles list, api_url template used)
    """
    if not EODHD_API_KEY:
        logger.error("EODHD_API_KEY not set")
        return [], ""
    
    if not symbols:
        return [], ""
    
    all_articles = []
    api_url_template = f"https://eodhd.com/api/news?s={{SYMBOL}}&from={from_date}&to={to_date}&limit=3&offset=0&fmt=json"
    
    # Fetch for each symbol (EODHD doesn't support batch)
    async with httpx.AsyncClient(timeout=30.0) as client:
        for symbol in symbols:
            try:
                params = {
                    "api_token": EODHD_API_KEY,
                    "s": symbol,
                    "from": from_date,
                    "to": to_date,
                    "limit": EODHD_FETCH_LIMIT,  # P53: Fetch 10 per ticker for detail view
                    "offset": 0,
                    "fmt": "json",
                }
                
                response = await client.get(
                    "https://eodhd.com/api/news",
                    params=params,
                    headers={"User-Agent": "RICHSTOX/1.0"}
                )
                response.raise_for_status()
                articles = response.json()
                
                if isinstance(articles, list):
                    all_articles.extend(articles)
                    
            except Exception as e:
                logger.warning(f"Error fetching news for {symbol}: {e}")
                continue
    
    logger.info(f"Fetched {len(all_articles)} articles for {len(symbols)} symbols")
    return all_articles, api_url_template


async def store_articles_with_mapping(db, articles: List[Dict[str, Any]]) -> Dict[str, int]:
    """
    P53: Store articles once and create ticker mappings.
    
    Tables:
    - news_articles: one row per unique article (dedup by source_link)
    - article_ticker_mapping: one row per (article, ticker) pair
    
    Returns:
        {"new_articles": X, "new_mappings": Y}
    """
    if not articles:
        return {"new_articles": 0, "new_mappings": 0}
    
    new_articles = 0
    new_mappings = 0
    
    for article in articles:
        source_link = article.get("link", "")
        title = article.get("title", "")
        published_at = article.get("date", "")
        
        article_id = generate_article_id(source_link, title, published_at)
        
        # Parse sentiment
        sentiment = article.get("sentiment", {})
        pos = sentiment.get("pos", 0)
        neg = sentiment.get("neg", 0)
        if pos > neg:
            sentiment_label = "positive"
        elif neg > pos:
            sentiment_label = "negative"
        else:
            sentiment_label = "neutral"
        
        # P53: Get all tickers from EODHD symbols
        eodhd_symbols_raw = article.get("symbols", [])
        
        # Prepare article document
        doc = {
            "article_id": article_id,
            "source_link": source_link,
            "published_at": published_at,
            "title": title,
            "content": article.get("content", ""),
            "tags": article.get("tags", []),
            "sentiment": sentiment,
            "sentiment_label": sentiment_label,
            "eodhd_symbols_raw": eodhd_symbols_raw,
            "created_at": datetime.now(timezone.utc),
        }
        
        # Upsert article (dedup by article_id = hash of source_link)
        result = await db.news_articles.update_one(
            {"article_id": article_id},
            {"$setOnInsert": doc},
            upsert=True
        )
        
        if result.upserted_id:
            new_articles += 1
        
        # P53: Create mappings for EACH ticker in eodhd_symbols_raw
        # No limit here - we store all mappings, limit is applied at query time
        for sym in eodhd_symbols_raw:
            clean_sym = sym.replace(".US", "").replace(".CC", "").upper()
            
            # Skip crypto and non-standard tickers
            if "-USD" in clean_sym or len(clean_sym) > 5:
                continue
            
            # Get current rank for ordering
            existing_count = await db.article_ticker_mapping.count_documents({
                "ticker": clean_sym
            })
            
            # P53: Store up to TICKER_DETAIL_LIMIT (100) mappings per ticker
            if existing_count >= TICKER_DETAIL_LIMIT:
                continue
            
            # Create mapping
            mapping_result = await db.article_ticker_mapping.update_one(
                {"article_id": article_id, "ticker": clean_sym},
                {"$setOnInsert": {
                    "article_id": article_id,
                    "ticker": clean_sym,
                    "rank": existing_count + 1,
                    "published_at": published_at,
                    "created_at": datetime.now(timezone.utc),
                }},
                upsert=True
            )
            
            if mapping_result.upserted_id:
                new_mappings += 1
    
    logger.info(f"Stored {new_articles} new articles, {new_mappings} new ticker mappings")
    return {"new_articles": new_articles, "new_mappings": new_mappings}


async def get_hot_symbols(db, n: int = 50) -> List[str]:
    """Get top N symbols from user follows and portfolios, with fallback to popular tickers."""
    pipeline = [
        {"$group": {"_id": "$ticker", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": n}
    ]
    
    # Combine watchlist and portfolio positions
    follows = await db.user_watchlist.aggregate(pipeline).to_list(100)
    holdings = await db.positions.aggregate(pipeline).to_list(100)
    
    # Merge and dedupe
    all_symbols = {}
    for item in follows + holdings:
        ticker = item.get("_id", "")
        if ticker and len(ticker) <= 5:
            all_symbols[ticker] = all_symbols.get(ticker, 0) + item.get("count", 0)
    
    # Fallback to popular tickers if no user data
    if not all_symbols:
        # Default popular US tickers
        return ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "TSLA", "META", "BRK-B", "JPM", "V",
                "JNJ", "WMT", "PG", "MA", "HD", "DIS", "NFLX", "PYPL", "ADBE", "CRM"][:n]
    
    # Sort by count and return top N
    sorted_symbols = sorted(all_symbols.items(), key=lambda x: -x[1])
    return [s[0] for s in sorted_symbols[:n]]


async def get_ticker_last_synced(db, ticker: str) -> Optional[str]:
    """P53: Get last_synced_at for a specific ticker."""
    doc = await db.news_ticker_sync.find_one({"ticker": ticker})
    if doc and doc.get("last_synced_at"):
        return doc["last_synced_at"].strftime("%Y-%m-%d")
    return None


async def update_ticker_last_synced(db, tickers: List[str], sync_date: str):
    """P53: Update last_synced_at for multiple tickers."""
    now = datetime.now(timezone.utc)
    for ticker in tickers:
        await db.news_ticker_sync.update_one(
            {"ticker": ticker},
            {"$set": {"last_synced_at": now, "last_sync_date": sync_date}},
            upsert=True
        )


async def refresh_hot_tickers_news(db) -> Dict[str, Any]:
    """
    P53 BINDING: Daily news refresh with batch API calls and delta sync.
    
    Process:
    1. Get hot tickers from follows/portfolios
    2. Group tickers by last_synced_at date
    3. For each date group, batch tickers (50 per request)
    4. Fetch from EODHD with s=TICKER1,TICKER2,...&from={last_synced}&to={today}
    5. Dedup articles, create ticker mappings (max 3 per ticker)
    """
    started_at = datetime.now(timezone.utc)
    today = started_at.strftime("%Y-%m-%d")
    
    # Step 1: Get hot symbols
    hot_symbols = await get_hot_symbols(db, n=100)
    if not hot_symbols:
        logger.warning("No hot symbols found")
        return {
            "job_type": "news_daily_refresh",
            "status": "completed",
            "message": "No hot symbols found",
        }
    
    logger.info(f"Processing {len(hot_symbols)} hot symbols")
    
    # Step 2: Group tickers by last_synced_at for delta sync
    # For simplicity, use global sync date (7 days ago for initial, yesterday for delta)
    global_sync = await db.news_sync_state.find_one({"_id": "global"})
    if global_sync and global_sync.get("last_synced_at"):
        # Delta sync: from last sync date
        from_date = global_sync["last_synced_at"].strftime("%Y-%m-%d")
    else:
        # Initial sync: last 7 days
        from_date = (started_at - timedelta(days=7)).strftime("%Y-%m-%d")
    
    # Step 3: Process tickers (EODHD requires one call per ticker)
    # Format symbols for EODHD: AAPL -> AAPL.US
    formatted_symbols = [f"{s}.US" for s in hot_symbols if "-" not in s]  # Skip tickers with dashes
    
    # Step 4: Fetch and store
    total_articles_fetched = 0
    total_new_articles = 0
    total_new_mappings = 0
    api_calls = len(formatted_symbols)  # One call per ticker
    errors = 0
    
    try:
        articles, api_url_template = await fetch_news_batch_from_eodhd(
            symbols=formatted_symbols,
            from_date=from_date,
            to_date=today,
            limit=3,  # 3 per ticker
            offset=0
        )
        total_articles_fetched = len(articles)
        
        # Store with mappings
        result = await store_articles_with_mapping(db, articles)
        total_new_articles = result["new_articles"]
        total_new_mappings = result["new_mappings"]
        
        # Update last_synced for these tickers
        await update_ticker_last_synced(db, hot_symbols, today)
        
    except Exception as e:
        logger.error(f"Error processing news fetch: {e}")
        errors = 1
    
    # Update global sync state
    await db.news_sync_state.update_one(
        {"_id": "global"},
        {"$set": {"last_synced_at": started_at}},
        upsert=True
    )
    
    finished_at = datetime.now(timezone.utc)
    
    return {
        "job_type": "news_daily_refresh",
        "status": "completed",
        "hot_symbols_count": len(hot_symbols),
        "api_calls": api_calls,
        "total_articles_fetched": total_articles_fetched,
        "new_articles_stored": total_new_articles,
        "new_ticker_mappings": total_new_mappings,
        "errors": errors,
        "from_date": from_date,
        "to_date": today,
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "duration_ms": int((finished_at - started_at).total_seconds() * 1000),
        # P53: API endpoint info for Admin Panel
        "api_endpoint_template": f"https://eodhd.com/api/news?s={{SYMBOL}}.US&from={{from_date}}&to={{to_date}}&limit={EODHD_FETCH_LIMIT}&offset=0&fmt=json",
        "sample_tickers": hot_symbols[:5],
    }


# Backward-compat alias — the scheduler imports this name.
news_daily_refresh = refresh_hot_tickers_news


async def create_news_indexes(db):
    """Create necessary indexes for news collections."""
    # news_articles indexes
    await db.news_articles.create_index("article_id", unique=True)
    await db.news_articles.create_index("source_link", unique=True, sparse=True)
    await db.news_articles.create_index("published_at")
    
    # article_ticker_mapping indexes
    await db.article_ticker_mapping.create_index([("article_id", 1), ("ticker", 1)], unique=True)
    await db.article_ticker_mapping.create_index("ticker")
    await db.article_ticker_mapping.create_index([("ticker", 1), ("published_at", -1)])
    
    # news_ticker_sync indexes
    await db.news_ticker_sync.create_index("ticker", unique=True)
    
    logger.info("Created news collection indexes")
