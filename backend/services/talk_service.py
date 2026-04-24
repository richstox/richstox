"""
RICHSTOX Talk Service
======================
Community posts (Talk) for stock discussion.

Features:
- Read-only public access
- Posting requires authentication (401 if not logged in)
- Rate limiting (5 posts/day)
- Report functionality
- RRR badge calculation

CRITICAL RULE: All Talk filters and counts are computed ONLY from internal database.
NEVER call EODHD or any external API for filter data.
Single source of truth: `tracked_tickers` collection.

DB Collection: talk_posts
"""

import uuid
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional

from utils.symbol_utils import normalize_symbol, normalize_symbols

logger = logging.getLogger("richstox.talk")

# Rate limit: posts per day
MAX_POSTS_PER_DAY = 5


async def get_talk_posts(
    db,
    symbol: Optional[str] = None,
    exchange: Optional[str] = None,
    sector: Optional[str] = None,
    industry: Optional[str] = None,
    rrr_min: Optional[float] = None,
    limit: int = 50,
    cursor: Optional[str] = None
) -> Dict[str, Any]:
    """
    Get talk posts with optional filters.
    
    Filter contract:
    - symbol: canonical base ticker (e.g., "AAPL")
    - exchange: "NASDAQ" or "NYSE"
    - sector: display name (e.g., "Technology")
    - industry: display name (e.g., "Software - Application")
    
    Public read - no auth required.
    """
    query = {}
    
    # Always exclude deleted posts from public feed
    query["deleted_at"] = {"$exists": False}
    
    # Debug logging
    logger.info(f"[FEED] Query params: symbol={symbol}, exchange={exchange}, sector={sector}, industry={industry}")
    
    # Collect all ticker filters
    ticker_filters = []
    
    # Direct symbol filter
    if symbol:
        clean_symbol = normalize_symbol(symbol)
        ticker_filters.append(clean_symbol)
        logger.info(f"[FEED] Direct symbol filter: {clean_symbol}")
    
    # Exchange/Sector/Industry -> lookup tickers from tracked_tickers (single source of truth)
    # RULE: All data comes from `tracked_tickers` collection ONLY. No external API calls.
    if exchange or sector or industry:
        fund_query = {
            "country": "US",
            "is_active": True,
        }
        if exchange:
            fund_query["exchange"] = exchange.upper()
        if sector:
            # Sector is display name, case-sensitive match
            fund_query["sector"] = sector
        if industry:
            # Industry is display name, case-sensitive match
            fund_query["industry"] = industry
        
        logger.info(f"[FEED] Fundamentals query: {fund_query}")
        
        # Get matching tickers from tracked_tickers
        tickers = await db.tracked_tickers.distinct("ticker", fund_query)
        # Normalize tickers to canonical format (remove .US suffix)
        matching_tickers = [normalize_symbol(t) for t in tickers if t]
        matching_tickers = [t for t in matching_tickers if t]  # Remove None
        
        logger.info(f"[FEED] Found {len(matching_tickers)} tickers for filters")
        
        if not matching_tickers:
            # No tickers match the filters - return empty
            logger.info("[FEED] No matching tickers, returning empty")
            return {"posts": [], "has_more": False}
        
        # If we also have a direct symbol filter, intersect
        if ticker_filters:
            # Direct symbol must be in the matching set
            intersected = [t for t in ticker_filters if t in matching_tickers]
            if not intersected:
                logger.info("[FEED] Symbol not in filtered set, returning empty")
                return {"posts": [], "has_more": False}
            ticker_filters = intersected
        else:
            ticker_filters = matching_tickers
    
    # Apply ticker filter to query
    if ticker_filters:
        if len(ticker_filters) == 1:
            query["symbol"] = ticker_filters[0]
        else:
            query["symbol"] = {"$in": ticker_filters}
        logger.info(f"[FEED] Final ticker filter: {len(ticker_filters)} symbols")
    
    if cursor:
        query["created_at"] = {"$lt": datetime.fromisoformat(cursor)}
    
    # Get posts
    posts_cursor = db.talk_posts.find(query, {"_id": 0}).sort("created_at", -1).limit(limit + 1)
    posts = await posts_cursor.to_list(length=limit + 1)
    
    logger.info(f"[FEED] Found {len(posts)} posts")
    
    has_more = len(posts) > limit
    posts = posts[:limit]
    
    # Enrich with user data and RRR
    enriched_posts = []
    for post in posts:
        user_id = post.get("user_id")
        user = await db.users.find_one({"user_id": user_id}, {"_id": 0, "user_id": 1, "name": 1, "picture": 1})
        
        # Get user RRR
        rrr = await calculate_user_rrr(db, user_id)
        
        # Apply RRR filter
        if rrr_min is not None:
            if rrr is None or rrr < rrr_min:
                continue
        
        enriched_posts.append({
            **post,
            "symbols": post.get("symbols", [post.get("symbol")] if post.get("symbol") else []),
            "edited_at": post.get("edited_at").isoformat() if post.get("edited_at") and hasattr(post.get("edited_at"), 'isoformat') else post.get("edited_at"),
            "user": user,
            "rrr": rrr,
            "created_at": post.get("created_at").isoformat() if hasattr(post.get("created_at"), 'isoformat') else post.get("created_at"),
        })
    
    next_cursor = posts[-1]["created_at"].isoformat() if has_more and posts else None
    
    return {
        "posts": enriched_posts,
        "has_more": has_more,
        "next_cursor": next_cursor,
    }


async def create_talk_post(
    db,
    user_id: str,
    text: str,
    symbol: Optional[str] = None,
    symbols: Optional[List[str]] = None
) -> Dict[str, Any]:
    """
    Create a new talk post with support for multiple symbols.

    Requires authentication. Returns 401 if not logged in.
    """
    # Validate text
    text = text.strip()
    if len(text) < 10:
        return {
            "success": False,
            "error": "text_too_short",
            "message": "Post must be at least 10 characters",
        }
    
    if len(text) > 2000:
        return {
            "success": False,
            "error": "text_too_long",
            "message": "Post must be at most 2000 characters",
        }
    
    # Check for forbidden content
    forbidden_phrases = ["buy now", "sell now", "guaranteed", "price target", "to the moon"]
    text_lower = text.lower()
    for phrase in forbidden_phrases:
        if phrase in text_lower:
            return {
                "success": False,
                "error": "forbidden_content",
                "message": f"Posts cannot contain '{phrase}'",
            }
    
    # Create post
    post_id = f"post_{uuid.uuid4().hex[:12]}"
    now = datetime.now(timezone.utc)
    
    # Process symbols - normalize to canonical format (e.g., "AAPL.US" -> "AAPL")
    processed_symbols = []
    if symbols and isinstance(symbols, list):
        processed_symbols = normalize_symbols(symbols)[:3]  # Max 3 symbols
    elif symbol:
        normalized = normalize_symbol(symbol)
        if normalized:
            processed_symbols = [normalized]
    
    # Primary symbol for backwards compatibility
    primary_symbol = processed_symbols[0] if processed_symbols else None
    
    post_doc = {
        "post_id": post_id,
        "user_id": user_id,
        "symbol": primary_symbol,  # Primary symbol for filtering
        "symbols": processed_symbols,  # All symbols
        "text": text,
        "created_at": now,
        "reports_count": 0,
    }
    
    await db.talk_posts.insert_one(post_doc)
    
    # Get user info for response
    user = await db.users.find_one({"user_id": user_id}, {"_id": 0, "user_id": 1, "name": 1, "picture": 1})
    rrr = await calculate_user_rrr(db, user_id)
    
    return {
        "success": True,
        "post": {
            "post_id": post_id,
            "user_id": user_id,
            "symbol": primary_symbol,
            "symbols": processed_symbols,  # All symbols for frontend display
            "text": text,
            "created_at": now.isoformat(),
            "user": user,
            "rrr": rrr,
        }
    }


async def report_talk_post(db, post_id: str, reporter_user_id: str, reason: str) -> Dict[str, Any]:
    """
    Report a talk post.
    
    Requires authentication.
    """
    # Check post exists
    post = await db.talk_posts.find_one({"post_id": post_id})
    if not post:
        return {"success": False, "error": "not_found", "message": "Post not found"}
    
    # Check if already reported by this user
    existing_report = await db.talk_reports.find_one({
        "post_id": post_id,
        "reporter_user_id": reporter_user_id
    })
    
    if existing_report:
        return {"success": False, "error": "already_reported", "message": "You already reported this post"}
    
    # Create report
    report_doc = {
        "post_id": post_id,
        "reporter_user_id": reporter_user_id,
        "reason": reason,
        "created_at": datetime.now(timezone.utc),
        "status": "pending",
    }
    
    await db.talk_reports.insert_one(report_doc)
    
    # Increment reports count
    await db.talk_posts.update_one(
        {"post_id": post_id},
        {"$inc": {"reports_count": 1}}
    )
    
    return {"success": True, "message": "Report submitted"}


async def calculate_user_rrr(db, user_id: str) -> Optional[float]:
    """
    Calculate user's RRR (Risk-Reward Ratio).
    
    RRR = max(0, total_return) / abs(max_drawdown) for last 365 days.
    If drawdown = 0, return None.
    """
    # Get user's performance data
    user_perf = await db.user_performance.find_one({"user_id": user_id})
    
    if not user_perf:
        return None
    
    total_return = user_perf.get("total_return_365d", 0)
    max_drawdown = user_perf.get("max_drawdown_365d", 0)
    
    if max_drawdown == 0:
        return None
    
    rrr = max(0, total_return) / abs(max_drawdown)
    return round(rrr, 2)


async def get_user_profile(db, user_id: str) -> Optional[Dict[str, Any]]:
    """
    Get user profile with stats.
    """
    user = await db.users.find_one({"user_id": user_id}, {"_id": 0})
    if not user:
        return None
    
    # Get stats
    posts_count = await db.talk_posts.count_documents({"user_id": user_id})
    
    # Get watchlist count
    watchlist_count = await db.user_watchlist.count_documents({"user_id": user_id})
    
    # Get RRR and performance
    rrr = await calculate_user_rrr(db, user_id)
    user_perf = await db.user_performance.find_one({"user_id": user_id})
    
    return {
        **user,
        "followed_companies_count": watchlist_count,
        "posts_count": posts_count,
        "rrr": rrr,
        "total_return_365d": user_perf.get("total_return_365d") if user_perf else None,
        "max_drawdown_365d": user_perf.get("max_drawdown_365d") if user_perf else None,
        "track_record_days": user_perf.get("track_record_days") if user_perf else None,
        "created_at": user.get("created_at").isoformat() if hasattr(user.get("created_at"), 'isoformat') else user.get("created_at"),
    }


async def get_user_posts(
    db,
    user_id: str,
    limit: int = 50,
    cursor: Optional[str] = None
) -> Dict[str, Any]:
    """
    Get all posts by a user.
    """
    query = {"user_id": user_id}
    
    if cursor:
        query["created_at"] = {"$lt": datetime.fromisoformat(cursor)}
    
    posts_cursor = db.talk_posts.find(query, {"_id": 0}).sort("created_at", -1).limit(limit + 1)
    posts = await posts_cursor.to_list(length=limit + 1)
    
    has_more = len(posts) > limit
    posts = posts[:limit]
    
    # Serialize dates
    for post in posts:
        if hasattr(post.get("created_at"), 'isoformat'):
            post["created_at"] = post["created_at"].isoformat()
    
    next_cursor = posts[-1]["created_at"] if has_more and posts else None
    
    return {
        "posts": posts,
        "has_more": has_more,
        "next_cursor": next_cursor,
    }


async def get_ticker_talk_preview(db, symbol: str, limit: int = 5) -> List[Dict[str, Any]]:
    """
    Get last N talk posts for a ticker (for ticker detail page).
    """
    symbol = symbol.upper()
    
    posts = await db.talk_posts.find(
        {"symbol": symbol},
        {"_id": 0}
    ).sort("created_at", -1).limit(limit).to_list(length=limit)
    
    enriched = []
    for post in posts:
        user = await db.users.find_one(
            {"user_id": post.get("user_id")},
            {"_id": 0, "user_id": 1, "name": 1, "picture": 1}
        )
        rrr = await calculate_user_rrr(db, post.get("user_id"))
        
        enriched.append({
            **post,
            "user": user,
            "rrr": rrr,
            "created_at": post.get("created_at").isoformat() if hasattr(post.get("created_at"), 'isoformat') else post.get("created_at"),
        })
    
    return enriched


async def create_indexes(db) -> None:
    """Create necessary indexes for talk collections."""
    # talk_posts indexes
    await db.talk_posts.create_index("post_id", unique=True)
    await db.talk_posts.create_index("user_id")
    await db.talk_posts.create_index("symbol")
    await db.talk_posts.create_index([("created_at", -1)])
    
    # talk_reports indexes
    await db.talk_reports.create_index("post_id")
    await db.talk_reports.create_index("reporter_user_id")
    await db.talk_reports.create_index([("post_id", 1), ("reporter_user_id", 1)], unique=True)
    
    logger.info("Created talk collection indexes")
