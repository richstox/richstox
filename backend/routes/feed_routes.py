"""
RICHSTOX Feed Routes (v1 API)
==============================
News feed endpoints.

Endpoints:
- GET /v1/me/feed - Personalized feed (based on watchlist)
- GET /v1/stocks/{symbol}/feed - Feed for specific stock
- GET /v1/markets/feed - General market feed
"""

from fastapi import APIRouter, Query, Request, HTTPException
from typing import Optional

router = APIRouter(prefix="/v1", tags=["feed"])


@router.get("/me/feed")
async def get_my_feed(
    request: Request,
    limit: int = Query(20, ge=1, le=50),
    offset: int = Query(0, ge=0)
):
    """
    Get personalized news feed for authenticated user.
    
    Based on user's watchlist tickers.
    If not authenticated, returns default tickers feed.
    """
    from services.news_service import get_feed_for_user
    from auth_service import validate_session, get_session_token_from_request
    
    db = request.app.state.db
    
    # Try to get user (optional auth)
    session_token = get_session_token_from_request(request)
    user = None
    if session_token:
        user = await validate_session(db, session_token)
    
    user_id = user.get("user_id") if user else "anonymous"
    
    feed = await get_feed_for_user(db, user_id, limit=limit, offset=offset)
    
    return {
        "feed": feed["articles"],
        "count": feed["count"],
        "offset": offset,
        "has_more": feed["has_more"],
    }


@router.get("/stocks/{symbol}/feed")
async def get_stock_feed(
    request: Request,
    symbol: str,
    limit: int = Query(20, ge=1, le=50),
    offset: int = Query(0, ge=0)
):
    """
    Get news feed for a specific stock.
    
    Includes lazy fetch: if no cached news exists for this ticker,
    it will fetch from EODHD API (limit=50, offset=0 only).
    
    PUBLIC - no auth required.
    """
    from services.news_service import get_feed_for_symbol, record_ticker_view
    
    db = request.app.state.db
    
    # Record view for HOT ticker calculation
    await record_ticker_view(db, symbol)
    
    # Get feed (with lazy fetch if needed)
    feed = await get_feed_for_symbol(
        db, 
        symbol, 
        limit=limit, 
        offset=offset,
        lazy_fetch=(offset == 0)  # Only lazy fetch on first page
    )
    
    return {
        "symbol": feed["symbol"],
        "feed": feed["articles"],
        "count": feed["count"],
        "offset": offset,
        "has_more": feed["has_more"],
    }


@router.get("/markets/feed")
async def get_market_feed(
    request: Request,
    limit: int = Query(20, ge=1, le=50),
    offset: int = Query(0, ge=0)
):
    """
    Get general market news feed (Market Digest).
    
    PUBLIC - no auth required.
    """
    from services.news_service import get_market_feed
    
    db = request.app.state.db
    
    feed = await get_market_feed(db, limit=limit, offset=offset)
    
    return {
        "feed": feed["articles"],
        "count": feed["count"],
        "offset": offset,
        "has_more": feed["has_more"],
    }
