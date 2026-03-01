"""
RICHSTOX Talk Routes (v1 API)
==============================
Community posts endpoints.

Endpoints:
- GET /v1/talk - Global talk feed (PUBLIC)
- POST /v1/talk - Create new post (AUTH REQUIRED - 401 if not logged in)
- PATCH /v1/talk/{id} - Edit post (author only, within 15 min)
- DELETE /v1/talk/{id} - Delete post (author or admin)
- POST /v1/talk/{id}/report - Report a post
- GET /v1/stocks/{symbol}/talk - Talk for specific stock (PUBLIC)

=============================================================================
VISIBLE UNIVERSE RULE (PERMANENT)
=============================================================================
All Talk filters and counts are computed ONLY from internal database.
NEVER call EODHD or any external API for filter data.
Single source of truth: `tracked_tickers` collection with is_visible=true.
=============================================================================
"""

from fastapi import APIRouter, Query, Request, HTTPException
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime, timezone
import logging

router = APIRouter(prefix="/v1", tags=["talk"])
logger = logging.getLogger("richstox.talk")

# Edit window in minutes
EDIT_WINDOW_MINUTES = 15

# ============================================================================
# VISIBLE UNIVERSE QUERY (PERMANENT)
# ============================================================================
# Only tickers with is_visible=true may appear in Talk filters.
# is_visible = is_seeded && has_price_data && has_classification
# 
# Where:
# - is_seeded: NYSE/NASDAQ + Common Stock
# - has_price_data: appears in daily bulk prices  
# - has_classification: sector AND industry are non-empty
# ============================================================================

# Import canonical visibility query from visibility_rules (DATA SUPREMACY MANIFESTO v1.0)
from visibility_rules import VISIBLE_TICKERS_QUERY

# VISIBLE UNIVERSE QUERY - use this everywhere
VISIBLE_UNIVERSE_QUERY = VISIBLE_TICKERS_QUERY

# Legacy query alias (for backwards compatibility during transition)
UNIVERSE_QUERY = VISIBLE_UNIVERSE_QUERY


class CreatePostRequest(BaseModel):
    """Request body for creating a talk post."""
    text: str
    symbol: Optional[str] = None
    symbols: Optional[List[str]] = None  # Multiple tickers support


class EditPostRequest(BaseModel):
    """Request body for editing a talk post."""
    text: str


class DeletePostRequest(BaseModel):
    """Request body for deleting a post (admin moderation)."""
    reason: Optional[str] = None


class ReportPostRequest(BaseModel):
    """Request body for reporting a post."""
    reason: str = "inappropriate"


@router.get("/talk/filters")
async def get_talk_filters(request: Request):
    """
    Get available filter options for Talk feed with counts.
    
    UNIVERSE SYSTEM: All data from `tracked_tickers` using UNIVERSE_QUERY.
    No external API calls.
    
    Returns counts for each filter option.
    """
    db = request.app.state.db
    
    # Total universe count
    total_count = await db.tracked_tickers.count_documents(UNIVERSE_QUERY)
    logger.info(f"[FILTERS] Universe total: {total_count} tickers")
    
    # Country counts (should be just US)
    country_counts = []
    async for doc in db.tracked_tickers.aggregate([
        {"$match": UNIVERSE_QUERY},
        {"$group": {"_id": "$country", "count": {"$sum": 1}}},
        {"$sort": {"_id": 1}}
    ]):
        if doc["_id"]:
            country_counts.append({"value": doc["_id"], "count": doc["count"]})
    
    # Exchange counts
    exchange_counts = []
    async for doc in db.tracked_tickers.aggregate([
        {"$match": UNIVERSE_QUERY},
        {"$group": {"_id": "$exchange", "count": {"$sum": 1}}},
        {"$sort": {"_id": 1}}
    ]):
        if doc["_id"]:
            exchange_counts.append({"value": doc["_id"], "count": doc["count"]})
    
    # Sector counts (filter out empty)
    sector_counts = []
    async for doc in db.tracked_tickers.aggregate([
        {"$match": {**UNIVERSE_QUERY, "sector": {"$ne": None, "$ne": ""}}},
        {"$group": {"_id": "$sector", "count": {"$sum": 1}}},
        {"$sort": {"_id": 1}}
    ]):
        if doc["_id"] and doc["_id"].strip():
            sector_counts.append({"value": doc["_id"].strip(), "count": doc["count"]})
    
    # Industry counts (filter out empty)
    industry_counts = []
    async for doc in db.tracked_tickers.aggregate([
        {"$match": {**UNIVERSE_QUERY, "industry": {"$ne": None, "$ne": ""}}},
        {"$group": {"_id": "$industry", "count": {"$sum": 1}}},
        {"$sort": {"_id": 1}}
    ]):
        if doc["_id"] and doc["_id"].strip():
            industry_counts.append({"value": doc["_id"].strip(), "count": doc["count"]})
    
    logger.info(f"[FILTERS] Counts: {len(country_counts)} countries, {len(exchange_counts)} exchanges, {len(sector_counts)} sectors, {len(industry_counts)} industries")
    
    return {
        "country": country_counts if country_counts else [{"value": "US", "count": total_count}],
        "exchange": exchange_counts if exchange_counts else [{"value": "NASDAQ", "count": 0}, {"value": "NYSE", "count": 0}],
        "sector": sector_counts,
        "industry": industry_counts,
        "total_count": total_count,
        # Legacy format for backwards compatibility
        "countries": [c["value"] for c in country_counts] if country_counts else ["US"],
        "exchanges": [e["value"] for e in exchange_counts] if exchange_counts else ["NASDAQ", "NYSE"],
        "sectors": [s["value"] for s in sector_counts],
        "industries": [i["value"] for i in industry_counts],
    }


@router.get("/talk/filters/dependent")
async def get_dependent_filters(
    request: Request,
    exchange: Optional[str] = Query(None),
    sector: Optional[str] = Query(None),
    industry: Optional[str] = Query(None),
):
    """
    Get filter options with dependency rules.
    
    RULE: All data comes from `tracked_tickers` collection ONLY.
    No external API calls.
    
    - Sectors: filtered by exchange
    - Industries: filtered by exchange + selected sectors
    - Returns counts for each option
    """
    db = request.app.state.db
    
    # Parse multi-value params (comma-separated)
    exchanges = [e.strip() for e in exchange.split(',')] if exchange else []
    sectors_selected = [s.strip() for s in sector.split(',')] if sector else []
    industries_selected = [i.strip() for i in industry.split(',')] if industry else []
    
    # Base query starts from universe definition
    base_query = {**UNIVERSE_QUERY}
    
    # Override exchange filter if specified
    if exchanges:
        base_query["exchange"] = {"$in": [e.upper() for e in exchanges]}
    
    # Get sectors available in universe with counts
    # Filter out empty/null sectors in the pipeline
    sector_pipeline = [
        {"$match": {**base_query, "sector": {"$ne": None, "$ne": ""}}},
        {"$group": {"_id": "$sector", "count": {"$sum": 1}}},
        {"$sort": {"_id": 1}}
    ]
    sector_results = await db.tracked_tickers.aggregate(sector_pipeline).to_list(200)
    available_sectors = [{"value": r["_id"].strip(), "count": r["count"]} for r in sector_results if r["_id"] and r["_id"].strip()]
    
    # Get industries - depends on selected sectors
    # Filter out empty/null industries in the pipeline
    industry_query = {**base_query, "industry": {"$ne": None, "$ne": ""}}
    if sectors_selected:
        industry_query["sector"] = {"$in": sectors_selected}
    
    industry_pipeline = [
        {"$match": industry_query},
        {"$group": {"_id": "$industry", "count": {"$sum": 1}}},
        {"$sort": {"_id": 1}}
    ]
    industry_results = await db.tracked_tickers.aggregate(industry_pipeline).to_list(500)
    available_industries = [{"value": r["_id"].strip(), "count": r["count"]} for r in industry_results if r["_id"] and r["_id"].strip()]
    
    # Get company count for current selection (from full universe, not just those with sector/industry)
    company_query = {**base_query}
    if sectors_selected:
        company_query["sector"] = {"$in": sectors_selected}
    if industries_selected:
        company_query["industry"] = {"$in": industries_selected}
    
    company_count = await db.tracked_tickers.count_documents(company_query)
    
    # Exchange counts from universe
    exchange_counts = []
    for ex in ["NASDAQ", "NYSE"]:
        ex_query = {**UNIVERSE_QUERY, "exchange": ex}
        count = await db.tracked_tickers.count_documents(ex_query)
        exchange_counts.append({"value": ex, "count": count})
    
    return {
        "sectors": available_sectors,
        "industries": available_industries,
        "company_count": company_count,
        "exchanges": exchange_counts
    }


@router.get("/talk")
async def get_talk_feed(
    request: Request,
    limit: int = Query(20, ge=1, le=50),
    offset: int = Query(0, ge=0),
    symbol: Optional[str] = Query(None, description="Filter by stock symbol"),
    country: Optional[str] = Query(None, description="Filter by country"),
    exchange: Optional[str] = Query(None, description="Filter by exchange"),
    sector: Optional[str] = Query(None, description="Filter by sector"),
    industry: Optional[str] = Query(None, description="Filter by industry"),
    min_rrr: Optional[float] = Query(None, description="Minimum RRR filter")
):
    """
    Get global talk feed with optional filters.
    
    PUBLIC - no auth required.
    
    Filters:
    - symbol: Show only posts about specific stock
    - country: Filter by country (US)
    - exchange: Filter by exchange (NASDAQ, NYSE)
    - sector: Show only posts about stocks in specific sector
    - industry: Show only posts about stocks in specific industry
    - min_rrr: Show only posts from users with RRR >= value
    """
    from services.talk_service import get_talk_posts
    
    db = request.app.state.db
    
    result = await get_talk_posts(
        db,
        symbol=symbol,
        exchange=exchange,
        sector=sector,
        industry=industry,
        rrr_min=min_rrr,
        limit=limit,
        cursor=None
    )
    
    return {
        "posts": result.get("posts", []),
        "count": len(result.get("posts", [])),
        "offset": offset,
        "has_more": result.get("has_more", False),
    }


@router.post("/talk")
async def create_talk_post_endpoint(
    request: Request,
    body: CreatePostRequest
):
    """
    Create a new talk post.
    
    AUTH REQUIRED - Returns 401 if not logged in.
    """
    from services.talk_service import create_talk_post
    from auth_service import validate_session, get_session_token_from_request
    
    db = request.app.state.db
    
    # Auth required for posting
    session_token = get_session_token_from_request(request)
    if not session_token:
        raise HTTPException(status_code=401, detail="Authentication required to post")
    
    user = await validate_session(db, session_token)
    if not user:
        raise HTTPException(status_code=401, detail="Session expired or invalid")
    
    # Create post
    result = await create_talk_post(
        db,
        user_id=user["user_id"],
        text=body.text,
        symbol=body.symbol,
        symbols=body.symbols
    )
    
    if not result.get("success"):
        raise HTTPException(status_code=400, detail=result.get("error", "Failed to create post"))
    
    # Create notifications for subscribed users
    from services.notification_service import create_notifications_for_post
    post = result.get("post", {})
    # Use all symbols for notifications
    post_symbols = post.get("symbols", []) or ([body.symbol] if body.symbol else [])
    for sym in post_symbols:
        await create_notifications_for_post(
            db,
            post_id=post.get("post_id", ""),
            post_symbol=sym,
            post_user_id=user["user_id"]
        )
    
    return {
        "post": result.get("post"),
        "message": "Post created successfully"
    }


@router.patch("/talk/{post_id}")
async def edit_post(
    request: Request,
    post_id: str,
    body: EditPostRequest
):
    """
    Edit a talk post.
    
    - Only the author can edit
    - Only within 15 minutes of creation
    - Saves edited_at timestamp
    
    AUTH REQUIRED.
    """
    from auth_service import validate_session, get_session_token_from_request
    
    db = request.app.state.db
    
    # Auth required
    session_token = get_session_token_from_request(request)
    if not session_token:
        raise HTTPException(status_code=401, detail="Authentication required")
    
    user = await validate_session(db, session_token)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid session")
    
    # Find the post
    post = await db.talk_posts.find_one({"post_id": post_id})
    if not post:
        raise HTTPException(status_code=404, detail="Post not found")
    
    # Check if deleted
    if post.get("deleted_at"):
        raise HTTPException(status_code=404, detail="Post has been deleted")
    
    # Only author can edit
    if post.get("user_id") != user.get("user_id"):
        raise HTTPException(status_code=403, detail="Only the author can edit this post")
    
    # Check edit window (15 minutes)
    created_at = post.get("created_at")
    if created_at:
        now = datetime.now(timezone.utc)
        if hasattr(created_at, 'replace'):
            created_at = created_at.replace(tzinfo=timezone.utc)
        diff_minutes = (now - created_at).total_seconds() / 60
        if diff_minutes > EDIT_WINDOW_MINUTES:
            raise HTTPException(
                status_code=403, 
                detail=f"Posts can only be edited within {EDIT_WINDOW_MINUTES} minutes of creation"
            )
    
    # Validate text
    text = body.text.strip()
    if len(text) < 10:
        raise HTTPException(status_code=400, detail="Post must be at least 10 characters")
    if len(text) > 2000:
        raise HTTPException(status_code=400, detail="Post must be at most 2000 characters")
    
    # Update the post
    await db.talk_posts.update_one(
        {"post_id": post_id},
        {"$set": {
            "text": text,
            "edited_at": datetime.now(timezone.utc)
        }}
    )
    
    return {
        "success": True,
        "message": "Post updated successfully"
    }


@router.delete("/talk/{post_id}")
async def delete_post(
    request: Request,
    post_id: str,
    reason: Optional[str] = Query(None, description="Deletion reason (for admin moderation)")
):
    """
    Delete a talk post (soft delete).
    
    - Author can delete their own posts
    - Admin can delete any post (moderation)
    - Soft delete: sets deleted_at and deleted_by
    
    AUTH REQUIRED.
    """
    from auth_service import validate_session, get_session_token_from_request
    
    db = request.app.state.db
    
    # Auth required
    session_token = get_session_token_from_request(request)
    if not session_token:
        raise HTTPException(status_code=401, detail="Authentication required")
    
    user = await validate_session(db, session_token)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid session")
    
    # Find the post
    post = await db.talk_posts.find_one({"post_id": post_id})
    if not post:
        raise HTTPException(status_code=404, detail="Post not found")
    
    # Check if already deleted
    if post.get("deleted_at"):
        raise HTTPException(status_code=404, detail="Post already deleted")
    
    user_id = user.get("user_id")
    is_admin = user.get("role") == "admin"
    is_author = post.get("user_id") == user_id
    
    # Only author or admin can delete
    if not is_author and not is_admin:
        raise HTTPException(status_code=403, detail="Only the author or admin can delete this post")
    
    # Soft delete
    update_data = {
        "deleted_at": datetime.now(timezone.utc),
        "deleted_by": user_id,
    }
    if reason and is_admin:
        update_data["deletion_reason"] = reason
    
    await db.talk_posts.update_one(
        {"post_id": post_id},
        {"$set": update_data}
    )
    
    return {
        "success": True,
        "message": "Post deleted successfully"
    }


class SubscriptionToggleRequest(BaseModel):
    """Request body for toggling a subscription."""
    type: str  # country, exchange, sector, industry, symbol
    value: str


class SubscribeRequest(BaseModel):
    type: str
    value: str


@router.post("/talk/subscriptions")
async def subscribe_endpoint(request: Request, body: SubscribeRequest):
    """
    Subscribe to a type/value. Upsert - won't create duplicates.
    AUTH REQUIRED.
    """
    from services.notification_service import subscribe
    from auth_service import validate_session, get_session_token_from_request
    
    db = request.app.state.db
    
    session_token = get_session_token_from_request(request)
    if not session_token:
        raise HTTPException(status_code=401, detail="Authentication required")
    
    user = await validate_session(db, session_token)
    if not user:
        raise HTTPException(status_code=401, detail="Session expired or invalid")
    
    result = await subscribe(db, user["user_id"], body.type, body.value)
    return result


@router.delete("/talk/subscriptions/{subscription_id}")
async def unsubscribe_endpoint(request: Request, subscription_id: str):
    """
    Unsubscribe by exact subscription_id. Always works.
    AUTH REQUIRED.
    """
    from services.notification_service import unsubscribe
    from auth_service import validate_session, get_session_token_from_request
    
    db = request.app.state.db
    
    session_token = get_session_token_from_request(request)
    if not session_token:
        raise HTTPException(status_code=401, detail="Authentication required")
    
    user = await validate_session(db, session_token)
    if not user:
        raise HTTPException(status_code=401, detail="Session expired or invalid")
    
    result = await unsubscribe(db, user["user_id"], subscription_id)
    
    if not result.get("success"):
        raise HTTPException(status_code=404, detail="Subscription not found")
    
    return result


@router.post("/talk/subscriptions/toggle")
async def toggle_subscription_endpoint(
    request: Request,
    body: SubscriptionToggleRequest
):
    """
    Toggle a subscription for the current user.
    
    AUTH REQUIRED.
    """
    from services.notification_service import toggle_subscription
    from auth_service import validate_session, get_session_token_from_request
    
    db = request.app.state.db
    
    # Auth required
    session_token = get_session_token_from_request(request)
    if not session_token:
        raise HTTPException(status_code=401, detail="Authentication required")
    
    user = await validate_session(db, session_token)
    if not user:
        raise HTTPException(status_code=401, detail="Session expired or invalid")
    
    result = await toggle_subscription(db, user["user_id"], body.type, body.value)
    
    if not result.get("success"):
        raise HTTPException(status_code=400, detail=result.get("error", "Failed to toggle subscription"))
    
    return result


@router.get("/talk/subscriptions")
async def get_subscriptions_endpoint(request: Request):
    """
    Get all subscriptions for the current user.
    
    AUTH REQUIRED.
    """
    from services.notification_service import get_user_subscriptions
    from auth_service import validate_session, get_session_token_from_request
    
    db = request.app.state.db
    
    # Auth required
    session_token = get_session_token_from_request(request)
    if not session_token:
        raise HTTPException(status_code=401, detail="Authentication required")
    
    user = await validate_session(db, session_token)
    if not user:
        raise HTTPException(status_code=401, detail="Session expired or invalid")
    
    subscriptions = await get_user_subscriptions(db, user["user_id"])
    
    return {"subscriptions": subscriptions}


@router.post("/talk/{post_id}/report")
async def report_talk_post_endpoint(
    request: Request,
    post_id: str,
    body: ReportPostRequest = None
):
    """
    Report a talk post for moderation.
    
    Can be done anonymously or with auth.
    """
    from services.talk_service import report_talk_post
    from auth_service import validate_session, get_session_token_from_request
    
    db = request.app.state.db
    
    # Get reporter user_id if authenticated
    reporter_user_id = "anonymous"
    session_token = get_session_token_from_request(request)
    if session_token:
        user = await validate_session(db, session_token)
        if user:
            reporter_user_id = user["user_id"]
    
    reason = body.reason if body else "inappropriate"
    
    result = await report_talk_post(db, post_id, reporter_user_id, reason)
    
    if not result.get("success"):
        raise HTTPException(status_code=400, detail=result.get("error", "Failed to report post"))
    
    return result


@router.get("/stocks/{symbol}/talk")
async def get_stock_talk(
    request: Request,
    symbol: str,
    limit: int = Query(20, ge=1, le=50),
    offset: int = Query(0, ge=0)
):
    """
    Get talk posts for a specific stock.
    
    PUBLIC - no auth required.
    """
    from services.talk_service import get_talk_posts
    
    db = request.app.state.db
    
    # Get posts filtered by symbol
    result = await get_talk_posts(
        db,
        symbol=symbol,
        limit=limit,
        cursor=None
    )
    
    return {
        "symbol": symbol.upper(),
        "posts": result.get("posts", []),
        "count": len(result.get("posts", [])),
        "offset": offset,
        "has_more": result.get("has_more", False),
    }



@router.get("/talk/tickers")
async def get_all_tickers(
    request: Request,
    exchange: Optional[str] = Query(None, description="Filter by exchange (comma-separated)"),
    sector: Optional[str] = Query(None, description="Filter by sector (comma-separated)"),
    industry: Optional[str] = Query(None, description="Filter by industry (comma-separated)"),
):
    """
    Get all tickers sorted alphabetically, with optional filters.
    
    PUBLIC - no auth required.
    Supports comma-separated values for union filtering.
    
    RULE: All data comes from `tracked_tickers` collection ONLY.
    Logo URLs are stored in tracked_tickers.logo_url field.
    No external API calls.
    """
    db = request.app.state.db
    
    # Parse comma-separated values
    exchanges = [e.strip() for e in exchange.split(',')] if exchange else []
    sectors = [s.strip() for s in sector.split(',')] if sector else []
    industries = [i.strip() for i in industry.split(',')] if industry else []
    
    # Build filter query - start from universe definition
    query = {**UNIVERSE_QUERY}
    
    # Override with user filters if specified
    if exchanges:
        query["exchange"] = {"$in": exchanges}
    if sectors:
        query["sector"] = {"$in": sectors}
    if industries:
        query["industry"] = {"$in": industries}
    
    # Get tickers from tracked_tickers collection (single source of truth)
    tickers_raw = []
    cursor = db.tracked_tickers.find(
        query,
        {"ticker": 1, "name": 1, "exchange": 1, "sector": 1, "industry": 1, "logo_url": 1, "_id": 0}
    ).sort("ticker", 1)  # Sort alphabetically by ticker
    
    async for stock in cursor:
        tickers_raw.append(stock)
    
    # Build response - logo_url comes directly from tracked_tickers
    tickers = []
    for stock in tickers_raw:
        symbol = stock.get("ticker", "")
        # Remove .US suffix if present for canonical symbol display
        canonical_symbol = symbol.replace(".US", "").upper() if symbol else ""
        
        tickers.append({
            "symbol": canonical_symbol,
            "name": stock.get("name", ""),
            "exchange": stock.get("exchange", ""),
            "sector": stock.get("sector", ""),
            "industry": stock.get("industry", ""),
            "logo_url": stock.get("logo_url"),  # From tracked_tickers only
        })
    
    return {
        "tickers": tickers,
        "count": len(tickers),
        "filters": {
            "exchange": exchange,
            "sector": sector,
            "industry": industry,
        }
    }


@router.get("/talk/filter-counts")
async def get_filter_counts(
    request: Request,
):
    """
    Get counts of tickers per sector, industry, and exchange.
    
    RULE: All data comes from `tracked_tickers` collection ONLY.
    No external API calls.
    
    PUBLIC - no auth required.
    """
    db = request.app.state.db
    
    # Count by sector - filter out empty/null, using universe query
    sector_counts = {}
    async for doc in db.tracked_tickers.aggregate([
        {"$match": {**UNIVERSE_QUERY, "sector": {"$ne": None, "$ne": ""}}},
        {"$group": {"_id": "$sector", "count": {"$sum": 1}}},
        {"$sort": {"_id": 1}}
    ]):
        if doc["_id"] and doc["_id"].strip():
            sector_counts[doc["_id"]] = doc["count"]
    
    # Count by industry - filter out empty/null, using universe query
    industry_counts = {}
    async for doc in db.tracked_tickers.aggregate([
        {"$match": {**UNIVERSE_QUERY, "industry": {"$ne": None, "$ne": ""}}},
        {"$group": {"_id": "$industry", "count": {"$sum": 1}}},
        {"$sort": {"_id": 1}}
    ]):
        if doc["_id"] and doc["_id"].strip():
            industry_counts[doc["_id"]] = doc["count"]
    
    # Count by exchange - using universe query
    exchange_counts = {}
    for ex in ["NASDAQ", "NYSE"]:
        ex_query = {**UNIVERSE_QUERY, "exchange": ex}
        exchange_counts[ex] = await db.tracked_tickers.count_documents(ex_query)
    
    # Total count in universe (6096)
    total_count = await db.tracked_tickers.count_documents(UNIVERSE_QUERY)
    
    return {
        "sector_counts": sector_counts,
        "industry_counts": industry_counts,
        "exchange_counts": exchange_counts,
        "total_count": total_count,
    }


@router.get("/talk/subscriptions/counts")
async def get_subscription_counts(request: Request):
    """
    Get subscription counts by type for the current user.
    
    AUTH REQUIRED.
    
    Returns counts for each subscription type (country, exchange, sector, industry, symbol).
    """
    from services.notification_service import get_user_subscriptions
    from auth_service import validate_session, get_session_token_from_request
    
    db = request.app.state.db
    
    # Auth required
    session_token = get_session_token_from_request(request)
    if not session_token:
        raise HTTPException(status_code=401, detail="Authentication required")
    
    user = await validate_session(db, session_token)
    if not user:
        raise HTTPException(status_code=401, detail="Session expired or invalid")
    
    subscriptions = await get_user_subscriptions(db, user["user_id"])
    
    # Count by type
    counts = {
        "country": 0,
        "exchange": 0, 
        "sector": 0,
        "industry": 0,
        "symbol": 0,
    }
    
    # Deduplicate symbols (handle both AAPL and AAPL.US)
    seen_symbols = set()
    
    for sub in subscriptions:
        sub_type = sub.get("type", "")
        if sub_type == "symbol":
            # Normalize symbol
            value = (sub.get("value") or "").replace(".US", "").upper()
            if value and value not in seen_symbols:
                seen_symbols.add(value)
                counts["symbol"] += 1
        elif sub_type in counts:
            counts[sub_type] += 1
    
    return {"counts": counts}


@router.get("/talk/users")
async def get_talk_users(
    request: Request,
    search: Optional[str] = Query(None, description="Search by username/nickname"),
):
    """
    Get users for @mention and user filter.
    
    PUBLIC - no auth required.
    
    Returns list of users who have posted in Talk.
    """
    db = request.app.state.db
    
    # Get distinct users who have posted
    pipeline = [
        {"$group": {
            "_id": "$user_id",
            "display_name": {"$first": "$user_display_name"},
            "avatar_url": {"$first": "$user_avatar_url"},
            "post_count": {"$sum": 1}
        }},
        {"$sort": {"display_name": 1}}
    ]
    
    if search:
        pipeline.insert(0, {
            "$match": {
                "user_display_name": {"$regex": search, "$options": "i"}
            }
        })
    
    users = []
    async for user in db.talk_posts.aggregate(pipeline):
        users.append({
            "user_id": user["_id"],
            "display_name": user.get("display_name", "Anonymous"),
            "avatar_url": user.get("avatar_url"),
            "post_count": user.get("post_count", 0),
        })
    
    return {
        "users": users,
        "count": len(users),
    }
