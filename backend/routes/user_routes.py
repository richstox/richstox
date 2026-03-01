"""
RICHSTOX User Routes (v1 API)
==============================
User profile endpoints.

Endpoints:
- GET /v1/users/{id} - Get user profile (PUBLIC)
- GET /v1/users/{id}/posts - Get user's posts (PUBLIC)
- GET /v1/me/notifications - Get current user's notifications (AUTH)
- POST /v1/me/notifications/mark_seen - Mark notifications as seen (AUTH)
"""

from fastapi import APIRouter, Query, Request, HTTPException
from pydantic import BaseModel
from typing import List, Optional

router = APIRouter(prefix="/v1", tags=["users"])


@router.get("/me/notifications")
async def get_my_notifications(
    request: Request,
    limit: int = Query(20, ge=1, le=50),
    unseen_only: bool = Query(False)
):
    """
    Get notifications for the current user.
    
    AUTH REQUIRED.
    """
    from services.notification_service import get_user_notifications
    from auth_service import validate_session, get_session_token_from_request
    
    db = request.app.state.db
    
    # Auth required
    session_token = get_session_token_from_request(request)
    if not session_token:
        raise HTTPException(status_code=401, detail="Authentication required")
    
    user = await validate_session(db, session_token)
    if not user:
        raise HTTPException(status_code=401, detail="Session expired or invalid")
    
    result = await get_user_notifications(db, user["user_id"], limit=limit, unseen_only=unseen_only)
    
    return result


class MarkSeenRequest(BaseModel):
    """Request body for marking notifications as seen."""
    notification_ids: Optional[List[str]] = None  # If None, marks all as seen


@router.post("/me/notifications/mark_seen")
async def mark_notifications_seen_endpoint(
    request: Request,
    body: MarkSeenRequest = None
):
    """
    Mark notifications as seen.
    
    AUTH REQUIRED.
    """
    from services.notification_service import mark_notifications_seen
    from auth_service import validate_session, get_session_token_from_request
    
    db = request.app.state.db
    
    # Auth required
    session_token = get_session_token_from_request(request)
    if not session_token:
        raise HTTPException(status_code=401, detail="Authentication required")
    
    user = await validate_session(db, session_token)
    if not user:
        raise HTTPException(status_code=401, detail="Session expired or invalid")
    
    notification_ids = body.notification_ids if body else None
    result = await mark_notifications_seen(db, user["user_id"], notification_ids)
    
    return result


@router.get("/me/notifications/count")
async def get_notification_count(request: Request):
    """
    Get unseen notification count for the current user.
    
    AUTH REQUIRED.
    """
    from services.notification_service import get_unseen_count
    from auth_service import validate_session, get_session_token_from_request
    
    db = request.app.state.db
    
    # Auth required
    session_token = get_session_token_from_request(request)
    if not session_token:
        raise HTTPException(status_code=401, detail="Authentication required")
    
    user = await validate_session(db, session_token)
    if not user:
        raise HTTPException(status_code=401, detail="Session expired or invalid")
    
    count = await get_unseen_count(db, user["user_id"])
    
    return {"unseen_count": count}


@router.get("/users/{user_id}")
async def get_user_profile_endpoint(
    request: Request,
    user_id: str
):
    """
    Get user profile with stats.
    
    PUBLIC - no auth required.
    
    Returns:
    - User basic info (name, picture)
    - Stats (post count, RRR, total gains/losses)
    - Recent posts
    """
    from services.talk_service import get_user_profile
    
    db = request.app.state.db
    
    profile = await get_user_profile(db, user_id)
    
    if not profile:
        raise HTTPException(status_code=404, detail="User not found")
    
    return profile


@router.get("/users/{user_id}/posts")
async def get_user_posts_endpoint(
    request: Request,
    user_id: str,
    limit: int = Query(20, ge=1, le=50),
    offset: int = Query(0, ge=0)
):
    """
    Get all posts by a specific user.
    
    PUBLIC - no auth required.
    """
    from services.talk_service import get_user_posts
    
    db = request.app.state.db
    
    # Check if user exists
    user = await db.users.find_one({"user_id": user_id})
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    result = await get_user_posts(db, user_id, limit=limit)
    
    return {
        "user_id": user_id,
        "posts": result.get("posts", []),
        "count": len(result.get("posts", [])),
        "offset": offset,
        "has_more": result.get("has_more", False),
    }
