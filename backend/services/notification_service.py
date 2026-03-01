"""
RICHSTOX Notification Service
==============================
Handles talk subscriptions and notifications.

DB Collections:
- talk_subscriptions: user_id, type, value, created_at
- talk_notifications: user_id, post_id, created_at, seen_at

Types: country, exchange, sector, industry, symbol
"""

import logging
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

from utils.symbol_utils import normalize_symbol

logger = logging.getLogger("richstox.notifications")


def canonicalize_value(value: str, sub_type: str) -> str:
    """
    Create canonical key for subscription value.
    
    For symbol: uppercase, no .US suffix
    For others: collapse whitespace, lowercase
    
    Examples:
        "  Software - Application  " -> "software - application"
        "AAPL.US" -> "AAPL"
    """
    if sub_type == "symbol":
        return normalize_symbol(value) or value.strip().upper()
    # For industry, sector, exchange, country: collapse whitespace + lowercase
    return " ".join(value.split()).strip().lower()


async def toggle_subscription(
    db,
    user_id: str,
    sub_type: str,
    value: str
) -> Dict[str, Any]:
    """
    Toggle a subscription for a user (atomic, idempotent).
    """
    import uuid
    request_id = str(uuid.uuid4())[:8]
    
    valid_types = ["country", "exchange", "sector", "industry", "symbol"]
    if sub_type not in valid_types:
        return {"success": False, "error": f"Invalid type. Must be one of: {valid_types}"}
    
    # Display value (trimmed)
    display_value = value.strip()
    # Canonical value for uniqueness
    canonical_value = canonicalize_value(value, sub_type)
    
    logger.info(f"[TOGGLE_REQ] request_id={request_id} user_id={user_id} type={sub_type} value={display_value} value_canonical={canonical_value}")
    
    # Query uses canonical value for matching
    query = {
        "user_id": user_id,
        "type": sub_type,
        "value_canonical": canonical_value
    }
    
    # Atomic toggle using findOneAndDelete
    deleted = await db.talk_subscriptions.find_one_and_delete(query)
    
    if deleted:
        logger.info(f"[TOGGLE_REQ] request_id={request_id} action=deleted")
        return {
            "success": True,
            "subscribed": False,
            "type": sub_type,
            "value": display_value,
            "value_canonical": canonical_value
        }
    else:
        # Was not subscribed, try to subscribe
        try:
            await db.talk_subscriptions.insert_one({
                "user_id": user_id,
                "type": sub_type,
                "value": display_value,
                "value_canonical": canonical_value,
                "created_at": datetime.now(timezone.utc)
            })
            logger.info(f"[TOGGLE_REQ] request_id={request_id} action=created")
            return {
                "success": True,
                "subscribed": True,
                "type": sub_type,
                "value": display_value,
                "value_canonical": canonical_value
            }
        except Exception as e:
            if "duplicate key" in str(e).lower() or "E11000" in str(e):
                logger.info(f"[TOGGLE_REQ] request_id={request_id} action=duplicate")
                return {
                    "success": True,
                    "subscribed": True,
                    "type": sub_type,
                    "value": display_value,
                    "value_canonical": canonical_value
                }
            raise


async def get_user_subscriptions(db, user_id: str) -> List[Dict[str, Any]]:
    """
    Get all subscriptions for a user.
    Returns _id, type, value, value_canonical for each subscription.
    """
    cursor = db.talk_subscriptions.find({"user_id": user_id})
    subs = await cursor.to_list(100)
    
    result = []
    for s in subs:
        canonical = s.get("value_canonical") or canonicalize_value(s.get("value", ""), s.get("type", ""))
        result.append({
            "subscription_id": str(s["_id"]),
            "type": s.get("type"),
            "value": s.get("value"),
            "value_canonical": canonical
        })
    
    return result


async def subscribe(db, user_id: str, sub_type: str, value: str) -> Dict[str, Any]:
    """
    Subscribe user to a type/value. Upsert - won't create duplicates.
    """
    display_value = value.strip()
    canonical_value = canonicalize_value(value, sub_type)
    
    # Upsert by canonical
    result = await db.talk_subscriptions.find_one_and_update(
        {"user_id": user_id, "type": sub_type, "value_canonical": canonical_value},
        {"$setOnInsert": {
            "user_id": user_id,
            "type": sub_type,
            "value": display_value,
            "value_canonical": canonical_value,
            "created_at": datetime.now(timezone.utc)
        }},
        upsert=True,
        return_document=True
    )
    
    logger.info(f"[SUBSCRIBE] user={user_id} type={sub_type} canonical={canonical_value}")
    
    return {
        "success": True,
        "subscription_id": str(result["_id"]),
        "type": sub_type,
        "value": display_value,
        "value_canonical": canonical_value
    }


async def unsubscribe(db, user_id: str, subscription_id: str) -> Dict[str, Any]:
    """
    Unsubscribe by exact _id. Always works.
    """
    from bson import ObjectId
    
    try:
        obj_id = ObjectId(subscription_id)
    except:
        return {"success": False, "error": "Invalid subscription_id"}
    
    result = await db.talk_subscriptions.delete_one({
        "_id": obj_id,
        "user_id": user_id  # Security: only delete own subscriptions
    })
    
    logger.info(f"[UNSUBSCRIBE] user={user_id} subscription_id={subscription_id} deleted={result.deleted_count}")
    
    return {
        "success": result.deleted_count > 0,
        "subscription_id": subscription_id
    }


async def create_notifications_for_post(
    db,
    post_id: str,
    post_symbol: Optional[str],
    post_user_id: str
) -> int:
    """
    Create notifications for all users subscribed to this post's attributes.
    Called when a new talk post is created.
    
    Args:
        db: MongoDB database
        post_id: ID of the new post
        post_symbol: Stock symbol if provided (e.g., "AAPL")
        post_user_id: Author of the post (won't be notified)
    
    Returns:
        Number of notifications created
    """
    # Derive post metadata
    country = "US"  # All stocks are US for now
    exchange = None
    sector = None
    industry = None
    
    # Get company data if symbol provided
    if post_symbol:
        symbol_clean = normalize_symbol(post_symbol)
        company = await db.company_fundamentals_cache.find_one(
            {"ticker": {"$regex": f"^{symbol_clean}", "$options": "i"}},
            {"exchange": 1, "sector": 1, "industry": 1}
        )
        if company:
            exchange = company.get("exchange")
            sector = company.get("sector")
            industry = company.get("industry")
    
    # Build match criteria for subscriptions
    match_criteria = [
        {"type": "country", "value": country}
    ]
    
    if exchange:
        match_criteria.append({"type": "exchange", "value": exchange})
    if sector:
        match_criteria.append({"type": "sector", "value": sector})
    if industry:
        match_criteria.append({"type": "industry", "value": industry})
    if post_symbol:
        symbol_clean = normalize_symbol(post_symbol)
        # Match only canonical format (no .US suffix anymore after migration)
        match_criteria.append({"type": "symbol", "value": symbol_clean})
    
    # Find all matching subscriptions
    cursor = db.talk_subscriptions.find({
        "$or": match_criteria,
        "user_id": {"$ne": post_user_id}  # Don't notify post author
    })
    
    subscribed_users = set()
    async for sub in cursor:
        subscribed_users.add(sub["user_id"])
    
    # Create notifications
    notifications = []
    for user_id in subscribed_users:
        notifications.append({
            "user_id": user_id,
            "post_id": post_id,
            "created_at": datetime.now(timezone.utc),
            "seen_at": None
        })
    
    if notifications:
        await db.talk_notifications.insert_many(notifications)
        logger.info(f"Created {len(notifications)} notifications for post {post_id}")
    
    return len(notifications)


async def get_user_notifications(
    db,
    user_id: str,
    limit: int = 20,
    unseen_only: bool = False
) -> Dict[str, Any]:
    """
    Get notifications for a user.
    
    Args:
        db: MongoDB database
        user_id: User's ID
        limit: Max notifications
        unseen_only: Only return unseen notifications
    
    Returns:
        Notifications with post data
    """
    query = {"user_id": user_id}
    if unseen_only:
        query["seen_at"] = None
    
    cursor = db.talk_notifications.find(
        query,
        {"_id": 0}
    ).sort("created_at", -1).limit(limit)
    
    notifications = await cursor.to_list(limit)
    
    # Enrich with post data
    enriched = []
    for notif in notifications:
        post = await db.talk_posts.find_one(
            {"post_id": notif["post_id"]},
            {"_id": 0}
        )
        if post:
            # Get post author info
            author = await db.users.find_one(
                {"user_id": post.get("user_id")},
                {"_id": 0, "name": 1, "picture": 1}
            )
            enriched.append({
                **notif,
                "post": post,
                "author": author
            })
    
    # Count unseen
    unseen_count = await db.talk_notifications.count_documents({
        "user_id": user_id,
        "seen_at": None
    })
    
    return {
        "notifications": enriched,
        "unseen_count": unseen_count,
        "count": len(enriched)
    }


async def mark_notifications_seen(
    db,
    user_id: str,
    notification_ids: Optional[List[str]] = None
) -> Dict[str, Any]:
    """
    Mark notifications as seen.
    
    Args:
        db: MongoDB database
        user_id: User's ID
        notification_ids: Specific notification IDs (or all if None)
    
    Returns:
        Update result
    """
    query = {"user_id": user_id, "seen_at": None}
    
    if notification_ids:
        query["post_id"] = {"$in": notification_ids}
    
    result = await db.talk_notifications.update_many(
        query,
        {"$set": {"seen_at": datetime.now(timezone.utc)}}
    )
    
    return {
        "success": True,
        "marked_count": result.modified_count
    }


async def get_unseen_count(db, user_id: str) -> int:
    """
    Get count of unseen notifications for a user.
    """
    return await db.talk_notifications.count_documents({
        "user_id": user_id,
        "seen_at": None
    })


async def create_indexes(db) -> None:
    """Create necessary indexes for notification collections."""
    # talk_subscriptions indexes
    await db.talk_subscriptions.create_index(
        [("user_id", 1), ("type", 1), ("value", 1)],
        unique=True
    )
    await db.talk_subscriptions.create_index("user_id")
    await db.talk_subscriptions.create_index([("type", 1), ("value", 1)])
    
    # talk_notifications indexes
    await db.talk_notifications.create_index("user_id")
    await db.talk_notifications.create_index([("user_id", 1), ("seen_at", 1)])
    await db.talk_notifications.create_index([("created_at", -1)])
    
    logger.info("Created notification collection indexes")
