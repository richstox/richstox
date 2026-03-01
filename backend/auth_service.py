"""
RICHSTOX Auth Service
======================
Google OAuth authentication - direct Google OAuth (no Emergent dependency).

Features:
- Google OAuth directly via Google APIs
- Session management (7-day expiry)
- Role-based access (admin/user)
- Admin seed for kurtarichard@gmail.com
"""

import os
import uuid
import httpx
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any
from pydantic import BaseModel

logger = logging.getLogger("richstox.auth")

# Constants
SESSION_EXPIRY_DAYS = 7
ADMIN_EMAIL = "kurtarichard@gmail.com"
GOOGLE_CLIENT_ID = os.environ.get("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET", "")
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GOOGLE_USERINFO_URL = "https://www.googleapis.com/oauth2/v2/userinfo"


class User(BaseModel):
    """User model for API responses."""
    user_id: str
    email: str
    name: str
    picture: Optional[str] = None
    role: str = "user"
    timezone: Optional[str] = None
    country: Optional[str] = None
    created_at: Optional[str] = None
    subscription_tier: str = "free"  # free, pro, pro_plus


def serialize_user(user_doc: Dict[str, Any]) -> Dict[str, Any]:
    if not user_doc:
        return None
    result = dict(user_doc)
    if 'subscription_tier' not in result:
        result['subscription_tier'] = 'free'
    for field in ['created_at', 'updated_at']:
        if field in result and result[field]:
            if hasattr(result[field], 'isoformat'):
                result[field] = result[field].isoformat()
    return result


async def exchange_google_code(code: str, redirect_uri: str) -> Optional[Dict[str, Any]]:
    """Exchange Google auth code for user data."""
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            token_response = await client.post(
                GOOGLE_TOKEN_URL,
                data={
                    "code": code,
                    "client_id": GOOGLE_CLIENT_ID,
                    "client_secret": GOOGLE_CLIENT_SECRET,
                    "redirect_uri": redirect_uri,
                    "grant_type": "authorization_code",
                }
            )
            if token_response.status_code != 200:
                logger.error(f"Google token error: {token_response.status_code} - {token_response.text}")
                return None
            tokens = token_response.json()
            access_token = tokens.get("access_token")
            userinfo_response = await client.get(
                GOOGLE_USERINFO_URL,
                headers={"Authorization": f"Bearer {access_token}"}
            )
            if userinfo_response.status_code != 200:
                logger.error(f"Google userinfo error: {userinfo_response.status_code}")
                return None
            userinfo = userinfo_response.json()
            return {
                "email": userinfo.get("email"),
                "name": userinfo.get("name"),
                "picture": userinfo.get("picture"),
                "session_token": f"session_{uuid.uuid4().hex}",
            }
    except Exception as e:
        logger.error(f"Error exchanging Google code: {e}")
        return None


async def exchange_session_id(session_id: str) -> Optional[Dict[str, Any]]:
    """Legacy function - kept for compatibility."""
    logger.warning("exchange_session_id called - Emergent Auth no longer supported")
    return None


async def create_or_update_user(db, user_data: Dict[str, Any]) -> Dict[str, Any]:
    email = user_data.get("email")
    now = datetime.now(timezone.utc)
    existing_user = await db.users.find_one({"email": email}, {"_id": 0})
    if existing_user:
        update_data = {
            "name": user_data.get("name"),
            "picture": user_data.get("picture"),
            "updated_at": now,
        }
        if email == ADMIN_EMAIL:
            update_data["role"] = "admin"
        await db.users.update_one({"email": email}, {"$set": update_data})
        return await db.users.find_one({"email": email}, {"_id": 0})
    user_id = f"user_{uuid.uuid4().hex[:12]}"
    role = "admin" if email == ADMIN_EMAIL else "user"
    user_doc = {
        "user_id": user_id,
        "email": email,
        "name": user_data.get("name"),
        "picture": user_data.get("picture"),
        "role": role,
        "timezone": None,
        "country": None,
        "created_at": now,
        "updated_at": now,
    }
    await db.users.insert_one(user_doc)
    logger.info(f"Created new user: {email} with role: {role}")
    return await db.users.find_one({"user_id": user_id}, {"_id": 0})


async def create_session(db, user_id: str, session_token: str) -> Dict[str, Any]:
    now = datetime.now(timezone.utc)
    expires_at = now + timedelta(days=SESSION_EXPIRY_DAYS)
    await db.user_sessions.delete_many({"user_id": user_id})
    session_doc = {
        "user_id": user_id,
        "session_token": session_token,
        "created_at": now,
        "expires_at": expires_at,
    }
    await db.user_sessions.insert_one(session_doc)
    logger.info(f"Created session for user: {user_id}")
    return {"session_token": session_token, "expires_at": expires_at.isoformat()}


async def validate_session(db, session_token: str) -> Optional[Dict[str, Any]]:
    if not session_token:
        return None
    session = await db.user_sessions.find_one({"session_token": session_token}, {"_id": 0})
    if not session:
        return None
    expires_at = session.get("expires_at")
    if isinstance(expires_at, str):
        expires_at = datetime.fromisoformat(expires_at)
    if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=timezone.utc)
    if expires_at < datetime.now(timezone.utc):
        await db.user_sessions.delete_one({"session_token": session_token})
        return None
    user = await db.users.find_one({"user_id": session.get("user_id")}, {"_id": 0})
    return user


async def delete_session(db, session_token: str) -> bool:
    result = await db.user_sessions.delete_one({"session_token": session_token})
    return result.deleted_count > 0


async def is_admin(db, session_token: str) -> bool:
    user = await validate_session(db, session_token)
    if not user:
        return False
    return user.get("role") == "admin"


async def update_user_timezone(db, user_id: str, timezone_str: str, country: str = None) -> Dict[str, Any]:
    update_data = {"timezone": timezone_str, "updated_at": datetime.now(timezone.utc)}
    if country:
        update_data["country"] = country
    await db.users.update_one({"user_id": user_id}, {"$set": update_data})
    return await db.users.find_one({"user_id": user_id}, {"_id": 0})


async def seed_admin_user(db) -> Dict[str, Any]:
    existing = await db.users.find_one({"email": ADMIN_EMAIL}, {"_id": 0})
    if existing:
        if existing.get("role") != "admin":
            await db.users.update_one(
                {"email": ADMIN_EMAIL},
                {"$set": {"role": "admin", "updated_at": datetime.now(timezone.utc)}}
            )
        return await db.users.find_one({"email": ADMIN_EMAIL}, {"_id": 0})
    user_id = f"user_{uuid.uuid4().hex[:12]}"
    now = datetime.now(timezone.utc)
    admin_doc = {
        "user_id": user_id,
        "email": ADMIN_EMAIL,
        "name": "Admin",
        "role": "admin",
        "picture": None,
        "timezone": "Europe/Prague",
        "country": "Czech Republic",
        "created_at": now,
        "updated_at": now,
    }
    await db.users.insert_one(admin_doc)
    logger.info(f"Seeded admin user: {ADMIN_EMAIL}")
    return await db.users.find_one({"email": ADMIN_EMAIL}, {"_id": 0})


def get_session_token_from_request(request) -> Optional[str]:
    session_token = request.cookies.get("session_token")
    if session_token:
        return session_token
    auth_header = request.headers.get("Authorization")
    if auth_header and auth_header.startswith("Bearer "):
        return auth_header[7:]
    return None
