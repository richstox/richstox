"""
RICHSTOX Auth Service
======================
Google OAuth authentication using Emergent Auth.

Features:
- Google OAuth via Emergent Auth
- Session management (7-day expiry)
- Role-based access (admin/user)
- Admin seed for kurtarichard@gmail.com

REMINDER: DO NOT HARDCODE THE URL, OR ADD ANY FALLBACKS OR REDIRECT URLS, THIS BREAKS THE AUTH
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
EMERGENT_AUTH_URL = "https://demobackend.emergentagent.com/auth/v1/env/oauth/session-data"
ADMIN_EMAIL = "kurtarichard@gmail.com"


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
    """
    Serialize user document for API response.
    Converts datetime objects to ISO strings.
    """
    if not user_doc:
        return None
    
    result = dict(user_doc)
    
    # Ensure subscription_tier has a default
    if 'subscription_tier' not in result:
        result['subscription_tier'] = 'free'
    
    # Convert datetime fields to ISO strings
    for field in ['created_at', 'updated_at']:
        if field in result and result[field]:
            if hasattr(result[field], 'isoformat'):
                result[field] = result[field].isoformat()
    
    return result


class SessionData(BaseModel):
    """Session data from Emergent Auth."""
    id: str
    email: str
    name: str
    picture: Optional[str] = None
    session_token: str


async def exchange_session_id(session_id: str) -> Optional[Dict[str, Any]]:
    """
    Exchange session_id for user data from Emergent Auth.
    
    Args:
        session_id: Temporary session ID from OAuth callback
    
    Returns:
        User data including session_token, or None if invalid
    """
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.get(
                EMERGENT_AUTH_URL,
                headers={"X-Session-ID": session_id}
            )
            
            if response.status_code != 200:
                logger.error(f"Emergent Auth error: {response.status_code} - {response.text}")
                return None
            
            data = response.json()
            logger.info(f"Successfully exchanged session_id for user: {data.get('email')}")
            return data
            
    except Exception as e:
        logger.error(f"Error exchanging session_id: {e}")
        return None


async def create_or_update_user(db, user_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create new user or update existing user.
    
    If user exists by email, updates their info but preserves role.
    If user is admin email, sets role to 'admin'.
    
    Args:
        db: MongoDB database
        user_data: User data from Emergent Auth
    
    Returns:
        User document
    """
    email = user_data.get("email")
    now = datetime.now(timezone.utc)
    
    # Check if user exists
    existing_user = await db.users.find_one({"email": email}, {"_id": 0})
    
    if existing_user:
        # Update existing user (preserve role unless admin)
        update_data = {
            "name": user_data.get("name"),
            "picture": user_data.get("picture"),
            "updated_at": now,
        }
        
        # Admin email always gets admin role
        if email == ADMIN_EMAIL:
            update_data["role"] = "admin"
        
        await db.users.update_one(
            {"email": email},
            {"$set": update_data}
        )
        
        # Return updated user
        return await db.users.find_one({"email": email}, {"_id": 0})
    
    # Create new user
    user_id = f"user_{uuid.uuid4().hex[:12]}"
    
    # Determine role (admin for admin email, user for others)
    role = "admin" if email == ADMIN_EMAIL else "user"
    
    user_doc = {
        "user_id": user_id,
        "email": email,
        "name": user_data.get("name"),
        "picture": user_data.get("picture"),
        "role": role,
        "timezone": None,  # Set during onboarding
        "country": None,
        "created_at": now,
        "updated_at": now,
    }
    
    await db.users.insert_one(user_doc)
    logger.info(f"Created new user: {email} with role: {role}")
    
    # Return without _id
    return await db.users.find_one({"user_id": user_id}, {"_id": 0})


async def create_session(db, user_id: str, session_token: str) -> Dict[str, Any]:
    """
    Create user session.
    
    Args:
        db: MongoDB database
        user_id: User's ID
        session_token: Session token from Emergent Auth
    
    Returns:
        Session document
    """
    now = datetime.now(timezone.utc)
    expires_at = now + timedelta(days=SESSION_EXPIRY_DAYS)
    
    # Delete any existing sessions for this user
    await db.user_sessions.delete_many({"user_id": user_id})
    
    session_doc = {
        "user_id": user_id,
        "session_token": session_token,
        "created_at": now,
        "expires_at": expires_at,
    }
    
    await db.user_sessions.insert_one(session_doc)
    logger.info(f"Created session for user: {user_id}")
    
    return {
        "session_token": session_token,
        "expires_at": expires_at.isoformat(),
    }


async def validate_session(db, session_token: str) -> Optional[Dict[str, Any]]:
    """
    Validate session token and return user data.
    
    Args:
        db: MongoDB database
        session_token: Session token from cookie or header
    
    Returns:
        User data if session is valid, None otherwise
    """
    if not session_token:
        return None
    
    # Find session
    session = await db.user_sessions.find_one(
        {"session_token": session_token},
        {"_id": 0}
    )
    
    if not session:
        logger.debug("Session not found")
        return None
    
    # Check expiry
    expires_at = session.get("expires_at")
    if isinstance(expires_at, str):
        expires_at = datetime.fromisoformat(expires_at)
    if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=timezone.utc)
    
    if expires_at < datetime.now(timezone.utc):
        logger.debug("Session expired")
        await db.user_sessions.delete_one({"session_token": session_token})
        return None
    
    # Get user data
    user = await db.users.find_one(
        {"user_id": session.get("user_id")},
        {"_id": 0}
    )
    
    if not user:
        logger.debug(f"User not found for session: {session.get('user_id')}")
        return None
    
    return user


async def delete_session(db, session_token: str) -> bool:
    """
    Delete user session (logout).
    
    Args:
        db: MongoDB database
        session_token: Session token to delete
    
    Returns:
        True if session was deleted
    """
    result = await db.user_sessions.delete_one({"session_token": session_token})
    return result.deleted_count > 0


async def is_admin(db, session_token: str) -> bool:
    """
    Check if session belongs to admin user.
    
    Args:
        db: MongoDB database
        session_token: Session token
    
    Returns:
        True if user is admin
    """
    user = await validate_session(db, session_token)
    if not user:
        return False
    return user.get("role") == "admin"


async def update_user_timezone(db, user_id: str, timezone_str: str, country: str = None) -> Dict[str, Any]:
    """
    Update user's timezone (for onboarding).
    
    Args:
        db: MongoDB database
        user_id: User ID
        timezone_str: Timezone string (e.g., "Europe/Prague")
        country: Country name
    
    Returns:
        Updated user document
    """
    update_data = {
        "timezone": timezone_str,
        "updated_at": datetime.now(timezone.utc),
    }
    if country:
        update_data["country"] = country
    
    await db.users.update_one(
        {"user_id": user_id},
        {"$set": update_data}
    )
    
    return await db.users.find_one({"user_id": user_id}, {"_id": 0})


async def seed_admin_user(db) -> Dict[str, Any]:
    """
    Seed admin user if not exists.
    
    Creates or updates the admin user with role='admin'.
    
    Returns:
        Admin user document
    """
    existing = await db.users.find_one({"email": ADMIN_EMAIL}, {"_id": 0})
    
    if existing:
        # Ensure role is admin
        if existing.get("role") != "admin":
            await db.users.update_one(
                {"email": ADMIN_EMAIL},
                {"$set": {"role": "admin", "updated_at": datetime.now(timezone.utc)}}
            )
            logger.info(f"Updated {ADMIN_EMAIL} to admin role")
        return await db.users.find_one({"email": ADMIN_EMAIL}, {"_id": 0})
    
    # Create admin user placeholder (will be fully populated on first login)
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
    """
    Extract session token from request (cookie first, then Authorization header).
    
    Args:
        request: FastAPI Request object
    
    Returns:
        Session token or None
    """
    # Try cookie first
    session_token = request.cookies.get("session_token")
    if session_token:
        return session_token
    
    # Fallback to Authorization header
    auth_header = request.headers.get("Authorization")
    if auth_header and auth_header.startswith("Bearer "):
        return auth_header[7:]
    
    return None
