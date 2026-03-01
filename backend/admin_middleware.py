"""
RICHSTOX Admin Middleware
=========================
Protects all /api/admin/* endpoints.
Only users with role='admin' can access.

Admin is determined by:
1. User must be authenticated (valid session)
2. User must have role='admin' in database
3. Initial admin is seeded: kurtarichard@gmail.com
"""

from functools import wraps
from fastapi import Request, HTTPException
from auth_service import validate_session, get_session_token_from_request


async def require_admin(request: Request, db):
    """
    Verify request is from an admin user.
    
    Raises:
        HTTPException 401: Not authenticated
        HTTPException 403: Not authorized (not admin)
    
    Returns:
        User dict if authorized
    """
    # Get session token
    session_token = get_session_token_from_request(request)
    
    if not session_token:
        raise HTTPException(
            status_code=401, 
            detail="Authentication required. Please sign in."
        )
    
    # Validate session and get user
    user = await validate_session(db, session_token)
    
    if not user:
        raise HTTPException(
            status_code=401, 
            detail="Session expired. Please sign in again."
        )
    
    # Check admin role
    if user.get("role") != "admin":
        raise HTTPException(
            status_code=403, 
            detail="Access denied. Admin privileges required."
        )
    
    return user


def admin_required(db):
    """
    Decorator for admin-only endpoints.
    
    Usage:
        @api_router.get("/admin/something")
        @admin_required(db)
        async def admin_endpoint(request: Request):
            ...
    """
    def decorator(func):
        @wraps(func)
        async def wrapper(request: Request, *args, **kwargs):
            await require_admin(request, db)
            return await func(request, *args, **kwargs)
        return wrapper
    return decorator
