"""
RICHSTOX Admin Auth Middleware (Zero Trust)
============================================
ASGI middleware that protects ALL /api/admin/* endpoints.

Security model:
  - No valid token/session → HTTP 401 Unauthorized
  - Valid token, role != "admin" → HTTP 403 Forbidden
  - Valid token, role == "admin" → request proceeds

Whitelist:
  - POST /api/admin/auth/seed-admin → dynamic bootstrap guard (only if 0 admins exist)
  - OPTIONS (CORS preflight) → always allowed

Audit logging:
  - Every 401/403 is logged to ops_security_log collection
  - In-memory rate limiter: max 10 denied requests/IP/minute (DoS protection)
"""

import time
import logging
from collections import defaultdict
from datetime import datetime, timezone
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

logger = logging.getLogger("richstox.security")

ADMIN_PREFIX = "/api/admin/"

WHITELIST_PATHS: set[str] = set()

RATE_LIMIT_WINDOW = 60
RATE_LIMIT_MAX = 10


class _RateLimiter:
    """In-memory per-IP rate limiter for audit log writes."""

    def __init__(self, window: int = RATE_LIMIT_WINDOW, max_hits: int = RATE_LIMIT_MAX):
        self._window = window
        self._max = max_hits
        self._buckets: dict[str, list[float]] = defaultdict(list)

    def allow(self, ip: str) -> bool:
        now = time.monotonic()
        bucket = self._buckets[ip]
        bucket[:] = [t for t in bucket if now - t < self._window]
        if len(bucket) >= self._max:
            return False
        bucket.append(now)
        return True


_rate_limiter = _RateLimiter()


class AdminAuthMiddleware(BaseHTTPMiddleware):
    """
    Global guard for /api/admin/* routes.
    Must be registered AFTER CORSMiddleware (Starlette processes
    middleware in reverse registration order, so register this BEFORE CORS
    in the add_middleware chain).
    """

    def __init__(self, app, *, db):
        super().__init__(app)
        self._db = db

    async def dispatch(self, request: Request, call_next):
        path = request.url.path

        if not path.startswith(ADMIN_PREFIX):
            return await call_next(request)

        if request.method == "OPTIONS":
            return await call_next(request)

        if path in WHITELIST_PATHS:
            return await call_next(request)

        # --- Dynamic bootstrap guard for seed-admin ---
        if path == "/api/admin/auth/seed-admin" and request.method == "POST":
            admin_count = await self._db.users.count_documents({"role": "admin"})
            if admin_count > 0:
                return JSONResponse(
                    status_code=403,
                    content={"detail": "Admin already exists. Bootstrap endpoint disabled."},
                )
            return await call_next(request)

        # --- Extract session token ---
        session_token = self._extract_token(request)
        client_ip = request.client.host if request.client else "unknown"

        if not session_token:
            await self._audit(path, request.method, client_ip, None, "no_token")
            return JSONResponse(
                status_code=401,
                content={"detail": "Authentication required. Please sign in."},
            )

        # --- Validate session against DB ---
        from auth_service import validate_session
        user = await validate_session(self._db, session_token)

        if not user:
            await self._audit(path, request.method, client_ip, None, "session_expired")
            return JSONResponse(
                status_code=401,
                content={"detail": "Session expired. Please sign in again."},
            )

        # --- Check admin role ---
        user_id = user.get("user_id")
        if user.get("role") != "admin":
            await self._audit(path, request.method, client_ip, user_id, "not_admin")
            return JSONResponse(
                status_code=403,
                content={"detail": "Access denied. Admin privileges required."},
            )

        return await call_next(request)

    @staticmethod
    def _extract_token(request: Request) -> str | None:
        token = request.cookies.get("session_token")
        if token:
            return token
        auth = request.headers.get("authorization", "")
        if auth.startswith("Bearer "):
            return auth[7:]
        return None

    async def _audit(self, path: str, method: str, ip: str,
                     user_id: str | None, reason: str) -> None:
        if not _rate_limiter.allow(ip):
            return

        status_code = 401 if reason in ("no_token", "session_expired") else 403
        logger.warning(
            "Admin auth denied: path=%s method=%s ip=%s user=%s reason=%s status=%d",
            path, method, ip, user_id or "-", reason, status_code,
        )
        try:
            await self._db.ops_security_log.insert_one({
                "path": path,
                "method": method,
                "ip": ip,
                "user_id": user_id,
                "reason": reason,
                "status_code": status_code,
                "timestamp": datetime.now(timezone.utc),
            })
        except Exception:
            logger.exception("Failed to write security audit log")
