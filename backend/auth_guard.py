"""
RICHSTOX User Auth Guard
=========================
Starlette middleware that protects user-facing write routes.

Protected path prefixes:
  /api/portfolios
  /api/positions
  /api/v1/watchlist
  /api/v1/positions   (legacy aliases)

Read-only GETs on public data (stock prices, search, news, etc.) are NOT gated.

On success the middleware stores the authenticated user dict in
``request.state.user`` so route handlers can read ``request.state.user["user_id"]``
without any manual ``Depends`` boilerplate.

Security model (same as AdminAuthMiddleware):
  - No token  → 401
  - Expired   → 401
  - Valid     → injects user, request continues
"""

import logging
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

logger = logging.getLogger("richstox.auth_guard")

PROTECTED_PREFIXES: tuple[str, ...] = (
    "/api/portfolios",
    "/api/positions",
    "/api/v1/watchlist",
    "/api/v1/positions",
    "/api/v1/me/notifications/mark_seen",
)

SAFE_METHODS = frozenset({"GET", "HEAD", "OPTIONS"})


class UserAuthMiddleware(BaseHTTPMiddleware):
    """
    Router-level guard for user-facing write endpoints.

    GET requests to protected prefixes also require auth (watchlist reads
    are per-user), with the exception of OPTIONS (CORS preflight).
    """

    def __init__(self, app, *, db):
        super().__init__(app)
        self._db = db

    async def dispatch(self, request: Request, call_next):
        path = request.url.path

        if request.method == "OPTIONS":
            return await call_next(request)

        if not self._needs_protection(path):
            return await call_next(request)

        token = self._extract_token(request)
        if not token:
            return JSONResponse(
                status_code=401,
                content={"detail": "Authentication required. Please sign in."},
            )

        from auth_service import validate_session
        user = await validate_session(self._db, token)
        if not user:
            return JSONResponse(
                status_code=401,
                content={"detail": "Session expired. Please sign in again."},
            )

        request.state.user = user
        return await call_next(request)

    @staticmethod
    def _needs_protection(path: str) -> bool:
        return any(path.startswith(p) for p in PROTECTED_PREFIXES)

    @staticmethod
    def _extract_token(request: Request) -> str | None:
        token = request.cookies.get("session_token")
        if token:
            return token
        auth = request.headers.get("authorization", "")
        if auth.startswith("Bearer "):
            return auth[7:]
        return None
