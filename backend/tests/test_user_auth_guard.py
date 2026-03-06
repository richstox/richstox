"""
Tests for UserAuthMiddleware — IDOR & Broken Auth prevention.

6 scenarios across two endpoints:
  POST /api/portfolios           (3 tests)
  DELETE /api/v1/watchlist/{t}   (3 tests)
"""

import uuid
from datetime import datetime, timezone, timedelta

import pytest
from httpx import AsyncClient, ASGITransport
from motor.motor_asyncio import AsyncIOMotorClient

TEST_DB = f"richstox_test_{uuid.uuid4().hex[:8]}"


@pytest.fixture()
def anyio_backend():
    return "asyncio"


@pytest.fixture()
async def test_db():
    client = AsyncIOMotorClient("mongodb://localhost:27017")
    db = client[TEST_DB]
    yield db
    await client.drop_database(TEST_DB)
    client.close()


def _build_app(db):
    from fastapi import FastAPI, APIRouter, HTTPException, Request
    from starlette.middleware.cors import CORSMiddleware
    from auth_guard import UserAuthMiddleware

    app = FastAPI()
    router = APIRouter(prefix="/api")

    # ── Portfolios ──────────────────────────────────────────────────────

    @router.post("/portfolios")
    async def create_portfolio(request: Request):
        user = request.state.user
        doc = {
            "id": str(uuid.uuid4()),
            "name": "Test Portfolio",
            "user_id": user["user_id"],
        }
        await db.portfolios.insert_one(doc)
        return {"id": doc["id"], "user_id": doc["user_id"]}

    # ── Watchlist ───────────────────────────────────────────────────────

    @router.delete("/v1/watchlist/{ticker}")
    async def unfollow(ticker: str, request: Request):
        user = request.state.user
        result = await db.user_watchlist.delete_one(
            {"ticker": ticker.upper(), "user_id": user["user_id"]}
        )
        if result.deleted_count == 0:
            raise HTTPException(404, "Ticker not found in your watchlist")
        return {"ticker": ticker.upper(), "status": "unfollowed"}

    app.include_router(router)
    app.add_middleware(UserAuthMiddleware, db=db)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"], allow_methods=["*"],
        allow_headers=["*"], allow_credentials=True,
    )
    return app


async def _make_user(db, *, user_id: str | None = None) -> tuple[str, str]:
    """Create user + session. Returns (user_id, session_token)."""
    uid = user_id or f"user_{uuid.uuid4().hex[:12]}"
    token = f"tok_{uuid.uuid4().hex}"
    now = datetime.now(timezone.utc)
    await db.users.insert_one({
        "user_id": uid, "email": f"{uid}@test.local",
        "name": "T", "role": "user",
        "created_at": now, "updated_at": now,
    })
    await db.user_sessions.insert_one({
        "user_id": uid, "session_token": token,
        "created_at": now, "expires_at": now + timedelta(days=7),
    })
    return uid, token


# ═══════════════════════════════════════════════════════════════════════════
# POST /api/portfolios
# ═══════════════════════════════════════════════════════════════════════════

@pytest.mark.anyio
async def test_portfolio_create_unauth(test_db):
    """No token → 401."""
    app = _build_app(test_db)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        r = await c.post("/api/portfolios", json={"name": "x"})
    assert r.status_code == 401


@pytest.mark.anyio
async def test_portfolio_create_happy(test_db):
    """Valid session → 200, user_id in response matches session."""
    uid, tok = await _make_user(test_db)
    app = _build_app(test_db)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        r = await c.post(
            "/api/portfolios", json={"name": "x"},
            headers={"Authorization": f"Bearer {tok}"},
        )
    assert r.status_code == 200
    assert r.json()["user_id"] == uid


@pytest.mark.anyio
async def test_portfolio_position_idor(test_db):
    """User B cannot add position to User A's portfolio → 404 (anti-enum)."""
    from fastapi import FastAPI, APIRouter, HTTPException, Request
    from auth_guard import UserAuthMiddleware
    from starlette.middleware.cors import CORSMiddleware

    app = FastAPI()
    router = APIRouter(prefix="/api")

    @router.post("/positions")
    async def add_pos(request: Request):
        user = request.state.user
        portfolio = await test_db.portfolios.find_one(
            {"id": "p_owned_by_a", "user_id": user["user_id"]}
        )
        if not portfolio:
            raise HTTPException(404, "Portfolio not found")
        return {"ok": True}

    app.include_router(router)
    app.add_middleware(UserAuthMiddleware, db=test_db)
    app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"],
                       allow_headers=["*"], allow_credentials=True)

    uid_a, _ = await _make_user(test_db, user_id="user_a")
    _, tok_b = await _make_user(test_db, user_id="user_b")

    await test_db.portfolios.insert_one({"id": "p_owned_by_a", "user_id": uid_a})

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        r = await c.post(
            "/api/positions",
            json={"portfolio_id": "p_owned_by_a"},
            headers={"Authorization": f"Bearer {tok_b}"},
        )
    assert r.status_code == 404


# ═══════════════════════════════════════════════════════════════════════════
# DELETE /api/v1/watchlist/{ticker}
# ═══════════════════════════════════════════════════════════════════════════

@pytest.mark.anyio
async def test_watchlist_delete_unauth(test_db):
    """No token → 401."""
    app = _build_app(test_db)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        r = await c.delete("/api/v1/watchlist/AAPL")
    assert r.status_code == 401


@pytest.mark.anyio
async def test_watchlist_delete_idor(test_db):
    """User B cannot delete User A's watchlist entry → 404 (anti-enum)."""
    uid_a, _ = await _make_user(test_db, user_id="user_a_wl")
    _, tok_b = await _make_user(test_db, user_id="user_b_wl")

    await test_db.user_watchlist.insert_one({
        "ticker": "AAPL", "user_id": uid_a,
        "followed_at": datetime.now(timezone.utc).isoformat(),
    })

    app = _build_app(test_db)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        r = await c.delete(
            "/api/v1/watchlist/AAPL",
            headers={"Authorization": f"Bearer {tok_b}"},
        )
    assert r.status_code == 404

    remaining = await test_db.user_watchlist.count_documents({"ticker": "AAPL", "user_id": uid_a})
    assert remaining == 1, "User A's data must remain untouched"


@pytest.mark.anyio
async def test_watchlist_delete_happy(test_db):
    """Owner deletes own entry → 200."""
    uid, tok = await _make_user(test_db, user_id="user_owner")
    await test_db.user_watchlist.insert_one({
        "ticker": "TSLA", "user_id": uid,
        "followed_at": datetime.now(timezone.utc).isoformat(),
    })

    app = _build_app(test_db)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        r = await c.delete(
            "/api/v1/watchlist/TSLA",
            headers={"Authorization": f"Bearer {tok}"},
        )
    assert r.status_code == 200
    assert r.json()["status"] == "unfollowed"

    remaining = await test_db.user_watchlist.count_documents({"ticker": "TSLA", "user_id": uid})
    assert remaining == 0
