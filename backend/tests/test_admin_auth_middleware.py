"""
Tests for AdminAuthMiddleware — Zero Trust enforcement on /api/admin/* endpoints.

3 scenarios tested against POST /api/admin/scheduler/run/fundamentals-sync:
  1. Unauthenticated (no token)        → 401
  2. Authenticated, non-admin (user)   → 403
  3. Authenticated, admin              → 200
"""

import uuid
from datetime import datetime, timezone, timedelta

import pytest
from httpx import AsyncClient, ASGITransport
from motor.motor_asyncio import AsyncIOMotorClient

TEST_DB_NAME = f"richstox_test_{uuid.uuid4().hex[:8]}"
ENDPOINT = "/api/admin/scheduler/run/fundamentals-sync"


@pytest.fixture()
def anyio_backend():
    return "asyncio"


@pytest.fixture()
async def test_db():
    client = AsyncIOMotorClient("mongodb://localhost:27017")
    db = client[TEST_DB_NAME]
    yield db
    await client.drop_database(TEST_DB_NAME)
    client.close()


def _build_app(db):
    from fastapi import FastAPI, APIRouter, BackgroundTasks
    from starlette.middleware.cors import CORSMiddleware
    from admin_middleware import AdminAuthMiddleware

    _app = FastAPI()
    router = APIRouter(prefix="/api")

    @router.post("/admin/scheduler/run/fundamentals-sync")
    async def fundamentals_sync(background_tasks: BackgroundTasks):
        return {"status": "started", "job_type": "fundamentals_sync"}

    @router.post("/admin/auth/seed-admin")
    async def seed():
        return {"seeded": True}

    _app.include_router(router)
    _app.add_middleware(AdminAuthMiddleware, db=db)
    _app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"], allow_methods=["*"],
        allow_headers=["*"], allow_credentials=True,
    )
    return _app


async def _create_user(db, *, role: str) -> str:
    user_id = f"user_{uuid.uuid4().hex[:12]}"
    session_token = f"tok_{uuid.uuid4().hex}"
    now = datetime.now(timezone.utc)
    await db.users.insert_one({
        "user_id": user_id,
        "email": f"{user_id}@test.local",
        "name": "Test",
        "role": role,
        "created_at": now,
        "updated_at": now,
    })
    await db.user_sessions.insert_one({
        "user_id": user_id,
        "session_token": session_token,
        "created_at": now,
        "expires_at": now + timedelta(days=7),
    })
    return session_token


# ─── Test 1: Unauthenticated → 401 ─────────────────────────────────────────

@pytest.mark.anyio
async def test_unauth_returns_401(test_db):
    app = _build_app(test_db)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post(ENDPOINT)
    assert resp.status_code == 401
    assert "Authentication required" in resp.json()["detail"]


# ─── Test 2: Non-admin user → 403 ──────────────────────────────────────────

@pytest.mark.anyio
async def test_non_admin_returns_403(test_db):
    token = await _create_user(test_db, role="user")
    app = _build_app(test_db)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post(
            ENDPOINT,
            headers={"Authorization": f"Bearer {token}"},
        )
    assert resp.status_code == 403
    assert "Admin privileges required" in resp.json()["detail"]


# ─── Test 3: Admin → 200 (happy path) ──────────────────────────────────────

@pytest.mark.anyio
async def test_admin_returns_200(test_db):
    token = await _create_user(test_db, role="admin")
    app = _build_app(test_db)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post(
            ENDPOINT,
            headers={"Authorization": f"Bearer {token}"},
        )
    assert resp.status_code == 200
    assert resp.json()["status"] == "started"


# ─── Test 4 (bonus): seed-admin bootstrap guard ────────────────────────────

@pytest.mark.anyio
async def test_seed_admin_blocked_when_admin_exists(test_db):
    await _create_user(test_db, role="admin")
    app = _build_app(test_db)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post("/api/admin/auth/seed-admin")
    assert resp.status_code == 403
    assert "Admin already exists" in resp.json()["detail"]
