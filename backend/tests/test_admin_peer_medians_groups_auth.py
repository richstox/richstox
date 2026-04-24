"""
Auth regression tests for GET /api/admin/peer-medians/groups.

Ensures the endpoint accepts the same admin session cookie mechanism used by
other admin routes and still returns 401 when unauthenticated.
"""

import os
import sys
import uuid
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from fastapi import APIRouter, FastAPI
from httpx import ASGITransport, AsyncClient
from starlette.middleware.cors import CORSMiddleware

os.environ.setdefault("DB_NAME", "test_db")
os.environ.setdefault("ENV", "development")
os.environ.setdefault("MONGO_URI", "mongodb://localhost:27017")
os.environ.setdefault("MONGO_URL", "mongodb://localhost:27017")

_backend = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _backend not in sys.path:
    sys.path.insert(0, _backend)


def _matches(doc, query):
    for key, expected in query.items():
        actual = doc.get(key)
        if isinstance(expected, dict):
            if "$ne" in expected and actual == expected["$ne"]:
                return False
        elif actual != expected:
            return False
    return True


class FakeCollection:
    def __init__(self, docs=None):
        self.docs = list(docs or [])

    async def find_one(self, query, projection=None):
        for doc in self.docs:
            if _matches(doc, query):
                return dict(doc)
        return None

    async def delete_one(self, query):
        before = len(self.docs)
        self.docs = [doc for doc in self.docs if not _matches(doc, query)]
        return SimpleNamespace(deleted_count=before - len(self.docs))

    async def insert_one(self, doc):
        self.docs.append(dict(doc))
        return SimpleNamespace(inserted_id=uuid.uuid4().hex)

    async def count_documents(self, query):
        return sum(1 for doc in self.docs if _matches(doc, query))

    async def distinct(self, field, query):
        seen = []
        for doc in self.docs:
            if _matches(doc, query):
                value = doc.get(field)
                if value not in seen:
                    seen.append(value)
        return seen


def _build_db():
    now = datetime.now(timezone.utc)
    admin_user_id = "admin_test_user"
    session_token = f"tok_{uuid.uuid4().hex}"
    return SimpleNamespace(
        users=FakeCollection([
            {
                "user_id": admin_user_id,
                "email": "admin@test.local",
                "name": "Admin",
                "role": "admin",
                "created_at": now,
                "updated_at": now,
            }
        ]),
        user_sessions=FakeCollection([
            {
                "user_id": admin_user_id,
                "session_token": session_token,
                "created_at": now,
                "expires_at": now + timedelta(days=7),
            }
        ]),
        peer_benchmarks=FakeCollection([
            {"industry": "Software", "sector": "Technology"},
            {"industry": "Semiconductors", "sector": "Technology"},
            {"industry": None, "sector": "Technology"},
        ]),
        ops_security_log=FakeCollection(),
        _session_token=session_token,
    )


def _build_app(db):
    import server as srv
    from admin_middleware import AdminAuthMiddleware

    route = next(
        route for route in srv.api_router.routes
        if getattr(route, "path", "") == "/api/admin/peer-medians/groups"
    )

    app = FastAPI()
    router = APIRouter()
    router.add_api_route(
        "/api/admin/peer-medians/groups",
        route.endpoint,
        methods=["GET"],
    )
    app.include_router(router)
    app.add_middleware(AdminAuthMiddleware, db=db)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
        allow_credentials=True,
    )
    return app, srv


@pytest.fixture()
def anyio_backend():
    return "asyncio"


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("level", "expected_groups"),
    [
        ("industry", ["Semiconductors", "Software"]),
        ("sector", ["Technology"]),
        ("market", ["US Market"]),
    ],
)
async def test_peer_medians_groups_returns_200_for_admin_session_cookie(level, expected_groups):
    fake_db = _build_db()
    app, srv = _build_app(fake_db)
    original_db = srv.db
    srv.db = fake_db
    try:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            client.cookies.set("session_token", fake_db._session_token)
            response = await client.get(f"/api/admin/peer-medians/groups?level={level}")
        assert response.status_code == 200
        assert response.json() == {
            "level": level,
            "groups": expected_groups,
        }
    finally:
        srv.db = original_db


@pytest.mark.anyio
async def test_peer_medians_groups_returns_401_without_auth():
    fake_db = _build_db()
    app, srv = _build_app(fake_db)
    original_db = srv.db
    srv.db = fake_db
    try:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get("/api/admin/peer-medians/groups?level=industry")
        assert response.status_code == 401
    finally:
        srv.db = original_db
