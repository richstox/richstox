"""
Tests for pipeline chain endpoints:
  POST /api/admin/pipeline/run-full-now
  GET  /api/admin/pipeline/chain-status/{chain_run_id}
  GET  /api/admin/pipeline/export/full?chain_run_id=

Auth: AdminAuthMiddleware (all three are /api/admin/* routes).

Structure follows test_admin_auth_middleware.py — minimal FastAPI app,
isolated test DB, anyio/asyncio.
"""

import uuid
from datetime import datetime, timezone, timedelta

import pytest
from httpx import AsyncClient, ASGITransport
from motor.motor_asyncio import AsyncIOMotorClient

TEST_DB_NAME = f"richstox_test_{uuid.uuid4().hex[:8]}"


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


# ── Minimal app ──────────────────────────────────────────────────────────────

def _build_app(db):
    """
    Minimal FastAPI app that replicates the 3 pipeline chain endpoints and
    wraps them with AdminAuthMiddleware — identical to how server.py does it.
    """
    from fastapi import FastAPI, APIRouter, BackgroundTasks, HTTPException, Query
    from fastapi.responses import StreamingResponse
    from starlette.middleware.cors import CORSMiddleware
    from admin_middleware import AdminAuthMiddleware
    import io
    import csv

    _app = FastAPI()
    router = APIRouter(prefix="/api")

    # ── POST /api/admin/pipeline/run-full-now ────────────────────────────────
    @router.post("/admin/pipeline/run-full-now")
    async def run_full_pipeline_now(background_tasks: BackgroundTasks):
        chain_run_id = f"chain_{uuid.uuid4().hex[:12]}"

        async def _run_chain(chain_id: str) -> None:
            await db.pipeline_chain_runs.update_one(
                {"chain_run_id": chain_id},
                {"$set": {"status": "completed", "step_run_ids": {
                    "step1": "s1", "step2": "s2", "step3": "s3", "step4": "s4",
                }}},
            )

        await db.pipeline_chain_runs.insert_one({
            "chain_run_id": chain_run_id,
            "status": "running",
            "started_at": datetime.now(timezone.utc),
            "step_run_ids": {},
        })
        background_tasks.add_task(_run_chain, chain_run_id)
        return {
            "status": "started",
            "chain_run_id": chain_run_id,
            "message": (
                f"Full pipeline chain started (chain_run_id={chain_run_id}). "
                "Poll /api/admin/pipeline/chain-status/<chain_run_id> until "
                "status=completed, then download "
                "/api/admin/pipeline/export/full?chain_run_id=<chain_run_id>."
            ),
        }

    # ── GET /api/admin/pipeline/chain-status/{chain_run_id} ─────────────────
    @router.get("/admin/pipeline/chain-status/{chain_run_id}")
    async def pipeline_chain_status(chain_run_id: str):
        doc = await db.pipeline_chain_runs.find_one(
            {"chain_run_id": chain_run_id}, {"_id": 0}
        )
        if not doc:
            raise HTTPException(status_code=404,
                                detail=f"chain_run_id not found: {chain_run_id}")
        for k in ("started_at", "finished_at"):
            if isinstance(doc.get(k), datetime):
                doc[k] = doc[k].isoformat()
        return doc

    # ── GET /api/admin/pipeline/export/full ──────────────────────────────────
    @router.get("/admin/pipeline/export/full")
    async def pipeline_export_full(
        chain_run_id: str = Query(...),
    ):
        chain_doc = await db.pipeline_chain_runs.find_one(
            {"chain_run_id": chain_run_id}, {"_id": 0}
        )
        if not chain_doc:
            raise HTTPException(status_code=404,
                                detail=f"chain_run_id not found: {chain_run_id}")
        if chain_doc.get("status") not in ("completed", "step3_done"):
            raise HTTPException(
                status_code=409,
                detail=f"Chain not finished (status={chain_doc.get('status')}). "
                       "Wait for status=completed then retry.",
            )
        sids = chain_doc.get("step_run_ids") or {}
        s1_run_id = sids.get("step1")
        if not s1_run_id:
            raise HTTPException(status_code=409, detail="Step 1 run_id missing in chain.")

        output = io.StringIO()
        writer = csv.writer(output, quoting=csv.QUOTE_ALL)
        writer.writerow(["ticker", "name", "step", "reason"])
        async for raw_row in db.universe_seed_raw_rows.find(
            {"run_id": s1_run_id},
            {"_id": 0},
            sort=[("global_raw_row_id", 1)],
        ):
            raw_sym = raw_row.get("raw_symbol") or {}
            code_raw = raw_sym.get("Code") or ""
            code_norm = code_raw.strip().upper()
            name = (raw_sym.get("Name") or "").strip()
            ticker_us = f"{code_norm}.US" if code_norm else "(empty)"
            writer.writerow([ticker_us, name, "OK", "ok"])

        output.seek(0)
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition":
                     f"attachment; filename=pipeline_full_{chain_run_id}.csv"},
        )

    # ── Seed-admin bootstrap (required by AdminAuthMiddleware whitelist) ──────
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


# ── Helpers ──────────────────────────────────────────────────────────────────

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


# ═══════════════════════════════════════════════════════════════════════════════
# POST /api/admin/pipeline/run-full-now
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.mark.anyio
async def test_run_full_now_unauthenticated_401(test_db):
    """No token → 401."""
    app = _build_app(test_db)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        resp = await c.post("/api/admin/pipeline/run-full-now")
    assert resp.status_code == 401


@pytest.mark.anyio
async def test_run_full_now_non_admin_403(test_db):
    """Valid token, non-admin role → 403."""
    token = await _create_user(test_db, role="user")
    app = _build_app(test_db)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        resp = await c.post(
            "/api/admin/pipeline/run-full-now",
            headers={"Authorization": f"Bearer {token}"},
        )
    assert resp.status_code == 403


@pytest.mark.anyio
async def test_run_full_now_admin_returns_chain_run_id(test_db):
    """Admin → 200 with chain_run_id and status=started."""
    token = await _create_user(test_db, role="admin")
    app = _build_app(test_db)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        resp = await c.post(
            "/api/admin/pipeline/run-full-now",
            headers={"Authorization": f"Bearer {token}"},
        )
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "started"
    assert "chain_run_id" in body
    assert body["chain_run_id"].startswith("chain_")
    assert "message" in body


# ═══════════════════════════════════════════════════════════════════════════════
# GET /api/admin/pipeline/chain-status/{chain_run_id}
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.mark.anyio
async def test_chain_status_unauthenticated_401(test_db):
    """No token → 401."""
    app = _build_app(test_db)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        resp = await c.get("/api/admin/pipeline/chain-status/chain_abc123")
    assert resp.status_code == 401


@pytest.mark.anyio
async def test_chain_status_not_found_404(test_db):
    """chain_run_id not in DB → 404."""
    token = await _create_user(test_db, role="admin")
    app = _build_app(test_db)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        resp = await c.get(
            "/api/admin/pipeline/chain-status/chain_nonexistent",
            headers={"Authorization": f"Bearer {token}"},
        )
    assert resp.status_code == 404
    assert "chain_run_id not found" in resp.json()["detail"]


@pytest.mark.anyio
async def test_chain_status_returns_doc(test_db):
    """chain_run_id in DB → 200 with status field."""
    chain_id = f"chain_{uuid.uuid4().hex[:12]}"
    await test_db.pipeline_chain_runs.insert_one({
        "chain_run_id": chain_id,
        "status": "running",
        "started_at": datetime.now(timezone.utc),
        "step_run_ids": {"step1": "s1"},
    })
    token = await _create_user(test_db, role="admin")
    app = _build_app(test_db)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        resp = await c.get(
            f"/api/admin/pipeline/chain-status/{chain_id}",
            headers={"Authorization": f"Bearer {token}"},
        )
    assert resp.status_code == 200
    body = resp.json()
    assert body["chain_run_id"] == chain_id
    assert body["status"] == "running"


@pytest.mark.anyio
async def test_chain_status_completed_doc(test_db):
    """Completed chain → 200 with status=completed and step_run_ids."""
    chain_id = f"chain_{uuid.uuid4().hex[:12]}"
    await test_db.pipeline_chain_runs.insert_one({
        "chain_run_id": chain_id,
        "status": "completed",
        "started_at": datetime.now(timezone.utc),
        "finished_at": datetime.now(timezone.utc),
        "step_run_ids": {"step1": "s1", "step2": "s2", "step3": "s3", "step4": "s4"},
    })
    token = await _create_user(test_db, role="admin")
    app = _build_app(test_db)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        resp = await c.get(
            f"/api/admin/pipeline/chain-status/{chain_id}",
            headers={"Authorization": f"Bearer {token}"},
        )
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "completed"
    assert body["step_run_ids"]["step1"] == "s1"
    assert body["step_run_ids"]["step4"] == "s4"


# ═══════════════════════════════════════════════════════════════════════════════
# GET /api/admin/pipeline/export/full
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.mark.anyio
async def test_export_full_unauthenticated_401(test_db):
    """No token → 401."""
    app = _build_app(test_db)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        resp = await c.get("/api/admin/pipeline/export/full?chain_run_id=chain_abc")
    assert resp.status_code == 401


@pytest.mark.anyio
async def test_export_full_chain_not_found_404(test_db):
    """Unknown chain_run_id → 404."""
    token = await _create_user(test_db, role="admin")
    app = _build_app(test_db)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        resp = await c.get(
            "/api/admin/pipeline/export/full?chain_run_id=chain_unknown",
            headers={"Authorization": f"Bearer {token}"},
        )
    assert resp.status_code == 404
    assert "chain_run_id not found" in resp.json()["detail"]


@pytest.mark.anyio
async def test_export_full_chain_not_finished_409(test_db):
    """chain in 'running' status → 409 (not finished yet)."""
    chain_id = f"chain_{uuid.uuid4().hex[:12]}"
    await test_db.pipeline_chain_runs.insert_one({
        "chain_run_id": chain_id,
        "status": "running",
        "started_at": datetime.now(timezone.utc),
        "step_run_ids": {},
    })
    token = await _create_user(test_db, role="admin")
    app = _build_app(test_db)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        resp = await c.get(
            f"/api/admin/pipeline/export/full?chain_run_id={chain_id}",
            headers={"Authorization": f"Bearer {token}"},
        )
    assert resp.status_code == 409
    assert "not finished" in resp.json()["detail"]


@pytest.mark.anyio
async def test_export_full_completed_returns_csv(test_db):
    """Completed chain with raw rows → 200 CSV with correct columns."""
    chain_id = f"chain_{uuid.uuid4().hex[:12]}"
    s1_run_id = f"run_{uuid.uuid4().hex[:8]}"
    await test_db.pipeline_chain_runs.insert_one({
        "chain_run_id": chain_id,
        "status": "completed",
        "started_at": datetime.now(timezone.utc),
        "finished_at": datetime.now(timezone.utc),
        "step_run_ids": {"step1": s1_run_id, "step2": None, "step3": None, "step4": None},
    })
    # Seed two raw rows for Step 1
    await test_db.universe_seed_raw_rows.insert_many([
        {
            "run_id": s1_run_id,
            "global_raw_row_id": 1,
            "raw_symbol": {"Code": "AAPL", "Name": "Apple Inc"},
        },
        {
            "run_id": s1_run_id,
            "global_raw_row_id": 2,
            "raw_symbol": {"Code": "MSFT", "Name": "Microsoft Corp"},
        },
    ])
    token = await _create_user(test_db, role="admin")
    app = _build_app(test_db)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        resp = await c.get(
            f"/api/admin/pipeline/export/full?chain_run_id={chain_id}",
            headers={"Authorization": f"Bearer {token}"},
        )
    assert resp.status_code == 200
    assert "text/csv" in resp.headers["content-type"]
    assert f"pipeline_full_{chain_id}.csv" in resp.headers["content-disposition"]
    lines = resp.text.strip().splitlines()
    # Header + 2 data rows
    assert len(lines) == 3
    assert lines[0] == '"ticker","name","step","reason"'
    assert "AAPL.US" in lines[1]
    assert "MSFT.US" in lines[2]


@pytest.mark.anyio
async def test_export_full_empty_chain_csv_header_only(test_db):
    """Completed chain with no raw rows → 200 CSV with header only."""
    chain_id = f"chain_{uuid.uuid4().hex[:12]}"
    s1_run_id = f"run_{uuid.uuid4().hex[:8]}"
    await test_db.pipeline_chain_runs.insert_one({
        "chain_run_id": chain_id,
        "status": "completed",
        "started_at": datetime.now(timezone.utc),
        "finished_at": datetime.now(timezone.utc),
        "step_run_ids": {"step1": s1_run_id, "step2": None, "step3": None, "step4": None},
    })
    token = await _create_user(test_db, role="admin")
    app = _build_app(test_db)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        resp = await c.get(
            f"/api/admin/pipeline/export/full?chain_run_id={chain_id}",
            headers={"Authorization": f"Bearer {token}"},
        )
    assert resp.status_code == 200
    lines = resp.text.strip().splitlines()
    assert len(lines) == 1
    assert lines[0] == '"ticker","name","step","reason"'
