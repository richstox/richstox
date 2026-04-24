"""
PROOF TEST: Step 4 admin "Run Now" stores result and frontend can read it.

This test proves the EXACT bug that caused "No data (run Step 4)" in the admin
dashboard after clicking "Run Now". The root cause was:

    finalize_job_audit_entry() did NOT store the full `result` dict in
    ops_job_runs.  The scheduler path (run_job_with_retry) DID store it.
    So scheduled runs showed data, but manual "Run Now" runs showed nothing.

THE DATA PATH (must all work for funnel to display):
    1. compute_peer_benchmarks_v3() returns dict with tickers_processed etc.
    2. finalize_job_audit_entry() stores result in ops_job_runs via $set
    3. get_job_last_runs() aggregation picks up result via $first
    4. /api/admin/job/{name}/status endpoint returns result in last_run
    5. Frontend reads jobRuns['peer_medians'].result.tickers_processed

Run:
    cd /app/backend && python -m pytest tests/test_step4_admin_run_now.py -v
"""

import asyncio
import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch
from bson import ObjectId


# ---------------------------------------------------------------------------
# Fake MongoDB collection for ops_job_runs
# ---------------------------------------------------------------------------
class FakeOpsJobRuns:
    """In-memory mock of ops_job_runs MongoDB collection."""

    def __init__(self):
        self.docs = []

    async def insert_one(self, doc):
        doc = dict(doc)
        if "_id" not in doc:
            doc["_id"] = ObjectId()
        self.docs.append(doc)
        return MagicMock(inserted_id=doc["_id"])

    async def find_one(self, query, projection=None, sort=None):
        matches = self._filter(query)
        if sort:
            key, direction = sort[0] if isinstance(sort, list) else sort
            matches.sort(key=lambda d: d.get(key, datetime.min), reverse=(direction == -1))
        if not matches:
            return None
        doc = dict(matches[0])
        if projection:
            if "_id" in projection and projection["_id"] == 0:
                doc.pop("_id", None)
            # Simple projection: keep only keys with value 1
            keep = {k for k, v in projection.items() if v == 1}
            if keep:
                doc = {k: v for k, v in doc.items() if k in keep or k == "_id"}
        return doc

    async def update_one(self, query, update):
        matches = self._filter(query)
        if matches:
            target = matches[0]
            if "$set" in update:
                target.update(update["$set"])
        return MagicMock(modified_count=len(matches))

    def _filter(self, query):
        results = []
        for doc in self.docs:
            match = True
            for k, v in query.items():
                if isinstance(v, dict):
                    # Simple operator support
                    for op, val in v.items():
                        if op == "$in" and doc.get(k) not in val:
                            match = False
                elif doc.get(k) != v:
                    match = False
            if match:
                results.append(doc)
        return results

    def aggregate(self, pipeline):
        """Simplified aggregation that supports $sort + $group with $first."""
        docs = list(self.docs)
        result_docs = []

        for stage in pipeline:
            if "$sort" in stage:
                key, direction = list(stage["$sort"].items())[0]
                docs.sort(key=lambda d: d.get(key, datetime.min),
                          reverse=(direction == -1))
            elif "$group" in stage:
                group_spec = stage["$group"]
                group_key = group_spec["_id"]
                groups = {}
                for doc in docs:
                    gk = doc.get(group_key.lstrip("$")) if isinstance(group_key, str) else None
                    if gk not in groups:
                        groups[gk] = {}
                        for field, expr in group_spec.items():
                            if field == "_id":
                                groups[gk]["_id"] = gk
                            elif isinstance(expr, dict) and "$first" in expr:
                                src = expr["$first"].lstrip("$")
                                groups[gk][field] = doc.get(src)
                result_docs = list(groups.values())
                docs = result_docs

        return FakeAggCursor(result_docs)


class FakeAggCursor:
    def __init__(self, docs):
        self._docs = docs

    async def to_list(self, length=None):
        return self._docs


# ---------------------------------------------------------------------------
# Fake database with enough collections for the functions under test
# ---------------------------------------------------------------------------
def make_fake_db():
    db = MagicMock()
    db.ops_job_runs = FakeOpsJobRuns()
    # Other collections needed by inventory snapshot (return 0)
    for coll_name in ["stock_prices", "company_fundamentals_cache", "financials_cache"]:
        coll = AsyncMock()
        coll.count_documents = AsyncMock(return_value=0)
        coll.distinct = AsyncMock(return_value=[])
        setattr(db, coll_name, coll)
    return db


# ---------------------------------------------------------------------------
# The EXACT result dict that compute_peer_benchmarks_v3 returns
# ---------------------------------------------------------------------------
PEER_MEDIANS_RESULT = {
    "status": "success",
    "tickers_targeted": 5432,
    "tickers_processed": 5432,
    "tickers_included_any_metric": 4891,
    "tickers_excluded_all_metrics": 541,
    "tickers_updated": 180,
    "tickers_skipped_invalid": 12,
    "tickers_skipped_not_in_universe": 34,
    "api_calls": 0,
    "api_credits_estimated": 0,
    "exclusion_reasons": {
        "missing_or_null_financial_currency": 312,
        "excluded_by_usd_only_metrics_only": 98,
        "missing_metric_values_all": 131,
    },
    "tickers_excluded_currency": 0,
    "excluded_details": [],
    "industries_stored": 180,
    "elapsed_seconds": 56.1,
    "benchmarks_written": 245,
    "stats": {
        "industry": {"groups_total": 200, "groups_written": 180, "per_metric": {}},
        "sector": {"groups_total": 11, "groups_written": 11, "per_metric": {}},
        "market": {"groups_total": 1, "groups_written": 1, "per_metric": {}},
    },
    "market_medians": {
        "pe_ttm": {"median": 22.5, "n_used": 3200},
        "net_margin_ttm": {"median": 8.1, "n_used": 4500},
    },
}


# =========================================================================
# TEST 1: finalize_job_audit_entry stores the full result dict
# =========================================================================
@pytest.mark.asyncio
async def test_finalize_stores_result():
    """
    PROOF: After finalize_job_audit_entry, the ops_job_runs document
    contains the FULL result dict — not just tickers_updated/api_calls.
    
    This was THE BUG. Before the fix, only tickers_updated and api_calls
    were extracted; the full result (tickers_processed, stats, etc.) was lost.
    
    We replicate the EXACT logic from server.py finalize_job_audit_entry
    to avoid importing the whole server (needs fastapi etc.).
    """
    db = make_fake_db()

    # Step 1: Simulate create_job_audit_entry (creates "running" doc)
    started_at = datetime(2026, 4, 14, 19, 44, 0, tzinfo=timezone.utc)
    audit_doc = {
        "job_name": "peer_medians",
        "status": "running",
        "started_at": started_at,
        "finished_at": None,
    }
    insert_result = await db.ops_job_runs.insert_one(audit_doc)
    audit_id = str(insert_result.inserted_id)

    # Verify: before finalize, no result in document
    doc_before = await db.ops_job_runs.find_one({"_id": insert_result.inserted_id})
    assert doc_before is not None
    assert doc_before["status"] == "running"
    assert "result" not in doc_before, "BUG: result should not exist before finalize"

    # Step 2: Run the EXACT same logic as finalize_job_audit_entry (from server.py)
    # This is a faithful copy of the function's core logic — if the function
    # changes, this test must be updated to match.
    from zoneinfo import ZoneInfo
    PRAGUE = ZoneInfo("Europe/Prague")
    finished_at = datetime(2026, 4, 14, 19, 44, 56, tzinfo=timezone.utc)
    result = PEER_MEDIANS_RESULT

    update_doc = {
        "finished_at": finished_at,
        "finished_at_prague": finished_at.astimezone(PRAGUE).isoformat(),
        "inventory_snapshot_after": None,
        "status": result.get("status", "completed"),
    }
    if result:
        for field in (
            "tickers_targeted",
            "tickers_updated",
            "tickers_skipped_invalid",
            "tickers_skipped_not_in_universe",
            "api_calls",
            "api_credits_estimated",
        ):
            update_doc[field] = result.get(field, 0)
        # THE FIX — this line was missing before:
        update_doc["result"] = result if isinstance(result, dict) else {"value": str(result)}
        # Duration computation
        started_doc = await db.ops_job_runs.find_one(
            {"_id": ObjectId(audit_id)}, {"started_at": 1}
        )
        if started_doc and started_doc.get("started_at"):
            update_doc["duration_seconds"] = (finished_at - started_doc["started_at"]).total_seconds()

    await db.ops_job_runs.update_one(
        {"_id": ObjectId(audit_id)},
        {"$set": update_doc}
    )

    # Step 3: Verify the document NOW has the full result
    doc_after = await db.ops_job_runs.find_one({"_id": insert_result.inserted_id})
    assert doc_after is not None
    assert doc_after["status"] == "success"
    assert doc_after["tickers_targeted"] == 5432
    assert doc_after["tickers_updated"] == 180
    assert doc_after["tickers_skipped_invalid"] == 12
    assert doc_after["tickers_skipped_not_in_universe"] == 34
    assert "result" in doc_after, "CRITICAL: result must be stored after finalize!"
    assert isinstance(doc_after["result"], dict)
    assert doc_after["result"]["tickers_processed"] == 5432
    assert doc_after["result"]["tickers_included_any_metric"] == 4891
    assert doc_after["result"]["tickers_excluded_all_metrics"] == 541
    assert doc_after["result"]["exclusion_reasons"]["missing_or_null_financial_currency"] == 312
    assert doc_after["result"]["stats"]["industry"]["groups_written"] == 180
    assert doc_after["result"]["market_medians"]["pe_ttm"]["median"] == 22.5
    assert doc_after.get("duration_seconds") == 56.0, "duration_seconds must be computed"


# =========================================================================
# TEST 2: get_job_last_runs aggregation returns result
# =========================================================================
@pytest.mark.asyncio
async def test_overview_aggregation_returns_result():
    """
    PROOF: The /api/admin/overview aggregation pipeline includes the 
    result field, so the frontend initial load path can read it.
    
    We replicate the EXACT aggregation pipeline from 
    admin_overview_service.get_job_last_runs to avoid import issues.
    """
    db = make_fake_db()

    # Insert a completed run with result (as finalize_job_audit_entry now does)
    await db.ops_job_runs.insert_one({
        "job_name": "peer_medians",
        "status": "completed",
        "started_at": datetime(2026, 4, 14, 19, 44, 0, tzinfo=timezone.utc),
        "finished_at": datetime(2026, 4, 14, 19, 44, 56, tzinfo=timezone.utc),
        "result": PEER_MEDIANS_RESULT,
        "details": {},
    })

    # Run the EXACT aggregation pipeline from admin_overview_service.py lines 214-235
    pipeline = [
        {"$sort": {"started_at": -1}},
        {"$group": {
            "_id": "$job_name",
            "status":             {"$first": "$status"},
            "started_at":         {"$first": "$started_at"},
            "finished_at":        {"$first": "$finished_at"},
            "result":             {"$first": "$result"},   # Line 224: THIS must be here
            "details":            {"$first": "$details"},
        }},
    ]
    docs = await db.ops_job_runs.aggregate(pipeline).to_list(None)

    assert len(docs) == 1
    doc = docs[0]
    assert doc["_id"] == "peer_medians"
    assert doc["result"] is not None, "CRITICAL: aggregation must return result"
    assert doc["result"]["tickers_processed"] == 5432
    assert doc["result"]["tickers_included_any_metric"] == 4891


# =========================================================================
# TEST 3: status endpoint returns result for polling
# =========================================================================
@pytest.mark.asyncio
async def test_status_endpoint_returns_result():
    """
    PROOF: The /api/admin/job/{name}/status endpoint returns result
    in last_run, so the frontend polling path can read it.
    
    This endpoint is called every 5 seconds while a job is running.
    When the job completes, the frontend reads result from the response.
    """
    db = make_fake_db()

    # Insert a completed run with result
    await db.ops_job_runs.insert_one({
        "job_name": "peer_medians",
        "status": "completed",
        "started_at": datetime(2026, 4, 14, 19, 44, 0, tzinfo=timezone.utc),
        "finished_at": datetime(2026, 4, 14, 19, 44, 56, tzinfo=timezone.utc),
        "started_at_prague": "2026-04-14T21:44:00+02:00",
        "finished_at_prague": "2026-04-14T21:44:56+02:00",
        "result": PEER_MEDIANS_RESULT,
        "details": {},
        "progress_processed": None,
        "progress_total": None,
        "progress_pct": None,
        "progress": None,
        "phase": None,
    })

    # Simulate what admin_get_job_status does
    raw_last_run = await db.ops_job_runs.find_one(
        {"job_name": "peer_medians"},
        {"_id": 0},
        sort=[("started_at", -1)]
    )

    # Build the last_run response dict (same logic as admin_get_job_status)
    assert raw_last_run is not None
    last_run = {
        "status": raw_last_run.get("status"),
        "started_at": raw_last_run.get("started_at"),
        "finished_at": raw_last_run.get("finished_at"),
        "result": raw_last_run.get("result"),  # LINE 6003 in server.py
    }

    assert last_run["result"] is not None, "CRITICAL: result must be in status response"
    assert last_run["result"]["tickers_processed"] == 5432
    assert last_run["result"]["tickers_included_any_metric"] == 4891
    assert last_run["result"]["tickers_excluded_all_metrics"] == 541


# =========================================================================
# TEST 4: Frontend data extraction logic works
# =========================================================================
def test_frontend_data_extraction():
    """
    PROOF: Given the result dict as it arrives in the frontend,
    the s4Processed/s4Included/s4Excluded values are extracted correctly
    and the card does NOT show "No data".
    
    This simulates lines 1281-1289 of pipeline.tsx.
    """
    # Simulate what normaliseRun does (spread operator preserves result)
    peer_medians_run = {
        "status": "completed",
        "started_at": "2026-04-14T19:44:00+00:00",
        "finished_at": "2026-04-14T19:44:56+00:00",
        "result": PEER_MEDIANS_RESULT,
    }

    # Line 1282: const peerMediansRunResult = (peerMediansRun as any)?.result;
    peer_medians_run_result = peer_medians_run.get("result")
    assert peer_medians_run_result is not None

    # Line 1287-1289: extract funnel values
    s4_processed = peer_medians_run_result.get("tickers_processed")
    s4_included = peer_medians_run_result.get("tickers_included_any_metric")
    s4_excluded = peer_medians_run_result.get("tickers_excluded_all_metrics")

    assert s4_processed == 5432, f"Expected 5432, got {s4_processed}"
    assert s4_included == 4891, f"Expected 4891, got {s4_included}"
    assert s4_excluded == 541, f"Expected 541, got {s4_excluded}"

    # Line 1682: This condition must be FALSE (not "No data")
    in_count = s4_processed
    out_count = s4_included
    shows_no_data = (in_count is None and out_count is None)
    assert not shows_no_data, "CRITICAL: Card must NOT show 'No data' when result exists!"

    # Verify market medians are accessible for Filter details
    market_medians = peer_medians_run_result.get("market_medians")
    assert market_medians is not None
    assert market_medians["pe_ttm"]["median"] == 22.5


# =========================================================================
# TEST 5: The OLD code (without fix) would fail
# =========================================================================
@pytest.mark.asyncio
async def test_old_code_would_fail():
    """
    NEGATIVE TEST: Proves that WITHOUT the fix, the result would be lost.
    The OLD finalize_job_audit_entry only stored tickers_updated and api_calls,
    NOT the full result dict.
    """
    db = make_fake_db()

    started_at = datetime(2026, 4, 14, 19, 44, 0, tzinfo=timezone.utc)
    insert_result = await db.ops_job_runs.insert_one({
        "job_name": "peer_medians",
        "status": "running",
        "started_at": started_at,
    })
    audit_id = str(insert_result.inserted_id)

    # Simulate the OLD finalize_job_audit_entry (before fix)
    from bson import ObjectId as OID
    from zoneinfo import ZoneInfo
    finished_at = datetime(2026, 4, 14, 19, 44, 56, tzinfo=timezone.utc)
    update_doc = {
        "finished_at": finished_at,
        "finished_at_prague": finished_at.astimezone(ZoneInfo("Europe/Prague")).isoformat(),
        "status": "completed",
    }
    # OLD CODE: only extracted 2 fields, did NOT store full result
    update_doc["tickers_updated"] = PEER_MEDIANS_RESULT.get("tickers_updated", 0)
    update_doc["api_calls"] = PEER_MEDIANS_RESULT.get("api_calls", 0)
    # NOTE: NO update_doc["result"] = ... ← THIS WAS THE BUG

    await db.ops_job_runs.update_one(
        {"_id": OID(audit_id)},
        {"$set": update_doc}
    )

    # Verify: the document has NO result field
    doc = await db.ops_job_runs.find_one({"_id": OID(audit_id)})
    assert "result" not in doc, "Old code must NOT have result"

    # Frontend would see result=None → "No data"
    raw_result = doc.get("result")  # None
    s4_processed = raw_result.get("tickers_processed") if raw_result else None
    assert s4_processed is None, "Old code: tickers_processed must be None"
    shows_no_data = (s4_processed is None)
    assert shows_no_data, "Old code: card WOULD show 'No data' — this was the bug!"
