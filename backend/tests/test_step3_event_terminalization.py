"""Tests for Step 3 event terminalization (fundamentals_events status updates).

Verifies that after Step 3 processing, pending events are marked 'completed'
(on success) or 'skipped' (on failure/sweep), so pending counts drop to zero.
"""

import asyncio
from bson import ObjectId
from types import SimpleNamespace
from pymongo.errors import DuplicateKeyError

from scheduler_service import run_fundamentals_changes_sync


# ---------------------------------------------------------------------------
# Mocks
# ---------------------------------------------------------------------------

class _AsyncCursor:
    def __init__(self, docs):
        self._docs = list(docs)

    def sort(self, *_args, **_kwargs):
        return self

    def limit(self, *_args, **_kwargs):
        return self

    async def to_list(self, _length=None):
        return list(self._docs)

    def __aiter__(self):
        self._iter = iter(self._docs)
        return self

    async def __anext__(self):
        try:
            return next(self._iter)
        except StopIteration:
            raise StopAsyncIteration


class _TrackedTickersWithEvents:
    """Tracked-tickers mock: tickers with needs_fundamentals_refresh flag."""

    def __init__(self, docs):
        self._docs = list(docs)
        self.updates = []

    def find(self, query, projection=None):
        if query == {"needs_fundamentals_refresh": True}:
            docs = [{"ticker": d["ticker"]} for d in self._docs if d.get("needs_fundamentals_refresh") is True]
            return _AsyncCursor(docs)
        # STEP3_QUERY-like match
        if (
            query.get("is_visible") is True
            and query.get("exchange", {}).get("$in") == ["NYSE", "NASDAQ"]
            and query.get("asset_type") == "Common Stock"
            and query.get("is_seeded") is True
            and query.get("has_price_data") is True
        ):
            return _AsyncCursor([])
        return _AsyncCursor([])

    async def distinct(self, _field, _query):
        return []

    async def count_documents(self, query):
        if query == {"needs_fundamentals_refresh": True}:
            return sum(1 for d in self._docs if d.get("needs_fundamentals_refresh") is True)
        return len(self._docs)

    async def update_many(self, query, update):
        self.updates.append({"query": query, "update": update})
        return SimpleNamespace(modified_count=0)


class _FundamentalsEventsWithTracking:
    """Fundamentals-events mock that tracks pending events and update_many calls."""

    def __init__(self, events):
        self._events = {e["_id"]: dict(e) for e in events}
        self.update_calls = []
        self._index_created = False

    async def count_documents(self, query):
        status = query.get("status")
        if status:
            return sum(1 for e in self._events.values() if e.get("status") == status)
        return len(self._events)

    def find(self, query, projection=None):
        status = query.get("status")
        if status:
            docs = [dict(e) for e in self._events.values() if e.get("status") == status]
            return _AsyncCursor(docs)
        return _AsyncCursor(list(self._events.values()))

    def aggregate(self, _pipeline):
        return _AsyncCursor([])

    async def update_many(self, query, update):
        self.update_calls.append({"query": dict(query), "update": dict(update)})
        modified = 0
        ids = query.get("_id", {}).get("$in", [])
        status_filter = query.get("status")
        for eid in ids:
            if eid in self._events:
                if status_filter and self._events[eid].get("status") != status_filter:
                    continue
                for k, v in update.get("$set", {}).items():
                    self._events[eid][k] = v
                modified += 1
        return SimpleNamespace(modified_count=modified)

    async def create_index(self, *args, **kwargs):
        self._index_created = True
        return "idx"


class _OpsJobRuns:
    def __init__(self):
        self.docs = {}
        self._seq = 0
        self.update_sets = []

    async def insert_one(self, doc):
        self._seq += 1
        self.docs[self._seq] = dict(doc)
        return SimpleNamespace(inserted_id=self._seq)

    async def find_one(self, filt, projection=None, sort=None):
        doc_id = filt.get("_id")
        if doc_id is not None and doc_id in self.docs:
            return dict(self.docs[doc_id])
        return None

    async def update_one(self, filt, update):
        doc = self.docs.get(filt["_id"], {})
        expected_status = filt.get("status")
        if expected_status is not None and doc.get("status") != expected_status:
            return SimpleNamespace(modified_count=0)
        self.update_sets.append(dict((update.get("$set") or {})))
        for key, value in (update.get("$set", {}) or {}).items():
            if "." not in key:
                doc[key] = value
                continue
            cursor = doc
            parts = key.split(".")
            for part in parts[:-1]:
                if part not in cursor or not isinstance(cursor[part], dict):
                    cursor[part] = {}
                cursor = cursor[part]
            cursor[parts[-1]] = value
        self.docs[filt["_id"]] = doc
        return SimpleNamespace(modified_count=1)

    async def update_many(self, filt, update):
        def _matches_heartbeat_clause(doc, clause):
            heartbeat_clause = clause.get("heartbeat_at")
            if isinstance(heartbeat_clause, dict) and "$exists" in heartbeat_clause:
                expected_exists = heartbeat_clause["$exists"]
                return ("heartbeat_at" in doc) == expected_exists
            if "heartbeat_at" in clause and clause["heartbeat_at"] is None:
                return doc.get("heartbeat_at") is None
            if isinstance(heartbeat_clause, dict) and "$lt" in heartbeat_clause:
                heartbeat = doc.get("heartbeat_at")
                return heartbeat is not None and heartbeat < heartbeat_clause["$lt"]
            return False

        modified = 0
        for doc_id, doc in self.docs.items():
            if doc.get("job_name") != filt.get("job_name"):
                continue
            if doc.get("status") != filt.get("status"):
                continue
            if not any(_matches_heartbeat_clause(doc, clause) for clause in filt.get("$or", [])):
                continue
            for key, value in (update.get("$set", {}) or {}).items():
                if "." not in key:
                    doc[key] = value
                    continue
                cursor = doc
                parts = key.split(".")
                for part in parts[:-1]:
                    if part not in cursor or not isinstance(cursor[part], dict):
                        cursor[part] = {}
                    cursor = cursor[part]
                cursor[parts[-1]] = value
            self.docs[doc_id] = doc
            modified += 1
        return SimpleNamespace(modified_count=modified)


class _OpsLocks:
    def __init__(self):
        self.docs = {}
        self.index_calls = []

    async def update_one(self, filt, update):
        return SimpleNamespace(matched_count=0, modified_count=0)

    async def insert_one(self, doc):
        if doc["_id"] in self.docs:
            raise DuplicateKeyError("duplicate _id")
        self.docs[doc["_id"]] = dict(doc)
        return SimpleNamespace(inserted_id=doc["_id"])

    async def create_index(self, keys, name=None, expireAfterSeconds=None):
        self.index_calls.append({"keys": list(keys), "name": name, "expireAfterSeconds": expireAfterSeconds})
        return name or "index"

    async def delete_one(self, filt):
        doc = self.docs.get(filt.get("_id"))
        if doc and doc.get("owner_run_id") == filt.get("owner_run_id"):
            del self.docs[filt["_id"]]
            return SimpleNamespace(deleted_count=1)
        return SimpleNamespace(deleted_count=0)


class _OpsConfig:
    async def find_one(self, _query):
        return None

    async def delete_one(self, _query):
        return SimpleNamespace(deleted_count=0)


class _FakeEventDB:
    def __init__(self, tracked_docs, events):
        self.tracked_tickers = _TrackedTickersWithEvents(tracked_docs)
        self.fundamentals_events = _FundamentalsEventsWithTracking(events)
        self.ops_job_runs = _OpsJobRuns()
        self.ops_locks = _OpsLocks()
        self.ops_config = _OpsConfig()


# ---------------------------------------------------------------------------
# Shared helpers / monkeypatch fixtures
# ---------------------------------------------------------------------------

def _apply_standard_patches(monkeypatch):
    """Apply the standard set of monkeypatches for Step 3 tests."""

    async def _fake_purge(_db):
        return None

    async def _fake_dedup(_db, _now):
        return {"pending_before_dedup": 0, "deduped_event_count": 0}

    async def _fake_skip(_db, _tickers, _now):
        return {"skipped_tickers": [], "skipped_ticker_count": 0, "skipped_event_count": 0}

    async def _fake_enqueue(*_args, **_kwargs):
        return {"new_inserts": 0, "skipped_existing": 0}

    async def _fake_step3_report(_db, _now):
        return {"step3_exclusion_rows": 0, "exclusion_report_run_id": None}

    async def _fake_visibility(_db, parent_run_id=None):
        return {"job_id": "vis1", "duration_seconds": 0, "stats": {"changed": 0}, "after": {"visible_count": 0}}

    async def _fake_visibility_report(_db, _now):
        return {"visibility_exclusion_rows": 0, "_debug": {}, "exclusion_report_run_id": None}

    monkeypatch.setattr("scheduler_service.purge_orphaned_fundamentals_events", _fake_purge)
    monkeypatch.setattr("scheduler_service._deduplicate_pending_events", _fake_dedup)
    monkeypatch.setattr("scheduler_service._skip_already_complete_tickers", _fake_skip)
    monkeypatch.setattr("scheduler_service._enqueue_fundamentals_events", _fake_enqueue)
    monkeypatch.setattr("scheduler_service.save_step3_exclusion_report", _fake_step3_report)
    monkeypatch.setattr("visibility_rules.recompute_visibility_all", _fake_visibility)
    monkeypatch.setattr("scheduler_service.save_step3_visibility_exclusion_report", _fake_visibility_report)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_successful_sync_marks_events_completed(monkeypatch):
    """When sync_single_ticker_fundamentals succeeds, pending events for that
    ticker must be marked 'completed'."""

    eid1 = ObjectId()
    eid2 = ObjectId()
    events = [
        {"_id": eid1, "ticker": "AAPL.US", "event_type": "split", "status": "pending"},
        {"_id": eid2, "ticker": "AAPL.US", "event_type": "dividend", "status": "pending"},
    ]
    db = _FakeEventDB(
        tracked_docs=[{
            "ticker": "AAPL.US",
            "needs_fundamentals_refresh": True,
            "exchange": "NASDAQ",
            "asset_type": "Common Stock",
            "is_seeded": True,
            "has_price_data": True,
        }],
        events=events,
    )

    async def _fake_sync_single(_db, ticker, source_job=None):
        # sync_single_ticker_fundamentals returns ticker with .US suffix
        t = ticker.upper().strip()
        ticker_full = t if t.endswith(".US") else f"{t}.US"
        return {"ticker": ticker_full, "success": True}

    _apply_standard_patches(monkeypatch)
    monkeypatch.setattr("batch_jobs_service.sync_single_ticker_fundamentals", _fake_sync_single)

    result = asyncio.run(
        run_fundamentals_changes_sync(db, batch_size=1, ignore_kill_switch=True)
    )

    assert result["status"] == "completed"
    assert result["success"] == 1
    assert result["failed"] == 0

    # Both events should be marked 'completed'
    for eid in [eid1, eid2]:
        assert db.fundamentals_events._events[eid]["status"] == "completed"
        assert "completed_at" in db.fundamentals_events._events[eid]


def test_failed_sync_marks_events_skipped(monkeypatch):
    """When sync_single_ticker_fundamentals fails, pending events for that
    ticker must be marked 'skipped' with reason 'sync_failed'."""

    eid1 = ObjectId()
    events = [
        {"_id": eid1, "ticker": "FAIL.US", "event_type": "earnings", "status": "pending"},
    ]
    db = _FakeEventDB(
        tracked_docs=[{
            "ticker": "FAIL.US",
            "needs_fundamentals_refresh": True,
            "exchange": "NYSE",
            "asset_type": "Common Stock",
            "is_seeded": True,
            "has_price_data": True,
        }],
        events=events,
    )

    async def _fake_sync_single(_db, ticker, source_job=None):
        t = ticker.upper().strip()
        ticker_full = t if t.endswith(".US") else f"{t}.US"
        return {"ticker": ticker_full, "success": False, "error": "API error"}

    _apply_standard_patches(monkeypatch)
    monkeypatch.setattr("batch_jobs_service.sync_single_ticker_fundamentals", _fake_sync_single)

    result = asyncio.run(
        run_fundamentals_changes_sync(db, batch_size=1, ignore_kill_switch=True)
    )

    assert result["status"] == "completed"
    assert result["success"] == 0
    assert result["failed"] == 1

    # Event should be marked 'skipped' with sync_failed reason
    evt = db.fundamentals_events._events[eid1]
    assert evt["status"] == "skipped"
    assert evt["skipped_reason"] == "sync_failed"
    assert "skipped_at" in evt


def test_mixed_success_and_failure_terminalize_all(monkeypatch):
    """With a mix of successful and failed tickers, ALL events must be
    terminated — none left pending."""

    eid_ok = ObjectId()
    eid_fail = ObjectId()
    events = [
        {"_id": eid_ok, "ticker": "GOOD.US", "event_type": "split", "status": "pending"},
        {"_id": eid_fail, "ticker": "BAD.US", "event_type": "dividend", "status": "pending"},
    ]
    db = _FakeEventDB(
        tracked_docs=[
            {
                "ticker": "GOOD.US",
                "needs_fundamentals_refresh": True,
                "exchange": "NYSE",
                "asset_type": "Common Stock",
                "is_seeded": True,
                "has_price_data": True,
            },
            {
                "ticker": "BAD.US",
                "needs_fundamentals_refresh": True,
                "exchange": "NASDAQ",
                "asset_type": "Common Stock",
                "is_seeded": True,
                "has_price_data": True,
            },
        ],
        events=events,
    )

    async def _fake_sync_single(_db, ticker, source_job=None):
        t = ticker.upper().strip()
        ticker_full = t if t.endswith(".US") else f"{t}.US"
        if "GOOD" in ticker_full:
            return {"ticker": ticker_full, "success": True}
        return {"ticker": ticker_full, "success": False, "error": "nope"}

    _apply_standard_patches(monkeypatch)
    monkeypatch.setattr("batch_jobs_service.sync_single_ticker_fundamentals", _fake_sync_single)

    result = asyncio.run(
        run_fundamentals_changes_sync(db, batch_size=2, ignore_kill_switch=True)
    )

    assert result["status"] == "completed"

    # Successful ticker → completed
    assert db.fundamentals_events._events[eid_ok]["status"] == "completed"
    # Failed ticker → skipped
    assert db.fundamentals_events._events[eid_fail]["status"] == "skipped"

    # No pending events remain
    pending = sum(
        1 for e in db.fundamentals_events._events.values()
        if e.get("status") == "pending"
    )
    assert pending == 0


def test_no_pending_events_still_zero_pending_after(monkeypatch):
    """When there are no pending events, Step 3 should still complete
    successfully and leave zero pending events."""

    db = _FakeEventDB(
        tracked_docs=[{
            "ticker": "AAPL.US",
            "needs_fundamentals_refresh": True,
            "exchange": "NASDAQ",
            "asset_type": "Common Stock",
            "is_seeded": True,
            "has_price_data": True,
        }],
        events=[],
    )

    async def _fake_sync_single(_db, ticker, source_job=None):
        t = ticker.upper().strip()
        ticker_full = t if t.endswith(".US") else f"{t}.US"
        return {"ticker": ticker_full, "success": True}

    _apply_standard_patches(monkeypatch)
    monkeypatch.setattr("batch_jobs_service.sync_single_ticker_fundamentals", _fake_sync_single)

    result = asyncio.run(
        run_fundamentals_changes_sync(db, batch_size=1, ignore_kill_switch=True)
    )

    assert result["status"] == "completed"
    assert result["success"] == 1
    pending = sum(
        1 for e in db.fundamentals_events._events.values()
        if e.get("status") == "pending"
    )
    assert pending == 0


def test_post_loop_sweep_catches_orphaned_events(monkeypatch):
    """Events for tickers that are flagged but filtered out of tickers_to_sync
    (e.g., not in eligible set) must be swept as 'skipped' by the post-loop
    safety sweep.

    We simulate this by having a pending event for a ticker that has
    needs_fundamentals_refresh=True (so it goes into events_by_ticker)
    but is NOT in the eligible set (so it's dropped from tickers_to_sync).
    After Step 3, the event should be marked skipped by the post-loop sweep.
    """

    eid_orphan = ObjectId()
    eid_normal = ObjectId()
    events = [
        {"_id": eid_orphan, "ticker": "ORPHAN.US", "event_type": "split", "status": "pending"},
        {"_id": eid_normal, "ticker": "NORM.US", "event_type": "dividend", "status": "pending"},
    ]
    db = _FakeEventDB(
        tracked_docs=[
            {
                "ticker": "ORPHAN.US",
                "needs_fundamentals_refresh": True,
                "exchange": "NYSE",
                "asset_type": "Common Stock",
                "is_seeded": True,
                "has_price_data": True,
            },
            {
                "ticker": "NORM.US",
                "needs_fundamentals_refresh": True,
                "exchange": "NASDAQ",
                "asset_type": "Common Stock",
                "is_seeded": True,
                "has_price_data": True,
            },
        ],
        events=events,
    )

    async def _fake_sync_single(_db, ticker, source_job=None):
        t = ticker.upper().strip()
        ticker_full = t if t.endswith(".US") else f"{t}.US"
        return {"ticker": ticker_full, "success": True}

    _apply_standard_patches(monkeypatch)
    monkeypatch.setattr("batch_jobs_service.sync_single_ticker_fundamentals", _fake_sync_single)

    result = asyncio.run(
        run_fundamentals_changes_sync(db, batch_size=2, ignore_kill_switch=True)
    )

    assert result["status"] == "completed"

    # No pending events should remain — both tickers' events should be terminal
    pending = sum(
        1 for e in db.fundamentals_events._events.values()
        if e.get("status") == "pending"
    )
    assert pending == 0
