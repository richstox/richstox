import asyncio
from types import SimpleNamespace
from pymongo.errors import DuplicateKeyError

from credit_log_service import get_pipeline_sync_status
from scheduler_service import run_fundamentals_changes_sync


class _AsyncCursor:
    def __init__(self, docs):
        self._docs = list(docs)

    def sort(self, *_args, **_kwargs):
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


class _TrackedTickersForStep3:
    def __init__(self, docs):
        self._docs = list(docs)

    def find(self, query, projection=None):
        if query == {"needs_fundamentals_refresh": True}:
            docs = [{"ticker": d["ticker"]} for d in self._docs if d.get("needs_fundamentals_refresh") is True]
            return _AsyncCursor(docs)
        if (
            query.get("is_visible") is True
            and query.get("exchange", {}).get("$in") == ["NYSE", "NASDAQ"]
            and query.get("asset_type") == "Common Stock"
            and query.get("is_seeded") is True
            and query.get("has_price_data") is True
        ):
            docs = []
            for d in self._docs:
                if d.get("exchange") not in {"NYSE", "NASDAQ"}:
                    continue
                if d.get("asset_type") != "Common Stock":
                    continue
                if d.get("is_seeded") is not True:
                    continue
                if d.get("has_price_data") is not True:
                    continue
                if d.get("is_visible") is not True:
                    continue
                if d.get("price_history_complete") is True and d.get("needs_price_redownload") is not True:
                    continue
                docs.append({
                    "ticker": d["ticker"],
                    "needs_price_redownload": bool(d.get("needs_price_redownload")),
                    "price_history_complete": d.get("price_history_complete"),
                })
            return _AsyncCursor(docs)
        return _AsyncCursor([])

    async def distinct(self, _field, _query):
        return []

    async def count_documents(self, query):
        if query == {"needs_fundamentals_refresh": True}:
            return sum(1 for d in self._docs if d.get("needs_fundamentals_refresh") is True)
        if query.get("fundamentals_status") == "complete":
            return 0
        return len(self._docs)


class _FundamentalsEventsForStep3:
    async def count_documents(self, query):
        if query == {"status": "pending"}:
            return 0
        return 0

    def find(self, _query, _projection=None):
        return _AsyncCursor([])

    def aggregate(self, _pipeline):
        return _AsyncCursor([])

    async def update_many(self, _query, _update):
        return SimpleNamespace(modified_count=0)


class _OpsJobRuns:
    def __init__(self):
        self.docs = {}
        self._seq = 0
        self.update_sets = []

    async def insert_one(self, doc):
        self._seq += 1
        self.docs[self._seq] = dict(doc)
        return SimpleNamespace(inserted_id=self._seq)

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
        doc = self.docs.get(filt.get("_id"))
        if doc is None:
            return SimpleNamespace(matched_count=0, modified_count=0)
        allowed = False
        for clause in filt.get("$or", []):
            if clause.get("owner_run_id") == doc.get("owner_run_id"):
                allowed = True
            expires_clause = clause.get("expires_at")
            if isinstance(expires_clause, dict) and "$lte" in expires_clause:
                expires_at = doc.get("expires_at")
                if expires_at is not None and expires_at <= expires_clause["$lte"]:
                    allowed = True
        if not allowed:
            return SimpleNamespace(matched_count=0, modified_count=0)
        for key, value in (update.get("$set", {}) or {}).items():
            doc[key] = value
        self.docs[filt["_id"]] = doc
        return SimpleNamespace(matched_count=1, modified_count=1)

    async def insert_one(self, doc):
        if doc["_id"] in self.docs:
            raise DuplicateKeyError("duplicate _id")
        self.docs[doc["_id"]] = dict(doc)
        return SimpleNamespace(inserted_id=doc["_id"])

    async def create_index(self, keys, name=None, expireAfterSeconds=None):
        self.index_calls.append({
            "keys": list(keys),
            "name": name,
            "expireAfterSeconds": expireAfterSeconds,
        })
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


class _FakeStep3DB:
    def __init__(self, tracked_docs):
        self.tracked_tickers = _TrackedTickersForStep3(tracked_docs)
        self.fundamentals_events = _FundamentalsEventsForStep3()
        self.ops_job_runs = _OpsJobRuns()
        self.ops_locks = _OpsLocks()
        self.ops_config = _OpsConfig()


def test_step3_processes_ticker_with_refresh_flag_even_without_pending_event(monkeypatch):
    db = _FakeStep3DB([
        {
            "ticker": "AAPL.US",
            "needs_fundamentals_refresh": True,
            "exchange": "NASDAQ",
            "asset_type": "Common Stock",
            "is_seeded": True,
            "has_price_data": True,
        }
    ])
    processed = []

    async def _fake_purge(_db):
        return None

    async def _fake_dedup(_db, _now):
        return {"pending_before_dedup": 0, "deduped_event_count": 0}

    async def _fake_skip(_db, _tickers, _now):
        return {"skipped_tickers": [], "skipped_ticker_count": 0, "skipped_event_count": 0}

    async def _fake_enqueue(*_args, **_kwargs):
        return {"new_inserts": 0, "skipped_existing": 0}

    async def _fake_sync_single(_db, ticker, source_job=None):
        processed.append((ticker, source_job))
        return {"ticker": ticker, "success": True}

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
    monkeypatch.setattr("batch_jobs_service.sync_single_ticker_fundamentals", _fake_sync_single)
    monkeypatch.setattr("scheduler_service.save_step3_exclusion_report", _fake_step3_report)
    monkeypatch.setattr("visibility_rules.recompute_visibility_all", _fake_visibility)
    monkeypatch.setattr("scheduler_service.save_step3_visibility_exclusion_report", _fake_visibility_report)

    result = asyncio.run(
        run_fundamentals_changes_sync(db, batch_size=1, ignore_kill_switch=True)
    )

    assert result["status"] == "completed"
    assert result["processed"] == 1
    assert result["success"] == 1
    assert [p[0] for p in processed] == ["AAPL"]
    assert result["_debug"]["queue_stats"]["actionable_count"] == 1
    run_doc = db.ops_job_runs.docs[1]
    assert "heartbeat_at" in run_doc
    assert run_doc["heartbeat_at"] is not None
    assert any("details.updated_at" in update for update in db.ops_job_runs.update_sets)
    assert any("details.updated_at_prague" in update for update in db.ops_job_runs.update_sets)
    telemetry = run_doc["details"]["step3_telemetry"]
    assert telemetry["phases"]["A"]["status"] in {"done", "error"}
    assert telemetry["phases"]["A"]["processed"] == 1
    assert telemetry["phases"]["A"]["total"] == 1
    assert telemetry["phases"]["B"]["status"] == "done"
    assert telemetry["phases"]["C"]["name"] == "PriceHistory"
    assert telemetry["phases"]["C"]["selection_audit"] == {
        "selection_sources": [],
        "counts_by_source_pre_dedupe": {
            "price_history_incomplete": 0,
            "needs_price_redownload": 0,
        },
        "pre_dedupe_total": 0,
        "post_dedupe_total": 0,
        "counts_by_reason": {
            "price_history_incomplete": 0,
            "needs_price_redownload": 0,
        },
        "sample_tickers_by_reason": {
            "price_history_incomplete": [],
            "needs_price_redownload": [],
        },
        "selection_criteria": "PhaseC: union(price_history_incomplete, needs_price_redownload) then dedupe by ticker",
        "overlap_possible": True,
    }
    assert "updated_at_prague" in telemetry
    assert db.ops_locks.docs == {}
    assert db.ops_locks.index_calls
    assert db.ops_locks.index_calls[0]["keys"] == [("expires_at", 1)]
    assert db.ops_locks.index_calls[0]["expireAfterSeconds"] == 0


def test_step3_single_flight_skips_when_lock_owned_by_another_run():
    from datetime import datetime, timezone, timedelta

    db = _FakeStep3DB([])
    now = datetime.now(timezone.utc)
    db.ops_locks.docs["fundamentals_sync"] = {
        "_id": "fundamentals_sync",
        "owner_run_id": "other-run-id",
        "heartbeat_at": now,
        "expires_at": now + timedelta(minutes=5),
    }

    result = asyncio.run(
        run_fundamentals_changes_sync(db, batch_size=1, ignore_kill_switch=True)
    )

    assert result["status"] == "skipped"
    assert result["reason"] == "single_flight_lock_held"
    run_doc = db.ops_job_runs.docs[1]
    assert run_doc["status"] == "skipped"
    assert run_doc["error"] == "single_flight_lock_held"
    assert run_doc["finished_at"] is not None
    assert db.ops_locks.docs["fundamentals_sync"]["owner_run_id"] == "other-run-id"


def test_step3_phase_c_parallel_counts_failures_per_ticker(monkeypatch):
    db = _FakeStep3DB([
        {
            "ticker": "AAA.US",
            "needs_fundamentals_refresh": False,
            "exchange": "NYSE",
            "asset_type": "Common Stock",
            "is_seeded": True,
            "has_price_data": True,
            "is_visible": True,
            "price_history_complete": False,
            "needs_price_redownload": False,
        },
        {
            "ticker": "BBB.US",
            "needs_fundamentals_refresh": False,
            "exchange": "NYSE",
            "asset_type": "Common Stock",
            "is_seeded": True,
            "has_price_data": True,
            "is_visible": True,
            "price_history_complete": False,
            "needs_price_redownload": True,
        },
        {
            "ticker": "CCC.US",
            "needs_fundamentals_refresh": False,
            "exchange": "NYSE",
            "asset_type": "Common Stock",
            "is_seeded": True,
            "has_price_data": True,
            "is_visible": True,
            "price_history_complete": False,
            "needs_price_redownload": False,
        },
    ])
    phase_c_calls = []

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
        return {"job_id": "vis1", "duration_seconds": 0, "stats": {"changed": 0}, "after": {"visible_count": 3}}

    async def _fake_visibility_report(_db, _now):
        return {"visibility_exclusion_rows": 0, "_debug": {}, "exclusion_report_run_id": None}

    async def _fake_process_price(_db, ticker, job_name=None, needs_redownload=False):
        phase_c_calls.append((ticker, needs_redownload, job_name))
        if ticker == "BBB.US":
            raise RuntimeError("boom")
        await asyncio.sleep(0)
        return {"ticker": ticker, "success": True, "records": 2}

    monkeypatch.setattr("scheduler_service.purge_orphaned_fundamentals_events", _fake_purge)
    monkeypatch.setattr("scheduler_service._deduplicate_pending_events", _fake_dedup)
    monkeypatch.setattr("scheduler_service._skip_already_complete_tickers", _fake_skip)
    monkeypatch.setattr("scheduler_service._enqueue_fundamentals_events", _fake_enqueue)
    monkeypatch.setattr("scheduler_service.save_step3_exclusion_report", _fake_step3_report)
    monkeypatch.setattr("visibility_rules.recompute_visibility_all", _fake_visibility)
    monkeypatch.setattr("scheduler_service.save_step3_visibility_exclusion_report", _fake_visibility_report)
    monkeypatch.setattr("full_sync_service._process_price_ticker", _fake_process_price)

    result = asyncio.run(
        run_fundamentals_changes_sync(db, batch_size=1, ignore_kill_switch=True)
    )

    assert result["status"] == "completed"
    assert result["phase_c_stats"]["tickers_targeted"] == 3
    assert result["phase_c_stats"]["tickers_succeeded"] == 2
    assert result["phase_c_stats"]["tickers_failed"] == 1
    assert result["phase_c_stats"]["total_records"] == 4
    assert sorted([t for (t, _, _) in phase_c_calls]) == ["AAA.US", "BBB.US", "CCC.US"]
    assert len(phase_c_calls) == 3
    telemetry_c = result["step3_telemetry"]["phases"]["C"]
    selection_audit = telemetry_c["selection_audit"]
    assert selection_audit["selection_sources"] == ["price_history_incomplete", "needs_price_redownload"]
    assert selection_audit["counts_by_source_pre_dedupe"] == {
        "price_history_incomplete": 3,
        "needs_price_redownload": 1,
    }
    assert selection_audit["pre_dedupe_total"] == 3
    assert selection_audit["post_dedupe_total"] == 3
    assert selection_audit["counts_by_reason"] == {
        "price_history_incomplete": 3,
        "needs_price_redownload": 1,
    }
    assert selection_audit["sample_tickers_by_reason"]["price_history_incomplete"] == ["AAA.US", "BBB.US", "CCC.US"]
    assert selection_audit["sample_tickers_by_reason"]["needs_price_redownload"] == ["BBB.US"]
    assert selection_audit["selection_criteria"]
    assert selection_audit["overlap_possible"] is True
    assert all(len(v) <= 10 for v in selection_audit["sample_tickers_by_reason"].values())
    assert all(v <= selection_audit["post_dedupe_total"] for v in selection_audit["counts_by_reason"].values())
    assert telemetry_c["selection_audit"]["post_dedupe_total"] == telemetry_c["total"]


def test_acquire_fundamentals_lock_sets_acquired_at():
    from datetime import datetime, timezone
    from scheduler_service import _acquire_fundamentals_sync_lock

    db = _FakeStep3DB([])
    now = datetime.now(timezone.utc)
    acquired = asyncio.run(_acquire_fundamentals_sync_lock(db, "run-123", now))

    assert acquired is True
    lock_doc = db.ops_locks.docs["fundamentals_sync"]
    assert lock_doc["owner_run_id"] == "run-123"
    assert lock_doc["acquired_at"] == now
    assert lock_doc["expires_at"] is not None


def test_finalize_zombie_step3_runs_marks_stale_running_docs_cancelled():
    from datetime import datetime, timezone, timedelta
    from scheduler_service import _finalize_zombie_fundamentals_runs

    db = _FakeStep3DB([])
    stale_started = datetime.now(timezone.utc) - timedelta(days=1)
    db.ops_job_runs.docs[1] = {
        "job_name": "fundamentals_sync",
        "status": "running",
        "started_at": stale_started,
    }

    finalized = asyncio.run(_finalize_zombie_fundamentals_runs(db, datetime.now(timezone.utc)))

    assert finalized == 1
    assert db.ops_job_runs.docs[1]["status"] == "cancelled"
    assert db.ops_job_runs.docs[1]["cancelled_at"] is not None
    assert db.ops_job_runs.docs[1]["finished_at"] is not None
    assert db.ops_job_runs.docs[1]["details"]["zombie_finalized"] is True
    assert db.ops_job_runs.docs[1]["details"]["zombie_reason"] == "stale_heartbeat_or_missing"


class _TrackedTickersForSyncStatus:
    def __init__(self, docs):
        self._docs = list(docs)

    def _is_base(self, doc):
        return (
            doc.get("exchange") in {"NYSE", "NASDAQ"}
            and doc.get("asset_type") == "Common Stock"
            and doc.get("is_seeded") is True
            and doc.get("has_price_data") is True
        )

    def aggregate(self, _pipeline):
        base_docs = [d for d in self._docs if self._is_base(d)]

        def _count(pred):
            return [{"n": sum(1 for d in base_docs if pred(d))}] if base_docs else []

        return _AsyncCursor([{
            "total": _count(lambda _d: True),
            "price_complete": _count(lambda d: d.get("price_history_complete") is True),
            "fundamentals_complete": _count(lambda d: d.get("fundamentals_complete") is True),
            "needs_price_redownload": _count(lambda d: d.get("needs_price_redownload") is True),
            "needs_fundamentals_refresh": _count(lambda d: d.get("needs_fundamentals_refresh") is True),
        }])

    async def count_documents(self, query):
        if query == {"needs_fundamentals_refresh": True}:
            return sum(1 for d in self._docs if d.get("needs_fundamentals_refresh") is True)
        return 0


class _FundamentalsEventsForSyncStatus:
    def __init__(self, pending_count):
        self._pending_count = pending_count

    async def count_documents(self, query):
        if query == {"status": "pending"}:
            return self._pending_count
        return 0


class _FakeSyncStatusDB:
    def __init__(self, tracked_docs, pending_events):
        self.tracked_tickers = _TrackedTickersForSyncStatus(tracked_docs)
        self.fundamentals_events = _FundamentalsEventsForSyncStatus(pending_events)


def test_admin_sync_status_pending_refresh_uses_tracked_tickers_flag_count(monkeypatch):
    db = _FakeSyncStatusDB(
        tracked_docs=[
            {
                "ticker": "AAPL.US",
                "exchange": "NASDAQ",
                "asset_type": "Common Stock",
                "is_seeded": True,
                "has_price_data": True,
                "needs_fundamentals_refresh": True,
            },
            {
                "ticker": "MSFT.US",
                "exchange": "NASDAQ",
                "asset_type": "Common Stock",
                "is_seeded": True,
                "has_price_data": True,
                "needs_fundamentals_refresh": False,
            },
            {
                "ticker": "OTC1.US",
                "exchange": "OTC",
                "asset_type": "Common Stock",
                "is_seeded": True,
                "has_price_data": True,
                "needs_fundamentals_refresh": True,
            },
        ],
        pending_events=7,
    )

    async def _fake_credits(_db):
        return {"total_credits": 123}

    monkeypatch.setattr("credit_log_service.get_daily_credit_usage", _fake_credits)

    status = asyncio.run(get_pipeline_sync_status(db))

    assert status["needs_fundamentals_refresh"] == 2
    assert status["pending_events_audit"] == 7


class _OpsJobRunsForTelemetryRead:
    def __init__(self, running=None, finished=None):
        self._running = running
        self._finished = finished

    async def find_one(self, query, _projection=None, **_kwargs):
        if query.get("status") == "running":
            return self._running
        if query.get("status", {}).get("$in"):
            return self._finished
        return None


class _CountCollection:
    def __init__(self, count):
        self._count = count

    async def count_documents(self, _query):
        return self._count


class _FakeStep3TelemetryReadDB:
    def __init__(self, running=None, finished=None, pending_refresh=0, pending_events=0):
        self.ops_job_runs = _OpsJobRunsForTelemetryRead(running=running, finished=finished)
        self.tracked_tickers = _CountCollection(pending_refresh)
        self.fundamentals_events = _CountCollection(pending_events)


def test_step3_live_telemetry_prefers_running_run_and_maps_status():
    from services.admin_overview_service import get_step3_live_telemetry

    running = {
        "_id": "run-1",
        "status": "running",
        "started_at_prague": "2026-01-01T10:00:00+01:00",
        "details": {
            "step3_telemetry": {
                "updated_at_prague": "2026-01-01T10:01:00+01:00",
                "phases": {
                    "A": {"name": "Fundamentals", "status": "running", "processed": 20, "total": 100, "pct": 20.0, "message": "Syncing fundamentals"},
                    "B": {"name": "Visibility", "status": "idle", "processed": 0, "total": None, "pct": None, "message": None},
                    "C": {"name": "PriceHistory", "status": "idle", "processed": 0, "total": None, "pct": None, "message": None},
                },
            }
        },
    }
    finished = {
        "_id": "run-0",
        "status": "failed",
        "started_at_prague": "2025-12-31T23:00:00+01:00",
    }
    db = _FakeStep3TelemetryReadDB(running=running, finished=finished, pending_refresh=9, pending_events=4)

    resp = asyncio.run(get_step3_live_telemetry(db))
    assert resp["run_id"] == "run-1"
    assert resp["status"] == "running"
    assert resp["pending_refresh_flags"] == 9
    assert resp["pending_events_audit"] == 4
    assert resp["phases"]["A"]["processed"] == 20
    assert resp["phases"]["A"]["total"] == 100


def test_step3_live_telemetry_returns_idle_when_no_runs():
    from services.admin_overview_service import get_step3_live_telemetry

    db = _FakeStep3TelemetryReadDB(running=None, finished=None, pending_refresh=1, pending_events=2)
    resp = asyncio.run(get_step3_live_telemetry(db))
    assert resp["status"] == "idle"
    assert resp["run_id"] is None
    assert resp["started_at_prague"] is None
    assert resp["updated_at_prague"] is None
    assert resp["pending_refresh_flags"] == 1
    assert resp["pending_events_audit"] == 2
