import asyncio
from types import SimpleNamespace

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

    async def insert_one(self, doc):
        self._seq += 1
        self.docs[self._seq] = dict(doc)
        return SimpleNamespace(inserted_id=self._seq)

    async def update_one(self, filt, update):
        doc = self.docs.get(filt["_id"], {})
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
    telemetry = run_doc["details"]["step3_telemetry"]
    assert telemetry["phases"]["A"]["status"] in {"done", "error"}
    assert telemetry["phases"]["A"]["processed"] == 1
    assert telemetry["phases"]["A"]["total"] == 1
    assert telemetry["phases"]["B"]["status"] == "done"
    assert telemetry["phases"]["C"]["name"] == "PriceHistory"
    assert "updated_at_prague" in telemetry


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
