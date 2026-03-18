import asyncio
from copy import deepcopy
from datetime import date, datetime, timezone, timedelta
from types import SimpleNamespace
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import scheduler_service


class _FakeCursor:
    def __init__(self, docs):
        self._docs = docs

    async def to_list(self, _):
        return deepcopy(self._docs)


def _set_path(doc, dotted_key, value):
    parts = dotted_key.split(".")
    cur = doc
    for part in parts[:-1]:
        if part not in cur or not isinstance(cur[part], dict):
            cur[part] = {}
        cur = cur[part]
    cur[parts[-1]] = value


class _FakeOpsJobRuns:
    def __init__(self, seeded_total=5000):
        self.docs = {}
        self._next_id = 1
        self.seeded_total = seeded_total

    async def insert_one(self, doc):
        _id = self._next_id
        self._next_id += 1
        self.docs[_id] = deepcopy(doc)
        self.docs[_id]["_id"] = _id
        return SimpleNamespace(inserted_id=_id)

    async def update_one(self, filt, update):
        _id = filt.get("_id")
        if _id not in self.docs:
            return SimpleNamespace(matched_count=0)
        for key, value in (update.get("$set") or {}).items():
            _set_path(self.docs[_id], key, value)
        return SimpleNamespace(matched_count=1)

    async def find_one(self, filt, projection=None, sort=None):
        if filt.get("job_name") == "universe_seed":
            return {"details": {"seeded_total": self.seeded_total}}
        _id = filt.get("_id")
        if _id in self.docs:
            return deepcopy(self.docs[_id])
        return None

    async def update_many(self, filt, update):
        _ = filt, update
        return SimpleNamespace(modified_count=0)

    @property
    def latest(self):
        if not self.docs:
            return None
        return deepcopy(self.docs[max(self.docs.keys())])


class _FakeTrackedTickers:
    def __init__(self, tickers):
        self._tickers = list(tickers)

    def find(self, query, projection):
        _ = query, projection
        return _FakeCursor([{"ticker": t, "name": t} for t in self._tickers])

    async def update_many(self, filt, update):
        _ = filt, update
        return SimpleNamespace(modified_count=len(self._tickers))


class _FakeOpsConfig:
    def __init__(self):
        self.docs = {}

    async def find_one(self, filt):
        key = filt.get("key")
        doc = self.docs.get(key)
        return deepcopy(doc) if doc else None

    async def delete_one(self, filt):
        key = filt.get("key")
        self.docs.pop(key, None)
        return SimpleNamespace(deleted_count=1)


class _FakePipelineState:
    def __init__(self, initial=None):
        self.docs = {}
        if initial:
            self.docs.update(deepcopy(initial))

    async def find_one(self, filt):
        _id = filt.get("_id")
        doc = self.docs.get(_id)
        return deepcopy(doc) if doc else None

    async def update_one(self, filt, update, upsert=False):
        _ = upsert
        _id = filt.get("_id")
        doc = deepcopy(self.docs.get(_id, {"_id": _id}))
        for key, value in (update.get("$set") or {}).items():
            _set_path(doc, key, value)
        self.docs[_id] = doc
        return SimpleNamespace(matched_count=1)


class _FakeStockPrices:
    def __init__(self, counts_by_date):
        self.counts_by_date = dict(counts_by_date)

    async def count_documents(self, filt):
        return int(self.counts_by_date.get(filt.get("date"), 0))


class _FakeDB:
    def __init__(self, *, stock_counts, initial_pipeline_state=None, seeded_tickers=None):
        self.ops_job_runs = _FakeOpsJobRuns()
        self.tracked_tickers = _FakeTrackedTickers(seeded_tickers or ["AAPL.US", "MSFT.US"])
        self.ops_config = _FakeOpsConfig()
        self.pipeline_state = _FakePipelineState(initial=initial_pipeline_state)
        self.stock_prices = _FakeStockPrices(stock_counts)


def _patch_non_gapfill_dependencies(monkeypatch):
    async def _fake_flags(db, include_exclusions=False, tickers_with_price=None):
        _ = db, include_exclusions, tickers_with_price
        return {
            "seeded_total": 2,
            "with_price_data": 2,
            "without_price_data": 0,
            "matched_price_tickers_raw": 2,
            "exclusions": [],
        }

    async def _fake_save_report(db, rows, now):
        _ = db, rows, now
        return {
            "exclusion_report_rows": 0,
            "exclusion_report_run_id": "price_sync_test_run",
            "exclusion_report_date": "2026-03-18",
        }

    async def _fake_detectors(db, progress_cb=None, exclusion_meta=None, cancel_check=None):
        _ = db, progress_cb, exclusion_meta, cancel_check
        return {"enqueued_total": 0, "skipped_total": 0, "cancelled": False}

    monkeypatch.setattr(scheduler_service, "sync_has_price_data_flags", _fake_flags)
    monkeypatch.setattr(scheduler_service, "save_price_sync_exclusion_report", _fake_save_report)
    monkeypatch.setattr(scheduler_service, "run_step2_event_detectors", _fake_detectors)


def test_step2_gapfill_bootstrap_writes_pipeline_state_with_prague_timestamp(monkeypatch):
    _patch_non_gapfill_dependencies(monkeypatch)
    db = _FakeDB(stock_counts={"2026-03-17": 5001})

    async def _fake_bulk(db, job_name="price_sync", progress_cb=None, seeded_tickers_override=None, bulk_date=None):
        _ = db, job_name, progress_cb, seeded_tickers_override
        return {
            "status": "success",
            "dates_processed": 1,
            "records_upserted": 5001,
            "api_calls": 1,
            "bulk_writes": 1,
            "tickers_with_price": ["AAPL.US", "MSFT.US"],
            "date": bulk_date,
        }

    async def _fake_missed_dates(db, today_dt):
        _ = db, today_dt
        return [date(2026, 3, 17)]

    monkeypatch.setattr("price_ingestion_service.run_daily_bulk_catchup", _fake_bulk)
    monkeypatch.setattr(scheduler_service, "_get_missed_trading_dates", _fake_missed_dates)

    result = asyncio.run(
        scheduler_service.run_daily_price_sync(
            db,
            ignore_kill_switch=True,
            parent_run_id="parent",
            chain_run_id="chain",
        )
    )

    assert result["status"] == "success"
    persisted = db.pipeline_state.docs["price_bulk"]
    assert persisted["_id"] == "price_bulk"
    assert persisted["global_last_bulk_date_processed"] == "2026-03-17"
    assert isinstance(persisted["updated_at"], datetime)
    assert persisted["updated_at"].tzinfo == timezone.utc
    assert isinstance(persisted["updated_at_prague"], str)
    assert persisted["updated_at_prague"]


def test_step2_gapfill_bootstrap_forces_target_end_date_when_missed_dates_empty(monkeypatch):
    _patch_non_gapfill_dependencies(monkeypatch)
    db = _FakeDB(stock_counts={"2026-03-18": 5001})
    called_dates = []

    async def _fake_bulk(db, job_name="price_sync", progress_cb=None, seeded_tickers_override=None, bulk_date=None):
        _ = db, job_name, progress_cb, seeded_tickers_override
        called_dates.append(bulk_date)
        return {
            "status": "success",
            "dates_processed": 1,
            "records_upserted": 5001,
            "api_calls": 1,
            "bulk_writes": 1,
            "tickers_with_price": ["AAPL.US"],
            "date": bulk_date,
        }

    async def _fake_missed_dates(db, today_dt):
        _ = db, today_dt
        return []

    monkeypatch.setattr("price_ingestion_service.run_daily_bulk_catchup", _fake_bulk)
    monkeypatch.setattr(scheduler_service, "_get_missed_trading_dates", _fake_missed_dates)
    monkeypatch.setattr(scheduler_service, "PRAGUE_TZ", timezone.utc)

    result = asyncio.run(
        scheduler_service.run_daily_price_sync(
            db,
            ignore_kill_switch=True,
            parent_run_id="parent",
            chain_run_id="chain",
        )
    )

    assert result["status"] == "success"
    assert result["dates_processed"] == 1
    assert result["api_calls"] > 0
    assert len(called_dates) == 1
    assert called_dates[0] == datetime.now(timezone.utc).date().strftime("%Y-%m-%d")


def test_step2_gapfill_skips_duplicate_day_at_watermark_boundary(monkeypatch):
    _patch_non_gapfill_dependencies(monkeypatch)
    db = _FakeDB(
        stock_counts={"2026-03-17": 5001},
        initial_pipeline_state={
            "price_bulk": {
                "_id": "price_bulk",
                "global_last_bulk_date_processed": "2026-03-17",
                "updated_at": datetime(2026, 3, 17, tzinfo=timezone.utc),
                "updated_at_prague": "2026-03-17T01:00:00+01:00",
            }
        },
    )
    called_dates = []

    async def _fake_bulk(db, job_name="price_sync", progress_cb=None, seeded_tickers_override=None, bulk_date=None):
        _ = db, job_name, progress_cb, seeded_tickers_override
        called_dates.append(bulk_date)
        return {"status": "success", "dates_processed": 1, "records_upserted": 5001, "api_calls": 1, "bulk_writes": 1}

    async def _fake_missed_dates(db, today_dt):
        _ = db, today_dt
        return [date(2026, 3, 17)]

    monkeypatch.setattr("price_ingestion_service.run_daily_bulk_catchup", _fake_bulk)
    monkeypatch.setattr(scheduler_service, "_get_missed_trading_dates", _fake_missed_dates)

    asyncio.run(
        scheduler_service.run_daily_price_sync(
            db,
            ignore_kill_switch=True,
            parent_run_id="parent",
            chain_run_id="chain",
        )
    )

    assert called_dates == []
    latest = db.ops_job_runs.latest
    assert latest["status"] == "error"
    assert "bulk guard triggered" in latest["error"]
    assert latest["details"]["price_bulk_gapfill"]["days"] == []


def test_step2_gapfill_stops_on_first_sanity_failure(monkeypatch):
    _patch_non_gapfill_dependencies(monkeypatch)
    db = _FakeDB(
        stock_counts={
            "2026-03-17": 5001,
            "2026-03-18": 3900,
            "2026-03-19": 7000,
        }
    )
    called_dates = []

    async def _fake_bulk(db, job_name="price_sync", progress_cb=None, seeded_tickers_override=None, bulk_date=None):
        _ = db, job_name, progress_cb, seeded_tickers_override
        called_dates.append(bulk_date)
        return {
            "status": "success",
            "dates_processed": 1,
            "records_upserted": 4500,
            "api_calls": 1,
            "bulk_writes": 1,
            "tickers_with_price": ["AAPL.US"],
            "date": bulk_date,
        }

    async def _fake_missed_dates(db, today_dt):
        _ = db, today_dt
        return [date(2026, 3, 17), date(2026, 3, 18), date(2026, 3, 19)]

    monkeypatch.setattr("price_ingestion_service.run_daily_bulk_catchup", _fake_bulk)
    monkeypatch.setattr(scheduler_service, "_get_missed_trading_dates", _fake_missed_dates)

    asyncio.run(
        scheduler_service.run_daily_price_sync(
            db,
            ignore_kill_switch=True,
            parent_run_id="parent",
            chain_run_id="chain",
        )
    )

    assert called_dates == ["2026-03-17", "2026-03-18"]
    assert len(called_dates) == 2
    assert "2026-03-19" not in called_dates
    assert db.pipeline_state.docs["price_bulk"]["global_last_bulk_date_processed"] == "2026-03-17"
    days = db.ops_job_runs.latest["details"]["price_bulk_gapfill"]["days"]
    assert [d["status"] for d in days] == ["success", "failed_sanity"]


def test_step2_gapfill_days_history_capped_to_60(monkeypatch):
    _patch_non_gapfill_dependencies(monkeypatch)
    start = date(2026, 1, 1)
    all_days = [start + timedelta(days=i) for i in range(65)]
    stock_counts = {d.strftime("%Y-%m-%d"): 5001 for d in all_days}
    db = _FakeDB(stock_counts=stock_counts)

    async def _fake_bulk(db, job_name="price_sync", progress_cb=None, seeded_tickers_override=None, bulk_date=None):
        _ = db, job_name, progress_cb, seeded_tickers_override
        return {
            "status": "success",
            "dates_processed": 1,
            "records_upserted": 5001,
            "api_calls": 1,
            "bulk_writes": 1,
            "tickers_with_price": ["AAPL.US"],
            "date": bulk_date,
        }

    async def _fake_missed_dates(db, today_dt):
        _ = db, today_dt
        return all_days

    monkeypatch.setattr("price_ingestion_service.run_daily_bulk_catchup", _fake_bulk)
    monkeypatch.setattr(scheduler_service, "_get_missed_trading_dates", _fake_missed_dates)

    asyncio.run(
        scheduler_service.run_daily_price_sync(
            db,
            ignore_kill_switch=True,
            parent_run_id="parent",
            chain_run_id="chain",
        )
    )

    days = db.ops_job_runs.latest["details"]["price_bulk_gapfill"]["days"]
    assert len(days) == 60
    assert days[0]["bulk_date"] == all_days[5].strftime("%Y-%m-%d")
    assert days[-1]["bulk_date"] == all_days[-1].strftime("%Y-%m-%d")
