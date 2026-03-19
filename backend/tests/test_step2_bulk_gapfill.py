import asyncio
from copy import deepcopy
from datetime import date, datetime, timezone, timedelta
from types import SimpleNamespace
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import scheduler_service
import price_ingestion_service


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


def _has_path(doc, dotted_key):
    parts = dotted_key.split(".")
    cur = doc
    for part in parts:
        if not isinstance(cur, dict) or part not in cur:
            return False
        cur = cur[part]
    return True


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

    async def update_one(self, filt, update, upsert=False):
        _ = upsert
        key = filt.get("key")
        doc = deepcopy(self.docs.get(key, {"key": key}))
        for k, v in (update.get("$set") or {}).items():
            _set_path(doc, k, v)
        for k, v in (update.get("$setOnInsert") or {}).items():
            if not _has_path(doc, k):
                _set_path(doc, k, v)
        self.docs[key] = doc
        return SimpleNamespace(matched_count=1)


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

    async def bulk_write(self, batch, ordered=False):
        _ = ordered
        return SimpleNamespace(upserted_count=len(batch), modified_count=0)


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

    async def _fake_detectors(db, progress_cb=None, exclusion_meta=None, cancel_check=None, processed_date=None):
        _ = db
        _ = progress_cb
        _ = exclusion_meta
        _ = cancel_check
        _ = processed_date
        return {"enqueued_total": 0, "skipped_total": 0, "cancelled": False}

    monkeypatch.setattr(scheduler_service, "sync_has_price_data_flags", _fake_flags)
    monkeypatch.setattr(scheduler_service, "save_price_sync_exclusion_report", _fake_save_report)
    monkeypatch.setattr(scheduler_service, "run_step2_event_detectors", _fake_detectors)
    monkeypatch.setattr(scheduler_service, "MIN_BULK_MATCHED_SEEDED_SANITY_CHECK", 1)
    monkeypatch.setattr(
        scheduler_service,
        "STEP2_SANITY_THRESHOLD_USED",
        "matched_seeded_tickers_count >= 1",
    )


def test_step2_gapfill_bootstrap_writes_pipeline_state_with_prague_timestamp(monkeypatch):
    _patch_non_gapfill_dependencies(monkeypatch)
    db = _FakeDB(stock_counts={})

    async def _fake_bulk(db, job_name="price_sync", progress_cb=None, seeded_tickers_override=None):
        _ = db, job_name, progress_cb, seeded_tickers_override
        return {
            "status": "success",
            "dates_processed": 1,
            "records_upserted": 5001,
            "api_calls": 1,
            "bulk_fetch_executed": True,
            "bulk_writes": 1,
            "tickers_with_price": ["AAPL.US", "MSFT.US"],
            "date": "2026-03-17",
            "processed_date": "2026-03-17",
            "unique_dates": ["2026-03-17"],
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


def test_step2_gapfill_bootstrap_fetches_provider_latest_without_bulk_date_kwarg(monkeypatch):
    _patch_non_gapfill_dependencies(monkeypatch)
    db = _FakeDB(stock_counts={})
    called_kwargs = []

    async def _fake_bulk(*args, **kwargs):
        called_kwargs.append(dict(kwargs))
        return {
            "status": "success",
            "dates_processed": 1,
            "records_upserted": 5001,
            "api_calls": 1,
            "bulk_fetch_executed": True,
            "bulk_writes": 1,
            "tickers_with_price": ["AAPL.US"],
            "date": "2026-03-18",
            "processed_date": "2026-03-18",
            "unique_dates": ["2026-03-18"],
            "bulk_url_used": "https://eodhd.com/api/eod-bulk-last-day/US",
        }

    async def _fake_missed_dates(db, today_dt):
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
    assert len(called_kwargs) == 1
    assert "bulk_date" not in called_kwargs[0]
    assert db.ops_job_runs.latest["details"]["bulk_url_used"] == "https://eodhd.com/api/eod-bulk-last-day/US"


def test_step2_gapfill_watermark_boundary_still_executes_single_latest_fetch(monkeypatch):
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
    calls_made = []

    async def _fake_bulk(db, job_name="price_sync", progress_cb=None, seeded_tickers_override=None):
        _ = db, job_name, progress_cb, seeded_tickers_override
        calls_made.append(True)
        return {
            "status": "success",
            "dates_processed": 1,
            "records_upserted": 5001,
            "api_calls": 1,
            "bulk_fetch_executed": True,
            "bulk_writes": 1,
            "tickers_with_price": ["AAPL.US", "MSFT.US"],
            "date": "2026-03-17",
            "processed_date": "2026-03-17",
            "unique_dates": ["2026-03-17"],
        }

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

    assert calls_made == [True]
    latest = db.ops_job_runs.latest
    assert latest["status"] == "success"
    assert latest["details"]["api_calls"] == 1
    days = latest["details"]["price_bulk_gapfill"]["days"]
    assert len(days) == 1
    assert days[0]["processed_date"] == "2026-03-17"


def test_step2_gapfill_stops_on_first_sanity_failure(monkeypatch):
    _patch_non_gapfill_dependencies(monkeypatch)
    monkeypatch.setattr(scheduler_service, "MIN_BULK_MATCHED_SEEDED_SANITY_CHECK", 4000)
    monkeypatch.setattr(
        scheduler_service,
        "STEP2_SANITY_THRESHOLD_USED",
        "matched_seeded_tickers_count >= 4000",
    )
    db = _FakeDB(stock_counts={})
    calls_made = []

    async def _fake_bulk(db, job_name="price_sync", progress_cb=None, seeded_tickers_override=None):
        _ = db, job_name, progress_cb, seeded_tickers_override
        calls_made.append(True)
        return {
            "status": "success",
            "dates_processed": 1,
            "records_upserted": 3900,
            "api_calls": 1,
            "bulk_fetch_executed": True,
            "bulk_writes": 1,
            "tickers_with_price": ["AAPL.US"],
            "date": "2026-03-18",
            "processed_date": "2026-03-18",
            "unique_dates": ["2026-03-18"],
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

    assert len(calls_made) == 1
    assert "price_bulk" not in db.pipeline_state.docs
    days = db.ops_job_runs.latest["details"]["price_bulk_gapfill"]["days"]
    assert [d["status"] for d in days] == ["failed_sanity"]
    assert days[0]["processed_date"] == "2026-03-18"


def test_step2_gapfill_errors_when_bulk_payload_has_multiple_dates(monkeypatch):
    _patch_non_gapfill_dependencies(monkeypatch)
    db = _FakeDB(stock_counts={})

    async def _fake_bulk(db, job_name="price_sync", progress_cb=None, seeded_tickers_override=None):
        _ = db, job_name, progress_cb, seeded_tickers_override
        return {
            "status": "error",
            "dates_processed": 0,
            "records_upserted": 0,
            "api_calls": 1,
            "bulk_fetch_executed": True,
            "bulk_writes": 0,
            "tickers_with_price": [],
            "date": None,
            "processed_date": None,
            "unique_dates": ["2026-03-18", "2026-03-19"],
            "bulk_url_used": "https://eodhd.com/api/eod-bulk-last-day/US",
        }

    async def _fake_missed_dates(db, today_dt):
        _ = db, today_dt
        return [date(2026, 3, 19)]

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

    assert result["status"] == "error"
    assert "price_bulk" not in db.pipeline_state.docs
    latest = db.ops_job_runs.latest
    day = latest["details"]["price_bulk_gapfill"]["days"][0]
    assert day["status"] == "error"
    assert day["unique_dates"] == ["2026-03-18", "2026-03-19"]
    assert latest["details"]["bulk_url_used"] == "https://eodhd.com/api/eod-bulk-last-day/US"


def test_step2_bulk_guard_uses_canonical_bulk_fetch_executed_signal(monkeypatch):
    _patch_non_gapfill_dependencies(monkeypatch)
    db = _FakeDB(stock_counts={})

    async def _fake_bulk(db, job_name="price_sync", progress_cb=None, seeded_tickers_override=None):
        _ = db, job_name, progress_cb, seeded_tickers_override
        # Regression case: legacy telemetry could have api_calls=1 but no
        # actual fetch decision/day output.
        return {
            "status": "success",
            "dates_processed": 0,
            "records_upserted": 0,
            "api_calls": 1,
            "bulk_fetch_executed": False,
            "raw_row_count": 0,
            "bulk_writes": 0,
            "tickers_with_price": [],
            "date": None,
            "processed_date": None,
            "unique_dates": [],
            "bulk_url_used": "https://eodhd.com/api/eod-bulk-last-day/US",
        }

    async def _fake_missed_dates(db, today_dt):
        _ = db, today_dt
        return [date(2026, 3, 19)]

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

    assert result["status"] == "error"
    assert "no bulk fetch executed" in result["error"]
    latest = db.ops_job_runs.latest
    details = latest["details"]
    assert details["bulk_fetch_executed"] is False
    assert details["api_calls"] == 0
    assert details["price_bulk_gapfill"]["days"] == []


def test_step2_gapfill_days_history_capped_to_60(monkeypatch):
    _patch_non_gapfill_dependencies(monkeypatch)
    start = date(2026, 1, 1)
    all_days = [start + timedelta(days=i) for i in range(65)]
    db = _FakeDB(stock_counts={})

    async def _fake_bulk(db, job_name="price_sync", progress_cb=None, seeded_tickers_override=None):
        _ = db, job_name, progress_cb, seeded_tickers_override
        return {
            "status": "success",
            "dates_processed": 1,
            "records_upserted": 5001,
            "api_calls": 1,
            "bulk_fetch_executed": True,
            "bulk_writes": 1,
            "tickers_with_price": ["AAPL.US"],
            "date": "2026-03-19",
            "processed_date": "2026-03-19",
            "unique_dates": ["2026-03-19"],
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
    assert len(days) == 1
    assert days[0]["bulk_date"] == "2026-03-19"


def test_step2_gapfill_days_empty_when_phase_not_entered_seeded_total_zero(monkeypatch):
    _patch_non_gapfill_dependencies(monkeypatch)
    db = _FakeDB(stock_counts={})
    db.ops_job_runs.seeded_total = 0
    calls_made = []

    async def _fake_bulk(db, job_name="price_sync", progress_cb=None, seeded_tickers_override=None):
        _ = db, job_name, progress_cb, seeded_tickers_override
        calls_made.append(True)
        return {
            "status": "success",
            "dates_processed": 1,
            "records_upserted": 5001,
            "api_calls": 1,
            "bulk_fetch_executed": True,
            "bulk_writes": 1,
            "date": "2026-03-19",
            "processed_date": "2026-03-19",
            "unique_dates": ["2026-03-19"],
        }

    monkeypatch.setattr("price_ingestion_service.run_daily_bulk_catchup", _fake_bulk)

    result = asyncio.run(
        scheduler_service.run_daily_price_sync(
            db,
            ignore_kill_switch=True,
            parent_run_id="parent",
            chain_run_id="chain",
        )
    )

    assert result["status"] == "success"
    assert calls_made == []
    latest = db.ops_job_runs.latest
    assert latest["status"] == "success"
    assert latest["details"]["api_calls"] == 0
    assert latest["details"]["price_bulk_gapfill"]["days"] == []
    assert "skip_reason" in latest["details"]["price_bulk_gapfill"]


def test_step2_detectors_use_bulk_processed_date_for_endpoints(monkeypatch):
    db = _FakeDB(stock_counts={})

    async def _fake_missed_dates(db, today_dt):
        _ = db, today_dt
        return [date(2026, 3, 10), date(2026, 3, 11)]

    monkeypatch.setattr(scheduler_service, "_get_missed_trading_dates", _fake_missed_dates)

    result = asyncio.run(
        scheduler_service.run_step2_event_detectors(
            db,
            processed_date="2026-03-18",
        )
    )

    split_endpoint = result["step_2_2_split"]["api_endpoint"]
    dividend_endpoint = result["step_2_4_dividend"]["api_endpoint"]
    earnings_endpoint = result["step_2_6_earnings"]["api_endpoint"]
    assert "date=2026-03-18" in split_endpoint
    assert "date=2026-03-18" in dividend_endpoint
    assert "from=2026-03-18&to=2026-03-18" in earnings_endpoint
    assert result["step_2_2_split"]["dates_checked"] == ["2026-03-18"]
    assert result["step_2_4_dividend"]["dates_checked"] == ["2026-03-18"]
    assert result["step_2_6_earnings"]["dates_checked"] == ["2026-03-18"]


def test_step2_run_persists_detector_endpoints_using_bulk_processed_date(monkeypatch):
    db = _FakeDB(stock_counts={})

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

    async def _fake_bulk(db, job_name="price_sync", progress_cb=None, seeded_tickers_override=None):
        _ = db, job_name, progress_cb, seeded_tickers_override
        return {
            "status": "success",
            "dates_processed": 1,
            "records_upserted": 5001,
            "api_calls": 1,
            "bulk_fetch_executed": True,
            "bulk_writes": 1,
            "tickers_with_price": ["AAPL.US", "MSFT.US"],
            "tickers_with_price_data": 2,
            "matched_price_tickers_raw": 2,
            "date": "2026-03-18",
            "processed_date": "2026-03-18",
            "unique_dates": ["2026-03-18"],
            "bulk_url_used": "https://eodhd.com/api/eod-bulk-last-day/US",
        }

    async def _fake_detectors(db, progress_cb=None, exclusion_meta=None, cancel_check=None, processed_date=None):
        _ = db
        _ = progress_cb
        _ = exclusion_meta
        _ = cancel_check
        _ = processed_date
        assert processed_date == "2026-03-18"
        return {
            "step_2_2_split": {
                "api_endpoint": f"https://eodhd.com/api/eod-bulk-last-day/US?type=splits&date={processed_date}",
                "dates_checked": [processed_date],
            },
            "step_2_4_dividend": {
                "api_endpoint": f"https://eodhd.com/api/eod-bulk-last-day/US?type=dividends&date={processed_date}",
                "dates_checked": [processed_date],
            },
            "step_2_6_earnings": {
                "api_endpoint": f"https://eodhd.com/api/calendar/earnings?from={processed_date}&to={processed_date}",
                "dates_checked": [processed_date],
            },
            "enqueued_total": 0,
            "skipped_total": 0,
            "cancelled": False,
        }

    monkeypatch.setattr(scheduler_service, "sync_has_price_data_flags", _fake_flags)
    monkeypatch.setattr(scheduler_service, "save_price_sync_exclusion_report", _fake_save_report)
    monkeypatch.setattr("price_ingestion_service.run_daily_bulk_catchup", _fake_bulk)
    monkeypatch.setattr(scheduler_service, "run_step2_event_detectors", _fake_detectors)

    result = asyncio.run(
        scheduler_service.run_daily_price_sync(
            db,
            ignore_kill_switch=True,
            parent_run_id="parent",
            chain_run_id="chain",
        )
    )

    assert result["status"] == "success"
    details = db.ops_job_runs.latest["details"]
    assert details["api_calls"] == 1
    assert len(details["price_bulk_gapfill"]["days"]) == 1
    assert details["price_bulk_gapfill"]["days"][0]["processed_date"] == "2026-03-18"
    assert "date=2026-03-18" in details["event_detectors"]["step_2_2_split"]["api_endpoint"]
    assert "date=2026-03-18" in details["event_detectors"]["step_2_4_dividend"]["api_endpoint"]
    assert (
        "from=2026-03-18&to=2026-03-18"
        in details["event_detectors"]["step_2_6_earnings"]["api_endpoint"]
    )


def test_step2_gapfill_bulk_matching_normalizes_tickers_and_persists_samples(monkeypatch):
    db = _FakeDB(stock_counts={"2026-03-18": 2}, seeded_tickers=["AAPL.US", "MSFT.US"])
    monkeypatch.setattr(scheduler_service, "MIN_BULK_ROWS_SANITY_CHECK", 0)
    monkeypatch.setattr(scheduler_service, "MIN_BULK_MATCHED_SEEDED_SANITY_CHECK", 1)
    monkeypatch.setattr(
        scheduler_service,
        "STEP2_SANITY_THRESHOLD_USED",
        "matched_seeded_tickers_count >= 1",
    )

    async def _fake_fetch_bulk(_exchange="US", include_meta=False):
        payload = [
            {
                "code": " aapl ",
                "date": "2026-03-18",
                "open": 100,
                "high": 101,
                "low": 99,
                "close": 100.5,
                "adjusted_close": 100.4,
                "volume": 1000,
            },
            {
                "code": "MSFT.US",
                "date": "2026-03-18",
                "open": 200,
                "high": 201,
                "low": 199,
                "close": 200.5,
                "adjusted_close": 200.4,
                "volume": 2000,
            },
        ]
        _ = _exchange
        if include_meta:
            return payload, True
        return payload

    async def _fake_missed_dates(db, today_dt):
        _ = db, today_dt
        return [date(2026, 3, 18)]

    async def _fake_save_report(db, rows, now):
        _ = db, rows, now
        return {
            "exclusion_report_rows": 0,
            "exclusion_report_run_id": "price_sync_test_run",
            "exclusion_report_date": "2026-03-18",
        }

    async def _fake_detectors(db, progress_cb=None, exclusion_meta=None, cancel_check=None, processed_date=None):
        _ = db, progress_cb, exclusion_meta, cancel_check, processed_date
        return {"enqueued_total": 0, "skipped_total": 0, "cancelled": False}

    monkeypatch.setattr(price_ingestion_service, "fetch_bulk_eod_latest", _fake_fetch_bulk)
    monkeypatch.setattr(scheduler_service, "_get_missed_trading_dates", _fake_missed_dates)
    monkeypatch.setattr(scheduler_service, "save_price_sync_exclusion_report", _fake_save_report)
    monkeypatch.setattr(scheduler_service, "run_step2_event_detectors", _fake_detectors)

    result = asyncio.run(
        scheduler_service.run_daily_price_sync(
            db,
            ignore_kill_switch=True,
            parent_run_id="parent",
            chain_run_id="chain",
        )
    )

    assert result["status"] == "success"
    assert result["matched_price_tickers_raw"] == 2
    assert result["tickers_with_price_data"] == 2
    assert result["matched_seeded_tickers_count"] == 2
    assert result["tickers_without_price_data"] == 0
    assert db.ops_job_runs.latest["details"]["rows_written"] > 0

    details = db.ops_job_runs.latest["details"]
    assert details["matched_price_tickers_raw"] == 2
    assert details["tickers_with_price_data"] == 2
    assert details["matched_seeded_tickers_count"] == 2
    assert details["match_ratio"] == 1.0
    assert details["sanity_threshold_used"] == "matched_seeded_tickers_count >= 1"
    ticker_samples = details["price_bulk_gapfill"]["ticker_samples"]
    assert "bulk_rows_sample" in ticker_samples
    assert "bulk_rows_normalized_sample" in ticker_samples
    assert "seeded_tickers_sample" in ticker_samples
    assert "seeded_tickers_normalized_sample" in ticker_samples


def test_step2_gapfill_sanity_uses_seeded_match_not_rows_written(monkeypatch):
    seeded = ["AAPL.US", "MSFT.US", "NVDA.US"]
    db = _FakeDB(stock_counts={"2026-03-18": 2}, seeded_tickers=seeded)
    db.ops_job_runs.seeded_total = len(seeded)
    monkeypatch.setattr(scheduler_service, "MIN_BULK_ROWS_SANITY_CHECK", 9999)
    monkeypatch.setattr(scheduler_service, "MIN_BULK_MATCHED_SEEDED_SANITY_CHECK", 2)
    monkeypatch.setattr(
        scheduler_service,
        "STEP2_SANITY_THRESHOLD_USED",
        "matched_seeded_tickers_count >= 2",
    )

    async def _fake_fetch_bulk(_exchange="US", include_meta=False):
        payload = [
            {"code": "AAPL", "date": "2026-03-18", "close": 100, "adjusted_close": 100, "open": 99, "high": 101, "low": 98, "volume": 1000},
            {"code": "MSFT.US", "date": "2026-03-18", "close": 200, "adjusted_close": 200, "open": 199, "high": 201, "low": 198, "volume": 2000},
        ]
        for i in range(120):
            payload.append(
                {
                    "code": f"0P000{i}",
                    "date": "2026-03-18",
                    "close": 10,
                    "adjusted_close": 10,
                    "open": 10,
                    "high": 10,
                    "low": 10,
                    "volume": 1,
                }
            )
        payload.append(
            {
                "code": "^TNX",
                "date": "2026-03-18",
                "close": 4,
                "adjusted_close": 4,
                "open": 4,
                "high": 4,
                "low": 4,
                "volume": 1,
            }
        )
        _ = _exchange
        if include_meta:
            return payload, True
        return payload

    async def _fake_missed_dates(db, today_dt):
        _ = db, today_dt
        return [date(2026, 3, 18)]

    async def _fake_save_report(db, rows, now):
        _ = db, rows, now
        return {
            "exclusion_report_rows": 1,
            "exclusion_report_run_id": "price_sync_test_run",
            "exclusion_report_date": "2026-03-18",
        }

    async def _fake_detectors(db, progress_cb=None, exclusion_meta=None, cancel_check=None, processed_date=None):
        _ = db, progress_cb, exclusion_meta, cancel_check, processed_date
        return {"enqueued_total": 0, "skipped_total": 0, "cancelled": False}

    monkeypatch.setattr(price_ingestion_service, "fetch_bulk_eod_latest", _fake_fetch_bulk)
    monkeypatch.setattr(scheduler_service, "_get_missed_trading_dates", _fake_missed_dates)
    monkeypatch.setattr(scheduler_service, "save_price_sync_exclusion_report", _fake_save_report)
    monkeypatch.setattr(scheduler_service, "run_step2_event_detectors", _fake_detectors)

    result = asyncio.run(
        scheduler_service.run_daily_price_sync(
            db,
            ignore_kill_switch=True,
            parent_run_id="parent",
            chain_run_id="chain",
        )
    )

    assert result["status"] == "success"
    assert result["rows_written"] == 2
    assert result["matched_price_tickers_raw"] == 2
    assert result["tickers_with_price_data"] == 2
    assert result["tickers_without_price_data"] == 1
    assert result["matched_seeded_tickers_count"] == 2
    assert abs(result["match_ratio"] - (2 / 3)) < 1e-12
    assert result["sanity_threshold_used"] == "matched_seeded_tickers_count >= 2"

    details = db.ops_job_runs.latest["details"]
    day = details["price_bulk_gapfill"]["days"][0]
    assert day["status"] == "success"
    assert day["rows_written"] == 2
    assert day["matched_seeded_tickers_count"] == 2
    assert abs(day["match_ratio"] - (2 / 3)) < 1e-12
    assert day["sanity_threshold_used"] == "matched_seeded_tickers_count >= 2"
    assert details["matched_price_tickers_raw"] == 2
    assert details["tickers_with_price_data"] == 2
