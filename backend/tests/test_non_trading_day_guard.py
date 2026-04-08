"""
Tests for non-trading-day guard in price ingestion and scheduler services.

Validates that when EODHD returns data dated on a holiday or weekend,
the pipeline:
1. Skips writing prices (price_ingestion_service.py)
2. Returns completed status so the scheduler advances (scheduler_service.py)
3. Does NOT skip when the market_calendar has no row for the date (fail-open)
"""

import asyncio
from copy import deepcopy
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
import os
import sys

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import price_ingestion_service
import scheduler_service


# ── Fake DB helpers ──────────────────────────────────────────────────────────

def _set_path(doc, dotted_key, value):
    parts = dotted_key.split(".")
    cur = doc
    for part in parts[:-1]:
        if part not in cur or not isinstance(cur[part], dict):
            cur[part] = {}
        cur = cur[part]
    cur[parts[-1]] = value


class _FakeCursor:
    def __init__(self, docs):
        self._docs = docs

    async def to_list(self, _):
        return deepcopy(self._docs)


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
        doc = self.docs[_id]
        # Respect additional filter fields (e.g. {"_id": X, "status": "running"})
        for k, v in filt.items():
            if k == "_id":
                continue
            if doc.get(k) != v:
                return SimpleNamespace(matched_count=0)
        for key, value in (update.get("$set") or {}).items():
            _set_path(self.docs[_id], key, value)
        return SimpleNamespace(matched_count=1)

    async def find_one(self, filt, projection=None, sort=None):
        if filt.get("job_name") == "universe_seed":
            return {"details": {"seeded_total": self.seeded_total}}
        _id = filt.get("_id")
        if _id in self.docs:
            doc = self.docs[_id]
            # Respect additional filter fields (e.g. {"_id": X, "status": "running"})
            for k, v in filt.items():
                if k == "_id":
                    continue
                if doc.get(k) != v:
                    return None
            return deepcopy(doc)
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
    async def count_documents(self, filt):
        return 0

    async def bulk_write(self, batch, ordered=False):
        _ = ordered
        return SimpleNamespace(upserted_count=len(batch), modified_count=0)


class _FakeMarketCalendar:
    """Fake market_calendar collection supporting find_one."""
    def __init__(self, rows=None):
        self.rows = list(rows or [])

    async def find_one(self, filt, projection=None):
        for row in self.rows:
            match = all(row.get(k) == v for k, v in filt.items() if k != "_id")
            if match:
                if projection:
                    return {k: row.get(k) for k in projection if k in row}
                return deepcopy(row)
        return None


class _FakeOpsLocks:
    """Fake ops_locks collection supporting single-flight lock operations."""
    def __init__(self):
        self.docs = {}

    async def create_index(self, keys, name=None, expireAfterSeconds=None):
        pass

    async def update_one(self, filt, update):
        _id = filt.get("_id")
        if _id and _id in self.docs:
            doc = self.docs[_id]
            # Check $or conditions for lock reuse
            or_conditions = filt.get("$or", [])
            matched = False
            for cond in or_conditions:
                if "owner_run_id" in cond and doc.get("owner_run_id") == cond["owner_run_id"]:
                    matched = True
                    break
                if "expires_at" in cond:
                    lte = cond["expires_at"].get("$lte")
                    if lte and doc.get("expires_at") and doc["expires_at"] <= lte:
                        matched = True
                        break
            if not matched and or_conditions:
                return SimpleNamespace(matched_count=0)
            for k, v in (update.get("$set") or {}).items():
                doc[k] = v
            return SimpleNamespace(matched_count=1)
        return SimpleNamespace(matched_count=0)

    async def insert_one(self, doc):
        _id = doc.get("_id")
        if _id in self.docs:
            from pymongo.errors import DuplicateKeyError
            raise DuplicateKeyError("duplicate key")
        self.docs[_id] = deepcopy(doc)
        return SimpleNamespace(inserted_id=_id)

    async def delete_one(self, filt):
        _id = filt.get("_id")
        owner = filt.get("owner_run_id")
        if _id in self.docs:
            if owner is None or self.docs[_id].get("owner_run_id") == owner:
                del self.docs[_id]
                return SimpleNamespace(deleted_count=1)
        return SimpleNamespace(deleted_count=0)


class _FakeDB:
    def __init__(
        self, *, stock_counts=None, initial_pipeline_state=None,
        seeded_tickers=None, calendar_rows=None, seeded_total=5000,
    ):
        self.ops_job_runs = _FakeOpsJobRuns(seeded_total=seeded_total)
        self.tracked_tickers = _FakeTrackedTickers(seeded_tickers or ["AAPL.US", "MSFT.US"])
        self.ops_config = _FakeOpsConfig()
        self.ops_locks = _FakeOpsLocks()
        self.pipeline_state = _FakePipelineState(initial=initial_pipeline_state)
        self.stock_prices = _FakeStockPrices()
        self.market_calendar = _FakeMarketCalendar(rows=calendar_rows)
        self._collections = {
            "market_calendar": self.market_calendar,
        }

    def __getitem__(self, key):
        return self._collections.get(key, MagicMock())


# ── Helper ───────────────────────────────────────────────────────────────────

def _patch_non_gapfill_dependencies(monkeypatch):
    async def _fake_flags(db, include_exclusions=False, tickers_with_price=None, bulk_date=None):
        return {
            "seeded_total": 2,
            "with_price_data": 2,
            "without_price_data": 0,
            "matched_price_tickers_raw": 2,
            "exclusions": [],
        }

    async def _fake_save_report(db, rows, now):
        return {
            "exclusion_report_rows": 0,
            "exclusion_report_run_id": "price_sync_test_run",
            "exclusion_report_date": "2026-04-02",
        }

    async def _fake_detectors(db, progress_cb=None, exclusion_meta=None, cancel_check=None, processed_date=None):
        return {"enqueued_total": 0, "skipped_total": 0, "cancelled": False}

    # Mock market_calendar helpers used by run_daily_price_sync
    import services.market_calendar_service as _mc
    async def _fake_is_calendar_fresh(db, market="US"):
        return True
    async def _fake_get_last_closing_day(db, market="US", *, as_of_date=None):
        return "2026-04-02"
    monkeypatch.setattr(_mc, "is_calendar_fresh", _fake_is_calendar_fresh)
    monkeypatch.setattr(_mc, "get_last_closing_day", _fake_get_last_closing_day)

    # Mock fetch_bulk_eod_latest for LCD pre-fetch
    async def _fake_fetch_bulk_for_lcd(_exchange="US", include_meta=False, *, for_date=None):
        _d = for_date or "2026-04-02"
        payload = [
            {"code": "AAPL", "date": _d, "close": 100, "open": 99, "high": 101, "low": 98, "volume": 1000, "adjusted_close": 100},
            {"code": "MSFT", "date": _d, "close": 200, "open": 199, "high": 201, "low": 198, "volume": 2000, "adjusted_close": 200},
        ]
        if include_meta:
            return payload, True
        return payload
    monkeypatch.setattr(price_ingestion_service, "fetch_bulk_eod_latest", _fake_fetch_bulk_for_lcd)

    monkeypatch.setattr(scheduler_service, "sync_has_price_data_flags", _fake_flags)
    monkeypatch.setattr(scheduler_service, "save_price_sync_exclusion_report", _fake_save_report)
    monkeypatch.setattr(scheduler_service, "run_step2_event_detectors", _fake_detectors)
    monkeypatch.setattr(scheduler_service, "MIN_BULK_MATCHED_SEEDED_SANITY_CHECK", 1)
    monkeypatch.setattr(
        scheduler_service,
        "STEP2_SANITY_THRESHOLD_USED",
        "matched_seeded_tickers_count >= 1",
    )


# =============================================================================
# Tests: price_ingestion_service — bulk catchup non-trading-day guard
# =============================================================================


class TestBulkCatchupNonTradingDayGuard:
    """Tests for market calendar guard in run_daily_bulk_catchup."""

    def test_skips_write_when_date_is_holiday(self, monkeypatch):
        """EODHD returns data dated on Good Friday → guard skips write."""
        holiday_date = "2026-04-03"
        calendar_rows = [
            {
                "market": "US",
                "date": holiday_date,
                "is_trading_day": False,
                "holiday_name": "Good Friday",
            },
        ]
        db = _FakeDB(calendar_rows=calendar_rows, seeded_tickers=["AAPL.US", "MSFT.US"])

        # Fake EODHD returning data for the holiday date
        async def _fake_fetch(exchange, include_meta=False, **kwargs):
            data = [
                {"code": "AAPL", "date": holiday_date, "open": 150, "high": 155, "low": 148, "close": 152, "adjusted_close": 152, "volume": 100},
                {"code": "MSFT", "date": holiday_date, "open": 300, "high": 310, "low": 295, "close": 305, "adjusted_close": 305, "volume": 200},
            ]
            if include_meta:
                return data, True
            return data

        monkeypatch.setattr(price_ingestion_service, "fetch_bulk_eod_latest", _fake_fetch)

        result = asyncio.run(
            price_ingestion_service.run_daily_bulk_catchup(
                db,
                seeded_tickers_override={"AAPL.US", "MSFT.US"},
                latest_trading_day=holiday_date,
            )
        )

        assert result["status"] == "skipped"
        assert result["skipped_reason"] == "non_trading_day"
        assert result["date"] == holiday_date
        assert result["records_upserted"] == 0
        assert result["bulk_writes"] == 0
        assert "Good Friday" in result["message"]

    def test_skips_write_when_date_is_weekend(self, monkeypatch):
        """EODHD returns data dated on Saturday → guard skips write."""
        weekend_date = "2026-04-04"
        calendar_rows = [
            {
                "market": "US",
                "date": weekend_date,
                "is_trading_day": False,
                "holiday_name": None,
            },
        ]
        db = _FakeDB(calendar_rows=calendar_rows, seeded_tickers=["AAPL.US"])

        async def _fake_fetch(exchange, include_meta=False, **kwargs):
            data = [
                {"code": "AAPL", "date": weekend_date, "open": 150, "high": 155, "low": 148, "close": 152, "adjusted_close": 152, "volume": 0},
            ]
            if include_meta:
                return data, True
            return data

        monkeypatch.setattr(price_ingestion_service, "fetch_bulk_eod_latest", _fake_fetch)

        result = asyncio.run(
            price_ingestion_service.run_daily_bulk_catchup(
                db,
                seeded_tickers_override={"AAPL.US"},
                latest_trading_day=weekend_date,
            )
        )

        assert result["status"] == "skipped"
        assert result["skipped_reason"] == "non_trading_day"

    def test_proceeds_when_date_is_trading_day(self, monkeypatch):
        """EODHD returns data for a normal trading day → guard allows write."""
        trading_date = "2026-04-02"
        calendar_rows = [
            {
                "market": "US",
                "date": trading_date,
                "is_trading_day": True,
                "trading_hours": {"open": "09:30", "close": "16:00"},
                "holiday_name": None,
            },
        ]
        db = _FakeDB(calendar_rows=calendar_rows, seeded_tickers=["AAPL.US"])

        async def _fake_fetch(exchange, include_meta=False, **kwargs):
            data = [
                {"code": "AAPL", "date": trading_date, "open": 150, "high": 155, "low": 148, "close": 152, "adjusted_close": 152, "volume": 50000000},
            ]
            if include_meta:
                return data, True
            return data

        monkeypatch.setattr(price_ingestion_service, "fetch_bulk_eod_latest", _fake_fetch)

        result = asyncio.run(
            price_ingestion_service.run_daily_bulk_catchup(
                db,
                seeded_tickers_override={"AAPL.US"},
                latest_trading_day=trading_date,
            )
        )

        assert result["status"] == "success"
        assert result["records_upserted"] > 0

    def test_fail_open_when_no_calendar_row(self, monkeypatch):
        """No calendar row for date → guard proceeds with write (fail-open)."""
        date_str = "2026-04-02"
        # No calendar rows at all
        db = _FakeDB(calendar_rows=[], seeded_tickers=["AAPL.US"])

        async def _fake_fetch(exchange, include_meta=False, **kwargs):
            data = [
                {"code": "AAPL", "date": date_str, "open": 150, "high": 155, "low": 148, "close": 152, "adjusted_close": 152, "volume": 50000000},
            ]
            if include_meta:
                return data, True
            return data

        monkeypatch.setattr(price_ingestion_service, "fetch_bulk_eod_latest", _fake_fetch)

        result = asyncio.run(
            price_ingestion_service.run_daily_bulk_catchup(
                db,
                seeded_tickers_override={"AAPL.US"},
                latest_trading_day=date_str,
            )
        )

        # Should NOT skip — fail-open means proceed with write
        assert result["status"] == "success"
        assert result["records_upserted"] > 0


class TestBulkDateRequired:
    """fetch_bulk_eod_latest and run_daily_bulk_catchup MUST receive a date."""

    def test_fetch_bulk_rejects_none_date(self):
        """fetch_bulk_eod_latest raises ValueError when for_date is missing."""
        with pytest.raises(ValueError, match="for_date is required"):
            asyncio.run(price_ingestion_service.fetch_bulk_eod_latest("US", for_date=None))

    def test_fetch_bulk_rejects_empty_date(self):
        """fetch_bulk_eod_latest raises ValueError when for_date is ''."""
        with pytest.raises(ValueError, match="for_date is required"):
            asyncio.run(price_ingestion_service.fetch_bulk_eod_latest("US", for_date=""))

    def test_bulk_catchup_rejects_none_date(self):
        """run_daily_bulk_catchup raises ValueError when latest_trading_day is None."""
        db = _FakeDB(seeded_tickers=["AAPL.US"])
        with pytest.raises(ValueError, match="latest_trading_day is required"):
            asyncio.run(
                price_ingestion_service.run_daily_bulk_catchup(
                    db, latest_trading_day=None,
                )
            )

    def test_bulk_catchup_rejects_empty_date(self):
        """run_daily_bulk_catchup raises ValueError when latest_trading_day is ''."""
        db = _FakeDB(seeded_tickers=["AAPL.US"])
        with pytest.raises(ValueError, match="latest_trading_day is required"):
            asyncio.run(
                price_ingestion_service.run_daily_bulk_catchup(
                    db, latest_trading_day="",
                )
            )


# =============================================================================
# Tests: scheduler_service — run_daily_price_sync non-trading-day handling
# =============================================================================


class TestPriceSyncNonTradingDaySkip:
    """Tests that run_daily_price_sync handles non-trading-day skip correctly."""

    def test_price_sync_returns_completed_on_non_trading_day(self, monkeypatch):
        """When bulk catchup detects a holiday, price_sync returns completed."""
        _patch_non_gapfill_dependencies(monkeypatch)
        db = _FakeDB(seeded_tickers=["AAPL.US", "MSFT.US"])

        async def _fake_bulk(db, job_name="price_sync", progress_cb=None, seeded_tickers_override=None, **kwargs):
            return {
                "status": "skipped",
                "skipped_reason": "non_trading_day",
                "date": "2026-04-03",
                "processed_date": "2026-04-03",
                "unique_dates": ["2026-04-03"],
                "dates_processed": 0,
                "records_upserted": 0,
                "rows_written": 0,
                "matched_price_tickers_raw": 0,
                "tickers_with_price_data": 0,
                "api_calls": 1,
                "bulk_fetch_executed": True,
                "raw_row_count": 8000,
                "bulk_writes": 0,
                "bulk_url_used": "https://eodhd.com/api/eod-bulk-last-day/US",
                "tickers_with_price": [],
                "ticker_samples": {},
                "holiday_name": "Good Friday",
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

        assert result["status"] == "completed"
        assert result.get("skipped_reason") == "non_trading_day"
        assert result.get("skipped_date") == "2026-04-03"
        # Should NOT have run detectors or written prices
        assert result.get("records_upserted", 0) == 0
        # BUT should have synced has_price_data flags via DB fallback
        flag_sync = result.get("non_trading_day_flag_sync")
        assert flag_sync is not None, (
            "Non-trading-day path must still sync has_price_data flags"
        )
        assert flag_sync["seeded_total"] > 0
        assert flag_sync["with_price_data"] > 0, (
            "DB fallback should find existing price data from previous trading days"
        )

    def test_price_sync_ops_job_runs_marked_completed(self, monkeypatch):
        """Non-trading-day skip updates ops_job_runs with completed status."""
        _patch_non_gapfill_dependencies(monkeypatch)
        db = _FakeDB(seeded_tickers=["AAPL.US"])

        async def _fake_bulk(db, job_name="price_sync", progress_cb=None, seeded_tickers_override=None, **kwargs):
            return {
                "status": "skipped",
                "skipped_reason": "non_trading_day",
                "date": "2026-04-03",
                "processed_date": "2026-04-03",
                "unique_dates": ["2026-04-03"],
                "dates_processed": 0,
                "records_upserted": 0,
                "rows_written": 0,
                "matched_price_tickers_raw": 0,
                "tickers_with_price_data": 0,
                "api_calls": 1,
                "bulk_fetch_executed": True,
                "raw_row_count": 100,
                "bulk_writes": 0,
                "bulk_url_used": "https://eodhd.com/api/eod-bulk-last-day/US",
                "tickers_with_price": [],
                "ticker_samples": {},
            }

        monkeypatch.setattr("price_ingestion_service.run_daily_bulk_catchup", _fake_bulk)

        asyncio.run(
            scheduler_service.run_daily_price_sync(
                db,
                ignore_kill_switch=True,
                parent_run_id="parent",
                chain_run_id="chain",
            )
        )

        latest_doc = db.ops_job_runs.latest
        assert latest_doc is not None
        assert latest_doc["status"] == "completed"
        assert latest_doc["phase"] == "completed"
        # Flag sync summary stored in details
        assert "non_trading_day_flag_sync" in latest_doc.get("details", {})

    def test_normal_trading_day_still_works(self, monkeypatch):
        """Normal trading day data goes through full pipeline."""
        _patch_non_gapfill_dependencies(monkeypatch)
        db = _FakeDB(seeded_tickers=["AAPL.US", "MSFT.US"])

        async def _fake_bulk(db, job_name="price_sync", progress_cb=None, seeded_tickers_override=None, **kwargs):
            return {
                "status": "success",
                "dates_processed": 1,
                "records_upserted": 5001,
                "api_calls": 1,
                "bulk_fetch_executed": True,
                "bulk_writes": 1,
                "tickers_with_price": ["AAPL.US", "MSFT.US"],
                "date": "2026-04-02",
                "processed_date": "2026-04-02",
                "unique_dates": ["2026-04-02"],
                "bulk_url_used": "https://eodhd.com/api/eod-bulk-last-day/US",
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

        assert result["status"] in ("success", "completed")
        assert result.get("skipped_reason") is None


# =============================================================================
# Tests: sync_has_price_data_flags — flag semantics
# =============================================================================


class TestSyncHasPriceDataFlagsGuard:
    """Tests that sync_has_price_data_flags sets the correct flag semantics."""

    def test_empty_list_skips_flag_reset(self, monkeypatch):
        """When tickers_with_price=[] (non-trading day), flags are NOT reset."""

        update_calls = []

        class _FakeTrackedTickersForFlags:
            def __init__(self, tickers):
                self._tickers = list(tickers)

            def find(self, query, projection=None):
                return _FakeCursor([{"ticker": t, "name": t} for t in self._tickers])

            async def update_many(self, filt, update):
                update_calls.append((filt, update))
                return SimpleNamespace(modified_count=0)

        class _FakeDBForFlags:
            def __init__(self, tickers):
                self.tracked_tickers = _FakeTrackedTickersForFlags(tickers)

        seeded = ["AAPL.US", "MSFT.US", "GOOGL.US", "AMZN.US"]
        db = _FakeDBForFlags(seeded)

        result = asyncio.run(
            scheduler_service.sync_has_price_data_flags(
                db,
                include_exclusions=False,
                tickers_with_price=[],  # empty list — non-trading day
            )
        )

        # Must skip flag reset entirely — no update_many calls
        assert result["skipped_flag_reset"] is True, (
            "Empty tickers_with_price should skip flag reset"
        )
        assert len(update_calls) == 0, (
            f"Expected 0 update_many calls on empty bulk, got {len(update_calls)}"
        )

    def test_nonempty_list_uses_fast_path(self, monkeypatch):
        """When tickers_with_price has entries, fast path is used."""

        class _FakeStockPricesEmpty:
            async def distinct(self, field, query):
                return []

        class _FakeTrackedTickersForFlags:
            def __init__(self, tickers):
                self._tickers = list(tickers)

            def find(self, query, projection=None):
                return _FakeCursor([{"ticker": t, "name": t} for t in self._tickers])

            async def update_many(self, filt, update):
                return SimpleNamespace(modified_count=0)

        class _FakeDBForFlags:
            def __init__(self, tickers):
                self.tracked_tickers = _FakeTrackedTickersForFlags(tickers)
                self.stock_prices = _FakeStockPricesEmpty()

        seeded = ["AAPL.US", "MSFT.US"]
        db = _FakeDBForFlags(seeded)

        result = asyncio.run(
            scheduler_service.sync_has_price_data_flags(
                db,
                include_exclusions=False,
                tickers_with_price=["AAPL.US", "MSFT.US"],
            )
        )

        assert result["with_price_data"] == 2
        assert result["with_latest_bulk_close"] == 2

    def test_ticker_not_in_bulk_becomes_invisible(self, monkeypatch):
        """Ticker NOT in today's bulk gets has_price_data=False (invisible),
        even if it has existing stock_prices records."""

        # Track which tickers get which flag values
        flag_writes: dict = {}

        class _FakeStockPricesWithExisting:
            async def distinct(self, field, query):
                # NYC.US has existing stock_prices from Phase C —
                # but that must NOT preserve visibility.
                tickers_queried = query.get("ticker", {}).get("$in", [])
                return [t for t in tickers_queried if t in ("NYC.US",)]

        class _FakeTrackedTickersForFlags:
            def __init__(self, tickers):
                self._tickers = list(tickers)

            def find(self, query, projection=None):
                return _FakeCursor([{"ticker": t, "name": t} for t in self._tickers])

            async def update_many(self, filt, update):
                set_fields = update.get("$set", {})
                tickers_in = filt.get("ticker", {}).get("$in", [])
                for t in tickers_in:
                    if t not in flag_writes:
                        flag_writes[t] = {}
                    flag_writes[t].update(set_fields)
                return SimpleNamespace(modified_count=0)

        class _FakeGapFreeExclusions:
            def __init__(self):
                self.written = []
            async def bulk_write(self, ops, ordered=False):
                self.written.extend(ops)
                return SimpleNamespace(upserted_count=len(ops))

        class _FakeDBForFlags:
            def __init__(self, tickers):
                self.tracked_tickers = _FakeTrackedTickersForFlags(tickers)
                self.stock_prices = _FakeStockPricesWithExisting()
                self.gap_free_exclusions = _FakeGapFreeExclusions()

        # AAPL.US is in today's bulk, NYC.US is NOT
        seeded = ["AAPL.US", "NYC.US"]
        db = _FakeDBForFlags(seeded)

        result = asyncio.run(
            scheduler_service.sync_has_price_data_flags(
                db,
                include_exclusions=False,
                tickers_with_price=["AAPL.US"],  # NYC.US NOT in today's bulk
            )
        )

        # Only AAPL should have has_latest_bulk_close=True
        assert result["with_latest_bulk_close"] == 1, (
            f"Expected 1 ticker with bulk close, got {result['with_latest_bulk_close']}"
        )

        # has_price_data = bulk tickers ONLY (no preservation)
        assert result["with_price_data"] == 1, (
            "has_price_data should include ONLY bulk tickers (AAPL), not preserved"
        )

        # NYC.US must have has_price_data=False — NOT in today's bulk
        assert flag_writes.get("NYC.US", {}).get("has_price_data") is False, (
            "NYC.US must have has_price_data=False (not in today's bulk)"
        )
        assert flag_writes.get("NYC.US", {}).get("has_latest_bulk_close") is False, (
            "NYC.US must NOT have has_latest_bulk_close=True (not in today's bulk)"
        )

    def test_not_in_bulk_writes_gap_free_exclusions(self, monkeypatch):
        """Tickers NOT in today's bulk get gap_free_exclusions entries
        when bulk_date is provided."""

        flag_writes: dict = {}

        class _FakeTrackedTickers:
            def __init__(self, tickers):
                self._tickers = list(tickers)
            def find(self, query, projection=None):
                return _FakeCursor([{"ticker": t, "name": t} for t in self._tickers])
            async def update_many(self, filt, update):
                set_fields = update.get("$set", {})
                tickers_in = filt.get("ticker", {}).get("$in", [])
                for t in tickers_in:
                    if t not in flag_writes:
                        flag_writes[t] = {}
                    flag_writes[t].update(set_fields)
                return SimpleNamespace(modified_count=0)

        class _FakeGapFreeExclusions:
            def __init__(self):
                self.ops = []
            async def bulk_write(self, ops, ordered=False):
                self.ops.extend(ops)
                return SimpleNamespace(upserted_count=len(ops))

        class _FakeDB:
            def __init__(self, tickers):
                self.tracked_tickers = _FakeTrackedTickers(tickers)
                self.gap_free_exclusions = _FakeGapFreeExclusions()

        seeded = ["AAPL.US", "NYC.US", "BYC.US"]
        db = _FakeDB(seeded)

        result = asyncio.run(
            scheduler_service.sync_has_price_data_flags(
                db,
                include_exclusions=False,
                tickers_with_price=["AAPL.US"],
                bulk_date="2026-04-02",
            )
        )

        # Should have written exclusions for NYC.US and BYC.US (not in bulk)
        assert result["gap_free_exclusions_written"] == 2, (
            f"Expected 2 exclusions, got {result['gap_free_exclusions_written']}"
        )
        assert len(db.gap_free_exclusions.ops) == 2, (
            "Expected 2 bulk_write ops for gap_free_exclusions"
        )

    def test_no_exclusions_without_bulk_date(self, monkeypatch):
        """No gap_free_exclusions written when bulk_date is not provided."""

        class _FakeTrackedTickers:
            def __init__(self, tickers):
                self._tickers = list(tickers)
            def find(self, query, projection=None):
                return _FakeCursor([{"ticker": t, "name": t} for t in self._tickers])
            async def update_many(self, filt, update):
                return SimpleNamespace(modified_count=0)

        class _FakeGapFreeExclusions:
            def __init__(self):
                self.ops = []
            async def bulk_write(self, ops, ordered=False):
                self.ops.extend(ops)
                return SimpleNamespace(upserted_count=len(ops))

        class _FakeDB:
            def __init__(self, tickers):
                self.tracked_tickers = _FakeTrackedTickers(tickers)
                self.gap_free_exclusions = _FakeGapFreeExclusions()

        seeded = ["AAPL.US", "NYC.US"]
        db = _FakeDB(seeded)

        result = asyncio.run(
            scheduler_service.sync_has_price_data_flags(
                db,
                include_exclusions=False,
                tickers_with_price=["AAPL.US"],
                # bulk_date NOT provided
            )
        )

        assert result["gap_free_exclusions_written"] == 0
        assert len(db.gap_free_exclusions.ops) == 0, (
            "No exclusions should be written without bulk_date"
        )

    def test_returning_ticker_flagged_for_redownload(self, monkeypatch):
        """Ticker absent from yesterday's bulk but back in today's bulk
        with price_history_complete=True gets needs_price_redownload=True."""

        flag_writes: dict = {}
        find_queries: list = []

        class _FakeTrackedTickers:
            def __init__(self, tickers):
                self._tickers = list(tickers)
            def find(self, query, projection=None):
                find_queries.append(query)
                # For the returning-tickers detection query
                if "has_latest_bulk_close" in query:
                    # NYC.US was absent yesterday (has_latest_bulk_close=False)
                    # and has price_history_complete=True
                    return _FakeCursor([{"ticker": "NYC.US"}])
                return _FakeCursor([{"ticker": t, "name": t} for t in self._tickers])
            async def update_many(self, filt, update):
                set_fields = update.get("$set", {})
                tickers_in = filt.get("ticker", {}).get("$in", [])
                for t in tickers_in:
                    if t not in flag_writes:
                        flag_writes[t] = {}
                    flag_writes[t].update(set_fields)
                return SimpleNamespace(modified_count=1)

        class _FakeGapFreeExclusions:
            async def bulk_write(self, ops, ordered=False):
                return SimpleNamespace(upserted_count=len(ops))

        class _FakeDB:
            def __init__(self, tickers):
                self.tracked_tickers = _FakeTrackedTickers(tickers)
                self.gap_free_exclusions = _FakeGapFreeExclusions()

        # Both in today's bulk; NYC.US was absent yesterday
        seeded = ["AAPL.US", "NYC.US"]
        db = _FakeDB(seeded)

        result = asyncio.run(
            scheduler_service.sync_has_price_data_flags(
                db,
                include_exclusions=False,
                tickers_with_price=["AAPL.US", "NYC.US"],
                bulk_date="2026-04-02",
            )
        )

        assert result["returning_tickers_flagged_for_redownload"] == 1, (
            f"Expected 1 returning ticker, got {result['returning_tickers_flagged_for_redownload']}"
        )
        # NYC.US should have needs_price_redownload=True
        assert flag_writes.get("NYC.US", {}).get("needs_price_redownload") is True, (
            "NYC.US must have needs_price_redownload=True after returning to bulk"
        )
        assert flag_writes.get("NYC.US", {}).get("price_history_complete") is False, (
            "NYC.US must have price_history_complete=False for Phase C re-download"
        )


# =============================================================================
# Tests: _reconcile_logo_completeness — stale-complete logo detection
# =============================================================================


class TestReconcileLogoCompleteness:
    """Tests that Step 3 detects tickers marked complete without resolved logos."""

    def test_resets_tickers_without_logo_status(self):
        """Tickers marked complete but with no logo_status in cache are reset."""

        updated_tickers = []

        class _FakeTrackedTickersForLogo:
            async def distinct(self, field, query):
                # Return tickers that are fundamentals_status="complete"
                return ["AAPL.US", "MSFT.US", "GOOGL.US"]

            async def update_many(self, filt, update):
                updated_tickers.extend(filt.get("ticker", {}).get("$in", []))
                return SimpleNamespace(modified_count=len(updated_tickers))

        class _FakeFundamentalsCache:
            async def distinct(self, field, query):
                # Only AAPL has logo resolved — MSFT and GOOGL don't
                return ["AAPL.US"]

        class _FakeDBForLogo:
            tracked_tickers = _FakeTrackedTickersForLogo()
            company_fundamentals_cache = _FakeFundamentalsCache()

        db = _FakeDBForLogo()
        result = asyncio.run(scheduler_service._reconcile_logo_completeness(db))

        assert result["reset_count"] == 2
        assert "MSFT.US" in result["reset_tickers"]
        assert "GOOGL.US" in result["reset_tickers"]
        assert "AAPL.US" not in result["reset_tickers"]
        # Verify update_many was called with the stale tickers
        assert "MSFT.US" in updated_tickers
        assert "GOOGL.US" in updated_tickers

    def test_no_reset_when_all_logos_resolved(self):
        """When all complete tickers have resolved logos, nothing is reset."""

        class _FakeTrackedTickersForLogo:
            async def distinct(self, field, query):
                return ["AAPL.US", "MSFT.US"]

            async def update_many(self, filt, update):
                raise AssertionError("update_many should not be called")

        class _FakeFundamentalsCache:
            async def distinct(self, field, query):
                # Both tickers have logo resolved
                return ["AAPL.US", "MSFT.US"]

        class _FakeDBForLogo:
            tracked_tickers = _FakeTrackedTickersForLogo()
            company_fundamentals_cache = _FakeFundamentalsCache()

        db = _FakeDBForLogo()
        result = asyncio.run(scheduler_service._reconcile_logo_completeness(db))

        assert result["reset_count"] == 0
        assert result["reset_tickers"] == []

    def test_no_reset_when_no_complete_tickers(self):
        """When no tickers are marked complete, nothing is reset."""

        class _FakeTrackedTickersForLogo:
            async def distinct(self, field, query):
                return []  # No complete tickers

        class _FakeFundamentalsCache:
            async def distinct(self, field, query):
                return []

        class _FakeDBForLogo:
            tracked_tickers = _FakeTrackedTickersForLogo()
            company_fundamentals_cache = _FakeFundamentalsCache()

        db = _FakeDBForLogo()
        result = asyncio.run(scheduler_service._reconcile_logo_completeness(db))

        assert result["reset_count"] == 0
        assert result["reset_tickers"] == []
