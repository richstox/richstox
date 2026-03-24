"""
Tests for market_calendar_service.py

Tests the core runtime functions using a mock MongoDB.
Does NOT test the EODHD refresh (that requires a real API key).
"""

import asyncio
from datetime import date, datetime, time, timezone
from unittest.mock import AsyncMock, MagicMock, patch
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

_NY_TZ = ZoneInfo("America/New_York")

# ── Helpers ──────────────────────────────────────────────────────────────────


def _make_cursor(docs):
    """Create an async iterable cursor that also has .to_list() and .sort().limit()."""
    class _Cursor:
        def __init__(self, data):
            self._data = list(data)
        def sort(self, *args, **kwargs):
            if args and args[0] == "date":
                direction = args[1] if len(args) > 1 else -1
                reverse = direction == -1
                self._data = sorted(self._data, key=lambda d: d.get("date", ""), reverse=reverse)
            return self
        def limit(self, n):
            self._data = self._data[:n]
            return self
        async def to_list(self, length=None):
            if length is not None:
                return self._data[:length]
            return self._data
        def __aiter__(self):
            return _CursorIter(self._data)
    class _CursorIter:
        def __init__(self, data):
            self._iter = iter(data)
        async def __anext__(self):
            try:
                return next(self._iter)
            except StopIteration:
                raise StopAsyncIteration
    return _Cursor(docs)


def _build_calendar_docs(dates_trading, dates_holidays=None, market="US"):
    """Build mock market_calendar documents.
    
    dates_trading: list of date strings that are trading days
    dates_holidays: dict of date_str -> holiday_name
    """
    if dates_holidays is None:
        dates_holidays = {}
    
    docs = []
    for d in dates_trading:
        docs.append({
            "market": market,
            "date": d,
            "is_trading_day": True,
            "trading_hours": {"open": "09:30", "close": "16:00"},
            "holiday_name": None,
            "early_close_time": None,
            "timezone": "America/New_York",
        })
    for d, name in dates_holidays.items():
        docs.append({
            "market": market,
            "date": d,
            "is_trading_day": False,
            "trading_hours": None,
            "holiday_name": name,
            "early_close_time": None,
            "timezone": "America/New_York",
        })
    return docs


class MockCollection:
    """Minimal mock for a MongoDB collection supporting find, find_one, aggregate."""
    
    def __init__(self, docs=None):
        self._docs = list(docs or [])
    
    async def find_one(self, query=None, projection=None, sort=None):
        matches = self._filter(query or {})
        if sort:
            if isinstance(sort, list):
                field, direction = sort[0]
            else:
                field, direction = sort
            matches = sorted(matches, key=lambda d: d.get(field, ""), reverse=(direction == -1))
        if matches:
            doc = dict(matches[0])
            if projection:
                result = {}
                for k in projection:
                    if k != "_id" and k in doc:
                        result[k] = doc[k]
                return result
            return doc
        return None
    
    def find(self, query=None, projection=None):
        matches = self._filter(query or {})
        if projection:
            filtered = []
            for doc in matches:
                result = {}
                for k in projection:
                    if k != "_id" and k in doc:
                        result[k] = doc[k]
                filtered.append(result)
            return _make_cursor(filtered)
        return _make_cursor(matches)
    
    def aggregate(self, pipeline):
        # Simplified: just return all matching docs for basic pipelines
        return _make_cursor(self._docs)
    
    async def create_index(self, *args, **kwargs):
        pass
    
    async def bulk_write(self, ops, **kwargs):
        return SimpleNamespace(upserted_count=len(ops), modified_count=0)
    
    def _filter(self, query):
        results = []
        for doc in self._docs:
            if self._matches(doc, query):
                results.append(doc)
        return results
    
    def _matches(self, doc, query):
        for key, condition in query.items():
            val = doc.get(key)
            if isinstance(condition, dict):
                for op, op_val in condition.items():
                    if op == "$lt" and not (val is not None and val < op_val):
                        return False
                    elif op == "$lte" and not (val is not None and val <= op_val):
                        return False
                    elif op == "$gt" and not (val is not None and val > op_val):
                        return False
                    elif op == "$gte" and not (val is not None and val >= op_val):
                        return False
                    elif op == "$in" and val not in op_val:
                        return False
                    elif op == "$exists" and op_val and val is None:
                        return False
            else:
                if val != condition:
                    return False
        return True


class MockDB:
    """Mock database with named collections."""
    def __init__(self, **collections):
        self._collections = collections
    
    def __getattr__(self, name):
        if name.startswith("_"):
            return super().__getattribute__(name)
        return self._collections.get(name, MockCollection())
    
    def __getitem__(self, name):
        return self._collections.get(name, MockCollection())


# ── Tests ────────────────────────────────────────────────────────────────────


class TestIsTradingDay:
    """Tests for is_trading_day()."""

    @pytest.mark.asyncio
    async def test_trading_day_returns_true(self):
        from services.market_calendar_service import is_trading_day
        
        db = MockDB(market_calendar=MockCollection([
            {"market": "US", "date": "2026-03-23", "is_trading_day": True},
        ]))
        assert await is_trading_day(db, "2026-03-23") is True

    @pytest.mark.asyncio
    async def test_holiday_returns_false(self):
        from services.market_calendar_service import is_trading_day
        
        db = MockDB(market_calendar=MockCollection([
            {"market": "US", "date": "2026-01-01", "is_trading_day": False,
             "holiday_name": "New Year's Day"},
        ]))
        assert await is_trading_day(db, "2026-01-01") is False

    @pytest.mark.asyncio
    async def test_missing_calendar_row_returns_false(self):
        """When no calendar row exists, is_trading_day returns False (fail-closed).
        
        market_calendar is the single source of truth — we do NOT fall back
        to weekday heuristics when a row is missing.
        """
        from services.market_calendar_service import is_trading_day
        
        db = MockDB(market_calendar=MockCollection([]))
        # 2026-03-24 is a Tuesday (would be a weekday) but no calendar row exists
        assert await is_trading_day(db, "2026-03-24") is False
        # 2026-03-22 is a Sunday — also False
        assert await is_trading_day(db, "2026-03-22") is False


class TestLastNTradingDays:
    """Tests for last_n_trading_days()."""

    @pytest.mark.asyncio
    async def test_returns_n_days_reverse_order(self):
        from services.market_calendar_service import last_n_trading_days
        
        trading_dates = [
            "2026-03-16", "2026-03-17", "2026-03-18",
            "2026-03-19", "2026-03-20", "2026-03-23",
        ]
        docs = _build_calendar_docs(trading_dates)
        db = MockDB(market_calendar=MockCollection(docs))
        
        result = await last_n_trading_days(db, 3, before_date="2026-03-24")
        assert result == ["2026-03-23", "2026-03-20", "2026-03-19"]

    @pytest.mark.asyncio
    async def test_returns_empty_when_no_data(self):
        from services.market_calendar_service import last_n_trading_days
        
        db = MockDB(market_calendar=MockCollection([]))
        result = await last_n_trading_days(db, 5, before_date="2026-03-24")
        assert result == []


class TestLastNCompletedTradingDays:
    """Tests for last_n_completed_trading_days()."""

    @pytest.mark.asyncio
    async def test_excludes_today_before_close(self):
        """If market hasn't closed yet, today should NOT be in results."""
        from services.market_calendar_service import last_n_completed_trading_days
        
        trading_dates = [
            "2026-03-19", "2026-03-20", "2026-03-23", "2026-03-24",
        ]
        docs = _build_calendar_docs(trading_dates)
        db = MockDB(market_calendar=MockCollection(docs))
        
        # Mock time as 10:00 AM ET (before 16:00 close)
        mock_now = datetime(2026, 3, 24, 10, 0, 0, tzinfo=_NY_TZ)
        
        with patch("services.market_calendar_service.datetime") as mock_dt:
            mock_dt.now.return_value = mock_now
            mock_dt.combine = datetime.combine
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            
            result = await last_n_completed_trading_days(db, 3)
            # Today (2026-03-24) should be excluded since market hasn't closed
            assert "2026-03-24" not in result

    @pytest.mark.asyncio
    async def test_returns_previous_days(self):
        """Should return previous completed trading days."""
        from services.market_calendar_service import last_n_completed_trading_days
        
        trading_dates = [
            "2026-03-16", "2026-03-17", "2026-03-18",
            "2026-03-19", "2026-03-20", "2026-03-23",
        ]
        docs = _build_calendar_docs(trading_dates)
        db = MockDB(market_calendar=MockCollection(docs))
        
        # Mock time as 10:00 AM ET on 2026-03-24 (not a trading day in our list)
        mock_now = datetime(2026, 3, 24, 10, 0, 0, tzinfo=_NY_TZ)
        
        with patch("services.market_calendar_service.datetime") as mock_dt:
            mock_dt.now.return_value = mock_now
            mock_dt.combine = datetime.combine
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            
            result = await last_n_completed_trading_days(db, 3)
            assert len(result) == 3
            assert result[0] == "2026-03-23"
            assert result[1] == "2026-03-20"
            assert result[2] == "2026-03-19"

    @pytest.mark.asyncio
    async def test_early_close_day_uses_stored_close_time(self):
        """On an early close day (e.g. 13:00), close time from calendar row is used."""
        from services.market_calendar_service import last_n_completed_trading_days
        
        # 2026-03-24 is an early close day at 13:00
        docs = [
            {"market": "US", "date": "2026-03-23", "is_trading_day": True,
             "trading_hours": {"open": "09:30", "close": "16:00"},
             "holiday_name": None, "early_close_time": None, "timezone": "America/New_York"},
            {"market": "US", "date": "2026-03-24", "is_trading_day": True,
             "trading_hours": {"open": "09:30", "close": "13:00"},
             "holiday_name": None, "early_close_time": "13:00", "timezone": "America/New_York"},
        ]
        db = MockDB(market_calendar=MockCollection(docs))
        
        # Mock time as 14:00 ET — after the early close of 13:00
        mock_now = datetime(2026, 3, 24, 14, 0, 0, tzinfo=_NY_TZ)
        
        with patch("services.market_calendar_service.datetime") as mock_dt:
            mock_dt.now.return_value = mock_now
            mock_dt.combine = datetime.combine
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            
            result = await last_n_completed_trading_days(db, 2)
            # Today's early close was 13:00, current time is 14:00 → today IS included
            assert "2026-03-24" in result

    @pytest.mark.asyncio
    async def test_early_close_day_excluded_before_close(self):
        """On an early close day, today excluded if before the early close time."""
        from services.market_calendar_service import last_n_completed_trading_days
        
        docs = [
            {"market": "US", "date": "2026-03-23", "is_trading_day": True,
             "trading_hours": {"open": "09:30", "close": "16:00"},
             "holiday_name": None, "early_close_time": None, "timezone": "America/New_York"},
            {"market": "US", "date": "2026-03-24", "is_trading_day": True,
             "trading_hours": {"open": "09:30", "close": "13:00"},
             "holiday_name": None, "early_close_time": "13:00", "timezone": "America/New_York"},
        ]
        db = MockDB(market_calendar=MockCollection(docs))
        
        # Mock time as 12:00 ET — before the early close of 13:00
        mock_now = datetime(2026, 3, 24, 12, 0, 0, tzinfo=_NY_TZ)
        
        with patch("services.market_calendar_service.datetime") as mock_dt:
            mock_dt.now.return_value = mock_now
            mock_dt.combine = datetime.combine
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            
            result = await last_n_completed_trading_days(db, 2)
            # Today's early close is 13:00, current time is 12:00 → today NOT included
            assert "2026-03-24" not in result


class TestMarketStatusNow:
    """Tests for market_status_now()."""

    @pytest.mark.asyncio
    async def test_closed_on_weekend(self):
        from services.market_calendar_service import market_status_now
        
        weekend_docs = [
            {"market": "US", "date": "2026-03-22", "is_trading_day": False,
             "trading_hours": None, "holiday_name": None, "early_close_time": None},
            {"market": "US", "date": "2026-03-23", "is_trading_day": True,
             "trading_hours": {"open": "09:30", "close": "16:00"},
             "holiday_name": None, "early_close_time": None},
        ]
        db = MockDB(market_calendar=MockCollection(weekend_docs))
        
        # Mock time as Sunday
        mock_now = datetime(2026, 3, 22, 12, 0, 0, tzinfo=_NY_TZ)
        
        with patch("services.market_calendar_service.datetime") as mock_dt:
            mock_dt.now.return_value = mock_now
            mock_dt.combine = datetime.combine
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            
            result = await market_status_now(db)
            assert result["state"] == "CLOSED"
            assert result["is_trading_day"] is False

    @pytest.mark.asyncio
    async def test_returns_timezone(self):
        from services.market_calendar_service import market_status_now
        
        docs = [{"market": "US", "date": "2026-03-24", "is_trading_day": True,
                 "trading_hours": {"open": "09:30", "close": "16:00"},
                 "holiday_name": None, "early_close_time": None}]
        db = MockDB(market_calendar=MockCollection(docs))
        
        mock_now = datetime(2026, 3, 24, 10, 0, 0, tzinfo=_NY_TZ)
        
        with patch("services.market_calendar_service.datetime") as mock_dt:
            mock_dt.now.return_value = mock_now
            mock_dt.combine = datetime.combine
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            
            result = await market_status_now(db)
            assert result["timezone"] == "America/New_York"


class TestCompletedTradingDaysHealth:
    """Tests for get_last_10_completed_trading_days_health()."""

    @pytest.mark.asyncio
    async def test_all_days_ok(self):
        from services.market_calendar_service import get_last_10_completed_trading_days_health
        
        trading_dates = [f"2026-03-{d:02d}" for d in range(9, 21) if date(2026, 3, d).weekday() < 5]
        cal_docs = _build_calendar_docs(trading_dates)
        
        # Mock ops_job_runs with successful price_sync for each date
        ops_docs = []
        for d in trading_dates:
            ops_docs.append({
                "job_name": "price_sync",
                "status": "success",
                "finished_at": datetime(2026, 3, 24, tzinfo=timezone.utc),
                "details": {
                    "price_bulk_gapfill": {
                        "days": [{"processed_date": d, "status": "success", "rows_written": 100}]
                    }
                }
            })
        
        db = MockDB(
            market_calendar=MockCollection(cal_docs),
            ops_job_runs=MockCollection(ops_docs),
        )
        
        mock_now = datetime(2026, 3, 24, 10, 0, 0, tzinfo=_NY_TZ)
        
        with patch("services.market_calendar_service.datetime") as mock_dt:
            mock_dt.now.return_value = mock_now
            mock_dt.combine = datetime.combine
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            
            result = await get_last_10_completed_trading_days_health(db)
            assert result["status"] == "green"
            assert result["missing_count"] == 0

    @pytest.mark.asyncio
    async def test_empty_calendar_returns_yellow(self):
        from services.market_calendar_service import get_last_10_completed_trading_days_health
        
        db = MockDB(
            market_calendar=MockCollection([]),
            ops_job_runs=MockCollection([]),
        )
        
        mock_now = datetime(2026, 3, 24, 10, 0, 0, tzinfo=_NY_TZ)
        
        with patch("services.market_calendar_service.datetime") as mock_dt:
            mock_dt.now.return_value = mock_now
            mock_dt.combine = datetime.combine
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            
            result = await get_last_10_completed_trading_days_health(db)
            assert result["status"] == "yellow"
            assert result["days"] == []

    @pytest.mark.asyncio
    async def test_missing_days_shows_red(self):
        from services.market_calendar_service import get_last_10_completed_trading_days_health
        
        # Create 10 trading days
        trading_dates = [
            "2026-03-09", "2026-03-10", "2026-03-11", "2026-03-12", "2026-03-13",
            "2026-03-16", "2026-03-17", "2026-03-18", "2026-03-19", "2026-03-20",
        ]
        cal_docs = _build_calendar_docs(trading_dates)
        
        # Only 5 days have price_sync data (missing 5)
        ops_docs = []
        for d in trading_dates[:5]:
            ops_docs.append({
                "job_name": "price_sync",
                "status": "success",
                "finished_at": datetime(2026, 3, 24, tzinfo=timezone.utc),
                "details": {
                    "price_bulk_gapfill": {
                        "days": [{"processed_date": d, "status": "success", "rows_written": 100}]
                    }
                }
            })
        
        db = MockDB(
            market_calendar=MockCollection(cal_docs),
            ops_job_runs=MockCollection(ops_docs),
        )
        
        mock_now = datetime(2026, 3, 24, 10, 0, 0, tzinfo=_NY_TZ)
        
        with patch("services.market_calendar_service.datetime") as mock_dt:
            mock_dt.now.return_value = mock_now
            mock_dt.combine = datetime.combine
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            
            result = await get_last_10_completed_trading_days_health(db)
            assert result["status"] == "red"
            assert result["missing_count"] == 5

    @pytest.mark.asyncio
    async def test_rows_written_zero_not_ok(self):
        """A day with status=success but rows_written=0 should NOT be considered OK."""
        from services.market_calendar_service import get_last_10_completed_trading_days_health
        
        trading_dates = ["2026-03-20"]
        cal_docs = _build_calendar_docs(trading_dates)
        
        ops_docs = [{
            "job_name": "price_sync",
            "status": "success",
            "finished_at": datetime(2026, 3, 24, tzinfo=timezone.utc),
            "details": {
                "price_bulk_gapfill": {
                    "days": [{"processed_date": "2026-03-20", "status": "success", "rows_written": 0}]
                }
            }
        }]
        
        db = MockDB(
            market_calendar=MockCollection(cal_docs),
            ops_job_runs=MockCollection(ops_docs),
        )
        
        mock_now = datetime(2026, 3, 24, 10, 0, 0, tzinfo=_NY_TZ)
        
        with patch("services.market_calendar_service.datetime") as mock_dt:
            mock_dt.now.return_value = mock_now
            mock_dt.combine = datetime.combine
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            
            result = await get_last_10_completed_trading_days_health(db)
            # rows_written=0 should NOT count as OK
            assert result["ok_count"] == 0
            assert result["missing_count"] == 1


class TestMarketOpenClosedNow:
    """Tests for market_open_closed_now().
    
    Semantics: is_open means the regular market session is active (REGULAR).
    PRE_MARKET and AFTER_HOURS are NOT considered "open".
    """

    @pytest.mark.asyncio
    async def test_returns_dict_shape(self):
        from services.market_calendar_service import market_open_closed_now
        
        docs = [{"market": "US", "date": "2026-03-24", "is_trading_day": True,
                 "trading_hours": {"open": "09:30", "close": "16:00"},
                 "holiday_name": None, "early_close_time": None}]
        db = MockDB(market_calendar=MockCollection(docs))
        
        mock_now = datetime(2026, 3, 24, 10, 0, 0, tzinfo=_NY_TZ)
        
        with patch("services.market_calendar_service.datetime") as mock_dt:
            mock_dt.now.return_value = mock_now
            mock_dt.combine = datetime.combine
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            
            result = await market_open_closed_now(db)
            assert "is_open" in result
            assert "state" in result
            assert "time_to_close_seconds" in result

    @pytest.mark.asyncio
    async def test_regular_session_is_open(self):
        """During regular hours (09:30-16:00), is_open should be True."""
        from services.market_calendar_service import market_open_closed_now
        
        docs = [{"market": "US", "date": "2026-03-24", "is_trading_day": True,
                 "trading_hours": {"open": "09:30", "close": "16:00"},
                 "holiday_name": None, "early_close_time": None}]
        db = MockDB(market_calendar=MockCollection(docs))
        
        mock_now = datetime(2026, 3, 24, 10, 0, 0, tzinfo=_NY_TZ)
        
        with patch("services.market_calendar_service.datetime") as mock_dt:
            mock_dt.now.return_value = mock_now
            mock_dt.combine = datetime.combine
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            
            result = await market_open_closed_now(db)
            assert result["is_open"] is True
            assert result["state"] == "REGULAR"

    @pytest.mark.asyncio
    async def test_pre_market_is_not_open(self):
        """During pre-market (04:00-09:30), is_open should be False."""
        from services.market_calendar_service import market_open_closed_now
        
        docs = [{"market": "US", "date": "2026-03-24", "is_trading_day": True,
                 "trading_hours": {"open": "09:30", "close": "16:00"},
                 "holiday_name": None, "early_close_time": None}]
        db = MockDB(market_calendar=MockCollection(docs))
        
        mock_now = datetime(2026, 3, 24, 5, 0, 0, tzinfo=_NY_TZ)
        
        with patch("services.market_calendar_service.datetime") as mock_dt:
            mock_dt.now.return_value = mock_now
            mock_dt.combine = datetime.combine
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            
            result = await market_open_closed_now(db)
            assert result["is_open"] is False
            assert result["state"] == "PRE_MARKET"

    @pytest.mark.asyncio
    async def test_after_hours_is_not_open(self):
        """During after-hours (16:00-20:00), is_open should be False."""
        from services.market_calendar_service import market_open_closed_now
        
        docs = [{"market": "US", "date": "2026-03-24", "is_trading_day": True,
                 "trading_hours": {"open": "09:30", "close": "16:00"},
                 "holiday_name": None, "early_close_time": None}]
        db = MockDB(market_calendar=MockCollection(docs))
        
        mock_now = datetime(2026, 3, 24, 17, 0, 0, tzinfo=_NY_TZ)
        
        with patch("services.market_calendar_service.datetime") as mock_dt:
            mock_dt.now.return_value = mock_now
            mock_dt.combine = datetime.combine
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            
            result = await market_open_closed_now(db)
            assert result["is_open"] is False
            assert result["state"] == "AFTER_HOURS"
