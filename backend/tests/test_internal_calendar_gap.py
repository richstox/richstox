"""
Tests for internal calendar-gap detection (PR16).

Verifies that the history completeness verifier detects internal gaps
where a trading day (per market calendar) is present between two
existing stock_prices rows but has no DB row itself.

Key scenarios:
  - CEV.US: 2026-04-14 and 2026-04-16 present, 2026-04-15 missing
  - Boundary-only range-proof must not mark complete if internal gap exists
  - Sweep path applies same logic and flags needs_price_redownload=True
  - After repair (backfill), ticker can become complete again
"""

import asyncio
import sys
import os
from datetime import datetime, timezone
from typing import List
from unittest.mock import AsyncMock, MagicMock

# Backend must be on the path so imports resolve
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from services.history_completeness_service import (
    ALLOWED_STATUSES,
    verify_ticker_history_completeness,
    persist_ticker_completeness,
    run_history_completeness_sweep,
    _check_internal_calendar_gaps,
    _get_calendar_trading_dates,
    _INTERNAL_GAP_RECENT_WINDOW,
    _MAX_MISSING_SAMPLE,
    _result,
)


# ── Helpers ────────────────────────────────────────────────────────────────────

class _Cursor:
    """Minimal async cursor mock that supports find/aggregate/sort/limit."""
    def __init__(self, docs):
        self._docs = list(docs)

    def sort(self, *a, **kw):
        return self

    def limit(self, n):
        self._docs = self._docs[:n]
        return self

    async def to_list(self, length=None):
        if length is not None:
            return self._docs[:length]
        return self._docs

    def __aiter__(self):
        return _CursorIter(self._docs)


class _CursorIter:
    def __init__(self, docs):
        self._iter = iter(docs)

    async def __anext__(self):
        try:
            return next(self._iter)
        except StopIteration:
            raise StopAsyncIteration


def _make_db(
    *,
    tracked_ticker_doc=None,
    price_dates=None,
    first_last=None,
    exclusions=None,
    bulk_processed_dates=None,
    visible_docs=None,
    calendar_trading_dates=None,
):
    """Build a mock db with collections needed by the verifier.

    ``calendar_trading_dates`` — list of date strings that are trading
    days in the market_calendar (used by internal gap check).
    """
    db = MagicMock()

    # tracked_tickers.find_one
    db.tracked_tickers.find_one = AsyncMock(return_value=tracked_ticker_doc)

    # tracked_tickers.find (for sweep)
    _vis = visible_docs or []
    db.tracked_tickers.find.return_value = _Cursor(_vis)

    # tracked_tickers.update_one / bulk_write
    db.tracked_tickers.update_one = AsyncMock()
    db.tracked_tickers.bulk_write = AsyncMock()

    # stock_prices.aggregate — returns different results depending on pipeline
    _price_dates = price_dates or []
    _first_last = first_last

    def _agg(pipeline):
        for stage in pipeline:
            if "$group" in stage:
                group = stage["$group"]
                if "dates" in group:
                    # Check if it's a per-ticker or single-ticker aggregation
                    if isinstance(_price_dates, list) and _price_dates and isinstance(_price_dates[0], dict):
                        return _Cursor(_price_dates)
                    return _Cursor([{"_id": None, "dates": _price_dates}] if _price_dates else [])
                if "first_date" in group:
                    if isinstance(_first_last, list):
                        return _Cursor(_first_last)
                    if _first_last:
                        return _Cursor([{"_id": None, **_first_last}])
                    return _Cursor([])
        return _Cursor([])

    db.stock_prices.aggregate = _agg

    # gap_free_exclusions.find
    _excls = exclusions or []
    db.gap_free_exclusions.find.return_value = _Cursor(_excls)

    # ops_job_runs (for _get_bulk_processed_dates)
    db.ops_job_runs.aggregate.return_value = _Cursor([])
    db.ops_job_runs.insert_one = AsyncMock()

    # market_calendar.find — supports the internal gap check query
    _cal_dates = calendar_trading_dates or []
    _cal_docs = [{"date": d} for d in _cal_dates]

    def _mc_find(query, projection=None):
        # Filter by date range if present
        docs = list(_cal_docs)
        if query and "date" in query:
            date_filter = query["date"]
            if isinstance(date_filter, dict):
                gte = date_filter.get("$gte")
                lte = date_filter.get("$lte")
                if "$in" in date_filter:
                    in_set = set(date_filter["$in"])
                    docs = [d for d in docs if d["date"] in in_set]
                else:
                    if gte:
                        docs = [d for d in docs if d["date"] >= gte]
                    if lte:
                        docs = [d for d in docs if d["date"] <= lte]
        return _Cursor(docs)

    db.market_calendar = MagicMock()
    db.market_calendar.find = _mc_find

    return db


# ── Test: missing_trading_days in ALLOWED_STATUSES ────────────────────────────

class TestAllowedStatuses:
    def test_missing_trading_days_present(self):
        assert "missing_trading_days" in ALLOWED_STATUSES


# ── Test: _get_calendar_trading_dates ─────────────────────────────────────────

class TestGetCalendarTradingDates:
    def test_returns_trading_dates_in_range(self):
        db = _make_db(calendar_trading_dates=[
            "2026-04-14", "2026-04-15", "2026-04-16", "2026-04-17",
        ])
        result = asyncio.get_event_loop().run_until_complete(
            _get_calendar_trading_dates(db, "2026-04-15", "2026-04-16")
        )
        assert result == ["2026-04-15", "2026-04-16"]

    def test_empty_when_no_calendar(self):
        db = _make_db(calendar_trading_dates=[])
        result = asyncio.get_event_loop().run_until_complete(
            _get_calendar_trading_dates(db, "2026-04-14", "2026-04-17")
        )
        assert result == []


# ── Test: _check_internal_calendar_gaps ──────────────────────────────────────

class TestCheckInternalCalendarGaps:
    def test_gap_detected_when_trading_day_missing(self):
        """2026-04-14 and 2026-04-16 present, 2026-04-15 is a trading day but missing."""
        # Calendar says 15, 16, 17 are trading days
        # stock_prices has 14, 16, 17 (15 missing)
        # anchor is 2026-04-14
        db = _make_db(
            price_dates=["2026-04-16", "2026-04-17"],
            calendar_trading_dates=["2026-04-15", "2026-04-16", "2026-04-17"],
        )
        # Override aggregate for the internal gap check's query
        def _gap_agg(pipeline):
            for stage in pipeline:
                if "$match" in stage:
                    match = stage["$match"]
                    if "date" in match and isinstance(match["date"], dict):
                        # Calendar gap check query — return dates that exist
                        in_dates = match["date"].get("$in", [])
                        existing = {"2026-04-16", "2026-04-17"}
                        found = [d for d in in_dates if d in existing]
                        return _Cursor([{"_id": None, "dates": found}] if found else [])
            return _Cursor([])
        db.stock_prices.aggregate = _gap_agg

        result = asyncio.get_event_loop().run_until_complete(
            _check_internal_calendar_gaps(db, "CEV.US", "2026-04-14", "2026-04-17")
        )
        assert result == ["2026-04-15"]

    def test_no_gap_when_all_trading_days_present(self):
        """All calendar trading days have DB rows → no gap."""
        db = _make_db(
            calendar_trading_dates=["2026-04-15", "2026-04-16", "2026-04-17"],
        )
        def _gap_agg(pipeline):
            for stage in pipeline:
                if "$match" in stage:
                    return _Cursor([{"_id": None, "dates": ["2026-04-15", "2026-04-16", "2026-04-17"]}])
            return _Cursor([])
        db.stock_prices.aggregate = _gap_agg

        result = asyncio.get_event_loop().run_until_complete(
            _check_internal_calendar_gaps(db, "CEV.US", "2026-04-14", "2026-04-17")
        )
        assert result == []

    def test_no_gap_when_last_date_before_anchor(self):
        """last_date <= anchor → nothing to check."""
        db = _make_db()
        result = asyncio.get_event_loop().run_until_complete(
            _check_internal_calendar_gaps(db, "CEV.US", "2026-04-17", "2026-04-14")
        )
        assert result == []

    def test_bounded_to_recent_window(self):
        """When calendar dates exceed the window, only the most recent N are checked."""
        # Generate more than _INTERNAL_GAP_RECENT_WINDOW trading dates
        from datetime import date, timedelta
        all_dates = []
        d = date(2025, 1, 2)
        while len(all_dates) < _INTERNAL_GAP_RECENT_WINDOW + 10:
            if d.weekday() < 5:  # Mon-Fri
                all_dates.append(d.isoformat())
            d += timedelta(days=1)

        db = _make_db(calendar_trading_dates=all_dates)
        # All dates present in stock_prices
        def _gap_agg(pipeline):
            for stage in pipeline:
                if "$match" in stage:
                    match = stage["$match"]
                    if "date" in match and isinstance(match["date"], dict):
                        in_dates = match["date"].get("$in", [])
                        return _Cursor([{"_id": None, "dates": in_dates}] if in_dates else [])
            return _Cursor([])
        db.stock_prices.aggregate = _gap_agg

        result = asyncio.get_event_loop().run_until_complete(
            _check_internal_calendar_gaps(db, "TEST.US", "2025-01-01", all_dates[-1])
        )
        assert result == []


# ── Test: verify_ticker_history_completeness with internal gaps ───────────────

class TestVerifyWithInternalGap:
    def test_cev_us_scenario_14_and_16_present_15_missing(self):
        """CEV.US: 2026-04-14 and 2026-04-16 exist, 2026-04-15 missing.

        Bulk-based check passes (expected_dates only has 16, 17 as
        post-anchor dates and both are present). But 2026-04-15 is a
        calendar trading day with no DB row → missing_trading_days.
        """
        # Bulk expected dates after anchor (2026-04-14)
        expected = ["2026-04-16", "2026-04-17"]

        # Calendar trading days: 15, 16, 17 are all trading days
        cal_dates = ["2026-04-15", "2026-04-16", "2026-04-17"]

        db = _make_db(
            tracked_ticker_doc={
                "history_download_proven_at": datetime(2026, 4, 15, 1, 7, 3, tzinfo=timezone.utc),
                "history_download_proven_anchor": "2026-04-14",
            },
            # Bulk dates 16 and 17 are present in stock_prices
            price_dates=["2026-04-16", "2026-04-17"],
            first_last={"first_date": "1999-01-27", "last_date": "2026-04-17"},
            calendar_trading_dates=cal_dates,
        )

        # Override aggregate to handle BOTH the bulk range-proof query
        # AND the internal gap query correctly
        def _agg(pipeline):
            for stage in pipeline:
                if "$group" in stage:
                    group = stage["$group"]
                    if "dates" in group:
                        # Check what dates are being queried
                        match_stage = pipeline[0].get("$match", {})
                        queried_dates = match_stage.get("date", {})
                        if isinstance(queried_dates, dict) and "$in" in queried_dates:
                            in_dates = set(queried_dates["$in"])
                            # stock_prices has: 2026-04-14, 2026-04-16, 2026-04-17
                            existing = {"2026-04-14", "2026-04-16", "2026-04-17"}
                            found = sorted(in_dates & existing)
                            return _Cursor([{"_id": None, "dates": found}] if found else [])
                        return _Cursor([{"_id": None, "dates": ["2026-04-16", "2026-04-17"]}])
                    if "first_date" in group:
                        return _Cursor([{"_id": None, "first_date": "1999-01-27", "last_date": "2026-04-17"}])
            return _Cursor([])
        db.stock_prices.aggregate = _agg

        result = asyncio.get_event_loop().run_until_complete(
            verify_ticker_history_completeness(db, "CEV.US", expected_dates=expected)
        )

        assert result["price_history_complete"] is False
        assert result["price_history_status"] == "missing_trading_days"
        assert result["price_history_missing_days_count"] >= 1
        assert "2026-04-15" in result["price_history_missing_days"]
        assert "2026-04-15" in result["calendar_gap_missing_sample"]

    def test_boundary_only_range_proof_fails_with_internal_gap(self):
        """Boundary-only range-proof (first/last exist) must NOT mark
        complete if there is an internal gap on a calendar trading day.

        anchor=2026-04-11, last=2026-04-17.
        Bulk dates: 14, 15, 16, 17 — all present in stock_prices.
        Calendar days: 14, 15, 16, 17 — but 15 is NOT in stock_prices.
        (This simulates the case where bulk says 15 is present but DB
        doesn't actually have the row.)
        """
        expected = ["2026-04-14", "2026-04-16", "2026-04-17"]
        cal_dates = ["2026-04-14", "2026-04-15", "2026-04-16", "2026-04-17"]

        db = _make_db(
            tracked_ticker_doc={
                "history_download_proven_at": datetime(2026, 4, 12, tzinfo=timezone.utc),
                "history_download_proven_anchor": "2026-04-11",
            },
            # Bulk expected has 14, 16, 17 and all are in stock_prices
            price_dates=["2026-04-14", "2026-04-16", "2026-04-17"],
            first_last={"first_date": "2020-01-02", "last_date": "2026-04-17"},
            calendar_trading_dates=cal_dates,
        )

        def _agg(pipeline):
            for stage in pipeline:
                if "$group" in stage:
                    group = stage["$group"]
                    if "dates" in group:
                        match_stage = pipeline[0].get("$match", {})
                        queried_dates = match_stage.get("date", {})
                        if isinstance(queried_dates, dict) and "$in" in queried_dates:
                            in_dates = set(queried_dates["$in"])
                            existing = {"2026-04-14", "2026-04-16", "2026-04-17"}
                            found = sorted(in_dates & existing)
                            return _Cursor([{"_id": None, "dates": found}] if found else [])
                        return _Cursor([{"_id": None, "dates": ["2026-04-14", "2026-04-16", "2026-04-17"]}])
                    if "first_date" in group:
                        return _Cursor([{"_id": None, "first_date": "2020-01-02", "last_date": "2026-04-17"}])
            return _Cursor([])
        db.stock_prices.aggregate = _agg

        result = asyncio.get_event_loop().run_until_complete(
            verify_ticker_history_completeness(db, "TEST.US", expected_dates=expected)
        )

        assert result["price_history_complete"] is False
        assert result["price_history_status"] == "missing_trading_days"
        assert "2026-04-15" in result["price_history_missing_days"]

    def test_complete_when_all_calendar_days_present(self):
        """When all calendar trading days have DB rows, result is complete."""
        expected = ["2026-04-15", "2026-04-16", "2026-04-17"]
        cal_dates = ["2026-04-15", "2026-04-16", "2026-04-17"]

        db = _make_db(
            tracked_ticker_doc={
                "history_download_proven_at": datetime(2026, 4, 15, tzinfo=timezone.utc),
                "history_download_proven_anchor": "2026-04-14",
            },
            price_dates=["2026-04-15", "2026-04-16", "2026-04-17"],
            first_last={"first_date": "2020-01-02", "last_date": "2026-04-17"},
            calendar_trading_dates=cal_dates,
        )

        def _agg(pipeline):
            for stage in pipeline:
                if "$group" in stage:
                    group = stage["$group"]
                    if "dates" in group:
                        match_stage = pipeline[0].get("$match", {})
                        queried_dates = match_stage.get("date", {})
                        if isinstance(queried_dates, dict) and "$in" in queried_dates:
                            in_dates = set(queried_dates["$in"])
                            existing = {"2026-04-15", "2026-04-16", "2026-04-17"}
                            found = sorted(in_dates & existing)
                            return _Cursor([{"_id": None, "dates": found}] if found else [])
                        return _Cursor([{"_id": None, "dates": ["2026-04-15", "2026-04-16", "2026-04-17"]}])
                    if "first_date" in group:
                        return _Cursor([{"_id": None, "first_date": "2020-01-02", "last_date": "2026-04-17"}])
            return _Cursor([])
        db.stock_prices.aggregate = _agg

        result = asyncio.get_event_loop().run_until_complete(
            verify_ticker_history_completeness(db, "TEST.US", expected_dates=expected)
        )

        assert result["price_history_complete"] is True
        assert result["price_history_status"] == "complete"

    def test_after_repair_becomes_complete(self):
        """After backfill of the missing day, ticker becomes complete again."""
        expected = ["2026-04-16", "2026-04-17"]
        cal_dates = ["2026-04-15", "2026-04-16", "2026-04-17"]

        db = _make_db(
            tracked_ticker_doc={
                "history_download_proven_at": datetime(2026, 4, 15, tzinfo=timezone.utc),
                "history_download_proven_anchor": "2026-04-14",
            },
            # After repair: all dates including 2026-04-15 now present
            price_dates=["2026-04-16", "2026-04-17"],
            first_last={"first_date": "1999-01-27", "last_date": "2026-04-17"},
            calendar_trading_dates=cal_dates,
        )

        def _agg(pipeline):
            for stage in pipeline:
                if "$group" in stage:
                    group = stage["$group"]
                    if "dates" in group:
                        match_stage = pipeline[0].get("$match", {})
                        queried_dates = match_stage.get("date", {})
                        if isinstance(queried_dates, dict) and "$in" in queried_dates:
                            in_dates = set(queried_dates["$in"])
                            # NOW all three dates exist
                            existing = {"2026-04-15", "2026-04-16", "2026-04-17"}
                            found = sorted(in_dates & existing)
                            return _Cursor([{"_id": None, "dates": found}] if found else [])
                        return _Cursor([{"_id": None, "dates": ["2026-04-16", "2026-04-17"]}])
                    if "first_date" in group:
                        return _Cursor([{"_id": None, "first_date": "1999-01-27", "last_date": "2026-04-17"}])
            return _Cursor([])
        db.stock_prices.aggregate = _agg

        result = asyncio.get_event_loop().run_until_complete(
            verify_ticker_history_completeness(db, "CEV.US", expected_dates=expected)
        )

        assert result["price_history_complete"] is True
        assert result["price_history_status"] == "complete"


# ── Test: persist_ticker_completeness with calendar gaps ─────────────────────

class TestPersistWithCalendarGap:
    def test_persist_sets_redownload_for_missing_trading_days(self):
        """persist_ticker_completeness sets needs_price_redownload=True
        when status is missing_trading_days."""
        db = _make_db()
        now = datetime.now(timezone.utc)
        result = _result(
            status="missing_trading_days",
            complete=False,
            first_date="1999-01-27",
            last_date="2026-04-17",
            missing_days_count=1,
            missing_days=["2026-04-15"],
            verified_at=now,
        )
        result["calendar_gap_missing_sample"] = ["2026-04-15"]

        asyncio.get_event_loop().run_until_complete(
            persist_ticker_completeness(db, "CEV.US", result)
        )
        db.tracked_tickers.update_one.assert_called_once()
        call_args = db.tracked_tickers.update_one.call_args
        set_fields = call_args[0][1]["$set"]
        assert set_fields["price_history_complete"] is False
        assert set_fields["price_history_status"] == "missing_trading_days"
        assert set_fields["needs_price_redownload"] is True
        assert set_fields["price_history_missing_days_sample"] == ["2026-04-15"]


# ── Test: Sweep with internal calendar gap ───────────────────────────────────

class TestSweepWithCalendarGap:
    def test_sweep_detects_internal_gap_and_flags_redownload(self):
        """Sweep path: CEV.US missing 2026-04-15 → missing_trading_days,
        needs_price_redownload=True, missing_days_sample persisted."""
        now = datetime.now(timezone.utc)
        visible = [
            {
                "ticker": "CEV.US",
                "history_download_proven_at": now,
                "history_download_proven_anchor": "2026-04-14",
            },
        ]
        # Bulk expected dates after anchor — 15 was NOT bulk-processed
        expected = ["2026-04-16", "2026-04-17"]
        # Calendar says 15 is a trading day
        cal_dates = ["2026-04-15", "2026-04-16", "2026-04-17"]

        db = _make_db(
            visible_docs=visible,
            calendar_trading_dates=cal_dates,
        )

        import services.history_completeness_service as svc
        original = svc._get_bulk_processed_dates

        async def _mock_bpd(db):
            return expected
        svc._get_bulk_processed_dates = _mock_bpd

        def _mock_agg(pipeline):
            for stage in pipeline:
                if "$group" in stage:
                    group = stage["$group"]
                    if "dates" in group:
                        match_stage = pipeline[0].get("$match", {})
                        queried_dates = match_stage.get("date", {})
                        if isinstance(queried_dates, dict) and "$in" in queried_dates:
                            in_dates = set(queried_dates["$in"])
                            existing = {"2026-04-14", "2026-04-16", "2026-04-17"}
                            found = sorted(in_dates & existing)
                            return _Cursor([{"_id": "CEV.US", "dates": found}] if found else [])
                        return _Cursor([{"_id": "CEV.US", "dates": ["2026-04-16", "2026-04-17"]}])
                    if "first_date" in group:
                        return _Cursor([{
                            "_id": "CEV.US",
                            "first_date": "1999-01-27",
                            "last_date": "2026-04-17",
                        }])
            return _Cursor([])
        db.stock_prices.aggregate = _mock_agg

        try:
            result = asyncio.get_event_loop().run_until_complete(
                run_history_completeness_sweep(db)
            )
            assert result["status"] == "success"
            assert result["total"] == 1
            assert result["complete"] == 0
            assert result["incomplete"] == 1
            assert result["calendar_gap_redownload"] == 1

            # Verify bulk_write was called with correct fields
            db.tracked_tickers.bulk_write.assert_called_once()
            call_args = db.tracked_tickers.bulk_write.call_args
            ops = call_args[0][0]
            assert len(ops) == 1
            update_doc = ops[0]._doc["$set"]
            assert update_doc["price_history_complete"] is False
            assert update_doc["price_history_status"] == "missing_trading_days"
            assert update_doc["needs_price_redownload"] is True
            assert "2026-04-15" in update_doc.get("price_history_missing_days_sample", [])
        finally:
            svc._get_bulk_processed_dates = original

    def test_sweep_complete_when_no_calendar_gaps(self):
        """Sweep path: all calendar trading days present → complete."""
        now = datetime.now(timezone.utc)
        visible = [
            {
                "ticker": "GOOD.US",
                "history_download_proven_at": now,
                "history_download_proven_anchor": "2026-04-14",
            },
        ]
        expected = ["2026-04-15", "2026-04-16", "2026-04-17"]
        cal_dates = ["2026-04-15", "2026-04-16", "2026-04-17"]

        db = _make_db(
            visible_docs=visible,
            calendar_trading_dates=cal_dates,
        )

        import services.history_completeness_service as svc
        original = svc._get_bulk_processed_dates

        async def _mock_bpd(db):
            return expected
        svc._get_bulk_processed_dates = _mock_bpd

        def _mock_agg(pipeline):
            for stage in pipeline:
                if "$group" in stage:
                    group = stage["$group"]
                    if "dates" in group:
                        match_stage = pipeline[0].get("$match", {})
                        queried_dates = match_stage.get("date", {})
                        if isinstance(queried_dates, dict) and "$in" in queried_dates:
                            in_dates = set(queried_dates["$in"])
                            existing = {"2026-04-15", "2026-04-16", "2026-04-17"}
                            found = sorted(in_dates & existing)
                            return _Cursor([{"_id": "GOOD.US", "dates": found}] if found else [])
                        return _Cursor([{"_id": "GOOD.US", "dates": ["2026-04-15", "2026-04-16", "2026-04-17"]}])
                    if "first_date" in group:
                        return _Cursor([{
                            "_id": "GOOD.US",
                            "first_date": "2020-01-02",
                            "last_date": "2026-04-17",
                        }])
            return _Cursor([])
        db.stock_prices.aggregate = _mock_agg

        try:
            result = asyncio.get_event_loop().run_until_complete(
                run_history_completeness_sweep(db)
            )
            assert result["status"] == "success"
            assert result["complete"] == 1
            assert result["incomplete"] == 0
            assert result["calendar_gap_redownload"] == 0

            # Verify bulk_write fields
            db.tracked_tickers.bulk_write.assert_called_once()
            call_args = db.tracked_tickers.bulk_write.call_args
            ops = call_args[0][0]
            update_doc = ops[0]._doc["$set"]
            assert update_doc["price_history_complete"] is True
            assert update_doc["price_history_status"] == "complete"
            assert "needs_price_redownload" not in update_doc
        finally:
            svc._get_bulk_processed_dates = original
