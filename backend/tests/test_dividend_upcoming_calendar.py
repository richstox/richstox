import os
import sys
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dividend_history_service import _detect_dividend_frequency, sync_upcoming_dividend_calendar_for_visible_tickers


class _FakeUpcomingCollection:
    def __init__(self):
        self.bulk_write = AsyncMock()
        self.create_index = AsyncMock()


class _FakeDb:
    def __init__(self, visible_tickers):
        self._visible_tickers = visible_tickers
        self.tracked_tickers = self
        self.upcoming_dividends = _FakeUpcomingCollection()

    async def distinct(self, field, query=None):
        assert field == "ticker"
        return self._visible_tickers


def _event(ex_date: str, amount: float, **kwargs):
    row = {"ex_date": ex_date, "amount": amount, "currency": "USD"}
    row.update(kwargs)
    return row


def test_frequency_prefers_provider_metadata():
    now = datetime.now(timezone.utc)
    events = [
        _event((now - timedelta(days=10)).strftime("%Y-%m-%d"), 0.25, period="Quarterly"),
        _event((now - timedelta(days=100)).strftime("%Y-%m-%d"), 0.24, period="Quarterly"),
        _event((now - timedelta(days=190)).strftime("%Y-%m-%d"), 0.23, period="Quarterly"),
    ]
    result = _detect_dividend_frequency(events)
    assert result["label"] == "Quarterly"
    assert result["source"] == "provider_metadata"


def test_frequency_infers_quarterly_when_metadata_missing():
    now = datetime.now(timezone.utc)
    events = [
        _event((now - timedelta(days=12)).strftime("%Y-%m-%d"), 0.25),
        _event((now - timedelta(days=103)).strftime("%Y-%m-%d"), 0.24),
        _event((now - timedelta(days=195)).strftime("%Y-%m-%d"), 0.24),
    ]
    result = _detect_dividend_frequency(events)
    assert result["label"] == "Quarterly"
    assert result["source"] == "inferred"


@pytest.mark.asyncio
async def test_upcoming_calendar_sync_writes_window_results_and_nulls():
    db = _FakeDb(["AAPL.US", "MSFT.US"])
    event_day = (datetime.now(timezone.utc) + timedelta(days=1)).strftime("%Y-%m-%d")

    with patch("dividend_history_service.EODHD_API_KEY", "test-key"):
        with patch("dividend_history_service.UPCOMING_DIVIDEND_WINDOW_DAYS", 1):
            with patch(
                "dividend_history_service._fetch_dividend_bulk_rows_for_day",
                side_effect=lambda _client, date_str: [
                    {
                        "code": "AAPL.US",
                        "exDate": event_day,
                        "paymentDate": (datetime.now(timezone.utc) + timedelta(days=21)).strftime("%Y-%m-%d"),
                        "dividend": "0.26",
                        "currency": "USD",
                    }
                ] if date_str == event_day else [],
            ):
                result = await sync_upcoming_dividend_calendar_for_visible_tickers(db)

    assert result["success"] is True
    assert result["status"] == "completed"
    assert result["coverage_complete"] is True
    assert result["tickers_with_upcoming"] == 1
    assert result["tickers_without_upcoming"] == 1
    assert len(result["days_fetched_ok"]) == 2
    assert result["days_failed"] == []
    assert db.upcoming_dividends.bulk_write.await_count == 1
    write_ops = db.upcoming_dividends.bulk_write.await_args.args[0]
    assert len(write_ops) == 2


@pytest.mark.asyncio
async def test_upcoming_calendar_sync_returns_incomplete_without_nulling_unknowns():
    db = _FakeDb(["AAPL.US", "MSFT.US"])
    event_day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    failed_day = (datetime.now(timezone.utc) + timedelta(days=1)).strftime("%Y-%m-%d")

    async def _fake_fetch(_client, date_str):
        if date_str == event_day:
            return [{
                "code": "AAPL.US",
                "exDate": event_day,
                "paymentDate": (datetime.now(timezone.utc) + timedelta(days=21)).strftime("%Y-%m-%d"),
                "dividend": "0.26",
                "currency": "USD",
            }]
        raise RuntimeError(f"GET /eod-bulk-last-day/US?type=dividends&date={date_str} returned HTTP 500")

    with patch("dividend_history_service.EODHD_API_KEY", "test-key"):
        with patch("dividend_history_service.UPCOMING_DIVIDEND_WINDOW_DAYS", 1):
            with patch("dividend_history_service._fetch_dividend_bulk_rows_for_day", side_effect=_fake_fetch):
                result = await sync_upcoming_dividend_calendar_for_visible_tickers(db)

    assert result["success"] is False
    assert result["status"] == "incomplete"
    assert result["coverage_complete"] is False
    assert result["tickers_with_upcoming"] == 1
    assert result["tickers_without_upcoming"] == 0
    assert result["days_fetched_ok"] == [event_day]
    assert result["days_failed"] == [failed_day]
    assert result["failed_day_errors"][0]["date"] == failed_day
    assert db.upcoming_dividends.bulk_write.await_count == 1
    write_ops = db.upcoming_dividends.bulk_write.await_args.args[0]
    assert len(write_ops) == 1


@pytest.mark.asyncio
async def test_upcoming_calendar_sync_raises_when_all_days_fail():
    db = _FakeDb(["AAPL.US"])

    async def _always_fail(_client, date_str):
        raise RuntimeError(f"GET /eod-bulk-last-day/US?type=dividends&date={date_str} returned HTTP 404")

    with patch("dividend_history_service.EODHD_API_KEY", "test-key"):
        with patch("dividend_history_service.UPCOMING_DIVIDEND_WINDOW_DAYS", 0):
            with patch("dividend_history_service._fetch_dividend_bulk_rows_for_day", side_effect=_always_fail):
                with pytest.raises(RuntimeError, match="unable to fetch any day in the requested window"):
                    await sync_upcoming_dividend_calendar_for_visible_tickers(db)
