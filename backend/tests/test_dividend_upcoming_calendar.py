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


class _FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


class _FakeAsyncClient:
    def __init__(self, payload):
        self._payload = payload

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def get(self, url, params=None):
        return _FakeResponse(self._payload)


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
    payload = {
        "dividends": [
            {
                "code": "AAPL.US",
                "exDate": (datetime.now(timezone.utc) + timedelta(days=7)).strftime("%Y-%m-%d"),
                "paymentDate": (datetime.now(timezone.utc) + timedelta(days=21)).strftime("%Y-%m-%d"),
                "dividend": "0.26",
                "currency": "USD",
            }
        ]
    }

    with patch("dividend_history_service.EODHD_API_KEY", "test-key"):
        with patch("dividend_history_service.httpx.AsyncClient", return_value=_FakeAsyncClient(payload)):
            result = await sync_upcoming_dividend_calendar_for_visible_tickers(db)

    assert result["success"] is True
    assert result["tickers_with_upcoming"] == 1
    assert result["tickers_without_upcoming"] == 1
    assert db.upcoming_dividends.bulk_write.await_count == 1
    write_ops = db.upcoming_dividends.bulk_write.await_args.args[0]
    assert len(write_ops) == 2
