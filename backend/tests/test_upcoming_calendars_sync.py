import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import dividend_history_service as svc


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


class _TrackedTickersCollection:
    def __init__(self, tickers):
        self._tickers = list(tickers)

    async def distinct(self, field, query=None):
        return list(self._tickers)


class _UpcomingSplitsCollection:
    def __init__(self):
        self.docs = {}

    async def bulk_write(self, ops, ordered=False):
        for op in ops:
            payload = op._doc["$set"]
            self.docs[payload["ticker"]] = dict(payload)


class _UpcomingIposCollection:
    def __init__(self):
        self.deleted = None
        self.inserted = []

    async def delete_many(self, query):
        self.deleted = query

    async def insert_many(self, docs, ordered=False):
        self.inserted = [dict(doc) for doc in docs]


class _Db:
    def __init__(self, tracked_tickers=None):
        self.tracked_tickers = _TrackedTickersCollection(tracked_tickers or [])
        self.upcoming_splits = _UpcomingSplitsCollection()
        self.upcoming_ipos = _UpcomingIposCollection()


@pytest.mark.asyncio
async def test_sync_upcoming_splits_persists_ratio_from_alias_fields(monkeypatch):
    payload = {
        "splits": [
            {
                "code": "CUE.US",
                "date": "2026-04-24",
                "ratio": "1/30",
            }
        ]
    }
    db = _Db(tracked_tickers=["CUE.US"])

    monkeypatch.setattr(svc, "EODHD_API_KEY", "test-key")
    monkeypatch.setattr(svc.httpx, "AsyncClient", lambda *args, **kwargs: _FakeAsyncClient(payload))

    result = await svc.sync_upcoming_splits_calendar_for_visible_tickers(db)

    assert result["tickers_with_upcoming"] == 1
    assert db.upcoming_splits.docs["CUE.US"]["split_date"] == "2026-04-24"
    assert db.upcoming_splits.docs["CUE.US"]["split_ratio"] == "1:30"


@pytest.mark.asyncio
async def test_sync_upcoming_ipos_accepts_start_date_payload(monkeypatch):
    payload = {
        "ipos": [
            {
                "code": "RIKU.US",
                "name": "RIKU DINING GROUP Ltd",
                "exchange": "NASDAQ",
                "start_date": "2026-04-24",
                "filing_date": "2026-04-24",
                "amended_date": "2026-04-24",
                "price_from": 4,
                "price_to": 6,
                "offer_price": 0,
                "shares": 5000000,
                "deal_type": "Expected",
            }
        ]
    }
    db = _Db()

    monkeypatch.setattr(svc, "EODHD_API_KEY", "test-key")
    monkeypatch.setattr(svc.httpx, "AsyncClient", lambda *args, **kwargs: _FakeAsyncClient(payload))

    result = await svc.sync_upcoming_ipos_calendar(db)

    assert result["records_written"] == 1
    assert db.upcoming_ipos.deleted == {}
    assert len(db.upcoming_ipos.inserted) == 1
    assert db.upcoming_ipos.inserted[0]["ticker"] == "RIKU.US"
    assert db.upcoming_ipos.inserted[0]["ipo_date"] == "2026-04-24"
    assert db.upcoming_ipos.inserted[0]["price_from"] == 4
    assert db.upcoming_ipos.inserted[0]["price_to"] == 6
    assert db.upcoming_ipos.inserted[0]["offer_price"] == 0
    assert db.upcoming_ipos.inserted[0]["deal_type"] == "Expected"
