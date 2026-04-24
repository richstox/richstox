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
        self._tickers = []
        for item in tickers:
            if isinstance(item, dict):
                self._tickers.append(dict(item))
            else:
                self._tickers.append({"ticker": item})

    async def distinct(self, field, query=None):
        docs = list(self._tickers)
        if query:
            docs = [doc for doc in docs if _matches_query(doc, query)]
        return [doc.get(field) for doc in docs if doc.get(field) is not None]


def _matches_query(doc, query):
    for key, expected in query.items():
        value = doc.get(key)
        if isinstance(expected, dict) and "$in" in expected:
            if value not in expected["$in"]:
                return False
        elif value != expected:
            return False
    return True


class _Cursor:
    def __init__(self, docs):
        self._docs = list(docs)

    async def to_list(self, length=None):
        if length is None:
            return list(self._docs)
        return list(self._docs[:length])


class _UpcomingSplitsCollection:
    def __init__(self, docs=None):
        self.docs = [dict(doc) for doc in (docs or [])]
        self.deleted = []

    async def bulk_write(self, ops, ordered=False):
        for op in ops:
            flt = dict(op._filter)
            payload = op._doc["$set"]
            idx = next(
                (
                    index for index, existing in enumerate(self.docs)
                    if all(existing.get(key) == value for key, value in flt.items())
                ),
                None,
            )
            if idx is None:
                self.docs.append(dict(payload))
            else:
                self.docs[idx] = dict(payload)

    def find(self, query, projection=None):
        docs = list(self.docs)
        if query:
            if "_id" in query and isinstance(query["_id"], dict) and "$in" in query["_id"]:
                allowed_ids = set(query["_id"]["$in"])
                docs = [doc for doc in docs if doc.get("_id") in allowed_ids]
        if projection:
            projected = []
            include_keys = {key for key, enabled in projection.items() if enabled}
            for doc in docs:
                projected.append({key: value for key, value in doc.items() if key in include_keys})
            docs = projected
        return _Cursor(docs)

    async def delete_many(self, query):
        if not query:
            self.deleted.extend(self.docs)
            self.docs = []
            return
        allowed_ids = set(query.get("_id", {}).get("$in", []))
        kept = []
        for doc in self.docs:
            if doc.get("_id") in allowed_ids:
                self.deleted.append(dict(doc))
            else:
                kept.append(doc)
        self.docs = kept

    async def create_index(self, *args, **kwargs):
        return None

    async def index_information(self):
        return {"upcoming_splits_ticker_unique": {"key": [("ticker", 1)], "unique": True}}

    async def drop_index(self, name):
        return None

    def get_docs_for_ticker(self, ticker):
        return [doc for doc in self.docs if doc.get("ticker") == ticker]


class _UpcomingIposCollection:
    def __init__(self):
        self.deleted = None
        self.inserted = []

    async def delete_many(self, query):
        self.deleted = query

    async def insert_many(self, docs, ordered=False):
        self.inserted = [dict(doc) for doc in docs]


class _Db:
    def __init__(self, tracked_tickers=None, upcoming_splits=None):
        self.tracked_tickers = _TrackedTickersCollection(tracked_tickers or [])
        self.upcoming_splits = _UpcomingSplitsCollection(upcoming_splits)
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
    db = _Db(tracked_tickers=[
        {
            "ticker": "CUE.US",
            "is_visible": True,
            "exchange": "NASDAQ",
            "asset_type": "Common Stock",
        }
    ])

    monkeypatch.setattr(svc, "EODHD_API_KEY", "test-key")
    monkeypatch.setattr(svc.httpx, "AsyncClient", lambda *args, **kwargs: _FakeAsyncClient(payload))

    result = await svc.sync_upcoming_splits_calendar_for_visible_tickers(db)

    assert result["status"] == "success"
    assert result["tickers_updated"] == 1
    assert db.upcoming_splits.get_docs_for_ticker("CUE.US")[0]["split_date"] == "2026-04-24"
    assert db.upcoming_splits.get_docs_for_ticker("CUE.US")[0]["split_ratio"] == "1:30"


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


@pytest.mark.asyncio
async def test_sync_upcoming_splits_accepts_nested_data_and_splitratio_alias(monkeypatch):
    payload = {
        "data": {
            "splits": [
                {
                    "Symbol": "CUE",
                    "Date": "2026-04-24",
                    "SplitRatio": "3:2",
                }
            ]
        }
    }
    db = _Db(tracked_tickers=[
        {
            "ticker": "CUE.US",
            "is_visible": True,
            "exchange": "NASDAQ",
            "asset_type": "Common Stock",
        }
    ])

    monkeypatch.setattr(svc, "EODHD_API_KEY", "test-key")
    monkeypatch.setattr(svc.httpx, "AsyncClient", lambda *args, **kwargs: _FakeAsyncClient(payload))

    result = await svc.sync_upcoming_splits_calendar_for_visible_tickers(db)

    assert result["tickers_updated"] == 1
    assert db.upcoming_splits.get_docs_for_ticker("CUE.US")[0]["split_ratio"] == "3:2"
    assert db.upcoming_splits.get_docs_for_ticker("CUE.US")[0]["old_shares"] == 3.0
    assert db.upcoming_splits.get_docs_for_ticker("CUE.US")[0]["new_shares"] == 2.0


@pytest.mark.asyncio
async def test_sync_upcoming_splits_filters_universe_and_invalid_rows_and_cleans_existing(monkeypatch):
    payload = {
        "splits": [
            {
                "code": "CUE.US",
                "date": "2026-04-24",
                "old_shares": 1,
                "new_shares": 30,
            },
            {
                "code": "ACNT.US",
                "date": None,
                "old_shares": 1,
                "new_shares": 10,
            },
            {
                "code": "3321.HK",
                "date": "2026-04-25",
                "old_shares": 2,
                "new_shares": 1,
            },
            {
                "code": "ETF.US",
                "date": "2026-04-25",
                "old_shares": 2,
                "new_shares": 1,
            },
        ]
    }
    db = _Db(
        tracked_tickers=[
            {
                "ticker": "CUE.US",
                "is_visible": True,
                "exchange": "NASDAQ",
                "asset_type": "Common Stock",
            },
            {
                "ticker": "ACNT.US",
                "is_visible": True,
                "exchange": "NYSE",
                "asset_type": "Common Stock",
            },
            {
                "ticker": "ETF.US",
                "is_visible": True,
                "exchange": "NYSE",
                "asset_type": "ETF",
            },
        ],
        upcoming_splits=[
            {
                "_id": 1,
                "ticker": "ACNT.US",
                "split_date": None,
                "old_shares": None,
                "new_shares": None,
                "source": svc.UPCOMING_SPLITS_SOURCE,
            },
            {
                "_id": 2,
                "ticker": "3321.HK",
                "split_date": "2026-04-25",
                "old_shares": 2,
                "new_shares": 1,
                "source": svc.UPCOMING_SPLITS_SOURCE,
            },
        ],
    )

    monkeypatch.setattr(svc, "EODHD_API_KEY", "test-key")
    monkeypatch.setattr(
        svc.httpx,
        "AsyncClient",
        lambda *args, **kwargs: _FakeAsyncClient(payload),
    )

    result = await svc.sync_upcoming_splits_calendar_for_visible_tickers(db)

    assert result["tickers_targeted"] == 4
    assert result["tickers_updated"] == 1
    assert result["tickers_skipped_invalid"] == 1
    assert result["tickers_skipped_not_in_universe"] == 2
    assert result["cleanup_deleted"] == 2
    assert [doc["ticker"] for doc in db.upcoming_splits.docs] == ["CUE.US"]


@pytest.mark.asyncio
async def test_sync_upcoming_splits_uses_composite_upsert_key_and_is_idempotent(monkeypatch):
    payload = {
        "splits": [
            {
                "code": "CUE.US",
                "date": "2026-04-24",
                "old_shares": 1,
                "new_shares": 30,
            },
            {
                "code": "CUE.US",
                "date": "2026-05-01",
                "old_shares": 2,
                "new_shares": 1,
            },
            {
                "code": "CUE.US",
                "date": "2026-04-24",
                "old_shares": 1,
                "new_shares": 30,
            },
        ]
    }
    db = _Db(tracked_tickers=[
        {
            "ticker": "CUE.US",
            "is_visible": True,
            "exchange": "NASDAQ",
            "asset_type": "Common Stock",
        }
    ])

    monkeypatch.setattr(svc, "EODHD_API_KEY", "test-key")
    monkeypatch.setattr(svc.httpx, "AsyncClient", lambda *args, **kwargs: _FakeAsyncClient(payload))

    first = await svc.sync_upcoming_splits_calendar_for_visible_tickers(db)
    second = await svc.sync_upcoming_splits_calendar_for_visible_tickers(db)

    assert first["tickers_updated"] == 2
    assert second["tickers_updated"] == 2
    assert len(db.upcoming_splits.get_docs_for_ticker("CUE.US")) == 2


@pytest.mark.asyncio
async def test_sync_upcoming_ipos_accepts_nested_data_and_symbol_company_price(monkeypatch):
    payload = {
        "data": {
            "ipos": [
                {
                    "Symbol": "RIKU",
                    "company": "RIKU DINING GROUP Ltd",
                    "date": "2026-04-24",
                    "exchange": "NASDAQ",
                    "price": 5,
                    "currency": "USD",
                    "amount": 25000000,
                }
            ]
        }
    }
    db = _Db()

    monkeypatch.setattr(svc, "EODHD_API_KEY", "test-key")
    monkeypatch.setattr(svc.httpx, "AsyncClient", lambda *args, **kwargs: _FakeAsyncClient(payload))

    result = await svc.sync_upcoming_ipos_calendar(db)

    assert result["records_written"] == 1
    assert db.upcoming_ipos.inserted[0]["ticker"] == "RIKU.US"
    assert db.upcoming_ipos.inserted[0]["ipo_date"] == "2026-04-24"
    assert db.upcoming_ipos.inserted[0]["name"] == "RIKU DINING GROUP Ltd"
    assert db.upcoming_ipos.inserted[0]["description"] == "RIKU DINING GROUP Ltd"
    assert db.upcoming_ipos.inserted[0]["ipo_price"] == 5
    assert db.upcoming_ipos.inserted[0]["currency"] == "USD"
    assert db.upcoming_ipos.inserted[0]["amount"] == 25000000
