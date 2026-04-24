import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dividend_history_service import get_calendar_events


class _Cursor:
    def __init__(self, docs):
        self._docs = list(docs)

    def sort(self, field, direction):
        reverse = direction == -1
        self._docs = sorted(self._docs, key=lambda doc: doc.get(field) or "", reverse=reverse)
        return self

    async def to_list(self, length=None):
        if length is None:
            return list(self._docs)
        return list(self._docs[:length])


def _apply_projection(docs, projection):
    if projection and any(key != "_id" for key in projection):
        projected = []
        for doc in docs:
            projected.append({
                key: value
                for key, value in doc.items()
                if key != "_id" and key in projection
            })
        return projected
    return docs


class _Collection:
    def __init__(self, docs):
        self._docs = list(docs)

    def find(self, query, projection=None):
        if not query:
            docs = list(self._docs)
        else:
            (field, bounds), = query.items()
            docs = [
                doc for doc in self._docs
                if doc.get(field) is not None
                and doc.get(field) >= bounds.get("$gte")
                and doc.get(field) <= bounds.get("$lte")
            ]
        return _Cursor(_apply_projection(docs, projection))


class _LookupCollection:
    def __init__(self, docs):
        self._docs = list(docs)

    def find(self, query, projection=None):
        items = list(query.items())
        assert len(items) == 1, "Expected a single-field lookup query"
        field, lookup = items[0]
        allowed = set(lookup.get("$in", []))
        docs = [doc for doc in self._docs if doc.get(field) in allowed]
        return _Cursor(_apply_projection(docs, projection))


class _Db:
    def __init__(self):
        self.upcoming_earnings = _Collection([
            {
                "ticker": "AAPL.US",
                "report_date": "2026-04-28",
                "before_after_market": "After Market",
                "estimate": -0.11,
                "currency": "USD",
                "fiscal_period_end": "2026-03-31",
            },
        ])
        self.upcoming_dividends = _Collection([
            {
                "ticker": "MSFT.US",
                "next_ex_date": "2026-04-27",
                "next_dividend_amount": 0.75,
                "next_dividend_currency": "USD",
                "next_pay_date": "2026-05-10",
                "event_type_label": None,
                "coverage_complete": True,
            },
        ])
        self.upcoming_splits = _Collection([
            {
                "ticker": "NVDA.US",
                "split_date": "2026-04-29",
                "old_shares": 10,
                "new_shares": 1,
            },
        ])
        self.upcoming_ipos = _Collection([
            {
                "ticker": "FIGR.US",
                "ipo_date": "2026-04-26",
                "description": "Figure Technology Solutions IPO",
                "exchange": "NASDAQ",
                "ipo_price": 18.0,
            },
        ])
        self.company_fundamentals_cache = _LookupCollection([
            {"ticker": "AAPL.US", "name": "Apple Inc.", "logo_url": "/logos/AAPL.png", "logo_status": "present"},
            {"ticker": "MSFT.US", "name": "Microsoft", "logo_url": "/logos/MSFT.png", "logo_status": "present"},
            {"ticker": "NVDA.US", "name": "NVIDIA", "logo_url": "/logos/NVDA.png", "logo_status": "present"},
        ])


@pytest.mark.asyncio
async def test_get_calendar_events_merges_and_sorts_sources():
    db = _Db()

    result = await get_calendar_events(db, "2026-04-26", "2026-04-29")

    assert result["from"] == "2026-04-26"
    assert result["to"] == "2026-04-29"
    assert result["count"] == 4
    assert [event["type"] for event in result["events"]] == ["ipo", "dividend", "earnings", "split"]
    assert result["events"][0]["ticker"] == "FIGR"
    assert result["events"][0]["company_name"] == "Figure Technology Solutions IPO"
    assert result["events"][0]["logo_url"] is None
    assert result["events"][1]["amount"] == 0.75
    assert result["events"][1]["company_name"] == "Microsoft"
    assert result["events"][1]["logo_url"] == "/api/logo/MSFT"
    assert result["events"][2]["estimate"] == -0.11
    assert result["events"][2]["company_name"] == "Apple Inc."
    assert result["events"][2]["logo_url"] == "/api/logo/AAPL"
    assert result["events"][3]["ratio"] == "10:1"
    assert result["events"][3]["logo_url"] == "/api/logo/NVDA"
    assert result["events"][3]["metadata"] == {"old_shares": 10, "new_shares": 1}


@pytest.mark.asyncio
async def test_get_calendar_events_validates_date_window():
    db = _Db()

    with pytest.raises(ValueError, match="Start date \\(from\\) must be on or before end date \\(to\\)"):
        await get_calendar_events(db, "2026-04-30", "2026-04-29")


@pytest.mark.asyncio
async def test_get_calendar_events_supports_legacy_split_and_ipo_fields():
    db = _Db()
    db.upcoming_earnings = _Collection([])
    db.upcoming_dividends = _Collection([])
    db.upcoming_splits = _Collection([
        {
            "ticker": "SHOP.US",
            "date": "2026-04-28",
            "ratio": "3/1",
        },
    ])
    db.upcoming_ipos = _Collection([
        {
            "ticker": "FING",
            "date": "2026-04-27",
            "Description": "Fin Growth Holdings",
            "Exchange": "NASDAQ",
            "Offer_Price": 12,
        },
    ])

    result = await get_calendar_events(db, "2026-04-27", "2026-04-28")

    assert [event["type"] for event in result["events"]] == ["ipo", "split"]
    assert result["events"][0]["ticker"] == "FING"
    assert result["events"][0]["amount"] == 12
    assert result["events"][0]["description"] == "NASDAQ"
    assert result["events"][1]["ticker"] == "SHOP"
    assert result["events"][1]["ratio"] == "3:1"
    assert result["events"][1]["metadata"] == {"old_shares": 3.0, "new_shares": 1.0}
