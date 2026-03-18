from types import SimpleNamespace
import asyncio
from datetime import datetime, timezone

import pytest

from batch_jobs_service import sync_single_ticker_fundamentals


class _FakeCollection:
    def __init__(self):
        self.docs = {}
        self.rows_count = 0

    async def update_one(self, filt, update, upsert=False):
        key = filt.get("ticker")
        doc = dict(self.docs.get(key, {})) if key else {}
        for field, value in (update.get("$set") or {}).items():
            doc[field] = value
        if key:
            doc["ticker"] = key
            self.docs[key] = doc
        return SimpleNamespace()

    async def find_one(self, filt, projection=None):
        key = filt.get("ticker")
        doc = self.docs.get(key)
        if doc is None:
            return None
        if not projection:
            return dict(doc)

        out = {}
        for field, include in projection.items():
            if field == "_id" or not include:
                continue
            if field in doc:
                out[field] = doc[field]
        return out

    async def bulk_write(self, ops, ordered=False):
        self.rows_count += len(ops)
        return SimpleNamespace(upserted_count=len(ops), matched_count=0, modified_count=0)

    async def count_documents(self, filt):
        return self.rows_count


class _FakeDB:
    def __init__(self):
        self.tracked_tickers = _FakeCollection()
        self.company_fundamentals_cache = _FakeCollection()
        self.company_financials = _FakeCollection()
        self.company_earnings_history = _FakeCollection()
        self.insider_activity_cache = _FakeCollection()


def test_sync_single_ticker_success_clears_stale_error_fields(monkeypatch):
    db = _FakeDB()
    db.tracked_tickers.docs["AAPL.US"] = {
        "ticker": "AAPL.US",
        "fundamentals_status": "complete",
        "fundamentals_complete": True,
        "needs_fundamentals_refresh": True,
        "fundamentals_error": "old",
        "fundamentals_error_code": "old_code",
        "fundamentals_error_at": "2025-01-01T00:00:00+00:00",
    }

    async def _fake_fetch(_ticker):
        return {
            "General": {"Name": "Apple Inc"},
            "Financials": {
                "Income_Statement": {
                    "yearly": {
                        "2025-12-31": {"totalRevenue": 1},
                    }
                }
            },
            "Earnings": {
                "History": {
                    "2025-12-31": {"reportDate": "2025-12-31", "epsActual": 1.23},
                }
            },
            "SharesStats": {"SharesOutstanding": "1000"},
        }

    async def _fake_debug_snapshot(**_kwargs):
        return {"stored": True}

    monkeypatch.setattr(
        "batch_jobs_service.fetch_fundamentals_from_eodhd",
        _fake_fetch,
    )
    monkeypatch.setattr(
        "batch_jobs_service.upsert_provider_debug_snapshot",
        _fake_debug_snapshot,
    )
    monkeypatch.setattr(
        "batch_jobs_service.parse_company_fundamentals",
        lambda ticker, data, raw_payload_hash=None: {
            "ticker": ticker,
            "name": "Apple Inc",
            "sector": "Technology",
            "industry": "Consumer Electronics",
        },
    )
    monkeypatch.setattr(
        "batch_jobs_service.parse_financials",
        lambda ticker, data: [
            {
                "ticker": ticker,
                "period_type": "annual",
                "period_date": "2025-12-31",
            }
        ],
    )
    monkeypatch.setattr(
        "batch_jobs_service.parse_earnings_history",
        lambda ticker, data: [
            {
                "ticker": ticker,
                "quarter_date": "2025-12-31",
            }
        ],
    )
    monkeypatch.setattr("batch_jobs_service.parse_insider_activity", lambda ticker, data: None)

    result = asyncio.run(sync_single_ticker_fundamentals(db, "AAPL"))

    assert result["success"] is True

    updated = db.tracked_tickers.docs["AAPL.US"]
    assert updated["fundamentals_status"] == "complete"
    assert updated["fundamentals_complete"] is True
    assert updated["needs_fundamentals_refresh"] is False
    assert isinstance(updated["fundamentals_updated_at"], datetime)
    assert updated["fundamentals_updated_at"].tzinfo == timezone.utc

    assert updated["fundamentals_error"] is None
    assert updated["fundamentals_error_code"] is None
    assert updated["fundamentals_error_at"] is None
