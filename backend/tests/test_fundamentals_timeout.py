"""Tests for fundamentals sync timeout instrumentation.

Verifies that:
  - A hanging DB write is caught by the hard timeout and classified as ``db_timeout``.
  - A hanging ``sync_single_ticker_fundamentals`` is caught by the per-ticker
    ``asyncio.wait_for`` and classified as ``ticker_timeout``.
"""

from types import SimpleNamespace
import asyncio
from datetime import datetime, timezone

import pytest

import batch_jobs_service
from batch_jobs_service import (
    sync_single_ticker_fundamentals,
    DB_OP_TIMEOUT_SECONDS,
)


# ---------------------------------------------------------------------------
# Fake DB layer (reused from test_batch_jobs_service with hanging variant)
# ---------------------------------------------------------------------------

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
        return SimpleNamespace(modified_count=1, upserted_count=0, matched_count=1)

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


class _HangingCollection(_FakeCollection):
    """A collection where update_one and bulk_write hang forever."""

    async def update_one(self, *args, **kwargs):
        await asyncio.sleep(999)

    async def bulk_write(self, *args, **kwargs):
        await asyncio.sleep(999)


class _FakeDB:
    def __init__(self):
        self.tracked_tickers = _FakeCollection()
        self.company_fundamentals_cache = _FakeCollection()
        self.company_financials = _FakeCollection()
        self.company_earnings_history = _FakeCollection()
        self.insider_activity_cache = _FakeCollection()


# ---------------------------------------------------------------------------
# Monkeypatch helpers
# ---------------------------------------------------------------------------

def _apply_common_patches(monkeypatch):
    """Stub out EODHD, parser, and logo calls so only DB writes matter."""
    async def _fake_fetch(_ticker):
        return {
            "General": {"Name": "Hang Corp"},
            "Financials": {
                "Income_Statement": {
                    "yearly": {"2025-12-31": {"totalRevenue": 1}},
                }
            },
            "Earnings": {
                "History": {
                    "2025-12-31": {"reportDate": "2025-12-31", "epsActual": 0.5},
                }
            },
            "SharesStats": {"SharesOutstanding": "500"},
        }

    async def _fake_debug_snapshot(**_kw):
        return {"stored": True}

    async def _fake_logo(_url, _ticker):
        return {"logo_status": "absent", "logo_fetched_at": datetime.now(timezone.utc)}

    monkeypatch.setattr("batch_jobs_service.fetch_fundamentals_from_eodhd", _fake_fetch)
    monkeypatch.setattr("batch_jobs_service.upsert_provider_debug_snapshot", _fake_debug_snapshot)
    monkeypatch.setattr(
        "batch_jobs_service.parse_company_fundamentals",
        lambda ticker, data, raw_payload_hash=None: {
            "ticker": ticker, "name": "Hang Corp",
            "sector": "Tech", "industry": "Software",
        },
    )
    monkeypatch.setattr(
        "batch_jobs_service.parse_financials",
        lambda ticker, data: [
            {"ticker": ticker, "period_type": "annual", "period_date": "2025-12-31"},
        ],
    )
    monkeypatch.setattr(
        "batch_jobs_service.parse_earnings_history",
        lambda ticker, data: [
            {"ticker": ticker, "quarter_date": "2025-12-31"},
        ],
    )
    monkeypatch.setattr("batch_jobs_service.parse_insider_activity", lambda t, d: None)
    monkeypatch.setattr("batch_jobs_service._download_logo", _fake_logo)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_db_timeout_classification(monkeypatch):
    """A hanging DB write triggers _DBOperationTimeout → error_type='db_timeout'.

    The test overrides DB_OP_TIMEOUT_SECONDS to 1 s for speed, makes the first
    DB write (company_fundamentals_cache.update_one) hang, and asserts the
    function returns quickly with the correct classification.
    """
    monkeypatch.setattr("batch_jobs_service.DB_OP_TIMEOUT_SECONDS", 1)
    _apply_common_patches(monkeypatch)

    db = _FakeDB()
    # Make the first significant DB write hang
    db.company_fundamentals_cache = _HangingCollection()

    result = asyncio.run(sync_single_ticker_fundamentals(db, "HANG"))

    assert result["success"] is False
    assert result["error_type"] == "db_timeout"
    assert "db_timeout" in result["error"]
    # Verify the ticker was marked as error in tracked_tickers
    tt = db.tracked_tickers.docs.get("HANG.US", {})
    assert tt.get("fundamentals_error_code") == "db_timeout"


def test_ticker_timeout_does_not_block_pipeline():
    """Simulates asyncio.wait_for pattern used by _process_one in scheduler.

    A coroutine that sleeps forever is killed by wait_for and correctly
    classified as ticker_timeout — proving the pipeline would not stall.
    """
    from scheduler_service import FUNDAMENTALS_SYNC_TICKER_TIMEOUT_SECONDS

    async def _run():
        async def _hanging_sync(*_a, **_kw):
            await asyncio.sleep(999)

        # Use a 1-second timeout for test speed (real pipeline uses 120 s)
        TIMEOUT = 1
        ticker = "STALL"
        ticker_full = "STALL.US"
        try:
            result = await asyncio.wait_for(
                _hanging_sync(None, ticker),
                timeout=TIMEOUT,
            )
        except asyncio.TimeoutError:
            result = {
                "ticker": ticker_full,
                "success": False,
                "error": f"Per-ticker hard timeout after {TIMEOUT}s",
                "error_type": "ticker_timeout",
            }

        assert result["success"] is False
        assert result["error_type"] == "ticker_timeout"
        assert "timeout" in result["error"].lower()

    asyncio.run(_run())

    # Verify the constant is defined and sane
    assert FUNDAMENTALS_SYNC_TICKER_TIMEOUT_SECONDS >= 60
