"""Tests for the Step 3 logo backfill worklist and completeness enforcement."""

import asyncio
from datetime import datetime, timezone, timedelta
from types import SimpleNamespace

import pytest

from scheduler_service import (
    _build_logo_backfill_worklist,
    _reconcile_logo_completeness,
    LOGO_CDN_FIX_CUTOFF,
    LOGO_BACKFILL_VISIBLE_LIMIT,
)


# ── Lightweight async helpers ──────────────────────────────────────────


class _AsyncCursor:
    def __init__(self, docs):
        self._docs = list(docs)

    def sort(self, *_a, **_kw):
        return self

    async def to_list(self, _length=None):
        return list(self._docs)

    def __aiter__(self):
        self._iter = iter(self._docs)
        return self

    async def __anext__(self):
        try:
            return next(self._iter)
        except StopIteration:
            raise StopAsyncIteration


class _FakeTrackedTickers:
    """Minimal tracked_tickers mock supporting distinct / find / update_one / update_many / count_documents."""

    def __init__(self, docs):
        # keyed by ticker
        self._docs = {d["ticker"]: dict(d) for d in docs}

    async def distinct(self, field, query):
        results = []
        for doc in self._docs.values():
            if not self._matches(doc, query):
                continue
            results.append(doc.get(field))
        return results

    async def count_documents(self, query):
        return sum(1 for d in self._docs.values() if self._matches(d, query))

    async def update_one(self, filt, update, upsert=False):
        ticker = filt.get("ticker")
        doc = self._docs.get(ticker) if ticker else None
        if doc is None:
            return SimpleNamespace(modified_count=0)
        # Check additional filter conditions
        for k, v in filt.items():
            if k == "ticker":
                continue
            if doc.get(k) != v:
                return SimpleNamespace(modified_count=0)
        for key, value in (update.get("$set") or {}).items():
            doc[key] = value
        return SimpleNamespace(modified_count=1)

    async def update_many(self, filt, update):
        tickers = filt.get("ticker", {}).get("$in", [])
        modified = 0
        for t in tickers:
            doc = self._docs.get(t)
            if doc:
                for key, value in (update.get("$set") or {}).items():
                    doc[key] = value
                modified += 1
        return SimpleNamespace(modified_count=modified)

    def find(self, query, projection=None):
        matched = [dict(d) for d in self._docs.values() if self._matches(d, query)]
        return _AsyncCursor(matched)

    @staticmethod
    def _matches(doc, query):
        """Simplified MongoDB query matcher for tests."""
        for key, condition in query.items():
            if key == "$or":
                if not any(_FakeTrackedTickers._matches(doc, clause) for clause in condition):
                    return False
                continue
            val = doc.get(key)
            if isinstance(condition, dict):
                for op, operand in condition.items():
                    if op == "$in" and val not in operand:
                        return False
                    if op == "$ne" and val == operand:
                        return False
                    if op == "$nin" and val in operand:
                        return False
                    if op == "$exists":
                        if operand and key not in doc:
                            return False
                        if not operand and key in doc:
                            return False
                    if op == "$lt" and (val is None or val >= operand):
                        return False
                    if op == "$gte" and (val is None or val < operand):
                        return False
            else:
                if val != condition:
                    return False
        return True


class _FakeCache:
    """Minimal company_fundamentals_cache mock."""

    def __init__(self, docs):
        self._docs = {d["ticker"]: dict(d) for d in docs}

    def find(self, query, projection=None):
        matched = [dict(d) for d in self._docs.values() if self._match(d, query)]
        return _AsyncCursor(matched)

    async def distinct(self, field, query):
        results = []
        for doc in self._docs.values():
            if self._match(doc, query):
                results.append(doc.get(field))
        return results

    async def update_one(self, filt, update, upsert=False):
        ticker = filt.get("ticker")
        doc = self._docs.get(ticker)
        if doc is None:
            return SimpleNamespace(modified_count=0)
        for key, value in (update.get("$set") or {}).items():
            doc[key] = value
        return SimpleNamespace(modified_count=1)

    @staticmethod
    def _match(doc, query):
        for key, condition in query.items():
            if key == "$or":
                if not any(_FakeCache._match(doc, clause) for clause in condition):
                    return False
                continue
            val = doc.get(key)
            if isinstance(condition, dict):
                for op, operand in condition.items():
                    if op == "$in" and val not in operand:
                        return False
                    if op == "$ne" and val == operand:
                        return False
                    if op == "$exists":
                        if operand and key not in doc:
                            return False
                        if not operand and key in doc:
                            return False
                    if op == "$lt" and (val is None or val >= operand):
                        return False
                    if op == "$gte" and (val is None or val < operand):
                        return False
            else:
                if val != condition:
                    return False
        return True


class _FakeDB:
    def __init__(self, tracked_docs, cache_docs):
        self.tracked_tickers = _FakeTrackedTickers(tracked_docs)
        self.company_fundamentals_cache = _FakeCache(cache_docs)


# ── Shared ticker template ─────────────────────────────────────────────

_BASE_TRACKED = {
    "exchange": "NYSE",
    "asset_type": "Common Stock",
    "is_seeded": True,
    "has_price_data": True,
    "status": "active",
    "is_visible": True,
}


# ═════════════════════════════════════════════════════════════════════════
# Tests: _build_logo_backfill_worklist
# ═════════════════════════════════════════════════════════════════════════


def test_backfill_worklist_picks_missing_logo_status():
    """Cache doc with no logo_status field → should be in worklist."""
    db = _FakeDB(
        [{"ticker": "AAPL.US", **_BASE_TRACKED}],
        [{"ticker": "AAPL.US", "logo_url": "/img/logos/US/AAPL.png"}],
    )
    result = asyncio.run(_build_logo_backfill_worklist(db))
    assert len(result) == 1
    assert result[0]["ticker"] == "AAPL.US"


def test_backfill_worklist_picks_error_logo_status():
    """Cache doc with logo_status='error' → should be in worklist."""
    db = _FakeDB(
        [{"ticker": "MSFT.US", **_BASE_TRACKED}],
        [{"ticker": "MSFT.US", "logo_status": "error", "logo_url": "/img/logos/US/MSFT.png"}],
    )
    result = asyncio.run(_build_logo_backfill_worklist(db))
    assert len(result) == 1


def test_backfill_worklist_picks_stale_absent():
    """Cache doc with logo_status='absent' fetched BEFORE cutoff → in worklist."""
    stale_date = LOGO_CDN_FIX_CUTOFF - timedelta(hours=1)
    db = _FakeDB(
        [{"ticker": "GOOG.US", **_BASE_TRACKED}],
        [{
            "ticker": "GOOG.US",
            "logo_status": "absent",
            "logo_fetched_at": stale_date,
            "logo_url": "/img/logos/US/GOOG.png",
        }],
    )
    result = asyncio.run(_build_logo_backfill_worklist(db))
    assert len(result) == 1


def test_backfill_worklist_picks_absent_status():
    """Cache doc with logo_status='absent' → always in worklist (visible scope)."""
    db = _FakeDB(
        [{"ticker": "GOOG.US", **_BASE_TRACKED}],
        [{
            "ticker": "GOOG.US",
            "logo_status": "absent",
            "logo_fetched_at": LOGO_CDN_FIX_CUTOFF + timedelta(hours=1),
            "logo_url": "/img/logos/US/GOOG.png",
        }],
    )
    result = asyncio.run(_build_logo_backfill_worklist(db))
    assert len(result) == 1


def test_backfill_worklist_skips_present():
    """Cache doc with logo_status='present' → NOT in worklist."""
    db = _FakeDB(
        [{"ticker": "AAPL.US", **_BASE_TRACKED}],
        [{
            "ticker": "AAPL.US",
            "logo_status": "present",
            "logo_fetched_at": datetime.now(timezone.utc),
            "logo_url": "/img/logos/US/AAPL.png",
        }],
    )
    result = asyncio.run(_build_logo_backfill_worklist(db))
    assert len(result) == 0


def test_backfill_worklist_excludes_specified_tickers():
    """Tickers in exclude_tickers are filtered out."""
    db = _FakeDB(
        [
            {"ticker": "AAPL.US", **_BASE_TRACKED},
            {"ticker": "MSFT.US", **_BASE_TRACKED},
        ],
        [
            {"ticker": "AAPL.US", "logo_status": "error"},
            {"ticker": "MSFT.US", "logo_status": "error"},
        ],
    )
    result = asyncio.run(_build_logo_backfill_worklist(db, exclude_tickers={"AAPL.US"}))
    assert len(result) == 1
    assert result[0]["ticker"] == "MSFT.US"


def test_backfill_worklist_absent_no_fetched_at():
    """Cache doc with logo_status='absent' but no logo_fetched_at → in worklist."""
    db = _FakeDB(
        [{"ticker": "TSLA.US", **_BASE_TRACKED}],
        [{"ticker": "TSLA.US", "logo_status": "absent", "logo_url": "/img/logos/US/TSLA.png"}],
    )
    result = asyncio.run(_build_logo_backfill_worklist(db))
    assert len(result) == 1


def test_backfill_worklist_scoped_to_visible_tickers():
    """Tickers that are not visible (is_visible=False or status!='active') are excluded."""
    db = _FakeDB(
        [
            {"ticker": "AAPL.US", **_BASE_TRACKED},
            {"ticker": "HIDDEN.US", "exchange": "NYSE", "asset_type": "Common Stock",
             "is_seeded": True, "has_price_data": True,
             "status": "active", "is_visible": False},
        ],
        [
            {"ticker": "AAPL.US", "logo_status": "error"},
            {"ticker": "HIDDEN.US", "logo_status": "error"},
        ],
    )
    result = asyncio.run(_build_logo_backfill_worklist(db))
    # HIDDEN.US is excluded because is_visible=False
    assert len(result) == 1
    assert result[0]["ticker"] == "AAPL.US"


def test_backfill_worklist_excludes_inactive_tickers():
    """Inactive tickers should not be in worklist even if visible."""
    db = _FakeDB(
        [
            {"ticker": "AAPL.US", "exchange": "NYSE", "asset_type": "Common Stock",
             "is_seeded": True, "has_price_data": True,
             "status": "inactive", "is_visible": True},
        ],
        [
            {"ticker": "AAPL.US", "logo_status": "error"},
        ],
    )
    result = asyncio.run(_build_logo_backfill_worklist(db))
    # Ticker has status='inactive' → not eligible
    assert len(result) == 0


def test_backfill_worklist_respects_limit():
    """Worklist should be capped at the provided limit."""
    tracked = [{"ticker": f"T{i}.US", **_BASE_TRACKED} for i in range(10)]
    cached = [{"ticker": f"T{i}.US", "logo_status": "error"} for i in range(10)]
    db = _FakeDB(tracked, cached)
    result = asyncio.run(_build_logo_backfill_worklist(db, limit=3))
    assert len(result) == 3


# ═════════════════════════════════════════════════════════════════════════
# Tests: _reconcile_logo_completeness (updated for CDN fix cutoff)
# ═════════════════════════════════════════════════════════════════════════


def test_reconcile_resets_stale_absent_before_cutoff():
    """Tickers with fundamentals_status='complete' but stale 'absent' logo
    (fetched before CDN fix) should be reset to partial."""
    stale_date = LOGO_CDN_FIX_CUTOFF - timedelta(hours=2)
    db = _FakeDB(
        [{
            "ticker": "AAPL.US",
            **_BASE_TRACKED,
            "fundamentals_status": "complete",
            "needs_fundamentals_refresh": False,
        }],
        [{
            "ticker": "AAPL.US",
            "logo_status": "absent",
            "logo_fetched_at": stale_date,
        }],
    )
    result = asyncio.run(_reconcile_logo_completeness(db))
    assert result["reset_count"] == 1
    assert "AAPL.US" in result["reset_tickers"]
    doc = db.tracked_tickers._docs["AAPL.US"]
    assert doc["fundamentals_status"] == "partial"
    assert doc["needs_fundamentals_refresh"] is True


def test_reconcile_keeps_fresh_absent():
    """Tickers with 'absent' logo fetched AFTER CDN fix → still considered resolved."""
    fresh_date = LOGO_CDN_FIX_CUTOFF + timedelta(hours=1)
    db = _FakeDB(
        [{
            "ticker": "AAPL.US",
            **_BASE_TRACKED,
            "fundamentals_status": "complete",
            "needs_fundamentals_refresh": False,
        }],
        [{
            "ticker": "AAPL.US",
            "logo_status": "absent",
            "logo_fetched_at": fresh_date,
        }],
    )
    result = asyncio.run(_reconcile_logo_completeness(db))
    assert result["reset_count"] == 0
    doc = db.tracked_tickers._docs["AAPL.US"]
    assert doc["fundamentals_status"] == "complete"


def test_reconcile_resets_missing_logo_status():
    """Tickers with fundamentals_status='complete' but no logo_status at all → reset."""
    db = _FakeDB(
        [{
            "ticker": "TSLA.US",
            **_BASE_TRACKED,
            "fundamentals_status": "complete",
            "needs_fundamentals_refresh": False,
        }],
        [{"ticker": "TSLA.US"}],  # no logo_status field
    )
    result = asyncio.run(_reconcile_logo_completeness(db))
    assert result["reset_count"] == 1


def test_reconcile_resets_error_logo_status():
    """Tickers with fundamentals_status='complete' but logo_status='error' → reset."""
    db = _FakeDB(
        [{
            "ticker": "META.US",
            **_BASE_TRACKED,
            "fundamentals_status": "complete",
            "needs_fundamentals_refresh": False,
        }],
        [{"ticker": "META.US", "logo_status": "error", "logo_fetched_at": datetime.now(timezone.utc)}],
    )
    result = asyncio.run(_reconcile_logo_completeness(db))
    assert result["reset_count"] == 1


def test_reconcile_keeps_present_logo():
    """Tickers with logo_status='present' and logo_fetched_at → NOT reset."""
    db = _FakeDB(
        [{
            "ticker": "AAPL.US",
            **_BASE_TRACKED,
            "fundamentals_status": "complete",
            "needs_fundamentals_refresh": False,
        }],
        [{
            "ticker": "AAPL.US",
            "logo_status": "present",
            "logo_fetched_at": datetime.now(timezone.utc),
        }],
    )
    result = asyncio.run(_reconcile_logo_completeness(db))
    assert result["reset_count"] == 0


def test_reconcile_requires_logo_fetched_at():
    """A 'present' logo without logo_fetched_at should NOT be considered resolved."""
    db = _FakeDB(
        [{
            "ticker": "AAPL.US",
            **_BASE_TRACKED,
            "fundamentals_status": "complete",
            "needs_fundamentals_refresh": False,
        }],
        [{
            "ticker": "AAPL.US",
            "logo_status": "present",
            # logo_fetched_at deliberately missing
        }],
    )
    result = asyncio.run(_reconcile_logo_completeness(db))
    assert result["reset_count"] == 1


# ═════════════════════════════════════════════════════════════════════════
# Tests: completeness enforcement in batch_jobs_service
# ═════════════════════════════════════════════════════════════════════════


def test_completeness_requires_logo_fetched_at(monkeypatch):
    """fundamentals_status='complete' requires logo_fetched_at to be set."""
    from batch_jobs_service import sync_single_ticker_fundamentals

    class _SimpleCollection:
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
            return dict(self.docs.get(key, {})) if key in self.docs else None

        async def bulk_write(self, ops, ordered=False):
            self.rows_count += len(ops)
            return SimpleNamespace(upserted_count=len(ops), matched_count=0, modified_count=0)

        async def count_documents(self, filt):
            return self.rows_count

    class _SimpleDB:
        def __init__(self):
            self.tracked_tickers = _SimpleCollection()
            self.company_fundamentals_cache = _SimpleCollection()
            self.company_financials = _SimpleCollection()
            self.company_earnings_history = _SimpleCollection()
            self.insider_activity_cache = _SimpleCollection()

    db = _SimpleDB()
    db.tracked_tickers.docs["TEST.US"] = {"ticker": "TEST.US"}

    async def _fake_fetch(_ticker):
        return {
            "General": {"Name": "Test Corp"},
            "Financials": {"Income_Statement": {"yearly": {"2025-12-31": {"totalRevenue": 1}}}},
            "Earnings": {"History": {"2025-12-31": {"reportDate": "2025-12-31", "epsActual": 1.0}}},
            "SharesStats": {"SharesOutstanding": "500"},
        }

    async def _fake_debug(**_kw):
        return {"stored": True}

    # Logo result with logo_fetched_at=None → should NOT be "complete"
    async def _fake_logo_with_null_fetched_at(_url, _ticker):
        return {"logo_status": "present", "logo_fetched_at": None, "logo_data": b"PNG", "logo_content_type": "image/png"}

    monkeypatch.setattr("batch_jobs_service.fetch_fundamentals_from_eodhd", _fake_fetch)
    monkeypatch.setattr("batch_jobs_service.upsert_provider_debug_snapshot", _fake_debug)
    monkeypatch.setattr("batch_jobs_service.parse_company_fundamentals",
                        lambda t, d, raw_payload_hash=None: {"ticker": t, "name": "Test", "sector": "Tech", "industry": "SW"})
    monkeypatch.setattr("batch_jobs_service.parse_financials",
                        lambda t, d: [{"ticker": t, "period_type": "annual", "period_date": "2025-12-31"}])
    monkeypatch.setattr("batch_jobs_service.parse_earnings_history",
                        lambda t, d: [{"ticker": t, "quarter_date": "2025-12-31"}])
    monkeypatch.setattr("batch_jobs_service.parse_insider_activity", lambda t, d: None)
    monkeypatch.setattr("batch_jobs_service._download_logo", _fake_logo_with_null_fetched_at)

    result = asyncio.run(sync_single_ticker_fundamentals(db, "TEST"))
    assert result["success"] is True

    updated = db.tracked_tickers.docs["TEST.US"]
    # Without logo_fetched_at, should remain partial
    assert updated["fundamentals_status"] == "partial"
    assert updated["needs_fundamentals_refresh"] is True
