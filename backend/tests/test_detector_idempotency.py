"""Tests for split/dividend detector idempotency.

After a ticker is detected and its price history is fully re-downloaded
(price_history_complete=True, last_split_detected=today), running the
same detector again on the same day must NOT re-flag the ticker.
"""

import asyncio
from copy import deepcopy
from types import SimpleNamespace

import scheduler_service
from scheduler_service import (
    _detect_split_candidates_eodhd,
    _detect_dividend_candidates_eodhd,
)


# ── helpers ────────────────────────────────────────────────────────────

class _FakeCursor:
    def __init__(self, docs):
        self._docs = list(docs)

    async def to_list(self, _length=None):
        return list(self._docs)


class _FakeTrackedTickers:
    """Simulates tracked_tickers with idempotent update_many support.

    The real MongoDB update_many applies the filter, so we replicate
    $or / $ne matching to return the correct modified_count.
    """

    def __init__(self, docs):
        self._docs = list(docs)           # [{ticker, ...fields}]
        self.update_many_calls = []

    def find(self, query, projection=None):
        return _FakeCursor(deepcopy(self._docs))

    async def update_many(self, filt, update):
        self.update_many_calls.append({
            "filter": deepcopy(filt),
            "update": deepcopy(update),
        })
        # Simulate the $or idempotency filter that limits which docs match
        target_tickers = set(filt.get("ticker", {}).get("$in", []))
        or_clauses = filt.get("$or", [])

        matched = 0
        for doc in self._docs:
            if doc["ticker"] not in target_tickers:
                continue
            if or_clauses:
                if not _any_or_matches(doc, or_clauses):
                    continue
            matched += 1
            # Apply the $set so subsequent calls see updated state
            for k, v in update.get("$set", {}).items():
                doc[k] = v
            # Apply the $unset so subsequent calls see removed fields
            for k in update.get("$unset", {}):
                doc.pop(k, None)

        return SimpleNamespace(modified_count=matched)


def _any_or_matches(doc, clauses):
    """Minimal $or evaluator supporting $ne."""
    for clause in clauses:
        if _matches_clause(doc, clause):
            return True
    return False


def _matches_clause(doc, clause):
    for key, cond in clause.items():
        if isinstance(cond, dict):
            if "$ne" in cond:
                if doc.get(key) == cond["$ne"]:
                    return False
            else:
                raise ValueError(f"Unsupported operator in test mock: {cond}")
        else:
            if doc.get(key) != cond:
                return False
    return True


class _FakeDB:
    def __init__(self, tracked_docs):
        self.tracked_tickers = _FakeTrackedTickers(tracked_docs)


async def _noop_log_credit(*_a, **_kw):
    pass


# ── split detector tests ──────────────────────────────────────────────

def test_split_detector_flags_on_first_run(monkeypatch):
    """First-time detection for a date should flag the ticker."""
    today = "2026-03-21"

    db = _FakeDB([
        {
            "ticker": "AAPL.US",
            "exchange": "NASDAQ",
            "asset_type": "Common Stock",
            "is_seeded": True,
            "has_price_data": True,
        },
    ])

    async def _fake_fetch(endpoint, params):
        return [{"code": "AAPL"}], 200, 10, "success"

    monkeypatch.setattr(scheduler_service, "_fetch_eodhd_bulk", _fake_fetch)
    monkeypatch.setattr("credit_log_service.log_api_credit", _noop_log_credit)

    result = asyncio.get_event_loop().run_until_complete(
        _detect_split_candidates_eodhd(db, today)
    )

    assert result["flagged_count"] == 1
    assert result["universe_count"] == 1

    # Verify the $or idempotency filter is present
    call = db.tracked_tickers.update_many_calls[0]
    assert "$or" in call["filter"]
    assert {"last_split_detected": {"$ne": today}} in call["filter"]["$or"]
    assert {"price_history_complete": {"$ne": True}} in call["filter"]["$or"]


def test_split_detector_skips_already_remediated(monkeypatch):
    """A ticker already remediated for today's split must NOT be re-flagged."""
    today = "2026-03-21"

    db = _FakeDB([
        {
            "ticker": "AAPL.US",
            "exchange": "NASDAQ",
            "asset_type": "Common Stock",
            "is_seeded": True,
            "has_price_data": True,
            # Already remediated: detected today AND price_history is complete
            "last_split_detected": today,
            "price_history_complete": True,
            "needs_price_redownload": False,
        },
    ])

    async def _fake_fetch(endpoint, params):
        return [{"code": "AAPL"}], 200, 10, "success"

    monkeypatch.setattr(scheduler_service, "_fetch_eodhd_bulk", _fake_fetch)
    monkeypatch.setattr("credit_log_service.log_api_credit", _noop_log_credit)

    result = asyncio.get_event_loop().run_until_complete(
        _detect_split_candidates_eodhd(db, today)
    )

    # API still found the ticker in universe, but flagged_count is 0
    assert result["universe_count"] == 1
    assert result["flagged_count"] == 0
    # The ticker's flags must remain untouched
    doc = db.tracked_tickers._docs[0]
    assert doc["price_history_complete"] is True
    assert doc["needs_price_redownload"] is False


def test_split_detector_reflags_incomplete_ticker(monkeypatch):
    """A ticker detected today but not yet remediated should be re-flagged."""
    today = "2026-03-21"

    db = _FakeDB([
        {
            "ticker": "AAPL.US",
            "exchange": "NASDAQ",
            "asset_type": "Common Stock",
            "is_seeded": True,
            "has_price_data": True,
            # Detected today, but NOT yet remediated
            "last_split_detected": today,
            "price_history_complete": False,
            "needs_price_redownload": True,
        },
    ])

    async def _fake_fetch(endpoint, params):
        return [{"code": "AAPL"}], 200, 10, "success"

    monkeypatch.setattr(scheduler_service, "_fetch_eodhd_bulk", _fake_fetch)
    monkeypatch.setattr("credit_log_service.log_api_credit", _noop_log_credit)

    result = asyncio.get_event_loop().run_until_complete(
        _detect_split_candidates_eodhd(db, today)
    )

    # Ticker is still incomplete, should be re-flagged
    assert result["flagged_count"] == 1


def test_split_detector_flags_new_date(monkeypatch):
    """Detection on a new date should flag even previously remediated tickers."""
    yesterday = "2026-03-20"
    today = "2026-03-21"

    db = _FakeDB([
        {
            "ticker": "AAPL.US",
            "exchange": "NASDAQ",
            "asset_type": "Common Stock",
            "is_seeded": True,
            "has_price_data": True,
            # Previously remediated for yesterday
            "last_split_detected": yesterday,
            "price_history_complete": True,
            "needs_price_redownload": False,
        },
    ])

    async def _fake_fetch(endpoint, params):
        return [{"code": "AAPL"}], 200, 10, "success"

    monkeypatch.setattr(scheduler_service, "_fetch_eodhd_bulk", _fake_fetch)
    monkeypatch.setattr("credit_log_service.log_api_credit", _noop_log_credit)

    result = asyncio.get_event_loop().run_until_complete(
        _detect_split_candidates_eodhd(db, today)
    )

    # New date → should be flagged
    assert result["flagged_count"] == 1


# ── dividend detector tests ───────────────────────────────────────────

def test_dividend_detector_flags_on_first_run(monkeypatch):
    """First-time detection for a date should flag the ticker."""
    today = "2026-03-21"

    db = _FakeDB([
        {
            "ticker": "MSFT.US",
            "exchange": "NASDAQ",
            "asset_type": "Common Stock",
            "is_seeded": True,
            "has_price_data": True,
        },
    ])

    async def _fake_fetch(endpoint, params):
        return [{"code": "MSFT"}], 200, 10, "success"

    monkeypatch.setattr(scheduler_service, "_fetch_eodhd_bulk", _fake_fetch)
    monkeypatch.setattr("credit_log_service.log_api_credit", _noop_log_credit)

    result = asyncio.get_event_loop().run_until_complete(
        _detect_dividend_candidates_eodhd(db, today)
    )

    assert result["flagged_count"] == 1
    assert result["universe_count"] == 1

    call = db.tracked_tickers.update_many_calls[0]
    assert "$or" in call["filter"]
    assert {"last_dividend_detected": {"$ne": today}} in call["filter"]["$or"]
    assert {"price_history_complete": {"$ne": True}} in call["filter"]["$or"]


def test_dividend_detector_skips_already_remediated(monkeypatch):
    """A ticker already remediated for today's dividend must NOT be re-flagged."""
    today = "2026-03-21"

    db = _FakeDB([
        {
            "ticker": "MSFT.US",
            "exchange": "NASDAQ",
            "asset_type": "Common Stock",
            "is_seeded": True,
            "has_price_data": True,
            "last_dividend_detected": today,
            "price_history_complete": True,
            "needs_price_redownload": False,
        },
    ])

    async def _fake_fetch(endpoint, params):
        return [{"code": "MSFT"}], 200, 10, "success"

    monkeypatch.setattr(scheduler_service, "_fetch_eodhd_bulk", _fake_fetch)
    monkeypatch.setattr("credit_log_service.log_api_credit", _noop_log_credit)

    result = asyncio.get_event_loop().run_until_complete(
        _detect_dividend_candidates_eodhd(db, today)
    )

    assert result["universe_count"] == 1
    assert result["flagged_count"] == 0
    doc = db.tracked_tickers._docs[0]
    assert doc["price_history_complete"] is True
    assert doc["needs_price_redownload"] is False


def test_mixed_tickers_partial_remediation(monkeypatch):
    """Only un-remediated tickers get flagged; already-remediated are skipped."""
    today = "2026-03-21"

    db = _FakeDB([
        {
            "ticker": "AAPL.US",
            "exchange": "NASDAQ",
            "asset_type": "Common Stock",
            "is_seeded": True,
            "has_price_data": True,
            # Already remediated
            "last_split_detected": today,
            "price_history_complete": True,
            "needs_price_redownload": False,
        },
        {
            "ticker": "MSFT.US",
            "exchange": "NYSE",
            "asset_type": "Common Stock",
            "is_seeded": True,
            "has_price_data": True,
            # Never detected before
        },
    ])

    async def _fake_fetch(endpoint, params):
        return [{"code": "AAPL"}, {"code": "MSFT"}], 200, 10, "success"

    monkeypatch.setattr(scheduler_service, "_fetch_eodhd_bulk", _fake_fetch)
    monkeypatch.setattr("credit_log_service.log_api_credit", _noop_log_credit)

    result = asyncio.get_event_loop().run_until_complete(
        _detect_split_candidates_eodhd(db, today)
    )

    assert result["universe_count"] == 2
    # Only MSFT should be flagged; AAPL was already remediated
    assert result["flagged_count"] == 1
