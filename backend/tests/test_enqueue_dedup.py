"""
Tests for _enqueue_fundamentals_events same-day dedup behavior.

Verifies that on repeated same-day pipeline runs, events already
completed/skipped for the same (ticker, event_type, detected_date)
are NOT re-enqueued, and needs_fundamentals_refresh is properly managed.
"""
import asyncio
import os
import sys
from copy import deepcopy
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from scheduler_service import _enqueue_fundamentals_events  # noqa: E402


# ---------------------------------------------------------------------------
# Lightweight in-memory mocks
# ---------------------------------------------------------------------------

class _AsyncCursor:
    """Async-iterable cursor wrapping a list of docs."""
    def __init__(self, docs):
        self._docs = list(docs)

    def __aiter__(self):
        self._iter = iter(self._docs)
        return self

    async def __anext__(self):
        try:
            return next(self._iter)
        except StopIteration:
            raise StopAsyncIteration


class _FakeFundamentalsEvents:
    """Minimal mock of db.fundamentals_events with find + bulk_write + create_index."""
    def __init__(self, existing_docs=None):
        self._docs = list(existing_docs or [])
        self._index_created = False

    async def create_index(self, keys, **kwargs):
        self._index_created = True

    def find(self, query, projection=None):
        """Return cursor matching query (simple subset-match)."""
        matched = []
        for doc in self._docs:
            if self._matches(doc, query):
                matched.append(deepcopy(doc))
        return _AsyncCursor(matched)

    def _matches(self, doc, query):
        for key, condition in query.items():
            val = doc.get(key)
            if isinstance(condition, dict):
                if "$in" in condition:
                    if val not in condition["$in"]:
                        return False
            else:
                if val != condition:
                    return False
        return True

    async def bulk_write(self, ops, ordered=False):
        """Simulate upsert: insert if no pending match exists."""
        upserted_ids = {}
        idx = 0
        for op in ops:
            filt = op._filter
            # Check if a matching pending doc exists
            found = any(self._matches(d, filt) for d in self._docs)
            if not found:
                new_doc = dict(filt)
                update = op._doc
                for k, v in (update.get("$setOnInsert") or {}).items():
                    new_doc[k] = v
                for k, v in (update.get("$set") or {}).items():
                    new_doc[k] = v
                new_doc["_id"] = f"gen_{idx}_{len(self._docs)}"
                self._docs.append(new_doc)
                upserted_ids[idx] = new_doc["_id"]
            idx += 1
        return SimpleNamespace(upserted_ids=upserted_ids)


class _FakeTrackedTickers:
    """Minimal mock of db.tracked_tickers."""
    def __init__(self):
        self.updates = []  # track all update_many calls

    async def update_many(self, filt, update):
        self.updates.append({"filter": deepcopy(filt), "update": deepcopy(update)})
        tickers = filt.get("ticker", {}).get("$in", [])
        return SimpleNamespace(modified_count=len(tickers))


class _FakeDB:
    def __init__(self, existing_events=None):
        self.fundamentals_events = _FakeFundamentalsEvents(existing_events)
        self.tracked_tickers = _FakeTrackedTickers()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_first_run_enqueues_all():
    """First run: no prior events → all tickers enqueued, all flagged."""
    db = _FakeDB()
    now = datetime.now(timezone.utc)
    result = await _enqueue_fundamentals_events(
        db,
        event_type="split",
        tickers=["AAPL.US", "MSFT.US"],
        now=now,
        source_job="price_sync",
        detector_step="2.2",
        detected_date="2026-03-21",
    )

    assert result["new_inserts"] == 2
    assert result["skipped_existing"] == 0

    # needs_fundamentals_refresh should be set for both
    assert len(db.tracked_tickers.updates) == 1
    flagged = db.tracked_tickers.updates[0]["filter"]["ticker"]["$in"]
    assert set(flagged) == {"AAPL.US", "MSFT.US"}
    assert db.tracked_tickers.updates[0]["update"]["$set"]["needs_fundamentals_refresh"] is True


@pytest.mark.asyncio
async def test_repeat_run_skips_completed_events():
    """Repeat run after Step 3 completed: events already completed for same
    detected_date are NOT re-enqueued, and needs_fundamentals_refresh is
    reset for completed tickers."""
    existing = [
        {
            "ticker": "AAPL.US",
            "event_type": "split",
            "status": "completed",
            "detected_date": "2026-03-21",
            "completed_at": datetime(2026, 3, 21, 14, 0, tzinfo=timezone.utc),
        },
        {
            "ticker": "MSFT.US",
            "event_type": "split",
            "status": "completed",
            "detected_date": "2026-03-21",
            "completed_at": datetime(2026, 3, 21, 14, 0, tzinfo=timezone.utc),
        },
    ]
    db = _FakeDB(existing_events=existing)
    now = datetime.now(timezone.utc)
    result = await _enqueue_fundamentals_events(
        db,
        event_type="split",
        tickers=["AAPL.US", "MSFT.US"],
        now=now,
        source_job="price_sync",
        detector_step="2.2",
        detected_date="2026-03-21",
    )

    # Both already completed → no new inserts, both reported as skipped
    assert result["new_inserts"] == 0
    assert result["skipped_existing"] == 2

    # needs_fundamentals_refresh should be RESET (not set) for completed tickers
    reset_calls = [
        u for u in db.tracked_tickers.updates
        if u["update"]["$set"].get("needs_fundamentals_refresh") is False
    ]
    assert len(reset_calls) == 1
    reset_tickers = set(reset_calls[0]["filter"]["ticker"]["$in"])
    assert reset_tickers == {"AAPL.US", "MSFT.US"}

    # No needs_fundamentals_refresh=True calls should have been made
    flag_true_calls = [
        u for u in db.tracked_tickers.updates
        if u["update"]["$set"].get("needs_fundamentals_refresh") is True
    ]
    assert len(flag_true_calls) == 0


@pytest.mark.asyncio
async def test_mixed_completed_and_new():
    """Mix of completed and new tickers: only new ones enqueued."""
    existing = [
        {
            "ticker": "AAPL.US",
            "event_type": "dividend",
            "status": "completed",
            "detected_date": "2026-03-21",
        },
    ]
    db = _FakeDB(existing_events=existing)
    now = datetime.now(timezone.utc)
    result = await _enqueue_fundamentals_events(
        db,
        event_type="dividend",
        tickers=["AAPL.US", "MSFT.US", "GOOG.US"],
        now=now,
        source_job="price_sync",
        detector_step="2.4",
        detected_date="2026-03-21",
    )

    # AAPL already completed → skipped; MSFT+GOOG → new inserts
    assert result["new_inserts"] == 2
    assert result["skipped_existing"] == 1

    # Only MSFT+GOOG should get needs_fundamentals_refresh=True
    flag_true_calls = [
        u for u in db.tracked_tickers.updates
        if u["update"]["$set"].get("needs_fundamentals_refresh") is True
    ]
    assert len(flag_true_calls) == 1
    flagged = set(flag_true_calls[0]["filter"]["ticker"]["$in"])
    assert flagged == {"GOOG.US", "MSFT.US"}

    # AAPL should get needs_fundamentals_refresh=False
    reset_calls = [
        u for u in db.tracked_tickers.updates
        if u["update"]["$set"].get("needs_fundamentals_refresh") is False
    ]
    assert len(reset_calls) == 1
    assert "AAPL.US" in reset_calls[0]["filter"]["ticker"]["$in"]


@pytest.mark.asyncio
async def test_skipped_events_also_excluded():
    """Events with status 'skipped' for the same detected_date are also
    treated as already processed and not re-enqueued."""
    existing = [
        {
            "ticker": "TSLA.US",
            "event_type": "earnings",
            "status": "skipped",
            "detected_date": "2026-03-21",
        },
    ]
    db = _FakeDB(existing_events=existing)
    now = datetime.now(timezone.utc)
    result = await _enqueue_fundamentals_events(
        db,
        event_type="earnings",
        tickers=["TSLA.US"],
        now=now,
        source_job="price_sync",
        detector_step="2.6",
        detected_date="2026-03-21",
    )

    assert result["new_inserts"] == 0
    assert result["skipped_existing"] == 1


@pytest.mark.asyncio
async def test_different_detected_date_enqueues_new():
    """Events completed for a DIFFERENT detected_date should NOT block
    enqueue for a new date."""
    existing = [
        {
            "ticker": "AAPL.US",
            "event_type": "split",
            "status": "completed",
            "detected_date": "2026-03-20",  # yesterday
        },
    ]
    db = _FakeDB(existing_events=existing)
    now = datetime.now(timezone.utc)
    result = await _enqueue_fundamentals_events(
        db,
        event_type="split",
        tickers=["AAPL.US"],
        now=now,
        source_job="price_sync",
        detector_step="2.2",
        detected_date="2026-03-21",  # today — different date
    )

    # Different detected_date → should enqueue as new
    assert result["new_inserts"] == 1
    assert result["skipped_existing"] == 0


@pytest.mark.asyncio
async def test_no_detected_date_skips_dedup():
    """When detected_date is None (e.g. manual enqueue), no dedup is performed."""
    existing = [
        {
            "ticker": "AAPL.US",
            "event_type": "manual_refresh",
            "status": "completed",
            "detected_date": None,
        },
    ]
    db = _FakeDB(existing_events=existing)
    now = datetime.now(timezone.utc)
    result = await _enqueue_fundamentals_events(
        db,
        event_type="manual_refresh",
        tickers=["AAPL.US"],
        now=now,
        source_job="manual",
        detector_step="manual",
        detected_date=None,
    )

    # No detected_date → dedup skipped → enqueue proceeds normally
    assert result["new_inserts"] == 1


@pytest.mark.asyncio
async def test_pending_event_still_updated():
    """If a pending event already exists (from same-day first run, Step 3 not
    yet run), the upsert should update it — not create a duplicate."""
    existing = [
        {
            "ticker": "AAPL.US",
            "event_type": "split",
            "status": "pending",
            "detected_date": "2026-03-21",
        },
    ]
    db = _FakeDB(existing_events=existing)
    now = datetime.now(timezone.utc)
    result = await _enqueue_fundamentals_events(
        db,
        event_type="split",
        tickers=["AAPL.US"],
        now=now,
        source_job="price_sync",
        detector_step="2.2",
        detected_date="2026-03-21",
    )

    # Pending event exists → upsert matches → no new insert, counted as skipped
    assert result["new_inserts"] == 0
    assert result["skipped_existing"] == 1

    # needs_fundamentals_refresh should still be set (pending = still needs processing)
    flag_true_calls = [
        u for u in db.tracked_tickers.updates
        if u["update"]["$set"].get("needs_fundamentals_refresh") is True
    ]
    assert len(flag_true_calls) == 1
