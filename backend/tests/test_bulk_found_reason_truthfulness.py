"""
Tests that the gap_free_exclusion reason is truthful:
  - bulk_found=True  → reason != "not_in_bulk_data"
  - bulk_found=False → reason == "not_in_bulk_data"

Covers the fix for the bug where tickers present in the EODHD bulk file
with close=0 were incorrectly labeled as "not_in_bulk_data".
"""

import asyncio
import os
import sys
from types import SimpleNamespace

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import scheduler_service


# ── Fake DB helpers (self-contained, following project test patterns) ─────────

class _FakeCursor:
    """Async cursor over a list of dicts."""
    def __init__(self, docs):
        self._docs = list(docs)
    async def to_list(self, _):
        return list(self._docs)
    def __aiter__(self):
        return _FakeCursorIter(self._docs)

class _FakeCursorIter:
    def __init__(self, docs):
        self._iter = iter(docs)
    async def __anext__(self):
        try:
            return next(self._iter)
        except StopIteration:
            raise StopAsyncIteration


class _FakeTrackedTickers:
    def __init__(self, tickers):
        self._tickers = list(tickers)
    def find(self, query, projection=None):
        return _FakeCursor([{"ticker": t, "name": t} for t in self._tickers])
    async def update_many(self, filt, update):
        return SimpleNamespace(modified_count=0)


class _FakeGapFreeExclusions:
    def __init__(self):
        self.ops = []
    async def bulk_write(self, ops, ordered=False):
        self.ops.extend(ops)
        return SimpleNamespace(upserted_count=len(ops))


class _FakeDB:
    def __init__(self, tickers):
        self.tracked_tickers = _FakeTrackedTickers(tickers)
        self.gap_free_exclusions = _FakeGapFreeExclusions()


def _get_excl_doc(op):
    """Extract $set doc from a pymongo UpdateOne operation."""
    return op._doc.get("$set", {})


# ── Tests ────────────────────────────────────────────────────────────────────

class TestBulkFoundReasonTruthfulness:

    def test_truly_not_in_bulk_gets_not_in_bulk_reason(self):
        """Ticker absent from bulk file → reason='not_in_bulk_data', bulk_found=False."""
        seeded = ["AAPL.US", "GHOST.US"]
        db = _FakeDB(seeded)

        asyncio.run(
            scheduler_service.sync_has_price_data_flags(
                db,
                include_exclusions=False,
                tickers_with_price=["AAPL.US"],
                bulk_date="2026-04-15",
                # GHOST.US not in zero_close set → truly absent from bulk
                bulk_zero_close_tickers=set(),
                bulk_zero_close_data={},
            )
        )

        assert len(db.gap_free_exclusions.ops) == 1
        doc = _get_excl_doc(db.gap_free_exclusions.ops[0])
        assert doc["ticker"] == "GHOST.US"
        assert doc["reason"] == "not_in_bulk_data"
        assert doc["bulk_found"] is False

    def test_zero_close_in_bulk_gets_bulk_found_reason(self):
        """Ticker present in bulk with close=0 → reason='bulk_found_but_close_is_zero',
        bulk_found=True, and bulk debug fields populated."""
        seeded = ["AAPL.US", "NEN.US"]
        db = _FakeDB(seeded)

        asyncio.run(
            scheduler_service.sync_has_price_data_flags(
                db,
                include_exclusions=False,
                tickers_with_price=["AAPL.US"],
                bulk_date="2026-04-15",
                bulk_zero_close_tickers={"NEN.US"},
                bulk_zero_close_data={
                    "NEN.US": {
                        "close": 0,
                        "adjusted_close": 0,
                        "volume": 0,
                    },
                },
            )
        )

        assert len(db.gap_free_exclusions.ops) == 1
        doc = _get_excl_doc(db.gap_free_exclusions.ops[0])
        assert doc["ticker"] == "NEN.US"
        assert doc["reason"] == "bulk_found_but_close_is_zero"
        assert doc["bulk_found"] is True
        # Audit fields present
        assert doc["bulk_close"] == 0
        assert doc["bulk_adjusted_close"] == 0
        assert doc["bulk_volume"] == 0

    def test_mixed_absent_and_zero_close(self):
        """When some tickers are absent and some have close=0,
        each gets the correct truthful reason."""
        seeded = ["AAPL.US", "BODI.US", "CEV.US", "GHOST.US"]
        db = _FakeDB(seeded)

        asyncio.run(
            scheduler_service.sync_has_price_data_flags(
                db,
                include_exclusions=False,
                tickers_with_price=["AAPL.US"],
                bulk_date="2026-04-15",
                bulk_zero_close_tickers={"BODI.US", "CEV.US"},
                bulk_zero_close_data={
                    "BODI.US": {"close": 0, "adjusted_close": 0, "volume": 100},
                    "CEV.US": {"close": 0, "adjusted_close": 0, "volume": 50},
                },
            )
        )

        assert len(db.gap_free_exclusions.ops) == 3
        docs_by_ticker = {_get_excl_doc(op)["ticker"]: _get_excl_doc(op)
                          for op in db.gap_free_exclusions.ops}

        # BODI.US — in bulk with close=0
        assert docs_by_ticker["BODI.US"]["reason"] == "bulk_found_but_close_is_zero"
        assert docs_by_ticker["BODI.US"]["bulk_found"] is True
        assert docs_by_ticker["BODI.US"]["bulk_volume"] == 100

        # CEV.US — in bulk with close=0
        assert docs_by_ticker["CEV.US"]["reason"] == "bulk_found_but_close_is_zero"
        assert docs_by_ticker["CEV.US"]["bulk_found"] is True

        # GHOST.US — truly not in bulk
        assert docs_by_ticker["GHOST.US"]["reason"] == "not_in_bulk_data"
        assert docs_by_ticker["GHOST.US"]["bulk_found"] is False

    def test_bulk_found_true_never_has_not_in_bulk_reason(self):
        """Invariant: if bulk_found=True then reason != 'not_in_bulk_data'."""
        seeded = ["AAPL.US", "SOR.US", "BLMZF.US", "ABSENT.US"]
        db = _FakeDB(seeded)

        asyncio.run(
            scheduler_service.sync_has_price_data_flags(
                db,
                include_exclusions=False,
                tickers_with_price=["AAPL.US"],
                bulk_date="2026-04-15",
                bulk_zero_close_tickers={"SOR.US", "BLMZF.US"},
                bulk_zero_close_data={
                    "SOR.US": {"close": 0, "adjusted_close": 0, "volume": 0},
                    "BLMZF.US": {"close": 0, "adjusted_close": 0, "volume": 0},
                },
            )
        )

        for op in db.gap_free_exclusions.ops:
            doc = _get_excl_doc(op)
            if doc["bulk_found"] is True:
                assert doc["reason"] != "not_in_bulk_data", (
                    f"Ticker {doc['ticker']} has bulk_found=True but reason='not_in_bulk_data'"
                )

    def test_bulk_found_false_always_has_not_in_bulk_reason(self):
        """Invariant: if bulk_found=False then reason == 'not_in_bulk_data'."""
        seeded = ["AAPL.US", "SOR.US", "GHOST1.US", "GHOST2.US"]
        db = _FakeDB(seeded)

        asyncio.run(
            scheduler_service.sync_has_price_data_flags(
                db,
                include_exclusions=False,
                tickers_with_price=["AAPL.US"],
                bulk_date="2026-04-15",
                bulk_zero_close_tickers={"SOR.US"},
                bulk_zero_close_data={
                    "SOR.US": {"close": 0, "adjusted_close": 0, "volume": 0},
                },
            )
        )

        for op in db.gap_free_exclusions.ops:
            doc = _get_excl_doc(op)
            if doc["bulk_found"] is False:
                assert doc["reason"] == "not_in_bulk_data", (
                    f"Ticker {doc['ticker']} has bulk_found=False but reason='{doc['reason']}'"
                )

    def test_no_zero_close_param_defaults_to_old_behavior(self):
        """When bulk_zero_close_tickers is not passed (None), all excluded
        tickers get not_in_bulk_data — backward compatible."""
        seeded = ["AAPL.US", "NYC.US"]
        db = _FakeDB(seeded)

        asyncio.run(
            scheduler_service.sync_has_price_data_flags(
                db,
                include_exclusions=False,
                tickers_with_price=["AAPL.US"],
                bulk_date="2026-04-15",
                # No bulk_zero_close_tickers — defaults to None
            )
        )

        assert len(db.gap_free_exclusions.ops) == 1
        doc = _get_excl_doc(db.gap_free_exclusions.ops[0])
        assert doc["ticker"] == "NYC.US"
        assert doc["reason"] == "not_in_bulk_data"
        assert doc["bulk_found"] is False

    def test_include_exclusions_report_distinguishes_zero_close(self):
        """When include_exclusions=True, the text report correctly labels
        zero-close tickers as 'Close/adjusted_close missing or zero'
        instead of 'Ticker not present in bulk data'."""
        seeded = ["AAPL.US", "NEN.US", "GHOST.US"]
        db = _FakeDB(seeded)

        result = asyncio.run(
            scheduler_service.sync_has_price_data_flags(
                db,
                include_exclusions=True,
                tickers_with_price=["AAPL.US"],
                bulk_date="2026-04-15",
                bulk_zero_close_tickers={"NEN.US"},
                bulk_zero_close_data={
                    "NEN.US": {"close": 0, "adjusted_close": 0, "volume": 0},
                },
            )
        )

        exclusions_by_ticker = {e["ticker"]: e for e in result["exclusions"]}
        # NEN (in bulk with close=0) → "Close/adjusted_close missing or zero"
        assert "Close" in exclusions_by_ticker["NEN"]["reason"], (
            f"NEN should have close-related reason, got: {exclusions_by_ticker['NEN']['reason']}"
        )
        # GHOST (truly not in bulk) → "Ticker not present in bulk data"
        assert "not present" in exclusions_by_ticker["GHOST"]["reason"], (
            f"GHOST should have 'not present' reason, got: {exclusions_by_ticker['GHOST']['reason']}"
        )
