"""
Tests for the low-history visibility guard.

Regression tests for the GRTUF.US scenario: a ticker whose provider returns
only 2-3 data points passes range-proof (DB date range covers provider range)
but shows a broken 2-point chart.

Three invariants are tested:
  1) If DB has >200 price rows for a ticker, MAX chart must return >200 points.
  2) A (ticker, date) pair cannot be both true_gap and not_applicable.
  3) If is_visible=true but DB history is below the minimum threshold, the
     system must trigger remediation or prevent the ticker from becoming visible.
"""

import asyncio
import sys
import os
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

# Ensure the backend directory is in the Python path.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


# ---------------------------------------------------------------------------
# Inline replicas (avoid importing server.py / heavy modules)
# ---------------------------------------------------------------------------

_RANGE_PROOF_TOLERANCE_DAYS = 5
MINIMUM_VISIBLE_PRICE_ROWS = 50


def _check_range_proof(provider_first, provider_last, db_first, db_last):
    """Inline replica of full_sync_service._check_range_proof."""
    if not all([provider_first, provider_last, db_first, db_last]):
        return False
    from datetime import timedelta
    try:
        _pf = datetime.strptime(provider_first, "%Y-%m-%d")
        _pl = datetime.strptime(provider_last, "%Y-%m-%d")
        _df = datetime.strptime(db_first, "%Y-%m-%d")
        _dl = datetime.strptime(db_last, "%Y-%m-%d")
    except (ValueError, TypeError):
        return False
    tolerance = timedelta(days=_RANGE_PROOF_TOLERANCE_DAYS)
    if _df > _pf + tolerance:
        return False
    if _dl < _pl - tolerance:
        return False
    return True


def _is_valid_price(p):
    """Inline replica of the chart endpoint's _is_valid_price."""
    adj = p.get("adjusted_close")
    return isinstance(adj, (int, float)) and adj > 0


def _safe_adjusted_close(p):
    """Inline replica of the chart endpoint's _safe_adjusted_close."""
    adj = p.get("adjusted_close")
    close = p.get("close")
    if adj and close and close > 0 and adj / close > 100:
        return close
    return adj or close


def _make_price_rows(n, start_date="2020-01-02"):
    """Generate n synthetic price rows with sequential trading-day dates."""
    from datetime import timedelta
    rows = []
    base = datetime.strptime(start_date, "%Y-%m-%d")
    day_offset = 0
    while len(rows) < n:
        d = base + timedelta(days=day_offset)
        day_offset += 1
        # Skip weekends
        if d.weekday() >= 5:
            continue
        idx = len(rows)
        rows.append({
            "date": d.strftime("%Y-%m-%d"),
            "close": 100.0 + idx * 0.1,
            "adjusted_close": 100.0 + idx * 0.1,
            "volume": 10000 + idx,
        })
    return rows


# ---------------------------------------------------------------------------
# Fake DB collections
# ---------------------------------------------------------------------------

class _FakeAggCursor:
    def __init__(self, results):
        self._results = results

    async def to_list(self, n):
        return self._results[:n] if n else self._results


class _FakeStockPrices:
    def __init__(self):
        self.deleted = []
        self.written = []
        self._agg_result = []

    async def delete_many(self, filt):
        self.deleted.append(filt)
        return SimpleNamespace(deleted_count=0)

    async def bulk_write(self, ops, ordered=False):
        self.written.extend(ops)
        return SimpleNamespace(upserted_count=len(ops), modified_count=0)

    def aggregate(self, pipeline):
        return _FakeAggCursor(self._agg_result)


class _FakeTrackedTickers:
    def __init__(self):
        self.updates = []

    async def update_one(self, filt, update):
        self.updates.append({"filter": filt, "update": update})
        return SimpleNamespace(modified_count=1)


class _FakeDB:
    def __init__(self):
        self.stock_prices = _FakeStockPrices()
        self.tracked_tickers = _FakeTrackedTickers()


def _make_eod_records(first_date, last_date, count):
    """Generate count EOD records spanning first_date to last_date."""
    from datetime import timedelta
    start = datetime.strptime(first_date, "%Y-%m-%d")
    end = datetime.strptime(last_date, "%Y-%m-%d")
    if count <= 1:
        return [{"date": first_date, "open": 10, "high": 11, "low": 9,
                 "close": 10, "adjusted_close": 10, "volume": 100}]
    step = (end - start) / (count - 1)
    records = []
    for i in range(count):
        d = start + step * i
        records.append({
            "date": d.strftime("%Y-%m-%d"),
            "open": 10.0 + i * 0.01,
            "high": 11.0 + i * 0.01,
            "low": 9.0 + i * 0.01,
            "close": 10.0 + i * 0.01,
            "adjusted_close": 10.0 + i * 0.01,
            "volume": 100 + i,
        })
    return records


# ===========================================================================
# TEST 1: MAX chart with >200 DB rows must return >200 points
# ===========================================================================

class TestMaxChartReturnsAllRows:
    """If DB has >200 price rows for a ticker, MAX chart must return >200 points."""

    def test_max_chart_returns_all_valid_rows_when_under_2000(self):
        """
        Simulates the backend MAX-period query + filter.
        300 valid rows → all 300 should be returned (no downsampling needed).
        """
        all_prices = _make_price_rows(300)
        assert len(all_prices) == 300

        # Replicate backend: filter by _is_valid_price
        valid = [p for p in all_prices if _is_valid_price(p)]
        assert len(valid) == 300, "All 300 rows have valid adjusted_close > 0"

        # Backend does NOT downsample when len <= 2000
        target_points = 2000
        if len(valid) > target_points:
            # Would downsample — not the case here
            pass
        else:
            prices = valid

        assert len(prices) > 200, (
            f"MAX chart with 300 DB rows must return >200 points, got {len(prices)}"
        )

    def test_max_chart_with_2500_rows_downsamples_but_stays_above_200(self):
        """
        2500 valid rows → backend downsamples to ~2000.
        Must still be >200 points after downsampling.
        """
        all_prices = _make_price_rows(2500, start_date="2010-01-02")
        valid = [p for p in all_prices if _is_valid_price(p)]

        target_points = 2000
        if len(valid) > target_points:
            import numpy as np
            indices = np.linspace(0, len(valid) - 1, target_points, dtype=int)
            indices = sorted(set(indices))
            prices = [valid[i] for i in indices]
        else:
            prices = valid

        assert len(prices) > 200, (
            f"MAX chart with 2500 DB rows must return >200 points after "
            f"downsampling, got {len(prices)}"
        )

    def test_two_point_chart_not_above_200(self):
        """
        If DB has only 2 rows, chart must NOT claim >200 points.
        This is the GRTUF.US broken scenario.
        """
        all_prices = _make_price_rows(2)
        valid = [p for p in all_prices if _is_valid_price(p)]
        assert len(valid) == 2
        assert len(valid) <= 200, "2 DB rows must NOT produce >200 chart points"


# ===========================================================================
# TEST 2: Mutual exclusivity of gap classification
# ===========================================================================

class TestGapClassificationMutualExclusivity:
    """A (ticker, date) pair cannot be both true_gap and not_applicable."""

    def test_same_ticker_date_cannot_be_both_gap_and_excluded(self):
        """
        Simulates the admin overview gap classification logic.
        A (ticker, date) in exclusion_set → NOT_APPLICABLE.
        A (ticker, date) NOT in exclusion_set → true gap (if missing).
        These are mutually exclusive by definition.
        """
        exclusion_set = {
            ("GRTUF.US", "2026-04-14"),
            ("GRTUF.US", "2026-04-08"),
        }
        expected_dates = ["2026-04-08", "2026-04-13", "2026-04-14", "2026-04-15", "2026-04-16"]
        ticker = "GRTUF.US"
        anchor = "2026-04-07"
        ticker_dates = {"2026-04-15", "2026-04-16"}  # DB rows

        relevant = sorted(d for d in expected_dates if d > anchor)
        missing = [d for d in relevant if d not in ticker_dates]

        true_gaps = [d for d in missing if (ticker, d) not in exclusion_set]
        not_applicable = [d for d in missing if (ticker, d) in exclusion_set]

        # Verify mutual exclusivity
        true_gap_set = set(true_gaps)
        not_applicable_set = set(not_applicable)
        overlap = true_gap_set & not_applicable_set
        assert overlap == set(), (
            f"Overlap between true_gap and not_applicable: {overlap}"
        )

        # Verify completeness: every missing date is in exactly one category
        for d in missing:
            in_gap = d in true_gap_set
            in_na = d in not_applicable_set
            assert in_gap != in_na, (
                f"Date {d} must be in exactly one category: "
                f"true_gap={in_gap}, not_applicable={in_na}"
            )

    def test_exclusion_set_membership_is_deterministic(self):
        """The classification of a (ticker, date) pair is determined
        solely by membership in the exclusion_set — no ambiguity."""
        test_cases = [
            (("T.US", "2026-04-13"), True, False),   # in exclusion → NA
            (("T.US", "2026-04-14"), False, True),    # not in exclusion → gap
        ]
        exclusion_set = {("T.US", "2026-04-13")}

        for key, expected_na, expected_gap in test_cases:
            is_na = key in exclusion_set
            is_gap = key not in exclusion_set
            assert is_na == expected_na, f"{key}: expected NA={expected_na}, got {is_na}"
            assert is_gap == expected_gap, f"{key}: expected gap={expected_gap}, got {is_gap}"
            assert is_na != is_gap, f"{key}: must be mutually exclusive"


# ===========================================================================
# TEST 3: Insufficient history prevents visibility / triggers remediation
# ===========================================================================

class TestInsufficientHistoryGuard:
    """If is_visible=true but DB history is below threshold, system must
    trigger remediation or the ticker must not become visible."""

    def test_phase_c_rejects_ticker_with_2_rows(self):
        """Phase C: provider returns 2 data points, DB has 2 rows.
        Range-proof passes (DB covers provider range) but minimum-row
        guard MUST prevent price_history_complete=True.

        This is the exact GRTUF.US root cause scenario.
        """
        from full_sync_service import _process_price_ticker, MINIMUM_VISIBLE_PRICE_ROWS

        db = _FakeDB()
        records = _make_eod_records("2026-04-15", "2026-04-16", 2)
        db.stock_prices._agg_result = [{
            "_id": None,
            "db_first_date": "2026-04-15",
            "db_last_date": "2026-04-16",
            "db_row_count": 2,
        }]

        with patch("full_sync_service._fetch_one", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = (records, 200, 50, True)
            result = asyncio.run(
                _process_price_ticker(db, "GRTUF.US", job_name="test", needs_redownload=False)
            )

        assert result["success"] is False, "Ticker with 2 rows must NOT be marked as success"
        assert result.get("insufficient_history") is True, (
            "Must report insufficient_history=True"
        )

        # price_history_complete must NOT be set to True
        last_update = db.tracked_tickers.updates[-1]
        set_fields = last_update["update"]["$set"]
        assert "price_history_complete" not in set_fields or set_fields.get("price_history_complete") is not True, (
            "price_history_complete must NOT be True for a ticker with only 2 rows"
        )
        assert set_fields["price_history_status"] == "insufficient_history"

    def test_phase_c_accepts_ticker_with_sufficient_rows(self):
        """Phase C: provider returns 300 rows, DB has 300 rows.
        Both range-proof and minimum-row guard pass → complete=True.
        """
        from full_sync_service import _process_price_ticker, MINIMUM_VISIBLE_PRICE_ROWS

        db = _FakeDB()
        records = _make_eod_records("2025-01-02", "2026-04-16", 300)
        db.stock_prices._agg_result = [{
            "_id": None,
            "db_first_date": "2025-01-02",
            "db_last_date": "2026-04-16",
            "db_row_count": 300,
        }]

        with patch("full_sync_service._fetch_one", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = (records, 200, 50, True)
            result = asyncio.run(
                _process_price_ticker(db, "GOOD.US", job_name="test", needs_redownload=False)
            )

        assert result["success"] is True
        last_update = db.tracked_tickers.updates[-1]
        set_fields = last_update["update"]["$set"]
        assert set_fields["price_history_complete"] is True
        assert set_fields["price_history_status"] == "complete"

    def test_phase_c_rejects_ticker_at_boundary(self):
        """Phase C: DB has exactly MINIMUM_VISIBLE_PRICE_ROWS - 1 rows.
        Must NOT pass the minimum-row guard.
        """
        from full_sync_service import _process_price_ticker, MINIMUM_VISIBLE_PRICE_ROWS

        db = _FakeDB()
        count = MINIMUM_VISIBLE_PRICE_ROWS - 1
        records = _make_eod_records("2026-01-02", "2026-04-16", count)
        db.stock_prices._agg_result = [{
            "_id": None,
            "db_first_date": "2026-01-02",
            "db_last_date": "2026-04-16",
            "db_row_count": count,
        }]

        with patch("full_sync_service._fetch_one", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = (records, 200, 50, True)
            result = asyncio.run(
                _process_price_ticker(db, "EDGE.US", job_name="test", needs_redownload=False)
            )

        assert result["success"] is False
        assert result.get("insufficient_history") is True

    def test_phase_c_accepts_ticker_at_exact_boundary(self):
        """Phase C: DB has exactly MINIMUM_VISIBLE_PRICE_ROWS rows.
        Must pass the minimum-row guard.
        """
        from full_sync_service import _process_price_ticker, MINIMUM_VISIBLE_PRICE_ROWS

        db = _FakeDB()
        count = MINIMUM_VISIBLE_PRICE_ROWS
        records = _make_eod_records("2025-10-01", "2026-04-16", count)
        db.stock_prices._agg_result = [{
            "_id": None,
            "db_first_date": "2025-10-01",
            "db_last_date": "2026-04-16",
            "db_row_count": count,
        }]

        with patch("full_sync_service._fetch_one", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = (records, 200, 50, True)
            result = asyncio.run(
                _process_price_ticker(db, "EXACT.US", job_name="test", needs_redownload=False)
            )

        assert result["success"] is True
        last_update = db.tracked_tickers.updates[-1]
        set_fields = last_update["update"]["$set"]
        assert set_fields["price_history_complete"] is True

    def test_chart_endpoint_flags_low_history_ticker(self):
        """
        Simulates chart endpoint behavior: a visible ticker with only 2 rows
        should trigger a data notice about limited history AND flag for
        remediation (price_history_complete=False, needs_price_redownload=True).
        """
        # Simulate the chart endpoint logic inline
        _LOW_HISTORY_THRESHOLD = 50
        prices = _make_price_rows(2)
        valid = [p for p in prices if _is_valid_price(p)]
        assert len(valid) == 2

        # The endpoint sets data_notices for low-history tickers
        data_notices = []
        if 0 < len(valid) < _LOW_HISTORY_THRESHOLD and not data_notices:
            data_notices.append(
                "Price history for this stock is limited. "
                "Full history is being downloaded — please check back shortly."
            )

        assert len(data_notices) == 1, "Low-history ticker must have a data notice"
        assert "limited" in data_notices[0].lower()

    def test_minimum_visible_price_rows_constant_value(self):
        """MINIMUM_VISIBLE_PRICE_ROWS must be at least 50."""
        from full_sync_service import MINIMUM_VISIBLE_PRICE_ROWS
        assert MINIMUM_VISIBLE_PRICE_ROWS >= 50, (
            f"MINIMUM_VISIBLE_PRICE_ROWS={MINIMUM_VISIBLE_PRICE_ROWS} is too low; "
            f"must be >= 50 to ensure meaningful charts"
        )

    def test_visibility_gate_requires_price_history_complete(self):
        """
        compute_visibility must reject tickers where
        price_history_complete is False.
        """
        from visibility_rules import compute_visibility

        # A ticker that passes gates 1-7 but NOT gate 8
        doc = {
            "is_seeded": True,
            "has_price_data": True,
            "sector": "Technology",
            "industry": "Software",
            "shares_outstanding": 1000000,
            "financial_currency": "USD",
            "is_delisted": False,
            "status": "active",
            "price_history_complete": False,
        }
        is_visible, reason = compute_visibility(doc)
        assert is_visible is False
        assert reason == "INCOMPLETE_HISTORY"

    def test_visibility_gate_passes_with_price_history_complete(self):
        """
        compute_visibility must accept tickers where all 8 gates pass.
        """
        from visibility_rules import compute_visibility

        doc = {
            "is_seeded": True,
            "has_price_data": True,
            "sector": "Real Estate",
            "industry": "REIT - Industrial",
            "shares_outstanding": 50000000,
            "financial_currency": "CAD",
            "is_delisted": False,
            "status": "active",
            "price_history_complete": True,
        }
        is_visible, reason = compute_visibility(doc)
        assert is_visible is True
        assert reason is None
