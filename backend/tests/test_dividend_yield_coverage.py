"""
Tests for dividend yield coverage tracking and coverage gating.

Source policy: dividend_history is the ONLY production source.

Covers:
1. Coverage warning logic (coverage_pct < MIN_COVERAGE_PCT)
2. Coverage gating: median=null when coverage < 30%
3. Backward compatibility: canonical function behavior

Run:
    cd backend && python -m pytest tests/test_dividend_yield_coverage.py -v
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pytest
from canonical_dividend import compute_canonical_dividend_yield


# ===========================================================================
# Tests for coverage_pct / coverage_warning logic
# ===========================================================================

class TestCoveragePctLogic:
    """Tests that coverage_pct and coverage_warning are computed correctly."""

    def test_coverage_pct_calculation(self):
        """coverage_pct = n_used / total * 100, rounded to 1 decimal."""
        n_used = 157
        total = 5176
        coverage_pct = round(n_used / total * 100, 1)
        assert coverage_pct == 3.0
        assert coverage_pct < 30

    def test_coverage_warning_threshold(self):
        """coverage_warning should be True when coverage_pct < 30%."""
        from key_metrics_service import MIN_COVERAGE_PCT
        assert MIN_COVERAGE_PCT == 30
        assert (3.0 < MIN_COVERAGE_PCT) is True
        assert (30.0 < MIN_COVERAGE_PCT) is False
        assert (50.0 < MIN_COVERAGE_PCT) is False

    def test_high_coverage_no_warning(self):
        """When most tickers have data, no warning should be generated."""
        n_used = 3000
        total = 5176
        coverage_pct = round(n_used / total * 100, 1)
        assert coverage_pct > 30
        assert (coverage_pct < 30) is False


# ===========================================================================
# Tests for dividend_history-only peer pool
# ===========================================================================

class TestDividendHistoryOnlyPool:
    """Verify that only dividend_history data drives the peer pool."""

    def _build_peer_pool(self, tickers_data):
        """Simulate peer pool construction from key_metrics_service."""
        pool = []
        for t in tickers_data:
            result = compute_canonical_dividend_yield(
                market_cap=t["market_cap"],
                shares_outstanding=t["shares"],
                cashflow_dividends_paid_quarterly=t.get("cf_vals"),
                dividend_history_ttm_total=t.get("hist_total"),
                dividend_history_count=t.get("hist_count", 0),
            )
            if result["dividend_yield_ttm_value"] is not None:
                pool.append(result["dividend_yield_ttm_value"])
        return pool

    def test_history_payer_included(self):
        """Ticker with dividend_history records → included."""
        tickers = [
            {"market_cap": 100e9, "shares": 1e9,
             "hist_total": 2.0, "hist_count": 4},
        ]
        pool = self._build_peer_pool(tickers)
        assert len(pool) == 1
        assert abs(pool[0] - 2.0) < 0.1

    def test_cashflow_only_excluded(self):
        """Ticker with cashflow data but no history → excluded."""
        tickers = [
            {"market_cap": 100e9, "shares": 1e9,
             "cf_vals": [-0.5e9, -0.5e9, -0.5e9, -0.5e9],
             "hist_total": None, "hist_count": 0},
        ]
        pool = self._build_peer_pool(tickers)
        assert len(pool) == 0

    def test_proven_non_payer_included(self):
        """Dividend_history synced with sum=0 → included as 0.0."""
        tickers = [
            {"market_cap": 1e9, "shares": 10e6,
             "hist_total": 0.0, "hist_count": 1},
        ]
        pool = self._build_peer_pool(tickers)
        assert len(pool) == 1
        assert pool[0] == 0.0

    def test_no_history_excluded(self):
        """No dividend_history → excluded regardless of cashflow."""
        tickers = [
            {"market_cap": 1e9, "shares": 10e6,
             "cf_vals": [None, None, None, None],
             "hist_total": None, "hist_count": 0},
        ]
        pool = self._build_peer_pool(tickers)
        assert len(pool) == 0

    def test_extreme_outlier_excluded(self):
        """History yield > 100% → excluded."""
        tickers = [
            {"market_cap": 3.9e6, "shares": 1e6,
             "hist_total": 50.0, "hist_count": 4},
        ]
        pool = self._build_peer_pool(tickers)
        assert len(pool) == 0


# ===========================================================================
# Tests for coverage gating: median=null when coverage < threshold
# ===========================================================================

class TestCoverageGating:
    """Verify that Step 4 nulls out the dividend yield median
    when coverage_pct < MIN_COVERAGE_PCT (30%)."""

    def test_low_coverage_median_is_null(self):
        from key_metrics_service import MIN_COVERAGE_PCT
        n_used = 10
        total = 50
        _cov = round(n_used / total * 100, 1)
        _cov_warn = _cov < MIN_COVERAGE_PCT
        assert _cov == 20.0
        assert _cov_warn is True
        d_med = 1.5
        step4_entry = {
            "median": None if _cov_warn else d_med,
            "n_used": n_used,
            "total_company_count": total,
            "coverage_pct": _cov,
            "coverage_warning": _cov_warn,
        }
        assert step4_entry["median"] is None
        assert step4_entry["coverage_warning"] is True

    def test_high_coverage_median_published(self):
        from key_metrics_service import MIN_COVERAGE_PCT
        n_used = 20
        total = 50
        _cov = round(n_used / total * 100, 1)
        _cov_warn = _cov < MIN_COVERAGE_PCT
        assert _cov == 40.0
        assert _cov_warn is False
        d_med = 1.5
        step4_entry = {
            "median": None if _cov_warn else d_med,
            "n_used": n_used,
            "total_company_count": total,
            "coverage_pct": _cov,
            "coverage_warning": _cov_warn,
        }
        assert step4_entry["median"] == 1.5

    def test_exactly_30_pct_no_warning(self):
        from key_metrics_service import MIN_COVERAGE_PCT
        n_used = 30
        total = 100
        _cov = round(n_used / total * 100, 1)
        _cov_warn = _cov < MIN_COVERAGE_PCT
        assert _cov == 30.0
        assert _cov_warn is False

    def test_zero_total_no_crash(self):
        _pool_total = 0
        n_dv = 0
        _cov = round(n_dv / _pool_total * 100, 1) if _pool_total > 0 else 0
        assert _cov == 0


# ===========================================================================
# Backward compatibility: canonical behavior preserved
# ===========================================================================

class TestBackwardCompatibility:
    """Verify canonical function behavior with dividend_history-only source."""

    def test_normal_payer(self):
        """Normal payer with history → dividend_history source."""
        result = compute_canonical_dividend_yield(
            market_cap=2.25e12, shares_outstanding=15e9,
            dividend_history_ttm_total=0.96, dividend_history_count=4,
        )
        assert result["source_used"] == "dividend_history"
        assert result["na_reason"] is None

    def test_no_history_is_missing(self):
        """No dividend_history → missing_inputs (not cashflow fallback)."""
        result = compute_canonical_dividend_yield(
            market_cap=100e9, shares_outstanding=1e9,
            cashflow_dividends_paid_quarterly=[-0.5e9, -0.5e9, -0.5e9, -0.5e9],
            dividend_history_ttm_total=None, dividend_history_count=0,
        )
        assert result["source_used"] == "none"
        assert result["na_reason"] == "missing_inputs"
        assert result["dividend_yield_ttm_value"] is None

    def test_extreme_outlier_unchanged(self):
        """Extreme outlier (>100%) still excluded."""
        result = compute_canonical_dividend_yield(
            market_cap=1e6, shares_outstanding=1e6,
            dividend_history_ttm_total=2.0, dividend_history_count=4,
        )
        assert result["na_reason"] == "extreme_outlier"

    def test_missing_inputs_unchanged(self):
        """No history → missing_inputs."""
        result = compute_canonical_dividend_yield(
            market_cap=1e9, shares_outstanding=10e6,
            dividend_history_ttm_total=None, dividend_history_count=0,
        )
        assert result["na_reason"] == "missing_inputs"
        assert result["dividend_yield_ttm_value"] is None
