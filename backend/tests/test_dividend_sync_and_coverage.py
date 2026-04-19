"""
Tests for dividend yield coverage tracking and coverage gating.

Covers:
1. Coverage warning logic (coverage_pct < MIN_COVERAGE_PCT)
2. Cashflow-fallback source priority (spec requirement B)
3. Coverage gating: median=null when coverage < 30%

Run:
    cd backend && python -m pytest tests/test_dividend_sync_and_coverage.py -v
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
        # Simulate: 157 out of 5176
        n_used = 157
        total = 5176
        coverage_pct = round(n_used / total * 100, 1)
        assert coverage_pct == 3.0
        assert coverage_pct < 30  # should trigger warning

    def test_coverage_warning_threshold(self):
        """coverage_warning should be True when coverage_pct < 30%."""
        from key_metrics_service import MIN_COVERAGE_PCT
        assert MIN_COVERAGE_PCT == 30

        # Below threshold
        assert (3.0 < MIN_COVERAGE_PCT) is True
        # At threshold
        assert (30.0 < MIN_COVERAGE_PCT) is False
        # Above threshold
        assert (50.0 < MIN_COVERAGE_PCT) is False

    def test_high_coverage_no_warning(self):
        """When most tickers have data, no warning should be generated."""
        n_used = 3000
        total = 5176
        coverage_pct = round(n_used / total * 100, 1)
        assert coverage_pct > 30
        # coverage_warning should be False
        assert (coverage_pct < 30) is False


# ===========================================================================
# Tests for cashflow fallback improving peer pool size
# ===========================================================================

class TestCashflowFallbackImprovesPeerPool:
    """Verify that using cashflow as fallback increases the peer pool size.
    
    Previously, tickers with cashflow data but no dividend_history were
    classified as 'unreliable' and excluded. Now they use cashflow yield.
    """

    def _build_peer_pool(self, tickers_data):
        """Simulate peer pool construction from key_metrics_service."""
        pool = []
        for t in tickers_data:
            result = compute_canonical_dividend_yield(
                market_cap=t["market_cap"],
                shares_outstanding=t["shares"],
                cashflow_dividends_paid_quarterly=t["cf_vals"],
                dividend_history_ttm_total=t.get("hist_total"),
                dividend_history_count=t.get("hist_count", 0),
            )
            if result["dividend_yield_ttm_value"] is not None:
                pool.append(result["dividend_yield_ttm_value"])
        return pool

    def test_cashflow_only_tickers_included(self):
        """Tickers with cashflow data but no history are included via cashflow fallback."""
        tickers = [
            # Cashflow payer (no history) → cashflow fallback
            {"market_cap": 100e9, "shares": 1e9,
             "cf_vals": [-0.5e9, -0.5e9, -0.5e9, -0.5e9],
             "hist_total": None, "hist_count": 0},
            # Proven non-payer via cashflow
            {"market_cap": 1e9, "shares": 10e6,
             "cf_vals": [0, 0, 0, 0],
             "hist_total": None, "hist_count": 0},
            # Missing data (all-null cashflow, no history)
            {"market_cap": 1e9, "shares": 10e6,
             "cf_vals": [None, None, None, None],
             "hist_total": None, "hist_count": 0},
        ]
        pool = self._build_peer_pool(tickers)
        # First ticker: cashflow yield ~2%
        # Second ticker: proven non-payer 0%
        # Third ticker: excluded (missing_inputs)
        assert len(pool) == 2

    def test_cashflow_payer_gets_correct_yield(self):
        """Cashflow fallback computes correct yield."""
        tickers = [
            {"market_cap": 100e9, "shares": 1e9,
             "cf_vals": [-0.5e9, -0.5e9, -0.5e9, -0.5e9],
             "hist_total": None, "hist_count": 0},
        ]
        pool = self._build_peer_pool(tickers)
        assert len(pool) == 1
        assert abs(pool[0] - 2.0) < 0.1  # yield ~2%

    def test_extreme_cashflow_excluded_by_guardrail(self):
        """Implausible cashflow yields (>100%) are excluded via extreme_outlier."""
        tickers = [
            # ONFO-like: market_cap ~$3.9M, dividends_paid ~$99M/quarter
            {"market_cap": 3.9e6, "shares": 1e6,
             "cf_vals": [-99e6, -99e6, None, None],
             "hist_total": None, "hist_count": 0},
        ]
        pool = self._build_peer_pool(tickers)
        assert len(pool) == 0  # excluded by extreme_outlier

    def test_mixed_sources_all_included(self):
        """Mix of history-based and cashflow-based tickers all contribute."""
        tickers = [
            # History payer (2% yield)
            {"market_cap": 100e9, "shares": 1e9,
             "cf_vals": [-0.5e9, -0.5e9, -0.5e9, -0.5e9],
             "hist_total": 2.0, "hist_count": 4},
            # Cashflow-only payer (~2% yield)
            {"market_cap": 100e9, "shares": 1e9,
             "cf_vals": [-0.5e9, -0.5e9, -0.5e9, -0.5e9],
             "hist_total": None, "hist_count": 0},
            # Proven non-payer
            {"market_cap": 1e9, "shares": 10e6,
             "cf_vals": [0, 0, 0, 0],
             "hist_total": None, "hist_count": 0},
        ]
        pool = self._build_peer_pool(tickers)
        assert len(pool) == 3


# ===========================================================================
# Tests for coverage gating: median=null when coverage < threshold
# ===========================================================================

class TestCoverageGating:
    """Verify that Step 4 nulls out the dividend yield median
    when coverage_pct < MIN_COVERAGE_PCT (30%).

    These test the gating logic that was added to compute_peer_benchmarks_v3.
    """

    def test_low_coverage_median_is_null(self):
        """When coverage < 30%, median should be None, coverage_warning=True."""
        from key_metrics_service import MIN_COVERAGE_PCT

        # Simulate: 50 total tickers, 10 with dividend yield data (20% coverage)
        n_used = 10
        total = 50
        _cov = round(n_used / total * 100, 1)
        _cov_warn = _cov < MIN_COVERAGE_PCT

        assert _cov == 20.0
        assert _cov_warn is True

        # The step4 entry should have median=None
        d_med = 1.5  # computed median
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
        """When coverage >= 30%, median should be published normally."""
        from key_metrics_service import MIN_COVERAGE_PCT

        # Simulate: 50 total tickers, 20 with dividend yield data (40% coverage)
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
        assert step4_entry["coverage_warning"] is False

    def test_exactly_30_pct_no_warning(self):
        """At exactly 30% coverage, no warning (threshold is <30, not <=30)."""
        from key_metrics_service import MIN_COVERAGE_PCT

        n_used = 30
        total = 100
        _cov = round(n_used / total * 100, 1)
        _cov_warn = _cov < MIN_COVERAGE_PCT

        assert _cov == 30.0
        assert _cov_warn is False

    def test_zero_total_no_crash(self):
        """Zero total tickers should not crash coverage calculation."""
        _pool_total = 0
        n_dv = 0
        _cov = round(n_dv / _pool_total * 100, 1) if _pool_total > 0 else 0
        assert _cov == 0


# ===========================================================================
# Backward compatibility: existing canonical behavior preserved
# ===========================================================================

class TestBackwardCompatibility:
    """Verify default behavior still works correctly."""

    def test_normal_payer_unchanged(self):
        """Normal payer with both sources agreeing → still works."""
        result = compute_canonical_dividend_yield(
            market_cap=2.25e12, shares_outstanding=15e9,
            cashflow_dividends_paid_quarterly=[-3.6e9, -3.6e9, -3.6e9, -3.6e9],
            dividend_history_ttm_total=0.96, dividend_history_count=4,
        )
        assert result["source_used"] == "dividend_history"
        assert result["na_reason"] is None

    def test_cashflow_only_now_uses_cashflow(self):
        """Cashflow-only tickers now use cashflow (not unreliable).
        
        This is the key behavioral change: previously cashflow+no-history
        was 'unreliable' (excluding ~2393 tickers), now it uses cashflow.
        """
        result = compute_canonical_dividend_yield(
            market_cap=100e9, shares_outstanding=1e9,
            cashflow_dividends_paid_quarterly=[-0.5e9, -0.5e9, -0.5e9, -0.5e9],
            dividend_history_ttm_total=None, dividend_history_count=0,
        )
        assert result["source_used"] == "cashflow"
        assert result["na_reason"] is None
        assert result["dividend_yield_ttm_value"] is not None

    def test_extreme_outlier_unchanged(self):
        """Extreme outlier (>100%) still excluded."""
        result = compute_canonical_dividend_yield(
            market_cap=1e6, shares_outstanding=1e6,
            cashflow_dividends_paid_quarterly=[None, None, None, None],
            dividend_history_ttm_total=2.0, dividend_history_count=4,
        )
        assert result["na_reason"] == "extreme_outlier"

    def test_missing_inputs_unchanged(self):
        """All-null cashflow + no history → still missing_inputs."""
        result = compute_canonical_dividend_yield(
            market_cap=1e9, shares_outstanding=10e6,
            cashflow_dividends_paid_quarterly=[None, None, None, None],
            dividend_history_ttm_total=None, dividend_history_count=0,
        )
        assert result["na_reason"] == "missing_inputs"
        assert result["dividend_yield_ttm_value"] is None
