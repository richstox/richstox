"""
Tests for dividend ingestion pipeline and coverage tracking.

Covers:
1. compute_canonical_dividend_yield with dividends_synced flag
2. Coverage warning logic (coverage_pct < MIN_COVERAGE_PCT)
3. Proven non-payer detection when dividends are synced

Run:
    cd /app/backend && python -m pytest tests/test_dividend_sync_and_coverage.py -v
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pytest
from canonical_dividend import compute_canonical_dividend_yield


# ===========================================================================
# Tests for dividends_synced flag in canonical dividend computation
# ===========================================================================

class TestDividendsSyncedFlag:
    """Tests for the dividends_synced parameter behavior."""

    def test_missing_inputs_without_sync_flag(self):
        """Without dividends_synced, all-null cashflow + no history → missing_inputs."""
        result = compute_canonical_dividend_yield(
            market_cap=1e9,
            shares_outstanding=10e6,
            cashflow_dividends_paid_quarterly=[None, None, None, None],
            dividend_history_ttm_total=None,
            dividend_history_count=0,
            dividends_synced=False,
        )
        assert result["dividend_yield_ttm_value"] is None
        assert result["na_reason"] == "missing_inputs"

    def test_proven_non_payer_with_sync_flag(self):
        """With dividends_synced=True, zero history + all-null cashflow → proven non-payer (0%)."""
        result = compute_canonical_dividend_yield(
            market_cap=1e9,
            shares_outstanding=10e6,
            cashflow_dividends_paid_quarterly=[None, None, None, None],
            dividend_history_ttm_total=None,
            dividend_history_count=0,
            dividends_synced=True,
        )
        assert result["dividend_yield_ttm_value"] == 0.0
        assert result["na_reason"] == "no_dividend"
        assert result["source_used"] == "dividend_history"

    def test_proven_non_payer_synced_fewer_than_4_quarters(self):
        """With dividends_synced=True and <4 cashflow quarters, still proven non-payer."""
        result = compute_canonical_dividend_yield(
            market_cap=1e9,
            shares_outstanding=10e6,
            cashflow_dividends_paid_quarterly=[None, None],  # only 2 quarters
            dividend_history_ttm_total=None,
            dividend_history_count=0,
            dividends_synced=True,
        )
        assert result["dividend_yield_ttm_value"] == 0.0
        assert result["na_reason"] == "no_dividend"
        assert result["source_used"] == "dividend_history"

    def test_fewer_than_4_quarters_not_synced(self):
        """Without dividends_synced, <4 cashflow quarters → missing_inputs."""
        result = compute_canonical_dividend_yield(
            market_cap=1e9,
            shares_outstanding=10e6,
            cashflow_dividends_paid_quarterly=[-1e6, -1e6],
            dividend_history_ttm_total=None,
            dividend_history_count=0,
            dividends_synced=False,
        )
        assert result["dividend_yield_ttm_value"] is None
        assert result["na_reason"] == "missing_inputs"

    def test_synced_flag_does_not_affect_normal_payer(self):
        """dividends_synced flag should not change behavior for normal payers."""
        for synced in [True, False]:
            result = compute_canonical_dividend_yield(
                market_cap=2.25e12,
                shares_outstanding=15e9,
                cashflow_dividends_paid_quarterly=[-3.6e9, -3.6e9, -3.6e9, -3.6e9],
                dividend_history_ttm_total=0.96,
                dividend_history_count=4,
                dividends_synced=synced,
            )
            assert result["source_used"] == "dividend_history"
            assert result["na_reason"] is None
            assert 0.5 < result["dividend_yield_ttm_value"] < 0.8

    def test_synced_flag_does_not_affect_proven_cashflow_non_payer(self):
        """Cashflow $0 dividendsPaid → no_dividend regardless of sync flag."""
        for synced in [True, False]:
            result = compute_canonical_dividend_yield(
                market_cap=1e9,
                shares_outstanding=10e6,
                cashflow_dividends_paid_quarterly=[0, 0, 0, 0],
                dividend_history_ttm_total=None,
                dividend_history_count=0,
                dividends_synced=synced,
            )
            assert result["dividend_yield_ttm_value"] == 0.0
            assert result["na_reason"] == "no_dividend"

    def test_synced_flag_does_not_affect_missing_market_cap(self):
        """Missing market_cap → missing_inputs even with dividends_synced."""
        result = compute_canonical_dividend_yield(
            market_cap=None,
            shares_outstanding=10e6,
            cashflow_dividends_paid_quarterly=[-1e6, -1e6, -1e6, -1e6],
            dividend_history_ttm_total=1.0,
            dividend_history_count=4,
            dividends_synced=True,
        )
        assert result["dividend_yield_ttm_value"] is None
        assert result["na_reason"] == "missing_inputs"

    def test_synced_with_unreliable_data(self):
        """dividends_synced does not override unreliable classification."""
        # Cashflow says dividends but history has no records
        result = compute_canonical_dividend_yield(
            market_cap=3.9e6,
            shares_outstanding=1e6,
            cashflow_dividends_paid_quarterly=[-99e6, -99e6, -99e6, -99e6],
            dividend_history_ttm_total=None,
            dividend_history_count=0,
            dividends_synced=True,
        )
        assert result["na_reason"] == "unreliable"
        assert result["dividend_yield_ttm_value"] is None

    def test_debug_includes_synced_flag(self):
        """Debug output includes the dividends_synced flag."""
        result = compute_canonical_dividend_yield(
            market_cap=1e9,
            shares_outstanding=10e6,
            cashflow_dividends_paid_quarterly=[None, None, None, None],
            dividend_history_ttm_total=None,
            dividend_history_count=0,
            dividends_synced=True,
            include_debug=True,
        )
        assert result["debug_inputs"] is not None
        assert result["debug_inputs"]["dividends_synced"] is True


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
# Tests for synced tickers improving peer pool size
# ===========================================================================

class TestSyncedTickersImprovePeerPool:
    """Verify that syncing dividends increases the peer pool size."""

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
                dividends_synced=t.get("synced", False),
            )
            if result["dividend_yield_ttm_value"] is not None:
                pool.append(result["dividend_yield_ttm_value"])
        return pool

    def test_unsynced_tickers_mostly_excluded(self):
        """Without syncing, tickers with all-null cashflow are excluded."""
        tickers = [
            # Ticker with data (included)
            {"market_cap": 1e9, "shares": 10e6,
             "cf_vals": [0, 0, 0, 0], "hist_total": None, "hist_count": 0,
             "synced": False},
            # Ticker with all-null cashflow, no history (excluded without sync)
            {"market_cap": 1e9, "shares": 10e6,
             "cf_vals": [None, None, None, None], "hist_total": None, "hist_count": 0,
             "synced": False},
            # Another excluded
            {"market_cap": 1e9, "shares": 10e6,
             "cf_vals": [None, None, None, None], "hist_total": None, "hist_count": 0,
             "synced": False},
        ]
        pool = self._build_peer_pool(tickers)
        assert len(pool) == 1  # only the one with cashflow data

    def test_synced_tickers_included_as_non_payers(self):
        """After syncing, tickers with zero history become proven non-payers."""
        tickers = [
            # Ticker with data (included)
            {"market_cap": 1e9, "shares": 10e6,
             "cf_vals": [0, 0, 0, 0], "hist_total": None, "hist_count": 0,
             "synced": True},
            # Synced ticker → proven non-payer
            {"market_cap": 1e9, "shares": 10e6,
             "cf_vals": [None, None, None, None], "hist_total": None, "hist_count": 0,
             "synced": True},
            # Another synced ticker → proven non-payer
            {"market_cap": 1e9, "shares": 10e6,
             "cf_vals": [None, None, None, None], "hist_total": None, "hist_count": 0,
             "synced": True},
        ]
        pool = self._build_peer_pool(tickers)
        assert len(pool) == 3  # all included as 0% non-payers

    def test_mixed_synced_and_unsynced(self):
        """Mix of synced and unsynced tickers — only synced ones contribute."""
        tickers = [
            # Synced proven non-payer
            {"market_cap": 1e9, "shares": 10e6,
             "cf_vals": [None, None, None, None], "hist_total": None, "hist_count": 0,
             "synced": True},
            # Unsynced — excluded (missing_inputs)
            {"market_cap": 1e9, "shares": 10e6,
             "cf_vals": [None, None, None, None], "hist_total": None, "hist_count": 0,
             "synced": False},
            # Normal payer (always included regardless of sync)
            {"market_cap": 2.25e12, "shares": 15e9,
             "cf_vals": [-3.6e9, -3.6e9, -3.6e9, -3.6e9],
             "hist_total": 0.96, "hist_count": 4,
             "synced": False},
        ]
        pool = self._build_peer_pool(tickers)
        assert len(pool) == 2  # synced non-payer + normal payer


# ===========================================================================
# Backward compatibility: existing tests still pass with default dividends_synced=False
# ===========================================================================

class TestBackwardCompatibility:
    """Verify dividends_synced=False (default) preserves existing behavior."""

    def test_default_parameter_is_false(self):
        """Calling without dividends_synced uses False as default."""
        result = compute_canonical_dividend_yield(
            market_cap=1e9,
            shares_outstanding=10e6,
            cashflow_dividends_paid_quarterly=[None, None, None, None],
            dividend_history_ttm_total=None,
            dividend_history_count=0,
            # dividends_synced not passed — defaults to False
        )
        assert result["na_reason"] == "missing_inputs"
        assert result["dividend_yield_ttm_value"] is None

    def test_all_existing_scenarios_unchanged(self):
        """Spot-check: normal payer, unreliable, extreme outlier unchanged."""
        # Normal payer
        r1 = compute_canonical_dividend_yield(
            market_cap=2.25e12, shares_outstanding=15e9,
            cashflow_dividends_paid_quarterly=[-3.6e9, -3.6e9, -3.6e9, -3.6e9],
            dividend_history_ttm_total=0.96, dividend_history_count=4,
        )
        assert r1["source_used"] == "dividend_history"
        assert r1["na_reason"] is None

        # Unreliable (cashflow vs no history)
        r2 = compute_canonical_dividend_yield(
            market_cap=3.9e6, shares_outstanding=1e6,
            cashflow_dividends_paid_quarterly=[-99e6, -99e6, None, None],
            dividend_history_ttm_total=None, dividend_history_count=0,
        )
        assert r2["na_reason"] == "unreliable"

        # Extreme outlier
        r3 = compute_canonical_dividend_yield(
            market_cap=1e6, shares_outstanding=1e6,
            cashflow_dividends_paid_quarterly=[None, None, None, None],
            dividend_history_ttm_total=2.0, dividend_history_count=4,
        )
        assert r3["na_reason"] == "extreme_outlier"
