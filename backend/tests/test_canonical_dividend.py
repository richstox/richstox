"""
Unit tests for canonical_dividend.compute_canonical_dividend_yield.

Source policy: dividend_history is the ONLY production source.
Cashflow data is NEVER used for production yield computation.

EODHD dividend events → dividend_history → sum by period → yield from price.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pytest
from canonical_dividend import compute_canonical_dividend_yield


class TestNormalDividendPayer:
    """Normal case: ticker with dividend_history records."""

    def test_dividend_history_computes_yield(self):
        """AAPL-like: 4 dividend payments in last year."""
        # price = 2.25T / 15B = $150, hist_total = $0.96/share
        # yield = 0.96 / 150 * 100 = 0.64%
        result = compute_canonical_dividend_yield(
            market_cap=2.25e12,
            shares_outstanding=15e9,
            dividend_history_ttm_total=0.96,
            dividend_history_count=4,
        )
        assert result["source_used"] == "dividend_history"
        assert result["na_reason"] is None
        assert result["dividend_yield_ttm_value"] is not None
        assert 0.5 < result["dividend_yield_ttm_value"] < 0.8

    def test_include_debug_shows_cashflow_diagnostic(self):
        """debug_inputs includes cashflow data for diagnostics only."""
        result = compute_canonical_dividend_yield(
            market_cap=2.25e12,
            shares_outstanding=15e9,
            cashflow_dividends_paid_quarterly=[-3.6e9, -3.6e9, -3.6e9, -3.6e9],
            dividend_history_ttm_total=0.96,
            dividend_history_count=4,
            include_debug=True,
        )
        assert result["debug_inputs"] is not None
        assert "market_cap" in result["debug_inputs"]
        assert "dividend_history_count" in result["debug_inputs"]
        # Cashflow is diagnostic only
        assert "cashflow_yield_pct_diagnostic" in result["debug_inputs"]

    def test_cashflow_ignored_for_yield(self):
        """Even with conflicting cashflow, dividend_history is always used."""
        # Cashflow says 5x more dividends than history — doesn't matter
        result = compute_canonical_dividend_yield(
            market_cap=100e9,
            shares_outstanding=1e9,
            cashflow_dividends_paid_quarterly=[-5e9, -5e9, -5e9, -5e9],
            dividend_history_ttm_total=2.0,
            dividend_history_count=4,
        )
        assert result["source_used"] == "dividend_history"
        assert result["na_reason"] is None
        assert abs(result["dividend_yield_ttm_value"] - 2.0) < 0.01

    def test_cashflow_zero_ignored_when_history_exists(self):
        """Cashflow says $0 but history has payments — history wins."""
        result = compute_canonical_dividend_yield(
            market_cap=100e9,
            shares_outstanding=1e9,
            cashflow_dividends_paid_quarterly=[0, 0, 0, 0],
            dividend_history_ttm_total=2.0,
            dividend_history_count=4,
        )
        assert result["source_used"] == "dividend_history"
        assert result["dividend_yield_ttm_value"] == 2.0


class TestNoDividendHistory:
    """No dividend_history records → missing_inputs regardless of cashflow."""

    def test_no_history_no_cashflow(self):
        result = compute_canonical_dividend_yield(
            market_cap=1e9,
            shares_outstanding=10e6,
            dividend_history_ttm_total=None,
            dividend_history_count=0,
        )
        assert result["dividend_yield_ttm_value"] is None
        assert result["na_reason"] == "missing_inputs"
        assert result["source_used"] == "none"

    def test_no_history_with_cashflow_positive(self):
        """Cashflow shows dividends but no history → still missing_inputs."""
        result = compute_canonical_dividend_yield(
            market_cap=100e9,
            shares_outstanding=1e9,
            cashflow_dividends_paid_quarterly=[-0.5e9, -0.5e9, -0.5e9, -0.5e9],
            dividend_history_ttm_total=None,
            dividend_history_count=0,
        )
        assert result["dividend_yield_ttm_value"] is None
        assert result["na_reason"] == "missing_inputs"
        assert result["source_used"] == "none"

    def test_no_history_with_cashflow_zero(self):
        """Cashflow says $0 but no history → still missing_inputs."""
        result = compute_canonical_dividend_yield(
            market_cap=100e9,
            shares_outstanding=1e9,
            cashflow_dividends_paid_quarterly=[0, 0, 0, 0],
            dividend_history_ttm_total=None,
            dividend_history_count=0,
        )
        assert result["dividend_yield_ttm_value"] is None
        assert result["na_reason"] == "missing_inputs"
        assert result["source_used"] == "none"

    def test_no_history_with_cashflow_null(self):
        """Cashflow all null + no history → missing_inputs."""
        result = compute_canonical_dividend_yield(
            market_cap=100e9,
            shares_outstanding=1e9,
            cashflow_dividends_paid_quarterly=[None, None, None, None],
            dividend_history_ttm_total=None,
            dividend_history_count=0,
        )
        assert result["dividend_yield_ttm_value"] is None
        assert result["na_reason"] == "missing_inputs"


class TestExtremeOutlier:
    """Yield > 100% → extreme_outlier guardrail."""

    def test_extreme_yield_from_history(self):
        """Even from history, >100% is capped."""
        result = compute_canonical_dividend_yield(
            market_cap=1e6,
            shares_outstanding=1e6,
            dividend_history_ttm_total=2.0,
            dividend_history_count=4,
        )
        assert result["dividend_yield_ttm_value"] is None
        assert result["na_reason"] == "extreme_outlier"


class TestMissingInputs:
    """Various missing data scenarios."""

    def test_no_market_cap(self):
        result = compute_canonical_dividend_yield(
            market_cap=None,
            shares_outstanding=10e6,
            dividend_history_ttm_total=1.0,
            dividend_history_count=4,
        )
        assert result["dividend_yield_ttm_value"] is None
        assert result["na_reason"] == "missing_inputs"

    def test_no_shares(self):
        result = compute_canonical_dividend_yield(
            market_cap=1e9,
            shares_outstanding=None,
            dividend_history_ttm_total=1.0,
            dividend_history_count=4,
        )
        assert result["dividend_yield_ttm_value"] is None
        assert result["na_reason"] == "missing_inputs"

    def test_zero_market_cap(self):
        result = compute_canonical_dividend_yield(
            market_cap=0,
            shares_outstanding=10e6,
            dividend_history_ttm_total=1.0,
            dividend_history_count=4,
        )
        assert result["dividend_yield_ttm_value"] is None
        assert result["na_reason"] == "missing_inputs"


class TestHistoryOnlySource:
    """Only dividend_history available (no cashflow data)."""

    def test_history_only(self):
        result = compute_canonical_dividend_yield(
            market_cap=100e9,
            shares_outstanding=1e9,
            dividend_history_ttm_total=2.0,
            dividend_history_count=4,
        )
        assert result["source_used"] == "dividend_history"
        assert result["na_reason"] is None
        assert abs(result["dividend_yield_ttm_value"] - 2.0) < 0.01

    def test_2_of_4_payments_in_window(self):
        """AAPL-like: only 2 payments in 365d window."""
        market_cap = 3.06e12
        shares = 15.3e9
        div_hist_ttm_total = 0.50  # 2 x $0.25
        result = compute_canonical_dividend_yield(
            market_cap=market_cap,
            shares_outstanding=shares,
            dividend_history_ttm_total=div_hist_ttm_total,
            dividend_history_count=2,
        )
        assert result["source_used"] == "dividend_history"
        assert result["na_reason"] is None
        assert result["dividend_yield_ttm_value"] > 0


class TestNoDividendSource:
    """source_used is never "cashflow" in production."""

    def test_source_never_cashflow(self):
        """No matter what cashflow says, source is either dividend_history or none."""
        # Case 1: has history
        r1 = compute_canonical_dividend_yield(
            market_cap=100e9,
            shares_outstanding=1e9,
            cashflow_dividends_paid_quarterly=[-1e9, -1e9, -1e9, -1e9],
            dividend_history_ttm_total=2.0,
            dividend_history_count=4,
        )
        assert r1["source_used"] in ("dividend_history", "none")

        # Case 2: no history, has cashflow
        r2 = compute_canonical_dividend_yield(
            market_cap=100e9,
            shares_outstanding=1e9,
            cashflow_dividends_paid_quarterly=[-1e9, -1e9, -1e9, -1e9],
            dividend_history_ttm_total=None,
            dividend_history_count=0,
        )
        assert r2["source_used"] in ("dividend_history", "none")

        # Case 3: no history, no cashflow
        r3 = compute_canonical_dividend_yield(
            market_cap=100e9,
            shares_outstanding=1e9,
            dividend_history_ttm_total=None,
            dividend_history_count=0,
        )
        assert r3["source_used"] in ("dividend_history", "none")
