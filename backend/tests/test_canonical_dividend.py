"""
Unit tests for canonical_dividend.compute_canonical_dividend_yield.

Tests the single shared function that drives:
  1. Ticker detail Key Metrics → dividend_yield_ttm
  2. Earnings & Dividends → dividend status
  3. Step 4 peer benchmarks → dividend_yield_ttm
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pytest
from canonical_dividend import compute_canonical_dividend_yield


class TestNormalDividendPayer:
    """Normal case: AAPL-like ticker where both sources agree."""

    def test_both_sources_agree(self):
        """When dividend_history and cashflow agree within 20%, use dividend_history."""
        # AAPL-like: $0.96/share annual dividend, 15B shares, $150 price
        # market_cap = 150 * 15e9 = 2.25T
        # hist_yield = 0.96 / 150 * 100 = 0.64%
        # cashflow: 0.96 * 15e9 = 14.4B total → yield = 14.4B / 2.25T * 100 = 0.64%
        result = compute_canonical_dividend_yield(
            market_cap=2.25e12,
            shares_outstanding=15e9,
            cashflow_dividends_paid_quarterly=[-3.6e9, -3.6e9, -3.6e9, -3.6e9],
            dividend_history_ttm_total=0.96,
            dividend_history_count=4,
        )
        assert result["source_used"] == "dividend_history"
        assert result["na_reason"] is None
        assert result["dividend_yield_ttm_value"] is not None
        assert 0.5 < result["dividend_yield_ttm_value"] < 0.8
        assert result["debug_inputs"] is None  # not requested

    def test_include_debug(self):
        """debug_inputs returned when include_debug=True."""
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
        assert "cashflow_yield_pct" in result["debug_inputs"]
        assert "dividend_history_yield_pct" in result["debug_inputs"]


class TestONFO:
    """ONFO.US-like: cashflow shows huge dividends, dividend_history has zero records.
    
    With the fixed logic, cashflow is used as the source when dividend_history
    is absent. The extreme cashflow_yield (~5077%) triggers extreme_outlier,
    which is the correct classification — the data is implausible.
    """

    def test_cashflow_only_no_history_is_extreme_outlier(self):
        """Cashflow says huge dividends but dividend_history is empty → extreme_outlier.
        
        The cashflow yield (198M / 3.9M * 100 = ~5077%) exceeds the 100% cap.
        """
        # ONFO: market_cap ~$3.9M, dividends_paid ~$99M/quarter, no dividend_history
        result = compute_canonical_dividend_yield(
            market_cap=3.9e6,
            shares_outstanding=1e6,
            cashflow_dividends_paid_quarterly=[-99e6, -99e6, None, None],
            dividend_history_ttm_total=None,
            dividend_history_count=0,
        )
        assert result["dividend_yield_ttm_value"] is None
        assert result["na_reason"] == "extreme_outlier"

    def test_onfo_key_metrics_and_earnings_agree(self):
        """Both Key Metrics (N/A extreme_outlier) and Earnings (no dividends) agree."""
        result = compute_canonical_dividend_yield(
            market_cap=3.9e6,
            shares_outstanding=1e6,
            cashflow_dividends_paid_quarterly=[-99e6, -99e6, -99e6, -99e6],
            dividend_history_ttm_total=None,
            dividend_history_count=0,
        )
        # Key Metrics: value is None → shows "N/A (extreme_outlier)"
        assert result["dividend_yield_ttm_value"] is None
        # Earnings & Dividends: dividend_history_count == 0 → shows "No dividends"
        # Both sections agree: no reliable dividend data


class TestIntegrityCheck:
    """Sources materially disagree (>20% relative difference) → unreliable."""

    def test_sources_disagree_by_more_than_20pct(self):
        """History says 2%, cashflow says 5% → disagree → unreliable."""
        # market_cap = 100B, shares = 1B, price = 100
        # hist_yield = 2.0 / 100 * 100 = 2.0%
        # cashflow: total = 5B → yield = 5B/100B * 100 = 5.0%
        # rel_diff = |2-5|/5 = 0.6 > 0.2 → unreliable
        result = compute_canonical_dividend_yield(
            market_cap=100e9,
            shares_outstanding=1e9,
            cashflow_dividends_paid_quarterly=[-1.25e9, -1.25e9, -1.25e9, -1.25e9],
            dividend_history_ttm_total=2.0,
            dividend_history_count=4,
        )
        assert result["dividend_yield_ttm_value"] is None
        assert result["na_reason"] == "unreliable"

    def test_sources_agree_within_20pct(self):
        """History says 2.0%, cashflow says 2.3% → agree → use history."""
        # market_cap = 100B, shares = 1B, price = 100
        # hist: 2.0 / 100 * 100 = 2.0%
        # cashflow: 2.3B → 2.3B/100B * 100 = 2.3%
        # rel_diff = |2.0-2.3|/2.3 = 0.13 < 0.2 → OK
        result = compute_canonical_dividend_yield(
            market_cap=100e9,
            shares_outstanding=1e9,
            cashflow_dividends_paid_quarterly=[-0.575e9, -0.575e9, -0.575e9, -0.575e9],
            dividend_history_ttm_total=2.0,
            dividend_history_count=4,
        )
        assert result["source_used"] == "dividend_history"
        assert result["na_reason"] is None
        assert 1.9 < result["dividend_yield_ttm_value"] < 2.1


class TestExtremeOutlier:
    """Yield > 100% → extreme_outlier (last-resort guardrail)."""

    def test_extreme_yield_from_history(self):
        """Even if history says >100%, cap it."""
        # price = 1, hist_total = 2.0/share → yield = 200%
        result = compute_canonical_dividend_yield(
            market_cap=1e6,
            shares_outstanding=1e6,
            cashflow_dividends_paid_quarterly=[None, None, None, None],
            dividend_history_ttm_total=2.0,
            dividend_history_count=4,
        )
        assert result["dividend_yield_ttm_value"] is None
        assert result["na_reason"] == "extreme_outlier"


class TestNoDividend:
    """Company pays no dividends at all."""

    def test_both_sources_zero(self):
        """Both cashflow=0 and no history → no_dividend."""
        result = compute_canonical_dividend_yield(
            market_cap=1e9,
            shares_outstanding=10e6,
            cashflow_dividends_paid_quarterly=[0, 0, 0, 0],
            dividend_history_ttm_total=None,
            dividend_history_count=0,
        )
        assert result["dividend_yield_ttm_value"] == 0.0
        assert result["na_reason"] == "no_dividend"

    def test_history_only_zero(self):
        """No history, cashflow all None → missing_inputs."""
        result = compute_canonical_dividend_yield(
            market_cap=1e9,
            shares_outstanding=10e6,
            cashflow_dividends_paid_quarterly=[None, None, None, None],
            dividend_history_ttm_total=None,
            dividend_history_count=0,
        )
        assert result["dividend_yield_ttm_value"] is None
        assert result["na_reason"] == "missing_inputs"


class TestMissingInputs:
    """Various missing data scenarios."""

    def test_no_market_cap(self):
        result = compute_canonical_dividend_yield(
            market_cap=None,
            shares_outstanding=10e6,
            cashflow_dividends_paid_quarterly=[-1e6, -1e6, -1e6, -1e6],
            dividend_history_ttm_total=1.0,
            dividend_history_count=4,
        )
        assert result["dividend_yield_ttm_value"] is None
        assert result["na_reason"] == "missing_inputs"

    def test_fewer_than_4_quarters(self):
        result = compute_canonical_dividend_yield(
            market_cap=1e9,
            shares_outstanding=10e6,
            cashflow_dividends_paid_quarterly=[-1e6, -1e6],
            dividend_history_ttm_total=None,
            dividend_history_count=0,
        )
        assert result["dividend_yield_ttm_value"] is None
        assert result["na_reason"] == "missing_inputs"

    def test_no_shares_but_has_market_cap(self):
        """Can compute cashflow yield but not history yield."""
        result = compute_canonical_dividend_yield(
            market_cap=1e9,
            shares_outstanding=None,
            cashflow_dividends_paid_quarterly=[-2.5e6, -2.5e6, -2.5e6, -2.5e6],
            dividend_history_ttm_total=1.0,
            dividend_history_count=4,
        )
        # Can't compare yields (no shares → no hist_yield), so uses cashflow only
        assert result["source_used"] == "cashflow"
        assert result["dividend_yield_ttm_value"] is not None


class TestHistoryOnlySource:
    """Only dividend_history available (no cashflow data)."""

    def test_history_only(self):
        # No cashflow data but has history
        result = compute_canonical_dividend_yield(
            market_cap=100e9,
            shares_outstanding=1e9,
            cashflow_dividends_paid_quarterly=[],
            dividend_history_ttm_total=2.0,
            dividend_history_count=4,
        )
        assert result["source_used"] == "dividend_history"
        assert result["na_reason"] is None
        # yield = 2.0 / (100e9/1e9) * 100 = 2.0%
        assert abs(result["dividend_yield_ttm_value"] - 2.0) < 0.01


class TestCashflowOnlySource:
    """Only cashflow available, dividend_history has records but no total."""

    def test_cashflow_with_known_history_count(self):
        """dividend_history says records exist but total is 0 somehow → use cashflow."""
        result = compute_canonical_dividend_yield(
            market_cap=100e9,
            shares_outstanding=1e9,
            cashflow_dividends_paid_quarterly=[-0.5e9, -0.5e9, -0.5e9, -0.5e9],
            dividend_history_ttm_total=0.0,  # edge: total is 0 but count > 0
            dividend_history_count=4,
        )
        # hist_yield is None (total <= 0), so falls through to cashflow
        assert result["source_used"] == "cashflow"


class TestCashflowFallbackSource:
    """Spec requirement B: Step 4 must compute dividend_yield using the
    best available source WITHOUT requiring dividend_history to exist.

    Priority:
    1) dividend_history records in last 365d → canonical yield
    2) cashflow non-null dividendsPaid:
       - sum==0 → proven non-payer → yield=0.0, na_reason="no_dividend"
       - sum>0 and price exists → compute yield from cashflow
    3) else → missing_inputs
    """

    def test_cashflow_positive_no_history_uses_cashflow(self):
        """Cashflow non-null sum>0 with price → included with computed yield.

        This is the key fix: previously this was classified as "unreliable"
        which excluded ~2393 tickers. Now it uses cashflow as the source.
        """
        # market_cap = 100B, cashflow = 2B total → yield = 2%
        result = compute_canonical_dividend_yield(
            market_cap=100e9,
            shares_outstanding=1e9,
            cashflow_dividends_paid_quarterly=[-0.5e9, -0.5e9, -0.5e9, -0.5e9],
            dividend_history_ttm_total=None,
            dividend_history_count=0,
        )
        assert result["dividend_yield_ttm_value"] is not None
        assert result["source_used"] == "cashflow"
        assert result["na_reason"] is None
        assert abs(result["dividend_yield_ttm_value"] - 2.0) < 0.1

    def test_cashflow_zero_no_history_proven_non_payer(self):
        """Cashflow non-null sum==0 → no_dividend → included as 0.0."""
        result = compute_canonical_dividend_yield(
            market_cap=100e9,
            shares_outstanding=1e9,
            cashflow_dividends_paid_quarterly=[0, 0, 0, 0],
            dividend_history_ttm_total=None,
            dividend_history_count=0,
        )
        assert result["dividend_yield_ttm_value"] == 0.0
        assert result["na_reason"] == "no_dividend"
        assert result["source_used"] == "cashflow"

    def test_cashflow_all_null_no_history_missing_inputs(self):
        """Cashflow null + no history → missing_inputs (excluded)."""
        result = compute_canonical_dividend_yield(
            market_cap=100e9,
            shares_outstanding=1e9,
            cashflow_dividends_paid_quarterly=[None, None, None, None],
            dividend_history_ttm_total=None,
            dividend_history_count=0,
        )
        assert result["dividend_yield_ttm_value"] is None
        assert result["na_reason"] == "missing_inputs"

    def test_reliability_check_does_not_trigger_when_cashflow_all_null(self):
        """Reliability check must NOT trigger when cashflow is all-null.
        
        When only dividend_history has data, there's nothing to compare against,
        so it uses history without an integrity check.
        """
        result = compute_canonical_dividend_yield(
            market_cap=100e9,
            shares_outstanding=1e9,
            cashflow_dividends_paid_quarterly=[None, None, None, None],
            dividend_history_ttm_total=2.0,
            dividend_history_count=4,
        )
        assert result["source_used"] == "dividend_history"
        assert result["na_reason"] is None  # NOT unreliable
        assert abs(result["dividend_yield_ttm_value"] - 2.0) < 0.01

    def test_cashflow_some_null_quarters_uses_non_null(self):
        """Cashflow with some non-null quarters uses those (quarterly payer pattern)."""
        # 2 non-null quarters with -1B each → TTM ~2B, yield ~2%
        result = compute_canonical_dividend_yield(
            market_cap=100e9,
            shares_outstanding=1e9,
            cashflow_dividends_paid_quarterly=[-1e9, None, -1e9, None],
            dividend_history_ttm_total=None,
            dividend_history_count=0,
        )
        assert result["source_used"] == "cashflow"
        assert result["dividend_yield_ttm_value"] is not None
        assert abs(result["dividend_yield_ttm_value"] - 2.0) < 0.1

    def test_cashflow_positive_with_reasonable_yield(self):
        """Cashflow fallback with a reasonable yield (not extreme) is included."""
        # yield = 10M / 500M * 100 = 2%
        result = compute_canonical_dividend_yield(
            market_cap=500e6,
            shares_outstanding=10e6,
            cashflow_dividends_paid_quarterly=[-2.5e6, -2.5e6, -2.5e6, -2.5e6],
            dividend_history_ttm_total=None,
            dividend_history_count=0,
        )
        assert result["source_used"] == "cashflow"
        assert abs(result["dividend_yield_ttm_value"] - 2.0) < 0.1
        assert result["na_reason"] is None

    def test_cashflow_extreme_yield_caught_by_guardrail(self):
        """Extreme cashflow yield (>100%) caught by guardrail, not by unreliable check."""
        # yield = 50M / 5M * 100 = 1000%
        result = compute_canonical_dividend_yield(
            market_cap=5e6,
            shares_outstanding=1e6,
            cashflow_dividends_paid_quarterly=[-12.5e6, -12.5e6, -12.5e6, -12.5e6],
            dividend_history_ttm_total=None,
            dividend_history_count=0,
        )
        assert result["dividend_yield_ttm_value"] is None
        assert result["na_reason"] == "extreme_outlier"
