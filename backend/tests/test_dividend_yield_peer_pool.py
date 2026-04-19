"""
Regression tests: Dividend Yield (TTM) peer median pool sizing.

Verifies that compute_peer_benchmarks_v3 correctly includes non-dividend-paying
tickers (those with no dividend_history and null cashflow dividends_paid) in the
peer pool as 0% yield rather than excluding them entirely.

Bug: A large sector like Financial Services (~1000 tickers) produced
dividend_yield median=0.00% with n=7 because only 7 tickers had a canonical
dividend_yield value; the rest were excluded as "missing_inputs".

Root cause: The canonical dividend function returned na_reason="missing_inputs"
for tickers with no dividend_history AND all-null cashflow dividends_paid.
These visible, complete-fundamentals tickers were silently excluded from the
peer pool.

Fix: When canonical returns missing_inputs and dividend_history_count=0,
treat the ticker as a non-payer (yield=0.0) for benchmarking purposes.

Run:
    cd /app/backend && python -m pytest tests/test_dividend_yield_peer_pool.py -v
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import statistics
import pytest
from canonical_dividend import compute_canonical_dividend_yield


# ---------------------------------------------------------------------------
# Constants matching key_metrics_service.py
# ---------------------------------------------------------------------------
MIN_PEER_COUNT = 5
MIN_DIVIDEND_PAYERS = 5


# ---------------------------------------------------------------------------
# Helper: simulate the per-ticker dividend yield assignment from
# compute_peer_benchmarks_v3 (post-fix version)
# ---------------------------------------------------------------------------
def _compute_dividend_yield_for_benchmark(
    *,
    market_cap,
    shares_outstanding,
    cashflow_dividends_paid_quarterly,
    dividend_history_ttm_total,
    dividend_history_count,
):
    """Replicate the fixed logic from compute_peer_benchmarks_v3."""
    canonical = compute_canonical_dividend_yield(
        market_cap=market_cap,
        shares_outstanding=shares_outstanding,
        cashflow_dividends_paid_quarterly=cashflow_dividends_paid_quarterly,
        dividend_history_ttm_total=dividend_history_ttm_total,
        dividend_history_count=dividend_history_count,
    )
    if canonical["dividend_yield_ttm_value"] is not None:
        return canonical["dividend_yield_ttm_value"]
    # FIX: non-payer with no dividend history → 0% for benchmarking
    if (
        canonical["na_reason"] == "missing_inputs"
        and dividend_history_count == 0
    ):
        return 0.0
    return None  # genuinely excluded (unreliable, extreme_outlier)


# ---------------------------------------------------------------------------
# Simulated sector: Financial Services with ~50 tickers
# ---------------------------------------------------------------------------
def _build_financial_services_sector(n_total=50, n_div_payers=8):
    """
    Build a realistic sector where:
    - n_div_payers tickers pay dividends (have dividend_history)
    - The rest have no dividend_history AND null cashflow dividends_paid.
    All are visible with complete fundamentals.
    """
    tickers = []
    for i in range(n_total):
        ticker_name = f"FIN{i:03d}.US"
        market_cap = 1e9 + i * 1e7
        shares = 1e7
        if i < n_div_payers:
            # Dividend payer: has dividend_history, yield ~2-6%
            yield_pct = 2.0 + (i * 0.5)
            price = market_cap / shares
            div_per_share = price * yield_pct / 100
            tickers.append({
                "ticker": ticker_name,
                "sector": "Financial Services",
                "industry": "Asset Management",
                "financial_currency": "USD",
                "market_cap": market_cap,
                "shares_outstanding": shares,
                "cashflow_dividends_paid_quarterly": [
                    -(div_per_share * shares / 4),
                    -(div_per_share * shares / 4),
                    -(div_per_share * shares / 4),
                    -(div_per_share * shares / 4),
                ],
                "dividend_history_ttm_total": div_per_share,
                "dividend_history_count": 4,
            })
        else:
            # Non-payer: no dividend_history, null cashflow dividends_paid
            tickers.append({
                "ticker": ticker_name,
                "sector": "Financial Services",
                "industry": "Asset Management",
                "financial_currency": "USD",
                "market_cap": market_cap,
                "shares_outstanding": shares,
                "cashflow_dividends_paid_quarterly": [None, None, None, None],
                "dividend_history_ttm_total": None,
                "dividend_history_count": 0,
            })
    return tickers


class TestDividendYieldPeerPool:
    """Regression tests for the dividend yield peer pool fix."""

    def test_non_payers_with_null_cashflow_included_as_zero(self):
        """Tickers with no dividend_history and all-null cashflow dividends_paid
        should be treated as 0% yield, not excluded."""
        result = _compute_dividend_yield_for_benchmark(
            market_cap=1e9,
            shares_outstanding=1e7,
            cashflow_dividends_paid_quarterly=[None, None, None, None],
            dividend_history_ttm_total=None,
            dividend_history_count=0,
        )
        assert result == 0.0, (
            "Non-payer with null cashflow should be 0% yield for benchmarking"
        )

    def test_non_payers_with_missing_cashflow_included_as_zero(self):
        """Tickers with no dividend_history and fewer than 4 cashflow quarters
        should also be treated as 0% yield."""
        result = _compute_dividend_yield_for_benchmark(
            market_cap=1e9,
            shares_outstanding=1e7,
            cashflow_dividends_paid_quarterly=[],  # < 4 quarters
            dividend_history_ttm_total=None,
            dividend_history_count=0,
        )
        assert result == 0.0, (
            "Non-payer with missing cashflow should be 0% yield for benchmarking"
        )

    def test_unreliable_tickers_still_excluded(self):
        """Tickers where cashflow says dividends but dividend_history disagrees
        should remain excluded (na_reason=unreliable)."""
        result = _compute_dividend_yield_for_benchmark(
            market_cap=3.9e6,
            shares_outstanding=1e6,
            cashflow_dividends_paid_quarterly=[-99e6, -99e6, None, None],
            dividend_history_ttm_total=None,
            dividend_history_count=0,
        )
        assert result is None, "Unreliable tickers should remain excluded"

    def test_extreme_outlier_still_excluded(self):
        """Yield > 100% should remain excluded."""
        result = _compute_dividend_yield_for_benchmark(
            market_cap=1e6,
            shares_outstanding=1e6,
            cashflow_dividends_paid_quarterly=[None, None, None, None],
            dividend_history_ttm_total=2.0,  # $2/share on $1 stock = 200%
            dividend_history_count=4,
        )
        assert result is None, "Extreme outlier should remain excluded"

    def test_actual_dividend_payer_still_included(self):
        """Normal dividend payer should still get correct yield."""
        result = _compute_dividend_yield_for_benchmark(
            market_cap=100e9,
            shares_outstanding=1e9,
            cashflow_dividends_paid_quarterly=[-0.5e9, -0.5e9, -0.5e9, -0.5e9],
            dividend_history_ttm_total=2.0,
            dividend_history_count=4,
        )
        assert result is not None
        assert 1.9 < result < 2.1, f"Expected ~2.0%, got {result}%"


class TestLargeSectorDividendMedian:
    """Regression: large sector must not produce median=0% with tiny n."""

    def test_sector_pool_includes_non_payers(self):
        """A sector with 50 tickers (8 payers + 42 non-payers) should have
        all 50 in the dividend yield pool, not just the 8 payers."""
        tickers = _build_financial_services_sector(n_total=50, n_div_payers=8)

        # Simulate the benchmark pipeline's dividend yield extraction
        dividend_yields = []
        for t in tickers:
            dy = _compute_dividend_yield_for_benchmark(
                market_cap=t["market_cap"],
                shares_outstanding=t["shares_outstanding"],
                cashflow_dividends_paid_quarterly=t["cashflow_dividends_paid_quarterly"],
                dividend_history_ttm_total=t["dividend_history_ttm_total"],
                dividend_history_count=t["dividend_history_count"],
            )
            if dy is not None and dy >= 0:
                dividend_yields.append(dy)

        # REGRESSION: Before the fix, only ~8 tickers had valid dividend_yield
        assert len(dividend_yields) == 50, (
            f"Expected all 50 tickers in pool, got {len(dividend_yields)}"
        )
        assert len(dividend_yields) >= MIN_PEER_COUNT, (
            f"Pool size {len(dividend_yields)} below MIN_PEER_COUNT={MIN_PEER_COUNT}"
        )

    def test_sector_median_all_is_zero_with_large_n(self):
        """When most tickers are non-payers, median_all should be 0.00% but
        n should reflect the full pool, not single digits."""
        tickers = _build_financial_services_sector(n_total=50, n_div_payers=8)

        dividend_yields = []
        for t in tickers:
            dy = _compute_dividend_yield_for_benchmark(
                market_cap=t["market_cap"],
                shares_outstanding=t["shares_outstanding"],
                cashflow_dividends_paid_quarterly=t["cashflow_dividends_paid_quarterly"],
                dividend_history_ttm_total=t["dividend_history_ttm_total"],
                dividend_history_count=t["dividend_history_count"],
            )
            if dy is not None and dy >= 0:
                dividend_yields.append(dy)

        median_all = statistics.median(sorted(dividend_yields))
        payers_only = [y for y in dividend_yields if y > 0]
        median_payers = statistics.median(sorted(payers_only)) if len(payers_only) >= MIN_DIVIDEND_PAYERS else None

        # median_all should be 0.0 (majority are non-payers)
        assert median_all == 0.0, f"Expected median_all=0.0, got {median_all}"
        # n should be the full pool, not tiny
        assert len(dividend_yields) == 50
        # median_payers should be positive (actual dividend payers)
        assert median_payers is not None and median_payers > 0, (
            f"Expected positive median_payers, got {median_payers}"
        )

    def test_industry_pool_not_artificially_empty(self):
        """An industry with 20 tickers (3 payers + 17 non-payers with null
        cashflow) should have 20 tickers in the dividend yield pool, not 3."""
        tickers = _build_financial_services_sector(n_total=20, n_div_payers=3)

        dividend_yields = []
        for t in tickers:
            dy = _compute_dividend_yield_for_benchmark(
                market_cap=t["market_cap"],
                shares_outstanding=t["shares_outstanding"],
                cashflow_dividends_paid_quarterly=t["cashflow_dividends_paid_quarterly"],
                dividend_history_ttm_total=t["dividend_history_ttm_total"],
                dividend_history_count=t["dividend_history_count"],
            )
            if dy is not None and dy >= 0:
                dividend_yields.append(dy)

        # Before fix: only 3 tickers had dividend_yield → below MIN_PEER_COUNT
        # After fix: all 20 are in the pool
        assert len(dividend_yields) == 20
        assert len(dividend_yields) >= MIN_PEER_COUNT
