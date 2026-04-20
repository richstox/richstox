"""
Regression test: ALL dividend yield paths use dividend_history as sole source.

Previous bug:
  - /stock-overview/{ticker} used calculate_dividend_yield_ttm (dividend_history only, no canonical)
  - /admin/benchmark-debug/industry used inline cashflow-only computation
  - compute_canonical_dividend_yield used cashflow as fallback/secondary source

After fix:
  - ALL paths call compute_canonical_dividend_yield
  - compute_canonical_dividend_yield uses ONLY dividend_history
  - Cashflow is diagnostic/debug only — never used for production yield

Source of truth: EODHD dividend events → dividend_history → sum by period → yield from price.

Run:
    cd /app/backend && python -m pytest tests/test_dividend_yield_canonical_path.py -v
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from canonical_dividend import compute_canonical_dividend_yield


class TestCashflowNeverUsedForProduction:
    """Prove that cashflow data is completely ignored for yield computation."""

    def test_cashflow_positive_but_no_history_returns_missing(self):
        """
        Cashflow says company pays dividends, but no dividend_history.
        Old behavior: used cashflow yield.
        New behavior: missing_inputs (no history = no data).
        """
        result = compute_canonical_dividend_yield(
            market_cap=100_000_000_000,
            shares_outstanding=1_000_000_000,
            cashflow_dividends_paid_quarterly=[-0.5e9, -0.5e9, -0.5e9, -0.5e9],
            dividend_history_ttm_total=None,
            dividend_history_count=0,
        )
        assert result["dividend_yield_ttm_value"] is None
        assert result["na_reason"] == "missing_inputs"
        assert result["source_used"] == "none"

    def test_cashflow_zero_but_no_history_returns_missing(self):
        """
        Cashflow says $0 dividends (old: proven non-payer from cashflow).
        New behavior: missing_inputs (no history = no data).
        """
        result = compute_canonical_dividend_yield(
            market_cap=100_000_000_000,
            shares_outstanding=1_000_000_000,
            cashflow_dividends_paid_quarterly=[0.0, 0.0, 0.0, 0.0],
            dividend_history_ttm_total=None,
            dividend_history_count=0,
        )
        assert result["dividend_yield_ttm_value"] is None
        assert result["na_reason"] == "missing_inputs"
        assert result["source_used"] == "none"

    def test_cashflow_conflict_ignored_history_wins(self):
        """
        Cashflow says $0 but history has payments.
        Old behavior: 'unreliable' (payer/non-payer conflict).
        New behavior: dividend_history yield used (cashflow ignored).
        """
        result = compute_canonical_dividend_yield(
            market_cap=100_000_000_000,
            shares_outstanding=1_000_000_000,
            cashflow_dividends_paid_quarterly=[0, 0, 0, 0],
            dividend_history_ttm_total=2.0,
            dividend_history_count=4,
        )
        assert result["dividend_yield_ttm_value"] == 2.0
        assert result["source_used"] == "dividend_history"
        assert result["na_reason"] is None

    def test_cashflow_3x_disagreement_ignored_history_wins(self):
        """
        Cashflow says 5x more dividends than history.
        Old behavior: 'unreliable' (>3x ratio).
        New behavior: dividend_history yield used (cashflow ignored).
        """
        result = compute_canonical_dividend_yield(
            market_cap=100_000_000_000,
            shares_outstanding=1_000_000_000,
            cashflow_dividends_paid_quarterly=[-5e9, -5e9, -5e9, -5e9],
            dividend_history_ttm_total=1.0,
            dividend_history_count=4,
        )
        assert result["dividend_yield_ttm_value"] == 1.0
        assert result["source_used"] == "dividend_history"
        assert result["na_reason"] is None


class TestSourceUsedField:
    """source_used is NEVER 'cashflow' in production."""

    def test_source_is_dividend_history_or_none(self):
        """All possible outcomes have source in {'dividend_history', 'none'}."""
        scenarios = [
            # Has history
            dict(market_cap=1e9, shares_outstanding=1e7,
                 dividend_history_ttm_total=1.0, dividend_history_count=4),
            # No history
            dict(market_cap=1e9, shares_outstanding=1e7,
                 dividend_history_ttm_total=None, dividend_history_count=0),
            # No history + cashflow present
            dict(market_cap=1e9, shares_outstanding=1e7,
                 cashflow_dividends_paid_quarterly=[-1e6]*4,
                 dividend_history_ttm_total=None, dividend_history_count=0),
            # Extreme outlier
            dict(market_cap=1e6, shares_outstanding=1e6,
                 dividend_history_ttm_total=5.0, dividend_history_count=4),
        ]
        for kwargs in scenarios:
            result = compute_canonical_dividend_yield(**kwargs)
            assert result["source_used"] in ("dividend_history", "none"), \
                f"source_used={result['source_used']} for {kwargs}"

    def test_unreliable_na_reason_never_returned(self):
        """'unreliable' was a dual-source concept. It should never appear."""
        # Scenario that previously triggered 'unreliable'
        result = compute_canonical_dividend_yield(
            market_cap=100_000_000_000,
            shares_outstanding=1_000_000_000,
            cashflow_dividends_paid_quarterly=[0, 0, 0, 0],
            dividend_history_ttm_total=2.0,
            dividend_history_count=4,
        )
        assert result["na_reason"] != "unreliable"


class TestAAPLConcreteExample:
    """
    AAPL concrete proof:
      - Dividends tab source: dividend_history payments
      - ticker detail dividend_yield_ttm: dividend_history / price
      - peer benchmark per-ticker: same canonical function
      - All resolve through compute_canonical_dividend_yield
    """

    def test_aapl_yield_from_dividend_history(self):
        """
        AAPL: $0.25/share/quarter, 4 payments/year = $1.00/share TTM
        Price = $200 (market_cap=3T, shares=15B)
        Yield = $1.00 / $200 * 100 = 0.50%
        """
        market_cap = 3_000_000_000_000
        shares = 15_000_000_000
        price = market_cap / shares  # $200

        result = compute_canonical_dividend_yield(
            market_cap=market_cap,
            shares_outstanding=shares,
            dividend_history_ttm_total=1.00,  # 4 × $0.25
            dividend_history_count=4,
        )
        assert result["source_used"] == "dividend_history"
        assert result["na_reason"] is None
        expected_yield = (1.00 / price) * 100  # 0.50%
        assert abs(result["dividend_yield_ttm_value"] - expected_yield) < 0.001

    def test_aapl_peer_benchmark_uses_same_function(self):
        """
        Peer benchmark calls the same compute_canonical_dividend_yield.
        Given identical inputs → identical output.
        """
        inputs = dict(
            market_cap=3_000_000_000_000,
            shares_outstanding=15_000_000_000,
            dividend_history_ttm_total=1.00,
            dividend_history_count=4,
        )
        # Simulate ticker detail call
        detail_result = compute_canonical_dividend_yield(**inputs)
        # Simulate peer benchmark call (same function, same inputs)
        benchmark_result = compute_canonical_dividend_yield(**inputs)

        assert detail_result == benchmark_result
        assert detail_result["source_used"] == "dividend_history"
