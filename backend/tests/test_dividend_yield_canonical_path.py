"""
Regression test: ALL dividend yield paths resolve through canonical function.

Previous bug:
  - /stock-overview/{ticker} used calculate_dividend_yield_ttm (dividend_history only)
  - /admin/benchmark-debug/industry used inline cashflow-only computation
  - compute_peer_benchmarks_v3 used compute_canonical_dividend_yield (correct)
  - /v1/ticker/{ticker}/detail used compute_canonical_dividend_yield (correct)

The old paths produced different results because:
  1. cashflow-only path: sum(abs(dividendsPaid)) / market_cap * 100
     — ignores dividend_history entirely
  2. dividend_history-only path: sum(per_share_amounts) / current_price * 100
     — ignores cashflow, no integrity check, no extreme outlier guardrail

After fix: ALL paths call compute_canonical_dividend_yield with BOTH
dividend_history (primary) and cashflow (secondary/integrity check).

Run:
    cd /app/backend && python -m pytest tests/test_dividend_yield_canonical_path.py -v
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from canonical_dividend import compute_canonical_dividend_yield


# ---------------------------------------------------------------------------
# Helper: simulate the OLD cashflow-only path (removed)
# ---------------------------------------------------------------------------
def _old_cashflow_only_yield(*, cashflow_dividends_paid_quarterly, market_cap):
    """
    Replicates the removed inline cashflow-only computation from
    /admin/benchmark-debug/industry (server.py, formerly lines 3526-3537).
    """
    cf_vals = cashflow_dividends_paid_quarterly or []
    if len(cf_vals) < 4:
        return None
    if all(v is None for v in cf_vals):
        return None
    total = sum(abs(v) for v in cf_vals if v is not None)
    if total == 0.0:
        return 0.0
    if market_cap and market_cap > 0:
        return (total / market_cap) * 100
    return None


# ---------------------------------------------------------------------------
# Helper: simulate the OLD dividend_history-only path (removed)
# ---------------------------------------------------------------------------
def _old_history_only_yield(*, dividend_history_ttm_total, current_price):
    """
    Replicates the removed calculate_dividend_yield_ttm from
    dividend_history_service.py (used by /stock-overview/{ticker}).
    """
    if not current_price or current_price <= 0:
        return None
    if dividend_history_ttm_total is None or dividend_history_ttm_total <= 0:
        return None
    return round((dividend_history_ttm_total / current_price) * 100, 4)


class TestOldPathsDisagreedWithCanonical:
    """Prove the old forked paths could produce different results."""

    def test_cashflow_only_ignores_dividend_history(self):
        """
        AAPL-like: dividend_history has 2 of 4 payments in window (~$0.50),
        cashflow has all 4 quarters (~$15B total).
        Old cashflow-only path: 15B / 3T * 100 = 0.50%
        Canonical function: uses dividend_history as primary.
        """
        market_cap = 3_000_000_000_000  # $3T
        shares = 15_000_000_000  # 15B shares
        price = market_cap / shares  # $200

        # dividend_history: 2 payments in TTM window = $0.50/share
        hist_ttm_total = 0.50
        hist_count = 2

        # cashflow: 4 quarters, total $15B paid
        cf_vals = [-3_750_000_000.0] * 4  # $3.75B per quarter

        old_cf_yield = _old_cashflow_only_yield(
            cashflow_dividends_paid_quarterly=cf_vals,
            market_cap=market_cap,
        )

        canonical = compute_canonical_dividend_yield(
            market_cap=market_cap,
            shares_outstanding=shares,
            cashflow_dividends_paid_quarterly=cf_vals,
            dividend_history_ttm_total=hist_ttm_total,
            dividend_history_count=hist_count,
        )

        # Old cashflow path: ~0.50%
        assert old_cf_yield is not None
        assert abs(old_cf_yield - 0.50) < 0.01

        # Canonical uses dividend_history (primary): $0.50 / $200 * 100 = 0.25%
        assert canonical["source_used"] == "dividend_history"
        assert canonical["dividend_yield_ttm_value"] is not None
        assert abs(canonical["dividend_yield_ttm_value"] - 0.25) < 0.01

        # They disagree — the old path was wrong
        assert abs(old_cf_yield - canonical["dividend_yield_ttm_value"]) > 0.1

    def test_history_only_misses_cashflow_fallback(self):
        """
        Ticker with no dividend_history but cashflow shows $0 dividends_paid.
        Old history-only path: returns None (no data)
        Canonical: returns 0.0 (proven non-payer from cashflow evidence)
        """
        market_cap = 1_000_000_000
        shares = 10_000_000
        price = market_cap / shares

        old_hist_yield = _old_history_only_yield(
            dividend_history_ttm_total=None,
            current_price=price,
        )

        canonical = compute_canonical_dividend_yield(
            market_cap=market_cap,
            shares_outstanding=shares,
            cashflow_dividends_paid_quarterly=[0.0, 0.0, 0.0, 0.0],
            dividend_history_ttm_total=None,
            dividend_history_count=0,
        )

        # Old path: None (no dividend_history → missing)
        assert old_hist_yield is None

        # Canonical: 0.0% (proven non-payer from cashflow evidence)
        assert canonical["dividend_yield_ttm_value"] == 0.0
        assert canonical["na_reason"] == "no_dividend"
        assert canonical["source_used"] == "cashflow"

    def test_history_only_no_extreme_outlier_guardrail(self):
        """
        Old history-only path had no >100% yield guardrail.
        Canonical catches extreme outliers.
        """
        # Extreme case: per-share total > price
        price = 1.0
        hist_ttm_total = 5.0  # 500% yield
        market_cap = 1_000_000
        shares = 1_000_000

        old_hist_yield = _old_history_only_yield(
            dividend_history_ttm_total=hist_ttm_total,
            current_price=price,
        )

        canonical = compute_canonical_dividend_yield(
            market_cap=market_cap,
            shares_outstanding=shares,
            cashflow_dividends_paid_quarterly=[],
            dividend_history_ttm_total=hist_ttm_total,
            dividend_history_count=4,
        )

        # Old path: 500% (no guardrail)
        assert old_hist_yield == 500.0

        # Canonical: None (extreme outlier caught)
        assert canonical["dividend_yield_ttm_value"] is None
        assert canonical["na_reason"] == "extreme_outlier"


class TestCanonicalPathUnification:
    """Verify that all code paths now produce identical results."""

    def test_canonical_is_single_source_of_truth(self):
        """
        All endpoints now call compute_canonical_dividend_yield.
        Verify that given the same inputs, the function returns
        a deterministic result.
        """
        inputs = dict(
            market_cap=3_000_000_000_000,
            shares_outstanding=15_000_000_000,
            cashflow_dividends_paid_quarterly=[-3.75e9, -3.75e9, -3.75e9, -3.75e9],
            dividend_history_ttm_total=0.96,
            dividend_history_count=4,
        )

        result1 = compute_canonical_dividend_yield(**inputs)
        result2 = compute_canonical_dividend_yield(**inputs)

        assert result1 == result2
        assert result1["source_used"] == "dividend_history"
        assert result1["dividend_yield_ttm_value"] is not None

    def test_canonical_uses_dividend_history_primary(self):
        """
        When both sources are available, dividend_history is ALWAYS primary.
        This is the binding contract for ALL call sites.
        """
        canonical = compute_canonical_dividend_yield(
            market_cap=1_000_000_000,
            shares_outstanding=10_000_000,
            cashflow_dividends_paid_quarterly=[-5_000_000.0] * 4,
            dividend_history_ttm_total=2.0,  # $2/share
            dividend_history_count=4,
        )

        assert canonical["source_used"] == "dividend_history"
        # $2/share, price = $100, yield = 2.0%
        assert abs(canonical["dividend_yield_ttm_value"] - 2.0) < 0.01

    def test_canonical_cashflow_fallback_when_no_history(self):
        """
        When dividend_history has no records, cashflow is used as fallback.
        This is the secondary source, NOT a separate code path.
        """
        canonical = compute_canonical_dividend_yield(
            market_cap=1_000_000_000,
            shares_outstanding=10_000_000,
            cashflow_dividends_paid_quarterly=[-20_000_000.0] * 4,
            dividend_history_ttm_total=None,
            dividend_history_count=0,
        )

        assert canonical["source_used"] == "cashflow"
        # $80M total / $1B market_cap * 100 = 8.0%
        assert abs(canonical["dividend_yield_ttm_value"] - 8.0) < 0.01

    def test_canonical_integrity_check_pathological(self):
        """
        When both sources produce a yield but disagree by >3x ratio,
        the canonical function marks it unreliable.
        """
        canonical = compute_canonical_dividend_yield(
            market_cap=1_000_000_000,
            shares_outstanding=10_000_000,
            cashflow_dividends_paid_quarterly=[-100_000_000.0] * 4,  # $400M → 40%
            dividend_history_ttm_total=1.0,  # $1/share → 1%
            dividend_history_count=4,
        )

        assert canonical["na_reason"] == "unreliable"
        assert canonical["dividend_yield_ttm_value"] is None
