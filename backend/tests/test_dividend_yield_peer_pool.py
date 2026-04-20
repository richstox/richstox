"""
Regression tests: Dividend Yield (TTM) peer median pool — evidence-based.

Verifies that compute_peer_benchmarks_v3 only includes tickers in the
dividend yield peer pool when there is PROOF of their dividend status:

- **Proven payer**: canonical function returns a positive dividend_yield_ttm_value
- **Proven non-payer**: canonical function returns dividend_yield_ttm_value=0.0
  with na_reason="no_dividend" (cashflow explicitly reports $0 dividends_paid)
- **Excluded (missing data)**: canonical returns na_reason="missing_inputs"
  when ALL cashflow dividendsPaid are null AND no dividend_history records exist.
  These tickers are NOT coerced to 0.0 because we cannot prove non-payer status.
- **Excluded (unreliable)**: cashflow and dividend_history disagree
- **Excluded (extreme)**: yield > 100%

Proof requirement: the 0.0 coercion is ONLY allowed when the cashflow
statement explicitly reports dividendsPaid as $0 (not null).  Null means
"field not reported" — it could be a non-payer OR data ingestion gap.

Run:
    cd /app/backend && python -m pytest tests/test_dividend_yield_peer_pool.py -v
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import statistics
from canonical_dividend import compute_canonical_dividend_yield


# ---------------------------------------------------------------------------
# Constants matching key_metrics_service.py
# ---------------------------------------------------------------------------
MIN_PEER_COUNT = 5
MIN_DIVIDEND_PAYERS = 5


# ---------------------------------------------------------------------------
# Helper: simulate the per-ticker dividend yield assignment from
# compute_peer_benchmarks_v3 (evidence-based version)
# ---------------------------------------------------------------------------
def _compute_dividend_yield_for_benchmark(
    *,
    market_cap,
    shares_outstanding,
    cashflow_dividends_paid_quarterly,
    dividend_history_ttm_total,
    dividend_history_count,
):
    """Replicate the evidence-based logic from compute_peer_benchmarks_v3.

    Returns a dividend yield value or None if excluded from the peer pool.
    Only returns 0.0 when there is PROOF of non-payer status (cashflow
    explicitly reports $0 dividends_paid, or canonical function determines
    no_dividend from available data).
    """
    canonical = compute_canonical_dividend_yield(
        market_cap=market_cap,
        shares_outstanding=shares_outstanding,
        cashflow_dividends_paid_quarterly=cashflow_dividends_paid_quarterly,
        dividend_history_ttm_total=dividend_history_ttm_total,
        dividend_history_count=dividend_history_count,
    )
    if canonical["dividend_yield_ttm_value"] is not None:
        return canonical["dividend_yield_ttm_value"]
    # EVIDENCE-BASED: do NOT coerce missing_inputs to 0.0.
    # missing_inputs means ALL cashflow dividendsPaid are null AND no
    # dividend_history — we cannot prove this ticker is a non-payer.
    return None


# ---------------------------------------------------------------------------
# Simulated sector: Financial Services with realistic dividend data
# ---------------------------------------------------------------------------
def _build_financial_services_sector(
    n_total=50,
    n_div_payers=8,
    n_proven_non_payers=20,
):
    """
    Build a realistic sector where:
    - n_div_payers tickers pay dividends (have dividend_history + cashflow)
    - n_proven_non_payers have cashflow with explicit $0 dividends_paid
      (proven non-payer via cashflow evidence)
    - The rest have ALL-null cashflow dividendsPaid + no dividend_history
      (missing dividend data — excluded from pool)
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
                "expected_in_pool": True,
            })
        elif i < n_div_payers + n_proven_non_payers:
            # Proven non-payer: cashflow explicitly reports $0 dividends_paid
            tickers.append({
                "ticker": ticker_name,
                "sector": "Financial Services",
                "industry": "Asset Management",
                "financial_currency": "USD",
                "market_cap": market_cap,
                "shares_outstanding": shares,
                "cashflow_dividends_paid_quarterly": [0, 0, 0, 0],
                "dividend_history_ttm_total": None,
                "dividend_history_count": 0,
                "expected_in_pool": True,
            })
        else:
            # Missing dividend data: ALL cashflow dividendsPaid null, no history
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
                "expected_in_pool": False,  # excluded — cannot prove non-payer
            })
    return tickers


class TestDividendYieldPeerPool:
    """Evidence-based dividend yield peer pool tests."""

    def test_proven_non_payer_via_cashflow_zero(self):
        """Ticker with cashflow explicitly reporting $0 dividends_paid
        in all 4 quarters → proven non-payer → yield 0.0 in pool."""
        result = _compute_dividend_yield_for_benchmark(
            market_cap=1e9,
            shares_outstanding=1e7,
            cashflow_dividends_paid_quarterly=[0, 0, 0, 0],
            dividend_history_ttm_total=None,
            dividend_history_count=0,
        )
        assert result == 0.0, (
            "Cashflow reports $0 → proven non-payer → should be 0.0 in pool"
        )

    def test_missing_data_all_null_cashflow_excluded(self):
        """Ticker with ALL cashflow dividendsPaid null AND no dividend_history
        → missing_inputs → excluded from pool (not coerced to 0.0)."""
        result = _compute_dividend_yield_for_benchmark(
            market_cap=1e9,
            shares_outstanding=1e7,
            cashflow_dividends_paid_quarterly=[None, None, None, None],
            dividend_history_ttm_total=None,
            dividend_history_count=0,
        )
        assert result is None, (
            "All-null cashflow + no history → missing data → must be excluded"
        )

    def test_missing_data_no_cashflow_excluded(self):
        """Ticker with fewer than 4 cashflow quarters and no dividend_history
        → missing_inputs → excluded from pool."""
        result = _compute_dividend_yield_for_benchmark(
            market_cap=1e9,
            shares_outstanding=1e7,
            cashflow_dividends_paid_quarterly=[],
            dividend_history_ttm_total=None,
            dividend_history_count=0,
        )
        assert result is None, (
            "No cashflow data + no history → missing data → must be excluded"
        )

    def test_unreliable_tickers_still_excluded(self):
        """Tickers where cashflow implies implausible yield (ONFO-like)
        are excluded via extreme_outlier guardrail, not 'unreliable'."""
        result = _compute_dividend_yield_for_benchmark(
            market_cap=3.9e6,
            shares_outstanding=1e6,
            cashflow_dividends_paid_quarterly=[-99e6, -99e6, None, None],
            dividend_history_ttm_total=None,
            dividend_history_count=0,
        )
        assert result is None, "Extreme cashflow yield (>100%) should be excluded"

    def test_extreme_outlier_still_excluded(self):
        """Yield > 100% should remain excluded."""
        result = _compute_dividend_yield_for_benchmark(
            market_cap=1e6,
            shares_outstanding=1e6,
            cashflow_dividends_paid_quarterly=[None, None, None, None],
            dividend_history_ttm_total=2.0,
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

    def test_mixed_cashflow_some_null_treated_as_zero(self):
        """Cashflow with mix of $0 and None quarters → proven non-payer.
        The canonical function sums non-None values; if all are $0, yield=0.0."""
        result = _compute_dividend_yield_for_benchmark(
            market_cap=1e9,
            shares_outstanding=1e7,
            cashflow_dividends_paid_quarterly=[0, None, 0, None],
            dividend_history_ttm_total=None,
            dividend_history_count=0,
        )
        assert result == 0.0, (
            "Mix of $0 and None → cashflow reports $0 total → proven non-payer"
        )


class TestLargeSectorDividendMedian:
    """Regression: pool must only include tickers with proven dividend status."""

    def test_sector_pool_excludes_unproven_tickers(self):
        """A sector with 50 tickers:
        - 8 payers (proven via dividend_history)
        - 20 proven non-payers (cashflow reports $0)
        - 22 with missing data (ALL-null cashflow + no history)

        Pool should contain 28 tickers (not 50, not 8)."""
        tickers = _build_financial_services_sector(
            n_total=50, n_div_payers=8, n_proven_non_payers=20
        )

        in_pool = []
        excluded = []
        for t in tickers:
            dy = _compute_dividend_yield_for_benchmark(
                market_cap=t["market_cap"],
                shares_outstanding=t["shares_outstanding"],
                cashflow_dividends_paid_quarterly=t["cashflow_dividends_paid_quarterly"],
                dividend_history_ttm_total=t["dividend_history_ttm_total"],
                dividend_history_count=t["dividend_history_count"],
            )
            if dy is not None and dy >= 0:
                in_pool.append(dy)
            else:
                excluded.append(t["ticker"])

        expected_in_pool = 8 + 20  # payers + proven non-payers
        assert len(in_pool) == expected_in_pool, (
            f"Expected {expected_in_pool} in pool (8 payers + 20 proven non-payers), "
            f"got {len(in_pool)}"
        )
        assert len(excluded) == 22, (
            f"Expected 22 excluded (missing data), got {len(excluded)}"
        )

    def test_pool_meets_min_peer_count(self):
        """Even without unproven tickers, the proven pool is large enough."""
        tickers = _build_financial_services_sector(
            n_total=50, n_div_payers=8, n_proven_non_payers=20
        )

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

        assert len(dividend_yields) >= MIN_PEER_COUNT, (
            f"Pool size {len(dividend_yields)} below MIN_PEER_COUNT={MIN_PEER_COUNT}"
        )

    def test_sector_median_reflects_proven_data_only(self):
        """Median should reflect only tickers with proven status."""
        tickers = _build_financial_services_sector(
            n_total=50, n_div_payers=8, n_proven_non_payers=20
        )

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

        # 28 tickers: 8 payers + 20 zeros → median should be 0.0
        median_all = statistics.median(dividend_yields)
        assert median_all == 0.0, f"Expected median_all=0.0, got {median_all}"
        # But n=28, not n=7 or n=50
        assert len(dividend_yields) == 28

        # median_payers should be positive
        payers_only = [y for y in dividend_yields if y > 0]
        assert len(payers_only) == 8
        median_payers = statistics.median(payers_only) if len(payers_only) >= MIN_DIVIDEND_PAYERS else None
        assert median_payers is not None and median_payers > 0

    def test_industry_with_only_missing_data(self):
        """An industry where ALL tickers have all-null cashflow + no dividend
        history should produce NO pool entries (not artificial zeros)."""
        tickers = _build_financial_services_sector(
            n_total=10, n_div_payers=0, n_proven_non_payers=0
        )

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

        # ALL tickers have missing data → pool is empty
        assert len(dividend_yields) == 0, (
            f"Expected empty pool (all tickers have missing data), got {len(dividend_yields)}"
        )

    def test_sector_with_proven_data_mostly_non_payers(self):
        """Sector with 100 tickers: 5 payers, 80 proven non-payers,
        15 missing data. Pool=85, median=0.0 with large n."""
        tickers = _build_financial_services_sector(
            n_total=100, n_div_payers=5, n_proven_non_payers=80
        )

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

        assert len(dividend_yields) == 85  # 5 + 80
        assert statistics.median(dividend_yields) == 0.0
        # Confirm it's not n=5 (the old bug)
        assert len(dividend_yields) > MIN_PEER_COUNT


class TestAuditCSVClassification:
    """Tests for the dividend yield audit CSV classification logic.

    These test the exact same per-ticker classification logic used by
    the admin audit CSV endpoint to label each ticker as:
      - included_in_peer_pool=True  (proven payer or proven non-payer)
      - excluded_reason=missing_dividend_data
      - excluded_reason=unreliable_sources_disagree
      - excluded_reason=extreme_outlier_gt_100pct
    """

    @staticmethod
    def _classify(*, market_cap, shares_outstanding,
                  cashflow_dividends_paid_quarterly,
                  dividend_history_ttm_total, dividend_history_count,
                  is_visible=True, fundamentals_status="complete",
                  has_price=True, has_shares=True):
        """Replicate the audit endpoint classification logic."""
        canonical = compute_canonical_dividend_yield(
            market_cap=market_cap,
            shares_outstanding=shares_outstanding,
            cashflow_dividends_paid_quarterly=cashflow_dividends_paid_quarterly,
            dividend_history_ttm_total=dividend_history_ttm_total,
            dividend_history_count=dividend_history_count,
        )
        dy_value = canonical["dividend_yield_ttm_value"]
        na_reason = canonical["na_reason"]

        if not is_visible:
            return False, "not_visible"
        if fundamentals_status != "complete":
            return False, "fundamentals_incomplete"
        if not has_price:
            return False, "no_price_data"
        if not has_shares:
            return False, "no_shares_outstanding"
        if dy_value is not None:
            return True, ""
        if na_reason == "missing_inputs":
            return False, "missing_dividend_data"
        if na_reason == "unreliable":
            return False, "unreliable_sources_disagree"
        if na_reason == "extreme_outlier":
            return False, "extreme_outlier_gt_100pct"
        return False, f"na_reason={na_reason}"

    def test_proven_payer_included(self):
        """Dividend payer with both sources agreeing → included."""
        included, reason = self._classify(
            market_cap=100e9, shares_outstanding=1e9,
            cashflow_dividends_paid_quarterly=[-0.5e9, -0.5e9, -0.5e9, -0.5e9],
            dividend_history_ttm_total=2.0, dividend_history_count=4,
        )
        assert included is True
        assert reason == ""

    def test_proven_non_payer_included(self):
        """Cashflow explicitly $0 in all quarters → included as 0.0."""
        included, reason = self._classify(
            market_cap=1e9, shares_outstanding=1e7,
            cashflow_dividends_paid_quarterly=[0, 0, 0, 0],
            dividend_history_ttm_total=None, dividend_history_count=0,
        )
        assert included is True
        assert reason == ""

    def test_missing_data_excluded(self):
        """ALL cashflow null + no dividend_history → missing_dividend_data."""
        included, reason = self._classify(
            market_cap=1e9, shares_outstanding=1e7,
            cashflow_dividends_paid_quarterly=[None, None, None, None],
            dividend_history_ttm_total=None, dividend_history_count=0,
        )
        assert included is False
        assert reason == "missing_dividend_data"

    def test_unreliable_excluded(self):
        """ONFO-like: Cashflow implies implausible yield → extreme_outlier_gt_100pct.
        
        With the fixed logic, cashflow is used as the source when history is absent.
        The extreme cashflow yield (~5077%) exceeds the 100% cap → extreme_outlier.
        """
        included, reason = self._classify(
            market_cap=3.9e6, shares_outstanding=1e6,
            cashflow_dividends_paid_quarterly=[-99e6, -99e6, None, None],
            dividend_history_ttm_total=None, dividend_history_count=0,
        )
        assert included is False
        assert reason == "extreme_outlier_gt_100pct"

    def test_true_unreliable_both_sources_disagree(self):
        """True unreliable: BOTH sources produce yields but disagree by >3x ratio."""
        # hist_yield = 1.0%, cashflow_yield = 5.0% → ratio = 5.0x > 3.0x → unreliable
        included, reason = self._classify(
            market_cap=100e9, shares_outstanding=1e9,
            cashflow_dividends_paid_quarterly=[-1.25e9, -1.25e9, -1.25e9, -1.25e9],
            dividend_history_ttm_total=1.0, dividend_history_count=4,
        )
        assert included is False
        assert reason == "unreliable_sources_disagree"

    def test_normal_disagreement_still_included(self):
        """Normal time-window disagreement (2.5x, <3x) → use dividend_history → included."""
        # hist_yield = 2.0%, cashflow_yield = 5.0% → ratio = 2.5x < 3.0x → use history
        included, reason = self._classify(
            market_cap=100e9, shares_outstanding=1e9,
            cashflow_dividends_paid_quarterly=[-1.25e9, -1.25e9, -1.25e9, -1.25e9],
            dividend_history_ttm_total=2.0, dividend_history_count=4,
        )
        assert included is True
        assert reason == ""

    def test_extreme_outlier_excluded(self):
        """Yield > 100% → extreme_outlier_gt_100pct."""
        included, reason = self._classify(
            market_cap=1e6, shares_outstanding=1e6,
            cashflow_dividends_paid_quarterly=[None, None, None, None],
            dividend_history_ttm_total=2.0, dividend_history_count=4,
        )
        assert included is False
        assert reason == "extreme_outlier_gt_100pct"

    def test_not_visible_excluded(self):
        """Non-visible ticker excluded regardless of data."""
        included, reason = self._classify(
            market_cap=1e9, shares_outstanding=1e7,
            cashflow_dividends_paid_quarterly=[0, 0, 0, 0],
            dividend_history_ttm_total=None, dividend_history_count=0,
            is_visible=False,
        )
        assert included is False
        assert reason == "not_visible"

    def test_sector_classification_summary(self):
        """Full sector classification matches expected breakdown."""
        tickers = _build_financial_services_sector(
            n_total=50, n_div_payers=8, n_proven_non_payers=20,
        )

        included = 0
        excl_counts = {}
        for t in tickers:
            inc, reason = self._classify(
                market_cap=t["market_cap"],
                shares_outstanding=t["shares_outstanding"],
                cashflow_dividends_paid_quarterly=t["cashflow_dividends_paid_quarterly"],
                dividend_history_ttm_total=t["dividend_history_ttm_total"],
                dividend_history_count=t["dividend_history_count"],
            )
            if inc:
                included += 1
            else:
                excl_counts[reason] = excl_counts.get(reason, 0) + 1

        assert included == 28, f"Expected 28 included (8 payers + 20 proven), got {included}"
        assert excl_counts.get("missing_dividend_data", 0) == 22, (
            f"Expected 22 missing_dividend_data, got {excl_counts}"
        )
