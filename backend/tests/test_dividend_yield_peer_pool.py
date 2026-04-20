"""
Regression tests: Dividend Yield (TTM) peer median pool — dividend_history only.

Source policy: dividend_history is the ONLY production source.

Peer pool classification:
- **Payer**: dividend_history records with positive TTM total → computed yield
- **Proven non-payer**: dividend_history synced, records sum to $0 → yield 0.0
- **Excluded (missing data)**: no dividend_history records (count == 0)
- **Excluded (extreme)**: yield > 100%

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
# compute_peer_benchmarks_v3
# ---------------------------------------------------------------------------
def _compute_dividend_yield_for_benchmark(
    *,
    market_cap,
    shares_outstanding,
    dividend_history_ttm_total,
    dividend_history_count,
    cashflow_dividends_paid_quarterly=None,
):
    """Replicate the logic from compute_peer_benchmarks_v3.

    Returns a dividend yield value or None if excluded from the peer pool.
    Source: dividend_history ONLY.
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
    - n_div_payers tickers pay dividends (have dividend_history records)
    - n_proven_non_payers have dividend_history synced but sum=0
      (proven non-payer via dividend_history evidence)
    - The rest have no dividend_history records at all
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
                "dividend_history_ttm_total": div_per_share,
                "dividend_history_count": 4,
                "expected_in_pool": True,
            })
        elif i < n_div_payers + n_proven_non_payers:
            # Proven non-payer: dividend_history synced, total = 0
            tickers.append({
                "ticker": ticker_name,
                "sector": "Financial Services",
                "industry": "Asset Management",
                "financial_currency": "USD",
                "market_cap": market_cap,
                "shares_outstanding": shares,
                "dividend_history_ttm_total": 0.0,
                "dividend_history_count": 1,  # synced, zero-sum records
                "expected_in_pool": True,
            })
        else:
            # Missing dividend data: no dividend_history at all
            tickers.append({
                "ticker": ticker_name,
                "sector": "Financial Services",
                "industry": "Asset Management",
                "financial_currency": "USD",
                "market_cap": market_cap,
                "shares_outstanding": shares,
                "dividend_history_ttm_total": None,
                "dividend_history_count": 0,
                "expected_in_pool": False,
            })
    return tickers


class TestDividendYieldPeerPool:
    """Dividend yield peer pool tests — dividend_history only."""

    def test_dividend_payer_included(self):
        """Ticker with dividend_history records → yield computed → included."""
        result = _compute_dividend_yield_for_benchmark(
            market_cap=100e9,
            shares_outstanding=1e9,
            dividend_history_ttm_total=2.0,
            dividend_history_count=4,
        )
        assert result is not None
        assert 1.9 < result < 2.1

    def test_proven_non_payer_included(self):
        """Dividend_history synced with sum=0 → yield 0.0 → included."""
        result = _compute_dividend_yield_for_benchmark(
            market_cap=1e9,
            shares_outstanding=1e7,
            dividend_history_ttm_total=0.0,
            dividend_history_count=1,
        )
        assert result == 0.0

    def test_missing_data_no_history_excluded(self):
        """No dividend_history records → missing_inputs → excluded."""
        result = _compute_dividend_yield_for_benchmark(
            market_cap=1e9,
            shares_outstanding=1e7,
            dividend_history_ttm_total=None,
            dividend_history_count=0,
        )
        assert result is None

    def test_cashflow_positive_but_no_history_excluded(self):
        """Cashflow shows dividends but no history → still excluded."""
        result = _compute_dividend_yield_for_benchmark(
            market_cap=1e9,
            shares_outstanding=1e7,
            cashflow_dividends_paid_quarterly=[-0.5e6, -0.5e6, -0.5e6, -0.5e6],
            dividend_history_ttm_total=None,
            dividend_history_count=0,
        )
        assert result is None

    def test_extreme_outlier_excluded(self):
        """Yield > 100% → excluded."""
        result = _compute_dividend_yield_for_benchmark(
            market_cap=1e6,
            shares_outstanding=1e6,
            dividend_history_ttm_total=2.0,
            dividend_history_count=4,
        )
        assert result is None


class TestLargeSectorDividendMedian:
    """Regression: pool must only include tickers with dividend_history."""

    def test_sector_pool_excludes_unproven_tickers(self):
        """50 tickers: 8 payers, 20 proven non-payers, 22 missing.
        Pool = 28 (not 50)."""
        tickers = _build_financial_services_sector(
            n_total=50, n_div_payers=8, n_proven_non_payers=20
        )

        in_pool = []
        excluded = []
        for t in tickers:
            dy = _compute_dividend_yield_for_benchmark(
                market_cap=t["market_cap"],
                shares_outstanding=t["shares_outstanding"],
                dividend_history_ttm_total=t["dividend_history_ttm_total"],
                dividend_history_count=t["dividend_history_count"],
            )
            if dy is not None and dy >= 0:
                in_pool.append(dy)
            else:
                excluded.append(t["ticker"])

        assert len(in_pool) == 28
        assert len(excluded) == 22

    def test_pool_meets_min_peer_count(self):
        """Proven pool is large enough even without missing tickers."""
        tickers = _build_financial_services_sector(
            n_total=50, n_div_payers=8, n_proven_non_payers=20
        )

        dividend_yields = []
        for t in tickers:
            dy = _compute_dividend_yield_for_benchmark(
                market_cap=t["market_cap"],
                shares_outstanding=t["shares_outstanding"],
                dividend_history_ttm_total=t["dividend_history_ttm_total"],
                dividend_history_count=t["dividend_history_count"],
            )
            if dy is not None and dy >= 0:
                dividend_yields.append(dy)

        assert len(dividend_yields) >= MIN_PEER_COUNT

    def test_sector_median_reflects_proven_data_only(self):
        """Median of 28 tickers: 8 payers + 20 zeros → median = 0.0."""
        tickers = _build_financial_services_sector(
            n_total=50, n_div_payers=8, n_proven_non_payers=20
        )

        dividend_yields = []
        for t in tickers:
            dy = _compute_dividend_yield_for_benchmark(
                market_cap=t["market_cap"],
                shares_outstanding=t["shares_outstanding"],
                dividend_history_ttm_total=t["dividend_history_ttm_total"],
                dividend_history_count=t["dividend_history_count"],
            )
            if dy is not None and dy >= 0:
                dividend_yields.append(dy)

        assert len(dividend_yields) == 28
        median_all = statistics.median(dividend_yields)
        assert median_all == 0.0

        payers_only = [y for y in dividend_yields if y > 0]
        assert len(payers_only) == 8
        median_payers = statistics.median(payers_only) if len(payers_only) >= MIN_DIVIDEND_PAYERS else None
        assert median_payers is not None and median_payers > 0

    def test_industry_with_only_missing_data(self):
        """All tickers have no dividend_history → empty pool."""
        tickers = _build_financial_services_sector(
            n_total=10, n_div_payers=0, n_proven_non_payers=0
        )

        dividend_yields = []
        for t in tickers:
            dy = _compute_dividend_yield_for_benchmark(
                market_cap=t["market_cap"],
                shares_outstanding=t["shares_outstanding"],
                dividend_history_ttm_total=t["dividend_history_ttm_total"],
                dividend_history_count=t["dividend_history_count"],
            )
            if dy is not None and dy >= 0:
                dividend_yields.append(dy)

        assert len(dividend_yields) == 0

    def test_sector_with_proven_data_mostly_non_payers(self):
        """100 tickers: 5 payers, 80 proven non-payers, 15 missing."""
        tickers = _build_financial_services_sector(
            n_total=100, n_div_payers=5, n_proven_non_payers=80
        )

        dividend_yields = []
        for t in tickers:
            dy = _compute_dividend_yield_for_benchmark(
                market_cap=t["market_cap"],
                shares_outstanding=t["shares_outstanding"],
                dividend_history_ttm_total=t["dividend_history_ttm_total"],
                dividend_history_count=t["dividend_history_count"],
            )
            if dy is not None and dy >= 0:
                dividend_yields.append(dy)

        assert len(dividend_yields) == 85
        assert statistics.median(dividend_yields) == 0.0


class TestAuditCSVClassification:
    """Tests for audit endpoint classification — dividend_history only."""

    @staticmethod
    def _classify(*, market_cap, shares_outstanding,
                  dividend_history_ttm_total, dividend_history_count,
                  cashflow_dividends_paid_quarterly=None,
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
        if na_reason == "extreme_outlier":
            return False, "extreme_outlier_gt_100pct"
        return False, f"na_reason={na_reason}"

    def test_payer_included(self):
        included, reason = self._classify(
            market_cap=100e9, shares_outstanding=1e9,
            dividend_history_ttm_total=2.0, dividend_history_count=4,
        )
        assert included is True
        assert reason == ""

    def test_proven_non_payer_included(self):
        """Dividend_history synced with sum=0 → included as 0.0."""
        included, reason = self._classify(
            market_cap=1e9, shares_outstanding=1e7,
            dividend_history_ttm_total=0.0, dividend_history_count=1,
        )
        assert included is True
        assert reason == ""

    def test_missing_data_excluded(self):
        included, reason = self._classify(
            market_cap=1e9, shares_outstanding=1e7,
            dividend_history_ttm_total=None, dividend_history_count=0,
        )
        assert included is False
        assert reason == "missing_dividend_data"

    def test_extreme_outlier_excluded(self):
        included, reason = self._classify(
            market_cap=1e6, shares_outstanding=1e6,
            dividend_history_ttm_total=2.0, dividend_history_count=4,
        )
        assert included is False
        assert reason == "extreme_outlier_gt_100pct"

    def test_not_visible_excluded(self):
        included, reason = self._classify(
            market_cap=1e9, shares_outstanding=1e7,
            dividend_history_ttm_total=0.0, dividend_history_count=1,
            is_visible=False,
        )
        assert included is False
        assert reason == "not_visible"

    def test_cashflow_positive_no_history_excluded(self):
        """Cashflow shows dividends but no history → excluded."""
        included, reason = self._classify(
            market_cap=100e9, shares_outstanding=1e9,
            cashflow_dividends_paid_quarterly=[-0.5e9, -0.5e9, -0.5e9, -0.5e9],
            dividend_history_ttm_total=None, dividend_history_count=0,
        )
        assert included is False
        assert reason == "missing_dividend_data"

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
                dividend_history_ttm_total=t["dividend_history_ttm_total"],
                dividend_history_count=t["dividend_history_count"],
            )
            if inc:
                included += 1
            else:
                excl_counts[reason] = excl_counts.get(reason, 0) + 1

        assert included == 28
        assert excl_counts.get("missing_dividend_data", 0) == 22
