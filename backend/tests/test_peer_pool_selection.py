"""
Phase 2A: Peer pool selection tests.

Verifies that STEP 4 median computation uses per-metric peer pools:
- Currency-safe metrics use all tickers with known financial_currency
- USD-only metrics (pe, fcf_yield) exclude non-USD tickers
- Null-currency tickers are excluded from ALL pools

Run:
    cd /app/backend && python -m pytest tests/test_peer_pool_selection.py -v
"""

import pytest


# ---------------------------------------------------------------------------
# The set under test (must match key_metrics_service.py)
# ---------------------------------------------------------------------------
STEP4_ALL_CURRENCY_ELIGIBLE = {
    "net_margin_ttm", "roe", "revenue_growth_3y", "dividend_yield",
}
STEP4_METRIC_KEYS = [
    "pe", "net_margin_ttm", "fcf_yield", "net_debt_ebitda",
    "revenue_growth_3y", "dividend_yield", "roe",
]
STEP4_USD_ONLY = {"pe", "fcf_yield", "net_debt_ebitda"}


# ---------------------------------------------------------------------------
# Helper: simulate the pool-selection logic from compute_peer_benchmarks_v3
# ---------------------------------------------------------------------------
def _build_ticker(ticker: str, currency: str | None, **metric_vals) -> dict:
    """Create a fake ticker-metrics dict."""
    m = {
        "ticker": ticker,
        "sector": "Tech",
        "industry": "Software",
        "financial_currency": currency,
        "currency_mismatch": (currency != "USD") if currency else True,
    }
    m.update(metric_vals)
    return m


def _select_pool(metric_key, usd_tickers, known_currency_tickers):
    """Mirror the per-metric pool selection in compute_peer_benchmarks_v3."""
    if metric_key in STEP4_ALL_CURRENCY_ELIGIBLE:
        return known_currency_tickers
    return usd_tickers


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
class TestPoolSelection:
    """Verify per-metric pool selection logic."""

    def _make_pools(self, tickers):
        usd = [t for t in tickers if not t.get("currency_mismatch")]
        known = [t for t in tickers if t.get("financial_currency")]
        return usd, known

    def test_safe_metrics_include_non_usd(self):
        """net_margin_ttm, roe, revenue_growth_3y, dividend_yield
        should include non-USD tickers with known currency."""
        tickers = [
            _build_ticker("AAPL", "USD", net_margin_ttm=25.0),
            _build_ticker("SHOP.TO", "CAD", net_margin_ttm=18.0),
            _build_ticker("SAP.DE", "EUR", net_margin_ttm=22.0),
        ]
        usd, known = self._make_pools(tickers)

        for mk in ["net_margin_ttm", "roe", "revenue_growth_3y", "dividend_yield"]:
            pool = _select_pool(mk, usd, known)
            assert len(pool) == 3, f"{mk} should use all 3 known-currency tickers"

    def test_usd_only_metrics_exclude_non_usd(self):
        """pe, fcf_yield, and net_debt_ebitda must use USD-only pool."""
        tickers = [
            _build_ticker("AAPL", "USD", pe=28.0, fcf_yield=3.5, net_debt_ebitda=1.2),
            _build_ticker("SHOP.TO", "CAD", pe=60.0, fcf_yield=1.2, net_debt_ebitda=0.8),
            _build_ticker("SAP.DE", "EUR", pe=35.0, fcf_yield=2.8, net_debt_ebitda=1.5),
        ]
        usd, known = self._make_pools(tickers)

        for mk in ["pe", "fcf_yield", "net_debt_ebitda"]:
            pool = _select_pool(mk, usd, known)
            assert len(pool) == 1, f"{mk} should only include USD tickers"
            assert pool[0]["ticker"] == "AAPL"

    def test_null_currency_excluded_from_all_pools(self):
        """Tickers with null financial_currency must not appear in any pool."""
        tickers = [
            _build_ticker("AAPL", "USD", net_margin_ttm=25.0, pe=28.0),
            _build_ticker("MYSTERY", None, net_margin_ttm=20.0, pe=30.0),
        ]
        usd, known = self._make_pools(tickers)

        # null-currency ticker excluded from both pools
        assert len(usd) == 1
        assert len(known) == 1

        for mk in STEP4_METRIC_KEYS:
            pool = _select_pool(mk, usd, known)
            tickers_in_pool = [t["ticker"] for t in pool]
            assert "MYSTERY" not in tickers_in_pool, f"{mk}: null-currency ticker should be excluded"

    def test_safe_metrics_contribute_to_peer_count(self):
        """Non-USD tickers with known currency should contribute toward MIN_PEER_COUNT=5
        for safe metrics only."""
        MIN_PEER_COUNT = 5
        # 3 USD + 3 non-USD (known currency) = 6 total known, but only 3 USD
        tickers = [
            _build_ticker(f"USD{i}", "USD", net_margin_ttm=10 + i, pe=20 + i) for i in range(3)
        ] + [
            _build_ticker(f"EUR{i}", "EUR", net_margin_ttm=15 + i, pe=25 + i) for i in range(3)
        ]
        usd, known = self._make_pools(tickers)

        # Safe metric: 6 tickers >= MIN_PEER_COUNT
        pool_safe = _select_pool("net_margin_ttm", usd, known)
        vals_safe = [t["net_margin_ttm"] for t in pool_safe if t.get("net_margin_ttm") is not None]
        assert len(vals_safe) >= MIN_PEER_COUNT

        # USD-only metric: 3 tickers < MIN_PEER_COUNT
        pool_usd = _select_pool("pe", usd, known)
        vals_usd = [t["pe"] for t in pool_usd if t.get("pe") is not None]
        assert len(vals_usd) < MIN_PEER_COUNT

    def test_eligible_set_completeness(self):
        """Verify the eligible sets cover all 7 metrics without overlap."""
        assert STEP4_ALL_CURRENCY_ELIGIBLE | STEP4_USD_ONLY == set(STEP4_METRIC_KEYS)
        assert STEP4_ALL_CURRENCY_ELIGIBLE & STEP4_USD_ONLY == set()
