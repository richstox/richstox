"""
Regression tests for the yellow-banner fallback logic.

The ticker page shows a yellow "Insufficient data at all levels" banner
when `has_benchmark` is False.  The banner must respect the
Industry → Sector → Market fallback chain so that if at least one
relevant page metric has a benchmark at *any* level, the banner is
suppressed.

Run:
    python -m pytest tests/test_banner_fallback.py -v
"""

import pytest

# ---------------------------------------------------------------------------
# Inline replica of the banner-decision logic used by *both* endpoints:
#   /api/stock-overview/{ticker}   (has_benchmark + benchmark_fallback)
#   /api/v1/ticker/{ticker}/detail (per-metric _s4_median_with_fallback)
# ---------------------------------------------------------------------------
_MIN_BM_N = 5
_BM_CHECK_METRICS = [
    "net_margin_ttm", "fcf_yield", "net_debt_ebitda",
    "revenue_growth_3y", "dividend_yield_ttm",
]


def _evaluate_banner(s4_industry: dict, s4_sector: dict, s4_market: dict):
    """
    Return (has_benchmark, benchmark_fallback, yellow_banner_should_show).

    Mirrors the logic in server.py get_stock_overview and the new endpoint.
    """
    levels = [
        (s4_industry, "industry"),
        (s4_sector, "sector"),
        (s4_market, "market"),
    ]
    has_peer_benchmark = False
    fallback_levels_used: set = set()

    for mk in _BM_CHECK_METRICS:
        for s4_data, level in levels:
            entry = s4_data.get(mk) or {}
            n = entry.get("n_used")
            med = entry.get("median")
            if med is not None and n is not None and n >= _MIN_BM_N:
                has_peer_benchmark = True
                fallback_levels_used.add(level)
                break  # found for this metric, stop walking chain

    fallback_level = None
    if fallback_levels_used:
        if "market" in fallback_levels_used:
            fallback_level = "market"
        elif "sector" in fallback_levels_used:
            fallback_level = "sector"
        else:
            fallback_level = "industry"

    return has_peer_benchmark, fallback_level, not has_peer_benchmark


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
class TestBannerFallback:
    """Yellow banner must respect industry → sector → market fallback."""

    def test_industry_sufficient_no_banner(self):
        """Industry has n >= 5 for one metric → no banner."""
        ind = {"net_margin_ttm": {"median": 12.5, "n_used": 8}}
        has_bm, fb, banner = _evaluate_banner(ind, {}, {})
        assert has_bm is True
        assert fb == "industry"
        assert banner is False

    def test_industry_insufficient_sector_sufficient_no_banner(self):
        """
        Industry has n < 5 but sector has n >= 5 for at least one metric.
        Banner must NOT show.  (This was the GOOG/AAPL bug.)
        """
        ind = {"net_margin_ttm": {"median": 12.5, "n_used": 3}}  # < 5
        sec = {"net_margin_ttm": {"median": 13.0, "n_used": 15}}
        has_bm, fb, banner = _evaluate_banner(ind, sec, {})
        assert has_bm is True
        assert fb == "sector"
        assert banner is False

    def test_industry_and_sector_insufficient_market_sufficient_no_banner(self):
        """
        Industry and sector both n < 5, but market has n >= 5.
        Banner must NOT show.
        """
        ind = {"net_margin_ttm": {"median": 12.5, "n_used": 2}}
        sec = {"net_margin_ttm": {"median": 13.0, "n_used": 4}}
        mkt = {"net_margin_ttm": {"median": 11.0, "n_used": 500}}
        has_bm, fb, banner = _evaluate_banner(ind, sec, mkt)
        assert has_bm is True
        assert fb == "market"
        assert banner is False

    def test_all_levels_insufficient_banner_shows(self):
        """
        All levels have n < 5 for ALL relevant metrics.
        Banner must show.
        """
        ind = {"net_margin_ttm": {"median": 12.5, "n_used": 2}}
        sec = {"fcf_yield": {"median": 5.0, "n_used": 3}}
        mkt = {"revenue_growth_3y": {"median": 8.0, "n_used": 4}}
        has_bm, fb, banner = _evaluate_banner(ind, sec, mkt)
        assert has_bm is False
        assert fb is None
        assert banner is True

    def test_empty_docs_banner_shows(self):
        """No peer_benchmarks docs at all → banner shows."""
        has_bm, fb, banner = _evaluate_banner({}, {}, {})
        assert has_bm is False
        assert fb is None
        assert banner is True

    def test_mixed_metrics_different_levels(self):
        """
        One metric at industry, another only at market.
        Most degraded level = market.
        """
        ind = {"net_margin_ttm": {"median": 12.5, "n_used": 10}}
        sec = {}
        mkt = {"fcf_yield": {"median": 4.0, "n_used": 200}}
        has_bm, fb, banner = _evaluate_banner(ind, sec, mkt)
        assert has_bm is True
        assert fb == "market"  # most degraded level used
        assert banner is False

    def test_median_none_not_counted(self):
        """If median is None (even with n >= 5), metric is not counted."""
        ind = {"net_margin_ttm": {"median": None, "n_used": 10}}
        has_bm, _, banner = _evaluate_banner(ind, {}, {})
        assert has_bm is False
        assert banner is True

    def test_n_none_not_counted(self):
        """If n_used is None, metric is not counted."""
        ind = {"net_margin_ttm": {"median": 12.5, "n_used": None}}
        has_bm, _, banner = _evaluate_banner(ind, {}, {})
        assert has_bm is False
        assert banner is True
