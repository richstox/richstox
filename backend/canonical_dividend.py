"""
Canonical Dividend Yield Logic
==============================
ONE shared function for dividend yield TTM computation.

Call sites:
  1. Ticker detail Key Metrics → dividend_yield_ttm
  2. Earnings & Dividends → dividend status / "No dividends"
  3. Step 4 peer benchmarks → dividend_yield_ttm
  4. All admin endpoints that display dividend yield

Source: dividend_history ONLY
------------------------------
The SOLE production source for dividend yield is the EODHD dividend event
data stored in the ``dividend_history`` collection.

  EODHD dividend events → dividend_history → sum by period → yield from price.

Cashflow ``dividendsPaid`` fields are NEVER used for production dividend yield
computation. They may appear in ``debug_inputs`` for admin diagnostics only.

Guardrails:
  - >100% yield → extreme_outlier (last-resort safety net)
"""

import logging
from typing import Optional, Dict, Any, List

logger = logging.getLogger("richstox.canonical_dividend")

# Maximum plausible dividend yield (%)
MAX_DIVIDEND_YIELD_PCT = 100.0


def compute_canonical_dividend_yield(
    *,
    market_cap: Optional[float],
    shares_outstanding: Optional[float],
    cashflow_dividends_paid_quarterly: List[Optional[float]] = None,
    dividend_history_ttm_total: Optional[float],
    dividend_history_count: int = 0,
    include_debug: bool = False,
) -> Dict[str, Any]:
    """
    Canonical dividend yield TTM computation.

    SOURCE POLICY (binding for all call sites):
      - dividend_history is the ONLY production source.
      - If dividend_history has records in last 365 days → compute yield.
      - If dividend_history has zero records → no dividend data available.
      - Cashflow data is NEVER used for yield computation; debug-only.

    Parameters
    ----------
    market_cap : float | None
        Current market capitalisation (price × shares).
    shares_outstanding : float | None
        Current shares outstanding.
    cashflow_dividends_paid_quarterly : list[float | None] | None
        IGNORED for production computation.  Accepted only for debug_inputs
        output when ``include_debug=True``.
    dividend_history_ttm_total : float | None
        Sum of per-share dividend amounts from ``dividend_history`` collection
        for records with ex_date/date within the last 365 days.
        None if no records exist.
    dividend_history_count : int
        Number of ``dividend_history`` records in the last 365 days.
    include_debug : bool
        When True, attach ``debug_inputs`` dict (admin/debug only).

    Returns
    -------
    dict with keys:
        dividend_yield_ttm_value : float | None
        source_used : "dividend_history" | "none"
        na_reason : None | "no_dividend" | "missing_inputs" | "extreme_outlier"
        debug_inputs : dict | None   (only when include_debug=True)
    """
    result: Dict[str, Any] = {
        "dividend_yield_ttm_value": None,
        "source_used": "none",
        "na_reason": None,
        "debug_inputs": None,
    }

    # ── Compute yield from dividend_history ──────────────────────────────
    if (
        dividend_history_count > 0
        and dividend_history_ttm_total is not None
        and dividend_history_ttm_total > 0
        and shares_outstanding
        and shares_outstanding > 0
        and market_cap
        and market_cap > 0
    ):
        price = market_cap / shares_outstanding
        hist_yield = (dividend_history_ttm_total / price) * 100
        result["dividend_yield_ttm_value"] = hist_yield
        result["source_used"] = "dividend_history"

    elif (
        dividend_history_count > 0
        and dividend_history_ttm_total is not None
        and dividend_history_ttm_total == 0.0
    ):
        # dividend_history records exist but sum to zero (e.g. stock dividends)
        result["dividend_yield_ttm_value"] = 0.0
        result["source_used"] = "dividend_history"
        result["na_reason"] = "no_dividend"

    elif dividend_history_count == 0:
        # No dividend_history records → missing data
        result["na_reason"] = "missing_inputs"

    else:
        # Edge case: count > 0 but missing market_cap/shares
        result["na_reason"] = "missing_inputs"

    # ── Extreme outlier guardrail ───────────────────────────────────────
    if (
        result["dividend_yield_ttm_value"] is not None
        and result["dividend_yield_ttm_value"] > MAX_DIVIDEND_YIELD_PCT
    ):
        logger.info(
            "Dividend yield extreme outlier: %.2f%% > %.0f%% cap",
            result["dividend_yield_ttm_value"],
            MAX_DIVIDEND_YIELD_PCT,
        )
        result["dividend_yield_ttm_value"] = None
        result["na_reason"] = "extreme_outlier"

    # ── Explicit no_dividend when value is exactly 0 ────────────────────
    if (
        result["dividend_yield_ttm_value"] is not None
        and result["dividend_yield_ttm_value"] == 0.0
        and result["na_reason"] is None
    ):
        result["na_reason"] = "no_dividend"

    # ── Debug inputs (admin only) ───────────────────────────────────────
    if include_debug:
        # Cashflow data included for diagnostic comparison only — never
        # used for production yield.
        cf_vals = cashflow_dividends_paid_quarterly or []
        cf_non_none = [v for v in cf_vals if v is not None]
        cashflow_ttm = sum(abs(v) for v in cf_non_none) if cf_non_none else None
        cashflow_yield = None
        if cashflow_ttm and cashflow_ttm > 0 and market_cap and market_cap > 0:
            cashflow_yield = (cashflow_ttm / market_cap) * 100

        result["debug_inputs"] = {
            "market_cap": market_cap,
            "shares_outstanding": shares_outstanding,
            "dividend_history_ttm_total_per_share": dividend_history_ttm_total,
            "dividend_history_count": dividend_history_count,
            # Diagnostic only — not used for production yield:
            "cashflow_dividends_paid_quarterly": cashflow_dividends_paid_quarterly,
            "cashflow_ttm_diagnostic": cashflow_ttm,
            "cashflow_yield_pct_diagnostic": cashflow_yield,
        }

    return result
