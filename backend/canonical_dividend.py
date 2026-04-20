"""
Canonical Dividend Yield Logic
==============================
ONE shared function for dividend yield TTM computation.

Call sites:
  1. Ticker detail Key Metrics → dividend_yield_ttm
  2. Earnings & Dividends → dividend status / "No dividends"
  3. Step 4 peer benchmarks → dividend_yield_ttm

Source priority (best available):
----------------------------------
1. dividend_history (EODHD /div/ API — per-share payment records in last 365 days)
2. company_financials cashflow dividendsPaid (last 4 quarterly cash-flow statements)

Integrity check (when BOTH sources have data):
  - dividend_history is ALWAYS the canonical/primary source when it has data.
  - The cashflow comparison is a *sanity check*, not a veto:
    * When both sources produce positive yields (both agree: company IS a payer),
      use dividend_history regardless of magnitude difference. Time window
      misalignment between "last 365 days" and "last 4 quarterly reports"
      routinely causes 30-50% relative differences for normal quarterly payers
      (e.g. AAPL with 2 of 4 payments in the dividend_history window).
    * "unreliable" is reserved for truly pathological disagreements where the
      sources suggest fundamentally different conclusions:
      one says non-payer (yield=0) while the other says payer, or >3x ratio.

When ONLY cashflow is available (dividend_history has no records):
  - cashflow sum == 0 → proven non-payer (yield = 0.0, na_reason = "no_dividend")
  - cashflow sum  > 0 → use cashflow yield (approximation; documented)
  - all-null quarters → missing_inputs

Guardrails:
  - >100% yield → extreme_outlier (last-resort safety net)
"""

import logging
from typing import Optional, Dict, Any, List

logger = logging.getLogger("richstox.canonical_dividend")

# Pathological disagreement ratio: cashflow yield > 3x dividend_history yield
# (or vice versa) when both are positive → unreliable.
# Normal time-window misalignment for quarterly payers causes up to ~50% diff.
PATHOLOGICAL_RATIO = 3.0

# Maximum plausible dividend yield (%)
MAX_DIVIDEND_YIELD_PCT = 100.0


def compute_canonical_dividend_yield(
    *,
    market_cap: Optional[float],
    shares_outstanding: Optional[float],
    cashflow_dividends_paid_quarterly: List[Optional[float]],
    dividend_history_ttm_total: Optional[float],
    dividend_history_count: int = 0,
    include_debug: bool = False,
) -> Dict[str, Any]:
    """
    Canonical dividend yield TTM computation.

    SOURCE POLICY (binding for all call sites):
      1. If dividend_history has records in last 365 days → compute yield from that (canonical primary)
      2. If BOTH sources exist:
         - Both positive → use dividend_history (canonical) — time window differences are normal
         - One payer + one non-payer, or >3x ratio → na_reason="unreliable"
      3. If only cashflow available:
         - sum==0 → proven non-payer (yield=0.0)
         - sum>0 → use cashflow yield (approximation)
         - all-null → missing_inputs
      4. If neither source → missing_inputs

    Parameters
    ----------
    market_cap : float | None
        Current market capitalisation (price × shares).
    shares_outstanding : float | None
        Current shares outstanding.
    cashflow_dividends_paid_quarterly : list[float | None]
        Last 4 quarterly ``dividends_paid`` values from company_financials
        (total dollars, typically negative in GAAP — caller should pass raw values).
        Must contain exactly 4 entries (latest quarter first); None = unreported.
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
        source_used : "dividend_history" | "cashflow" | "none"
        na_reason : None | "no_dividend" | "missing_inputs" | "unreliable" | "extreme_outlier"
        debug_inputs : dict | None   (only when include_debug=True)
    """
    result: Dict[str, Any] = {
        "dividend_yield_ttm_value": None,
        "source_used": "none",
        "na_reason": None,
        "debug_inputs": None,
    }

    # ── Step 1: Compute cashflow-based yield ────────────────────────────
    cashflow_yield = None
    cashflow_ttm = None
    cf_vals = cashflow_dividends_paid_quarterly or []

    if len(cf_vals) >= 4:
        non_none = [v for v in cf_vals if v is not None]
        if non_none:
            # EXCEPTION: None quarters are skipped (company may pay annually).
            cashflow_ttm = sum(abs(v) for v in non_none)
            if cashflow_ttm > 0 and market_cap and market_cap > 0:
                cashflow_yield = (cashflow_ttm / market_cap) * 100
            elif cashflow_ttm == 0.0:
                cashflow_yield = 0.0

    # ── Step 2: Compute dividend_history-based yield ────────────────────
    hist_yield = None
    hist_ttm_dollars = None  # total dollar amount (per-share × shares)

    if (
        dividend_history_count > 0
        and dividend_history_ttm_total is not None
        and dividend_history_ttm_total > 0
        and shares_outstanding
        and shares_outstanding > 0
        and market_cap
        and market_cap > 0
    ):
        # Convert per-share total to full-company total for comparable scale
        hist_ttm_dollars = dividend_history_ttm_total * shares_outstanding
        price = market_cap / shares_outstanding
        hist_yield = (dividend_history_ttm_total / price) * 100

    # ── Step 3: Determine primary value + integrity check ───────────────

    if hist_yield is not None and cashflow_yield is not None:
        # Both sources available — dividend_history is ALWAYS the canonical primary.
        if hist_yield == 0.0 and cashflow_yield == 0.0:
            # Both say 0 — consistent non-payer
            result["dividend_yield_ttm_value"] = 0.0
            result["source_used"] = "dividend_history"
            result["na_reason"] = "no_dividend"
        elif hist_yield > 0 and cashflow_yield > 0:
            # Both sources agree the company IS a dividend payer.
            # Use dividend_history (canonical primary) regardless of magnitude.
            # Time window misalignment between "last 365 days" and "last 4 quarterly
            # reports" routinely causes large relative diffs for quarterly payers.
            min_yield = min(hist_yield, cashflow_yield)
            max_yield = max(hist_yield, cashflow_yield)
            ratio = max_yield / min_yield if min_yield > 0 else 999

            if ratio > PATHOLOGICAL_RATIO:
                # Pathological disagreement (>3x) — likely a data quality issue
                result["dividend_yield_ttm_value"] = None
                result["source_used"] = "none"
                result["na_reason"] = "unreliable"
                logger.info(
                    "Dividend yield pathological disagreement: "
                    "hist_yield=%.2f%% cashflow_yield=%.2f%% ratio=%.1fx",
                    hist_yield, cashflow_yield, ratio,
                )
            else:
                # Normal disagreement — use dividend_history (canonical primary)
                result["dividend_yield_ttm_value"] = hist_yield
                result["source_used"] = "dividend_history"
        else:
            # One says payer, other says non-payer — pathological mismatch
            result["dividend_yield_ttm_value"] = None
            result["source_used"] = "none"
            result["na_reason"] = "unreliable"
            logger.info(
                "Dividend yield payer/non-payer conflict: "
                "hist_yield=%.2f%% cashflow_yield=%.2f%%",
                hist_yield, cashflow_yield,
            )

    elif hist_yield is not None:
        # Only dividend_history available
        result["dividend_yield_ttm_value"] = hist_yield
        result["source_used"] = "dividend_history"

    elif cashflow_yield is not None:
        # Only cashflow available — dividend_history has no records or no total.
        # Use cashflow as the source (approximation: last 4 quarters / market_cap).
        # This is NOT "unreliable" — it just means dividend_history wasn't synced.
        result["dividend_yield_ttm_value"] = cashflow_yield
        result["source_used"] = "cashflow"

    else:
        # Neither source produced a yield
        if market_cap is None or market_cap <= 0:
            result["na_reason"] = "missing_inputs"
        elif len(cf_vals) < 4:
            result["na_reason"] = "missing_inputs"
        else:
            # Have 4+ quarters but all zeros / all None
            all_none = all(v is None for v in cf_vals)
            if all_none and dividend_history_count == 0:
                # No cashflow data, no dividend_history → truly missing
                result["na_reason"] = "missing_inputs"
            else:
                result["dividend_yield_ttm_value"] = 0.0
                result["source_used"] = "cashflow"
                result["na_reason"] = "no_dividend"

    # ── Step 4: Extreme outlier guardrail ───────────────────────────────
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

    # ── Step 5: Explicit no_dividend when value is exactly 0 ────────────
    if (
        result["dividend_yield_ttm_value"] is not None
        and result["dividend_yield_ttm_value"] == 0.0
        and result["na_reason"] is None
    ):
        result["na_reason"] = "no_dividend"

    # ── Step 6: Debug inputs (admin only) ───────────────────────────────
    if include_debug:
        result["debug_inputs"] = {
            "market_cap": market_cap,
            "shares_outstanding": shares_outstanding,
            "cashflow_dividends_paid_quarterly": cashflow_dividends_paid_quarterly,
            "cashflow_ttm": cashflow_ttm,
            "cashflow_yield_pct": cashflow_yield,
            "dividend_history_ttm_total_per_share": dividend_history_ttm_total,
            "dividend_history_ttm_total_dollars": hist_ttm_dollars,
            "dividend_history_yield_pct": hist_yield,
            "dividend_history_count": dividend_history_count,
            "pathological_ratio": PATHOLOGICAL_RATIO,
        }

    return result
