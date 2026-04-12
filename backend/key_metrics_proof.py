"""
Key Metrics Proof – read-only audit of what the ticker-detail endpoint computes.

This module mirrors the **exact** calculation logic used by
``get_ticker_detail_mobile()`` in ``server.py`` (lines 3946-4200) and returns
every raw input, intermediate value and final result so an admin can verify
each Key Metric end-to-end without changing any production numbers.

All formulas here MUST stay in lock-step with server.py.  If a formula changes
in server.py it must be updated here too (single source of truth for audit).
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


# ---------------------------------------------------------------------------
# Helpers (duplicated from server.py to keep this module self-contained)
# ---------------------------------------------------------------------------

def _sf(val: Any) -> Optional[float]:
    """Safely coerce *val* to float; return ``None`` on failure."""
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _format_large_num(val: Optional[float]) -> Optional[str]:
    if val is None:
        return None
    if val >= 1e12:
        return f"${val / 1e12:.2f}T"
    if val >= 1e9:
        return f"${val / 1e9:.2f}B"
    if val >= 1e6:
        return f"${val / 1e6:.2f}M"
    return f"${val:,.0f}"


def _format_shares(val: Optional[float]) -> Optional[str]:
    if val is None:
        return None
    if val >= 1e9:
        return f"{val / 1e9:.2f}B"
    if val >= 1e6:
        return f"{val / 1e6:.1f}M"
    return f"{val:,.0f}"


# ---------------------------------------------------------------------------
# Core proof function — pure computation, no DB access
# ---------------------------------------------------------------------------

def compute_proof(
    *,
    shares_outstanding: Optional[float],
    current_price: Optional[float],
    price_date: Optional[str],
    quarterly_rows: List[Dict[str, Any]],
    annual_rows: List[Dict[str, Any]],
    forward_dividend_yield: Any,
    valuation_cache: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Return a deterministic proof dict for every Key Metric.

    The caller is responsible for reading data from the DB; this function
    only performs arithmetic and bookkeeping.

    Parameters mirror what ``get_ticker_detail_mobile()`` has available at the
    point where the "Hybrid 7 Key Metrics" block begins.
    """
    proof: Dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }

    # ------------------------------------------------------------------
    # 1. Market Cap
    # ------------------------------------------------------------------
    market_cap = None
    if shares_outstanding and current_price and current_price > 0:
        market_cap = shares_outstanding * current_price

    proof["market_cap"] = {
        "value": market_cap,
        "formatted": _format_large_num(market_cap),
        "na_reason": None if market_cap else "missing_data",
        "inputs": {
            "shares_outstanding": shares_outstanding,
            "current_price": current_price,
            "price_date": price_date,
        },
        "formula": "shares_outstanding * current_price",
    }

    # ------------------------------------------------------------------
    # 2. Shares Outstanding
    # ------------------------------------------------------------------
    proof["shares_outstanding"] = {
        "value": shares_outstanding,
        "formatted": _format_shares(shares_outstanding),
        "na_reason": None if shares_outstanding else "missing_data",
        "source": "tracked_tickers.shares_outstanding",
    }

    # ------------------------------------------------------------------
    # 3-6. TTM metrics from last 4 quarterly rows
    # ------------------------------------------------------------------
    _top4 = quarterly_rows[:4] if len(quarterly_rows) >= 4 else []
    quarter_dates_used = [q.get("period_date") for q in _top4]

    # 3a. Revenue & Net Income TTM
    ttm_revenue: Optional[float] = None
    ttm_net_income: Optional[float] = None
    ttm_ebitda: Optional[float] = None
    revenue_per_q: List[Optional[float]] = []
    net_income_per_q: List[Optional[float]] = []
    ebitda_per_q: List[Optional[float]] = []

    if len(_top4) >= 4:
        revenues = [_sf(q.get("revenue")) for q in _top4 if _sf(q.get("revenue")) is not None]
        net_incomes = [_sf(q.get("net_income")) for q in _top4 if _sf(q.get("net_income")) is not None]
        ebitdas = [_sf(q.get("ebitda")) for q in _top4 if _sf(q.get("ebitda")) is not None]

        revenue_per_q = [_sf(q.get("revenue")) for q in _top4]
        net_income_per_q = [_sf(q.get("net_income")) for q in _top4]
        ebitda_per_q = [_sf(q.get("ebitda")) for q in _top4]

        if len(revenues) >= 4:
            ttm_revenue = sum(revenues[:4])
        if len(net_incomes) >= 4:
            ttm_net_income = sum(net_incomes[:4])
        if len(ebitdas) >= 4:
            ttm_ebitda = sum(ebitdas[:4])

    # 3b. Net Margin TTM
    net_margin_ttm: Optional[float] = None
    net_margin_na = "missing_data"
    if ttm_revenue and ttm_revenue > 0 and ttm_net_income is not None:
        net_margin_ttm = (ttm_net_income / ttm_revenue) * 100
        net_margin_na = None
    elif ttm_net_income is not None and ttm_net_income < 0:
        # server.py shows "unprofitable" when net_income < 0 and margin is None
        # but this branch only fires when revenue is missing/zero
        net_margin_na = "unprofitable" if (ttm_net_income < 0) else "missing_data"

    proof["net_margin_ttm"] = {
        "value": net_margin_ttm,
        "formatted": f"{net_margin_ttm:.1f}%" if net_margin_ttm is not None else None,
        "na_reason": net_margin_na,
        "quarter_dates_used": quarter_dates_used,
        "revenue_per_quarter": revenue_per_q,
        "net_income_per_quarter": net_income_per_q,
        "revenue_ttm": ttm_revenue,
        "net_income_ttm": ttm_net_income,
        "formula": "(net_income_ttm / revenue_ttm) * 100",
        "source": "company_financials (quarterly, last 4 by period_date desc)",
    }

    # ------------------------------------------------------------------
    # 4. FCF Yield
    # ------------------------------------------------------------------
    ttm_fcf: Optional[float] = None
    ocf_per_q: List[Optional[float]] = []
    capex_per_q: List[Optional[float]] = []
    fcf_per_q: List[Optional[float]] = []

    if len(_top4) >= 4:
        for q in _top4:
            raw_ocf = _sf(q.get("operating_cash_flow"))
            raw_capex = _sf(q.get("capital_expenditures"))
            ocf = raw_ocf or 0
            capex = abs(raw_capex or 0)
            ocf_per_q.append(raw_ocf)
            capex_per_q.append(raw_capex)
            fcf_per_q.append(ocf - capex)
        if any(_sf(q.get("operating_cash_flow")) is not None for q in _top4):
            ttm_fcf = sum(fcf_per_q)

    fcf_yield: Optional[float] = None
    fcf_na = "missing_data"
    if ttm_fcf is not None and market_cap and market_cap > 0:
        fcf_yield = (ttm_fcf / market_cap) * 100
        fcf_na = None
    elif ttm_fcf is not None and ttm_fcf < 0:
        fcf_na = "negative_fcf"

    proof["fcf_yield"] = {
        "value": fcf_yield,
        "formatted": f"{fcf_yield:.1f}%" if fcf_yield is not None else None,
        "na_reason": fcf_na,
        "quarter_dates_used": quarter_dates_used,
        "operating_cash_flow_per_quarter": ocf_per_q,
        "capital_expenditures_per_quarter": capex_per_q,
        "fcf_per_quarter": fcf_per_q,
        "ttm_fcf": ttm_fcf,
        "market_cap_used": market_cap,
        "formula": "(ttm_fcf / market_cap) * 100  where fcf_q = ocf - abs(capex)",
        "source": "company_financials (quarterly, last 4)",
    }

    # ------------------------------------------------------------------
    # 5. Net Debt / EBITDA
    # ------------------------------------------------------------------
    ttm_cash: Optional[float] = None
    ttm_debt: Optional[float] = None
    latest_q_date: Optional[str] = None

    if quarterly_rows:
        _latest_q = quarterly_rows[0]
        latest_q_date = _latest_q.get("period_date")
        _cash_val = _sf(_latest_q.get("cash_and_equivalents"))
        ttm_cash = _cash_val if _cash_val is not None else 0.0
        _total_debt_val = _sf(_latest_q.get("total_debt"))
        ttm_debt = _total_debt_val if _total_debt_val is not None else 0.0

    net_debt_ebitda: Optional[float] = None
    ebitda_ttm = ttm_ebitda
    ebitda_source = "company_financials (quarterly TTM)"
    if ebitda_ttm is None and valuation_cache:
        ebitda_ttm = valuation_cache.get("raw_inputs", {}).get("ebitda_ttm")
        ebitda_source = "ticker_valuations_cache.raw_inputs.ebitda_ttm (fallback)"

    nd_ebitda_na = "missing_data"
    if ttm_debt is not None and ttm_cash is not None and ebitda_ttm and ebitda_ttm > 0:
        net_debt = ttm_debt - ttm_cash
        net_debt_ebitda = net_debt / ebitda_ttm
        nd_ebitda_na = None
    else:
        if ebitda_ttm is not None and ebitda_ttm <= 0:
            nd_ebitda_na = "negative_ebitda"
        elif ebitda_ttm is None:
            nd_ebitda_na = "ebitda_missing"
        elif ttm_debt is None:
            nd_ebitda_na = "missing_debt_data"
        elif ttm_cash is None:
            nd_ebitda_na = "missing_cash_data"

    proof["net_debt_ebitda"] = {
        "value": net_debt_ebitda,
        "formatted": f"{net_debt_ebitda:.1f}x" if net_debt_ebitda is not None else None,
        "na_reason": nd_ebitda_na,
        "latest_quarter_date": latest_q_date,
        "total_debt": ttm_debt,
        "cash_and_equivalents": ttm_cash,
        "net_debt": (ttm_debt - ttm_cash) if ttm_debt is not None and ttm_cash is not None else None,
        "ebitda_per_quarter": ebitda_per_q,
        "ebitda_ttm": ebitda_ttm,
        "ebitda_source": ebitda_source,
        "formula": "(total_debt - cash_and_equivalents) / ebitda_ttm",
        "source": "company_financials (quarterly)",
    }

    # ------------------------------------------------------------------
    # 6. Revenue Growth (3Y CAGR) — from annual rows
    # ------------------------------------------------------------------
    revenue_growth_3y: Optional[float] = None
    rg_na = "insufficient_annual_history"
    annual_revenues_raw: List[Dict[str, Any]] = []
    annual_revenues: List[float] = []

    for row in annual_rows[:4]:
        rev = _sf(row.get("revenue"))
        annual_revenues_raw.append({
            "period_date": row.get("period_date"),
            "revenue": rev,
        })
        if rev is not None:
            annual_revenues.append(rev)

    if len(annual_revenues) >= 4:
        end_revenue = annual_revenues[0]
        start_revenue = annual_revenues[3]
        if start_revenue is None or start_revenue <= 0:
            rg_na = "negative_or_zero_base_revenue"
        elif end_revenue is None or end_revenue <= 0:
            rg_na = "negative_end_revenue"
        else:
            cagr = ((end_revenue / start_revenue) ** (1 / 3) - 1) * 100
            revenue_growth_3y = round(cagr, 1)
            rg_na = None

    proof["revenue_growth_3y"] = {
        "value": revenue_growth_3y,
        "formatted": f"{revenue_growth_3y:.1f}%" if revenue_growth_3y is not None else None,
        "na_reason": rg_na,
        "annual_rows_used": annual_revenues_raw,
        "end_revenue": annual_revenues[0] if len(annual_revenues) >= 4 else None,
        "start_revenue": annual_revenues[3] if len(annual_revenues) >= 4 else None,
        "formula": "((end_revenue / start_revenue) ** (1/3) - 1) * 100",
        "source": "company_financials (annual, last 4 by period_date desc)",
    }

    # ------------------------------------------------------------------
    # 7. Dividend Yield (TTM) — currently from forward_dividend_yield
    # ------------------------------------------------------------------
    dividend_yield_ttm: Optional[float] = None
    div_na = "no_dividend"
    if forward_dividend_yield is not None:
        try:
            dividend_yield_ttm = float(forward_dividend_yield) * 100
            div_na = None
        except (ValueError, TypeError):
            pass

    # Also gather the quarterly dividends_paid data for transparency
    dividends_paid_per_q: List[Optional[float]] = []
    for q in _top4:
        dividends_paid_per_q.append(_sf(q.get("dividends_paid")))

    proof["dividend_yield_ttm"] = {
        "value": dividend_yield_ttm,
        "formatted": f"{dividend_yield_ttm:.2f}%" if dividend_yield_ttm else "0.00%",
        "na_reason": div_na,
        "current_source": "company_fundamentals_cache.forward_dividend_yield",
        "forward_dividend_yield_raw": forward_dividend_yield,
        "note": (
            "Currently uses provider-supplied forward_dividend_yield (decimal) * 100. "
            "Quarterly dividends_paid values shown below for reference only."
        ),
        "quarterly_dividends_paid": dividends_paid_per_q,
        "quarter_dates_used": quarter_dates_used,
    }

    return proof


# ---------------------------------------------------------------------------
# Async wrapper that reads from DB (used by the admin endpoint)
# ---------------------------------------------------------------------------

async def generate_key_metrics_proof(db, ticker: str) -> Dict[str, Any]:
    """Read all source data from DB and generate a full proof payload.

    Parameters
    ----------
    db : motor AsyncIOMotorDatabase
    ticker : str  — bare or suffixed ticker (e.g. ``"AAPL"`` or ``"AAPL.US"``)
    """
    ticker_full = ticker.upper()
    if not ticker_full.endswith(".US"):
        ticker_full = f"{ticker_full}.US"

    import asyncio

    # --- Parallel reads (mirrors server.py Phase 1 + Key Metrics) ----------
    tracked_task = db.tracked_tickers.find_one(
        {"ticker": ticker_full},
        {"_id": 0, "shares_outstanding": 1, "is_visible": 1},
    )
    latest_price_task = db.stock_prices.find_one(
        {"ticker": ticker_full}, {"_id": 0}, sort=[("date", -1)]
    )
    cache_task = db.company_fundamentals_cache.find_one(
        {"ticker": ticker_full},
        {"_id": 0, "forward_dividend_yield": 1},
    )
    valuation_task = db.ticker_valuations_cache.find_one(
        {"ticker": ticker_full}, {"_id": 0}
    )

    tracked, latest_price, cache_doc, valuation_cache_raw = await asyncio.gather(
        tracked_task, latest_price_task, cache_task, valuation_task,
    )

    shares_outstanding = tracked.get("shares_outstanding") if tracked else None
    current_price = None
    price_date = None
    if latest_price:
        current_price = latest_price.get("close") or latest_price.get("adjusted_close")
        price_date = latest_price.get("date")

    forward_dividend_yield = cache_doc.get("forward_dividend_yield") if cache_doc else None

    # --- Quarterly rows (same query as server.py:3974-3977) ----------------
    quarterly_rows = await db.company_financials.find(
        {"ticker": ticker_full, "period_type": "quarterly"},
        {"_id": 0},
    ).sort("period_date", -1).limit(12).to_list(length=12)

    # --- Annual rows (same query as server.py:4161-4164) -------------------
    annual_rows = await db.company_financials.find(
        {"ticker": ticker_full, "period_type": "annual"},
        {"_id": 0},
    ).sort("period_date", -1).limit(6).to_list(length=6)

    # --- Compute proof -----------------------------------------------------
    proof = compute_proof(
        shares_outstanding=shares_outstanding,
        current_price=current_price,
        price_date=price_date,
        quarterly_rows=quarterly_rows,
        annual_rows=annual_rows,
        forward_dividend_yield=forward_dividend_yield,
        valuation_cache=valuation_cache_raw,
    )

    proof["ticker"] = ticker_full
    proof["data_availability"] = {
        "tracked_tickers_found": tracked is not None,
        "stock_prices_found": latest_price is not None,
        "company_fundamentals_cache_found": cache_doc is not None,
        "quarterly_rows_count": len(quarterly_rows),
        "annual_rows_count": len(annual_rows),
    }

    return proof
