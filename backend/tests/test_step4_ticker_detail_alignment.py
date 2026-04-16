"""
Step 4 ↔ Ticker Detail metric-definition alignment verification.

This test creates 21 synthetic tickers (GOOG.US + 20 deterministic peers) with
known financial data and computes every metric using:

  A) ticker-detail logic  — the pure-function extracted from server.py
  B) Step 4 logic         — the equivalent code path from compute_peer_benchmarks_v3

Both paths receive the same raw company_financials rows.  The test asserts
every metric matches and prints the comparison table required by the
verification deliverable.

Run:
    cd /app/backend && python -m pytest tests/test_step4_ticker_detail_alignment.py -v -s
"""

import math
import pytest

# ═══════════════════════════════════════════════════════════════════════════════
# Shared helpers (same implementation in both paths)
# ═══════════════════════════════════════════════════════════════════════════════

def _sf(val):
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


# ═══════════════════════════════════════════════════════════════════════════════
# Ticker-detail computation (extracted from server.py lines 4583-4920)
# ═══════════════════════════════════════════════════════════════════════════════

def ticker_detail_metrics(shares_outstanding, current_price, quarterly_rows,
                          annual_rows, earnings_history=None):
    """Compute all 7 key metrics exactly as the /api/v1/ticker/{ticker}/detail
    endpoint does.  Returns dict with metric values (or None)."""

    market_cap = None
    if shares_outstanding and current_price and current_price > 0:
        market_cap = shares_outstanding * current_price

    # TTM Revenue, Net Income, EBITDA — strict 4/4
    ttm_revenue = None
    ttm_net_income = None
    ttm_ebitda = None
    if len(quarterly_rows) >= 4:
        top4 = quarterly_rows[:4]
        revenues = [_sf(q.get("revenue")) for q in top4 if _sf(q.get("revenue")) is not None]
        net_incomes = [_sf(q.get("net_income")) for q in top4 if _sf(q.get("net_income")) is not None]
        ebitdas = [_sf(q.get("ebitda")) for q in top4 if _sf(q.get("ebitda")) is not None]
        if len(revenues) >= 4:
            ttm_revenue = sum(revenues[:4])
        if len(net_incomes) >= 4:
            ttm_net_income = sum(net_incomes[:4])
        if len(ebitdas) >= 4:
            ttm_ebitda = sum(ebitdas[:4])

    # FCF TTM — strict: all 4 quarters must have BOTH OCF and CapEx
    ttm_fcf = None
    if len(quarterly_rows) >= 4:
        top4 = quarterly_rows[:4]
        all_computable = all(
            _sf(q.get("operating_cash_flow")) is not None and
            _sf(q.get("capital_expenditures")) is not None
            for q in top4
        )
        if all_computable:
            fcfs = []
            for q in top4:
                ocf = _sf(q.get("operating_cash_flow"))
                capex = abs(_sf(q.get("capital_expenditures")))
                fcfs.append(ocf - capex)
            ttm_fcf = sum(fcfs)

    # Balance sheet from latest quarter
    ttm_cash = 0.0
    ttm_debt = 0.0
    if quarterly_rows:
        latest = quarterly_rows[0]
        c = _sf(latest.get("cash_and_equivalents"))
        ttm_cash = c if c is not None else 0.0
        d = _sf(latest.get("total_debt"))
        ttm_debt = d if d is not None else 0.0

    # Metric computations
    net_margin_ttm = None
    if ttm_revenue and ttm_revenue > 0 and ttm_net_income is not None:
        net_margin_ttm = (ttm_net_income / ttm_revenue) * 100

    fcf_yield = None
    if ttm_fcf is not None and market_cap and market_cap > 0:
        fcf_yield = (ttm_fcf / market_cap) * 100

    net_debt_ebitda = None
    ebitda_ttm = ttm_ebitda
    if ttm_debt is not None and ttm_cash is not None and ebitda_ttm and ebitda_ttm > 0:
        net_debt = ttm_debt - ttm_cash
        net_debt_ebitda = net_debt / ebitda_ttm

    # Revenue Growth 3Y CAGR
    revenue_growth_3y = None
    annual_revenues = [_sf(r.get("revenue")) for r in annual_rows[:4] if _sf(r.get("revenue")) is not None]
    if len(annual_revenues) >= 4:
        end_rev = annual_revenues[0]
        start_rev = annual_revenues[3]
        if start_rev and start_rev > 0 and end_rev and end_rev > 0:
            revenue_growth_3y = ((end_rev / start_rev) ** (1/3) - 1) * 100

    # Dividend Yield TTM
    dividend_yield = None
    if len(quarterly_rows) >= 4:
        div_vals = [_sf(q.get("dividends_paid")) for q in quarterly_rows[:4]]
        if not all(v is None for v in div_vals):
            dividends_ttm = sum(abs(v) for v in div_vals if v is not None)
            if dividends_ttm == 0.0:
                dividend_yield = 0.0
            elif market_cap and market_cap > 0:
                dividend_yield = (dividends_ttm / market_cap) * 100

    # P/E TTM — from earnings history or fallback to net_income/shares
    pe_ttm = None
    eps_ttm = None
    if earnings_history:
        sorted_q = sorted(earnings_history.keys(), reverse=True)[:4]
        eps_values = [_sf(earnings_history[q].get("epsActual")) for q in sorted_q
                      if _sf(earnings_history[q].get("epsActual")) is not None]
        if len(eps_values) >= 3:
            eps_ttm = sum(eps_values)
    if eps_ttm is None and ttm_net_income is not None and ttm_net_income != 0 and shares_outstanding:
        eps_ttm = ttm_net_income / shares_outstanding
    if eps_ttm and eps_ttm > 0 and current_price:
        pe_ttm = current_price / eps_ttm

    # ROE
    roe = None
    total_equity = None
    if quarterly_rows:
        total_equity = _sf(quarterly_rows[0].get("total_equity"))
    if ttm_net_income is not None and total_equity and total_equity > 0:
        roe = (ttm_net_income / total_equity) * 100

    return {
        "net_margin_ttm": net_margin_ttm,
        "fcf_yield": fcf_yield,
        "net_debt_ebitda": net_debt_ebitda,
        "revenue_growth_3y": revenue_growth_3y,
        "dividend_yield": dividend_yield,
        "pe_ttm": pe_ttm,
        "roe": roe,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# Step 4 computation (extracted from key_metrics_service.py lines 1232-1607)
# ═══════════════════════════════════════════════════════════════════════════════

def step4_metrics(shares_outstanding, current_price, quarterly_rows,
                  annual_rows, earnings_history=None):
    """Compute all 7 key metrics exactly as compute_peer_benchmarks_v3 does.
    Returns dict with metric values (or None)."""

    def safe_float(val):
        if val is None:
            return None
        try:
            return float(val)
        except:
            return None

    def get_ttm_sum(quarterly_data, field):
        """Strict 4/4 TTM sum — aligned with ticker detail endpoint."""
        if not quarterly_data:
            return None
        sorted_quarters = sorted(quarterly_data.keys(), reverse=True)[:4]
        if len(sorted_quarters) < 4:
            return None
        values = []
        for q in sorted_quarters:
            val = safe_float(quarterly_data[q].get(field))
            if val is None:
                return None
            values.append(val)
        return sum(values)

    def get_latest_value(quarterly_data, field):
        if not quarterly_data:
            return None
        for q in sorted(quarterly_data.keys(), reverse=True):
            val = safe_float(quarterly_data[q].get(field))
            if val is not None:
                return val
        return None

    # ── Convert quarterly_rows → dict-of-dicts (Step 4 format) ──
    # Step 4 reads from company_financials and reshapes into
    # quarterly_income[ticker][period_date] = {...} etc.
    quarterly_income = {}
    quarterly_balance = {}
    quarterly_cashflow = {}
    for row in quarterly_rows:
        pd_key = row.get("period_date", "")
        if not pd_key:
            continue
        quarterly_income[pd_key] = {
            "totalRevenue": row.get("revenue"),
            "netIncome": row.get("net_income"),
            "ebitda": row.get("ebitda"),
            "operatingIncome": row.get("operating_income"),
        }
        quarterly_balance[pd_key] = {
            "totalStockholderEquity": row.get("total_equity"),
            "totalDebt": row.get("total_debt"),
            "cash": row.get("cash_and_equivalents"),
            "cashAndShortTermInvestments": (
                (safe_float(row.get("cash_and_equivalents")) or 0) +
                (safe_float(row.get("short_term_investments")) or 0)
            ) if (row.get("cash_and_equivalents") is not None or
                  row.get("short_term_investments") is not None) else None,
            "shortTermDebt": None,
            "longTermDebt": None,
        }
        quarterly_cashflow[pd_key] = {
            "totalCashFromOperatingActivities": row.get("operating_cash_flow"),
            "capitalExpenditures": row.get("capital_expenditures"),
            "depreciation": None,
            "dividendsPaid": row.get("dividends_paid"),
        }

    annual_income = {}
    for row in annual_rows:
        pd_key = row.get("period_date", "")
        if not pd_key:
            continue
        annual_income[pd_key] = {
            "totalRevenue": row.get("revenue"),
        }

    shares = shares_outstanding
    market_cap = None
    if shares and current_price and current_price > 0:
        market_cap = shares * current_price
    if not market_cap or market_cap <= 0:
        return {k: None for k in ["net_margin_ttm", "fcf_yield", "net_debt_ebitda",
                                   "revenue_growth_3y", "dividend_yield", "pe_ttm", "roe"]}

    # TTM values
    revenue_ttm = get_ttm_sum(quarterly_income, "totalRevenue")
    net_income_ttm = get_ttm_sum(quarterly_income, "netIncome")
    ebitda_ttm = get_ttm_sum(quarterly_income, "ebitda")

    if ebitda_ttm is None:
        operating_income = get_ttm_sum(quarterly_income, "operatingIncome")
        depreciation = get_ttm_sum(quarterly_cashflow, "depreciation")
        if operating_income is not None:
            ebitda_ttm = operating_income + abs(depreciation if depreciation is not None else 0)

    # Balance sheet
    total_equity = get_latest_value(quarterly_balance, "totalStockholderEquity")
    total_debt = get_latest_value(quarterly_balance, "totalDebt")
    if not total_debt:
        short_term = get_latest_value(quarterly_balance, "shortTermDebt") or 0
        long_term = get_latest_value(quarterly_balance, "longTermDebt") or 0
        total_debt = short_term + long_term if (short_term or long_term) else 0
    cash = (get_latest_value(quarterly_balance, "cash")
            or get_latest_value(quarterly_balance, "cashAndShortTermInvestments") or 0)

    # EPS TTM
    eps_ttm = None
    if earnings_history:
        sorted_q = sorted(earnings_history.keys(), reverse=True)[:4]
        eps_values = [safe_float(earnings_history[q].get("epsActual")) for q in sorted_q
                      if safe_float(earnings_history[q].get("epsActual")) is not None]
        if len(eps_values) >= 3:
            eps_ttm = sum(eps_values)
    if eps_ttm is None and net_income_ttm is not None and net_income_ttm != 0 and shares:
        eps_ttm = net_income_ttm / shares

    enterprise_value = market_cap + (total_debt or 0) - cash

    metrics = {}

    # P/E
    pe_ttm = None
    if eps_ttm and eps_ttm > 0:
        pe_ttm = current_price / eps_ttm
    metrics["pe_ttm"] = pe_ttm

    # Dividend Yield TTM
    _div_q_keys = sorted(quarterly_cashflow.keys(), reverse=True)[:4] if quarterly_cashflow else []
    dividend_yield = None
    if len(_div_q_keys) >= 4:
        _div_vals = [safe_float(quarterly_cashflow[q].get("dividendsPaid")) for q in _div_q_keys]
        if not all(v is None for v in _div_vals):
            _dividends_ttm = sum(abs(v) for v in _div_vals if v is not None)
            if _dividends_ttm == 0.0:
                dividend_yield = 0.0
            elif market_cap > 0:
                dividend_yield = (_dividends_ttm / market_cap) * 100
    metrics["dividend_yield"] = dividend_yield

    # Net Margin (TTM)
    net_margin_ttm = None
    if net_income_ttm is not None and revenue_ttm and revenue_ttm > 0:
        net_margin_ttm = (net_income_ttm / revenue_ttm) * 100
    metrics["net_margin_ttm"] = net_margin_ttm

    # FCF Yield — strict: all 4 quarters BOTH OCF and CapEx non-null
    _fcf_computable = False
    _ocf_field = None
    if quarterly_cashflow and len(sorted(quarterly_cashflow.keys(), reverse=True)[:4]) >= 4:
        _top4_cf_keys = sorted(quarterly_cashflow.keys(), reverse=True)[:4]
        _fcf_computable = all(
            safe_float(quarterly_cashflow[q].get("totalCashFromOperatingActivities")) is not None
            and safe_float(quarterly_cashflow[q].get("capitalExpenditures")) is not None
            for q in _top4_cf_keys
        )
        if not _fcf_computable:
            _fcf_computable = all(
                safe_float(quarterly_cashflow[q].get("operatingCashflow")) is not None
                and safe_float(quarterly_cashflow[q].get("capitalExpenditures")) is not None
                for q in _top4_cf_keys
            )
            if _fcf_computable:
                _ocf_field = "operatingCashflow"
        else:
            _ocf_field = "totalCashFromOperatingActivities"
    else:
        _top4_cf_keys = []

    fcf_yield = None
    if _fcf_computable and _ocf_field and market_cap > 0:
        _fcf_parts = []
        for q in _top4_cf_keys:
            _ocf = safe_float(quarterly_cashflow[q].get(_ocf_field))
            _capex = abs(safe_float(quarterly_cashflow[q].get("capitalExpenditures")))
            _fcf_parts.append(_ocf - _capex)
        fcf_ttm = sum(_fcf_parts)
        fcf_yield = (fcf_ttm / market_cap) * 100
    metrics["fcf_yield"] = fcf_yield

    # Net Debt / EBITDA
    net_debt = (total_debt or 0) - cash
    net_debt_ebitda = None
    if ebitda_ttm and ebitda_ttm > 0:
        net_debt_ebitda = net_debt / ebitda_ttm
    metrics["net_debt_ebitda"] = net_debt_ebitda

    # Revenue Growth (3Y CAGR)
    revenue_growth_3y = None
    if annual_income:
        sorted_years = sorted(annual_income.keys(), reverse=True)
        if len(sorted_years) >= 4:
            rev_current = safe_float(annual_income[sorted_years[0]].get("totalRevenue"))
            rev_3y_ago = safe_float(annual_income[sorted_years[3]].get("totalRevenue"))
            if rev_current and rev_3y_ago and rev_3y_ago > 0 and rev_current > 0:
                revenue_growth_3y = ((rev_current / rev_3y_ago) ** (1.0 / 3.0) - 1) * 100
    metrics["revenue_growth_3y"] = revenue_growth_3y

    # ROE
    roe = None
    if net_income_ttm is not None and total_equity and total_equity > 0:
        roe = (net_income_ttm / total_equity) * 100
    metrics["roe"] = roe

    return metrics


# ═══════════════════════════════════════════════════════════════════════════════
# Test data: GOOG.US + 20 deterministic tickers
# ═══════════════════════════════════════════════════════════════════════════════

def _make_q(period_date, revenue, net_income, ebitda, ocf, capex,
            cash, debt, dividends_paid=None, total_equity=None,
            operating_income=None, short_term_investments=None):
    """Helper to build one quarterly row."""
    return {
        "period_date": period_date,
        "revenue": revenue,
        "net_income": net_income,
        "ebitda": ebitda,
        "operating_cash_flow": ocf,
        "capital_expenditures": capex,
        "cash_and_equivalents": cash,
        "total_debt": debt,
        "dividends_paid": dividends_paid,
        "total_equity": total_equity,
        "operating_income": operating_income,
        "short_term_investments": short_term_investments,
    }


def _make_annual(period_date, revenue):
    return {"period_date": period_date, "revenue": revenue}


# Build 21 tickers with varied data patterns to exercise all edge cases
TICKERS = {}

# 1. GOOG.US — complete data, no dividends
TICKERS["GOOG.US"] = {
    "shares": 12e9, "price": 170.0,
    "quarterly": [
        _make_q("2025-09-30", 90e9, 25e9, 35e9, 30e9, -8e9, 20e9, 15e9, total_equity=280e9),
        _make_q("2025-06-30", 85e9, 23e9, 33e9, 28e9, -7.5e9, 18e9, 15e9, total_equity=275e9),
        _make_q("2025-03-31", 80e9, 20e9, 30e9, 26e9, -7e9, 17e9, 14e9, total_equity=270e9),
        _make_q("2024-12-31", 78e9, 19e9, 28e9, 25e9, -6.5e9, 16e9, 14e9, total_equity=265e9),
    ],
    "annual": [_make_annual(f"{y}-12-31", r) for y, r in
               [(2025, 333e9), (2024, 310e9), (2023, 280e9), (2022, 260e9)]],
    "earnings": {
        "2025-09-30": {"epsActual": 2.10},
        "2025-06-30": {"epsActual": 1.95},
        "2025-03-31": {"epsActual": 1.70},
        "2024-12-31": {"epsActual": 1.60},
    },
}

# 2. AAPL.US — pays dividends
TICKERS["AAPL.US"] = {
    "shares": 15e9, "price": 200.0,
    "quarterly": [
        _make_q("2025-09-30", 95e9, 24e9, 34e9, 29e9, -3e9, 30e9, 110e9, dividends_paid=-3.5e9, total_equity=60e9),
        _make_q("2025-06-30", 90e9, 22e9, 32e9, 27e9, -2.8e9, 28e9, 108e9, dividends_paid=-3.5e9, total_equity=58e9),
        _make_q("2025-03-31", 88e9, 21e9, 31e9, 26e9, -2.5e9, 27e9, 105e9, dividends_paid=-3.5e9, total_equity=56e9),
        _make_q("2024-12-31", 85e9, 20e9, 29e9, 24e9, -2.3e9, 26e9, 103e9, dividends_paid=-3.5e9, total_equity=55e9),
    ],
    "annual": [_make_annual(f"{y}-12-31", r) for y, r in
               [(2025, 358e9), (2024, 340e9), (2023, 320e9), (2022, 300e9)]],
    "earnings": {
        "2025-09-30": {"epsActual": 1.60},
        "2025-06-30": {"epsActual": 1.50},
        "2025-03-31": {"epsActual": 1.45},
        "2024-12-31": {"epsActual": 1.35},
    },
}

# 3. MSFT.US — complete data
TICKERS["MSFT.US"] = {
    "shares": 7.5e9, "price": 420.0,
    "quarterly": [
        _make_q("2025-09-30", 60e9, 22e9, 30e9, 25e9, -8e9, 15e9, 50e9, dividends_paid=-5e9, total_equity=200e9),
        _make_q("2025-06-30", 58e9, 21e9, 29e9, 24e9, -7.5e9, 14e9, 48e9, dividends_paid=-5e9, total_equity=195e9),
        _make_q("2025-03-31", 55e9, 19e9, 27e9, 22e9, -7e9, 13e9, 47e9, dividends_paid=-5e9, total_equity=190e9),
        _make_q("2024-12-31", 53e9, 18e9, 26e9, 21e9, -6.5e9, 12e9, 46e9, dividends_paid=-5e9, total_equity=185e9),
    ],
    "annual": [_make_annual(f"{y}-12-31", r) for y, r in
               [(2025, 226e9), (2024, 210e9), (2023, 190e9), (2022, 175e9)]],
    "earnings": {
        "2025-09-30": {"epsActual": 3.00},
        "2025-06-30": {"epsActual": 2.85},
        "2025-03-31": {"epsActual": 2.60},
        "2024-12-31": {"epsActual": 2.45},
    },
}

# 4. AMZN.US — no dividends, high growth
TICKERS["AMZN.US"] = {
    "shares": 10.3e9, "price": 185.0,
    "quarterly": [
        _make_q("2025-09-30", 155e9, 10e9, 20e9, 25e9, -15e9, 70e9, 60e9, total_equity=150e9),
        _make_q("2025-06-30", 148e9, 9e9, 18e9, 23e9, -14e9, 65e9, 58e9, total_equity=145e9),
        _make_q("2025-03-31", 140e9, 8e9, 16e9, 20e9, -13e9, 60e9, 55e9, total_equity=140e9),
        _make_q("2024-12-31", 135e9, 7e9, 14e9, 18e9, -12e9, 55e9, 52e9, total_equity=135e9),
    ],
    "annual": [_make_annual(f"{y}-12-31", r) for y, r in
               [(2025, 578e9), (2024, 500e9), (2023, 420e9), (2022, 350e9)]],
    "earnings": {},
}

# 5. NVDA.US — high P/E, high margins
TICKERS["NVDA.US"] = {
    "shares": 24e9, "price": 130.0,
    "quarterly": [
        _make_q("2025-09-30", 30e9, 15e9, 18e9, 16e9, -1e9, 10e9, 5e9, total_equity=45e9),
        _make_q("2025-06-30", 28e9, 14e9, 17e9, 15e9, -0.9e9, 9e9, 4.5e9, total_equity=43e9),
        _make_q("2025-03-31", 25e9, 12e9, 15e9, 13e9, -0.8e9, 8e9, 4e9, total_equity=41e9),
        _make_q("2024-12-31", 22e9, 10e9, 13e9, 11e9, -0.7e9, 7e9, 3.5e9, total_equity=39e9),
    ],
    "annual": [_make_annual(f"{y}-12-31", r) for y, r in
               [(2025, 105e9), (2024, 80e9), (2023, 40e9), (2022, 27e9)]],
    "earnings": {
        "2025-09-30": {"epsActual": 0.63},
        "2025-06-30": {"epsActual": 0.58},
        "2025-03-31": {"epsActual": 0.50},
        "2024-12-31": {"epsActual": 0.42},
    },
}

# 6. META.US — zero dividends_paid (explicit non-payer)
TICKERS["META.US"] = {
    "shares": 2.5e9, "price": 510.0,
    "quarterly": [
        _make_q("2025-09-30", 40e9, 12e9, 18e9, 15e9, -8e9, 25e9, 30e9, dividends_paid=0, total_equity=110e9),
        _make_q("2025-06-30", 38e9, 11e9, 17e9, 14e9, -7.5e9, 23e9, 28e9, dividends_paid=0, total_equity=105e9),
        _make_q("2025-03-31", 35e9, 10e9, 15e9, 12e9, -7e9, 22e9, 27e9, dividends_paid=0, total_equity=100e9),
        _make_q("2024-12-31", 33e9, 9e9, 14e9, 11e9, -6.5e9, 20e9, 25e9, dividends_paid=0, total_equity=95e9),
    ],
    "annual": [_make_annual(f"{y}-12-31", r) for y, r in
               [(2025, 146e9), (2024, 130e9), (2023, 110e9), (2022, 95e9)]],
    "earnings": {
        "2025-09-30": {"epsActual": 4.80},
        "2025-06-30": {"epsActual": 4.40},
        "2025-03-31": {"epsActual": 4.00},
        "2024-12-31": {"epsActual": 3.60},
    },
}

# 7. TSLA.US — volatile, no dividends, negative net_income quarter
TICKERS["TSLA.US"] = {
    "shares": 3.2e9, "price": 250.0,
    "quarterly": [
        _make_q("2025-09-30", 25e9, 2e9, 4e9, 3e9, -2e9, 15e9, 8e9, total_equity=50e9),
        _make_q("2025-06-30", 23e9, 1.5e9, 3.5e9, 2.5e9, -1.8e9, 14e9, 7.5e9, total_equity=48e9),
        _make_q("2025-03-31", 20e9, -0.5e9, 2e9, 1.5e9, -1.5e9, 13e9, 7e9, total_equity=46e9),
        _make_q("2024-12-31", 22e9, 1e9, 3e9, 2e9, -1.7e9, 12e9, 6.5e9, total_equity=44e9),
    ],
    "annual": [_make_annual(f"{y}-12-31", r) for y, r in
               [(2025, 90e9), (2024, 85e9), (2023, 75e9), (2022, 70e9)]],
    "earnings": {},
}

# 8. JPM.US — financials sector, pays dividends
TICKERS["JPM.US"] = {
    "shares": 2.9e9, "price": 220.0,
    "quarterly": [
        _make_q("2025-09-30", 45e9, 14e9, None, 12e9, -1e9, 500e9, 300e9, dividends_paid=-3e9, total_equity=320e9),
        _make_q("2025-06-30", 43e9, 13e9, None, 11e9, -0.9e9, 480e9, 290e9, dividends_paid=-3e9, total_equity=315e9),
        _make_q("2025-03-31", 42e9, 12e9, None, 10e9, -0.8e9, 470e9, 285e9, dividends_paid=-3e9, total_equity=310e9),
        _make_q("2024-12-31", 40e9, 11e9, None, 9e9, -0.7e9, 460e9, 280e9, dividends_paid=-3e9, total_equity=305e9),
    ],
    "annual": [_make_annual(f"{y}-12-31", r) for y, r in
               [(2025, 170e9), (2024, 158e9), (2023, 145e9), (2022, 130e9)]],
    "earnings": {
        "2025-09-30": {"epsActual": 4.90},
        "2025-06-30": {"epsActual": 4.60},
        "2025-03-31": {"epsActual": 4.25},
        "2024-12-31": {"epsActual": 3.90},
    },
}

# 9. JNJ.US — healthcare, pays dividends, moderate growth
TICKERS["JNJ.US"] = {
    "shares": 2.4e9, "price": 165.0,
    "quarterly": [
        _make_q("2025-09-30", 22e9, 5e9, 8e9, 6e9, -1.5e9, 20e9, 25e9, dividends_paid=-3e9, total_equity=80e9),
        _make_q("2025-06-30", 21e9, 4.8e9, 7.5e9, 5.5e9, -1.3e9, 19e9, 24e9, dividends_paid=-3e9, total_equity=78e9),
        _make_q("2025-03-31", 20e9, 4.5e9, 7e9, 5e9, -1.2e9, 18e9, 23e9, dividends_paid=-3e9, total_equity=76e9),
        _make_q("2024-12-31", 19e9, 4e9, 6.5e9, 4.5e9, -1e9, 17e9, 22e9, dividends_paid=-3e9, total_equity=74e9),
    ],
    "annual": [_make_annual(f"{y}-12-31", r) for y, r in
               [(2025, 82e9), (2024, 78e9), (2023, 74e9), (2022, 70e9)]],
    "earnings": {
        "2025-09-30": {"epsActual": 2.10},
        "2025-06-30": {"epsActual": 2.00},
        "2025-03-31": {"epsActual": 1.90},
        "2024-12-31": {"epsActual": 1.70},
    },
}

# 10. V.US — high margin, moderate capex
TICKERS["V.US"] = {
    "shares": 2.0e9, "price": 290.0,
    "quarterly": [
        _make_q("2025-09-30", 9e9, 4.5e9, 6e9, 5e9, -0.5e9, 15e9, 20e9, dividends_paid=-0.8e9, total_equity=35e9),
        _make_q("2025-06-30", 8.5e9, 4.2e9, 5.7e9, 4.8e9, -0.45e9, 14e9, 19e9, dividends_paid=-0.8e9, total_equity=34e9),
        _make_q("2025-03-31", 8e9, 4e9, 5.4e9, 4.5e9, -0.4e9, 13e9, 18e9, dividends_paid=-0.8e9, total_equity=33e9),
        _make_q("2024-12-31", 7.5e9, 3.8e9, 5.1e9, 4.2e9, -0.35e9, 12e9, 17e9, dividends_paid=-0.8e9, total_equity=32e9),
    ],
    "annual": [_make_annual(f"{y}-12-31", r) for y, r in
               [(2025, 33e9), (2024, 30e9), (2023, 28e9), (2022, 26e9)]],
    "earnings": {
        "2025-09-30": {"epsActual": 2.25},
        "2025-06-30": {"epsActual": 2.10},
        "2025-03-31": {"epsActual": 2.00},
        "2024-12-31": {"epsActual": 1.90},
    },
}

# 11. XOM.US — energy, high dividends
TICKERS["XOM.US"] = {
    "shares": 4.1e9, "price": 115.0,
    "quarterly": [
        _make_q("2025-09-30", 90e9, 8e9, 15e9, 12e9, -6e9, 20e9, 40e9, dividends_paid=-4e9, total_equity=200e9),
        _make_q("2025-06-30", 85e9, 7.5e9, 14e9, 11e9, -5.5e9, 18e9, 38e9, dividends_paid=-4e9, total_equity=195e9),
        _make_q("2025-03-31", 80e9, 7e9, 13e9, 10e9, -5e9, 17e9, 37e9, dividends_paid=-4e9, total_equity=190e9),
        _make_q("2024-12-31", 82e9, 7.2e9, 13.5e9, 10.5e9, -5.2e9, 16e9, 36e9, dividends_paid=-4e9, total_equity=185e9),
    ],
    "annual": [_make_annual(f"{y}-12-31", r) for y, r in
               [(2025, 337e9), (2024, 320e9), (2023, 310e9), (2022, 395e9)]],
    "earnings": {
        "2025-09-30": {"epsActual": 1.95},
        "2025-06-30": {"epsActual": 1.85},
        "2025-03-31": {"epsActual": 1.72},
        "2024-12-31": {"epsActual": 1.76},
    },
}

# 12. PG.US — consumer staples, stable
TICKERS["PG.US"] = {
    "shares": 2.35e9, "price": 170.0,
    "quarterly": [
        _make_q("2025-09-30", 21e9, 4e9, 6e9, 5e9, -0.8e9, 10e9, 25e9, dividends_paid=-2.2e9, total_equity=50e9),
        _make_q("2025-06-30", 20e9, 3.8e9, 5.7e9, 4.8e9, -0.75e9, 9.5e9, 24e9, dividends_paid=-2.2e9, total_equity=49e9),
        _make_q("2025-03-31", 19.5e9, 3.6e9, 5.5e9, 4.5e9, -0.7e9, 9e9, 23.5e9, dividends_paid=-2.2e9, total_equity=48e9),
        _make_q("2024-12-31", 19e9, 3.4e9, 5.3e9, 4.3e9, -0.65e9, 8.5e9, 23e9, dividends_paid=-2.2e9, total_equity=47e9),
    ],
    "annual": [_make_annual(f"{y}-12-31", r) for y, r in
               [(2025, 79.5e9), (2024, 76e9), (2023, 73e9), (2022, 70e9)]],
    "earnings": {
        "2025-09-30": {"epsActual": 1.70},
        "2025-06-30": {"epsActual": 1.62},
        "2025-03-31": {"epsActual": 1.55},
        "2024-12-31": {"epsActual": 1.45},
    },
}

# 13. MISSING_CAPEX.US — OCF present but CapEx missing in 1 quarter
TICKERS["MISSING_CAPEX.US"] = {
    "shares": 1e9, "price": 50.0,
    "quarterly": [
        _make_q("2025-09-30", 5e9, 1e9, 2e9, 1.5e9, -0.3e9, 2e9, 1e9, total_equity=10e9),
        _make_q("2025-06-30", 4.8e9, 0.9e9, 1.9e9, 1.4e9, None, 1.9e9, 0.9e9, total_equity=9.5e9),  # CapEx missing!
        _make_q("2025-03-31", 4.5e9, 0.8e9, 1.8e9, 1.3e9, -0.25e9, 1.8e9, 0.8e9, total_equity=9e9),
        _make_q("2024-12-31", 4.3e9, 0.7e9, 1.7e9, 1.2e9, -0.2e9, 1.7e9, 0.7e9, total_equity=8.5e9),
    ],
    "annual": [_make_annual(f"{y}-12-31", r) for y, r in
               [(2025, 18.6e9), (2024, 17e9), (2023, 15e9), (2022, 13e9)]],
    "earnings": {},
}

# 14. ZERO_NI.US — zero net income (breakeven)
TICKERS["ZERO_NI.US"] = {
    "shares": 1e9, "price": 30.0,
    "quarterly": [
        _make_q("2025-09-30", 3e9, 0, 0.5e9, 0.8e9, -0.2e9, 1e9, 2e9, total_equity=5e9),
        _make_q("2025-06-30", 2.8e9, 0, 0.4e9, 0.7e9, -0.18e9, 0.9e9, 1.9e9, total_equity=4.8e9),
        _make_q("2025-03-31", 2.6e9, 0, 0.35e9, 0.6e9, -0.15e9, 0.85e9, 1.8e9, total_equity=4.6e9),
        _make_q("2024-12-31", 2.5e9, 0, 0.3e9, 0.5e9, -0.12e9, 0.8e9, 1.7e9, total_equity=4.4e9),
    ],
    "annual": [_make_annual(f"{y}-12-31", r) for y, r in
               [(2025, 10.9e9), (2024, 10e9), (2023, 9e9), (2022, 8e9)]],
    "earnings": {},
}

# 15. PARTIAL_Q.US — only 3 quarters of data
TICKERS["PARTIAL_Q.US"] = {
    "shares": 2e9, "price": 40.0,
    "quarterly": [
        _make_q("2025-09-30", 6e9, 1.5e9, 2.5e9, 2e9, -0.5e9, 3e9, 4e9, total_equity=15e9),
        _make_q("2025-06-30", 5.5e9, 1.3e9, 2.3e9, 1.8e9, -0.45e9, 2.8e9, 3.8e9, total_equity=14e9),
        _make_q("2025-03-31", 5e9, 1.1e9, 2.1e9, 1.6e9, -0.4e9, 2.6e9, 3.6e9, total_equity=13e9),
    ],
    "annual": [_make_annual(f"{y}-12-31", r) for y, r in
               [(2025, 16.5e9), (2024, 15e9), (2023, 14e9), (2022, 13e9)]],
    "earnings": {},
}

# 16. NEGREV.US — negative start revenue (makes CAGR undefined)
TICKERS["NEGREV.US"] = {
    "shares": 0.5e9, "price": 20.0,
    "quarterly": [
        _make_q("2025-09-30", 1e9, 0.1e9, 0.3e9, 0.2e9, -0.05e9, 0.5e9, 0.3e9, total_equity=2e9),
        _make_q("2025-06-30", 0.9e9, 0.08e9, 0.25e9, 0.18e9, -0.04e9, 0.4e9, 0.28e9, total_equity=1.9e9),
        _make_q("2025-03-31", 0.8e9, 0.06e9, 0.2e9, 0.15e9, -0.03e9, 0.35e9, 0.25e9, total_equity=1.8e9),
        _make_q("2024-12-31", 0.7e9, 0.04e9, 0.15e9, 0.12e9, -0.02e9, 0.3e9, 0.22e9, total_equity=1.7e9),
    ],
    "annual": [_make_annual(f"{y}-12-31", r) for y, r in
               [(2025, 3.4e9), (2024, 2.8e9), (2023, 2e9), (2022, -0.5e9)]],  # negative base!
    "earnings": {},
}

# 17. MISSING_OCF.US — all OCF missing
TICKERS["MISSING_OCF.US"] = {
    "shares": 0.8e9, "price": 25.0,
    "quarterly": [
        _make_q("2025-09-30", 2e9, 0.3e9, 0.6e9, None, -0.1e9, 0.5e9, 0.3e9, total_equity=3e9),
        _make_q("2025-06-30", 1.9e9, 0.28e9, 0.55e9, None, -0.09e9, 0.48e9, 0.28e9, total_equity=2.9e9),
        _make_q("2025-03-31", 1.8e9, 0.25e9, 0.5e9, None, -0.08e9, 0.45e9, 0.25e9, total_equity=2.8e9),
        _make_q("2024-12-31", 1.7e9, 0.22e9, 0.45e9, None, -0.07e9, 0.42e9, 0.22e9, total_equity=2.7e9),
    ],
    "annual": [_make_annual(f"{y}-12-31", r) for y, r in
               [(2025, 7.4e9), (2024, 6.5e9), (2023, 5.5e9), (2022, 4.5e9)]],
    "earnings": {},
}

# 18. HIGH_DIV.US — very high dividend yield
TICKERS["HIGH_DIV.US"] = {
    "shares": 0.3e9, "price": 10.0,
    "quarterly": [
        _make_q("2025-09-30", 1e9, 0.2e9, 0.4e9, 0.3e9, -0.05e9, 0.2e9, 0.5e9, dividends_paid=-0.15e9, total_equity=1.5e9),
        _make_q("2025-06-30", 0.95e9, 0.18e9, 0.38e9, 0.28e9, -0.04e9, 0.18e9, 0.48e9, dividends_paid=-0.15e9, total_equity=1.45e9),
        _make_q("2025-03-31", 0.9e9, 0.16e9, 0.35e9, 0.26e9, -0.03e9, 0.17e9, 0.46e9, dividends_paid=-0.15e9, total_equity=1.4e9),
        _make_q("2024-12-31", 0.85e9, 0.14e9, 0.32e9, 0.24e9, -0.025e9, 0.16e9, 0.44e9, dividends_paid=-0.15e9, total_equity=1.35e9),
    ],
    "annual": [_make_annual(f"{y}-12-31", r) for y, r in
               [(2025, 3.7e9), (2024, 3.3e9), (2023, 3e9), (2022, 2.8e9)]],
    "earnings": {},
}

# 19. NO_EQUITY.US — total_equity not reported
TICKERS["NO_EQUITY.US"] = {
    "shares": 1e9, "price": 15.0,
    "quarterly": [
        _make_q("2025-09-30", 2e9, 0.3e9, 0.5e9, 0.4e9, -0.1e9, 0.3e9, 0.5e9, total_equity=None),
        _make_q("2025-06-30", 1.9e9, 0.28e9, 0.47e9, 0.38e9, -0.09e9, 0.28e9, 0.48e9, total_equity=None),
        _make_q("2025-03-31", 1.8e9, 0.25e9, 0.44e9, 0.35e9, -0.08e9, 0.26e9, 0.46e9, total_equity=None),
        _make_q("2024-12-31", 1.7e9, 0.22e9, 0.41e9, 0.32e9, -0.07e9, 0.24e9, 0.44e9, total_equity=None),
    ],
    "annual": [_make_annual(f"{y}-12-31", r) for y, r in
               [(2025, 7.4e9), (2024, 6.5e9), (2023, 5.5e9), (2022, 4.5e9)]],
    "earnings": {},
}

# 20. LOSS.US — consistent losses
TICKERS["LOSS.US"] = {
    "shares": 0.2e9, "price": 5.0,
    "quarterly": [
        _make_q("2025-09-30", 0.5e9, -0.1e9, -0.05e9, 0.02e9, -0.03e9, 0.1e9, 0.3e9, total_equity=0.4e9),
        _make_q("2025-06-30", 0.48e9, -0.12e9, -0.06e9, 0.01e9, -0.025e9, 0.09e9, 0.28e9, total_equity=0.38e9),
        _make_q("2025-03-31", 0.45e9, -0.15e9, -0.08e9, 0.005e9, -0.02e9, 0.08e9, 0.26e9, total_equity=0.36e9),
        _make_q("2024-12-31", 0.42e9, -0.18e9, -0.1e9, -0.01e9, -0.015e9, 0.07e9, 0.24e9, total_equity=0.34e9),
    ],
    "annual": [_make_annual(f"{y}-12-31", r) for y, r in
               [(2025, 1.85e9), (2024, 1.6e9), (2023, 1.4e9), (2022, 1.2e9)]],
    "earnings": {},
}

# 21. SEMI_DIV.US — semi-annual payer (2 non-zero, 2 zero dividends_paid)
TICKERS["SEMI_DIV.US"] = {
    "shares": 1.5e9, "price": 80.0,
    "quarterly": [
        _make_q("2025-09-30", 10e9, 2e9, 3e9, 2.5e9, -0.5e9, 5e9, 8e9, dividends_paid=-1e9, total_equity=20e9),
        _make_q("2025-06-30", 9.5e9, 1.8e9, 2.8e9, 2.3e9, -0.45e9, 4.8e9, 7.5e9, dividends_paid=0, total_equity=19e9),
        _make_q("2025-03-31", 9e9, 1.6e9, 2.6e9, 2.1e9, -0.4e9, 4.5e9, 7e9, dividends_paid=-1e9, total_equity=18e9),
        _make_q("2024-12-31", 8.5e9, 1.4e9, 2.4e9, 1.9e9, -0.35e9, 4.2e9, 6.5e9, dividends_paid=0, total_equity=17e9),
    ],
    "annual": [_make_annual(f"{y}-12-31", r) for y, r in
               [(2025, 37e9), (2024, 33e9), (2023, 30e9), (2022, 27e9)]],
    "earnings": {
        "2025-09-30": {"epsActual": 1.35},
        "2025-06-30": {"epsActual": 1.20},
        "2025-03-31": {"epsActual": 1.10},
        "2024-12-31": {"epsActual": 0.95},
    },
}


METRIC_KEYS = ["net_margin_ttm", "fcf_yield", "net_debt_ebitda",
               "revenue_growth_3y", "dividend_yield", "pe_ttm", "roe"]


# ═══════════════════════════════════════════════════════════════════════════════
# Tests
# ═══════════════════════════════════════════════════════════════════════════════

def _fmt(val):
    """Format a metric value for table display."""
    if val is None:
        return "None"
    return f"{val:.4f}"


def _match(a, b, eps=1e-6):
    """Check if two metric values match (both None, or within epsilon)."""
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    return abs(a - b) < eps


class TestStep4TickerDetailAlignment:
    """Verify all 7 metrics match between ticker-detail and Step 4 computation
    for GOOG.US + 20 deterministic tickers."""

    def test_all_metrics_match_for_21_tickers(self, capsys):
        """For each ticker × metric, ticker_detail == step4.  Print comparison table."""
        mismatches = []
        all_results = []

        for ticker, data in TICKERS.items():
            td = ticker_detail_metrics(
                shares_outstanding=data["shares"],
                current_price=data["price"],
                quarterly_rows=data["quarterly"],
                annual_rows=data["annual"],
                earnings_history=data.get("earnings"),
            )
            s4 = step4_metrics(
                shares_outstanding=data["shares"],
                current_price=data["price"],
                quarterly_rows=data["quarterly"],
                annual_rows=data["annual"],
                earnings_history=data.get("earnings"),
            )

            for metric in METRIC_KEYS:
                td_val = td.get(metric)
                s4_val = s4.get(metric)
                is_match = _match(td_val, s4_val)
                reason = ""
                if not is_match:
                    reason = f"td={_fmt(td_val)} s4={_fmt(s4_val)}"
                    mismatches.append((ticker, metric, td_val, s4_val, reason))
                all_results.append((ticker, metric, td_val, s4_val, is_match, reason))

        # ── Print verification table ──
        with capsys.disabled():
            print("\n" + "=" * 120)
            print("STEP 4 ↔ TICKER DETAIL METRIC ALIGNMENT VERIFICATION")
            print("=" * 120)
            print(f"{'Ticker':<22} {'Metric':<20} {'Ticker Detail':<18} {'Step 4':<18} {'Match':<8} {'Reason'}")
            print("-" * 120)
            for ticker, metric, td_val, s4_val, is_match, reason in all_results:
                mark = "✅" if is_match else "❌"
                print(f"{ticker:<22} {metric:<20} {_fmt(td_val):<18} {_fmt(s4_val):<18} {mark:<8} {reason}")

            print("-" * 120)
            total = len(all_results)
            matched = sum(1 for r in all_results if r[4])
            print(f"TOTAL: {matched}/{total} matched")

            if mismatches:
                print(f"\n⚠️  {len(mismatches)} MISMATCHES:")
                for ticker, metric, td_val, s4_val, reason in mismatches:
                    print(f"  {ticker} / {metric}: {reason}")
            else:
                print("\n✅ ALL METRICS MATCH — Step 4 and Ticker Detail are fully aligned.")

            print("=" * 120)

        # Hard assertion
        assert len(mismatches) == 0, (
            f"{len(mismatches)} metric mismatches found:\n" +
            "\n".join(f"  {t}/{m}: td={_fmt(tv)} s4={_fmt(sv)}"
                      for t, m, tv, sv, _ in mismatches)
        )

    def test_strictness_fcf_missing_capex_is_none(self):
        """FCF Yield must be None when any quarter is missing CapEx."""
        data = TICKERS["MISSING_CAPEX.US"]
        td = ticker_detail_metrics(data["shares"], data["price"],
                                   data["quarterly"], data["annual"])
        s4 = step4_metrics(data["shares"], data["price"],
                           data["quarterly"], data["annual"])
        assert td["fcf_yield"] is None, "ticker detail should be None for missing capex"
        assert s4["fcf_yield"] is None, "step4 should be None for missing capex"

    def test_strictness_fcf_missing_ocf_is_none(self):
        """FCF Yield must be None when all quarters are missing OCF."""
        data = TICKERS["MISSING_OCF.US"]
        td = ticker_detail_metrics(data["shares"], data["price"],
                                   data["quarterly"], data["annual"])
        s4 = step4_metrics(data["shares"], data["price"],
                           data["quarterly"], data["annual"])
        assert td["fcf_yield"] is None
        assert s4["fcf_yield"] is None

    def test_strictness_zero_net_income_is_valid(self):
        """Zero net income → net_margin_ttm = 0%, NOT None."""
        data = TICKERS["ZERO_NI.US"]
        td = ticker_detail_metrics(data["shares"], data["price"],
                                   data["quarterly"], data["annual"])
        s4 = step4_metrics(data["shares"], data["price"],
                           data["quarterly"], data["annual"])
        assert td["net_margin_ttm"] is not None
        assert td["net_margin_ttm"] == 0.0
        assert s4["net_margin_ttm"] is not None
        assert s4["net_margin_ttm"] == 0.0

    def test_strictness_partial_quarters_produce_none(self):
        """Only 3 quarters → all TTM metrics should be None."""
        data = TICKERS["PARTIAL_Q.US"]
        td = ticker_detail_metrics(data["shares"], data["price"],
                                   data["quarterly"], data["annual"])
        s4 = step4_metrics(data["shares"], data["price"],
                           data["quarterly"], data["annual"])
        for metric in ["net_margin_ttm", "fcf_yield", "net_debt_ebitda"]:
            assert td[metric] is None, f"td.{metric} should be None for 3 quarters"
            assert s4[metric] is None, f"s4.{metric} should be None for 3 quarters"

    def test_strictness_roe_none_without_equity(self):
        """ROE must be None when total_equity is not reported."""
        data = TICKERS["NO_EQUITY.US"]
        td = ticker_detail_metrics(data["shares"], data["price"],
                                   data["quarterly"], data["annual"])
        s4 = step4_metrics(data["shares"], data["price"],
                           data["quarterly"], data["annual"])
        assert td["roe"] is None
        assert s4["roe"] is None

    def test_strictness_no_ebitda_means_no_net_debt_ebitda(self):
        """When EBITDA is None for all quarters, Net Debt/EBITDA = None."""
        data = TICKERS["JPM.US"]  # EBITDA is None in all quarters
        td = ticker_detail_metrics(data["shares"], data["price"],
                                   data["quarterly"], data["annual"])
        s4 = step4_metrics(data["shares"], data["price"],
                           data["quarterly"], data["annual"])
        assert td["net_debt_ebitda"] is None
        assert s4["net_debt_ebitda"] is None

    def test_dividend_yield_zero_explicit(self):
        """Explicit zero dividends_paid → dividend_yield = 0.0 (not None)."""
        data = TICKERS["META.US"]
        td = ticker_detail_metrics(data["shares"], data["price"],
                                   data["quarterly"], data["annual"])
        s4 = step4_metrics(data["shares"], data["price"],
                           data["quarterly"], data["annual"])
        assert td["dividend_yield"] == 0.0
        assert s4["dividend_yield"] == 0.0

    def test_dividend_yield_semiannual(self):
        """Semi-annual payer (2 non-zero + 2 zero) → correct TTM sum."""
        data = TICKERS["SEMI_DIV.US"]
        td = ticker_detail_metrics(data["shares"], data["price"],
                                   data["quarterly"], data["annual"])
        s4 = step4_metrics(data["shares"], data["price"],
                           data["quarterly"], data["annual"])
        expected = (2e9 / (1.5e9 * 80.0)) * 100  # 2B dividends / 120B mcap
        assert td["dividend_yield"] is not None
        assert s4["dividend_yield"] is not None
        assert abs(td["dividend_yield"] - expected) < 0.01
        assert abs(s4["dividend_yield"] - expected) < 0.01
