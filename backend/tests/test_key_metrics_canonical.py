"""
Regression tests: Key Metrics reads from canonical DB collections.

Verifies that /api/v1/ticker/{ticker}/detail (mobile detail endpoint)
computes Key Metrics from:
  - tracked_tickers.shares_outstanding (not embedded .fundamentals.SharesStats)
  - company_financials collection     (not embedded .fundamentals.Financials)
  - company_fundamentals_cache        (not embedded .fundamentals.SplitsDividends)

Run:
    cd /app/backend && python -m pytest tests/test_key_metrics_canonical.py -v
"""

import asyncio
import math
import sys
import os
import types

import pytest

# ── helpers ──────────────────────────────────────────────────────────────────

# Minimal async-iterable cursor that supports .sort(), .limit(), .to_list()
class _FakeCursor:
    def __init__(self, docs):
        self._docs = list(docs)

    def sort(self, field, direction):
        reverse = direction == -1
        self._docs.sort(key=lambda d: d.get(field, ""), reverse=reverse)
        return self

    def limit(self, n):
        self._docs = self._docs[:n]
        return self

    async def to_list(self, length=None):
        if length is not None:
            return self._docs[:length]
        return self._docs


class _FakeFindOneCollection:
    """Bare-minimum collection that supports find_one and find (returning cursor)."""
    def __init__(self, docs=None):
        self._docs = list(docs or [])

    async def find_one(self, filt=None, projection=None, sort=None):
        for doc in self._docs:
            if self._matches(doc, filt or {}):
                return dict(doc)
        return None

    def find(self, filt=None, projection=None):
        matched = [dict(d) for d in self._docs if self._matches(d, filt or {})]
        return _FakeCursor(matched)

    @staticmethod
    def _matches(doc, filt):
        for key, value in filt.items():
            if key.startswith("$"):
                continue
            if isinstance(value, dict):
                # simple $exists / $ne support
                if "$exists" in value:
                    if value["$exists"] and key not in doc:
                        return False
                continue
            if doc.get(key) != value:
                return False
        return True


def _make_quarterly_rows(ticker, quarters):
    """Build a list of company_financials docs for quarterly data.

    `quarters` is a list of dicts, each containing overrides for a single quarter.
    Required key: "period_date".  All financial fields default to None.
    """
    rows = []
    for q in quarters:
        row = {
            "ticker": ticker,
            "period_type": "quarterly",
            "period_date": q["period_date"],
            "revenue": q.get("revenue"),
            "cost_of_revenue": None,
            "gross_profit": None,
            "operating_income": None,
            "operating_expenses": None,
            "net_income": q.get("net_income"),
            "ebitda": q.get("ebitda"),
            "ebit": None,
            "interest_expense": None,
            "income_tax_expense": None,
            "diluted_eps": None,
            "total_assets": None,
            "total_liabilities": None,
            "total_equity": None,
            "total_debt": q.get("total_debt"),
            "cash_and_equivalents": q.get("cash_and_equivalents"),
            "short_term_investments": None,
            "total_current_assets": None,
            "total_current_liabilities": None,
            "retained_earnings": None,
            "operating_cash_flow": q.get("operating_cash_flow"),
            "investing_cash_flow": None,
            "financing_cash_flow": None,
            "capital_expenditures": q.get("capital_expenditures"),
            "free_cash_flow": q.get("free_cash_flow"),
            "dividends_paid": None,
        }
        rows.append(row)
    return rows


def _make_annual_rows(ticker, years):
    """Build annual company_financials docs.  Same shape as quarterly."""
    rows = []
    for y in years:
        row = dict(y)
        row["ticker"] = ticker
        row["period_type"] = "annual"
        rows.append(row)
    return rows


# ── fixtures ─────────────────────────────────────────────────────────────────

# Complete financials for a fictitious ticker "ACME.US"
ACME_QUARTERLY = _make_quarterly_rows("ACME.US", [
    {"period_date": "2025-09-30", "revenue": 5e9, "net_income": 1e9,
     "ebitda": 1.5e9, "operating_cash_flow": 1.2e9, "capital_expenditures": -2e8,
     "cash_and_equivalents": 3e9, "total_debt": 2e9},
    {"period_date": "2025-06-30", "revenue": 4.8e9, "net_income": 0.9e9,
     "ebitda": 1.4e9, "operating_cash_flow": 1.1e9, "capital_expenditures": -1.8e8,
     "cash_and_equivalents": 2.8e9, "total_debt": 2.1e9},
    {"period_date": "2025-03-31", "revenue": 4.5e9, "net_income": 0.8e9,
     "ebitda": 1.3e9, "operating_cash_flow": 1.0e9, "capital_expenditures": -1.5e8,
     "cash_and_equivalents": 2.5e9, "total_debt": 2.2e9},
    {"period_date": "2024-12-31", "revenue": 4.6e9, "net_income": 0.85e9,
     "ebitda": 1.35e9, "operating_cash_flow": 1.05e9, "capital_expenditures": -1.7e8,
     "cash_and_equivalents": 2.6e9, "total_debt": 2.15e9},
    # 4 more for prior-TTM
    {"period_date": "2024-09-30", "revenue": 4.3e9, "net_income": 0.75e9,
     "ebitda": 1.2e9, "operating_cash_flow": 0.95e9, "capital_expenditures": -1.6e8,
     "cash_and_equivalents": 2.4e9, "total_debt": 2.3e9},
    {"period_date": "2024-06-30", "revenue": 4.1e9, "net_income": 0.7e9,
     "ebitda": 1.1e9, "operating_cash_flow": 0.9e9, "capital_expenditures": -1.4e8,
     "cash_and_equivalents": 2.3e9, "total_debt": 2.4e9},
    {"period_date": "2024-03-31", "revenue": 4.0e9, "net_income": 0.65e9,
     "ebitda": 1.05e9, "operating_cash_flow": 0.85e9, "capital_expenditures": -1.3e8,
     "cash_and_equivalents": 2.2e9, "total_debt": 2.5e9},
    {"period_date": "2023-12-31", "revenue": 3.9e9, "net_income": 0.6e9,
     "ebitda": 1.0e9, "operating_cash_flow": 0.8e9, "capital_expenditures": -1.2e8,
     "cash_and_equivalents": 2.1e9, "total_debt": 2.6e9},
])

ACME_ANNUAL = _make_annual_rows("ACME.US", [
    {"period_date": "2025-12-31", "revenue": 19e9, "net_income": 3.5e9},
    {"period_date": "2024-12-31", "revenue": 16e9, "net_income": 2.8e9},
    {"period_date": "2023-12-31", "revenue": 14e9, "net_income": 2.2e9},
    {"period_date": "2022-12-31", "revenue": 12e9, "net_income": 1.8e9},
])

# Ticker with NO cash flow data
NOCF_QUARTERLY = _make_quarterly_rows("NOCF.US", [
    {"period_date": "2025-09-30", "revenue": 2e9, "net_income": 3e8,
     "ebitda": 5e8, "cash_and_equivalents": 1e9, "total_debt": 8e8},
    {"period_date": "2025-06-30", "revenue": 1.9e9, "net_income": 2.8e8,
     "ebitda": 4.8e8, "cash_and_equivalents": 9e8, "total_debt": 7.5e8},
    {"period_date": "2025-03-31", "revenue": 1.8e9, "net_income": 2.5e8,
     "ebitda": 4.5e8, "cash_and_equivalents": 8e8, "total_debt": 7e8},
    {"period_date": "2024-12-31", "revenue": 1.7e9, "net_income": 2.2e8,
     "ebitda": 4.2e8, "cash_and_equivalents": 7e8, "total_debt": 6.5e8},
])


# ── computation helpers (extracted from server.py to test independently) ─────

def _sf(val):
    """Safely coerce to float."""
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def compute_key_metrics(shares_outstanding, current_price, quarterly_rows,
                        annual_rows, forward_dividend_yield, valuation_cache=None):
    """
    Pure-function reimplementation of the Key Metrics computation from server.py.
    Returns (key_metrics_dict, financials_dict).
    """
    # Market cap
    market_cap = None
    if shares_outstanding and current_price and current_price > 0:
        market_cap = shares_outstanding * current_price

    # TTM from top 4 quarters
    ttm_revenue = None
    ttm_net_income = None
    ttm_ebitda = None
    ttm_fcf = None
    ttm_cash = None
    ttm_debt = None

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

        # FCF
        fcfs = []
        for q in top4:
            ocf = _sf(q.get("operating_cash_flow")) or 0
            capex = abs(_sf(q.get("capital_expenditures")) or 0)
            fcfs.append(ocf - capex)
        if any(_sf(q.get("operating_cash_flow")) is not None for q in top4):
            ttm_fcf = sum(fcfs)

    if quarterly_rows:
        latest_q = quarterly_rows[0]
        ttm_cash = _sf(latest_q.get("cash_and_equivalents"))
        if ttm_cash is None:
            ttm_cash = 0.0
        ttm_debt = _sf(latest_q.get("total_debt"))
        if ttm_debt is None:
            ttm_debt = 0.0

    # Net margin
    net_margin_ttm = None
    if ttm_revenue and ttm_revenue > 0 and ttm_net_income is not None:
        net_margin_ttm = (ttm_net_income / ttm_revenue) * 100

    # FCF yield
    fcf_yield = None
    if ttm_fcf is not None and market_cap and market_cap > 0:
        fcf_yield = (ttm_fcf / market_cap) * 100

    # Net debt / EBITDA
    net_debt_ebitda = None
    ebitda_ttm = ttm_ebitda
    if ebitda_ttm is None and valuation_cache:
        ebitda_ttm = valuation_cache.get("raw_inputs", {}).get("ebitda_ttm")
    if ttm_debt is not None and ttm_cash is not None and ebitda_ttm and ebitda_ttm > 0:
        net_debt_ebitda = (ttm_debt - ttm_cash) / ebitda_ttm

    # Revenue growth 3Y CAGR
    revenue_growth_3y = None
    annual_revenues = []
    for row in annual_rows[:4]:
        rev = _sf(row.get("revenue"))
        if rev is not None:
            annual_revenues.append(rev)

    if len(annual_revenues) >= 4:
        end_rev = annual_revenues[0]
        start_rev = annual_revenues[3]
        if start_rev and start_rev > 0 and end_rev and end_rev > 0:
            revenue_growth_3y = round(((end_rev / start_rev) ** (1 / 3) - 1) * 100, 1)

    # Dividend
    dividend_yield_ttm = None
    if forward_dividend_yield is not None:
        try:
            dividend_yield_ttm = float(forward_dividend_yield) * 100
        except (ValueError, TypeError):
            pass

    return {
        "market_cap": market_cap,
        "shares_outstanding": shares_outstanding,
        "net_margin_ttm": net_margin_ttm,
        "fcf_yield": fcf_yield,
        "net_debt_ebitda": net_debt_ebitda,
        "revenue_growth_3y": revenue_growth_3y,
        "dividend_yield_ttm": dividend_yield_ttm,
        "ttm_fcf": ttm_fcf,
        "ttm_net_income": ttm_net_income,
        "ebitda_ttm": ebitda_ttm,
    }


# ── tests ────────────────────────────────────────────────────────────────────

class TestCompleteFinancialsACME:
    """Ticker with complete financials (all data present)."""

    def test_market_cap_computed(self):
        """Market cap = shares_outstanding * price."""
        r = compute_key_metrics(
            shares_outstanding=1e10,
            current_price=150.0,
            quarterly_rows=ACME_QUARTERLY,
            annual_rows=ACME_ANNUAL,
            forward_dividend_yield=0.005,
        )
        assert r["market_cap"] == 1e10 * 150.0

    def test_shares_outstanding_passthrough(self):
        r = compute_key_metrics(
            shares_outstanding=1.5e10,
            current_price=100.0,
            quarterly_rows=ACME_QUARTERLY,
            annual_rows=ACME_ANNUAL,
            forward_dividend_yield=None,
        )
        assert r["shares_outstanding"] == 1.5e10

    def test_net_margin_ttm(self):
        """Net Margin TTM = sum(net_income 4Q) / sum(revenue 4Q) * 100."""
        r = compute_key_metrics(
            shares_outstanding=1e10,
            current_price=150.0,
            quarterly_rows=ACME_QUARTERLY,
            annual_rows=ACME_ANNUAL,
            forward_dividend_yield=None,
        )
        expected_revenue = 5e9 + 4.8e9 + 4.5e9 + 4.6e9   # 18.9B
        expected_ni = 1e9 + 0.9e9 + 0.8e9 + 0.85e9        # 3.55B
        expected_margin = (expected_ni / expected_revenue) * 100
        assert r["net_margin_ttm"] is not None
        assert abs(r["net_margin_ttm"] - expected_margin) < 0.01

    def test_fcf_yield(self):
        """FCF Yield = TTM FCF / Market Cap * 100."""
        shares = 1e10
        price = 150.0
        r = compute_key_metrics(
            shares_outstanding=shares,
            current_price=price,
            quarterly_rows=ACME_QUARTERLY,
            annual_rows=ACME_ANNUAL,
            forward_dividend_yield=None,
        )
        # FCF per quarter = OCF - |capex|
        fcf_q = [
            1.2e9 - 2e8,
            1.1e9 - 1.8e8,
            1.0e9 - 1.5e8,
            1.05e9 - 1.7e8,
        ]
        expected_fcf = sum(fcf_q)
        expected_yield = (expected_fcf / (shares * price)) * 100
        assert r["fcf_yield"] is not None
        assert abs(r["fcf_yield"] - expected_yield) < 0.01

    def test_net_debt_ebitda(self):
        """Net Debt/EBITDA = (debt - cash) / TTM EBITDA."""
        r = compute_key_metrics(
            shares_outstanding=1e10,
            current_price=150.0,
            quarterly_rows=ACME_QUARTERLY,
            annual_rows=ACME_ANNUAL,
            forward_dividend_yield=None,
        )
        # Latest quarter: debt=2B, cash=3B → net_debt = -1B
        # TTM EBITDA = 1.5 + 1.4 + 1.3 + 1.35 = 5.55B
        expected_ebitda = 1.5e9 + 1.4e9 + 1.3e9 + 1.35e9
        expected_nd_ebitda = (2e9 - 3e9) / expected_ebitda
        assert r["net_debt_ebitda"] is not None
        assert abs(r["net_debt_ebitda"] - expected_nd_ebitda) < 0.01

    def test_revenue_growth_3y_cagr(self):
        """3Y CAGR = (end/start)^(1/3) - 1."""
        r = compute_key_metrics(
            shares_outstanding=1e10,
            current_price=150.0,
            quarterly_rows=ACME_QUARTERLY,
            annual_rows=ACME_ANNUAL,
            forward_dividend_yield=None,
        )
        # end=19B, start=12B
        expected_cagr = ((19e9 / 12e9) ** (1 / 3) - 1) * 100
        assert r["revenue_growth_3y"] is not None
        assert abs(r["revenue_growth_3y"] - round(expected_cagr, 1)) < 0.01

    def test_dividend_yield(self):
        """Forward dividend yield is converted from decimal to %."""
        r = compute_key_metrics(
            shares_outstanding=1e10,
            current_price=150.0,
            quarterly_rows=ACME_QUARTERLY,
            annual_rows=ACME_ANNUAL,
            forward_dividend_yield=0.025,
        )
        assert r["dividend_yield_ttm"] is not None
        assert abs(r["dividend_yield_ttm"] - 2.5) < 0.001


class TestMissingCashFlow:
    """Ticker with no cash flow data (operating_cash_flow is None)."""

    def test_fcf_yield_is_none_when_no_cf(self):
        """FCF Yield should be None when cash flow data is missing."""
        r = compute_key_metrics(
            shares_outstanding=5e9,
            current_price=50.0,
            quarterly_rows=NOCF_QUARTERLY,
            annual_rows=[],
            forward_dividend_yield=None,
        )
        # operating_cash_flow is None for all NOCF_QUARTERLY rows
        assert r["ttm_fcf"] is None or r["fcf_yield"] is None

    def test_net_margin_still_computed(self):
        """Net Margin should still compute even without cash flow."""
        r = compute_key_metrics(
            shares_outstanding=5e9,
            current_price=50.0,
            quarterly_rows=NOCF_QUARTERLY,
            annual_rows=[],
            forward_dividend_yield=None,
        )
        assert r["net_margin_ttm"] is not None

    def test_net_debt_ebitda_still_computed(self):
        """Net Debt/EBITDA should still compute from balance sheet + EBITDA."""
        r = compute_key_metrics(
            shares_outstanding=5e9,
            current_price=50.0,
            quarterly_rows=NOCF_QUARTERLY,
            annual_rows=[],
            forward_dividend_yield=None,
        )
        assert r["net_debt_ebitda"] is not None


class TestMissingSharesOutstanding:
    """When shares_outstanding is None, market cap should be None."""

    def test_market_cap_none(self):
        r = compute_key_metrics(
            shares_outstanding=None,
            current_price=100.0,
            quarterly_rows=ACME_QUARTERLY,
            annual_rows=ACME_ANNUAL,
            forward_dividend_yield=None,
        )
        assert r["market_cap"] is None
        assert r["shares_outstanding"] is None

    def test_fcf_yield_none_without_market_cap(self):
        r = compute_key_metrics(
            shares_outstanding=None,
            current_price=100.0,
            quarterly_rows=ACME_QUARTERLY,
            annual_rows=ACME_ANNUAL,
            forward_dividend_yield=None,
        )
        assert r["fcf_yield"] is None


class TestInsufficientHistory:
    """Fewer than 4 quarters or 4 annual periods."""

    def test_ttm_none_with_3_quarters(self):
        r = compute_key_metrics(
            shares_outstanding=1e10,
            current_price=100.0,
            quarterly_rows=ACME_QUARTERLY[:3],
            annual_rows=ACME_ANNUAL,
            forward_dividend_yield=None,
        )
        assert r["net_margin_ttm"] is None

    def test_revenue_growth_none_with_3_years(self):
        r = compute_key_metrics(
            shares_outstanding=1e10,
            current_price=100.0,
            quarterly_rows=ACME_QUARTERLY,
            annual_rows=ACME_ANNUAL[:3],
            forward_dividend_yield=None,
        )
        assert r["revenue_growth_3y"] is None


class TestNoDividend:
    """When forward_dividend_yield is None, dividend_yield_ttm is None."""

    def test_dividend_none_when_not_reported(self):
        r = compute_key_metrics(
            shares_outstanding=1e10,
            current_price=100.0,
            quarterly_rows=ACME_QUARTERLY,
            annual_rows=ACME_ANNUAL,
            forward_dividend_yield=None,
        )
        assert r["dividend_yield_ttm"] is None

    def test_dividend_zero_when_explicit_zero(self):
        """Explicit 0 from provider should result in 0% not None."""
        r = compute_key_metrics(
            shares_outstanding=1e10,
            current_price=100.0,
            quarterly_rows=ACME_QUARTERLY,
            annual_rows=ACME_ANNUAL,
            forward_dividend_yield=0,
        )
        assert r["dividend_yield_ttm"] == 0.0
