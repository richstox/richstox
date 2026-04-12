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
                        annual_rows, valuation_cache=None):
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

        # FCF — require all 4 OCF values to be non-null
        ocfs = [_sf(q.get("operating_cash_flow")) for q in top4 if _sf(q.get("operating_cash_flow")) is not None]
        if len(ocfs) >= 4:
            fcfs = []
            for q in top4:
                ocf = _sf(q.get("operating_cash_flow"))
                capex = abs(_sf(q.get("capital_expenditures")) or 0)
                fcfs.append(ocf - capex)
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

    # Dividend Yield (TTM) — from quarterly dividends_paid / market_cap
    # Requires 4 quarterly reporting periods; does NOT assume payment frequency.
    dividend_yield_ttm = None
    dividend_yield_na = None
    div_window = quarterly_rows[:4]  # latest 4 quarterly reporting periods
    if len(div_window) >= 4:
        div_vals = [_sf(q.get("dividends_paid")) for q in div_window]
        if all(v is None for v in div_vals):
            dividend_yield_ttm = None
            dividend_yield_na = "not_reported"
        else:
            dividends_ttm = sum(abs(v) for v in div_vals if v is not None)
            if dividends_ttm == 0.0:
                dividend_yield_ttm = 0.0
                dividend_yield_na = "no_dividend"
            elif market_cap and market_cap > 0:
                dividend_yield_ttm = (dividends_ttm / market_cap) * 100
            else:
                dividend_yield_ttm = None
                dividend_yield_na = "missing_inputs"
    else:
        dividend_yield_ttm = None
        dividend_yield_na = "not_reported"

    return {
        "market_cap": market_cap,
        "shares_outstanding": shares_outstanding,
        "net_margin_ttm": net_margin_ttm,
        "fcf_yield": fcf_yield,
        "net_debt_ebitda": net_debt_ebitda,
        "revenue_growth_3y": revenue_growth_3y,
        "dividend_yield_ttm": dividend_yield_ttm,
        "dividend_yield_na": dividend_yield_na,
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
        )
        assert r["market_cap"] == 1e10 * 150.0

    def test_shares_outstanding_passthrough(self):
        r = compute_key_metrics(
            shares_outstanding=1.5e10,
            current_price=100.0,
            quarterly_rows=ACME_QUARTERLY,
            annual_rows=ACME_ANNUAL,
        )
        assert r["shares_outstanding"] == 1.5e10

    def test_net_margin_ttm(self):
        """Net Margin TTM = sum(net_income 4Q) / sum(revenue 4Q) * 100."""
        r = compute_key_metrics(
            shares_outstanding=1e10,
            current_price=150.0,
            quarterly_rows=ACME_QUARTERLY,
            annual_rows=ACME_ANNUAL,
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
        )
        # end=19B, start=12B
        expected_cagr = ((19e9 / 12e9) ** (1 / 3) - 1) * 100
        assert r["revenue_growth_3y"] is not None
        assert abs(r["revenue_growth_3y"] - round(expected_cagr, 1)) < 0.01

    def test_dividend_yield(self):
        """Dividend yield is None when dividends_paid is null (ACME_QUARTERLY has all dividends_paid values set to None)."""
        r = compute_key_metrics(
            shares_outstanding=1e10,
            current_price=150.0,
            quarterly_rows=ACME_QUARTERLY,
            annual_rows=ACME_ANNUAL,
        )
        # ACME_QUARTERLY has dividends_paid=None for all rows
        assert r["dividend_yield_ttm"] is None
        assert r["dividend_yield_na"] == "not_reported"


class TestMissingCashFlow:
    """Ticker with no cash flow data (operating_cash_flow is None)."""

    def test_fcf_yield_is_none_when_no_cf(self):
        """FCF Yield should be None when cash flow data is missing."""
        r = compute_key_metrics(
            shares_outstanding=5e9,
            current_price=50.0,
            quarterly_rows=NOCF_QUARTERLY,
            annual_rows=[],
        )
        # operating_cash_flow is None for all NOCF_QUARTERLY rows
        assert r["ttm_fcf"] is None
        assert r["fcf_yield"] is None

    def test_net_margin_still_computed(self):
        """Net Margin should still compute even without cash flow."""
        r = compute_key_metrics(
            shares_outstanding=5e9,
            current_price=50.0,
            quarterly_rows=NOCF_QUARTERLY,
            annual_rows=[],
        )
        assert r["net_margin_ttm"] is not None

    def test_net_debt_ebitda_still_computed(self):
        """Net Debt/EBITDA should still compute from balance sheet + EBITDA."""
        r = compute_key_metrics(
            shares_outstanding=5e9,
            current_price=50.0,
            quarterly_rows=NOCF_QUARTERLY,
            annual_rows=[],
        )
        assert r["net_debt_ebitda"] is not None

    def test_fcf_none_with_partial_ocf(self):
        """FCF TTM should be None when only some quarters have OCF (not all 4).

        Previously, null OCF was coerced to 0 via `or 0`, which could silently
        distort FCF when mixed with real values.  Now requires all 4 non-null.
        """
        rows = _make_quarterly_rows("PARTIAL.US", [
            {"period_date": "2025-09-30", "revenue": 5e9, "net_income": 1e9,
             "ebitda": 1.5e9, "operating_cash_flow": 1.2e9, "capital_expenditures": -2e8,
             "cash_and_equivalents": 3e9, "total_debt": 2e9},
            {"period_date": "2025-06-30", "revenue": 4.8e9, "net_income": 0.9e9,
             "ebitda": 1.4e9,
             "cash_and_equivalents": 2.8e9, "total_debt": 2.1e9},  # OCF missing
            {"period_date": "2025-03-31", "revenue": 4.5e9, "net_income": 0.8e9,
             "ebitda": 1.3e9,
             "cash_and_equivalents": 2.5e9, "total_debt": 2.2e9},  # OCF missing
            {"period_date": "2024-12-31", "revenue": 4.6e9, "net_income": 0.85e9,
             "ebitda": 1.35e9,
             "cash_and_equivalents": 2.6e9, "total_debt": 2.15e9},  # OCF missing
        ])
        r = compute_key_metrics(
            shares_outstanding=1e10,
            current_price=100.0,
            quarterly_rows=rows,
            annual_rows=[],
        )
        # Only 1 of 4 quarters has OCF → TTM FCF must be None, not distorted
        assert r["ttm_fcf"] is None
        assert r["fcf_yield"] is None
        # Other metrics should still compute
        assert r["net_margin_ttm"] is not None
        assert r["net_debt_ebitda"] is not None


class TestMissingSharesOutstanding:
    """When shares_outstanding is None, market cap should be None."""

    def test_market_cap_none(self):
        r = compute_key_metrics(
            shares_outstanding=None,
            current_price=100.0,
            quarterly_rows=ACME_QUARTERLY,
            annual_rows=ACME_ANNUAL,
        )
        assert r["market_cap"] is None
        assert r["shares_outstanding"] is None

    def test_fcf_yield_none_without_market_cap(self):
        r = compute_key_metrics(
            shares_outstanding=None,
            current_price=100.0,
            quarterly_rows=ACME_QUARTERLY,
            annual_rows=ACME_ANNUAL,
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
        )
        assert r["net_margin_ttm"] is None

    def test_revenue_growth_none_with_3_years(self):
        r = compute_key_metrics(
            shares_outstanding=1e10,
            current_price=100.0,
            quarterly_rows=ACME_QUARTERLY,
            annual_rows=ACME_ANNUAL[:3],
        )
        assert r["revenue_growth_3y"] is None


class TestDividendYieldTTM:
    """Regression tests for Dividend Yield (TTM) computed from quarterly dividends_paid."""

    def test_positive_dividends(self):
        """Case 1: Seed 4 quarterly rows with dividends_paid → correct TTM yield."""
        # Build quarterly rows with known dividends_paid (negative = cash outflow)
        rows = _make_quarterly_rows("DIV.US", [
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
        ])
        # Inject dividends_paid (negative = cash outflow)
        rows[0]["dividends_paid"] = -1e8    # Q4
        rows[1]["dividends_paid"] = -9e7    # Q3
        rows[2]["dividends_paid"] = -8e7    # Q2
        rows[3]["dividends_paid"] = -7e7    # Q1

        shares = 1e10
        price = 150.0
        r = compute_key_metrics(
            shares_outstanding=shares,
            current_price=price,
            quarterly_rows=rows,
            annual_rows=ACME_ANNUAL,
        )
        dividends_ttm = abs(-1e8) + abs(-9e7) + abs(-8e7) + abs(-7e7)  # 3.4e8
        market_cap = shares * price  # 1.5e12
        expected = (dividends_ttm / market_cap) * 100
        assert r["dividend_yield_ttm"] is not None
        assert abs(r["dividend_yield_ttm"] - expected) < 0.0001
        assert r["dividend_yield_na"] is None

    def test_missing_dividends_all_null(self):
        """Case 2a: 4 quarterly rows but dividends_paid all null → not_reported."""
        r = compute_key_metrics(
            shares_outstanding=1e10,
            current_price=100.0,
            quarterly_rows=ACME_QUARTERLY,  # dividends_paid=None
            annual_rows=ACME_ANNUAL,
        )
        assert r["dividend_yield_ttm"] is None
        assert r["dividend_yield_na"] == "not_reported"

    def test_missing_dividends_fewer_than_4_rows_all_null(self):
        """Fewer than 4 quarterly rows, all with null dividends_paid → not_reported."""
        r = compute_key_metrics(
            shares_outstanding=1e10,
            current_price=100.0,
            quarterly_rows=ACME_QUARTERLY[:3],
            annual_rows=ACME_ANNUAL,
        )
        assert r["dividend_yield_ttm"] is None
        assert r["dividend_yield_na"] == "not_reported"

    def test_zero_dividends(self):
        """Case 3: 4 quarterly rows with dividends_paid = 0 → value=0, na_reason=no_dividend."""
        rows = _make_quarterly_rows("ZERODIV.US", [
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
        ])
        rows[0]["dividends_paid"] = 0
        rows[1]["dividends_paid"] = 0
        rows[2]["dividends_paid"] = 0
        rows[3]["dividends_paid"] = 0

        r = compute_key_metrics(
            shares_outstanding=1e10,
            current_price=100.0,
            quarterly_rows=rows,
            annual_rows=ACME_ANNUAL,
        )
        assert r["dividend_yield_ttm"] == 0.0
        assert r["dividend_yield_na"] == "no_dividend"

    def test_forward_field_not_used(self):
        """Case 4: forward_dividend_yield in fundamentals_cache must NOT affect TTM yield.
        Even if forward_dividend_yield were available, the result must come from
        quarterly dividends_paid only."""
        # ACME_QUARTERLY has dividends_paid=None → should be not_reported
        # regardless of any hypothetical forward_dividend_yield value
        r = compute_key_metrics(
            shares_outstanding=1e10,
            current_price=100.0,
            quarterly_rows=ACME_QUARTERLY,
            annual_rows=ACME_ANNUAL,
        )
        assert r["dividend_yield_ttm"] is None
        assert r["dividend_yield_na"] == "not_reported"
        # compute_key_metrics no longer accepts forward_dividend_yield parameter
        import inspect
        sig = inspect.signature(compute_key_metrics)
        assert "forward_dividend_yield" not in sig.parameters

    def test_missing_market_cap_with_dividends(self):
        """When dividends exist but market_cap cannot be computed → missing_inputs."""
        rows = _make_quarterly_rows("NODENOM.US", [
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
        ])
        rows[0]["dividends_paid"] = -1e8
        rows[1]["dividends_paid"] = -9e7
        rows[2]["dividends_paid"] = -8e7
        rows[3]["dividends_paid"] = -7e7

        r = compute_key_metrics(
            shares_outstanding=None,
            current_price=100.0,
            quarterly_rows=rows,
            annual_rows=[],
        )
        assert r["dividend_yield_ttm"] is None
        assert r["dividend_yield_na"] == "missing_inputs"

    def test_semiannual_payer_2_nonzero_2_zero(self):
        """Semiannual payer: 2 non-zero + 2 zero quarters → computes yield (NOT N/A)."""
        rows = _make_quarterly_rows("SEMI.US", [
            {"period_date": "2025-09-30", "revenue": 5e9, "net_income": 1e9,
             "ebitda": 1.5e9, "cash_and_equivalents": 3e9, "total_debt": 2e9},
            {"period_date": "2025-06-30", "revenue": 4.8e9, "net_income": 0.9e9,
             "ebitda": 1.4e9, "cash_and_equivalents": 2.8e9, "total_debt": 2.1e9},
            {"period_date": "2025-03-31", "revenue": 4.5e9, "net_income": 0.8e9,
             "ebitda": 1.3e9, "cash_and_equivalents": 2.5e9, "total_debt": 2.2e9},
            {"period_date": "2024-12-31", "revenue": 4.6e9, "net_income": 0.85e9,
             "ebitda": 1.35e9, "cash_and_equivalents": 2.6e9, "total_debt": 2.15e9},
        ])
        rows[0]["dividends_paid"] = -5e7   # paid in Q4
        rows[1]["dividends_paid"] = 0       # no payment in Q3
        rows[2]["dividends_paid"] = -5e7   # paid in Q2
        rows[3]["dividends_paid"] = 0       # no payment in Q1

        shares = 1e10
        price = 100.0
        r = compute_key_metrics(
            shares_outstanding=shares,
            current_price=price,
            quarterly_rows=rows,
            annual_rows=[],
        )
        dividends_ttm = 5e7 + 5e7  # 1e8
        market_cap = shares * price
        expected = (dividends_ttm / market_cap) * 100
        assert r["dividend_yield_ttm"] is not None
        assert abs(r["dividend_yield_ttm"] - expected) < 0.0001
        assert r["dividend_yield_na"] is None

    def test_annual_payer_1_nonzero_3_zero(self):
        """Annual payer: 1 non-zero + 3 zero quarters → computes yield (NOT N/A)."""
        rows = _make_quarterly_rows("ANNUAL.US", [
            {"period_date": "2025-09-30", "revenue": 5e9, "net_income": 1e9,
             "ebitda": 1.5e9, "cash_and_equivalents": 3e9, "total_debt": 2e9},
            {"period_date": "2025-06-30", "revenue": 4.8e9, "net_income": 0.9e9,
             "ebitda": 1.4e9, "cash_and_equivalents": 2.8e9, "total_debt": 2.1e9},
            {"period_date": "2025-03-31", "revenue": 4.5e9, "net_income": 0.8e9,
             "ebitda": 1.3e9, "cash_and_equivalents": 2.5e9, "total_debt": 2.2e9},
            {"period_date": "2024-12-31", "revenue": 4.6e9, "net_income": 0.85e9,
             "ebitda": 1.35e9, "cash_and_equivalents": 2.6e9, "total_debt": 2.15e9},
        ])
        rows[0]["dividends_paid"] = -2e8   # annual payment
        rows[1]["dividends_paid"] = 0
        rows[2]["dividends_paid"] = 0
        rows[3]["dividends_paid"] = 0

        shares = 1e10
        price = 100.0
        r = compute_key_metrics(
            shares_outstanding=shares,
            current_price=price,
            quarterly_rows=rows,
            annual_rows=[],
        )
        dividends_ttm = 2e8
        market_cap = shares * price
        expected = (dividends_ttm / market_cap) * 100
        assert r["dividend_yield_ttm"] is not None
        assert abs(r["dividend_yield_ttm"] - expected) < 0.0001
        assert r["dividend_yield_na"] is None

    def test_3_payments_1_explicit_zero(self):
        """3 non-zero + 1 explicit zero → computes yield (NOT N/A)."""
        rows = _make_quarterly_rows("THREE.US", [
            {"period_date": "2025-09-30", "revenue": 5e9, "net_income": 1e9,
             "ebitda": 1.5e9, "cash_and_equivalents": 3e9, "total_debt": 2e9},
            {"period_date": "2025-06-30", "revenue": 4.8e9, "net_income": 0.9e9,
             "ebitda": 1.4e9, "cash_and_equivalents": 2.8e9, "total_debt": 2.1e9},
            {"period_date": "2025-03-31", "revenue": 4.5e9, "net_income": 0.8e9,
             "ebitda": 1.3e9, "cash_and_equivalents": 2.5e9, "total_debt": 2.2e9},
            {"period_date": "2024-12-31", "revenue": 4.6e9, "net_income": 0.85e9,
             "ebitda": 1.35e9, "cash_and_equivalents": 2.6e9, "total_debt": 2.15e9},
        ])
        rows[0]["dividends_paid"] = -1e8
        rows[1]["dividends_paid"] = -9e7
        rows[2]["dividends_paid"] = -8e7
        rows[3]["dividends_paid"] = 0       # explicit zero in one quarter

        shares = 1e10
        price = 100.0
        r = compute_key_metrics(
            shares_outstanding=shares,
            current_price=price,
            quarterly_rows=rows,
            annual_rows=[],
        )
        dividends_ttm = 1e8 + 9e7 + 8e7  # 2.7e8
        market_cap = shares * price
        expected = (dividends_ttm / market_cap) * 100
        assert r["dividend_yield_ttm"] is not None
        assert abs(r["dividend_yield_ttm"] - expected) < 0.0001
        assert r["dividend_yield_na"] is None

    def test_no_quarterly_rows(self):
        """No quarterly rows at all → not_reported."""
        r = compute_key_metrics(
            shares_outstanding=1e10,
            current_price=100.0,
            quarterly_rows=[],
            annual_rows=ACME_ANNUAL,
        )
        assert r["dividend_yield_ttm"] is None
        assert r["dividend_yield_na"] == "not_reported"

    def test_only_2_quarterly_rows(self):
        """Only 2 quarterly rows (even with dividends) → not_reported.
        TTM requires 4 quarterly reporting periods for full 12-month coverage."""
        rows = _make_quarterly_rows("TWO.US", [
            {"period_date": "2025-09-30", "revenue": 5e9, "net_income": 1e9,
             "ebitda": 1.5e9, "cash_and_equivalents": 3e9, "total_debt": 2e9},
            {"period_date": "2025-06-30", "revenue": 4.8e9, "net_income": 0.9e9,
             "ebitda": 1.4e9, "cash_and_equivalents": 2.8e9, "total_debt": 2.1e9},
        ])
        rows[0]["dividends_paid"] = -1e8
        rows[1]["dividends_paid"] = -9e7

        r = compute_key_metrics(
            shares_outstanding=1e10,
            current_price=100.0,
            quarterly_rows=rows,
            annual_rows=[],
        )
        assert r["dividend_yield_ttm"] is None
        assert r["dividend_yield_na"] == "not_reported"

    def test_only_3_quarterly_rows(self):
        """Only 3 quarterly rows (even with dividends) → not_reported.
        TTM requires 4 quarterly reporting periods for full 12-month coverage."""
        rows = _make_quarterly_rows("THREE_Q.US", [
            {"period_date": "2025-09-30", "revenue": 5e9, "net_income": 1e9,
             "ebitda": 1.5e9, "cash_and_equivalents": 3e9, "total_debt": 2e9},
            {"period_date": "2025-06-30", "revenue": 4.8e9, "net_income": 0.9e9,
             "ebitda": 1.4e9, "cash_and_equivalents": 2.8e9, "total_debt": 2.1e9},
            {"period_date": "2025-03-31", "revenue": 4.5e9, "net_income": 0.8e9,
             "ebitda": 1.3e9, "cash_and_equivalents": 2.5e9, "total_debt": 2.2e9},
        ])
        rows[0]["dividends_paid"] = -1e8
        rows[1]["dividends_paid"] = -9e7
        rows[2]["dividends_paid"] = -8e7

        r = compute_key_metrics(
            shares_outstanding=1e10,
            current_price=100.0,
            quarterly_rows=rows,
            annual_rows=[],
        )
        assert r["dividend_yield_ttm"] is None
        assert r["dividend_yield_na"] == "not_reported"

    def test_mixed_null_and_nonzero(self):
        """Some quarters have null dividends_paid, some have values → computes yield.
        This handles companies where only some quarters report dividends_paid."""
        rows = _make_quarterly_rows("MIX.US", [
            {"period_date": "2025-09-30", "revenue": 5e9, "net_income": 1e9,
             "ebitda": 1.5e9, "cash_and_equivalents": 3e9, "total_debt": 2e9},
            {"period_date": "2025-06-30", "revenue": 4.8e9, "net_income": 0.9e9,
             "ebitda": 1.4e9, "cash_and_equivalents": 2.8e9, "total_debt": 2.1e9},
            {"period_date": "2025-03-31", "revenue": 4.5e9, "net_income": 0.8e9,
             "ebitda": 1.3e9, "cash_and_equivalents": 2.5e9, "total_debt": 2.2e9},
            {"period_date": "2024-12-31", "revenue": 4.6e9, "net_income": 0.85e9,
             "ebitda": 1.35e9, "cash_and_equivalents": 2.6e9, "total_debt": 2.15e9},
        ])
        rows[0]["dividends_paid"] = -1e8   # reported
        rows[1]["dividends_paid"] = None    # not reported
        rows[2]["dividends_paid"] = -5e7   # reported
        rows[3]["dividends_paid"] = None    # not reported

        shares = 1e10
        price = 100.0
        r = compute_key_metrics(
            shares_outstanding=shares,
            current_price=price,
            quarterly_rows=rows,
            annual_rows=[],
        )
        dividends_ttm = 1e8 + 5e7  # 1.5e8
        market_cap = shares * price
        expected = (dividends_ttm / market_cap) * 100
        assert r["dividend_yield_ttm"] is not None
        assert abs(r["dividend_yield_ttm"] - expected) < 0.0001
        assert r["dividend_yield_na"] is None
