"""
Tests for key_metrics_proof — deterministic audit of Key Metrics calculations.

Verifies that ``compute_proof()`` mirrors the exact logic used by
``get_ticker_detail_mobile()`` in ``server.py`` for every Key Metric.

Run:
    cd /app/backend && python -m pytest tests/test_key_metrics_proof.py -v
"""

import math
import pytest

# Import the proof function under test
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from key_metrics_proof import compute_proof, _sf


# ── test fixtures ────────────────────────────────────────────────────────────

def _make_quarterly_rows(ticker, quarters):
    """Build company_financials docs for quarterly data."""
    rows = []
    for q in quarters:
        row = {
            "ticker": ticker,
            "period_type": "quarterly",
            "period_date": q["period_date"],
            "revenue": q.get("revenue"),
            "net_income": q.get("net_income"),
            "ebitda": q.get("ebitda"),
            "operating_cash_flow": q.get("operating_cash_flow"),
            "capital_expenditures": q.get("capital_expenditures"),
            "cash_and_equivalents": q.get("cash_and_equivalents"),
            "total_debt": q.get("total_debt"),
            "dividends_paid": q.get("dividends_paid"),
            "free_cash_flow": q.get("free_cash_flow"),
        }
        rows.append(row)
    return rows


def _make_annual_rows(ticker, years):
    rows = []
    for y in years:
        row = dict(y)
        row["ticker"] = ticker
        row["period_type"] = "annual"
        rows.append(row)
    return rows


# Complete quarterly data (4 quarters + 4 prior, sorted desc by period_date)
ACME_QUARTERLY = _make_quarterly_rows("ACME.US", [
    {"period_date": "2025-09-30", "revenue": 5e9, "net_income": 1e9,
     "ebitda": 1.5e9, "operating_cash_flow": 1.2e9, "capital_expenditures": -2e8,
     "cash_and_equivalents": 3e9, "total_debt": 2e9, "dividends_paid": -1e8},
    {"period_date": "2025-06-30", "revenue": 4.8e9, "net_income": 0.9e9,
     "ebitda": 1.4e9, "operating_cash_flow": 1.1e9, "capital_expenditures": -1.8e8,
     "cash_and_equivalents": 2.8e9, "total_debt": 2.1e9, "dividends_paid": -9e7},
    {"period_date": "2025-03-31", "revenue": 4.5e9, "net_income": 0.8e9,
     "ebitda": 1.3e9, "operating_cash_flow": 1.0e9, "capital_expenditures": -1.5e8,
     "cash_and_equivalents": 2.5e9, "total_debt": 2.2e9, "dividends_paid": -8e7},
    {"period_date": "2024-12-31", "revenue": 4.6e9, "net_income": 0.85e9,
     "ebitda": 1.35e9, "operating_cash_flow": 1.05e9, "capital_expenditures": -1.7e8,
     "cash_and_equivalents": 2.6e9, "total_debt": 2.15e9, "dividends_paid": -7e7},
])

ACME_ANNUAL = _make_annual_rows("ACME.US", [
    {"period_date": "2025-12-31", "revenue": 19e9},
    {"period_date": "2024-12-31", "revenue": 16e9},
    {"period_date": "2023-12-31", "revenue": 14e9},
    {"period_date": "2022-12-31", "revenue": 12e9},
])


# ── tests ────────────────────────────────────────────────────────────────────

class TestProofStructure:
    """Proof payload must include all 7 metrics plus metadata."""

    REQUIRED_KEYS = [
        "market_cap",
        "shares_outstanding",
        "net_margin_ttm",
        "fcf_yield",
        "net_debt_ebitda",
        "revenue_growth_3y",
        "dividend_yield_ttm",
        "generated_at",
    ]

    def test_all_keys_present(self):
        proof = compute_proof(
            shares_outstanding=1e10,
            current_price=150.0,
            price_date="2025-10-01",
            quarterly_rows=ACME_QUARTERLY,
            annual_rows=ACME_ANNUAL,
            forward_dividend_yield=0.005,
        )
        for key in self.REQUIRED_KEYS:
            assert key in proof, f"Missing key: {key}"

    def test_each_metric_has_value_and_na_reason(self):
        proof = compute_proof(
            shares_outstanding=1e10,
            current_price=150.0,
            price_date="2025-10-01",
            quarterly_rows=ACME_QUARTERLY,
            annual_rows=ACME_ANNUAL,
            forward_dividend_yield=0.005,
        )
        for metric_key in ["market_cap", "shares_outstanding", "net_margin_ttm",
                           "fcf_yield", "net_debt_ebitda", "revenue_growth_3y",
                           "dividend_yield_ttm"]:
            metric = proof[metric_key]
            assert "value" in metric, f"{metric_key} missing 'value'"
            assert "na_reason" in metric, f"{metric_key} missing 'na_reason'"


class TestMarketCapProof:
    """Market Cap = shares_outstanding * current_price."""

    def test_correct_value(self):
        proof = compute_proof(
            shares_outstanding=1e10,
            current_price=150.0,
            price_date="2025-10-01",
            quarterly_rows=ACME_QUARTERLY,
            annual_rows=ACME_ANNUAL,
            forward_dividend_yield=None,
        )
        assert proof["market_cap"]["value"] == 1e10 * 150.0
        assert proof["market_cap"]["na_reason"] is None

    def test_inputs_exposed(self):
        proof = compute_proof(
            shares_outstanding=1e10,
            current_price=150.0,
            price_date="2025-10-01",
            quarterly_rows=ACME_QUARTERLY,
            annual_rows=ACME_ANNUAL,
            forward_dividend_yield=None,
        )
        inputs = proof["market_cap"]["inputs"]
        assert inputs["shares_outstanding"] == 1e10
        assert inputs["current_price"] == 150.0
        assert inputs["price_date"] == "2025-10-01"

    def test_none_when_missing_shares(self):
        proof = compute_proof(
            shares_outstanding=None,
            current_price=150.0,
            price_date="2025-10-01",
            quarterly_rows=ACME_QUARTERLY,
            annual_rows=ACME_ANNUAL,
            forward_dividend_yield=None,
        )
        assert proof["market_cap"]["value"] is None
        assert proof["market_cap"]["na_reason"] == "missing_data"


class TestNetMarginTTMProof:
    """Net Margin TTM = (sum 4Q net_income / sum 4Q revenue) * 100."""

    def test_correct_value(self):
        proof = compute_proof(
            shares_outstanding=1e10,
            current_price=150.0,
            price_date="2025-10-01",
            quarterly_rows=ACME_QUARTERLY,
            annual_rows=ACME_ANNUAL,
            forward_dividend_yield=None,
        )
        expected_rev = 5e9 + 4.8e9 + 4.5e9 + 4.6e9   # 18.9B
        expected_ni = 1e9 + 0.9e9 + 0.8e9 + 0.85e9    # 3.55B
        expected_margin = (expected_ni / expected_rev) * 100

        result = proof["net_margin_ttm"]
        assert result["value"] is not None
        assert abs(result["value"] - expected_margin) < 0.01
        assert result["na_reason"] is None

    def test_quarter_dates_exposed(self):
        proof = compute_proof(
            shares_outstanding=1e10,
            current_price=150.0,
            price_date="2025-10-01",
            quarterly_rows=ACME_QUARTERLY,
            annual_rows=ACME_ANNUAL,
            forward_dividend_yield=None,
        )
        result = proof["net_margin_ttm"]
        assert result["quarter_dates_used"] == [
            "2025-09-30", "2025-06-30", "2025-03-31", "2024-12-31"
        ]
        assert result["revenue_ttm"] == 5e9 + 4.8e9 + 4.5e9 + 4.6e9
        assert result["net_income_ttm"] == 1e9 + 0.9e9 + 0.8e9 + 0.85e9

    def test_raw_per_quarter_values(self):
        proof = compute_proof(
            shares_outstanding=1e10,
            current_price=150.0,
            price_date="2025-10-01",
            quarterly_rows=ACME_QUARTERLY,
            annual_rows=ACME_ANNUAL,
            forward_dividend_yield=None,
        )
        result = proof["net_margin_ttm"]
        assert result["revenue_per_quarter"] == [5e9, 4.8e9, 4.5e9, 4.6e9]
        assert result["net_income_per_quarter"] == [1e9, 0.9e9, 0.8e9, 0.85e9]

    def test_none_when_insufficient_quarters(self):
        """Less than 4 quarters → None with reason."""
        proof = compute_proof(
            shares_outstanding=1e10,
            current_price=150.0,
            price_date="2025-10-01",
            quarterly_rows=ACME_QUARTERLY[:3],
            annual_rows=ACME_ANNUAL,
            forward_dividend_yield=None,
        )
        result = proof["net_margin_ttm"]
        assert result["value"] is None
        assert result["quarter_dates_used"] == []


class TestFCFYieldProof:
    """FCF Yield = (TTM FCF / Market Cap) * 100."""

    def test_correct_value(self):
        shares = 1e10
        price = 150.0
        proof = compute_proof(
            shares_outstanding=shares,
            current_price=price,
            price_date="2025-10-01",
            quarterly_rows=ACME_QUARTERLY,
            annual_rows=ACME_ANNUAL,
            forward_dividend_yield=None,
        )
        fcf_q = [
            1.2e9 - 2e8,
            1.1e9 - 1.8e8,
            1.0e9 - 1.5e8,
            1.05e9 - 1.7e8,
        ]
        expected_fcf = sum(fcf_q)
        expected_yield = (expected_fcf / (shares * price)) * 100

        result = proof["fcf_yield"]
        assert result["value"] is not None
        assert abs(result["value"] - expected_yield) < 0.01

    def test_per_quarter_values_exposed(self):
        proof = compute_proof(
            shares_outstanding=1e10,
            current_price=150.0,
            price_date="2025-10-01",
            quarterly_rows=ACME_QUARTERLY,
            annual_rows=ACME_ANNUAL,
            forward_dividend_yield=None,
        )
        result = proof["fcf_yield"]
        assert len(result["operating_cash_flow_per_quarter"]) == 4
        assert len(result["capital_expenditures_per_quarter"]) == 4
        assert len(result["fcf_per_quarter"]) == 4
        assert result["ttm_fcf"] is not None
        assert result["market_cap_used"] == 1e10 * 150.0


class TestNetDebtEbitdaProof:
    """Net Debt/EBITDA = (debt - cash) / TTM EBITDA."""

    def test_correct_value(self):
        proof = compute_proof(
            shares_outstanding=1e10,
            current_price=150.0,
            price_date="2025-10-01",
            quarterly_rows=ACME_QUARTERLY,
            annual_rows=ACME_ANNUAL,
            forward_dividend_yield=None,
        )
        expected_ebitda = 1.5e9 + 1.4e9 + 1.3e9 + 1.35e9
        expected_nd = (2e9 - 3e9) / expected_ebitda

        result = proof["net_debt_ebitda"]
        assert result["value"] is not None
        assert abs(result["value"] - expected_nd) < 0.01

    def test_balance_sheet_exposed(self):
        proof = compute_proof(
            shares_outstanding=1e10,
            current_price=150.0,
            price_date="2025-10-01",
            quarterly_rows=ACME_QUARTERLY,
            annual_rows=ACME_ANNUAL,
            forward_dividend_yield=None,
        )
        result = proof["net_debt_ebitda"]
        assert result["total_debt"] == 2e9
        assert result["cash_and_equivalents"] == 3e9
        assert result["net_debt"] == 2e9 - 3e9
        assert result["latest_quarter_date"] == "2025-09-30"


class TestRevenueGrowth3YProof:
    """Revenue Growth 3Y CAGR = ((end/start)^(1/3) - 1) * 100."""

    def test_correct_value(self):
        proof = compute_proof(
            shares_outstanding=1e10,
            current_price=150.0,
            price_date="2025-10-01",
            quarterly_rows=ACME_QUARTERLY,
            annual_rows=ACME_ANNUAL,
            forward_dividend_yield=None,
        )
        expected = ((19e9 / 12e9) ** (1 / 3) - 1) * 100
        result = proof["revenue_growth_3y"]
        assert result["value"] is not None
        assert abs(result["value"] - round(expected, 1)) < 0.01

    def test_annual_rows_exposed(self):
        proof = compute_proof(
            shares_outstanding=1e10,
            current_price=150.0,
            price_date="2025-10-01",
            quarterly_rows=ACME_QUARTERLY,
            annual_rows=ACME_ANNUAL,
            forward_dividend_yield=None,
        )
        result = proof["revenue_growth_3y"]
        assert result["end_revenue"] == 19e9
        assert result["start_revenue"] == 12e9
        assert len(result["annual_rows_used"]) == 4

    def test_none_when_insufficient_history(self):
        proof = compute_proof(
            shares_outstanding=1e10,
            current_price=150.0,
            price_date="2025-10-01",
            quarterly_rows=ACME_QUARTERLY,
            annual_rows=ACME_ANNUAL[:3],
            forward_dividend_yield=None,
        )
        result = proof["revenue_growth_3y"]
        assert result["value"] is None
        assert result["na_reason"] == "insufficient_annual_history"


class TestDividendYieldProof:
    """Dividend yield currently uses forward_dividend_yield * 100."""

    def test_correct_value(self):
        proof = compute_proof(
            shares_outstanding=1e10,
            current_price=150.0,
            price_date="2025-10-01",
            quarterly_rows=ACME_QUARTERLY,
            annual_rows=ACME_ANNUAL,
            forward_dividend_yield=0.025,
        )
        result = proof["dividend_yield_ttm"]
        assert result["value"] is not None
        assert abs(result["value"] - 2.5) < 0.001

    def test_source_transparency(self):
        """Proof must expose the current source (forward_dividend_yield)."""
        proof = compute_proof(
            shares_outstanding=1e10,
            current_price=150.0,
            price_date="2025-10-01",
            quarterly_rows=ACME_QUARTERLY,
            annual_rows=ACME_ANNUAL,
            forward_dividend_yield=0.025,
        )
        result = proof["dividend_yield_ttm"]
        assert result["current_source"] == "company_fundamentals_cache.forward_dividend_yield"
        assert result["forward_dividend_yield_raw"] == 0.025
        assert "note" in result

    def test_quarterly_dividends_paid_exposed(self):
        """Even though not currently used for yield, quarterly dividends_paid
        must be exposed for audit transparency."""
        proof = compute_proof(
            shares_outstanding=1e10,
            current_price=150.0,
            price_date="2025-10-01",
            quarterly_rows=ACME_QUARTERLY,
            annual_rows=ACME_ANNUAL,
            forward_dividend_yield=0.025,
        )
        result = proof["dividend_yield_ttm"]
        assert result["quarterly_dividends_paid"] == [-1e8, -9e7, -8e7, -7e7]

    def test_none_when_not_reported(self):
        proof = compute_proof(
            shares_outstanding=1e10,
            current_price=150.0,
            price_date="2025-10-01",
            quarterly_rows=ACME_QUARTERLY,
            annual_rows=ACME_ANNUAL,
            forward_dividend_yield=None,
        )
        result = proof["dividend_yield_ttm"]
        assert result["value"] is None
        assert result["na_reason"] == "no_dividend"

    def test_zero_when_explicit_zero(self):
        proof = compute_proof(
            shares_outstanding=1e10,
            current_price=150.0,
            price_date="2025-10-01",
            quarterly_rows=ACME_QUARTERLY,
            annual_rows=ACME_ANNUAL,
            forward_dividend_yield=0,
        )
        result = proof["dividend_yield_ttm"]
        assert result["value"] == 0.0


class TestProofMatchesServerLogic:
    """Cross-check: proof values must match the pure compute_key_metrics()
    function from test_key_metrics_canonical (which mirrors server.py)."""

    def test_all_seven_match(self):
        """Verify every metric value from proof matches the canonical test helper."""
        # Import canonical helper
        from tests.test_key_metrics_canonical import compute_key_metrics

        # Same inputs
        shares = 1e10
        price = 150.0
        fwd_div = 0.005

        canonical = compute_key_metrics(
            shares_outstanding=shares,
            current_price=price,
            quarterly_rows=ACME_QUARTERLY,
            annual_rows=ACME_ANNUAL,
            forward_dividend_yield=fwd_div,
        )

        proof = compute_proof(
            shares_outstanding=shares,
            current_price=price,
            price_date="2025-10-01",
            quarterly_rows=ACME_QUARTERLY,
            annual_rows=ACME_ANNUAL,
            forward_dividend_yield=fwd_div,
        )

        # Market Cap
        assert proof["market_cap"]["value"] == canonical["market_cap"]

        # Net Margin TTM
        if canonical["net_margin_ttm"] is not None:
            assert abs(proof["net_margin_ttm"]["value"] - canonical["net_margin_ttm"]) < 0.01
        else:
            assert proof["net_margin_ttm"]["value"] is None

        # FCF Yield
        if canonical["fcf_yield"] is not None:
            assert abs(proof["fcf_yield"]["value"] - canonical["fcf_yield"]) < 0.01
        else:
            assert proof["fcf_yield"]["value"] is None

        # Net Debt / EBITDA
        if canonical["net_debt_ebitda"] is not None:
            assert abs(proof["net_debt_ebitda"]["value"] - canonical["net_debt_ebitda"]) < 0.01
        else:
            assert proof["net_debt_ebitda"]["value"] is None

        # Revenue Growth 3Y
        if canonical["revenue_growth_3y"] is not None:
            assert abs(proof["revenue_growth_3y"]["value"] - canonical["revenue_growth_3y"]) < 0.01
        else:
            assert proof["revenue_growth_3y"]["value"] is None

        # Dividend Yield
        if canonical["dividend_yield_ttm"] is not None:
            assert abs(proof["dividend_yield_ttm"]["value"] - canonical["dividend_yield_ttm"]) < 0.01
        else:
            assert proof["dividend_yield_ttm"]["value"] is None


class TestMissingQuarterlyData:
    """Ticker with fewer than 4 quarters → every TTM metric is None."""

    def test_all_ttm_none_with_empty_rows(self):
        proof = compute_proof(
            shares_outstanding=1e10,
            current_price=150.0,
            price_date="2025-10-01",
            quarterly_rows=[],
            annual_rows=[],
            forward_dividend_yield=None,
        )
        assert proof["net_margin_ttm"]["value"] is None
        assert proof["fcf_yield"]["value"] is None
        assert proof["net_debt_ebitda"]["value"] is None
        assert proof["revenue_growth_3y"]["value"] is None
        assert proof["dividend_yield_ttm"]["value"] is None

    def test_market_cap_still_computed(self):
        """Market cap does not depend on quarterly rows."""
        proof = compute_proof(
            shares_outstanding=1e10,
            current_price=150.0,
            price_date="2025-10-01",
            quarterly_rows=[],
            annual_rows=[],
            forward_dividend_yield=None,
        )
        assert proof["market_cap"]["value"] == 1e10 * 150.0
