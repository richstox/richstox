from fundamentals_service import parse_company_fundamentals
from provider_debug_service import build_provider_debug_snapshot


def test_build_provider_debug_snapshot_keeps_debug_only_sections():
    raw_payload = {
        "General": {
            "Name": "Acme Corp",
            "Code": "ACME",
            "Officers": [{"name": "CEO"}],
            "CurrencyName": "US Dollar",
        },
        "Highlights": {"PERatio": 22.1, "MarketCapitalization": 123456},
        "Valuation": {"PriceBookMRQ": 3.1},
        "Technicals": {"Beta": 1.2},
        "Financials": {"Income_Statement": {"quarterly": {"2025-12-31": {"netIncome": 1}}}},
    }

    snapshot = build_provider_debug_snapshot(
        ticker="acme",
        raw_payload=raw_payload,
        source_job="unit_test",
    )

    assert snapshot["debug_only"] is True
    assert snapshot["runtime_allowed"] is False
    assert snapshot["ticker"] == "ACME.US"
    assert "Highlights" in snapshot["forbidden_sections"]
    assert "Valuation" in snapshot["forbidden_sections"]
    assert "Technicals" in snapshot["forbidden_sections"]
    assert "GeneralForbiddenFields" in snapshot["forbidden_sections"]
    assert "Financials" not in snapshot["forbidden_sections"]


def test_parse_company_fundamentals_never_stores_provider_metrics():
    raw_payload = {
        "General": {
            "Code": "ACME",
            "Name": "Acme Corp",
            "Exchange": "NASDAQ",
            "Sector": "Technology",
            "Industry": "Software",
        },
        "Highlights": {
            "MarketCapitalization": 999999999,
            "PERatio": 50,
            "ProfitMargin": 0.31,
            "EarningsShare": 99,  # should not be used as fallback
        },
        "Valuation": {"PriceBookMRQ": 8.8, "EnterpriseValue": 123456789},
        "Technicals": {"Beta": 1.9, "52WeekHigh": 200},
        "Earnings": {
            "History": {
                "q1": {"reportDate": "2025-12-31", "epsActual": 1.0},
                "q2": {"reportDate": "2025-09-30", "epsActual": 2.0},
                "q3": {"reportDate": "2025-06-30", "epsActual": 3.0},
                "q4": {"reportDate": "2025-03-31", "epsActual": 4.0},
            }
        },
    }

    doc = parse_company_fundamentals("ACME", raw_payload)

    assert doc["market_cap"] is None
    assert doc["enterprise_value"] is None
    assert doc["pe_ratio"] is None
    assert doc["ps_ratio"] is None
    assert doc["pb_ratio"] is None
    assert doc["ev_ebitda"] is None
    assert doc["ev_revenue"] is None
    assert doc["dividend_yield"] is None
    assert doc["beta"] is None
    assert doc["book_value"] is None
    assert doc["ebitda"] is None
    assert doc["eps_ttm"] == 10.0
