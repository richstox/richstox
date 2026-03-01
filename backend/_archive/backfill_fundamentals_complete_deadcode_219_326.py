# ==============================================================================
# ARCHIVED DEAD CODE from /app/backend/backfill_fundamentals_complete.py
# Lines 219-326 (unreachable code after return statement on line 218)
# Archived on: 2024-12-XX as part of P0 Phase 3 implementation
# Reason: This code stored provider-computed metrics which violates RAW-INPUTS-ONLY policy
# DO NOT DELETE until P0 Phase 3 is fully closed and confirmed
# ==============================================================================
# Context: This code block was part of the extract_all_fundamentals() function
# but was placed AFTER a return statement, making it unreachable.
# The OLD logic stored provider-computed metrics (P/E, Market Cap, EV, TTM values)
# which we now strip on write per BINDING rules.
# ==============================================================================

"""
ORIGINAL DEAD CODE (lines 219-326):
These fields were previously extracted but are now STRIPPED because they are
provider-computed metrics. We compute everything ourselves from raw inputs.

STRIPPED FIELDS (provider-computed, price-dependent):
- eps, eps_estimate_*, book_value, dividend_share, dividend_yield
- profit_margin, operating_margin_ttm, return_on_assets_ttm, return_on_equity_ttm
- revenue_ttm, revenue_per_share_ttm, quarterly_revenue_growth_yoy
- gross_profit_ttm, diluted_eps_ttm, quarterly_earnings_growth_yoy
- ebitda, wall_street_target_price
- beta, 52_week_high, 52_week_low, 50_day_ma, 200_day_ma
- short_ratio, short_percent, shares_short, shares_short_prior_month
- trailing_pe, forward_pe, price_sales_ttm, price_book_mrq
- enterprise_value, enterprise_value_revenue, enterprise_value_ebitda
- percent_insiders, percent_institutions, short_percent_outstanding
- forward_annual_dividend_yield, payout_ratio
- analyst_rating (computed from raw counts)
"""

ARCHIVED_DEAD_CODE = '''
        "eps": highlights.get("EarningsShare"),
        "eps_estimate_current_year": highlights.get("EPSEstimateCurrentYear"),
        "eps_estimate_next_year": highlights.get("EPSEstimateNextYear"),
        "eps_estimate_next_quarter": highlights.get("EPSEstimateNextQuarter"),
        "eps_estimate_current_quarter": highlights.get("EPSEstimateCurrentQuarter"),
        "book_value": highlights.get("BookValue"),
        "dividend_share": highlights.get("DividendShare"),
        "dividend_yield": highlights.get("DividendYield"),
        "profit_margin": highlights.get("ProfitMargin"),
        "operating_margin_ttm": highlights.get("OperatingMarginTTM"),
        "return_on_assets_ttm": highlights.get("ReturnOnAssetsTTM"),
        "return_on_equity_ttm": highlights.get("ReturnOnEquityTTM"),
        "revenue_ttm": highlights.get("RevenueTTM"),
        "revenue_per_share_ttm": highlights.get("RevenuePerShareTTM"),
        "quarterly_revenue_growth_yoy": highlights.get("QuarterlyRevenueGrowthYOY"),
        "gross_profit_ttm": highlights.get("GrossProfitTTM"),
        "diluted_eps_ttm": highlights.get("DilutedEpsTTM"),
        "quarterly_earnings_growth_yoy": highlights.get("QuarterlyEarningsGrowthYOY"),
        "ebitda": highlights.get("EBITDA"),
        "wall_street_target_price": highlights.get("WallStreetTargetPrice"),
        "most_recent_quarter": highlights.get("MostRecentQuarter"),
        
        # === TECHNICALS ===
        "beta": technicals.get("Beta"),
        "52_week_high": technicals.get("52WeekHigh"),
        "52_week_low": technicals.get("52WeekLow"),
        "50_day_ma": technicals.get("50DayMA"),
        "200_day_ma": technicals.get("200DayMA"),
        "short_ratio": technicals.get("ShortRatio"),
        "short_percent": technicals.get("ShortPercent"),
        "shares_short": technicals.get("SharesShort"),
        "shares_short_prior_month": technicals.get("SharesShortPriorMonth"),
        
        # === VALUATION ===
        "trailing_pe": valuation.get("TrailingPE"),
        "forward_pe": valuation.get("ForwardPE"),
        "price_sales_ttm": valuation.get("PriceSalesTTM"),
        "price_book_mrq": valuation.get("PriceBookMRQ"),
        "enterprise_value": valuation.get("EnterpriseValue"),
        "enterprise_value_revenue": valuation.get("EnterpriseValueRevenue"),
        "enterprise_value_ebitda": valuation.get("EnterpriseValueEbitda"),
        
        # === SHARES STATISTICS ===
        "shares_outstanding": shares_stats.get("SharesOutstanding"),
        "shares_float": shares_stats.get("SharesFloat"),
        "percent_insiders": shares_stats.get("PercentInsiders"),
        "percent_institutions": shares_stats.get("PercentInstitutions"),
        "shares_short_stats": shares_stats.get("SharesShort"),
        "short_ratio_stats": shares_stats.get("ShortRatio"),
        "short_percent_outstanding": shares_stats.get("ShortPercentOfFloat"),
        
        # === SPLITS & DIVIDENDS ===
        "forward_annual_dividend_rate": splits_dividends.get("ForwardAnnualDividendRate"),
        "forward_annual_dividend_yield": splits_dividends.get("ForwardAnnualDividendYield"),
        "payout_ratio": splits_dividends.get("PayoutRatio"),
        "dividend_date": splits_dividends.get("DividendDate"),
        "ex_dividend_date": splits_dividends.get("ExDividendDate"),
        "last_split_factor": splits_dividends.get("LastSplitFactor"),
        "last_split_date": splits_dividends.get("LastSplitDate"),
        
        # === ANALYST RATINGS ===
        "analyst_target_price": analyst_ratings.get("TargetPrice"),
        "analyst_strong_buy": analyst_ratings.get("StrongBuy"),
        "analyst_buy": analyst_ratings.get("Buy"),
        "analyst_hold": analyst_ratings.get("Hold"),
        "analyst_sell": analyst_ratings.get("Sell"),
        "analyst_strong_sell": analyst_ratings.get("StrongSell"),
        "analyst_rating": analyst_ratings.get("Rating"),
        
        # === ESG SCORES (if available) ===
        "esg_total_score": esg_scores.get("TotalEsg") if esg_scores else None,
        "esg_environment_score": esg_scores.get("EnvironmentScore") if esg_scores else None,
        "esg_social_score": esg_scores.get("SocialScore") if esg_scores else None,
        "esg_governance_score": esg_scores.get("GovernanceScore") if esg_scores else None,
        
        # === OUTSTANDING SHARES HISTORY ===
        "outstanding_shares_annual": outstandingShares.get("annual", []) if outstandingShares else [],
        "outstanding_shares_quarterly": outstandingShares.get("quarterly", []) if outstandingShares else [],
        
        # === HOLDERS (store counts + top holders) ===
        "institutions_count": len(holders.get("Institutions", {}).keys()) if holders.get("Institutions") else 0,
        "funds_count": len(holders.get("Funds", {}).keys()) if holders.get("Funds") else 0,
        "top_institutional_holders": list(holders.get("Institutions", {}).values())[:10] if holders.get("Institutions") else [],
        "top_fund_holders": list(holders.get("Funds", {}).values())[:10] if holders.get("Funds") else [],
        
        # === INSIDER TRANSACTIONS (last 10) ===
        "insider_transactions": list(insider_transactions.values())[:10] if isinstance(insider_transactions, dict) else [],
        
        # === EARNINGS (History + Trend) ===
        "earnings_history": earnings.get("History", {}) if earnings else {},
        "earnings_trend": earnings.get("Trend", {}) if earnings else {},
        "earnings_annual": earnings.get("Annual", {}) if earnings else {},
        
        # === FINANCIALS (Store full data for TTM calculations) ===
        "financials_balance_sheet_quarterly": financials.get("Balance_Sheet", {}).get("quarterly", {}) if financials else {},
        "financials_balance_sheet_yearly": financials.get("Balance_Sheet", {}).get("yearly", {}) if financials else {},
        "financials_cash_flow_quarterly": financials.get("Cash_Flow", {}).get("quarterly", {}) if financials else {},
        "financials_cash_flow_yearly": financials.get("Cash_Flow", {}).get("yearly", {}) if financials else {},
        "financials_income_statement_quarterly": financials.get("Income_Statement", {}).get("quarterly", {}) if financials else {},
        "financials_income_statement_yearly": financials.get("Income_Statement", {}).get("yearly", {}) if financials else {},
        
        # === META ===
        "last_updated_at": datetime.now(timezone.utc),
        "data_source": "EODHD",
        "backfill_version": "complete_v1",
    }
    
    return result
'''
