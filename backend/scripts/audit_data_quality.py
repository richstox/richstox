#!/usr/bin/env python3
"""
RICHSTOX Data Quality Audit Script
===================================
READ-ONLY audit of all is_visible=True tickers.

Categories (per guardrail):
- MISSING: Field absent or not enough quarters
- STALE: Data present but outdated (>7 days)
- INVALID: Data present but unreasonable values

Checks:
1. Fundamentals completeness (Income Statement, Balance Sheet, Cash Flow quarterly)
2. Price freshness (last update within 7 days)
3. Shares outstanding (non-zero and reasonable)
4. TTM validity (at least 4 consecutive quarters)
5. Currency consistency (no mixed currencies)

Output:
- Data quality score (% of tickers with 100% valid fields)
- Red flags list (tickers failing critical checks)
- Recommendations (tickers to flip is_visible: False)
"""

import os
import sys
import json
from datetime import datetime, timezone, timedelta
from pymongo import MongoClient
from collections import defaultdict

# Config
MONGO_URL = os.environ.get("MONGO_URL", "mongodb://localhost:27017")
DB_NAME = os.environ.get("DB_NAME", "richstox_prod")
PRICE_FRESHNESS_DAYS = 7
MIN_QUARTERS_FOR_TTM = 4
REASONABLE_SHARES_MIN = 1_000_000  # 1M shares minimum
REASONABLE_SHARES_MAX = 100_000_000_000  # 100B shares maximum

def connect_db():
    client = MongoClient(MONGO_URL)
    return client[DB_NAME]

def audit_fundamentals(ticker_doc):
    """Check if all required fundamental data is present."""
    issues = []
    fundamentals = ticker_doc.get("fundamentals", {})
    
    if not fundamentals:
        return [("missing", "fundamentals_entirely")]
    
    financials = fundamentals.get("Financials", {})
    
    if not financials:
        return [("missing", "financials_section")]
    
    # Check Income Statement quarterly
    income_q = financials.get("Income_Statement", {}).get("quarterly", {})
    if not income_q:
        issues.append(("missing", "income_statement"))
    elif len(income_q) < MIN_QUARTERS_FOR_TTM:
        issues.append(("missing", f"income_quarters_only_{len(income_q)}"))
    
    # Check Balance Sheet quarterly
    balance_q = financials.get("Balance_Sheet", {}).get("quarterly", {})
    if not balance_q:
        issues.append(("missing", "balance_sheet"))
    
    # Check Cash Flow quarterly
    cashflow_q = financials.get("Cash_Flow", {}).get("quarterly", {})
    if not cashflow_q:
        issues.append(("missing", "cashflow"))
    elif len(cashflow_q) < MIN_QUARTERS_FOR_TTM:
        issues.append(("missing", f"cashflow_quarters_only_{len(cashflow_q)}"))
    
    return issues

def audit_price_freshness(latest_prices, ticker):
    """Check if price data is fresh (within 7 days)."""
    cutoff = (datetime.now(timezone.utc) - timedelta(days=PRICE_FRESHNESS_DAYS)).strftime("%Y-%m-%d")
    
    latest_date = latest_prices.get(ticker)
    
    if not latest_date:
        return [("missing", "price_data")]
    
    if latest_date < cutoff:
        return [("stale", f"price_last_{latest_date}")]
    
    return []

def audit_shares_outstanding(ticker_doc):
    """Check shares outstanding is present and reasonable."""
    fundamentals = ticker_doc.get("fundamentals", {})
    shares_data = fundamentals.get("SharesStats", {})
    
    shares = shares_data.get("SharesOutstanding")
    
    if not shares:
        # Try alternative location
        highlights = fundamentals.get("Highlights", {})
        shares = highlights.get("SharesOutstanding")
    
    if not shares:
        return [("missing", "shares_outstanding")]
    
    try:
        shares_float = float(shares)
        if shares_float <= 0:
            return [("invalid", "shares_non_positive")]
        if shares_float < REASONABLE_SHARES_MIN:
            return [("invalid", f"shares_too_low_{shares_float:.0f}")]
        if shares_float > REASONABLE_SHARES_MAX:
            return [("invalid", f"shares_too_high_{shares_float:.0f}")]
    except (ValueError, TypeError):
        return [("invalid", f"shares_not_numeric")]
    
    return []

def audit_ttm_validity(ticker_doc):
    """Check if at least 4 consecutive quarters are present for TTM."""
    fundamentals = ticker_doc.get("fundamentals", {})
    financials = fundamentals.get("Financials", {})
    income_q = financials.get("Income_Statement", {}).get("quarterly", {})
    
    if not income_q:
        return [("missing", "quarterly_data_for_ttm")]
    
    # Get sorted quarters
    quarters = sorted(income_q.keys(), reverse=True)
    
    if len(quarters) < MIN_QUARTERS_FOR_TTM:
        return [("missing", f"ttm_insufficient_quarters_{len(quarters)}")]
    
    # Check if top 4 quarters have revenue data
    missing_revenue = 0
    for q in quarters[:4]:
        if not income_q[q].get("totalRevenue"):
            missing_revenue += 1
    
    if missing_revenue > 0:
        return [("invalid", f"ttm_missing_revenue_in_{missing_revenue}_quarters")]
    
    return []

def audit_currency_consistency(ticker_doc):
    """Check that currency is consistent (not mixed)."""
    fundamentals = ticker_doc.get("fundamentals", {})
    general = fundamentals.get("General", {})
    
    currency = general.get("CurrencyCode")
    
    if not currency:
        return [("missing", "currency_code")]
    
    # For US stocks (.US suffix), should be USD
    ticker = ticker_doc.get("ticker", "")
    if ticker.endswith(".US") and currency != "USD":
        return [("invalid", f"non_usd_currency_{currency}")]
    
    return []

def run_audit():
    """Run full data quality audit."""
    db = connect_db()
    
    print("=" * 70)
    print("RICHSTOX Data Quality Audit")
    print("=" * 70)
    print(f"Database: {DB_NAME}")
    print(f"Started: {datetime.now(timezone.utc).isoformat()}")
    print()
    
    # Optimized: Get all latest prices in one aggregation query
    print("Loading latest prices...")
    latest_prices = {}
    price_cursor = db.stock_prices.aggregate([
        {"$sort": {"ticker": 1, "date": -1}},
        {"$group": {"_id": "$ticker", "date": {"$first": "$date"}}},
    ])
    for doc in price_cursor:
        latest_prices[doc["_id"]] = doc["date"]
    print(f"  Loaded prices for {len(latest_prices)} tickers")
    
    # Get all visible tickers
    print("Loading visible tickers...")
    visible_tickers = list(db.tracked_tickers.find(
        {"is_visible": True},
        {"ticker": 1, "name": 1, "fundamentals": 1, "sector": 1, "industry": 1}
    ))
    
    total = len(visible_tickers)
    print(f"  Total visible tickers: {total}")
    print()
    
    # Audit results - separated by category per guardrail
    results = {
        "audit_timestamp": datetime.now(timezone.utc).isoformat(),
        "database": DB_NAME,
        "total_audited": total,
        "passed": 0,
        "failed": 0,
        "by_category": {
            "missing": defaultdict(int),
            "stale": defaultdict(int),
            "invalid": defaultdict(int),
        },
        "red_flags": [],  # Critical failures
        "warnings": [],   # Non-critical issues
        "recommendations": [],  # Tickers to flip is_visible
    }
    
    print("Running audit...")
    for i, ticker_doc in enumerate(visible_tickers):
        ticker = ticker_doc["ticker"]
        name = ticker_doc.get("name", "Unknown")
        
        if (i + 1) % 1000 == 0:
            print(f"  Progress: {i + 1}/{total} ({((i+1)/total)*100:.1f}%)")
        
        all_issues = []
        critical_issues = []
        
        # 1. Fundamentals completeness
        fund_issues = audit_fundamentals(ticker_doc)
        all_issues.extend(fund_issues)
        if any(cat == "missing" and "fundamentals_entirely" in issue for cat, issue in fund_issues):
            critical_issues.append("missing_fundamentals")
        
        # 2. Price freshness
        price_issues = audit_price_freshness(latest_prices, ticker)
        all_issues.extend(price_issues)
        if any(cat == "missing" and "price_data" in issue for cat, issue in price_issues):
            critical_issues.append("missing_price")
        elif any(cat == "stale" for cat, issue in price_issues):
            critical_issues.append("stale_price")
        
        # 3. Shares outstanding
        shares_issues = audit_shares_outstanding(ticker_doc)
        all_issues.extend(shares_issues)
        if any(cat == "missing" and "shares_outstanding" in issue for cat, issue in shares_issues):
            critical_issues.append("missing_shares")
        elif any(cat == "invalid" for cat, issue in shares_issues):
            critical_issues.append("invalid_shares")
        
        # 4. TTM validity
        ttm_issues = audit_ttm_validity(ticker_doc)
        all_issues.extend(ttm_issues)
        
        # 5. Currency consistency
        currency_issues = audit_currency_consistency(ticker_doc)
        all_issues.extend(currency_issues)
        
        # Aggregate results by category
        for category, issue in all_issues:
            issue_key = issue.split("_")[0] if "_" in issue else issue
            results["by_category"][category][issue_key] += 1
        
        if not all_issues:
            results["passed"] += 1
        else:
            results["failed"] += 1
            
            issue_summary = [(cat, iss) for cat, iss in all_issues]
            
            if critical_issues:
                results["red_flags"].append({
                    "ticker": ticker,
                    "name": name,
                    "critical_issues": critical_issues,
                    "all_issues": issue_summary
                })
                # Only recommend visibility flip for truly broken tickers
                if "missing_fundamentals" in critical_issues or "missing_price" in critical_issues:
                    results["recommendations"].append(ticker)
            else:
                results["warnings"].append({
                    "ticker": ticker,
                    "name": name,
                    "issues": issue_summary
                })
    
    print(f"  Done. Processed {total} tickers.")
    
    # Calculate score
    results["quality_score"] = (results["passed"] / total) * 100 if total > 0 else 0
    
    # Convert defaultdicts to regular dicts for JSON serialization
    results["by_category"] = {
        cat: dict(issues) for cat, issues in results["by_category"].items()
    }
    
    return results

def print_report(results):
    """Print formatted audit report."""
    print()
    print("=" * 70)
    print("AUDIT RESULTS")
    print("=" * 70)
    print()
    
    print(f"DATA QUALITY SCORE: {results['quality_score']:.1f}%")
    print(f"  Passed (100% valid): {results['passed']} tickers")
    print(f"  Failed (has issues): {results['failed']} tickers")
    print()
    
    print("ISSUES BY CATEGORY:")
    for category in ["missing", "stale", "invalid"]:
        issues = results["by_category"].get(category, {})
        if issues:
            total_in_cat = sum(issues.values())
            print(f"\n  {category.upper()} ({total_in_cat} total):")
            for issue_type, count in sorted(issues.items(), key=lambda x: -x[1]):
                print(f"    {issue_type}: {count}")
    print()
    
    print(f"RED FLAGS ({len(results['red_flags'])} tickers with critical issues):")
    for item in results["red_flags"][:10]:  # Show top 10
        crits = ", ".join(item["critical_issues"])
        print(f"  {item['ticker']}: {crits}")
    if len(results["red_flags"]) > 10:
        print(f"  ... and {len(results['red_flags']) - 10} more")
    print()
    
    print(f"RECOMMENDATIONS: {len(results['recommendations'])} tickers to flip is_visible: False")
    if results["recommendations"]:
        print(f"  First 20: {results['recommendations'][:20]}")
    print()
    
    # Save full report to JSON
    report_path = "/app/backend/scripts/audit_report.json"
    with open(report_path, "w") as f:
        json.dump(results, f, indent=2, default=str)
    print(f"Full report saved to: {report_path}")

if __name__ == "__main__":
    results = run_audit()
    print_report(results)
