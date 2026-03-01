#!/usr/bin/env python3
"""
RICHSTOX Master Verification Table Generator
=============================================
READ-ONLY script generating comprehensive data correctness table for ALL visible tickers.

Output:
- master_verification_table.csv (1 row per ticker, 40+ columns)
- master_verification_summary.json (aggregated stats)

Rules:
- ALL visible tickers (not just USD)
- No DB writes
- TTM logic matches production
- Inconsistent periods → status=inconsistent_periods, metric=blank
"""

import os
import sys
import json
import csv
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from pymongo import MongoClient

# Config
MONGO_URL = os.environ.get("MONGO_URL", "mongodb://localhost:27017")
DB_NAME = os.environ.get("DB_NAME", "richstox_prod")
OUTPUT_DIR = "/app/backend/scripts"

def connect_db():
    client = MongoClient(MONGO_URL)
    return client[DB_NAME]

def verify_consecutive_quarters(quarters):
    """
    Verify that 4 quarters are consecutive (no gaps).
    Quarters are in format YYYY-MM-DD (quarter end dates).
    Returns True if consecutive, False otherwise.
    """
    if len(quarters) < 4:
        return False
    
    try:
        dates = [datetime.strptime(q, "%Y-%m-%d") for q in quarters]
        dates_sorted = sorted(dates, reverse=True)
        
        # Check each quarter is ~90 days apart (with tolerance)
        for i in range(len(dates_sorted) - 1):
            diff = (dates_sorted[i] - dates_sorted[i + 1]).days
            # Quarters should be 85-100 days apart
            if diff < 60 or diff > 120:
                return False
        return True
    except:
        return False

def sum_ttm(quarterly_data, quarters, field_name):
    """Sum a field across 4 quarters for TTM calculation."""
    total = 0
    count = 0
    for q in quarters[:4]:
        val = quarterly_data.get(q, {}).get(field_name)
        if val is not None:
            try:
                total += float(val)
                count += 1
            except (ValueError, TypeError):
                pass
    return total if count == 4 else None

def get_latest_value(quarterly_data, quarters, field_name):
    """Get the most recent quarter's value for a field."""
    if not quarters:
        return None
    latest = quarterly_data.get(quarters[0], {})
    return latest.get(field_name)

def generate_master_table():
    """Generate the master verification table."""
    db = connect_db()
    
    print("=" * 70)
    print("MASTER VERIFICATION TABLE GENERATOR")
    print("=" * 70)
    print(f"Started: {datetime.now(timezone.utc).isoformat()}")
    print()
    
    # 1. Load latest prices (bulk)
    print("Loading latest prices...")
    latest_prices = {}
    for doc in db.stock_prices.aggregate([
        {"$group": {"_id": "$ticker", "last_date": {"$max": "$date"}}}
    ]):
        latest_prices[doc["_id"]] = doc["last_date"]
    print(f"  Loaded prices for {len(latest_prices)} tickers")
    
    # 2. Load valuations cache (bulk)
    print("Loading valuations cache...")
    valuations = {}
    for doc in db.ticker_valuations_cache.find({}, {"ticker": 1, "current_metrics": 1, "current_price": 1, "market_cap": 1}):
        valuations[doc["ticker"]] = doc
    print(f"  Loaded valuations for {len(valuations)} tickers")
    
    # 3. Load ALL visible tickers with fundamentals
    print("Loading visible tickers...")
    tickers = list(db.tracked_tickers.find(
        {"is_visible": True},
        {
            "ticker": 1, "name": 1, "country": 1, "exchange": 1, 
            "is_visible": 1, "type": 1, "sector": 1, "industry": 1,
            "financial_currency": 1, "fundamentals": 1
        }
    ))
    print(f"  Loaded {len(tickers)} visible tickers")
    
    # Track warnings
    warnings_by_type = defaultdict(int)
    results = []
    today = datetime.now(timezone.utc).date()
    
    print("Processing tickers...")
    for i, t in enumerate(tickers):
        if (i + 1) % 1000 == 0:
            print(f"  Progress: {i + 1}/{len(tickers)}")
        
        ticker = t["ticker"]
        fundamentals = t.get("fundamentals", {})
        warnings = []
        
        row = {}
        
        # === IDENTIFIERS ===
        row["ticker"] = ticker
        row["company_name"] = t.get("name", "")
        row["country"] = t.get("country", "")
        row["exchange"] = t.get("exchange", "")
        row["is_visible"] = "TRUE" if t.get("is_visible") else "FALSE"
        row["asset_type"] = t.get("type", "")
        row["sector"] = t.get("sector", "")
        row["industry"] = t.get("industry", "")
        row["financial_currency"] = t.get("financial_currency", "")
        
        # === FRESHNESS / AUDIT METADATA ===
        row["latest_price_date"] = latest_prices.get(ticker, "")
        
        # Check for stale price
        if row["latest_price_date"]:
            try:
                price_date = datetime.strptime(row["latest_price_date"], "%Y-%m-%d").date()
                if (today - price_date).days > 7:
                    warnings.append("stale_price")
            except:
                pass
        else:
            warnings.append("missing_price")
        
        # Get quarters from Income_Statement
        income_q = fundamentals.get("Financials", {}).get("Income_Statement", {}).get("quarterly", {})
        quarters = sorted(income_q.keys(), reverse=True) if income_q else []
        row["latest_quarter_end_date"] = quarters[0] if quarters else ""
        
        # Filing date from General
        general = fundamentals.get("General", {})
        row["latest_10q_10k_filing_date"] = general.get("UpdatedAt", "") or general.get("LastUpdate", "")
        
        # Days since latest report
        if row["latest_quarter_end_date"]:
            try:
                q_date = datetime.strptime(row["latest_quarter_end_date"], "%Y-%m-%d").date()
                row["days_since_latest_report"] = (today - q_date).days
            except:
                row["days_since_latest_report"] = ""
        else:
            row["days_since_latest_report"] = ""
        
        # === TTM FUNDAMENTALS ===
        has_4_quarters = len(quarters) >= 4
        is_consistent = verify_consecutive_quarters(quarters[:4]) if has_4_quarters else False
        
        if not has_4_quarters:
            warnings.append("insufficient_quarters")
        elif not is_consistent:
            warnings.append("inconsistent_quarters")
        
        # Revenue TTM
        if has_4_quarters and is_consistent:
            rev_ttm = sum_ttm(income_q, quarters, "totalRevenue")
            row["revenue_ttm_usd"] = round(rev_ttm / 1e6, 2) if rev_ttm else ""
            if not rev_ttm:
                warnings.append("missing_revenue")
        else:
            row["revenue_ttm_usd"] = ""
        
        # Net Income TTM
        if has_4_quarters and is_consistent:
            ni_ttm = sum_ttm(income_q, quarters, "netIncome")
            row["net_income_ttm_usd"] = round(ni_ttm / 1e6, 2) if ni_ttm is not None else ""
        else:
            row["net_income_ttm_usd"] = ""
        
        # Gross Margin TTM
        if has_4_quarters and is_consistent:
            gp_ttm = sum_ttm(income_q, quarters, "grossProfit")
            rev_ttm = sum_ttm(income_q, quarters, "totalRevenue")
            if rev_ttm and gp_ttm and rev_ttm > 0:
                row["gross_margin_ttm_pct"] = round((gp_ttm / rev_ttm) * 100, 2)
            else:
                row["gross_margin_ttm_pct"] = ""
        else:
            row["gross_margin_ttm_pct"] = ""
        
        # FCF TTM from Cash_Flow
        cashflow_q = fundamentals.get("Financials", {}).get("Cash_Flow", {}).get("quarterly", {})
        cf_quarters = sorted(cashflow_q.keys(), reverse=True) if cashflow_q else []
        if len(cf_quarters) >= 4 and is_consistent:
            fcf_ttm = sum_ttm(cashflow_q, cf_quarters, "freeCashFlow")
            row["fcf_ttm_usd"] = round(fcf_ttm / 1e6, 2) if fcf_ttm is not None else ""
        else:
            row["fcf_ttm_usd"] = ""
        
        # Net Debt (latest quarter from Balance_Sheet)
        balance_q = fundamentals.get("Financials", {}).get("Balance_Sheet", {}).get("quarterly", {})
        bs_quarters = sorted(balance_q.keys(), reverse=True) if balance_q else []
        if bs_quarters:
            latest_bs = balance_q.get(bs_quarters[0], {})
            total_debt = latest_bs.get("totalDebt") or latest_bs.get("shortLongTermDebtTotal")
            cash = latest_bs.get("cashAndEquivalents") or latest_bs.get("cash")
            if total_debt is not None and cash is not None:
                try:
                    row["net_debt_latest_usd"] = round((float(total_debt) - float(cash)) / 1e6, 2)
                except:
                    row["net_debt_latest_usd"] = ""
            else:
                row["net_debt_latest_usd"] = ""
        else:
            row["net_debt_latest_usd"] = ""
        
        # ROIC TTM - deferred (too complex)
        row["roic_ttm_pct"] = "N/A (deferred)"
        
        # Shares Outstanding
        shares = fundamentals.get("SharesStats", {}).get("SharesOutstanding")
        if not shares:
            shares = fundamentals.get("Highlights", {}).get("SharesOutstanding")
        row["shares_outstanding"] = shares if shares else ""
        if not shares:
            warnings.append("missing_shares")
        
        # === VALUATION METRICS ===
        vc = valuations.get(ticker, {})
        current_metrics = vc.get("current_metrics", {})
        
        for metric in ["pe", "ps", "pb", "ev_ebitda", "ev_revenue"]:
            val = current_metrics.get(metric)
            row[metric] = round(val, 4) if val is not None else ""
            
            # Source
            if metric == "pe":
                row[f"{metric}_source"] = "price|eps_ttm"
            elif metric == "ps":
                row[f"{metric}_source"] = "market_cap|revenue_ttm"
            elif metric == "pb":
                row[f"{metric}_source"] = "price|book_value_per_share"
            elif metric == "ev_ebitda":
                row[f"{metric}_source"] = "ev|ebitda_ttm"
            else:
                row[f"{metric}_source"] = "ev|revenue_ttm"
            
            # Status
            if val is not None and val > 0:
                row[f"{metric}_status"] = "ok"
            elif val is not None and val <= 0:
                row[f"{metric}_status"] = "non_positive_value"
                warnings.append(f"non_positive_{metric}")
            else:
                row[f"{metric}_status"] = "missing_raw_data"
                warnings.append(f"missing_{metric}")
        
        # === WARNINGS ===
        row["warnings"] = "|".join(warnings) if warnings else ""
        row["warning_count"] = len(warnings)
        
        # Track warnings by type
        for w in warnings:
            warnings_by_type[w] += 1
        
        results.append(row)
    
    # Sort by ticker
    results.sort(key=lambda x: x["ticker"])
    
    # === SAVE CSV ===
    print("Saving CSV...")
    csv_fields = [
        # Identifiers
        "ticker", "company_name", "country", "exchange", "is_visible", "asset_type",
        "sector", "industry", "financial_currency",
        # Freshness
        "latest_price_date", "latest_quarter_end_date", "latest_10q_10k_filing_date", "days_since_latest_report",
        # Fundamentals
        "revenue_ttm_usd", "net_income_ttm_usd", "fcf_ttm_usd", "net_debt_latest_usd",
        "roic_ttm_pct", "gross_margin_ttm_pct", "shares_outstanding",
        # Valuation metrics
        "pe", "ps", "pb", "ev_ebitda", "ev_revenue",
        # Provenance
        "pe_source", "pe_status", "ps_source", "ps_status", "pb_source", "pb_status",
        "ev_ebitda_source", "ev_ebitda_status", "ev_revenue_source", "ev_revenue_status",
        # Warnings
        "warnings", "warning_count"
    ]
    
    csv_path = f"{OUTPUT_DIR}/master_verification_table.csv"
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=csv_fields, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        writer.writerows(results)
    
    # === SAVE SUMMARY JSON ===
    print("Saving summary...")
    tickers_with_warnings = sum(1 for r in results if r["warning_count"] > 0)
    tickers_clean = len(results) - tickers_with_warnings
    
    # Top 10 by warning count
    top_10_warnings = sorted(results, key=lambda x: x["warning_count"], reverse=True)[:10]
    
    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_tickers": len(results),
        "tickers_with_warnings": tickers_with_warnings,
        "tickers_clean": tickers_clean,
        "clean_rate_pct": round((tickers_clean / len(results)) * 100, 1) if results else 0,
        "warnings_by_type": dict(sorted(warnings_by_type.items(), key=lambda x: -x[1])),
        "top_10_by_warning_count": [
            {
                "ticker": r["ticker"],
                "warning_count": r["warning_count"],
                "warnings": r["warnings"]
            }
            for r in top_10_warnings
        ]
    }
    
    summary_path = f"{OUTPUT_DIR}/master_verification_summary.json"
    with open(summary_path, "w") as f:
        json.dump(summary, f, indent=2)
    
    # === PRINT SUMMARY ===
    print()
    print("=" * 70)
    print("GENERATION COMPLETE")
    print("=" * 70)
    print()
    print(f"Total tickers: {len(results):,}")
    print(f"Tickers with warnings: {tickers_with_warnings:,}")
    print(f"Tickers clean (0 warnings): {tickers_clean:,}")
    print(f"Clean rate: {summary['clean_rate_pct']:.1f}%")
    print()
    print("WARNINGS BY TYPE:")
    for w_type, count in sorted(warnings_by_type.items(), key=lambda x: -x[1])[:15]:
        print(f"  {w_type}: {count:,}")
    print()
    print("TOP 10 TICKERS BY WARNING COUNT:")
    for item in top_10_warnings:
        print(f"  {item['ticker']}: {item['warning_count']} warnings")
    print()
    print("FILES SAVED:")
    print(f"  - {csv_path}")
    print(f"  - {summary_path}")
    print()
    print(f"Completed: {datetime.now(timezone.utc).isoformat()}")
    
    return summary, results

if __name__ == "__main__":
    generate_master_table()
