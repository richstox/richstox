# ==============================================================================
# P0 PHASE 3: Completeness Report Script
# ==============================================================================
# Purpose: Generate data completeness report for ALL visible tickers (6,446)
# Run: python scripts/completeness_report.py
# ==============================================================================

import asyncio
import os
import sys
import json
from datetime import datetime, timezone
from typing import Dict, Any, List

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
load_dotenv()

from motor.motor_asyncio import AsyncIOMotorClient
from config import get_mongo_url, get_db_name

MONGO_URL = get_mongo_url()
DB_NAME = get_db_name()


async def generate_completeness_report(db) -> Dict[str, Any]:
    """
    Generate completeness report for ALL visible tickers.
    
    Checks:
    - officers present (non-empty array)
    - identifiers present (ISIN, CUSIP, CIK)
    - address + phone present
    - statements coverage (annual + quarterly IS/BS/CF exist)
    
    Returns:
        Dict with totals + top offenders list
    """
    report_id = f"completeness_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    
    # Get ALL visible tickers
    visible_query = {"is_visible": True}
    visible_tickers = await db.tracked_tickers.find(
        visible_query,
        {"ticker": 1, "symbol": 1, "name": 1, "_id": 0}
    ).to_list(None)
    
    total_visible = len(visible_tickers)
    print(f"Total visible tickers: {total_visible}")
    
    # Initialize counters and offender lists
    missing = {
        "officers": [],
        "isin": [],
        "cusip": [],
        "cik": [],
        "address": [],
        "phone": [],
        "income_statement_yearly": [],
        "income_statement_quarterly": [],
        "balance_sheet_yearly": [],
        "balance_sheet_quarterly": [],
        "cash_flow_yearly": [],
        "cash_flow_quarterly": [],
        "no_fundamentals": [],
    }
    
    present = {
        "officers": 0,
        "isin": 0,
        "cusip": 0,
        "cik": 0,
        "address": 0,
        "phone": 0,
        "income_statement_yearly": 0,
        "income_statement_quarterly": 0,
        "balance_sheet_yearly": 0,
        "balance_sheet_quarterly": 0,
        "cash_flow_yearly": 0,
        "cash_flow_quarterly": 0,
        "has_fundamentals": 0,
    }
    
    # Process each visible ticker
    for i, ticker_doc in enumerate(visible_tickers):
        if i > 0 and i % 1000 == 0:
            print(f"Processing {i}/{total_visible}...")
        
        ticker = ticker_doc["ticker"]
        symbol = ticker_doc.get("symbol", ticker.replace(".US", ""))
        name = ticker_doc.get("name", "")
        
        # Find fundamentals in cache
        fund = await db.company_fundamentals_cache.find_one(
            {"$or": [{"symbol": symbol}, {"ticker": ticker}]},
            {
                "officers": 1, "isin": 1, "cusip": 1, "cik": 1,
                "address": 1, "phone": 1,
                "financials_income_statement_yearly": 1,
                "financials_income_statement_quarterly": 1,
                "financials_balance_sheet_yearly": 1,
                "financials_balance_sheet_quarterly": 1,
                "financials_cash_flow_yearly": 1,
                "financials_cash_flow_quarterly": 1,
                "_id": 0
            }
        )
        
        ticker_info = {"ticker": ticker, "symbol": symbol, "name": name}
        
        if not fund:
            missing["no_fundamentals"].append(ticker_info)
            # All fields missing if no fundamentals
            for key in missing:
                if key != "no_fundamentals":
                    missing[key].append(ticker_info)
            continue
        
        present["has_fundamentals"] += 1
        
        # Check officers
        officers = fund.get("officers")
        if officers and isinstance(officers, list) and len(officers) > 0:
            present["officers"] += 1
        else:
            missing["officers"].append(ticker_info)
        
        # Check identifiers
        for ident in ["isin", "cusip", "cik"]:
            val = fund.get(ident)
            if val and str(val).strip():
                present[ident] += 1
            else:
                missing[ident].append(ticker_info)
        
        # Check address + phone
        address = fund.get("address")
        if address and str(address).strip():
            present["address"] += 1
        else:
            missing["address"].append(ticker_info)
        
        phone = fund.get("phone")
        if phone and str(phone).strip():
            present["phone"] += 1
        else:
            missing["phone"].append(ticker_info)
        
        # Check financial statements (yearly + quarterly for IS/BS/CF)
        statements = [
            ("income_statement_yearly", "financials_income_statement_yearly"),
            ("income_statement_quarterly", "financials_income_statement_quarterly"),
            ("balance_sheet_yearly", "financials_balance_sheet_yearly"),
            ("balance_sheet_quarterly", "financials_balance_sheet_quarterly"),
            ("cash_flow_yearly", "financials_cash_flow_yearly"),
            ("cash_flow_quarterly", "financials_cash_flow_quarterly"),
        ]
        
        for stat_key, fund_key in statements:
            data = fund.get(fund_key)
            if data and isinstance(data, dict) and len(data) > 0:
                present[stat_key] += 1
            else:
                missing[stat_key].append(ticker_info)
    
    # Build summary
    summary = {
        "total_visible": total_visible,
        "has_fundamentals": present["has_fundamentals"],
        "missing_fundamentals": len(missing["no_fundamentals"]),
    }
    
    for key in present:
        if key != "has_fundamentals":
            summary[f"has_{key}"] = present[key]
            summary[f"missing_{key}"] = len(missing[key])
            summary[f"pct_{key}"] = round(100 * present[key] / total_visible, 2) if total_visible > 0 else 0
    
    # Top offenders (first 50 for each category)
    top_offenders = {}
    for key in missing:
        top_offenders[key] = missing[key][:50]
    
    report = {
        "report_id": report_id,
        "generated_at": datetime.now(timezone.utc),
        "db_name": DB_NAME,
        "total_visible_tickers": total_visible,
        "summary": summary,
        "top_offenders": top_offenders,
        "full_missing_counts": {k: len(v) for k, v in missing.items()},
    }
    
    # Store to DB
    await db.completeness_reports.insert_one(report)
    print(f"Report saved to DB: {report_id}")
    
    return report


async def main():
    """Run completeness report."""
    client = AsyncIOMotorClient(MONGO_URL)
    db = client[DB_NAME]
    
    print(f"Connecting to DB: {DB_NAME}")
    print("=" * 70)
    
    report = await generate_completeness_report(db)
    
    print("\n" + "=" * 70)
    print("COMPLETENESS REPORT SUMMARY")
    print("=" * 70)
    
    summary = report["summary"]
    print(f"Total visible tickers: {summary['total_visible']}")
    print(f"Has fundamentals: {summary['has_fundamentals']} ({100*summary['has_fundamentals']/summary['total_visible']:.1f}%)")
    print()
    print("COVERAGE:")
    print(f"  Officers:    {summary.get('has_officers', 0):>5} ({summary.get('pct_officers', 0):.1f}%)")
    print(f"  ISIN:        {summary.get('has_isin', 0):>5} ({summary.get('pct_isin', 0):.1f}%)")
    print(f"  CUSIP:       {summary.get('has_cusip', 0):>5} ({summary.get('pct_cusip', 0):.1f}%)")
    print(f"  CIK:         {summary.get('has_cik', 0):>5} ({summary.get('pct_cik', 0):.1f}%)")
    print(f"  Address:     {summary.get('has_address', 0):>5} ({summary.get('pct_address', 0):.1f}%)")
    print(f"  Phone:       {summary.get('has_phone', 0):>5} ({summary.get('pct_phone', 0):.1f}%)")
    print()
    print("STATEMENTS (Yearly/Quarterly):")
    print(f"  IS Yearly:   {summary.get('has_income_statement_yearly', 0):>5} ({summary.get('pct_income_statement_yearly', 0):.1f}%)")
    print(f"  IS Qtrly:    {summary.get('has_income_statement_quarterly', 0):>5} ({summary.get('pct_income_statement_quarterly', 0):.1f}%)")
    print(f"  BS Yearly:   {summary.get('has_balance_sheet_yearly', 0):>5} ({summary.get('pct_balance_sheet_yearly', 0):.1f}%)")
    print(f"  BS Qtrly:    {summary.get('has_balance_sheet_quarterly', 0):>5} ({summary.get('pct_balance_sheet_quarterly', 0):.1f}%)")
    print(f"  CF Yearly:   {summary.get('has_cash_flow_yearly', 0):>5} ({summary.get('pct_cash_flow_yearly', 0):.1f}%)")
    print(f"  CF Qtrly:    {summary.get('has_cash_flow_quarterly', 0):>5} ({summary.get('pct_cash_flow_quarterly', 0):.1f}%)")
    print()
    print(f"Report ID: {report['report_id']}")
    print("=" * 70)
    
    # Output JSON for piping
    print("\n--- JSON OUTPUT ---")
    print(json.dumps({
        "report_id": report["report_id"],
        "summary": summary,
        "full_missing_counts": report["full_missing_counts"],
    }, indent=2, default=str))
    
    client.close()


if __name__ == "__main__":
    asyncio.run(main())
