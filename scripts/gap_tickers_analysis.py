#!/usr/bin/env python3
"""
Gap Tickers Analysis
====================

Detailed analysis of all tickers in the gap (visible but not processed).
Every ticker must have a deterministic reason - no "unknown" category.

Output: CSV + distribution report
"""

import os
import sys
import asyncio
import csv
from datetime import datetime, timezone
from collections import defaultdict

sys.path.insert(0, '/app/backend')

from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv
load_dotenv('/app/backend/.env')


# Reason taxonomy (deterministic)
REASONS = {
    "no_fundamentals": "No record in company_fundamentals_cache",
    "no_price_data": "No record in stock_prices",
    "no_shares_outstanding_direct": "shares_outstanding field is null/missing",
    "no_shares_outstanding_quarterly": "shares_outstanding_quarterly has no valid data",
    "no_shares_outstanding_annual": "shares_outstanding_annual has no valid data",
    "shares_outstanding_parse_failed": "shares_outstanding exists but cannot be parsed",
    "no_sector_or_industry": "Missing sector or industry classification",
    "invalid_ticker_format": "Ticker format is invalid or unsupported",
    "price_is_zero_or_null": "Latest price is 0 or null",
    "fundamentals_incomplete": "Fundamentals exist but missing critical fields",
}


async def analyze_gap_tickers():
    mongo_url = os.environ.get('MONGO_URL', 'mongodb://localhost:27017')
    db_name = os.environ.get('DB_NAME', 'test_database')
    
    client = AsyncIOMotorClient(mongo_url)
    db = client[db_name]
    
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    
    try:
        # Get all visible tickers
        visible_tickers = await db.tracked_tickers.find(
            {"is_visible": True},
            {"_id": 0}
        ).to_list(length=None)
        
        # Get all processed tickers for today
        processed_docs = await db.ticker_key_metrics_daily.find(
            {"date": today},
            {"_id": 0, "ticker": 1}
        ).to_list(length=None)
        processed_set = {doc["ticker"] for doc in processed_docs}
        
        print(f"Visible tickers: {len(visible_tickers)}")
        print(f"Processed tickers: {len(processed_set)}")
        
        # Find gap tickers
        gap_tickers = []
        for t in visible_tickers:
            if t["ticker"] not in processed_set:
                gap_tickers.append(t)
        
        print(f"Gap tickers: {len(gap_tickers)}")
        
        # Analyze each gap ticker
        results = []
        reason_counts = defaultdict(int)
        
        for t in gap_tickers:
            ticker = t["ticker"]
            symbol = ticker.replace(".US", "").upper()
            
            row = {
                "primary_ticker": ticker,
                "reason": None,
                "reason_detail": None,
                "sector": t.get("sector", ""),
                "industry": t.get("industry", ""),
                "is_visible": t.get("is_visible", False),
                "first_seen_date": t.get("seeded_at", t.get("created_at", "")),
                "name": t.get("name", ""),
            }
            
            # Check 1: Invalid ticker format
            if not ticker.endswith(".US") or len(ticker) < 4:
                row["reason"] = "invalid_ticker_format"
                row["reason_detail"] = f"Ticker format: {ticker}"
                results.append(row)
                reason_counts["invalid_ticker_format"] += 1
                continue
            
            # Check 2: No sector or industry
            if not t.get("sector") or not t.get("industry"):
                row["reason"] = "no_sector_or_industry"
                row["reason_detail"] = f"sector={t.get('sector')}, industry={t.get('industry')}"
                results.append(row)
                reason_counts["no_sector_or_industry"] += 1
                continue
            
            # Check 3: No fundamentals
            fund = await db.company_fundamentals_cache.find_one({"symbol": symbol})
            if not fund:
                row["reason"] = "no_fundamentals"
                row["reason_detail"] = f"No record for symbol={symbol}"
                results.append(row)
                reason_counts["no_fundamentals"] += 1
                continue
            
            # Check 4: No price data
            price_doc = await db.stock_prices.find_one(
                {"ticker": ticker},
                sort=[("date", -1)]
            )
            if not price_doc:
                row["reason"] = "no_price_data"
                row["reason_detail"] = "No price records found"
                results.append(row)
                reason_counts["no_price_data"] += 1
                continue
            
            # Check 5: Price is zero or null
            latest_price = price_doc.get("close")
            if not latest_price or latest_price <= 0:
                row["reason"] = "price_is_zero_or_null"
                row["reason_detail"] = f"Latest price: {latest_price}"
                results.append(row)
                reason_counts["price_is_zero_or_null"] += 1
                continue
            
            # Check 6: Shares outstanding analysis
            shares_outstanding = fund.get("shares_outstanding")
            shares_quarterly = fund.get("shares_outstanding_quarterly", {})
            shares_annual = fund.get("shares_outstanding_annual", {})
            
            # Try direct shares_outstanding
            if shares_outstanding is not None:
                try:
                    if isinstance(shares_outstanding, (int, float)):
                        val = float(shares_outstanding)
                        if val > 0:
                            # Should have been processed - check further
                            pass
                        else:
                            row["reason"] = "no_shares_outstanding_direct"
                            row["reason_detail"] = f"shares_outstanding={shares_outstanding} (<=0)"
                            results.append(row)
                            reason_counts["no_shares_outstanding_direct"] += 1
                            continue
                    elif isinstance(shares_outstanding, dict):
                        shares_val = shares_outstanding.get("shares") or shares_outstanding.get("value")
                        if not shares_val:
                            row["reason"] = "shares_outstanding_parse_failed"
                            row["reason_detail"] = f"Dict format but no value: {shares_outstanding}"
                            results.append(row)
                            reason_counts["shares_outstanding_parse_failed"] += 1
                            continue
                    elif isinstance(shares_outstanding, list):
                        if len(shares_outstanding) == 0:
                            row["reason"] = "no_shares_outstanding_direct"
                            row["reason_detail"] = "shares_outstanding is empty list"
                            results.append(row)
                            reason_counts["no_shares_outstanding_direct"] += 1
                            continue
                except Exception as e:
                    row["reason"] = "shares_outstanding_parse_failed"
                    row["reason_detail"] = f"Parse error: {e}"
                    results.append(row)
                    reason_counts["shares_outstanding_parse_failed"] += 1
                    continue
            
            # Try quarterly shares
            if shares_quarterly and isinstance(shares_quarterly, dict) and len(shares_quarterly) > 0:
                # Find latest with valid data
                has_valid_quarterly = False
                for key, entry in shares_quarterly.items():
                    if isinstance(entry, dict) and entry.get("shares"):
                        has_valid_quarterly = True
                        break
                
                if not has_valid_quarterly:
                    row["reason"] = "no_shares_outstanding_quarterly"
                    row["reason_detail"] = f"Quarterly data exists but no valid 'shares' field"
                    results.append(row)
                    reason_counts["no_shares_outstanding_quarterly"] += 1
                    continue
            
            # Try annual shares
            if shares_annual and isinstance(shares_annual, dict) and len(shares_annual) > 0:
                has_valid_annual = False
                for key, entry in shares_annual.items():
                    if isinstance(entry, dict) and entry.get("shares"):
                        has_valid_annual = True
                        break
                
                if not has_valid_annual:
                    row["reason"] = "no_shares_outstanding_annual"
                    row["reason_detail"] = f"Annual data exists but no valid 'shares' field"
                    results.append(row)
                    reason_counts["no_shares_outstanding_annual"] += 1
                    continue
            
            # If we got here, all share sources are empty/null
            if shares_outstanding is None and not shares_quarterly and not shares_annual:
                row["reason"] = "no_shares_outstanding_direct"
                row["reason_detail"] = "All shares_outstanding sources are null/empty"
                results.append(row)
                reason_counts["no_shares_outstanding_direct"] += 1
                continue
            
            # Check for fundamentals incomplete (missing income statement)
            income = fund.get("income_statement_quarterly", {})
            if not income or len(income) < 4:
                row["reason"] = "fundamentals_incomplete"
                row["reason_detail"] = f"income_statement_quarterly has {len(income) if income else 0} quarters (need 4)"
                results.append(row)
                reason_counts["fundamentals_incomplete"] += 1
                continue
            
            # If still no reason found, deep dive
            row["reason"] = "shares_outstanding_parse_failed"
            row["reason_detail"] = f"Could not extract shares: direct={type(shares_outstanding).__name__}, quarterly_keys={len(shares_quarterly) if shares_quarterly else 0}, annual_keys={len(shares_annual) if shares_annual else 0}"
            results.append(row)
            reason_counts["shares_outstanding_parse_failed"] += 1
        
        # Sort results by reason then ticker
        results.sort(key=lambda x: (x["reason"] or "", x["primary_ticker"]))
        
        # Write CSV
        csv_path = "/app/reports/gap_tickers_analysis.csv"
        os.makedirs("/app/reports", exist_ok=True)
        
        with open(csv_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=[
                "primary_ticker", "reason", "reason_detail", 
                "sector", "industry", "is_visible", "first_seen_date", "name"
            ])
            writer.writeheader()
            writer.writerows(results)
        
        print(f"\nCSV saved: {csv_path}")
        
        # Print distribution
        print("\n" + "="*80)
        print("REASON DISTRIBUTION")
        print("="*80)
        
        total = sum(reason_counts.values())
        for reason, count in sorted(reason_counts.items(), key=lambda x: -x[1]):
            pct = (count / total * 100) if total > 0 else 0
            desc = REASONS.get(reason, "")
            print(f"{reason:<40} {count:>5} ({pct:>5.1f}%)  | {desc}")
        
        print(f"{'TOTAL':<40} {total:>5}")
        
        # Print reason taxonomy
        print("\n" + "="*80)
        print("REASON TAXONOMY (FINAL)")
        print("="*80)
        
        used_reasons = set(reason_counts.keys())
        for reason in sorted(used_reasons):
            desc = REASONS.get(reason, "No description")
            print(f"  {reason}: {desc}")
        
        # Print sample tickers per reason
        print("\n" + "="*80)
        print("SAMPLE TICKERS PER REASON")
        print("="*80)
        
        reason_samples = defaultdict(list)
        for r in results:
            reason_samples[r["reason"]].append(r)
        
        for reason in sorted(reason_samples.keys()):
            samples = reason_samples[reason][:5]
            print(f"\n{reason} ({len(reason_samples[reason])} tickers):")
            for s in samples:
                print(f"  - {s['primary_ticker']}: {s['reason_detail'][:60]}")
        
        # Print full table
        print("\n" + "="*80)
        print("FULL GAP TICKERS TABLE")
        print("="*80)
        print(f"{'Ticker':<15} {'Reason':<35} {'Sector':<20} {'Industry':<25}")
        print("-"*95)
        
        for r in results:
            print(f"{r['primary_ticker']:<15} {r['reason']:<35} {r['sector'][:19]:<20} {r['industry'][:24]:<25}")
        
        return results, reason_counts
        
    finally:
        client.close()


if __name__ == "__main__":
    asyncio.run(analyze_gap_tickers())
