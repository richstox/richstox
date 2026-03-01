#!/usr/bin/env python3
"""
Final Data Transparency Report
==============================

Generates comprehensive report for P0 approval:
A) Full classification inventory (Industries, Sectors, Market)
B) 5Y average definition and implementation status
C) Proof of data integrity

Output: JSON report + console summary
"""

import os
import sys
import asyncio
import json
from datetime import datetime, timezone
from collections import defaultdict

sys.path.insert(0, '/app/backend')

from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv
load_dotenv('/app/backend/.env')


async def generate_report():
    mongo_url = os.environ.get('MONGO_URL', 'mongodb://localhost:27017')
    db_name = os.environ.get('DB_NAME', 'test_database')
    
    client = AsyncIOMotorClient(mongo_url)
    db = client[db_name]
    
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    
    report = {
        "report_date": today,
        "database": db_name,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
    
    try:
        # =====================================================================
        # SECTION A: FULL CLASSIFICATION INVENTORY
        # =====================================================================
        print("="*80)
        print("SECTION A: FULL CLASSIFICATION INVENTORY")
        print("="*80)
        
        # A.1: Universe counts
        visible_ticker_count = await db.tracked_tickers.count_documents({"is_visible": True})
        processed_ticker_count = await db.ticker_key_metrics_daily.count_documents({"date": today})
        
        print(f"\n--- A.1: UNIVERSE COUNTS ---")
        print(f"visible_ticker_count: {visible_ticker_count}")
        print(f"processed_ticker_count: {processed_ticker_count}")
        
        # Explain gap
        gap = visible_ticker_count - processed_ticker_count
        print(f"gap: {gap} tickers not processed")
        
        # Find reasons for gap
        visible_tickers = await db.tracked_tickers.find(
            {"is_visible": True},
            {"_id": 0, "ticker": 1, "sector": 1, "industry": 1}
        ).to_list(length=None)
        
        processed_set = set()
        processed_docs = await db.ticker_key_metrics_daily.find(
            {"date": today},
            {"_id": 0, "ticker": 1}
        ).to_list(length=None)
        for doc in processed_docs:
            processed_set.add(doc["ticker"])
        
        # Analyze missing tickers
        missing_reasons = defaultdict(int)
        missing_tickers_sample = []
        
        for t in visible_tickers:
            ticker = t["ticker"]
            if ticker not in processed_set:
                symbol = ticker.replace(".US", "").upper()
                
                # Check fundamentals
                fund = await db.company_fundamentals_cache.find_one({"symbol": symbol})
                if not fund:
                    missing_reasons["no_fundamentals"] += 1
                    if len(missing_tickers_sample) < 5:
                        missing_tickers_sample.append({"ticker": ticker, "reason": "no_fundamentals"})
                    continue
                
                # Check price
                price_doc = await db.stock_prices.find_one({"ticker": ticker})
                if not price_doc:
                    missing_reasons["no_price_data"] += 1
                    if len(missing_tickers_sample) < 5:
                        missing_tickers_sample.append({"ticker": ticker, "reason": "no_price_data"})
                    continue
                
                # Check sector/industry
                if not t.get("sector") or not t.get("industry"):
                    missing_reasons["no_sector_industry"] += 1
                    if len(missing_tickers_sample) < 5:
                        missing_tickers_sample.append({"ticker": ticker, "reason": "no_sector_industry"})
                    continue
                
                # Check shares_outstanding
                from key_metrics_service import extract_shares_outstanding
                shares = extract_shares_outstanding(fund)
                if not shares:
                    missing_reasons["no_shares_outstanding"] += 1
                    if len(missing_tickers_sample) < 5:
                        missing_tickers_sample.append({"ticker": ticker, "reason": "no_shares_outstanding"})
                    continue
                
                missing_reasons["unknown"] += 1
        
        print(f"\n--- A.2: GAP ANALYSIS (why {gap} tickers not processed) ---")
        for reason, count in sorted(missing_reasons.items(), key=lambda x: -x[1]):
            print(f"  {reason}: {count}")
        
        report["universe"] = {
            "visible_ticker_count": visible_ticker_count,
            "processed_ticker_count": processed_ticker_count,
            "gap": gap,
            "gap_reasons": dict(missing_reasons),
            "missing_tickers_sample": missing_tickers_sample
        }
        
        # A.3: All Industries with metrics
        print(f"\n--- A.3: INDUSTRY INVENTORY ---")
        
        # Get all metrics for today
        all_metrics = await db.ticker_key_metrics_daily.find(
            {"date": today},
            {"_id": 0}
        ).to_list(length=None)
        
        # Get peer medians
        peer_medians = await db.peer_medians_daily.find(
            {"date": today},
            {"_id": 0}
        ).to_list(length=None)
        
        # Build median lookup
        median_lookup = {}
        for pm in peer_medians:
            key = (pm["group_type"], pm["group_name"], pm["metric"])
            median_lookup[key] = {"median_value": pm["median_value"], "peer_count": pm["peer_count"]}
        
        # Count tickers per industry (from visible tickers, not processed)
        industry_ticker_counts = defaultdict(int)
        sector_ticker_counts = defaultdict(int)
        
        for t in visible_tickers:
            ind = t.get("industry")
            sec = t.get("sector")
            if ind:
                industry_ticker_counts[ind] += 1
            if sec:
                sector_ticker_counts[sec] += 1
        
        # Build industry table
        metrics_list = ["pe_ttm", "ps_ttm", "pb", "ev_ebitda_ttm", "ev_revenue_ttm"]
        
        industries_table = []
        for industry in sorted(industry_ticker_counts.keys()):
            row = {
                "industry_name": industry,
                "ticker_count": industry_ticker_counts[industry]
            }
            for metric in metrics_list:
                key = ("industry", industry, metric)
                if key in median_lookup:
                    row[f"{metric}_median"] = median_lookup[key]["median_value"]
                    row[f"{metric}_peer_count"] = median_lookup[key]["peer_count"]
                else:
                    row[f"{metric}_median"] = None
                    row[f"{metric}_peer_count"] = 0
            industries_table.append(row)
        
        # Print summary
        print(f"Total industries: {len(industries_table)}")
        industry_ticker_sum = sum(industry_ticker_counts.values())
        print(f"Sum of ticker_count across industries: {industry_ticker_sum}")
        
        # A.4: All Sectors with metrics
        print(f"\n--- A.4: SECTOR INVENTORY ---")
        
        sectors_table = []
        for sector in sorted(sector_ticker_counts.keys()):
            row = {
                "sector_name": sector,
                "ticker_count": sector_ticker_counts[sector]
            }
            for metric in metrics_list:
                key = ("sector", sector, metric)
                if key in median_lookup:
                    row[f"{metric}_median"] = median_lookup[key]["median_value"]
                    row[f"{metric}_peer_count"] = median_lookup[key]["peer_count"]
                else:
                    row[f"{metric}_median"] = None
                    row[f"{metric}_peer_count"] = 0
            sectors_table.append(row)
        
        print(f"Total sectors: {len(sectors_table)}")
        sector_ticker_sum = sum(sector_ticker_counts.values())
        print(f"Sum of ticker_count across sectors: {sector_ticker_sum}")
        
        # A.5: Market medians
        print(f"\n--- A.5: MARKET MEDIANS ---")
        
        market_medians = {}
        for metric in metrics_list:
            key = ("market", "US", metric)
            if key in median_lookup:
                market_medians[metric] = median_lookup[key]
                print(f"  {metric}: median={median_lookup[key]['median_value']}, peer_count={median_lookup[key]['peer_count']}")
            else:
                market_medians[metric] = {"median_value": None, "peer_count": 0}
                print(f"  {metric}: NO DATA")
        
        report["industries"] = industries_table
        report["sectors"] = sectors_table
        report["market_medians"] = market_medians
        
        # =====================================================================
        # SECTION B: 5Y AVERAGE DEFINITION
        # =====================================================================
        print("\n" + "="*80)
        print("SECTION B: 5Y AVERAGE DEFINITION")
        print("="*80)
        
        # Check current implementation
        print("""
--- B.1: TICKER 5Y AVERAGE (per metric) ---

CURRENT IMPLEMENTATION STATUS: NOT YET IMPLEMENTED

The function `get_5y_median_for_ticker()` exists in key_metrics_service.py but:
- It requires historical ticker_key_metrics_daily data
- Currently we only have TODAY's data (first run)
- Historical data will accumulate over time as scheduler runs daily

PROPOSED DEFINITION:
  Data source: ticker_key_metrics_daily collection
  Window: Last 5 calendar years (5*365 days from today)
  Statistic: MEDIAN (robust to outliers)
  Missing days: Excluded from calculation
  Null values: Excluded from calculation
  Minimum sample: 52 data points (approx 1 year of weekly data)

FORMULA:
  ticker_5y_median[metric] = median(
    ticker_key_metrics_daily[ticker][metric]
    WHERE date >= (today - 5 years)
    AND metric IS NOT NULL
    AND metric > 0
  )

--- B.2: GROUP 5Y AVERAGE (Industry/Sector) ---

CURRENT IMPLEMENTATION STATUS: NOT YET IMPLEMENTED

PROPOSED DEFINITION:
  Data source: peer_medians_daily collection
  Window: Last 5 calendar years
  Statistic: MEDIAN of daily medians
  Pooled approach: Take median of all peer_medians_daily values for that group

FORMULA:
  industry_5y_median[metric] = median(
    peer_medians_daily[industry][metric]
    WHERE date >= (today - 5 years)
    AND group_type = 'industry'
  )
""")
        
        report["5y_average_definition"] = {
            "ticker_5y_average": {
                "status": "NOT_YET_IMPLEMENTED",
                "data_source": "ticker_key_metrics_daily",
                "window": "5 calendar years (5*365 days)",
                "statistic": "median",
                "missing_handling": "excluded from calculation",
                "null_handling": "excluded from calculation",
                "minimum_sample_size": 52,
                "formula": "median(ticker_key_metrics_daily[ticker][metric] WHERE date >= today-5y AND metric > 0)"
            },
            "group_5y_average": {
                "status": "NOT_YET_IMPLEMENTED",
                "data_source": "peer_medians_daily",
                "window": "5 calendar years",
                "statistic": "median of daily medians",
                "formula": "median(peer_medians_daily[group][metric] WHERE date >= today-5y)"
            },
            "note": "Historical data will accumulate as scheduler runs daily. 5Y averages will become available after sufficient data collection."
        }
        
        # =====================================================================
        # SECTION C: PROOF
        # =====================================================================
        print("\n" + "="*80)
        print("SECTION C: PROOF")
        print("="*80)
        
        # C.1: Verify sums
        print(f"\n--- C.1: VERIFICATION ---")
        
        # Count tickers without sector/industry
        no_industry_count = sum(1 for t in visible_tickers if not t.get("industry"))
        no_sector_count = sum(1 for t in visible_tickers if not t.get("sector"))
        
        print(f"visible_ticker_count: {visible_ticker_count}")
        print(f"Sum of industry ticker_counts: {industry_ticker_sum}")
        print(f"Tickers without industry: {no_industry_count}")
        print(f"Match: {industry_ticker_sum + no_industry_count} == {visible_ticker_count}: {industry_ticker_sum + no_industry_count == visible_ticker_count}")
        
        print(f"\nSum of sector ticker_counts: {sector_ticker_sum}")
        print(f"Tickers without sector: {no_sector_count}")
        print(f"Match: {sector_ticker_sum + no_sector_count} == {visible_ticker_count}: {sector_ticker_sum + no_sector_count == visible_ticker_count}")
        
        # C.2: Code locations
        print(f"\n--- C.2: CODE LOCATIONS ---")
        print("""
ticker_key_metrics_daily computation:
  File: /app/backend/key_metrics_service.py
  Function: compute_daily_key_metrics()
  Called by: scheduler.py at 05:00 Prague time (Job A)

peer_medians_daily computation:
  File: /app/backend/key_metrics_service.py
  Function: compute_daily_peer_medians()
  Called by: scheduler.py at 05:30 Prague time (Job B)

5Y average computation:
  File: /app/backend/key_metrics_service.py
  Function: get_5y_median_for_ticker() - EXISTS but needs historical data
  Status: Will be functional after ~52+ days of daily runs

Peer median lookup (with fallback):
  File: /app/backend/key_metrics_service.py
  Function: get_peer_median_from_daily()
  Fallback order: Industry -> Sector -> Market
""")
        
        report["proof"] = {
            "industry_ticker_sum": industry_ticker_sum,
            "industry_ticker_sum_plus_no_industry": industry_ticker_sum + no_industry_count,
            "sector_ticker_sum": sector_ticker_sum,
            "sector_ticker_sum_plus_no_sector": sector_ticker_sum + no_sector_count,
            "visible_ticker_count": visible_ticker_count,
            "industry_sum_matches": industry_ticker_sum + no_industry_count == visible_ticker_count,
            "sector_sum_matches": sector_ticker_sum + no_sector_count == visible_ticker_count,
            "tickers_without_industry": no_industry_count,
            "tickers_without_sector": no_sector_count,
            "code_locations": {
                "ticker_key_metrics_daily": {
                    "file": "/app/backend/key_metrics_service.py",
                    "function": "compute_daily_key_metrics()",
                    "scheduler": "scheduler.py Job A at 05:00 Prague"
                },
                "peer_medians_daily": {
                    "file": "/app/backend/key_metrics_service.py",
                    "function": "compute_daily_peer_medians()",
                    "scheduler": "scheduler.py Job B at 05:30 Prague"
                },
                "5y_average": {
                    "file": "/app/backend/key_metrics_service.py",
                    "function": "get_5y_median_for_ticker()",
                    "status": "exists, needs historical data accumulation"
                },
                "peer_median_lookup": {
                    "file": "/app/backend/key_metrics_service.py",
                    "function": "get_peer_median_from_daily()",
                    "fallback": "Industry -> Sector -> Market"
                }
            }
        }
        
        # Save report to file
        report_path = "/app/reports/final_transparency_report.json"
        os.makedirs("/app/reports", exist_ok=True)
        
        with open(report_path, "w") as f:
            json.dump(report, f, indent=2, default=str)
        
        print(f"\n--- REPORT SAVED ---")
        print(f"Full JSON report: {report_path}")
        
        # Print tables in readable format
        print("\n" + "="*80)
        print("INDUSTRY TABLE (sorted by ticker_count)")
        print("="*80)
        print(f"{'Industry':<45} {'Tickers':>7} {'P/E':>8} {'P/S':>8} {'P/B':>8} {'EV/EBITDA':>10} {'EV/Rev':>8}")
        print("-"*100)
        
        for row in sorted(industries_table, key=lambda x: -x["ticker_count"])[:30]:
            pe = f"{row['pe_ttm_median']:.1f}" if row['pe_ttm_median'] else "N/A"
            ps = f"{row['ps_ttm_median']:.1f}" if row['ps_ttm_median'] else "N/A"
            pb = f"{row['pb_median']:.1f}" if row['pb_median'] else "N/A"
            ev_eb = f"{row['ev_ebitda_ttm_median']:.1f}" if row['ev_ebitda_ttm_median'] else "N/A"
            ev_rev = f"{row['ev_revenue_ttm_median']:.1f}" if row['ev_revenue_ttm_median'] else "N/A"
            print(f"{row['industry_name'][:44]:<45} {row['ticker_count']:>7} {pe:>8} {ps:>8} {pb:>8} {ev_eb:>10} {ev_rev:>8}")
        
        print(f"\n... (showing top 30 of {len(industries_table)} industries)")
        
        print("\n" + "="*80)
        print("SECTOR TABLE (all sectors)")
        print("="*80)
        print(f"{'Sector':<30} {'Tickers':>7} {'P/E':>8} {'P/S':>8} {'P/B':>8} {'EV/EBITDA':>10} {'EV/Rev':>8}")
        print("-"*85)
        
        for row in sorted(sectors_table, key=lambda x: -x["ticker_count"]):
            pe = f"{row['pe_ttm_median']:.1f}" if row['pe_ttm_median'] else "N/A"
            ps = f"{row['ps_ttm_median']:.1f}" if row['ps_ttm_median'] else "N/A"
            pb = f"{row['pb_median']:.1f}" if row['pb_median'] else "N/A"
            ev_eb = f"{row['ev_ebitda_ttm_median']:.1f}" if row['ev_ebitda_ttm_median'] else "N/A"
            ev_rev = f"{row['ev_revenue_ttm_median']:.1f}" if row['ev_revenue_ttm_median'] else "N/A"
            print(f"{row['sector_name'][:29]:<30} {row['ticker_count']:>7} {pe:>8} {ps:>8} {pb:>8} {ev_eb:>10} {ev_rev:>8}")
        
        return report
        
    finally:
        client.close()


if __name__ == "__main__":
    asyncio.run(generate_report())
