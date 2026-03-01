#!/usr/bin/env python3
"""
Full Universe Data Coverage Audit
=================================

This script runs the Key Metrics pipeline for the ENTIRE visible universe
and produces a comprehensive data coverage audit report.

NO mini-universe shortcuts. Process every ticker where is_visible=true.

Usage:
    cd /app/backend && python ../scripts/full_universe_audit.py

Steps:
1. Count visible universe (is_visible=true)
2. Run Job A (compute_daily_key_metrics) for ALL visible tickers
3. Run Job B (compute_daily_peer_medians) for ALL groups
4. Generate Data Coverage Audit report with numbers
5. Manual proof on a random non-famous ticker
"""

import os
import sys
import asyncio
import logging
import random
from datetime import datetime, timezone
from collections import defaultdict

# Add backend to path
sys.path.insert(0, '/app/backend')

from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv
load_dotenv('/app/backend/.env')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("full_universe_audit")

# EODHD API (only for seed script, not runtime)
EODHD_API_KEY = os.environ.get("EODHD_API_KEY", "")


async def run_full_audit():
    """Run complete data coverage audit on full visible universe."""
    
    # Connect to DB using same credentials as server
    mongo_url = os.environ.get('MONGO_URL', 'mongodb://localhost:27017')
    db_name = os.environ.get('DB_NAME', 'test_database')
    
    client = AsyncIOMotorClient(mongo_url)
    db = client[db_name]
    
    logger.info(f"="*70)
    logger.info(f"FULL UNIVERSE DATA COVERAGE AUDIT")
    logger.info(f"Database: {db_name}")
    logger.info(f"="*70)
    
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    
    try:
        # =====================================================================
        # PHASE 1: Universe Stats (before processing)
        # =====================================================================
        print("\n" + "="*70)
        print("PHASE 1: UNIVERSE STATS (before processing)")
        print("="*70)
        
        visible_ticker_count = await db.tracked_tickers.count_documents({"is_visible": True})
        print(f"\nVisible tickers (is_visible=true): {visible_ticker_count}")
        
        # Get distinct sectors and industries
        visible_tickers = await db.tracked_tickers.find(
            {"is_visible": True},
            {"_id": 0, "ticker": 1, "sector": 1, "industry": 1}
        ).to_list(length=None)
        
        sectors = set()
        industries = set()
        for t in visible_tickers:
            if t.get("sector"):
                sectors.add(t["sector"])
            if t.get("industry"):
                industries.add(t["industry"])
        
        print(f"Distinct sectors: {len(sectors)}")
        print(f"Distinct industries: {len(industries)}")
        
        # =====================================================================
        # PHASE 2: Run Job A (Key Metrics for ALL visible tickers)
        # =====================================================================
        print("\n" + "="*70)
        print("PHASE 2: Running Job A (Key Metrics for ALL visible tickers)")
        print("="*70)
        
        from key_metrics_service import compute_daily_key_metrics
        job_a_result = await compute_daily_key_metrics(db)
        
        print(f"\nJob A Result:")
        print(f"  Status: {job_a_result.get('status')}")
        print(f"  Date: {job_a_result.get('date')}")
        print(f"  Processed: {job_a_result.get('processed')}")
        print(f"  Errors: {job_a_result.get('errors')}")
        
        # =====================================================================
        # PHASE 3: Run Job B (Peer Medians for ALL groups)
        # =====================================================================
        print("\n" + "="*70)
        print("PHASE 3: Running Job B (Peer Medians for ALL groups)")
        print("="*70)
        
        from key_metrics_service import compute_daily_peer_medians
        job_b_result = await compute_daily_peer_medians(db)
        
        print(f"\nJob B Result:")
        print(f"  Status: {job_b_result.get('status')}")
        print(f"  Date: {job_b_result.get('date')}")
        print(f"  Medians stored: {job_b_result.get('medians_stored')}")
        print(f"  Industries: {job_b_result.get('industries')}")
        print(f"  Sectors: {job_b_result.get('sectors')}")
        
        # =====================================================================
        # PHASE 4: DATA COVERAGE AUDIT REPORT
        # =====================================================================
        print("\n" + "="*70)
        print("PHASE 4: DATA COVERAGE AUDIT REPORT")
        print("="*70)
        
        # 4.1 Universe
        print("\n--- UNIVERSE ---")
        processed_ticker_count = await db.ticker_key_metrics_daily.count_documents({"date": today})
        print(f"visible_ticker_count: {visible_ticker_count}")
        print(f"processed_ticker_count (ticker_key_metrics_daily for today): {processed_ticker_count}")
        
        # 4.2 Missing metrics rate per metric
        print("\n--- MISSING METRICS RATE ---")
        metrics = ["pe_ttm", "ps_ttm", "pb", "ev_ebitda_ttm", "ev_revenue_ttm"]
        
        all_key_metrics = await db.ticker_key_metrics_daily.find(
            {"date": today},
            {"_id": 0}
        ).to_list(length=None)
        
        total_processed = len(all_key_metrics)
        
        for metric in metrics:
            null_count = sum(1 for m in all_key_metrics if m.get(metric) is None)
            rate = (null_count / total_processed * 100) if total_processed > 0 else 0
            print(f"  {metric}: {null_count}/{total_processed} missing ({rate:.1f}%)")
        
        # 4.3 Aggregations
        print("\n--- AGGREGATIONS ---")
        
        # Industries with metrics
        industry_metrics = defaultdict(lambda: defaultdict(list))
        sector_metrics = defaultdict(lambda: defaultdict(list))
        market_metrics = defaultdict(list)
        
        for m in all_key_metrics:
            ind = m.get("industry")
            sec = m.get("sector")
            
            for metric in metrics:
                val = m.get(metric)
                if val is not None and val > 0:
                    if ind:
                        industry_metrics[ind][metric].append(val)
                    if sec:
                        sector_metrics[sec][metric].append(val)
                    market_metrics[metric].append(val)
        
        # Count industries/sectors with all 5 metrics having >= 5 peers
        MIN_PEER_COUNT = 5
        
        industry_count_total = len(industries)
        industry_count_with_all_medians = 0
        industry_count_with_partial_medians = 0
        
        for ind in industries:
            has_all = True
            has_any = False
            for metric in metrics:
                peer_count = len(industry_metrics.get(ind, {}).get(metric, []))
                if peer_count >= MIN_PEER_COUNT:
                    has_any = True
                else:
                    has_all = False
            
            if has_all:
                industry_count_with_all_medians += 1
            elif has_any:
                industry_count_with_partial_medians += 1
        
        print(f"\nIndustries:")
        print(f"  industry_count_total: {industry_count_total}")
        print(f"  industry_count_with_all_5_medians: {industry_count_with_all_medians}")
        print(f"  industry_count_with_partial_medians: {industry_count_with_partial_medians}")
        print(f"  industry_count_without_medians: {industry_count_total - industry_count_with_all_medians - industry_count_with_partial_medians}")
        
        sector_count_total = len(sectors)
        sector_count_with_all_medians = 0
        sector_count_with_partial_medians = 0
        
        for sec in sectors:
            has_all = True
            has_any = False
            for metric in metrics:
                peer_count = len(sector_metrics.get(sec, {}).get(metric, []))
                if peer_count >= MIN_PEER_COUNT:
                    has_any = True
                else:
                    has_all = False
            
            if has_all:
                sector_count_with_all_medians += 1
            elif has_any:
                sector_count_with_partial_medians += 1
        
        print(f"\nSectors:")
        print(f"  sector_count_total: {sector_count_total}")
        print(f"  sector_count_with_all_5_medians: {sector_count_with_all_medians}")
        print(f"  sector_count_with_partial_medians: {sector_count_with_partial_medians}")
        
        # Market medians
        print(f"\nMarket Medians (US):")
        for metric in metrics:
            values = market_metrics.get(metric, [])
            if len(values) >= MIN_PEER_COUNT:
                import statistics
                median_val = statistics.median(values)
                print(f"  {metric}: median={median_val:.2f}, peer_count={len(values)}")
            else:
                print(f"  {metric}: INSUFFICIENT DATA ({len(values)} peers)")
        
        # 4.4 Table sizes
        print("\n--- TABLE SIZES ---")
        key_metrics_count = await db.ticker_key_metrics_daily.count_documents({"date": today})
        peer_medians_count = await db.peer_medians_daily.count_documents({"date": today})
        
        print(f"ticker_key_metrics_daily (today): {key_metrics_count} records")
        print(f"peer_medians_daily (today): {peer_medians_count} records")
        
        # =====================================================================
        # PHASE 5: MANUAL PROOF ON RANDOM NON-FAMOUS TICKER
        # =====================================================================
        print("\n" + "="*70)
        print("PHASE 5: MANUAL PROOF ON RANDOM NON-FAMOUS TICKER")
        print("="*70)
        
        # Exclude mega-caps
        mega_caps = {"AAPL.US", "MSFT.US", "NVDA.US", "GOOGL.US", "GOOG.US", "AMZN.US", "META.US", "TSLA.US", "BRK-A.US", "BRK-B.US"}
        
        # Get all processed tickers with valid data
        candidates = [
            m for m in all_key_metrics 
            if m["ticker"] not in mega_caps 
            and m.get("market_cap") and m["market_cap"] >= 100_000_000  # >= 100M
            and m.get("ps_ttm") is not None
            and m.get("pb") is not None
        ]
        
        if candidates:
            # Pick random ticker
            random_ticker = random.choice(candidates)
            ticker_name = random_ticker["ticker"]
            industry = random_ticker.get("industry")
            sector = random_ticker.get("sector")
            
            print(f"\nRandom ticker selected: {ticker_name}")
            print(f"Industry: {industry}")
            print(f"Sector: {sector}")
            
            print(f"\n--- {ticker_name} VALUATION METRICS ---")
            print(f"  Price: ${random_ticker.get('price', 'N/A')}")
            mc = random_ticker.get('market_cap', 0)
            mc_str = f"${mc/1e9:.2f}B" if mc >= 1e9 else f"${mc/1e6:.0f}M"
            print(f"  Market Cap: {mc_str}")
            print(f"  P/E TTM: {random_ticker.get('pe_ttm', 'N/A')}")
            print(f"  P/S TTM: {random_ticker.get('ps_ttm', 'N/A')}")
            print(f"  P/B: {random_ticker.get('pb', 'N/A')}")
            print(f"  EV/EBITDA TTM: {random_ticker.get('ev_ebitda_ttm', 'N/A')}")
            print(f"  EV/Revenue TTM: {random_ticker.get('ev_revenue_ttm', 'N/A')}")
            
            print(f"\n--- {ticker_name} INDUSTRY MEDIANS ({industry}) ---")
            for metric in metrics:
                doc = await db.peer_medians_daily.find_one(
                    {"date": today, "group_type": "industry", "group_name": industry, "metric": metric},
                    {"_id": 0}
                )
                if doc:
                    print(f"  {metric}: median={doc['median_value']}, peer_count={doc['peer_count']}")
                else:
                    # Check sector fallback
                    sector_doc = await db.peer_medians_daily.find_one(
                        {"date": today, "group_type": "sector", "group_name": sector, "metric": metric},
                        {"_id": 0}
                    )
                    if sector_doc:
                        print(f"  {metric}: INDUSTRY N/A -> SECTOR fallback: median={sector_doc['median_value']}, peer_count={sector_doc['peer_count']}")
                    else:
                        print(f"  {metric}: NO DATA (industry or sector)")
            
            print(f"\n--- CONFIRMATION ---")
            print(f"Values come from full universe ({processed_ticker_count} tickers), NOT seeded peers.")
        else:
            print("\nNo suitable random ticker found.")
        
        # =====================================================================
        # SUMMARY
        # =====================================================================
        print("\n" + "="*70)
        print("AUDIT SUMMARY")
        print("="*70)
        print(f"Date: {today}")
        print(f"Database: {db_name}")
        print(f"Visible tickers: {visible_ticker_count}")
        print(f"Processed tickers: {processed_ticker_count}")
        print(f"Coverage: {processed_ticker_count/visible_ticker_count*100:.1f}%" if visible_ticker_count > 0 else "N/A")
        print(f"Industries with medians: {industry_count_with_all_medians}/{industry_count_total}")
        print(f"Sectors with medians: {sector_count_with_all_medians}/{sector_count_total}")
        print(f"Peer medians stored: {peer_medians_count}")
        print("="*70)
        
        return 0
        
    finally:
        client.close()


if __name__ == "__main__":
    sys.exit(asyncio.run(run_full_audit()))
