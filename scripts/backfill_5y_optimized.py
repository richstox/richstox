#!/usr/bin/env python3
"""
5Y Historical Backfill for ticker_key_metrics_daily (OPTIMIZED)
===============================================================

Optimized for speed:
- Batch inserts
- Pre-load all fundamentals and prices into memory
- Parallel processing where possible
"""

import os
import sys
import asyncio
import logging
from datetime import datetime, timezone, timedelta
from collections import defaultdict

sys.path.insert(0, '/app/backend')

from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv
load_dotenv('/app/backend/.env')

from key_metrics_service import (
    extract_shares_outstanding,
    compute_pe_ttm,
    compute_ps_ttm,
    compute_pb,
    compute_ev_ebitda_ttm,
    compute_ev_revenue_ttm,
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("backfill_5y")

BACKFILL_YEARS = 5
BATCH_SIZE = 1000


async def backfill_5y_metrics():
    mongo_url = os.environ.get('MONGO_URL')
    db_name = os.environ.get('DB_NAME', 'test_database')
    
    client = AsyncIOMotorClient(mongo_url)
    db = client[db_name]
    
    today = datetime.now(timezone.utc)
    start_date = today - timedelta(days=365 * BACKFILL_YEARS)
    
    logger.info("="*70)
    logger.info("5Y HISTORICAL BACKFILL (OPTIMIZED)")
    logger.info("="*70)
    logger.info(f"Date range: {start_date.strftime('%Y-%m-%d')} to {today.strftime('%Y-%m-%d')}")
    
    try:
        # Count before
        count_before = await db.ticker_key_metrics_daily.count_documents({})
        logger.info(f"Records before backfill: {count_before}")
        
        # Get visible tickers
        visible_tickers = await db.tracked_tickers.find(
            {"is_visible": True},
            {"_id": 0, "ticker": 1, "sector": 1, "industry": 1}
        ).to_list(length=None)
        
        logger.info(f"Visible tickers: {len(visible_tickers)}")
        
        # Generate sample dates (end of each month)
        sample_dates = []
        current = start_date
        while current < today:
            if current.month == 12:
                next_month = current.replace(year=current.year + 1, month=1, day=1)
            else:
                next_month = current.replace(month=current.month + 1, day=1)
            last_day = next_month - timedelta(days=1)
            sample_dates.append(last_day.strftime("%Y-%m-%d"))
            current = next_month
        
        logger.info(f"Sample dates: {len(sample_dates)}")
        
        # Pre-load all fundamentals
        logger.info("Loading fundamentals cache...")
        all_fundamentals = {}
        async for fund in db.company_fundamentals_cache.find({}, {"_id": 0}):
            symbol = fund.get("symbol")
            if symbol:
                all_fundamentals[symbol] = fund
        logger.info(f"Loaded {len(all_fundamentals)} fundamentals")
        
        # Get existing records to skip
        logger.info("Loading existing records...")
        existing = set()
        async for doc in db.ticker_key_metrics_daily.find({}, {"_id": 0, "ticker": 1, "date": 1}):
            existing.add((doc["ticker"], doc["date"]))
        logger.info(f"Existing records: {len(existing)}")
        
        # Stats
        stats = {
            "processed": 0,
            "inserted": 0,
            "skipped_no_fundamentals": 0,
            "skipped_no_shares": 0,
            "skipped_no_price": 0,
            "skipped_exists": 0,
            "full_coverage": 0,
            "partial_coverage": 0,
            "no_coverage": 0,
        }
        
        failed_tickers = defaultdict(list)
        ticker_coverage = {}
        
        batch = []
        
        for i, t in enumerate(visible_tickers):
            ticker = t["ticker"]
            symbol = ticker.replace(".US", "").upper()
            sector = t.get("sector")
            industry = t.get("industry")
            
            if i % 500 == 0:
                logger.info(f"Processing {i}/{len(visible_tickers)}... (batch: {len(batch)})")
                if batch:
                    await db.ticker_key_metrics_daily.insert_many(batch)
                    stats["inserted"] += len(batch)
                    batch = []
            
            # Get fundamentals
            fund = all_fundamentals.get(symbol)
            if not fund:
                stats["skipped_no_fundamentals"] += 1
                failed_tickers["no_fundamentals"].append(ticker)
                ticker_coverage[ticker] = 0
                continue
            
            shares = extract_shares_outstanding(fund)
            if not shares or shares <= 0:
                stats["skipped_no_shares"] += 1
                failed_tickers["no_shares"].append(ticker)
                ticker_coverage[ticker] = 0
                continue
            
            # Get all prices for this ticker
            prices = await db.stock_prices.find(
                {"ticker": ticker},
                {"_id": 0, "date": 1, "close": 1}
            ).to_list(length=None)
            
            if not prices:
                stats["skipped_no_price"] += 1
                failed_tickers["no_price"].append(ticker)
                ticker_coverage[ticker] = 0
                continue
            
            price_lookup = {p["date"]: p["close"] for p in prices if p.get("close")}
            
            dates_covered = 0
            
            for sample_date in sample_dates:
                if (ticker, sample_date) in existing:
                    stats["skipped_exists"] += 1
                    dates_covered += 1
                    continue
                
                # Find price
                price = price_lookup.get(sample_date)
                if not price:
                    for offset in range(1, 8):
                        check_date = (datetime.strptime(sample_date, "%Y-%m-%d") - timedelta(days=offset)).strftime("%Y-%m-%d")
                        if check_date in price_lookup:
                            price = price_lookup[check_date]
                            break
                
                if not price or price <= 0:
                    continue
                
                market_cap = price * shares
                
                doc = {
                    "ticker": ticker,
                    "date": sample_date,
                    "price": round(price, 2),
                    "market_cap": round(market_cap, 0),
                    "pe_ttm": compute_pe_ttm(fund, price),
                    "ps_ttm": compute_ps_ttm(fund, market_cap),
                    "pb": compute_pb(fund, market_cap),
                    "ev_ebitda_ttm": compute_ev_ebitda_ttm(fund, market_cap),
                    "ev_revenue_ttm": compute_ev_revenue_ttm(fund, market_cap),
                    "sector": sector,
                    "industry": industry,
                    "backfilled": True,
                }
                
                batch.append(doc)
                dates_covered += 1
            
            ticker_coverage[ticker] = dates_covered
            stats["processed"] += 1
        
        # Insert remaining batch
        if batch:
            await db.ticker_key_metrics_daily.insert_many(batch)
            stats["inserted"] += len(batch)
        
        # Calculate coverage stats
        for ticker, coverage in ticker_coverage.items():
            if coverage >= len(sample_dates) * 0.9:
                stats["full_coverage"] += 1
            elif coverage > 0:
                stats["partial_coverage"] += 1
            else:
                stats["no_coverage"] += 1
        
        # Create indexes
        await db.ticker_key_metrics_daily.create_index([("ticker", 1), ("date", 1)], unique=True)
        await db.ticker_key_metrics_daily.create_index([("date", 1)])
        await db.ticker_key_metrics_daily.create_index([("industry", 1), ("date", 1)])
        
        count_after = await db.ticker_key_metrics_daily.count_documents({})
        
        logger.info("="*70)
        logger.info("BACKFILL COMPLETE")
        logger.info("="*70)
        logger.info(f"Records before: {count_before}")
        logger.info(f"Records after: {count_after}")
        logger.info(f"New records: {count_after - count_before}")
        logger.info(f"Tickers processed: {stats['processed']}")
        logger.info(f"Records inserted: {stats['inserted']}")
        logger.info(f"Skipped (exists): {stats['skipped_exists']}")
        logger.info(f"")
        logger.info(f"COVERAGE:")
        logger.info(f"  Full (90%+): {stats['full_coverage']}")
        logger.info(f"  Partial: {stats['partial_coverage']}")
        logger.info(f"  None: {stats['no_coverage']}")
        logger.info(f"")
        logger.info(f"FAILURES BY REASON:")
        for reason, tickers in failed_tickers.items():
            logger.info(f"  {reason}: {len(tickers)}")
            if len(tickers) <= 10:
                for t in tickers:
                    logger.info(f"    - {t}")
            else:
                for t in tickers[:5]:
                    logger.info(f"    - {t}")
                logger.info(f"    ... and {len(tickers) - 5} more")
        
        return {
            "count_before": count_before,
            "count_after": count_after,
            "stats": stats,
            "failed_tickers": {k: list(v) for k, v in failed_tickers.items()},
        }
        
    finally:
        client.close()


if __name__ == "__main__":
    result = asyncio.run(backfill_5y_metrics())
    print("\n" + "="*70)
    print("RESULT SUMMARY")
    print("="*70)
    print(f"Total rows before: {result['count_before']}")
    print(f"Total rows after: {result['count_after']}")
