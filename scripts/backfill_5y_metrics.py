#!/usr/bin/env python3
"""
5Y Historical Backfill for ticker_key_metrics_daily
====================================================

One-time job to populate historical key metrics data.

Scope:
- Visible tickers only (is_visible=true)
- Historical dates where raw price + fundamentals data exists
- Metrics: P/E TTM, P/S, P/B, EV/EBITDA, EV/Revenue

This enables "vs its 5Y average" comparisons.
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
    safe_float,
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("backfill_5y")

# Backfill parameters
BACKFILL_YEARS = 5
SAMPLE_DATES_PER_YEAR = 12  # Monthly samples to save space


async def backfill_5y_metrics():
    """Backfill 5 years of historical key metrics."""
    
    mongo_url = os.environ.get('MONGO_URL')
    db_name = os.environ.get('DB_NAME', 'test_database')
    
    client = AsyncIOMotorClient(mongo_url)
    db = client[db_name]
    
    today = datetime.now(timezone.utc)
    start_date = today - timedelta(days=365 * BACKFILL_YEARS)
    
    logger.info("="*70)
    logger.info("5Y HISTORICAL BACKFILL")
    logger.info("="*70)
    logger.info(f"Date range: {start_date.strftime('%Y-%m-%d')} to {today.strftime('%Y-%m-%d')}")
    
    try:
        # Count before
        count_before = await db.ticker_key_metrics_daily.count_documents({})
        logger.info(f"Records before backfill: {count_before}")
        
        # Get visible tickers with shares_outstanding
        visible_tickers = await db.tracked_tickers.find(
            {"is_visible": True},
            {"_id": 0, "ticker": 1, "sector": 1, "industry": 1}
        ).to_list(length=None)
        
        logger.info(f"Visible tickers to process: {len(visible_tickers)}")
        
        # Generate sample dates (end of each month for past 5 years)
        sample_dates = []
        current = start_date
        while current < today:
            # Get last day of month
            if current.month == 12:
                next_month = current.replace(year=current.year + 1, month=1, day=1)
            else:
                next_month = current.replace(month=current.month + 1, day=1)
            last_day = next_month - timedelta(days=1)
            sample_dates.append(last_day.strftime("%Y-%m-%d"))
            current = next_month
        
        logger.info(f"Sample dates to backfill: {len(sample_dates)}")
        
        # Stats
        stats = {
            "processed": 0,
            "inserted": 0,
            "skipped_no_fundamentals": 0,
            "skipped_no_shares": 0,
            "skipped_no_price": 0,
            "skipped_exists": 0,
        }
        
        # Process each ticker
        for i, t in enumerate(visible_tickers):
            ticker = t["ticker"]
            symbol = ticker.replace(".US", "").upper()
            sector = t.get("sector")
            industry = t.get("industry")
            
            if i % 500 == 0:
                logger.info(f"Processing {i}/{len(visible_tickers)}...")
            
            # Get fundamentals
            fund = await db.company_fundamentals_cache.find_one({"symbol": symbol})
            if not fund:
                stats["skipped_no_fundamentals"] += 1
                continue
            
            # Extract shares (assumed constant for historical)
            shares = extract_shares_outstanding(fund)
            if not shares or shares <= 0:
                stats["skipped_no_shares"] += 1
                continue
            
            # Get all historical prices for this ticker
            prices = await db.stock_prices.find(
                {"ticker": ticker},
                {"_id": 0, "date": 1, "close": 1}
            ).sort([("date", 1)]).to_list(length=None)
            
            if not prices:
                stats["skipped_no_price"] += 1
                continue
            
            # Create price lookup by date
            price_lookup = {p["date"]: p["close"] for p in prices if p.get("close")}
            
            # Process each sample date
            for sample_date in sample_dates:
                # Check if already exists
                exists = await db.ticker_key_metrics_daily.find_one(
                    {"ticker": ticker, "date": sample_date},
                    {"_id": 1}
                )
                if exists:
                    stats["skipped_exists"] += 1
                    continue
                
                # Find closest price to sample date
                price = price_lookup.get(sample_date)
                if not price:
                    # Try to find closest date within 7 days
                    for offset in range(1, 8):
                        check_date = (datetime.strptime(sample_date, "%Y-%m-%d") - timedelta(days=offset)).strftime("%Y-%m-%d")
                        if check_date in price_lookup:
                            price = price_lookup[check_date]
                            break
                
                if not price or price <= 0:
                    continue
                
                # Compute metrics
                market_cap = price * shares
                pe_ttm = compute_pe_ttm(fund, price)
                ps_ttm = compute_ps_ttm(fund, market_cap)
                pb = compute_pb(fund, market_cap)
                ev_ebitda_ttm = compute_ev_ebitda_ttm(fund, market_cap)
                ev_revenue_ttm = compute_ev_revenue_ttm(fund, market_cap)
                
                # Insert record
                doc = {
                    "ticker": ticker,
                    "date": sample_date,
                    "price": round(price, 2),
                    "market_cap": round(market_cap, 0),
                    "pe_ttm": pe_ttm,
                    "ps_ttm": ps_ttm,
                    "pb": pb,
                    "ev_ebitda_ttm": ev_ebitda_ttm,
                    "ev_revenue_ttm": ev_revenue_ttm,
                    "sector": sector,
                    "industry": industry,
                    "backfilled": True,
                }
                
                await db.ticker_key_metrics_daily.insert_one(doc)
                stats["inserted"] += 1
            
            stats["processed"] += 1
        
        # Count after
        count_after = await db.ticker_key_metrics_daily.count_documents({})
        
        # Create indexes
        await db.ticker_key_metrics_daily.create_index([("ticker", 1), ("date", 1)], unique=True)
        await db.ticker_key_metrics_daily.create_index([("date", 1)])
        await db.ticker_key_metrics_daily.create_index([("industry", 1), ("date", 1)])
        
        logger.info("="*70)
        logger.info("BACKFILL COMPLETE")
        logger.info("="*70)
        logger.info(f"Records before: {count_before}")
        logger.info(f"Records after: {count_after}")
        logger.info(f"New records: {count_after - count_before}")
        logger.info(f"Tickers processed: {stats['processed']}")
        logger.info(f"Records inserted: {stats['inserted']}")
        logger.info(f"Skipped (no fundamentals): {stats['skipped_no_fundamentals']}")
        logger.info(f"Skipped (no shares): {stats['skipped_no_shares']}")
        logger.info(f"Skipped (no price): {stats['skipped_no_price']}")
        logger.info(f"Skipped (already exists): {stats['skipped_exists']}")
        
        # Coverage calculation
        coverage = (stats['processed'] / len(visible_tickers) * 100) if visible_tickers else 0
        logger.info(f"Coverage: {coverage:.1f}%")
        
        return stats
        
    finally:
        client.close()


if __name__ == "__main__":
    asyncio.run(backfill_5y_metrics())
