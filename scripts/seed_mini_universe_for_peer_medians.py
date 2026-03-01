#!/usr/bin/env python3
"""
Mini-Universe Seed Script for Peer Medians Testing
===================================================

This script creates a reproducible test environment for validating
Job A (Key Metrics) + Job B (Peer Medians) using real tickers.

NO FAKE DATA - uses same raw ingestion logic as production.

Usage:
    python scripts/seed_mini_universe_for_peer_medians.py

Steps:
1. Insert mini-universe tickers into tracked_tickers (is_visible=true)
2. Fetch fundamentals + prices using existing EODHD ingestion
3. Run Job A (compute_daily_key_metrics)
4. Run Job B (compute_daily_peer_medians)
5. Print results for validation
"""

import os
import sys
import asyncio
import logging
from datetime import datetime, timezone, timedelta

# Add backend to path
sys.path.insert(0, '/app/backend')

from motor.motor_asyncio import AsyncIOMotorClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("seed_mini_universe")

# Mini-universe: AAPL + Consumer Electronics peers + other Tech tickers
# All are real US tickers available in EODHD
MINI_UNIVERSE = [
    # AAPL and Consumer Electronics industry peers
    {"ticker": "AAPL.US", "symbol": "AAPL", "name": "Apple Inc", "sector": "Technology", "industry": "Consumer Electronics"},
    {"ticker": "SNE.US", "symbol": "SNE", "name": "Sony Group Corporation", "sector": "Technology", "industry": "Consumer Electronics"},
    {"ticker": "LPL.US", "symbol": "LPL", "name": "LG Display", "sector": "Technology", "industry": "Consumer Electronics"},
    {"ticker": "SONO.US", "symbol": "SONO", "name": "Sonos Inc", "sector": "Technology", "industry": "Consumer Electronics"},
    {"ticker": "GPRO.US", "symbol": "GPRO", "name": "GoPro Inc", "sector": "Technology", "industry": "Consumer Electronics"},
    {"ticker": "HEAR.US", "symbol": "HEAR", "name": "Turtle Beach", "sector": "Technology", "industry": "Consumer Electronics"},
    {"ticker": "KOSS.US", "symbol": "KOSS", "name": "Koss Corporation", "sector": "Technology", "industry": "Consumer Electronics"},
    {"ticker": "VUZI.US", "symbol": "VUZI", "name": "Vuzix Corporation", "sector": "Technology", "industry": "Consumer Electronics"},
    
    # Additional Technology sector peers (for fallback)
    {"ticker": "MSFT.US", "symbol": "MSFT", "name": "Microsoft", "sector": "Technology", "industry": "Software-Infrastructure"},
    {"ticker": "GOOGL.US", "symbol": "GOOGL", "name": "Alphabet", "sector": "Technology", "industry": "Internet Content"},
    {"ticker": "NVDA.US", "symbol": "NVDA", "name": "NVIDIA", "sector": "Technology", "industry": "Semiconductors"},
    {"ticker": "AMD.US", "symbol": "AMD", "name": "AMD", "sector": "Technology", "industry": "Semiconductors"},
    {"ticker": "INTC.US", "symbol": "INTC", "name": "Intel", "sector": "Technology", "industry": "Semiconductors"},
    {"ticker": "TSM.US", "symbol": "TSM", "name": "TSMC", "sector": "Technology", "industry": "Semiconductors"},
    {"ticker": "QCOM.US", "symbol": "QCOM", "name": "Qualcomm", "sector": "Technology", "industry": "Semiconductors"},
    {"ticker": "AVGO.US", "symbol": "AVGO", "name": "Broadcom", "sector": "Technology", "industry": "Semiconductors"},
    {"ticker": "CRM.US", "symbol": "CRM", "name": "Salesforce", "sector": "Technology", "industry": "Software-Application"},
    {"ticker": "ORCL.US", "symbol": "ORCL", "name": "Oracle", "sector": "Technology", "industry": "Software-Infrastructure"},
    {"ticker": "CSCO.US", "symbol": "CSCO", "name": "Cisco", "sector": "Technology", "industry": "Communication Equipment"},
    {"ticker": "IBM.US", "symbol": "IBM", "name": "IBM", "sector": "Technology", "industry": "Information Technology"},
]

EODHD_API_KEY = os.environ.get("EODHD_API_KEY", "")


async def seed_tracked_tickers(db):
    """Insert mini-universe tickers into tracked_tickers."""
    logger.info("Step 1: Seeding tracked_tickers...")
    
    for ticker_data in MINI_UNIVERSE:
        await db.tracked_tickers.update_one(
            {"ticker": ticker_data["ticker"]},
            {"$set": {
                **ticker_data,
                "is_visible": True,
                "asset_type": "Common Stock",
                "exchange": "US",
                "seeded_at": datetime.now(timezone.utc).isoformat()
            }},
            upsert=True
        )
    
    count = await db.tracked_tickers.count_documents({"is_visible": True})
    logger.info(f"  Seeded {len(MINI_UNIVERSE)} tickers, total visible: {count}")
    return count


async def fetch_fundamentals_for_ticker(db, ticker: str, symbol: str):
    """Fetch fundamentals from EODHD and store in company_fundamentals_cache."""
    import httpx
    
    url = f"https://eodhd.com/api/fundamentals/{symbol}.US?api_token={EODHD_API_KEY}&fmt=json"
    
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(url)
            response.raise_for_status()
            data = response.json()
        
        if not data or "General" not in data:
            return False
        
        # Extract key fields
        general = data.get("General", {})
        financials = data.get("Financials", {})
        highlights = data.get("Highlights", {})
        
        # Build fundamentals document
        fundamentals_doc = {
            "symbol": symbol,
            "ticker": ticker,
            "name": general.get("Name"),
            "sector": general.get("Sector"),
            "industry": general.get("Industry"),
            "shares_outstanding": data.get("SharesStats", {}).get("SharesOutstanding") or highlights.get("SharesOutstanding"),
            "income_statement_quarterly": financials.get("Income_Statement", {}).get("quarterly", {}),
            "balance_sheet_quarterly": financials.get("Balance_Sheet", {}).get("quarterly", {}),
            "cash_flow_quarterly": financials.get("Cash_Flow", {}).get("quarterly", {}),
            "fetched_at": datetime.now(timezone.utc).isoformat()
        }
        
        await db.company_fundamentals_cache.update_one(
            {"symbol": symbol},
            {"$set": fundamentals_doc},
            upsert=True
        )
        
        return True
        
    except Exception as e:
        logger.error(f"  Error fetching fundamentals for {symbol}: {e}")
        return False


async def fetch_prices_for_ticker(db, ticker: str, symbol: str):
    """Fetch price history from EODHD and store in stock_prices."""
    import httpx
    
    # Fetch last 30 days of prices
    start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    url = f"https://eodhd.com/api/eod/{symbol}.US?api_token={EODHD_API_KEY}&fmt=json&from={start_date}"
    
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(url)
            response.raise_for_status()
            data = response.json()
        
        if not data:
            return 0
        
        count = 0
        for row in data:
            await db.stock_prices.update_one(
                {"ticker": ticker, "date": row["date"]},
                {"$set": {
                    "ticker": ticker,
                    "date": row["date"],
                    "open": row.get("open"),
                    "high": row.get("high"),
                    "low": row.get("low"),
                    "close": row.get("close"),
                    "adjusted_close": row.get("adjusted_close"),
                    "volume": row.get("volume"),
                }},
                upsert=True
            )
            count += 1
        
        return count
        
    except Exception as e:
        logger.error(f"  Error fetching prices for {symbol}: {e}")
        return 0


async def fetch_all_raw_data(db):
    """Fetch fundamentals + prices for all mini-universe tickers."""
    logger.info("Step 2: Fetching raw data from EODHD...")
    
    fundamentals_ok = 0
    prices_total = 0
    
    for ticker_data in MINI_UNIVERSE:
        ticker = ticker_data["ticker"]
        symbol = ticker_data["symbol"]
        
        # Fetch fundamentals
        if await fetch_fundamentals_for_ticker(db, ticker, symbol):
            fundamentals_ok += 1
        
        # Fetch prices
        prices = await fetch_prices_for_ticker(db, ticker, symbol)
        prices_total += prices
        
        logger.info(f"  {symbol}: fundamentals={'OK' if fundamentals_ok else 'FAIL'}, prices={prices}")
    
    logger.info(f"  Total: {fundamentals_ok}/{len(MINI_UNIVERSE)} fundamentals, {prices_total} price records")
    return fundamentals_ok, prices_total


async def run_job_a(db):
    """Run Job A: Compute daily key metrics."""
    logger.info("Step 3: Running Job A (Key Metrics)...")
    
    from key_metrics_service import compute_daily_key_metrics
    result = await compute_daily_key_metrics(db)
    
    logger.info(f"  Job A result: {result}")
    return result


async def run_job_b(db):
    """Run Job B: Compute peer medians."""
    logger.info("Step 4: Running Job B (Peer Medians)...")
    
    from key_metrics_service import compute_daily_peer_medians
    result = await compute_daily_peer_medians(db)
    
    logger.info(f"  Job B result: {result}")
    return result


async def print_results(db):
    """Print validation results."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    
    print("\n" + "="*70)
    print("VALIDATION RESULTS")
    print("="*70)
    
    # 1. Peer list tickers used
    print("\n1. PEER TICKERS (Consumer Electronics industry):")
    industry_tickers = await db.ticker_key_metrics_daily.find(
        {"date": today, "industry": "Consumer Electronics"},
        {"_id": 0, "ticker": 1, "market_cap": 1}
    ).to_list(length=100)
    
    for t in industry_tickers:
        mc = t.get("market_cap", 0)
        mc_str = f"${mc/1e9:.1f}B" if mc >= 1e9 else f"${mc/1e6:.0f}M" if mc else "N/A"
        print(f"  - {t['ticker']}: Market Cap {mc_str}")
    
    # 2. Peer medians
    print("\n2. PEER MEDIANS (Consumer Electronics):")
    metrics = ["pe_ttm", "ps_ttm", "pb", "ev_ebitda_ttm", "ev_revenue_ttm"]
    
    for metric in metrics:
        doc = await db.peer_medians_daily.find_one(
            {"date": today, "group_type": "industry", "group_name": "Consumer Electronics", "metric": metric},
            {"_id": 0}
        )
        if doc:
            print(f"  - {metric}: median={doc['median_value']}, peer_count={doc['peer_count']}")
        else:
            print(f"  - {metric}: NO DATA")
    
    # 3. AAPL specific metrics
    print("\n3. AAPL KEY METRICS:")
    aapl = await db.ticker_key_metrics_daily.find_one(
        {"date": today, "ticker": "AAPL.US"},
        {"_id": 0}
    )
    if aapl:
        print(f"  - Price: ${aapl.get('price', 'N/A')}")
        print(f"  - Market Cap: ${aapl.get('market_cap', 0)/1e12:.2f}T")
        print(f"  - P/E: {aapl.get('pe_ttm', 'N/A')}")
        print(f"  - P/S: {aapl.get('ps_ttm', 'N/A')}")
        print(f"  - P/B: {aapl.get('pb', 'N/A')}")
        print(f"  - EV/EBITDA: {aapl.get('ev_ebitda_ttm', 'N/A')}")
        print(f"  - EV/Revenue: {aapl.get('ev_revenue_ttm', 'N/A')}")
    else:
        print("  NO AAPL DATA")
    
    # 4. Sector fallback medians
    print("\n4. SECTOR MEDIANS (Technology fallback):")
    for metric in metrics:
        doc = await db.peer_medians_daily.find_one(
            {"date": today, "group_type": "sector", "group_name": "Technology", "metric": metric},
            {"_id": 0}
        )
        if doc:
            print(f"  - {metric}: median={doc['median_value']}, peer_count={doc['peer_count']}")
        else:
            print(f"  - {metric}: NO DATA")
    
    print("\n" + "="*70)


async def main():
    """Main entry point."""
    if not EODHD_API_KEY:
        logger.error("EODHD_API_KEY not set!")
        return 1
    
    client = AsyncIOMotorClient(os.environ.get('MONGO_URL', 'mongodb://localhost:27017'))
    db_name = os.environ.get('DB_NAME', 'test_database')
    db = client[db_name]
    
    logger.info(f"Using database: {db_name}")
    
    try:
        # Step 1: Seed tickers
        await seed_tracked_tickers(db)
        
        # Step 2: Fetch raw data
        await fetch_all_raw_data(db)
        
        # Step 3: Run Job A
        await run_job_a(db)
        
        # Step 4: Run Job B
        await run_job_b(db)
        
        # Step 5: Print results
        await print_results(db)
        
        return 0
        
    finally:
        client.close()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
