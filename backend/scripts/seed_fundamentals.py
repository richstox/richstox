#!/usr/bin/env python3
"""
Seed script to populate company_fundamentals_cache with logo URLs.
This is a one-time script to bootstrap the database with EODHD data.

Usage: python seed_fundamentals.py
"""
import asyncio
import httpx
import os
import sys
from datetime import datetime, timezone

# Load env
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
load_dotenv('/app/backend/.env')

from motor.motor_asyncio import AsyncIOMotorClient

EODHD_API_KEY = os.environ.get('EODHD_API_KEY', '')
EODHD_BASE_URL = "https://eodhd.com/api"

# Top 100 tickers to seed (most popular US stocks)
TOP_TICKERS = [
    "AAPL", "MSFT", "GOOGL", "GOOG", "AMZN", "NVDA", "META", "TSLA", "BRK-B", "UNH",
    "JPM", "JNJ", "V", "PG", "XOM", "HD", "MA", "CVX", "MRK", "ABBV",
    "LLY", "AVGO", "PEP", "KO", "COST", "ADBE", "WMT", "MCD", "CSCO", "TMO",
    "PFE", "CRM", "BAC", "ABT", "ORCL", "ACN", "NFLX", "AMD", "DIS", "NKE",
    "DHR", "TXN", "LIN", "VZ", "WFC", "NEE", "INTC", "PM", "BMY", "UPS",
    "RTX", "QCOM", "SPGI", "HON", "INTU", "IBM", "COP", "LOW", "CAT", "SBUX",
    "T", "GS", "BA", "DE", "ELV", "AMGN", "MS", "AXP", "AMAT", "BLK",
    "GILD", "GE", "MDT", "ISRG", "SYK", "CVS", "BKNG", "LMT", "ADP", "PLD",
    "NOW", "TJX", "MDLZ", "MMC", "REGN", "CI", "ZTS", "ADI", "CB", "VRTX",
    "SO", "MO", "PYPL", "SLB", "DUK", "EOG", "CME", "EQIX", "PGR", "ETN",
]


async def fetch_fundamentals(client: httpx.AsyncClient, ticker: str) -> dict:
    """Fetch fundamentals from EODHD API."""
    # Normalize ticker
    ticker_normalized = ticker.upper().strip()
    ticker_api = f"{ticker_normalized}.US"
    
    url = f"{EODHD_BASE_URL}/fundamentals/{ticker_api}"
    params = {"api_token": EODHD_API_KEY, "fmt": "json"}
    
    try:
        response = await client.get(url, params=params, timeout=30)
        if response.status_code == 404:
            return {"ticker": ticker_api, "not_found": True}
        response.raise_for_status()
        return {"ticker": ticker_api, "data": response.json()}
    except Exception as e:
        return {"ticker": ticker_api, "error": str(e)[:200]}


async def seed_fundamentals():
    """Main seed function."""
    mongo_url = os.environ.get('MONGO_URL', 'mongodb://localhost:27017')
    db_name = os.environ.get('DB_NAME', 'richstox')
    
    client = AsyncIOMotorClient(mongo_url)
    db = client[db_name]
    
    print(f"Seeding {len(TOP_TICKERS)} tickers into company_fundamentals_cache...")
    print(f"MongoDB: {mongo_url}, DB: {db_name}")
    
    stats = {"success": 0, "failed": 0, "skipped": 0}
    
    async with httpx.AsyncClient() as http_client:
        for i, ticker in enumerate(TOP_TICKERS):
            ticker_normalized = f"{ticker.upper()}.US"
            
            # Check if already exists
            existing = await db.company_fundamentals_cache.find_one({"ticker": ticker_normalized})
            if existing and existing.get("logo_url"):
                stats["skipped"] += 1
                if (i + 1) % 20 == 0:
                    print(f"Progress: {i+1}/{len(TOP_TICKERS)} | Success: {stats['success']} | Skipped: {stats['skipped']}")
                continue
            
            result = await fetch_fundamentals(http_client, ticker)
            
            if result.get("error") or result.get("not_found"):
                stats["failed"] += 1
                print(f"  Failed: {ticker} - {result.get('error', 'not found')}")
                continue
            
            data = result.get("data", {})
            general = data.get("General", {})
            highlights = data.get("Highlights", {})
            technicals = data.get("Technicals", {})
            
            if not general:
                stats["failed"] += 1
                continue
            
            # Build document - normalize ticker to uppercase
            doc = {
                "ticker": ticker_normalized,  # Always uppercase with .US
                "code": general.get("Code", "").upper(),
                "name": general.get("Name", ""),
                "exchange": general.get("Exchange", ""),
                "sector": general.get("Sector", ""),
                "industry": general.get("Industry", ""),
                "description": (general.get("Description") or "")[:1000],
                "logo_url": general.get("LogoURL") or None,  # KEY: logo from EODHD
                "web_url": general.get("WebURL", ""),
                "ipo_date": general.get("IPODate", ""),
                "country": general.get("CountryName", "USA"),
                "currency": general.get("CurrencyCode", "USD"),
                "market_cap": highlights.get("MarketCapitalization"),
                "pe_ratio": highlights.get("PERatio"),
                "eps": highlights.get("EarningsShare"),
                "dividend_yield": highlights.get("DividendYield"),
                "beta": technicals.get("Beta"),
                "high_52w": technicals.get("52WeekHigh"),
                "low_52w": technicals.get("52WeekLow"),
                "updated_at": datetime.now(timezone.utc),
            }
            
            # Upsert
            await db.company_fundamentals_cache.update_one(
                {"ticker": ticker_normalized},
                {"$set": doc},
                upsert=True
            )
            
            # Also update/create tracked_tickers entry
            await db.tracked_tickers.update_one(
                {"ticker": ticker_normalized},
                {"$set": {
                    "ticker": ticker_normalized,
                    "code": doc["code"],
                    "name": doc["name"],
                    "exchange": doc["exchange"],
                    "sector": doc["sector"],
                    "industry": doc["industry"],
                    "is_active": True,
                    "status": "active",
                    "updated_at": datetime.now(timezone.utc),
                }},
                upsert=True
            )
            
            stats["success"] += 1
            
            if (i + 1) % 10 == 0:
                print(f"Progress: {i+1}/{len(TOP_TICKERS)} | Success: {stats['success']} | Failed: {stats['failed']}")
            
            # Rate limit protection
            await asyncio.sleep(0.2)
    
    # Final stats
    print(f"\n=== SEED COMPLETE ===")
    print(f"Success: {stats['success']}")
    print(f"Failed: {stats['failed']}")
    print(f"Skipped (already exists): {stats['skipped']}")
    
    # Verify
    count = await db.company_fundamentals_cache.count_documents({})
    with_logo = await db.company_fundamentals_cache.count_documents({"logo_url": {"$ne": None}})
    print(f"\nTotal in cache: {count}")
    print(f"With logo_url: {with_logo}")
    
    # Sample
    sample = await db.company_fundamentals_cache.find_one({"ticker": "AAPL.US"})
    if sample:
        print(f"\nSample AAPL:")
        print(f"  Name: {sample.get('name')}")
        print(f"  Logo URL: {sample.get('logo_url')}")
    
    client.close()


if __name__ == "__main__":
    asyncio.run(seed_fundamentals())
