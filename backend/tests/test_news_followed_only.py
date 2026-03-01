#!/usr/bin/env python3
"""
P32.1: Homepage News Followed-Only + Visible-Only Test
=======================================================
DO NOT REMOVE WITHOUT RICHARD APPROVAL (kurtarichard@gmail.com)

This test ensures that homepage news ONLY shows tickers that are:
1. In user's followed/portfolio (positions.ticker)
2. AND have is_visible=true in tracked_tickers

NO fallback to random/default tickers allowed.
"""

import asyncio
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv
import httpx

load_dotenv()


async def test_news_followed_and_visible():
    """Test that /api/news returns ONLY followed + visible tickers."""
    
    api_url = os.environ.get("EXPO_PUBLIC_BACKEND_URL", "http://localhost:8001")
    mongo_url = os.environ.get("MONGO_URL")
    db_name = os.environ.get("DB_NAME")
    
    client = AsyncIOMotorClient(mongo_url)
    db = client[db_name]
    
    # Get followed tickers from positions
    followed_raw = await db.positions.distinct("ticker")
    followed_tickers = {t.replace(".US", "").upper() for t in followed_raw if t}
    print(f"Followed tickers (from positions): {sorted(followed_tickers)}")
    
    # Get visible tickers
    followed_full = [f"{t}.US" for t in followed_tickers]
    visible_docs = await db.tracked_tickers.find(
        {"ticker": {"$in": followed_full}, "is_visible": True},
        {"_id": 0, "ticker": 1}
    ).to_list(length=None)
    visible_and_followed = {doc["ticker"].replace(".US", "") for doc in visible_docs}
    print(f"Visible + Followed tickers: {sorted(visible_and_followed)}")
    
    # Check for any invisible followed tickers
    invisible_followed = followed_tickers - visible_and_followed
    if invisible_followed:
        print(f"WARNING: These followed tickers are NOT visible: {invisible_followed}")
    
    client.close()
    
    # Call API (without tickers param = should use followed+visible only)
    async with httpx.AsyncClient(timeout=30.0) as http_client:
        response = await http_client.get(f"{api_url}/api/news?offset=0&limit=50")
        assert response.status_code == 200, f"API returned {response.status_code}"
        
        data = response.json()
        news_items = data.get("news", [])
        api_followed = set(data.get("followed_tickers", []))
        
        print(f"\nAPI returned followed_tickers: {sorted(api_followed)}")
        print(f"News items: {len(news_items)}")
        
        # Verify API used correct followed+visible list
        assert api_followed == visible_and_followed, f"API used wrong tickers: {api_followed} vs {visible_and_followed}"
        
        # Verify all news items are for followed+visible tickers
        violations = []
        for item in news_items:
            ticker = item.get("ticker", "").replace(".US", "").upper()
            if ticker and ticker not in visible_and_followed:
                violations.append({
                    "ticker": ticker,
                    "title": item.get("title", "")[:50],
                })
        
        if violations:
            print("\n❌ FOLLOWED+VISIBLE GUARANTEE VIOLATED!")
            for v in violations:
                print(f"  - {v['ticker']}: {v['title']}...")
            raise AssertionError(f"P32.1 violated: {len(violations)} items from non-followed or invisible tickers")
        
        print("\n✅ All news items are from followed + visible tickers")
        print("✅ Both conditions pass: followed-only AND visible-only")


def test_followed_and_visible():
    """Pytest entry point."""
    asyncio.run(test_news_followed_and_visible())


if __name__ == "__main__":
    asyncio.run(test_news_followed_and_visible())
