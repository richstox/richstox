#!/usr/bin/env python3
"""
P31: News Logo Guarantee Test
=============================
DO NOT REMOVE WITHOUT RICHARD APPROVAL (kurtarichard@gmail.com)

This test ensures that every news item in the API response has either:
- logo_url: A valid URL to a company logo
- fallback_logo_key: A single character for fallback badge rendering

NEVER should a news item be returned without one of these fields.
"""

import asyncio
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv
import httpx

load_dotenv()


async def test_news_logo_guarantee_via_api():
    """Test that /api/news returns logo_url or fallback_logo_key for every item."""
    
    api_url = os.environ.get("EXPO_PUBLIC_BACKEND_URL", "http://localhost:8001")
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.get(f"{api_url}/api/news?offset=0&limit=20")
        assert response.status_code == 200, f"API returned {response.status_code}"
        
        data = response.json()
        news_items = data.get("news", [])
        
        print(f"Testing {len(news_items)} news items...")
        
        violations = []
        for item in news_items:
            has_logo = bool(item.get("logo_url"))
            has_fallback = bool(item.get("fallback_logo_key"))
            
            if not has_logo and not has_fallback:
                violations.append({
                    "id": item.get("id"),
                    "ticker": item.get("ticker"),
                    "title": item.get("title", "")[:50],
                    "logo_url": item.get("logo_url"),
                    "fallback_logo_key": item.get("fallback_logo_key"),
                })
        
        if violations:
            print("\n❌ LOGO GUARANTEE VIOLATED!")
            for v in violations:
                print(f"  - {v['ticker']}: {v['title']}...")
            raise AssertionError(f"P31 violated: {len(violations)} items missing logo guarantee")
        
        print("✅ All news items have logo_url or fallback_logo_key")
        
        # Print sample
        if news_items:
            sample = news_items[0]
            print(f"\nSample item:")
            print(f"  ticker: {sample.get('ticker')}")
            print(f"  logo_url: {sample.get('logo_url')}")
            print(f"  fallback_logo_key: {sample.get('fallback_logo_key')}")


def test_logo_guarantee():
    """Pytest entry point."""
    asyncio.run(test_news_logo_guarantee_via_api())


if __name__ == "__main__":
    asyncio.run(test_news_logo_guarantee_via_api())
