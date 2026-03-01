#!/usr/bin/env python3
"""
VISIBILITY GUARD TESTS
======================

Automated tests that prevent visibility rule regressions:
1. No endpoint should return a ticker where is_visible=false
2. AAPL/MSFT/GOOGL must be accessible when is_visible=true

Run: cd /app/backend && python -m pytest tests/test_visibility_guard.py -v
"""

import os
import sys
import asyncio
import pytest

sys.path.insert(0, '/app/backend')

from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv
load_dotenv('/app/backend/.env')


# Test configuration
MEGA_CAP_TICKERS = ["AAPL.US", "MSFT.US", "GOOGL.US", "AMZN.US", "NVDA.US"]
API_BASE_URL = "http://localhost:8001/api"


@pytest.fixture
async def db():
    """Get database connection."""
    client = AsyncIOMotorClient(os.environ.get('MONGO_URL'))
    database = client[os.environ.get('DB_NAME', 'test_database')]
    yield database
    client.close()


class TestVisibilityGuard:
    """Test suite for visibility rule enforcement."""
    
    @pytest.mark.asyncio
    async def test_mega_caps_are_visible(self, db):
        """AAPL, MSFT, GOOGL must be is_visible=true."""
        for ticker in MEGA_CAP_TICKERS:
            doc = await db.tracked_tickers.find_one(
                {"ticker": ticker},
                {"_id": 0, "ticker": 1, "is_visible": 1}
            )
            assert doc is not None, f"{ticker} not found in tracked_tickers"
            assert doc.get("is_visible") == True, f"{ticker} has is_visible={doc.get('is_visible')}, expected True"
        
        print(f"✅ All {len(MEGA_CAP_TICKERS)} mega-cap tickers are visible")
    
    @pytest.mark.asyncio
    async def test_no_invisible_tickers_in_search(self, db):
        """Search API should only return is_visible=true tickers."""
        import httpx
        
        async with httpx.AsyncClient() as client:
            # Search for "A" which should return many results
            response = await client.get(f"{API_BASE_URL}/whitelist/search", params={"q": "A", "limit": 100})
            assert response.status_code == 200
            data = response.json()
            
            tickers = data.get("results", [])
            
            # Verify each ticker in results is visible
            for t in tickers:
                ticker = t.get("ticker")
                if ticker:
                    full_ticker = ticker if ticker.endswith(".US") else f"{ticker}.US"
                    doc = await db.tracked_tickers.find_one(
                        {"ticker": full_ticker},
                        {"_id": 0, "ticker": 1, "is_visible": 1}
                    )
                    # Doc might not exist for all returned tickers (legacy data)
                    if doc:
                        assert doc.get("is_visible") == True, f"Search returned invisible ticker: {ticker}"
        
        print(f"✅ Search API returned only visible tickers ({len(tickers)} checked)")
    
    @pytest.mark.asyncio
    async def test_stock_overview_blocks_invisible(self, db):
        """stock-overview endpoint should return 404 for invisible tickers."""
        import httpx
        
        # Find an invisible ticker
        invisible = await db.tracked_tickers.find_one(
            {"is_visible": False},
            {"_id": 0, "ticker": 1}
        )
        
        if invisible:
            ticker = invisible["ticker"]
            async with httpx.AsyncClient() as client:
                response = await client.get(f"{API_BASE_URL}/stock-overview/{ticker}")
                assert response.status_code == 404, f"Expected 404 for invisible ticker {ticker}, got {response.status_code}"
            print(f"✅ stock-overview correctly blocks invisible ticker: {ticker}")
        else:
            print("⚠️ No invisible tickers found to test (this is OK)")
    
    @pytest.mark.asyncio
    async def test_mega_caps_accessible_via_api(self, db):
        """AAPL, MSFT, GOOGL must be accessible via stock-overview API."""
        import httpx
        
        async with httpx.AsyncClient() as client:
            for ticker in MEGA_CAP_TICKERS[:3]:  # Test first 3
                response = await client.get(f"{API_BASE_URL}/stock-overview/{ticker}")
                assert response.status_code == 200, f"Expected 200 for {ticker}, got {response.status_code}"
                data = response.json()
                assert "ticker" in data, f"Response for {ticker} missing 'ticker' field"
        
        print(f"✅ Mega-cap tickers accessible via stock-overview API")
    
    @pytest.mark.asyncio
    async def test_ticker_detail_blocks_invisible(self, db):
        """v1/ticker/{ticker}/detail should return 404 for invisible tickers."""
        import httpx
        
        # Find an invisible ticker
        invisible = await db.tracked_tickers.find_one(
            {"is_visible": False},
            {"_id": 0, "ticker": 1}
        )
        
        if invisible:
            ticker = invisible["ticker"]
            async with httpx.AsyncClient() as client:
                response = await client.get(f"{API_BASE_URL}/v1/ticker/{ticker}/detail")
                assert response.status_code == 404, f"Expected 404 for invisible ticker {ticker}, got {response.status_code}"
            print(f"✅ ticker detail correctly blocks invisible ticker: {ticker}")
        else:
            print("⚠️ No invisible tickers found to test")
    
    @pytest.mark.asyncio
    async def test_visible_universe_query_constant_exists(self, db):
        """Verify VISIBLE_UNIVERSE_QUERY constant is defined in server.py."""
        import server
        
        assert hasattr(server, 'VISIBLE_UNIVERSE_QUERY'), "VISIBLE_UNIVERSE_QUERY not defined in server.py"
        assert server.VISIBLE_UNIVERSE_QUERY == {"is_visible": True}, f"VISIBLE_UNIVERSE_QUERY is {server.VISIBLE_UNIVERSE_QUERY}, expected {{'is_visible': True}}"
        
        print("✅ VISIBLE_UNIVERSE_QUERY constant correctly defined")


# Run tests
if __name__ == "__main__":
    async def run_all():
        """Run all tests."""
        client = AsyncIOMotorClient(os.environ.get('MONGO_URL'))
        database = client[os.environ.get('DB_NAME', 'test_database')]
        
        tests = TestVisibilityGuard()
        
        print("="*70)
        print("VISIBILITY GUARD TEST SUITE")
        print("="*70)
        
        try:
            await tests.test_mega_caps_are_visible(database)
            await tests.test_no_invisible_tickers_in_search(database)
            await tests.test_stock_overview_blocks_invisible(database)
            await tests.test_mega_caps_accessible_via_api(database)
            await tests.test_ticker_detail_blocks_invisible(database)
            await tests.test_visible_universe_query_constant_exists(database)
            
            print("\n" + "="*70)
            print("ALL TESTS PASSED ✅")
            print("="*70)
            return 0
        except AssertionError as e:
            print(f"\n❌ TEST FAILED: {e}")
            return 1
        finally:
            client.close()
    
    exit_code = asyncio.run(run_all())
    sys.exit(exit_code)
