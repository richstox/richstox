"""
P8 Regression Test: Financials Data Pipeline

Tests:
1. AAPL financials block must NOT be empty
2. Microcap with no financials should show empty-state gracefully

Run: cd /app/backend && python -m pytest tests/test_financials_pipeline.py -v
"""

import pytest
import httpx
import asyncio


class TestFinancialsDataPipeline:
    """
    P8 CRITICAL: Ensure financials data is properly served via the mobile detail API.
    """
    
    @pytest.fixture
    def api_base(self):
        return "https://ticker-detail-v2.preview.emergentagent.com"
    
    @pytest.mark.asyncio
    async def test_aapl_financials_not_empty(self, api_base):
        """
        CRITICAL: AAPL.US must have financials data in the API response.
        
        Expected: financials block with annual, quarterly arrays and ttm object.
        Fails if: financials is None or empty.
        """
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{api_base}/api/v1/ticker/AAPL.US/detail")
            assert response.status_code == 200
            
            data = response.json()
            
            # Financials must exist
            assert data.get("financials") is not None, \
                "AAPL.US financials block must NOT be None"
            
            financials = data["financials"]
            
            # Must have annual and quarterly data
            assert "annual" in financials, "Missing annual financials"
            assert "quarterly" in financials, "Missing quarterly financials"
            assert "ttm" in financials, "Missing TTM financials"
            
            # Annual must have data
            assert len(financials["annual"]) > 0, \
                "AAPL.US must have annual financials data"
            
            # Quarterly must have data
            assert len(financials["quarterly"]) > 0, \
                "AAPL.US must have quarterly financials data"
    
    @pytest.mark.asyncio
    async def test_aapl_ttm_has_5_essential_metrics(self, api_base):
        """
        P8 Requirement: TTM must include 5 essential metrics.
        - Revenue, Net Income, Free Cash Flow, Cash, Total Debt
        """
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{api_base}/api/v1/ticker/AAPL.US/detail")
            data = response.json()
            
            ttm = data.get("financials", {}).get("ttm", {})
            
            # All 5 essential metrics must be present
            assert "revenue" in ttm, "TTM missing revenue"
            assert "net_income" in ttm, "TTM missing net_income"
            assert "free_cash_flow" in ttm, "TTM missing free_cash_flow"
            assert "cash" in ttm, "TTM missing cash"
            assert "total_debt" in ttm, "TTM missing total_debt"
            
            # AAPL should have actual values (not all None)
            assert ttm["revenue"] is not None, "AAPL TTM revenue should not be None"
            assert ttm["net_income"] is not None, "AAPL TTM net_income should not be None"
    
    @pytest.mark.asyncio
    async def test_aapl_financials_has_correct_structure(self, api_base):
        """
        Each annual/quarterly entry must have period_date, revenue, net_income.
        """
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{api_base}/api/v1/ticker/AAPL.US/detail")
            data = response.json()
            
            annual = data.get("financials", {}).get("annual", [])
            
            if annual:
                first_entry = annual[0]
                assert "period_date" in first_entry, "Annual entry missing period_date"
                assert "revenue" in first_entry, "Annual entry missing revenue"
                assert "net_income" in first_entry, "Annual entry missing net_income"


class TestEmptyFinancialsState:
    """
    Test that tickers with no financials return proper empty state.
    """
    
    @pytest.fixture
    def api_base(self):
        return "https://ticker-detail-v2.preview.emergentagent.com"
    
    @pytest.mark.asyncio
    async def test_xxii_financials_structure(self, api_base):
        """
        XXII.US (small company) should still return proper financials structure.
        Even if data is limited, the structure should be consistent.
        """
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{api_base}/api/v1/ticker/XXII.US/detail")
            
            if response.status_code == 200:
                data = response.json()
                financials = data.get("financials")
                
                # Structure should exist (may be None if no data)
                # If it exists, should have proper structure
                if financials is not None:
                    assert "annual" in financials or "quarterly" in financials, \
                        "Financials should have annual or quarterly key"
    
    @pytest.mark.asyncio
    async def test_api_returns_200_even_without_financials(self, api_base):
        """
        API should return 200 OK even for tickers that may lack financials.
        The financials field can be None, but API shouldn't fail.
        """
        async with httpx.AsyncClient() as client:
            # Test with a known ticker
            response = await client.get(f"{api_base}/api/v1/ticker/XXII.US/detail")
            assert response.status_code == 200, \
                "API should return 200 even if financials are sparse"


if __name__ == '__main__':
    pytest.main([__file__, '-v', '--tb=short'])
