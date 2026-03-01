"""
P0 Prevention Tests: Hybrid 7 API Contract Tests

These tests prevent regressions in the ticker detail API response.
Run in CI/CD before deployment.

Test Categories:
1. Reality Check consistency (signs must match)
2. Key Metrics reason codes (always present)
3. Peer transparency fields (always populated)
"""

import pytest
import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
import os
import sys

# Add backend to path
sys.path.insert(0, '/app/backend')

from dotenv import load_dotenv
load_dotenv('/app/backend/.env')


# Helper to run async functions in sync tests
def run_async(coro):
    return asyncio.get_event_loop().run_until_complete(coro)


def get_db():
    client = AsyncIOMotorClient(os.environ['MONGO_URL'])
    return client[os.environ.get('DB_NAME', 'test_database')]


class TestRealityCheckConsistency:
    """
    PREVENTION: Reality Check must have consistent signs.
    If total_return_pct is negative, CAGR cannot be positive.
    """
    
    def test_xxii_negative_return_negative_cagr(self):
        """XXII.US has -100% total return, CAGR must be negative."""
        from local_metrics_service import calculate_reality_check_max
        
        db = get_db()
        result = run_async(calculate_reality_check_max(db, "XXII.US"))
        
        assert result is not None, "Reality check should return data for XXII.US"
        
        total_return = result.get('total_return_pct')
        cagr = result.get('cagr_pct')
        
        # CRITICAL: Signs must be consistent
        if total_return is not None and total_return <= -99:
            assert cagr is not None and cagr < 0, \
                f"BUG: Total return is {total_return}% but CAGR is {cagr}%. " \
                f"CAGR cannot be positive when total return is near -100%"
    
    def test_aapl_positive_return_positive_cagr(self):
        """AAPL.US has positive total return, CAGR should be positive."""
        from local_metrics_service import calculate_reality_check_max
        
        db = get_db()
        result = run_async(calculate_reality_check_max(db, "AAPL.US"))
        
        if result:
            total_return = result.get('total_return_pct')
            cagr = result.get('cagr_pct')
            
            if total_return and total_return > 0:
                assert cagr is None or cagr > 0, \
                    f"BUG: Total return is +{total_return}% but CAGR is {cagr}%"


class TestKeyMetricsReasonCodes:
    """
    PREVENTION: Every metric in key_metrics must have:
    - 'value' field (number or null)
    - 'formatted' field (string or null)
    - 'na_reason' field (string or null)
    
    If value is null/None, na_reason MUST be populated.
    """
    
    REQUIRED_METRICS = [
        'market_cap',
        'shares_outstanding', 
        'net_margin_ttm',
        'fcf_yield',
        'net_debt_ebitda',
        'revenue_growth_3y',
        'dividend_yield_ttm'
    ]
    
    VALID_REASON_CODES = [
        'unprofitable',
        'missing_shares',
        'missing_data',
        'missing_cf_data',
        'missing_debt_data',
        'insufficient_history',
        'negative_value',
        'negative_fcf',
        'missing_revenue',
    ]
    
    def test_xxii_metrics_have_reason_codes(self):
        """XXII.US unprofitable metrics must have reason codes."""
        from local_metrics_service import calculate_hybrid_7_metrics
        
        db = get_db()
        
        # Get current price
        price_doc = run_async(db.stock_prices.find_one(
            {"ticker": "XXII.US"},
            sort=[("date", -1)]
        ))
        current_price = price_doc['close'] if price_doc else 5.69
        
        result = run_async(calculate_hybrid_7_metrics(db, "XXII.US", current_price))
        
        assert result is not None, "Hybrid 7 should return data"
        
        for metric_name in self.REQUIRED_METRICS:
            assert metric_name in result, f"Missing metric: {metric_name}"
            
            metric = result[metric_name]
            assert 'value' in metric, f"{metric_name} missing 'value' field"
            assert 'formatted' in metric, f"{metric_name} missing 'formatted' field"
            assert 'na_reason' in metric, f"{metric_name} missing 'na_reason' field"
            
            # If value is None, na_reason MUST be populated
            if metric['value'] is None:
                assert metric['na_reason'] is not None, \
                    f"BUG: {metric_name} has null value but no reason code"
                assert metric['na_reason'] in self.VALID_REASON_CODES, \
                    f"BUG: {metric_name} has invalid reason code: {metric['na_reason']}"
    
    def test_net_debt_ebitda_unprofitable_reason(self):
        """XXII.US Net Debt/EBITDA should be N/A (unprofitable)."""
        from local_metrics_service import calculate_hybrid_7_metrics
        
        db = get_db()
        
        price_doc = run_async(db.stock_prices.find_one(
            {"ticker": "XXII.US"},
            sort=[("date", -1)]
        ))
        current_price = price_doc['close'] if price_doc else 5.69
        
        result = run_async(calculate_hybrid_7_metrics(db, "XXII.US", current_price))
        
        net_debt = result.get('net_debt_ebitda', {})
        
        # XXII is unprofitable, so this should be N/A
        if net_debt.get('value') is None:
            assert net_debt.get('na_reason') == 'unprofitable', \
                f"Expected 'unprofitable', got: {net_debt.get('na_reason')}"


class TestPeerTransparency:
    """
    PREVENTION: peer_transparency must always be populated with:
    - total_industry_peers (>= 0)
    - valid_metric_peers (dict)
    - industry (string or null)
    - group_type (string or null)
    """
    
    def test_xxii_peer_transparency_populated(self):
        """XXII.US must have peer transparency data."""
        from local_metrics_service import get_peer_transparency
        
        db = get_db()
        result = run_async(get_peer_transparency(db, "XXII.US"))
        
        assert result is not None, "Peer transparency should return data"
        assert 'total_industry_peers' in result, "Missing total_industry_peers"
        assert 'valid_metric_peers' in result, "Missing valid_metric_peers"
        assert 'industry' in result, "Missing industry"
        assert 'group_type' in result, "Missing group_type"
        
        # CRITICAL: If ticker has industry, peers count should be > 0
        if result.get('industry'):
            assert result['total_industry_peers'] > 0, \
                f"BUG: Industry is '{result['industry']}' but peer count is 0"
    
    def test_tobacco_industry_has_peers(self):
        """Tobacco industry must have multiple peers."""
        from local_metrics_service import get_peer_transparency
        
        db = get_db()
        result = run_async(get_peer_transparency(db, "XXII.US"))
        
        assert result.get('industry') == 'Tobacco', \
            f"Expected 'Tobacco', got: {result.get('industry')}"
        
        # Tobacco should have at least 5 peers
        assert result['total_industry_peers'] >= 5, \
            f"BUG: Tobacco industry should have peers, got: {result['total_industry_peers']}"


class TestValuationMetrics:
    """
    PREVENTION: Valuation Overview must return all 5 metrics,
    even if they are N/A.
    """
    
    REQUIRED_VALUATION_METRICS = ['pe', 'ps', 'pb', 'ev_ebitda', 'ev_revenue']
    
    def test_xxii_has_all_valuation_metrics(self):
        """XXII.US valuation must include all 5 metrics."""
        from local_metrics_service import get_valuation_overview
        
        db = get_db()
        
        price_doc = run_async(db.stock_prices.find_one(
            {"ticker": "XXII.US"},
            sort=[("date", -1)]
        ))
        current_price = price_doc['close'] if price_doc else 5.69
        
        result = run_async(get_valuation_overview(db, "XXII.US", current_price))
        
        assert result is not None, "Valuation should return data"
        
        metrics = result.get('metrics', {})
        
        for metric_name in self.REQUIRED_VALUATION_METRICS:
            assert metric_name in metrics, \
                f"BUG: Missing valuation metric: {metric_name}. " \
                f"All 5 metrics must be present, even if N/A."


class TestValuation5YHistory:
    """
    P2 PREVENTION: API must ALWAYS return vs_5y_avg structure.
    Never hide it when history is insufficient.
    """
    
    def test_xxii_has_history_5y_block(self):
        """XXII.US must have history_5y block in response."""
        from local_metrics_service import get_valuation_overview
        
        db = get_db()
        
        price_doc = run_async(db.stock_prices.find_one(
            {"ticker": "XXII.US"},
            sort=[("date", -1)]
        ))
        current_price = price_doc['close'] if price_doc else 5.69
        
        result = run_async(get_valuation_overview(db, "XXII.US", current_price))
        
        assert result is not None, "Valuation should return data"
        assert 'history_5y' in result, "BUG: history_5y block is missing from API response"
        
        h5y = result['history_5y']
        assert 'available' in h5y, "history_5y must have 'available' field"
        assert 'overall' in h5y, "history_5y must have 'overall' field"
        assert 'na_reason' in h5y, "history_5y must have 'na_reason' field"
    
    def test_metrics_have_avg_5y_field(self):
        """Every metric must have avg_5y field (even if null)."""
        from local_metrics_service import get_valuation_overview
        
        db = get_db()
        
        price_doc = run_async(db.stock_prices.find_one(
            {"ticker": "XXII.US"},
            sort=[("date", -1)]
        ))
        current_price = price_doc['close'] if price_doc else 5.69
        
        result = run_async(get_valuation_overview(db, "XXII.US", current_price))
        
        for metric_name, metric in result['metrics'].items():
            assert 'avg_5y' in metric, f"BUG: {metric_name} missing 'avg_5y' field"
            assert 'vs_5y' in metric, f"BUG: {metric_name} missing 'vs_5y' field"
    
    def test_na_reason_when_history_unavailable(self):
        """When history is not available, na_reason must be set."""
        from local_metrics_service import get_valuation_overview
        
        db = get_db()
        
        price_doc = run_async(db.stock_prices.find_one(
            {"ticker": "XXII.US"},
            sort=[("date", -1)]
        ))
        current_price = price_doc['close'] if price_doc else 5.69
        
        result = run_async(get_valuation_overview(db, "XXII.US", current_price))
        
        h5y = result['history_5y']
        
        # If not available, must have na_reason
        if not h5y['available']:
            assert h5y['na_reason'] is not None, \
                "BUG: history_5y.available=False but na_reason is None"


if __name__ == '__main__':
    pytest.main([__file__, '-v', '--tb=short'])
