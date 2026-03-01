"""
Test Stock Detail Page Fixes - Backend API Tests
=================================================
Tests for:
1. gradient_colors with 'direction' field
2. Benchmark endpoint returning pe_ratio_median
3. Stock overview endpoint structure
"""
import pytest
import requests
import os

BASE_URL = os.environ.get('EXPO_PUBLIC_BACKEND_URL', 'https://ticker-detail-v2.preview.emergentagent.com')


class TestStockOverviewAPI:
    """Test /api/stock-overview/{ticker} endpoint"""
    
    def test_aapl_overview_returns_200(self):
        """Stock overview returns 200 for AAPL"""
        response = requests.get(f"{BASE_URL}/api/stock-overview/AAPL")
        assert response.status_code == 200, f"Expected 200, got {response.status_code}"
        print("PASS: /api/stock-overview/AAPL returns 200")
    
    def test_gradient_colors_has_direction_field(self):
        """gradient_colors should include 'direction' field"""
        response = requests.get(f"{BASE_URL}/api/stock-overview/AAPL")
        assert response.status_code == 200
        data = response.json()
        
        # Check gradient_colors exists
        assert "gradient_colors" in data, "Response missing gradient_colors"
        gradient_colors = data["gradient_colors"]
        
        # Check at least one metric has direction field
        assert len(gradient_colors) > 0, "gradient_colors is empty"
        
        # Check pe_ratio has direction field
        assert "pe_ratio" in gradient_colors, "gradient_colors missing pe_ratio"
        pe_ratio_colors = gradient_colors["pe_ratio"]
        assert "direction" in pe_ratio_colors, "pe_ratio gradient missing 'direction' field"
        
        # Direction should be 'above', 'below', or 'in_line'
        direction = pe_ratio_colors["direction"]
        assert direction in ["above", "below", "in_line"], f"Invalid direction: {direction}"
        
        print(f"PASS: gradient_colors.pe_ratio.direction = '{direction}'")
    
    def test_all_metrics_have_direction(self):
        """All metrics in gradient_colors should have direction field"""
        response = requests.get(f"{BASE_URL}/api/stock-overview/AAPL")
        data = response.json()
        gradient_colors = data.get("gradient_colors", {})
        
        metrics_with_direction = []
        metrics_without_direction = []
        
        for metric_name, metric_data in gradient_colors.items():
            if isinstance(metric_data, dict):
                if "direction" in metric_data:
                    metrics_with_direction.append(metric_name)
                else:
                    metrics_without_direction.append(metric_name)
        
        print(f"Metrics with direction: {metrics_with_direction}")
        if metrics_without_direction:
            print(f"Metrics WITHOUT direction: {metrics_without_direction}")
        
        # All should have direction
        assert len(metrics_without_direction) == 0, f"Missing direction in: {metrics_without_direction}"
        print("PASS: All gradient_colors metrics have 'direction' field")
    
    def test_key_metrics_has_pe_benchmark(self):
        """key_metrics should include pe_benchmark"""
        response = requests.get(f"{BASE_URL}/api/stock-overview/AAPL")
        data = response.json()
        
        assert "key_metrics" in data, "Response missing key_metrics"
        key_metrics = data["key_metrics"]
        
        assert "pe_benchmark" in key_metrics, "key_metrics missing pe_benchmark"
        pe_benchmark = key_metrics["pe_benchmark"]
        
        # pe_benchmark should be a number > 0 for AAPL
        assert pe_benchmark is not None, "pe_benchmark is None"
        assert pe_benchmark > 0, f"pe_benchmark should be > 0, got {pe_benchmark}"
        
        print(f"PASS: key_metrics.pe_benchmark = {pe_benchmark}")
    
    def test_key_metrics_has_dividend_benchmark(self):
        """key_metrics should include dividend_benchmark"""
        response = requests.get(f"{BASE_URL}/api/stock-overview/AAPL")
        data = response.json()
        key_metrics = data.get("key_metrics", {})
        
        assert "dividend_benchmark" in key_metrics, "key_metrics missing dividend_benchmark"
        dividend_benchmark = key_metrics["dividend_benchmark"]
        
        # dividend_benchmark should be a number for AAPL (can be small)
        assert dividend_benchmark is not None, "dividend_benchmark is None"
        print(f"PASS: key_metrics.dividend_benchmark = {dividend_benchmark}")
    
    def test_peer_context_exists(self):
        """peer_context should be present for AAPL"""
        response = requests.get(f"{BASE_URL}/api/stock-overview/AAPL")
        data = response.json()
        
        assert "peer_context" in data, "Response missing peer_context"
        peer_context = data["peer_context"]
        
        assert peer_context is not None, "peer_context is None"
        assert "industry" in peer_context, "peer_context missing industry"
        assert "company_count" in peer_context, "peer_context missing company_count"
        
        print(f"PASS: peer_context.industry = {peer_context['industry']}, count = {peer_context['company_count']}")
    
    def test_valuation_status_label(self):
        """valuation should have status_label like 'Above peers' or 'Below peers'"""
        response = requests.get(f"{BASE_URL}/api/stock-overview/AAPL")
        data = response.json()
        
        assert "valuation" in data, "Response missing valuation"
        valuation = data["valuation"]
        
        if valuation is not None:
            assert "status_label" in valuation, "valuation missing status_label"
            status_label = valuation["status_label"]
            
            # status_label should be one of the expected values
            expected_labels = ["Above peers", "Below peers", "In line"]
            assert status_label in expected_labels, f"Unexpected status_label: {status_label}"
            
            print(f"PASS: valuation.status_label = '{status_label}'")
        else:
            print("SKIP: valuation is None (may be expected for some stocks)")


class TestBenchmarkAPI:
    """Test /api/benchmarks/{industry} endpoint"""
    
    def test_consumer_electronics_benchmark_returns_200(self):
        """Benchmark for Consumer Electronics returns 200"""
        response = requests.get(f"{BASE_URL}/api/benchmarks/Consumer%20Electronics")
        assert response.status_code == 200, f"Expected 200, got {response.status_code}"
        print("PASS: /api/benchmarks/Consumer Electronics returns 200")
    
    def test_consumer_electronics_has_pe_ratio_median(self):
        """Consumer Electronics benchmark should have pe_ratio_median"""
        response = requests.get(f"{BASE_URL}/api/benchmarks/Consumer%20Electronics")
        data = response.json()
        
        assert "pe_ratio_median" in data, "Benchmark missing pe_ratio_median"
        pe_ratio_median = data["pe_ratio_median"]
        
        assert pe_ratio_median is not None, "pe_ratio_median is None"
        assert pe_ratio_median > 0, f"pe_ratio_median should be > 0, got {pe_ratio_median}"
        
        print(f"PASS: Consumer Electronics pe_ratio_median = {pe_ratio_median}")
    
    def test_benchmark_has_company_count(self):
        """Benchmark should have company_count"""
        response = requests.get(f"{BASE_URL}/api/benchmarks/Consumer%20Electronics")
        data = response.json()
        
        assert "company_count" in data, "Benchmark missing company_count"
        company_count = data["company_count"]
        
        assert company_count >= 5, f"company_count should be >= 5, got {company_count}"
        print(f"PASS: Consumer Electronics company_count = {company_count}")
    
    def test_benchmark_has_dividend_yield_median(self):
        """Benchmark should have dividend_yield_median"""
        response = requests.get(f"{BASE_URL}/api/benchmarks/Consumer%20Electronics")
        data = response.json()
        
        assert "dividend_yield_median" in data, "Benchmark missing dividend_yield_median"
        dividend_yield_median = data["dividend_yield_median"]
        
        # Dividend yield median should exist (can be small)
        assert dividend_yield_median is not None, "dividend_yield_median is None"
        print(f"PASS: Consumer Electronics dividend_yield_median = {dividend_yield_median}")


class TestPriceDataAPI:
    """Test price data for chart rendering"""
    
    def test_aapl_prices_returns_data(self):
        """Price API should return data for AAPL"""
        response = requests.get(f"{BASE_URL}/api/stock/AAPL/prices?days=365")
        assert response.status_code == 200
        data = response.json()
        
        assert "prices" in data, "Response missing prices"
        prices = data["prices"]
        
        assert len(prices) > 200, f"Expected > 200 price points, got {len(prices)}"
        print(f"PASS: AAPL has {len(prices)} price points")
    
    def test_aapl_price_range_correct(self):
        """AAPL prices should be in reasonable range (~$160-$300)"""
        response = requests.get(f"{BASE_URL}/api/stock/AAPL/prices?days=365")
        data = response.json()
        prices = [p.get("adjusted_close", 0) for p in data.get("prices", [])]
        
        min_price = min(prices)
        max_price = max(prices)
        
        # AAPL should be in range $150-$350 over past year
        assert min_price > 100, f"Min price {min_price} too low"
        assert max_price < 400, f"Max price {max_price} too high"
        assert max_price > min_price, "Max should be greater than min"
        
        print(f"PASS: AAPL price range: ${min_price:.2f} - ${max_price:.2f}")


class TestHealthEndpoint:
    """Basic health check"""
    
    def test_health_endpoint(self):
        """Health endpoint should return healthy"""
        response = requests.get(f"{BASE_URL}/api/health")
        assert response.status_code == 200
        data = response.json()
        assert data.get("status") == "healthy"
        print("PASS: /api/health returns healthy")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
