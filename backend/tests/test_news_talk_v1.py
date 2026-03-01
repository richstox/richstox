"""
RICHSTOX News & Talk v1 API Tests
==================================
Tests for the News & Talk feature v1 API endpoints.

Endpoints tested:
- GET /api/v1/markets/feed - Market news feed (public)
- GET /api/v1/talk - Global talk posts (public)
- POST /api/v1/talk - Create talk post (requires auth, returns 401)
- GET /api/v1/stocks/{symbol}/feed - Stock-specific news (public)
- GET /api/v1/stocks/{symbol}/talk - Stock-specific talk (public)
- GET /api/v1/users/{id} - User profile (404 for non-existent)
- GET /api/v1/users/{id}/posts - User posts (404 for non-existent)
"""

import pytest
import requests
import os

BASE_URL = os.environ.get('REACT_APP_BACKEND_URL', 'https://ticker-detail-v2.preview.emergentagent.com').rstrip('/')


class TestMarketsFeedEndpoint:
    """Tests for GET /api/v1/markets/feed"""
    
    def test_markets_feed_returns_200(self):
        """Market feed should return 200 OK"""
        response = requests.get(f"{BASE_URL}/api/v1/markets/feed?limit=5")
        assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
        print("PASS: GET /api/v1/markets/feed returns 200")
    
    def test_markets_feed_response_structure(self):
        """Market feed should have correct response structure"""
        response = requests.get(f"{BASE_URL}/api/v1/markets/feed?limit=5")
        assert response.status_code == 200
        
        data = response.json()
        assert "feed" in data, "Response should have 'feed' field"
        assert "count" in data, "Response should have 'count' field"
        assert "offset" in data, "Response should have 'offset' field"
        assert "has_more" in data, "Response should have 'has_more' field"
        
        # Feed is a list
        assert isinstance(data["feed"], list), "'feed' should be a list"
        assert isinstance(data["count"], int), "'count' should be an integer"
        assert isinstance(data["has_more"], bool), "'has_more' should be a boolean"
        print("PASS: Markets feed has correct response structure")


class TestTalkEndpoints:
    """Tests for Talk API endpoints"""
    
    def test_get_talk_returns_200(self):
        """GET /api/v1/talk should return 200 OK"""
        response = requests.get(f"{BASE_URL}/api/v1/talk?limit=20&offset=0")
        assert response.status_code == 200, f"Expected 200, got {response.status_code}"
        print("PASS: GET /api/v1/talk returns 200")
    
    def test_get_talk_response_structure(self):
        """Talk feed should have correct response structure"""
        response = requests.get(f"{BASE_URL}/api/v1/talk?limit=20")
        assert response.status_code == 200
        
        data = response.json()
        assert "posts" in data, "Response should have 'posts' field"
        assert "count" in data, "Response should have 'count' field"
        assert "offset" in data, "Response should have 'offset' field"
        assert "has_more" in data, "Response should have 'has_more' field"
        
        assert isinstance(data["posts"], list), "'posts' should be a list"
        print("PASS: Talk feed has correct response structure")
    
    def test_post_talk_requires_auth(self):
        """POST /api/v1/talk should return 401 without auth"""
        response = requests.post(
            f"{BASE_URL}/api/v1/talk",
            json={"text": "This is a test post"},
            headers={"Content-Type": "application/json"}
        )
        assert response.status_code == 401, f"Expected 401, got {response.status_code}"
        
        data = response.json()
        assert "detail" in data, "Error response should have 'detail' field"
        assert "Authentication required" in data["detail"], "Error should mention authentication required"
        print("PASS: POST /api/v1/talk returns 401 without auth")
    
    def test_talk_with_symbol_filter(self):
        """Talk feed should accept symbol filter"""
        response = requests.get(f"{BASE_URL}/api/v1/talk?symbol=AAPL&limit=10")
        assert response.status_code == 200, f"Expected 200, got {response.status_code}"
        print("PASS: Talk feed accepts symbol filter")
    
    def test_talk_with_rrr_filter(self):
        """Talk feed should accept min_rrr filter"""
        response = requests.get(f"{BASE_URL}/api/v1/talk?min_rrr=1.0&limit=10")
        assert response.status_code == 200, f"Expected 200, got {response.status_code}"
        print("PASS: Talk feed accepts min_rrr filter")


class TestStockFeedEndpoint:
    """Tests for GET /api/v1/stocks/{symbol}/feed"""
    
    def test_stock_feed_returns_200(self):
        """Stock feed should return 200 OK"""
        response = requests.get(f"{BASE_URL}/api/v1/stocks/AAPL/feed?limit=5")
        assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
        print("PASS: GET /api/v1/stocks/AAPL/feed returns 200")
    
    def test_stock_feed_response_structure(self):
        """Stock feed should have correct response structure"""
        response = requests.get(f"{BASE_URL}/api/v1/stocks/MSFT/feed?limit=5")
        assert response.status_code == 200
        
        data = response.json()
        assert "symbol" in data, "Response should have 'symbol' field"
        assert "feed" in data, "Response should have 'feed' field"
        assert "count" in data, "Response should have 'count' field"
        assert "has_more" in data, "Response should have 'has_more' field"
        
        assert isinstance(data["feed"], list), "'feed' should be a list"
        print("PASS: Stock feed has correct response structure")
    
    def test_stock_feed_for_different_symbols(self):
        """Stock feed should work for different symbols"""
        symbols = ["AAPL", "MSFT", "GOOGL"]
        for symbol in symbols:
            response = requests.get(f"{BASE_URL}/api/v1/stocks/{symbol}/feed?limit=5")
            assert response.status_code == 200, f"Expected 200 for {symbol}, got {response.status_code}"
        print("PASS: Stock feed works for multiple symbols")


class TestStockTalkEndpoint:
    """Tests for GET /api/v1/stocks/{symbol}/talk"""
    
    def test_stock_talk_returns_200(self):
        """Stock talk should return 200 OK"""
        response = requests.get(f"{BASE_URL}/api/v1/stocks/AAPL/talk?limit=5")
        assert response.status_code == 200, f"Expected 200, got {response.status_code}"
        print("PASS: GET /api/v1/stocks/AAPL/talk returns 200")
    
    def test_stock_talk_response_structure(self):
        """Stock talk should have correct response structure"""
        response = requests.get(f"{BASE_URL}/api/v1/stocks/TSLA/talk?limit=5")
        assert response.status_code == 200
        
        data = response.json()
        assert "symbol" in data, "Response should have 'symbol' field"
        assert "posts" in data, "Response should have 'posts' field"
        assert "count" in data, "Response should have 'count' field"
        assert "has_more" in data, "Response should have 'has_more' field"
        
        assert isinstance(data["posts"], list), "'posts' should be a list"
        print("PASS: Stock talk has correct response structure")


class TestUserEndpoints:
    """Tests for User API endpoints"""
    
    def test_get_nonexistent_user_returns_404(self):
        """GET /api/v1/users/{id} should return 404 for non-existent user"""
        response = requests.get(f"{BASE_URL}/api/v1/users/nonexistent-user-id-12345")
        assert response.status_code == 404, f"Expected 404, got {response.status_code}"
        
        data = response.json()
        assert "detail" in data, "Error response should have 'detail' field"
        assert "not found" in data["detail"].lower(), "Error should mention 'not found'"
        print("PASS: GET /api/v1/users/{id} returns 404 for non-existent user")
    
    def test_get_nonexistent_user_posts_returns_404(self):
        """GET /api/v1/users/{id}/posts should return 404 for non-existent user"""
        response = requests.get(f"{BASE_URL}/api/v1/users/nonexistent-user-id-12345/posts?limit=20")
        assert response.status_code == 404, f"Expected 404, got {response.status_code}"
        
        data = response.json()
        assert "detail" in data, "Error response should have 'detail' field"
        print("PASS: GET /api/v1/users/{id}/posts returns 404 for non-existent user")


class TestHealthEndpoint:
    """Basic health check"""
    
    def test_health_check(self):
        """Health endpoint should return 200"""
        response = requests.get(f"{BASE_URL}/api/health")
        assert response.status_code == 200
        
        data = response.json()
        assert data.get("status") == "healthy"
        print("PASS: Health check returns healthy")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
