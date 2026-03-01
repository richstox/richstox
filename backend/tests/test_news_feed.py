"""
Test News Feed Functionality - RICHSTOX
========================================
Tests the /api/news endpoint for:
- Multiple tickers support (AAPL, MSFT, GOOGL, AMZN, NVDA)
- Aggregate sentiment calculation
- Individual news item sentiment badges
- Pagination and load more functionality
"""

import pytest
import requests
import os

BASE_URL = os.environ.get('EXPO_PUBLIC_BACKEND_URL', 'https://ticker-detail-v2.preview.emergentagent.com').rstrip('/')


class TestHomepageEndpoint:
    """Test /api/homepage endpoint"""
    
    def test_homepage_returns_200(self):
        """Homepage endpoint should return 200"""
        response = requests.get(f"{BASE_URL}/api/homepage")
        assert response.status_code == 200, f"Expected 200, got {response.status_code}"
        print("PASS: Homepage returns 200")
    
    def test_homepage_has_required_fields(self):
        """Homepage should have required fields"""
        response = requests.get(f"{BASE_URL}/api/homepage")
        data = response.json()
        
        required_fields = ['last_updated', 'market_status', 'benchmark', 'popular_stocks', 'data_source', 'api_mode']
        for field in required_fields:
            assert field in data, f"Missing field: {field}"
        
        print(f"PASS: Homepage has all required fields: {required_fields}")
    
    def test_homepage_benchmark_data(self):
        """Homepage benchmark should have valid data"""
        response = requests.get(f"{BASE_URL}/api/homepage")
        data = response.json()
        benchmark = data.get('benchmark', {})
        
        assert 'name' in benchmark, "Benchmark missing name"
        assert 'value' in benchmark, "Benchmark missing value"
        assert benchmark['value'] > 0, "Benchmark value should be positive"
        
        print(f"PASS: Benchmark data valid - {benchmark['name']}: {benchmark['value']}")


class TestNewsEndpoint:
    """Test /api/news endpoint"""
    
    def test_news_returns_200(self):
        """News endpoint should return 200"""
        response = requests.get(f"{BASE_URL}/api/news?offset=0&limit=10")
        assert response.status_code == 200, f"Expected 200, got {response.status_code}"
        print("PASS: News endpoint returns 200")
    
    def test_news_returns_multiple_tickers(self):
        """News should return news from multiple tickers, not just AAPL"""
        response = requests.get(f"{BASE_URL}/api/news?offset=0&limit=20")
        data = response.json()
        news_items = data.get('news', [])
        
        # Get unique tickers from news
        tickers = set([item['ticker'] for item in news_items])
        
        assert len(tickers) > 1, f"Expected multiple tickers, got only: {tickers}"
        print(f"PASS: News contains {len(tickers)} different tickers: {sorted(tickers)}")
        
        # Check that expected tickers are present
        expected_tickers = {'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA'}
        found_expected = tickers.intersection(expected_tickers)
        assert len(found_expected) >= 2, f"Expected at least 2 of {expected_tickers}, found: {found_expected}"
        print(f"PASS: Found expected tickers: {found_expected}")
    
    def test_news_has_aggregate_sentiment(self):
        """News response should include aggregate_sentiment field"""
        response = requests.get(f"{BASE_URL}/api/news?offset=0&limit=10")
        data = response.json()
        
        assert 'aggregate_sentiment' in data, "Missing aggregate_sentiment in response"
        aggregate = data['aggregate_sentiment']
        
        # Check required aggregate sentiment fields
        required_fields = ['score', 'label', 'color', 'total_articles', 'positive_count', 'negative_count', 'neutral_count']
        for field in required_fields:
            assert field in aggregate, f"aggregate_sentiment missing field: {field}"
        
        print(f"PASS: aggregate_sentiment has all fields: {required_fields}")
        print(f"  Score: {aggregate['score']}, Label: {aggregate['label']}, Color: {aggregate['color']}")
        print(f"  Counts: +{aggregate['positive_count']} / -{aggregate['negative_count']} / ~{aggregate['neutral_count']}")
    
    def test_aggregate_sentiment_values(self):
        """Aggregate sentiment should have valid values"""
        response = requests.get(f"{BASE_URL}/api/news?offset=0&limit=10")
        data = response.json()
        aggregate = data.get('aggregate_sentiment', {})
        
        # Score should be between -1 and 1
        assert -1 <= aggregate['score'] <= 1, f"Score {aggregate['score']} out of range [-1, 1]"
        
        # Label should be one of the valid values
        valid_labels = ['positive', 'negative', 'neutral']
        assert aggregate['label'] in valid_labels, f"Invalid label: {aggregate['label']}"
        
        # Color should be a valid hex color
        assert aggregate['color'].startswith('#'), f"Color should be hex: {aggregate['color']}"
        
        # Counts should match total
        total = aggregate['positive_count'] + aggregate['negative_count'] + aggregate['neutral_count']
        assert total == aggregate['total_articles'], f"Counts don't match total: {total} vs {aggregate['total_articles']}"
        
        print(f"PASS: Aggregate sentiment values are valid")
    
    def test_individual_news_has_sentiment_label(self):
        """Each news item should have sentiment_label field"""
        response = requests.get(f"{BASE_URL}/api/news?offset=0&limit=10")
        data = response.json()
        news_items = data.get('news', [])
        
        assert len(news_items) > 0, "No news items returned"
        
        for i, item in enumerate(news_items[:5]):  # Check first 5 items
            assert 'sentiment_label' in item, f"News item {i} missing sentiment_label"
            assert item['sentiment_label'] in ['positive', 'negative', 'neutral'], f"Invalid sentiment_label: {item['sentiment_label']}"
        
        print(f"PASS: All news items have valid sentiment_label")
    
    def test_news_item_structure(self):
        """News items should have all required fields"""
        response = requests.get(f"{BASE_URL}/api/news?offset=0&limit=5")
        data = response.json()
        news_items = data.get('news', [])
        
        required_fields = ['id', 'ticker', 'company_name', 'title', 'content', 'link', 'time_ago', 'sentiment_label']
        
        for item in news_items:
            for field in required_fields:
                assert field in item, f"News item missing field: {field}"
        
        print(f"PASS: News items have all required fields: {required_fields}")
    
    def test_news_pagination(self):
        """News should support pagination with offset and limit"""
        # Get first page
        response1 = requests.get(f"{BASE_URL}/api/news?offset=0&limit=5")
        data1 = response1.json()
        
        # Get second page
        response2 = requests.get(f"{BASE_URL}/api/news?offset=5&limit=5")
        data2 = response2.json()
        
        news1 = data1.get('news', [])
        news2 = data2.get('news', [])
        
        # Pages should have different news items (by ID)
        ids1 = set([n['id'] for n in news1])
        ids2 = set([n['id'] for n in news2])
        
        # There should be no overlap (or minimal)
        overlap = ids1.intersection(ids2)
        assert len(overlap) == 0, f"Pagination overlapping items: {overlap}"
        
        print(f"PASS: Pagination works - Page 1: {len(news1)} items, Page 2: {len(news2)} items, No overlap")
    
    def test_news_has_more_flag(self):
        """News response should have has_more flag for load more"""
        response = requests.get(f"{BASE_URL}/api/news?offset=0&limit=5")
        data = response.json()
        
        assert 'has_more' in data, "Missing has_more flag"
        assert isinstance(data['has_more'], bool), "has_more should be boolean"
        
        print(f"PASS: has_more flag present: {data['has_more']}")
    
    def test_news_from_different_companies(self):
        """News should include items from different companies (not just Apple)"""
        response = requests.get(f"{BASE_URL}/api/news?offset=0&limit=20")
        data = response.json()
        news_items = data.get('news', [])
        
        company_names = set([item['company_name'] for item in news_items])
        
        # Should have more than just Apple
        assert len(company_names) > 1, f"Only one company in news: {company_names}"
        
        # Check we have news from non-Apple companies
        non_apple = [c for c in company_names if 'Apple' not in c]
        assert len(non_apple) > 0, f"All news from Apple, expected other companies"
        
        print(f"PASS: News from multiple companies: {sorted(company_names)}")


class TestHealthEndpoint:
    """Test health check"""
    
    def test_health_check(self):
        """Health endpoint should return healthy status"""
        response = requests.get(f"{BASE_URL}/api/health")
        assert response.status_code == 200
        data = response.json()
        assert data['status'] == 'healthy'
        print(f"PASS: Health check - Status: {data['status']}, Mode: {data['mode']}")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
