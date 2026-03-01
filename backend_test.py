#!/usr/bin/env python3

import requests
import json
import sys
import time
from datetime import datetime

# Configuration
BASE_URL = "https://ticker-detail-v2.preview.emergentagent.com/api"

class RichstoxBackendTester:
    def __init__(self):
        self.base_url = BASE_URL
        self.session = requests.Session()
        self.test_results = []
        self.portfolio_id = None
        self.position_id = None
        
    def log_test(self, test_name, status, response_data=None, error_msg=None):
        """Log test results."""
        result = {
            "test": test_name,
            "status": status,
            "timestamp": datetime.now().isoformat(),
        }
        
        if response_data is not None:
            result["response"] = response_data
        if error_msg:
            result["error"] = error_msg
            
        self.test_results.append(result)
        
        status_symbol = "✅" if status == "PASS" else "❌"
        print(f"{status_symbol} {test_name}")
        if error_msg:
            print(f"   Error: {error_msg}")
        if response_data and isinstance(response_data, dict):
            if "message" in response_data:
                print(f"   Response: {response_data['message']}")
    
    def make_request(self, method, endpoint, data=None, params=None):
        """Make HTTP request with error handling."""
        url = f"{self.base_url}{endpoint}"
        try:
            if method.upper() == "GET":
                response = self.session.get(url, params=params, timeout=30)
            elif method.upper() == "POST":
                response = self.session.post(url, json=data, timeout=30)
            elif method.upper() == "PUT":
                response = self.session.put(url, json=data, timeout=30)
            elif method.upper() == "DELETE":
                response = self.session.delete(url, timeout=30)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")
                
            return response
        except requests.exceptions.RequestException as e:
            return None, str(e)
    
    def test_1_get_tickers(self):
        """Test GET /api/tickers - Get all whitelisted tickers"""
        response = self.make_request("GET", "/tickers")
        
        if isinstance(response, tuple):
            self.log_test("GET /tickers", "FAIL", error_msg=response[1])
            return False
            
        if response.status_code == 200:
            data = response.json()
            if isinstance(data, list) and len(data) > 0:
                # Check structure of first ticker
                ticker = data[0]
                required_fields = ['ticker', 'name', 'sector', 'dividend_yield']
                if all(field in ticker for field in required_fields):
                    self.log_test("GET /tickers", "PASS", {"count": len(data), "first_ticker": ticker['ticker']})
                    return True
                else:
                    self.log_test("GET /tickers", "FAIL", error_msg="Missing required fields in ticker object")
                    return False
            else:
                self.log_test("GET /tickers", "FAIL", error_msg="Expected non-empty list of tickers")
                return False
        else:
            self.log_test("GET /tickers", "FAIL", error_msg=f"Status: {response.status_code}")
            return False
    
    def test_2_search_tickers(self):
        """Test GET /api/tickers/search?q=AAPL - Search for tickers"""
        response = self.make_request("GET", "/tickers/search", params={"q": "AAPL"})
        
        if isinstance(response, tuple):
            self.log_test("GET /tickers/search?q=AAPL", "FAIL", error_msg=response[1])
            return False
            
        if response.status_code == 200:
            data = response.json()
            if isinstance(data, list):
                # Should find AAPL
                aapl_found = any(ticker.get('ticker') == 'AAPL' for ticker in data)
                if aapl_found:
                    self.log_test("GET /tickers/search?q=AAPL", "PASS", {"results_count": len(data)})
                    return True
                else:
                    self.log_test("GET /tickers/search?q=AAPL", "FAIL", error_msg="AAPL not found in search results")
                    return False
            else:
                self.log_test("GET /tickers/search?q=AAPL", "FAIL", error_msg="Expected list response")
                return False
        else:
            self.log_test("GET /tickers/search?q=AAPL", "FAIL", error_msg=f"Status: {response.status_code}")
            return False
    
    def test_3_get_ticker_info(self):
        """Test GET /api/tickers/AAPL - Get ticker info"""
        response = self.make_request("GET", "/tickers/AAPL")
        
        if isinstance(response, tuple):
            self.log_test("GET /tickers/AAPL", "FAIL", error_msg=response[1])
            return False
            
        if response.status_code == 200:
            data = response.json()
            required_fields = ['ticker', 'name', 'sector', 'current_price', 'dividend_yield', 'week_52_high', 'week_52_low', 'max_drawdown']
            if all(field in data for field in required_fields):
                self.log_test("GET /tickers/AAPL", "PASS", {"ticker": data['ticker'], "current_price": data['current_price']})
                return True
            else:
                missing_fields = [field for field in required_fields if field not in data]
                self.log_test("GET /tickers/AAPL", "FAIL", error_msg=f"Missing fields: {missing_fields}")
                return False
        else:
            self.log_test("GET /tickers/AAPL", "FAIL", error_msg=f"Status: {response.status_code}")
            return False
    
    def test_4_get_ticker_prices(self):
        """Test GET /api/tickers/AAPL/prices - Get price history"""
        response = self.make_request("GET", "/tickers/AAPL/prices")
        
        if isinstance(response, tuple):
            self.log_test("GET /tickers/AAPL/prices", "FAIL", error_msg=response[1])
            return False
            
        if response.status_code == 200:
            data = response.json()
            if isinstance(data, list) and len(data) > 0:
                # Check structure of first price entry
                price = data[0]
                required_fields = ['ticker', 'date', 'open_price', 'high', 'low', 'close', 'volume']
                if all(field in price for field in required_fields):
                    self.log_test("GET /tickers/AAPL/prices", "PASS", {"price_entries": len(data)})
                    return True
                else:
                    self.log_test("GET /tickers/AAPL/prices", "FAIL", error_msg="Missing required fields in price entry")
                    return False
            else:
                self.log_test("GET /tickers/AAPL/prices", "FAIL", error_msg="Expected non-empty list of prices")
                return False
        else:
            self.log_test("GET /tickers/AAPL/prices", "FAIL", error_msg=f"Status: {response.status_code}")
            return False
    
    def test_5_get_ticker_dividends(self):
        """Test GET /api/tickers/AAPL/dividends - Get dividend history"""
        response = self.make_request("GET", "/tickers/AAPL/dividends")
        
        if isinstance(response, tuple):
            self.log_test("GET /tickers/AAPL/dividends", "FAIL", error_msg=response[1])
            return False
            
        if response.status_code == 200:
            data = response.json()
            if isinstance(data, list):
                if len(data) > 0:
                    # Check structure of first dividend entry
                    dividend = data[0]
                    required_fields = ['id', 'ticker', 'date', 'amount']
                    if all(field in dividend for field in required_fields):
                        self.log_test("GET /tickers/AAPL/dividends", "PASS", {"dividend_entries": len(data)})
                        return True
                    else:
                        self.log_test("GET /tickers/AAPL/dividends", "FAIL", error_msg="Missing required fields in dividend entry")
                        return False
                else:
                    # AAPL might have no dividends, that's ok
                    self.log_test("GET /tickers/AAPL/dividends", "PASS", {"dividend_entries": 0})
                    return True
            else:
                self.log_test("GET /tickers/AAPL/dividends", "FAIL", error_msg="Expected list response")
                return False
        else:
            self.log_test("GET /tickers/AAPL/dividends", "FAIL", error_msg=f"Status: {response.status_code}")
            return False
    
    def test_6_create_portfolio(self):
        """Test POST /api/portfolios - Create a portfolio"""
        portfolio_data = {
            "name": "Investment Growth Portfolio",
            "portfolio_type": "growth",
            "goal": "track",
            "cash": 25000
        }
        
        response = self.make_request("POST", "/portfolios", data=portfolio_data)
        
        if isinstance(response, tuple):
            self.log_test("POST /portfolios", "FAIL", error_msg=response[1])
            return False
            
        if response.status_code == 200:
            data = response.json()
            if 'id' in data and 'name' in data:
                self.portfolio_id = data['id']  # Store for later tests
                self.log_test("POST /portfolios", "PASS", {"portfolio_id": self.portfolio_id, "name": data['name']})
                return True
            else:
                self.log_test("POST /portfolios", "FAIL", error_msg="Missing id or name in response")
                return False
        else:
            self.log_test("POST /portfolios", "FAIL", error_msg=f"Status: {response.status_code}")
            return False
    
    def test_7_get_portfolios(self):
        """Test GET /api/portfolios - List all portfolios"""
        response = self.make_request("GET", "/portfolios")
        
        if isinstance(response, tuple):
            self.log_test("GET /portfolios", "FAIL", error_msg=response[1])
            return False
            
        if response.status_code == 200:
            data = response.json()
            if isinstance(data, list):
                # Should have at least the portfolio we created
                if len(data) > 0 and any(p.get('id') == self.portfolio_id for p in data):
                    self.log_test("GET /portfolios", "PASS", {"portfolios_count": len(data)})
                    return True
                else:
                    self.log_test("GET /portfolios", "FAIL", error_msg="Created portfolio not found in list")
                    return False
            else:
                self.log_test("GET /portfolios", "FAIL", error_msg="Expected list response")
                return False
        else:
            self.log_test("GET /portfolios", "FAIL", error_msg=f"Status: {response.status_code}")
            return False
    
    def test_8_create_position(self):
        """Test POST /api/positions - Create a position"""
        if not self.portfolio_id:
            self.log_test("POST /positions", "FAIL", error_msg="No portfolio_id available from previous test")
            return False
            
        position_data = {
            "portfolio_id": self.portfolio_id,
            "ticker": "AAPL",
            "buy_date": "2024-01-15",
            "entry_price": 175.00,
            "shares": 10,
            "thesis": "Strong brand ecosystem and consistent innovation in technology"
        }
        
        response = self.make_request("POST", "/positions", data=position_data)
        
        if isinstance(response, tuple):
            self.log_test("POST /positions", "FAIL", error_msg=response[1])
            return False
            
        if response.status_code == 200:
            data = response.json()
            if 'id' in data and 'ticker' in data:
                self.position_id = data['id']  # Store for later tests
                self.log_test("POST /positions", "PASS", {"position_id": self.position_id, "ticker": data['ticker']})
                return True
            else:
                self.log_test("POST /positions", "FAIL", error_msg="Missing id or ticker in response")
                return False
        else:
            self.log_test("POST /positions", "FAIL", error_msg=f"Status: {response.status_code}")
            return False
    
    def test_9_get_portfolio_with_metrics(self):
        """Test GET /api/portfolios/{portfolio_id} - Get portfolio with calculated metrics"""
        if not self.portfolio_id:
            self.log_test("GET /portfolios/{id}", "FAIL", error_msg="No portfolio_id available")
            return False
            
        response = self.make_request("GET", f"/portfolios/{self.portfolio_id}")
        
        if isinstance(response, tuple):
            self.log_test("GET /portfolios/{id}", "FAIL", error_msg=response[1])
            return False
            
        if response.status_code == 200:
            data = response.json()
            required_fields = ['id', 'name', 'total_value', 'total_return', 'max_drawdown', 'positions']
            if all(field in data for field in required_fields):
                self.log_test("GET /portfolios/{id}", "PASS", {"total_value": data['total_value'], "positions": len(data['positions'])})
                return True
            else:
                missing_fields = [field for field in required_fields if field not in data]
                self.log_test("GET /portfolios/{id}", "FAIL", error_msg=f"Missing fields: {missing_fields}")
                return False
        else:
            self.log_test("GET /portfolios/{id}", "FAIL", error_msg=f"Status: {response.status_code}")
            return False
    
    def test_10_get_dashboard(self):
        """Test GET /api/dashboard/{portfolio_id} - Get dashboard stats"""
        if not self.portfolio_id:
            self.log_test("GET /dashboard/{id}", "FAIL", error_msg="No portfolio_id available")
            return False
            
        response = self.make_request("GET", f"/dashboard/{self.portfolio_id}")
        
        if isinstance(response, tuple):
            self.log_test("GET /dashboard/{id}", "FAIL", error_msg=response[1])
            return False
            
        if response.status_code == 200:
            data = response.json()
            required_fields = ['portfolio_value', 'total_return', 'max_drawdown', 'benchmark_return', 'calm_message']
            if all(field in data for field in required_fields):
                self.log_test("GET /dashboard/{id}", "PASS", {"portfolio_value": data['portfolio_value']})
                return True
            else:
                missing_fields = [field for field in required_fields if field not in data]
                self.log_test("GET /dashboard/{id}", "FAIL", error_msg=f"Missing fields: {missing_fields}")
                return False
        else:
            self.log_test("GET /dashboard/{id}", "FAIL", error_msg=f"Status: {response.status_code}")
            return False
    
    def test_11_get_position_details(self):
        """Test GET /api/positions/{position_id} - Get position details"""
        if not self.position_id:
            self.log_test("GET /positions/{id}", "FAIL", error_msg="No position_id available")
            return False
            
        response = self.make_request("GET", f"/positions/{self.position_id}")
        
        if isinstance(response, tuple):
            self.log_test("GET /positions/{id}", "FAIL", error_msg=response[1])
            return False
            
        if response.status_code == 200:
            data = response.json()
            required_fields = ['id', 'ticker', 'current_price', 'market_value', 'return_pct', 'price_history']
            if all(field in data for field in required_fields):
                self.log_test("GET /positions/{id}", "PASS", {"ticker": data['ticker'], "return_pct": data['return_pct']})
                return True
            else:
                missing_fields = [field for field in required_fields if field not in data]
                self.log_test("GET /positions/{id}", "FAIL", error_msg=f"Missing fields: {missing_fields}")
                return False
        else:
            self.log_test("GET /positions/{id}", "FAIL", error_msg=f"Status: {response.status_code}")
            return False
    
    def test_12_get_benchmark(self):
        """Test GET /api/benchmark - Get S&P 500 benchmark data"""
        response = self.make_request("GET", "/benchmark")
        
        if isinstance(response, tuple):
            self.log_test("GET /benchmark", "FAIL", error_msg=response[1])
            return False
            
        if response.status_code == 200:
            data = response.json()
            if isinstance(data, list) and len(data) > 0:
                # Check structure of first benchmark entry
                entry = data[0]
                if 'date' in entry and 'value' in entry:
                    self.log_test("GET /benchmark", "PASS", {"data_points": len(data)})
                    return True
                else:
                    self.log_test("GET /benchmark", "FAIL", error_msg="Missing date or value in benchmark entry")
                    return False
            else:
                self.log_test("GET /benchmark", "FAIL", error_msg="Expected non-empty list of benchmark data")
                return False
        else:
            self.log_test("GET /benchmark", "FAIL", error_msg=f"Status: {response.status_code}")
            return False
    
    def test_13_request_ticker(self):
        """Test POST /api/ticker-requests - Request a new ticker"""
        ticker_request = {
            "ticker": "XYZ"
        }
        
        response = self.make_request("POST", "/ticker-requests", data=ticker_request)
        
        if isinstance(response, tuple):
            self.log_test("POST /ticker-requests", "FAIL", error_msg=response[1])
            return False
            
        if response.status_code == 200:
            data = response.json()
            if 'message' in data and 'ticker' in data:
                self.log_test("POST /ticker-requests", "PASS", {"message": data['message']})
                return True
            else:
                self.log_test("POST /ticker-requests", "FAIL", error_msg="Missing message or ticker in response")
                return False
        else:
            self.log_test("POST /ticker-requests", "FAIL", error_msg=f"Status: {response.status_code}")
            return False

    def run_all_tests(self):
        """Run all tests in sequence."""
        print(f"🚀 Starting RICHSTOX Backend API Tests")
        print(f"📡 Base URL: {self.base_url}")
        print("=" * 60)
        
        # Run tests in order
        tests = [
            self.test_1_get_tickers,
            self.test_2_search_tickers,
            self.test_3_get_ticker_info,
            self.test_4_get_ticker_prices,
            self.test_5_get_ticker_dividends,
            self.test_6_create_portfolio,
            self.test_7_get_portfolios,
            self.test_8_create_position,
            self.test_9_get_portfolio_with_metrics,
            self.test_10_get_dashboard,
            self.test_11_get_position_details,
            self.test_12_get_benchmark,
            self.test_13_request_ticker,
        ]
        
        passed = 0
        failed = 0
        
        for test_func in tests:
            result = test_func()
            if result:
                passed += 1
            else:
                failed += 1
            time.sleep(0.5)  # Small delay between tests
        
        print("=" * 60)
        print(f"📊 Test Results Summary:")
        print(f"   ✅ Passed: {passed}")
        print(f"   ❌ Failed: {failed}")
        print(f"   📈 Success Rate: {(passed/(passed+failed)*100):.1f}%")
        
        if failed > 0:
            print("\n❌ Failed Tests:")
            for result in self.test_results:
                if result['status'] == 'FAIL':
                    print(f"   - {result['test']}: {result.get('error', 'Unknown error')}")
        
        return passed, failed

def main():
    """Main test execution."""
    tester = RichstoxBackendTester()
    passed, failed = tester.run_all_tests()
    
    # Write detailed results to file
    with open("/app/backend_test_results.json", "w") as f:
        json.dump(tester.test_results, f, indent=2)
    
    print(f"\n📄 Detailed results saved to: /app/backend_test_results.json")
    
    # Exit with appropriate code
    sys.exit(0 if failed == 0 else 1)

if __name__ == "__main__":
    main()