"""
EODHD API Service with Smart Caching
=====================================
This service handles all EODHD API calls with:
- Mock mode for testing (no API calls)
- Smart caching to minimize API usage
- Rate limiting protection
- Proper error handling

Set EODHD_API_KEY in .env to enable live mode.
If not set, mock mode is used automatically.
"""

import os
import json
import hashlib
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List, Dict, Any
import httpx
import logging
import random

logger = logging.getLogger(__name__)

# Cache directory
CACHE_DIR = Path(__file__).parent / "cache"
CACHE_DIR.mkdir(exist_ok=True)

# EODHD API Base URL
EODHD_BASE_URL = "https://eodhd.com/api"

# Cache durations (in hours)
CACHE_DURATION = {
    "eod": 24,           # End of day prices - cache for 24 hours
    "dividends": 168,    # Dividends - cache for 1 week
    "fundamentals": 168, # Fundamentals - cache for 1 week
    "search": 720,       # Search results - cache for 1 month
}


class EODHDService:
    """
    EODHD API Service with automatic mock/live mode switching.
    
    Usage:
        service = EODHDService()
        
        # Check mode
        if service.is_live_mode:
            print("Using real EODHD API")
        else:
            print("Using mock data (set EODHD_API_KEY to enable live)")
        
        # Get prices (works in both modes)
        prices = await service.get_eod_prices("AAPL.US", days=365)
    """
    
    def __init__(self):
        self.api_key = os.getenv("EODHD_API_KEY", "")
        self.is_live_mode = bool(self.api_key and self.api_key != "demo")
        self._request_count = 0
        self._last_request_time = None
        
        if self.is_live_mode:
            logger.info("EODHD Service initialized in LIVE mode")
        else:
            logger.info("EODHD Service initialized in MOCK mode (set EODHD_API_KEY for live data)")
    
    def _get_cache_key(self, endpoint: str, params: dict) -> str:
        """Generate a unique cache key for the request."""
        key_string = f"{endpoint}:{json.dumps(params, sort_keys=True)}"
        return hashlib.md5(key_string.encode()).hexdigest()
    
    def _get_cache_path(self, cache_key: str) -> Path:
        """Get the file path for a cached response."""
        return CACHE_DIR / f"{cache_key}.json"
    
    def _is_cache_valid(self, cache_path: Path, cache_type: str) -> bool:
        """Check if cached data is still valid."""
        if not cache_path.exists():
            return False
        
        # Get cache duration for this type
        duration_hours = CACHE_DURATION.get(cache_type, 24)
        
        # Check file modification time
        mtime = datetime.fromtimestamp(cache_path.stat().st_mtime)
        expiry = mtime + timedelta(hours=duration_hours)
        
        return datetime.now() < expiry
    
    def _read_cache(self, cache_path: Path) -> Optional[dict]:
        """Read cached data from file."""
        try:
            with open(cache_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Failed to read cache: {e}")
            return None
    
    def _write_cache(self, cache_path: Path, data: dict):
        """Write data to cache file."""
        try:
            with open(cache_path, 'w') as f:
                json.dump(data, f)
        except Exception as e:
            logger.warning(f"Failed to write cache: {e}")
    
    async def _make_request(self, endpoint: str, params: dict, cache_type: str) -> dict:
        """
        Make an API request with caching.
        Returns cached data if available and valid.
        """
        # Check cache first
        cache_key = self._get_cache_key(endpoint, params)
        cache_path = self._get_cache_path(cache_key)
        
        if self._is_cache_valid(cache_path, cache_type):
            cached_data = self._read_cache(cache_path)
            if cached_data:
                logger.info(f"Cache HIT for {endpoint}")
                return cached_data
        
        logger.info(f"Cache MISS for {endpoint} - fetching from API")
        
        # If not live mode, return mock data
        if not self.is_live_mode:
            return self._generate_mock_data(endpoint, params)
        
        # Rate limiting - max 1 request per second
        if self._last_request_time:
            elapsed = (datetime.now() - self._last_request_time).total_seconds()
            if elapsed < 1:
                import asyncio
                await asyncio.sleep(1 - elapsed)
        
        # Make the actual API request
        url = f"{EODHD_BASE_URL}/{endpoint}"
        params["api_token"] = self.api_key
        params["fmt"] = "json"
        
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                response = await client.get(url, params=params)
                response.raise_for_status()
                data = response.json()
                
                self._request_count += 1
                self._last_request_time = datetime.now()
                
                # Cache the response
                self._write_cache(cache_path, data)
                
                logger.info(f"API request successful: {endpoint} (total requests: {self._request_count})")
                return data
                
        except httpx.HTTPStatusError as e:
            logger.error(f"API HTTP error: {e.response.status_code} - {e.response.text}")
            raise Exception(f"EODHD API error: {e.response.status_code}")
        except Exception as e:
            logger.error(f"API request failed: {e}")
            raise Exception(f"EODHD API request failed: {str(e)}")
    
    def _generate_mock_data(self, endpoint: str, params: dict) -> Any:
        """Generate realistic mock data for testing."""
        
        if endpoint.startswith("eod/"):
            return self._generate_mock_eod(params)
        elif endpoint.startswith("div/"):
            return self._generate_mock_dividends(params)
        elif endpoint.startswith("search/"):
            return self._generate_mock_search(params)
        else:
            return {}
    
    def _generate_mock_eod(self, params: dict) -> List[dict]:
        """Generate mock end-of-day price data."""
        ticker = params.get("ticker", "AAPL")
        
        # Use ticker as seed for consistent data
        random.seed(hash(ticker))
        
        # Base prices for common tickers
        base_prices = {
            "AAPL": 175, "MSFT": 380, "GOOGL": 140, "AMZN": 180,
            "NVDA": 480, "META": 500, "TSLA": 250, "JPM": 195,
        }
        base_ticker = ticker.split(".")[0] if "." in ticker else ticker
        base_price = base_prices.get(base_ticker, random.uniform(50, 300))
        
        # Generate 365 days of data
        days = 365
        data = []
        current_price = base_price
        
        for i in range(days, 0, -1):
            date = (datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d")
            
            # Random walk with slight upward bias
            change = random.gauss(0.0002, 0.015)
            current_price = current_price * (1 + change)
            
            daily_range = current_price * random.uniform(0.01, 0.03)
            open_price = current_price + random.uniform(-daily_range/2, daily_range/2)
            high = max(open_price, current_price) + random.uniform(0, daily_range/2)
            low = min(open_price, current_price) - random.uniform(0, daily_range/2)
            
            data.append({
                "date": date,
                "open": round(open_price, 4),
                "high": round(high, 4),
                "low": round(low, 4),
                "close": round(current_price, 4),
                "adjusted_close": round(current_price, 4),
                "volume": random.randint(10000000, 100000000)
            })
        
        return data
    
    def _generate_mock_dividends(self, params: dict) -> List[dict]:
        """Generate mock dividend data."""
        ticker = params.get("ticker", "AAPL")
        
        # Use ticker as seed
        random.seed(hash(ticker + "_div"))
        
        # Dividend yields for common tickers
        div_yields = {
            "AAPL": 0.005, "MSFT": 0.008, "JNJ": 0.029, "PG": 0.024,
            "KO": 0.03, "XOM": 0.033, "JPM": 0.024, "CVX": 0.04,
        }
        base_ticker = ticker.split(".")[0] if "." in ticker else ticker
        div_yield = div_yields.get(base_ticker, random.uniform(0, 0.03))
        
        if div_yield == 0:
            return []
        
        # Generate quarterly dividends for past 2 years
        data = []
        base_dividend = div_yield * 100 / 4  # Quarterly amount per $100 investment
        
        for quarter in range(8):
            months_ago = quarter * 3 + random.randint(0, 2)
            date = (datetime.now() - timedelta(days=months_ago * 30)).strftime("%Y-%m-%d")
            
            data.append({
                "date": date,
                "declarationDate": (datetime.now() - timedelta(days=months_ago * 30 + 30)).strftime("%Y-%m-%d"),
                "recordDate": (datetime.now() - timedelta(days=months_ago * 30 + 14)).strftime("%Y-%m-%d"),
                "paymentDate": date,
                "value": round(base_dividend * random.uniform(0.95, 1.05), 4),
                "unadjustedValue": round(base_dividend * random.uniform(0.95, 1.05), 4),
                "currency": "USD"
            })
        
        return sorted(data, key=lambda x: x["date"], reverse=True)
    
    def _generate_mock_search(self, params: dict) -> List[dict]:
        """Generate mock search results."""
        query = params.get("query", "").upper()
        
        # Sample tickers
        tickers = [
            {"Code": "AAPL", "Name": "Apple Inc", "Exchange": "US", "Type": "Common Stock"},
            {"Code": "MSFT", "Name": "Microsoft Corporation", "Exchange": "US", "Type": "Common Stock"},
            {"Code": "GOOGL", "Name": "Alphabet Inc", "Exchange": "US", "Type": "Common Stock"},
            {"Code": "AMZN", "Name": "Amazon.com Inc", "Exchange": "US", "Type": "Common Stock"},
            {"Code": "NVDA", "Name": "NVIDIA Corporation", "Exchange": "US", "Type": "Common Stock"},
            {"Code": "META", "Name": "Meta Platforms Inc", "Exchange": "US", "Type": "Common Stock"},
            {"Code": "TSLA", "Name": "Tesla Inc", "Exchange": "US", "Type": "Common Stock"},
            {"Code": "JPM", "Name": "JPMorgan Chase & Co", "Exchange": "US", "Type": "Common Stock"},
        ]
        
        return [t for t in tickers if query in t["Code"] or query in t["Name"].upper()][:10]
    
    # ==================== PUBLIC API METHODS ====================
    
    async def get_eod_prices(
        self, 
        ticker: str, 
        days: int = 365,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None
    ) -> List[dict]:
        """
        Get end-of-day prices for a ticker.
        
        Args:
            ticker: Stock ticker (e.g., "AAPL.US" or "AAPL")
            days: Number of days of history (default 365)
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)
        
        Returns:
            List of price data with date, open, high, low, close, volume
        """
        # Ensure ticker has exchange suffix
        if "." not in ticker:
            ticker = f"{ticker}.US"
        
        # Calculate dates
        if not to_date:
            to_date = datetime.now().strftime("%Y-%m-%d")
        if not from_date:
            from_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        
        params = {
            "ticker": ticker,
            "from": from_date,
            "to": to_date,
            "period": "d"
        }
        
        endpoint = f"eod/{ticker}"
        return await self._make_request(endpoint, params, "eod")
    
    async def get_dividends(
        self,
        ticker: str,
        from_date: Optional[str] = None
    ) -> List[dict]:
        """
        Get dividend history for a ticker.
        
        Args:
            ticker: Stock ticker (e.g., "AAPL.US")
            from_date: Start date (YYYY-MM-DD)
        
        Returns:
            List of dividend data with dates and amounts
        """
        if "." not in ticker:
            ticker = f"{ticker}.US"
        
        params = {"ticker": ticker}
        if from_date:
            params["from"] = from_date
        
        endpoint = f"div/{ticker}"
        return await self._make_request(endpoint, params, "dividends")
    
    async def search_tickers(self, query: str) -> List[dict]:
        """
        Search for tickers by name or symbol.
        
        Args:
            query: Search query
        
        Returns:
            List of matching tickers
        """
        params = {"query": query}
        endpoint = f"search/{query}"
        return await self._make_request(endpoint, params, "search")
    
    async def get_current_price(self, ticker: str) -> float:
        """
        Get the current (latest) price for a ticker.
        
        Args:
            ticker: Stock ticker
        
        Returns:
            Current price as float
        """
        prices = await self.get_eod_prices(ticker, days=5)
        if prices:
            return prices[-1].get("close", prices[-1].get("adjusted_close", 0))
        return 0.0
    
    def get_api_stats(self) -> dict:
        """Get statistics about API usage."""
        return {
            "mode": "LIVE" if self.is_live_mode else "MOCK",
            "total_requests": self._request_count,
            "last_request": self._last_request_time.isoformat() if self._last_request_time else None,
            "cache_dir": str(CACHE_DIR),
            "cache_files": len(list(CACHE_DIR.glob("*.json")))
        }
    
    def clear_cache(self, cache_type: Optional[str] = None):
        """Clear cached data."""
        count = 0
        for cache_file in CACHE_DIR.glob("*.json"):
            cache_file.unlink()
            count += 1
        logger.info(f"Cleared {count} cache files")
        return count


# Singleton instance
_eodhd_service: Optional[EODHDService] = None

def get_eodhd_service() -> EODHDService:
    """Get the EODHD service singleton."""
    global _eodhd_service
    if _eodhd_service is None:
        _eodhd_service = EODHDService()
    return _eodhd_service
