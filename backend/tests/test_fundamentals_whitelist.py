# ==============================================================================
# RICHSTOX FUNDAMENTALS WHITELIST - UNIT TESTS
# ==============================================================================
# BINDING: These tests MUST pass before any deployment.
# Tests verify that no forbidden keys are stored in fundamentals.
# ==============================================================================

import pytest
import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
import os
import sys

# Add parent to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from whitelist_mapper import (
    apply_whitelist,
    FORBIDDEN_SECTIONS,
    GENERAL_FORBIDDEN_FIELDS,
    WHITELIST_VERSION,
    verify_whitelist_integrity
)

# ==============================================================================
# FORBIDDEN KEYS - MUST NEVER BE IN stored fundamentals
# ==============================================================================
FORBIDDEN_TOP_LEVEL_KEYS = {
    "Highlights",       # Computed metrics (PE, MarketCap, etc.)
    "Valuation",        # Computed ratios
    "Technicals",       # Price-derived data (Beta, 52wHigh, etc.)
    "ETF_Data",         # Not relevant for stocks
    "MutualFund_Data",  # Not relevant for stocks
    "AnalystRatings",   # Analyst recommendations
    "ESGScores",        # Third-party scores
}

ALLOWED_TOP_LEVEL_KEYS = {
    "General",
    "Earnings", 
    "Financials",
    "Holders",
    "InsiderTransactions",
    "SharesStats",
    "SplitsDividends",
    "outstandingShares",
}


class TestWhitelistMapper:
    """Tests for whitelist_mapper.py functions."""
    
    def test_whitelist_version_is_set(self):
        """WHITELIST_VERSION must be set."""
        assert WHITELIST_VERSION is not None
        assert len(WHITELIST_VERSION) > 0
    
    def test_whitelist_integrity(self):
        """Whitelist integrity check must pass."""
        passed, message = verify_whitelist_integrity()
        assert passed, f"Whitelist integrity failed: {message}"
    
    def test_forbidden_sections_defined(self):
        """FORBIDDEN_SECTIONS must contain expected keys."""
        for key in ["Highlights", "Valuation", "Technicals"]:
            assert key in FORBIDDEN_SECTIONS, f"{key} must be in FORBIDDEN_SECTIONS"
    
    def test_apply_whitelist_strips_highlights(self):
        """apply_whitelist must strip Highlights section."""
        raw_payload = {
            "General": {"Name": "Test Corp"},
            "Highlights": {"PERatio": 25.5, "MarketCapitalization": 1000000000}
        }
        
        filtered, audit = apply_whitelist(raw_payload, "TEST.US")
        
        assert "Highlights" not in filtered, "Highlights must be stripped"
        assert "Highlights" in audit["sections_stripped"]
    
    def test_apply_whitelist_strips_valuation(self):
        """apply_whitelist must strip Valuation section."""
        raw_payload = {
            "General": {"Name": "Test Corp"},
            "Valuation": {"TrailingPE": 20, "ForwardPE": 18}
        }
        
        filtered, audit = apply_whitelist(raw_payload, "TEST.US")
        
        assert "Valuation" not in filtered, "Valuation must be stripped"
    
    def test_apply_whitelist_strips_technicals(self):
        """apply_whitelist must strip Technicals section."""
        raw_payload = {
            "General": {"Name": "Test Corp"},
            "Technicals": {"Beta": 1.2, "52WeekHigh": 150}
        }
        
        filtered, audit = apply_whitelist(raw_payload, "TEST.US")
        
        assert "Technicals" not in filtered, "Technicals must be stripped"
    
    def test_apply_whitelist_strips_officers(self):
        """apply_whitelist must strip Officers from General."""
        raw_payload = {
            "General": {
                "Name": "Test Corp",
                "Officers": [{"Name": "CEO", "Title": "Chief Executive Officer"}]
            }
        }
        
        filtered, audit = apply_whitelist(raw_payload, "TEST.US")
        
        assert "Officers" not in filtered.get("General", {}), "Officers must be stripped"
    
    def test_apply_whitelist_keeps_allowed_sections(self):
        """apply_whitelist must keep allowed sections."""
        raw_payload = {
            "General": {"Name": "Test Corp", "Sector": "Technology"},
            "Financials": {"Income_Statement": {"yearly": {}}},
            "Earnings": {"History": {}, "Annual": {}},
        }
        
        filtered, audit = apply_whitelist(raw_payload, "TEST.US")
        
        assert "General" in filtered
        assert "Financials" in filtered
        assert "Earnings" in filtered
    
    def test_apply_whitelist_strips_earnings_trend(self):
        """apply_whitelist must strip Earnings.Trend."""
        raw_payload = {
            "Earnings": {
                "History": {"2024-Q1": {}},
                "Trend": {"forecast": {}},  # Must be stripped
            }
        }
        
        filtered, audit = apply_whitelist(raw_payload, "TEST.US")
        
        assert "Trend" not in filtered.get("Earnings", {}), "Earnings.Trend must be stripped"
    
    def test_no_forbidden_keys_in_output(self):
        """Complete test: no forbidden keys in filtered output."""
        # Simulate a full EODHD payload
        raw_payload = {
            "General": {"Name": "Apple Inc", "Sector": "Technology", "Officers": []},
            "Highlights": {"PERatio": 25},
            "Valuation": {"TrailingPE": 25},
            "Technicals": {"Beta": 1.2},
            "AnalystRatings": {"Rating": "Buy"},
            "ESGScores": {"Score": 75},
            "Financials": {"Income_Statement": {}},
            "Earnings": {"History": {}, "Trend": {}},
            "Holders": {"Institutions": {}},
            "InsiderTransactions": [],
            "SharesStats": {"SharesOutstanding": 15000000000, "PercentInsiders": 0.1},
            "SplitsDividends": {},
            "outstandingShares": {},
        }
        
        filtered, audit = apply_whitelist(raw_payload, "AAPL.US")
        
        # Check no forbidden top-level keys
        for key in FORBIDDEN_TOP_LEVEL_KEYS:
            assert key not in filtered, f"Forbidden key '{key}' found in filtered output"
        
        # Check only allowed keys
        for key in filtered.keys():
            assert key in ALLOWED_TOP_LEVEL_KEYS, f"Unknown key '{key}' in filtered output"
        
        # Check Officers is stripped from General
        assert "Officers" not in filtered.get("General", {})
        
        # Check Earnings.Trend is stripped
        assert "Trend" not in filtered.get("Earnings", {})
        
        # Check SharesStats only has allowed fields
        shares_stats = filtered.get("SharesStats", {})
        for key in shares_stats:
            assert key in {"SharesOutstanding", "SharesFloat"}, f"Forbidden SharesStats key: {key}"


class TestDatabaseCompliance:
    """Tests that verify stored data in MongoDB complies with whitelist."""
    
    @pytest.fixture
    def db(self):
        """Get MongoDB connection."""
        mongo_url = os.environ.get("MONGO_URL", "mongodb://localhost:27017")
        client = AsyncIOMotorClient(mongo_url)
        return client["richstox_prod"]
    
    @pytest.mark.asyncio
    async def test_no_forbidden_keys_in_database(self, db):
        """
        CRITICAL: Verify NO stored fundamentals contain forbidden keys.
        This test MUST pass - any failure indicates data integrity breach.
        """
        violations = []
        
        # Sample 100 random tickers with fundamentals
        cursor = db.tracked_tickers.aggregate([
            {"$match": {"fundamentals": {"$exists": True, "$ne": None}}},
            {"$sample": {"size": 100}},
            {"$project": {"ticker": 1, "fundamentals": 1}}
        ])
        
        async for doc in cursor:
            ticker = doc.get("ticker", "unknown")
            fund = doc.get("fundamentals", {})
            
            if not isinstance(fund, dict):
                continue
            
            # Check for forbidden top-level keys
            forbidden_found = set(fund.keys()) & FORBIDDEN_TOP_LEVEL_KEYS
            if forbidden_found:
                violations.append({
                    "ticker": ticker,
                    "forbidden_keys": list(forbidden_found)
                })
        
        # Report violations
        if violations:
            print("\n" + "=" * 60)
            print("FORBIDDEN KEYS FOUND IN DATABASE!")
            print("=" * 60)
            for v in violations[:10]:  # Show first 10
                print(f"  {v['ticker']}: {v['forbidden_keys']}")
            print("=" * 60)
        
        assert len(violations) == 0, f"Found {len(violations)} tickers with forbidden keys"
    
    @pytest.mark.asyncio
    async def test_count_tickers_with_forbidden_keys(self, db):
        """
        Admin audit counter: Count tickers with forbidden keys.
        Result MUST be 0.
        """
        # Build query for any forbidden key
        forbidden_conditions = []
        for key in FORBIDDEN_TOP_LEVEL_KEYS:
            forbidden_conditions.append({f"fundamentals.{key}": {"$exists": True}})
        
        if not forbidden_conditions:
            return  # No conditions to check
        
        count = await db.tracked_tickers.count_documents({
            "$or": forbidden_conditions
        })
        
        print(f"\nAdmin Audit: Tickers with forbidden keys = {count}")
        
        assert count == 0, f"CRITICAL: {count} tickers have forbidden keys in fundamentals"


def run_tests():
    """Run all tests and return results."""
    import subprocess
    result = subprocess.run(
        ["python", "-m", "pytest", __file__, "-v", "--tb=short"],
        capture_output=True,
        text=True
    )
    print(result.stdout)
    if result.stderr:
        print(result.stderr)
    return result.returncode == 0


if __name__ == "__main__":
    # Quick standalone test
    print("=" * 60)
    print("FUNDAMENTALS WHITELIST - UNIT TESTS")
    print("=" * 60)
    
    # Run whitelist mapper tests
    test = TestWhitelistMapper()
    
    tests_passed = 0
    tests_failed = 0
    
    for method_name in dir(test):
        if method_name.startswith("test_"):
            try:
                getattr(test, method_name)()
                print(f"  ✅ {method_name}")
                tests_passed += 1
            except AssertionError as e:
                print(f"  ❌ {method_name}: {e}")
                tests_failed += 1
    
    print("=" * 60)
    print(f"RESULTS: {tests_passed} passed, {tests_failed} failed")
    print("=" * 60)
    
    # Run database compliance test
    print("\nRunning database compliance audit...")
    
    async def run_db_audit():
        mongo_url = os.environ.get("MONGO_URL", "mongodb://localhost:27017")
        client = AsyncIOMotorClient(mongo_url)
        db = client["richstox_prod"]
        
        # Count forbidden keys
        forbidden_conditions = []
        for key in FORBIDDEN_TOP_LEVEL_KEYS:
            forbidden_conditions.append({f"fundamentals.{key}": {"$exists": True}})
        
        count = 0
        if forbidden_conditions:
            count = await db.tracked_tickers.count_documents({
                "$or": forbidden_conditions
            })
        
        print(f"\n  Admin Audit: Tickers with forbidden keys = {count}")
        
        if count == 0:
            print("  ✅ DATABASE COMPLIANCE: PASS")
        else:
            print(f"  ❌ DATABASE COMPLIANCE: FAIL ({count} violations)")
        
        client.close()
        return count
    
    violations = asyncio.run(run_db_audit())
    
    exit(0 if tests_failed == 0 and violations == 0 else 1)
