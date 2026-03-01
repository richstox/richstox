#!/usr/bin/env python3
"""
Safety Badge Classification
===========================

Classifies all visible tickers into safety types:
- standard: Normal common stock with full data
- spac_shell: SPAC / Shell company (pre-merger, no active business)
- recent_ipo: Recently listed, fundamentals still loading

Rules are deterministic and documented.
"""

import os
import sys
import asyncio
import re
from datetime import datetime, timezone, timedelta
from collections import defaultdict

sys.path.insert(0, '/app/backend')

from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv
load_dotenv('/app/backend/.env')


# =============================================================================
# CLASSIFICATION RULES (DETERMINISTIC)
# =============================================================================

"""
SPAC/SHELL CLASSIFICATION RULE:
-------------------------------
A ticker is classified as `spac_shell` if ANY of:
1. industry == "Shell Companies"
2. Name contains SPAC indicators: "Acquisition Corp", "SPAC", "Blank Check"
3. Name contains shell patterns: "Holdings" + no revenue (future enhancement)

Current implementation uses Rule #1 (industry-based) as primary.
This matches the EODHD classification which is authoritative.

RECENT IPO CLASSIFICATION RULE:
-------------------------------
A ticker is classified as `recent_ipo` if ALL of:
1. is_visible == True (common stock)
2. NOT classified as spac_shell
3. Missing key fundamentals data:
   - No shares_outstanding (direct, quarterly, or annual)
   - OR first_seen_date within last 180 days AND missing >50% of metrics

Current implementation uses the "missing shares_outstanding" gate.
"""

SPAC_SHELL_INDUSTRIES = {
    "Shell Companies",
}

SPAC_NAME_PATTERNS = [
    r'\bAcquisition Corp',
    r'\bSPAC\b',
    r'\bBlank Check\b',
    r'\bMerger Corp',
]


async def classify_safety_types():
    mongo_url = os.environ.get('MONGO_URL', 'mongodb://localhost:27017')
    db_name = os.environ.get('DB_NAME', 'test_database')
    
    client = AsyncIOMotorClient(mongo_url)
    db = client[db_name]
    
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    now = datetime.now(timezone.utc)
    
    try:
        print("="*80)
        print("SAFETY BADGE CLASSIFICATION")
        print("="*80)
        
        # Get all visible tickers
        visible_tickers = await db.tracked_tickers.find(
            {"is_visible": True},
            {"_id": 0}
        ).to_list(length=None)
        
        print(f"\nTotal visible tickers: {len(visible_tickers)}")
        
        # Get processed tickers (have key metrics)
        processed_docs = await db.ticker_key_metrics_daily.find(
            {"date": today},
            {"_id": 0, "ticker": 1}
        ).to_list(length=None)
        processed_set = {doc["ticker"] for doc in processed_docs}
        
        # Classification counters
        counts = defaultdict(int)
        classified = []
        
        for t in visible_tickers:
            ticker = t["ticker"]
            name = t.get("name", "")
            industry = t.get("industry", "")
            
            safety_type = "standard"
            
            # Rule 1: SPAC/Shell classification
            # Primary: Industry-based (authoritative from EODHD)
            if industry in SPAC_SHELL_INDUSTRIES:
                safety_type = "spac_shell"
            else:
                # Secondary: Name pattern matching
                for pattern in SPAC_NAME_PATTERNS:
                    if re.search(pattern, name, re.IGNORECASE):
                        safety_type = "spac_shell"
                        break
            
            # Rule 2: Recent IPO classification (only if not spac_shell)
            if safety_type == "standard":
                # Check if ticker was processed (has fundamentals)
                if ticker not in processed_set:
                    # Missing from key_metrics = missing fundamentals
                    safety_type = "recent_ipo"
            
            counts[safety_type] += 1
            classified.append({
                "ticker": ticker,
                "safety_type": safety_type,
                "name": name,
                "industry": industry
            })
            
            # Update in database
            await db.tracked_tickers.update_one(
                {"ticker": ticker},
                {"$set": {
                    "safety_type": safety_type,
                    "safety_classified_at": now.isoformat()
                }}
            )
        
        # Create index for querying
        await db.tracked_tickers.create_index([("safety_type", 1)])
        
        # Print results
        print("\n" + "="*80)
        print("CLASSIFICATION RESULTS")
        print("="*80)
        
        print(f"\nCOUNTS BY SAFETY_TYPE:")
        for st, count in sorted(counts.items()):
            pct = (count / len(visible_tickers) * 100) if visible_tickers else 0
            print(f"  {st:<15} {count:>5} ({pct:.1f}%)")
        print(f"  {'TOTAL':<15} {sum(counts.values()):>5}")
        
        # Sample each type
        print("\n" + "="*80)
        print("SAMPLE TICKERS BY TYPE")
        print("="*80)
        
        for safety_type in ["standard", "spac_shell", "recent_ipo"]:
            samples = [c for c in classified if c["safety_type"] == safety_type][:5]
            print(f"\n{safety_type.upper()} ({counts[safety_type]} tickers):")
            for s in samples:
                print(f"  {s['ticker']:<15} {s['industry'][:25]:<25} {s['name'][:30]}")
        
        # Print classification rules
        print("\n" + "="*80)
        print("CLASSIFICATION RULES (DETERMINISTIC)")
        print("="*80)
        print("""
SPAC/SHELL (spac_shell):
  Rule: industry == "Shell Companies" OR name matches SPAC patterns
  Patterns: "Acquisition Corp", "SPAC", "Blank Check", "Merger Corp"
  Source: EODHD industry classification (authoritative)
  
RECENT IPO (recent_ipo):
  Rule: is_visible=True AND NOT spac_shell AND missing from ticker_key_metrics_daily
  Meaning: Common stock but missing shares_outstanding or fundamentals
  Source: Absence in today's key metrics computation
  
STANDARD (standard):
  Rule: All other visible tickers (common stock with full data)
""")
        
        return counts
        
    finally:
        client.close()


if __name__ == "__main__":
    asyncio.run(classify_safety_types())
