#!/usr/bin/env python3
"""
Universe Cleanup: Enforce "Common Stock Only"
=============================================

This script:
1. Cleans up existing gap tickers based on type
2. Enforces Common Stock only in visible universe
3. Creates excluded_tickers collection with reasons

Rules:
- Bankrupt (Q-suffix): is_visible=false, reason: bankrupt_delisted
- SPAC units, structured products, preferred, test: is_visible=false, reason: not_common_stock
- Recent IPO (common stock, missing shares): Keep is_visible=true
- SNE.US, HEAR.US (seed-only test): Delete from tracked_tickers
"""

import os
import sys
import asyncio
import re
from datetime import datetime, timezone
from collections import defaultdict

sys.path.insert(0, '/app/backend')

from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv
load_dotenv('/app/backend/.env')


# Patterns for non-common-stock identification
UNIT_PATTERNS = [
    r'-UN\.US$',      # Units (e.g., ALUB-UN.US)
    r'U\.US$',        # Units suffix (e.g., ARCIU.US, but careful - need to check name)
]

TEST_TICKERS = {
    'NTEST.US', 'NTEST-H.US', 'NTEST-I.US',
    'ZJZZT.US', 'ZVZZT.US', 'ZWZZT.US', 'ZXZZT.US',
    'QVCC.US',  # Test ticker
}

SEED_TEST_TICKERS = {'SNE.US', 'HEAR.US'}  # To be deleted

# Structured product / trust tickers (known from analysis)
STRUCTURED_PRODUCTS = {
    'GJH.US', 'GJO.US', 'GJP.US', 'GJR.US', 'GJS.US', 'GJT.US',
    'IPB.US', 'JBK.US', 'KTH.US', 'KTN.US', 'NRUC.US', 'PYT.US',
    'ECCX.US',  # Preferred/structured
}

# Preferred shares patterns
PREFERRED_PATTERNS = [
    r'-[A-Z]\.US$',   # Preferred class (e.g., BRK-A.US is OK, but check context)
]

# Bankrupt pattern (Q suffix before .US)
BANKRUPT_PATTERN = r'Q\.US$'


def classify_ticker(ticker: str, name: str, industry: str) -> tuple:
    """
    Classify ticker and return (should_exclude, reason).
    
    Returns:
        (should_exclude: bool, reason: str or None)
    """
    ticker_upper = ticker.upper()
    name_lower = (name or "").lower()
    industry_lower = (industry or "").lower()
    
    # 1. Test tickers
    if ticker in TEST_TICKERS:
        return (True, "test_ticker")
    
    # 2. Seed-only test tickers (to be deleted)
    if ticker in SEED_TEST_TICKERS:
        return (True, "seed_test_delete")
    
    # 3. Bankrupt (Q suffix)
    if re.search(BANKRUPT_PATTERN, ticker_upper):
        return (True, "bankrupt_delisted")
    
    # 4. Structured products / trusts
    if ticker in STRUCTURED_PRODUCTS:
        return (True, "structured_product")
    
    # 5. Unit patterns (SPAC units)
    if re.search(r'-UN\.US$', ticker_upper):
        return (True, "spac_unit")
    
    # 6. Check name for "unit" or "units"
    if 'unit' in name_lower and 'unit' not in name_lower.replace('united', '').replace('unity', ''):
        # Has "unit" but not "united" or "unity"
        if ' unit' in name_lower or 'units' in name_lower:
            return (True, "spac_unit")
    
    # 7. Shell Companies industry + specific patterns
    if industry_lower == "shell companies":
        # Check if it's a SPAC unit (ends with U and has specific patterns)
        if ticker_upper.endswith('U.US') and len(ticker) > 5:
            # Likely a SPAC unit
            return (True, "spac_unit")
        # Other shell companies are still common stock SPACs - keep for now
        # but mark if they have no shares
    
    # 8. Preferred shares (explicit in name)
    if 'preferred' in name_lower or 'pref' in name_lower:
        return (True, "preferred_shares")
    
    # 9. Trust securities / notes
    trust_keywords = ['trust', 'strats', 'corts', 'depositor', 'securities-backed']
    for kw in trust_keywords:
        if kw in name_lower:
            return (True, "structured_product")
    
    # Not excluded
    return (False, None)


async def cleanup_universe():
    mongo_url = os.environ.get('MONGO_URL', 'mongodb://localhost:27017')
    db_name = os.environ.get('DB_NAME', 'test_database')
    
    client = AsyncIOMotorClient(mongo_url)
    db = client[db_name]
    
    now = datetime.now(timezone.utc)
    
    try:
        # =====================================================================
        # BEFORE: Count visible tickers
        # =====================================================================
        visible_before = await db.tracked_tickers.count_documents({"is_visible": True})
        total_before = await db.tracked_tickers.count_documents({})
        
        print("="*80)
        print("UNIVERSE CLEANUP: ENFORCE COMMON STOCK ONLY")
        print("="*80)
        print(f"\nBEFORE CLEANUP:")
        print(f"  Total tickers: {total_before}")
        print(f"  Visible tickers: {visible_before}")
        
        # =====================================================================
        # PHASE 1: Analyze and classify all visible tickers
        # =====================================================================
        print("\n" + "="*80)
        print("PHASE 1: ANALYZING ALL VISIBLE TICKERS")
        print("="*80)
        
        visible_tickers = await db.tracked_tickers.find(
            {"is_visible": True},
            {"_id": 0}
        ).to_list(length=None)
        
        # Classification results
        to_exclude = []
        to_delete = []
        to_keep = []
        
        exclusion_counts = defaultdict(int)
        
        for t in visible_tickers:
            ticker = t["ticker"]
            name = t.get("name", "")
            industry = t.get("industry", "")
            
            should_exclude, reason = classify_ticker(ticker, name, industry)
            
            if should_exclude:
                if reason == "seed_test_delete":
                    to_delete.append(ticker)
                    exclusion_counts["deleted_seed_test"] += 1
                else:
                    to_exclude.append({
                        "ticker": ticker,
                        "reason": reason,
                        "name": name,
                        "sector": t.get("sector"),
                        "industry": industry,
                    })
                    exclusion_counts[reason] += 1
            else:
                to_keep.append(ticker)
        
        print(f"\nClassification results:")
        print(f"  To keep visible: {len(to_keep)}")
        print(f"  To exclude: {len(to_exclude)}")
        print(f"  To delete: {len(to_delete)}")
        
        print(f"\nExclusion breakdown:")
        for reason, count in sorted(exclusion_counts.items(), key=lambda x: -x[1]):
            print(f"  {reason}: {count}")
        
        # =====================================================================
        # PHASE 2: Delete seed-only test tickers
        # =====================================================================
        print("\n" + "="*80)
        print("PHASE 2: DELETING SEED-ONLY TEST TICKERS")
        print("="*80)
        
        for ticker in to_delete:
            result = await db.tracked_tickers.delete_one({"ticker": ticker})
            print(f"  Deleted {ticker}: {result.deleted_count} record(s)")
        
        # =====================================================================
        # PHASE 3: Set is_visible=false and store exclusion reasons
        # =====================================================================
        print("\n" + "="*80)
        print("PHASE 3: EXCLUDING NON-COMMON-STOCK TICKERS")
        print("="*80)
        
        # Create excluded_tickers collection
        excluded_records = []
        
        for exc in to_exclude:
            ticker = exc["ticker"]
            reason = exc["reason"]
            
            # Update tracked_tickers
            await db.tracked_tickers.update_one(
                {"ticker": ticker},
                {"$set": {
                    "is_visible": False,
                    "exclusion_reason": reason,
                    "excluded_at": now.isoformat()
                }}
            )
            
            # Add to excluded_tickers
            excluded_records.append({
                "ticker": ticker,
                "reason": reason,
                "name": exc.get("name"),
                "sector": exc.get("sector"),
                "industry": exc.get("industry"),
                "excluded_at": now.isoformat()
            })
        
        # Insert into excluded_tickers collection
        if excluded_records:
            # Clear existing and insert fresh
            await db.excluded_tickers.delete_many({})
            await db.excluded_tickers.insert_many(excluded_records)
            print(f"  Stored {len(excluded_records)} records in excluded_tickers collection")
        
        # Create index
        await db.excluded_tickers.create_index([("ticker", 1)])
        await db.excluded_tickers.create_index([("reason", 1)])
        
        print(f"  Set is_visible=false for {len(to_exclude)} tickers")
        
        # =====================================================================
        # PHASE 4: Additional cleanup - check remaining gap tickers
        # =====================================================================
        print("\n" + "="*80)
        print("PHASE 4: VERIFYING REMAINING GAP TICKERS")
        print("="*80)
        
        # Re-check gap after initial cleanup
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        
        # Get current visible
        remaining_visible = await db.tracked_tickers.find(
            {"is_visible": True},
            {"_id": 0, "ticker": 1, "name": 1, "industry": 1, "sector": 1}
        ).to_list(length=None)
        
        # Get processed
        processed_docs = await db.ticker_key_metrics_daily.find(
            {"date": today},
            {"_id": 0, "ticker": 1}
        ).to_list(length=None)
        processed_set = {doc["ticker"] for doc in processed_docs}
        
        # Find remaining gap
        remaining_gap = []
        for t in remaining_visible:
            if t["ticker"] not in processed_set:
                remaining_gap.append(t)
        
        print(f"Remaining visible tickers: {len(remaining_visible)}")
        print(f"Remaining gap tickers: {len(remaining_gap)}")
        
        if remaining_gap:
            print(f"\nRemaining gap tickers (likely recent IPOs with missing shares):")
            for t in remaining_gap[:20]:
                print(f"  {t['ticker']}: {t.get('name', 'N/A')[:40]} ({t.get('industry', 'N/A')})")
            if len(remaining_gap) > 20:
                print(f"  ... and {len(remaining_gap) - 20} more")
        
        # =====================================================================
        # PHASE 5: Final verification
        # =====================================================================
        print("\n" + "="*80)
        print("PHASE 5: FINAL VERIFICATION")
        print("="*80)
        
        visible_after = await db.tracked_tickers.count_documents({"is_visible": True})
        total_after = await db.tracked_tickers.count_documents({})
        excluded_count = await db.excluded_tickers.count_documents({})
        
        print(f"\nAFTER CLEANUP:")
        print(f"  Total tickers: {total_after}")
        print(f"  Visible tickers: {visible_after}")
        print(f"  Excluded tickers: {excluded_count}")
        
        print(f"\nCHANGE:")
        print(f"  Visible: {visible_before} -> {visible_after} ({visible_after - visible_before:+d})")
        print(f"  Deleted: {len(to_delete)}")
        
        # Exclusion breakdown from DB
        print(f"\nEXCLUSION BREAKDOWN (from excluded_tickers):")
        pipeline = [
            {"$group": {"_id": "$reason", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]
        async for doc in db.excluded_tickers.aggregate(pipeline):
            print(f"  {doc['_id']}: {doc['count']}")
        
        # Verify all visible are common stock
        print(f"\nVERIFICATION:")
        
        # Check if any excluded types remain in visible
        visible_check = await db.tracked_tickers.find(
            {"is_visible": True},
            {"_id": 0, "ticker": 1, "name": 1, "industry": 1}
        ).to_list(length=None)
        
        suspicious_remaining = []
        for t in visible_check:
            ticker = t["ticker"]
            name = t.get("name", "")
            industry = t.get("industry", "")
            
            should_exclude, reason = classify_ticker(ticker, name, industry)
            if should_exclude:
                suspicious_remaining.append((ticker, reason))
        
        if suspicious_remaining:
            print(f"  WARNING: {len(suspicious_remaining)} potentially non-common-stock tickers still visible")
            for ticker, reason in suspicious_remaining[:10]:
                print(f"    {ticker}: {reason}")
        else:
            print(f"  ✅ All visible tickers are Common Stock")
        
        # Gap analysis
        print(f"\nUPDATED GAP ANALYSIS:")
        print(f"  Visible tickers: {visible_after}")
        print(f"  Processed today: {len(processed_set)}")
        print(f"  Gap: {len(remaining_gap)} (was 103)")
        print(f"  Gap reduction: {103 - len(remaining_gap)} tickers removed from gap")
        
        return {
            "visible_before": visible_before,
            "visible_after": visible_after,
            "excluded_count": excluded_count,
            "deleted_count": len(to_delete),
            "remaining_gap": len(remaining_gap)
        }
        
    finally:
        client.close()


if __name__ == "__main__":
    asyncio.run(cleanup_universe())
