"""
RICHSTOX Visible Universe Migration
====================================
Implements permanent "Visible Universe" rule:

DEFINITIONS:
- Seed Universe: NYSE+NASDAQ, Type=="Common Stock"
- Price-eligible: has price data
- Visible Universe: price-eligible + sector non-empty + industry non-empty

FLAGS in tracked_tickers:
- is_seeded: from weekly seed (NYSE/NASDAQ Common Stock)
- has_price_data: appears in daily bulk prices
- has_classification: sector && industry non-empty
- is_visible: is_seeded && has_price_data && has_classification

EXCLUDED_TICKERS REASONS:
- NOT_IN_SEED_LIST
- NOT_COMMON_STOCK
- NO_PRICE_DATA
- MISSING_SECTOR_INDUSTRY
- DELISTED
- OTHER

Usage:
    python migrations/visible_universe.py
"""

import asyncio
import os
import logging
from datetime import datetime, timezone
from motor.motor_asyncio import AsyncIOMotorClient

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("visible_universe")

MONGO_URL = os.environ.get("MONGO_URL", "mongodb://localhost:27017")
DB_NAME = os.environ.get("DB_NAME", "test_database")

# =============================================================================
# VISIBLE UNIVERSE RULE (PERMANENT)
# =============================================================================
# Only tickers with is_visible=true may appear in the app.
# is_visible = is_seeded && has_price_data && has_classification
# =============================================================================


async def run_migration():
    """
    Migrate tracked_tickers to use Visible Universe flags.
    Populate excluded_tickers collection with reasons.
    """
    client = AsyncIOMotorClient(MONGO_URL)
    db = client[DB_NAME]
    
    logger.info("=" * 70)
    logger.info("VISIBLE UNIVERSE MIGRATION")
    logger.info("=" * 70)
    
    # Stats
    stats = {
        "total_tracked": 0,
        "seeded": 0,
        "has_price_data": 0,
        "has_classification": 0,
        "visible": 0,
        "excluded_by_reason": {
            "NOT_IN_SEED_LIST": 0,
            "NOT_COMMON_STOCK": 0,
            "NO_PRICE_DATA": 0,
            "MISSING_SECTOR_INDUSTRY": 0,
            "DELISTED": 0,
            "OTHER": 0,
        }
    }
    
    # Create excluded_tickers collection with index
    await db.excluded_tickers.create_index("symbol", unique=True)
    await db.excluded_tickers.create_index("excluded_reason")
    
    # Get all tracked tickers
    cursor = db.tracked_tickers.find({})
    all_tickers = await cursor.to_list(length=20000)
    stats["total_tracked"] = len(all_tickers)
    
    logger.info(f"Processing {len(all_tickers)} tracked tickers...")
    
    excluded_batch = []
    
    for ticker_doc in all_tickers:
        ticker = ticker_doc.get("ticker", "")
        symbol = ticker.replace(".US", "")
        exchange = ticker_doc.get("exchange", "")
        asset_type = ticker_doc.get("asset_type", "")
        sector = ticker_doc.get("sector") or ""
        industry = ticker_doc.get("industry") or ""
        is_delisted = ticker_doc.get("is_delisted", False)
        name = ticker_doc.get("name", "")
        
        # Use canonical visibility sieve from visibility_rules (DATA SUPREMACY MANIFESTO v1.0)
        from visibility_rules import compute_visibility
        
        is_visible, visibility_failed_reason = compute_visibility(ticker_doc)
        
        # Determine flags for funnel steps
        is_seeded = (
            exchange in ["NYSE", "NASDAQ"] and
            asset_type == "Common Stock"
        )
        
        # Check if has price data (via has_price_data flag or check stock_prices)
        has_price_data = ticker_doc.get("has_price_data", ticker_doc.get("is_active", False))
        
        # has_classification = sector AND industry are non-empty
        has_classification = bool(sector.strip()) and bool(industry.strip())
        
        # Update tracked_tickers with flags
        await db.tracked_tickers.update_one(
            {"ticker": ticker},
            {"$set": {
                "is_seeded": is_seeded,
                "has_price_data": has_price_data,
                "has_classification": has_classification,
                "is_visible": is_visible,
                "visibility_failed_reason": visibility_failed_reason,
                "visibility_updated_at": datetime.now(timezone.utc),
            }}
        )
        
        # Update stats
        if is_seeded:
            stats["seeded"] += 1
        if has_price_data:
            stats["has_price_data"] += 1
        if has_classification:
            stats["has_classification"] += 1
        if is_visible:
            stats["visible"] += 1
        
        # Determine exclusion reason
        if not is_visible:
            excluded_reason = None
            details = None
            
            if is_delisted:
                excluded_reason = "DELISTED"
                details = "Ticker is delisted"
            elif not is_seeded:
                if exchange not in ["NYSE", "NASDAQ"]:
                    excluded_reason = "NOT_IN_SEED_LIST"
                    details = f"Exchange: {exchange}"
                elif asset_type != "Common Stock":
                    excluded_reason = "NOT_COMMON_STOCK"
                    details = f"asset_type: {asset_type}"
                else:
                    excluded_reason = "NOT_IN_SEED_LIST"
                    details = "Unknown seed issue"
            elif not has_price_data:
                excluded_reason = "NO_PRICE_DATA"
                details = "No price data in stock_prices collection"
            elif not has_classification:
                excluded_reason = "MISSING_SECTOR_INDUSTRY"
                # Determine if it's a warrant/right/unit
                name_lower = name.lower()
                ticker_upper = ticker.upper()
                if 'warrant' in name_lower or ticker_upper.endswith('W.US') or ticker_upper.endswith('WS.US'):
                    details = "Warrant (name contains 'warrant' or ticker ends with W/WS)"
                elif 'right' in name_lower or ticker_upper.endswith('R.US'):
                    details = "Rights (name contains 'right' or ticker ends with R)"
                elif 'unit' in name_lower or ticker_upper.endswith('U.US'):
                    details = "Unit (name contains 'unit' or ticker ends with U)"
                elif 'note' in name_lower or 'senior' in name_lower or 'subordinate' in name_lower:
                    details = "Notes/Bonds (name contains 'note/senior/subordinate')"
                elif 'preferred' in name_lower:
                    details = "Preferred stock"
                else:
                    details = f"sector='{sector}', industry='{industry}'"
            else:
                excluded_reason = "OTHER"
                details = "Unknown exclusion reason"
            
            if excluded_reason:
                stats["excluded_by_reason"][excluded_reason] += 1
                
                excluded_batch.append({
                    "symbol": symbol,
                    "primary_ticker": ticker,
                    "exchange": exchange,
                    "asset_type": asset_type,
                    "name": name,
                    "excluded_reason": excluded_reason,
                    "details": details,
                    "sector": sector,
                    "industry": industry,
                    "last_checked_at": datetime.now(timezone.utc),
                })
    
    # Clear and repopulate excluded_tickers
    await db.excluded_tickers.delete_many({})
    if excluded_batch:
        await db.excluded_tickers.insert_many(excluded_batch)
    
    logger.info("=" * 70)
    logger.info("MIGRATION COMPLETE")
    logger.info("=" * 70)
    logger.info(f"Total tracked: {stats['total_tracked']}")
    logger.info(f"Seeded (NYSE/NASDAQ Common Stock): {stats['seeded']}")
    logger.info(f"Has price data: {stats['has_price_data']}")
    logger.info(f"Has classification (sector+industry): {stats['has_classification']}")
    logger.info(f"VISIBLE UNIVERSE: {stats['visible']}")
    logger.info("")
    logger.info("Excluded by reason:")
    for reason, count in stats["excluded_by_reason"].items():
        logger.info(f"  {reason}: {count}")
    
    # Create index on is_visible
    await db.tracked_tickers.create_index("is_visible")
    await db.tracked_tickers.create_index([("is_visible", 1), ("exchange", 1)])
    await db.tracked_tickers.create_index([("is_visible", 1), ("sector", 1)])
    await db.tracked_tickers.create_index([("is_visible", 1), ("industry", 1)])
    
    logger.info("")
    logger.info("Created indexes on is_visible")
    
    client.close()
    return stats


if __name__ == "__main__":
    result = asyncio.run(run_migration())
    print(f"\nFinal counts: {result}")
