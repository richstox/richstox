"""
One-Time Migration: Populate financial_currency field
======================================================
Iterates over all tracked_tickers with fundamentals data,
extracts currency using the new fallback logic, and sets
the financial_currency field.

Run once with: python scripts/migrate_financial_currency.py
"""

import asyncio
import os
import sys
from datetime import datetime, timezone
from collections import Counter

# Add backend to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv
load_dotenv()

from utils.currency_utils import extract_statement_currency


async def run_migration():
    """
    One-time migration to populate financial_currency field on all tracked_tickers.
    """
    client = AsyncIOMotorClient(os.environ.get('MONGO_URL'))
    db = client[os.environ.get('DB_NAME', 'richstox_prod')]
    
    print("=" * 70)
    print("ONE-TIME MIGRATION: Populate financial_currency field")
    print("=" * 70)
    print(f"Timestamp: {datetime.now(timezone.utc).isoformat()}")
    print()
    
    # Get all tracked_tickers with fundamentals
    cursor = db.tracked_tickers.find(
        {"fundamentals": {"$exists": True, "$ne": None}},
        {"_id": 1, "ticker": 1, "fundamentals": 1}
    )
    
    total = 0
    updated = 0
    currency_counts = Counter()
    errors = []
    
    print("Processing tickers...")
    
    async for doc in cursor:
        total += 1
        ticker = doc.get("ticker", "UNKNOWN")
        
        try:
            # Extract currency using new fallback logic
            currency = extract_statement_currency(doc.get("fundamentals"))
            
            # Track counts
            currency_counts[currency or "NULL"] += 1
            
            # Update the document
            result = await db.tracked_tickers.update_one(
                {"_id": doc["_id"]},
                {"$set": {"financial_currency": currency}}
            )
            
            if result.modified_count > 0 or result.matched_count > 0:
                updated += 1
            
            # Progress logging
            if total % 500 == 0:
                print(f"  Processed {total} tickers...")
                
        except Exception as e:
            errors.append(f"{ticker}: {str(e)}")
    
    print()
    print("=" * 70)
    print("MIGRATION COMPLETE")
    print("=" * 70)
    print(f"Total tickers processed: {total}")
    print(f"Tickers updated: {updated}")
    print()
    
    # Top currencies
    print("TOP 10 CURRENCIES FOUND:")
    for currency, count in currency_counts.most_common(10):
        pct = (count / total) * 100 if total > 0 else 0
        print(f"  {currency or 'NULL':>6}: {count:5d} ({pct:5.1f}%)")
    
    print()
    print(f"Total unique currencies: {len(currency_counts)}")
    print(f"Tickers with NULL currency: {currency_counts.get('NULL', 0)}")
    
    if errors:
        print()
        print(f"ERRORS ({len(errors)}):")
        for err in errors[:5]:
            print(f"  {err}")
        if len(errors) > 5:
            print(f"  ... and {len(errors) - 5} more")
    
    # Create index for fast queries
    print()
    print("Creating index on financial_currency...")
    await db.tracked_tickers.create_index("financial_currency")
    print("Index created.")
    
    client.close()
    
    return {
        "total": total,
        "updated": updated,
        "currency_counts": dict(currency_counts),
        "errors": len(errors)
    }


if __name__ == "__main__":
    asyncio.run(run_migration())
