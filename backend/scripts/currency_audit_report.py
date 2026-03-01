"""
Currency Audit Report (Read-Only, Memory-Lightweight)
=====================================================
Extracts authoritative currency from fundamentals.Financials.*.currency_symbol
Reports counts by currency and lists non-USD tickers.

BINDING: This is discovery only - does NOT change any filtering/valuation logic.
GUARDRAIL: Only projects the 3 specific currency fields + metadata, not full Financials blobs.
"""

import asyncio
import json
from motor.motor_asyncio import AsyncIOMotorClient
from datetime import datetime, timezone
import os
from typing import Dict, Any


async def run_currency_audit_report(db) -> Dict[str, Any]:
    """
    Generate currency audit report for all visible tickers.
    
    MEMORY-LIGHTWEIGHT: Only projects specific currency_symbol fields,
    not full Financials blobs.
    
    Returns:
        {
            "total_visible": 5662,
            "with_currency_data": 5500,
            "without_currency_data": 162,
            "currency_counts": {"USD": 5200, "JPY": 50, "EUR": 30, ...},
            "top_10_currencies": [["USD", 5200], ["JPY", 50], ...],
            "non_usd_count": 150,
            "non_usd_tickers": [
                {"ticker": "SONY.US", "currency": "JPY", "name": "Sony Group Corp", ...},
                ...
            ]
        }
    """
    print("=" * 70)
    print("CURRENCY AUDIT REPORT (Read-Only, Memory-Lightweight)")
    print("=" * 70)
    print(f"Timestamp: {datetime.now(timezone.utc).isoformat()}")
    print()
    
    total_visible = await db.tracked_tickers.count_documents({"is_visible": True})
    
    # GUARDRAIL: Only project the 3 specific currency fields + metadata
    # Do NOT load full Financials blobs
    projection = {
        "_id": 0,
        "ticker": 1,
        "name": 1,
        "sector": 1,
        "industry": 1,
        "fundamentals.Financials.Income_Statement.currency_symbol": 1,
        "fundamentals.Financials.Balance_Sheet.currency_symbol": 1,
        "fundamentals.Financials.Cash_Flow.currency_symbol": 1
    }
    
    currency_counts = {}
    non_usd_tickers = []
    without_currency = 0
    with_currency = 0
    
    # Stream through results to minimize memory
    cursor = db.tracked_tickers.find(
        {"is_visible": True, "fundamentals": {"$exists": True, "$ne": None}},
        projection
    )
    
    async for doc in cursor:
        ticker = doc.get("ticker", "")
        name = doc.get("name", "")
        sector = doc.get("sector", "")
        industry = doc.get("industry", "")
        
        financials = doc.get("fundamentals", {}).get("Financials", {})
        
        # Extract currency with priority: Income_Statement > Balance_Sheet > Cash_Flow
        currency = (
            financials.get("Income_Statement", {}).get("currency_symbol") or
            financials.get("Balance_Sheet", {}).get("currency_symbol") or
            financials.get("Cash_Flow", {}).get("currency_symbol")
        )
        
        if currency is None:
            without_currency += 1
            continue
        
        with_currency += 1
        
        # Count currencies
        currency_counts[currency] = currency_counts.get(currency, 0) + 1
        
        # Track non-USD
        if currency != "USD":
            non_usd_tickers.append({
                "ticker": ticker,
                "currency": currency,
                "name": name[:40] if name else "",
                "industry": industry or "",
                "sector": sector or ""
            })
    
    # Sort currency counts
    sorted_currencies = sorted(currency_counts.items(), key=lambda x: x[1], reverse=True)
    top_10 = sorted_currencies[:10]
    
    # Sort non-USD by currency then ticker
    non_usd_tickers.sort(key=lambda x: (x["currency"], x["ticker"]))
    
    # Print report
    print(f"Total visible tickers: {total_visible}")
    print(f"With currency data: {with_currency}")
    print(f"Without currency data: {without_currency}")
    print()
    
    print("--- TOP 10 CURRENCIES (by ticker count) ---")
    for currency, count in top_10:
        pct = (count / with_currency) * 100 if with_currency > 0 else 0
        print(f"  {currency}: {count} ({pct:.1f}%)")
    
    print()
    print(f"--- NON-USD TICKERS ({len(non_usd_tickers)} total) ---")
    for t in non_usd_tickers[:50]:  # First 50
        print(f"  {t['ticker']:<12} {t['currency']:<5} {t['name']:<40} {t['industry']}")
    
    if len(non_usd_tickers) > 50:
        print(f"  ... and {len(non_usd_tickers) - 50} more")
    
    result = {
        "audit_timestamp": datetime.now(timezone.utc).isoformat(),
        "total_visible": total_visible,
        "with_currency_data": with_currency,
        "without_currency_data": without_currency,
        "currency_counts": currency_counts,
        "top_10_currencies": [[c, n] for c, n in top_10],
        "non_usd_count": len(non_usd_tickers),
        "non_usd_tickers": non_usd_tickers
    }
    
    print()
    print("--- JSON OUTPUT ---")
    print(json.dumps(result, indent=2, default=str))
    
    return result


async def main():
    client = AsyncIOMotorClient(os.environ.get('MONGO_URL'))
    db = client['richstox_prod']
    
    await run_currency_audit_report(db)
    
    client.close()


if __name__ == "__main__":
    asyncio.run(main())
