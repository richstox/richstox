"""
Peer Medians Audit Report
=========================
Read-only script that outputs a report showing the impact of P1 Policy
(USD-only filter) on peer median calculations.

Data Sources:
- tracked_tickers: Total visible tickers, financial_currency field
- peer_benchmarks: Stored medians (pe_median, ps_median, pb_median, ev_ebitda_median)

Output: Table sorted by Excluded (Non-USD) descending, top 20 + totals.

Run with: python scripts/audit_peer_medians.py
"""

import asyncio
import os
import sys
from datetime import datetime, timezone
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv
load_dotenv()


async def run_audit():
    """
    Generate peer medians audit report.
    READ-ONLY - does not modify any data.
    """
    client = AsyncIOMotorClient(os.environ.get('MONGO_URL'))
    db = client[os.environ.get('DB_NAME', 'richstox_prod')]
    
    print("=" * 140)
    print("PEER MEDIANS AUDIT REPORT - P1 Policy Impact Analysis")
    print("=" * 140)
    print(f"Generated: {datetime.now(timezone.utc).isoformat()}")
    print()
    print("DATA SOURCES:")
    print("  - tracked_tickers.is_visible: Total visible tickers per industry")
    print("  - tracked_tickers.financial_currency: USD/Non-USD/NULL classification")
    print("  - peer_benchmarks.benchmarks.*_median: Stored peer medians")
    print("  - peer_benchmarks.peer_count_total/peer_count_used: Before/after filter counts")
    print()
    
    # Step 1: Aggregate ticker counts by industry and currency
    # Source: tracked_tickers collection
    pipeline = [
        {"$match": {"is_visible": True}},
        {"$group": {
            "_id": {
                "industry": "$industry",
                "sector": "$sector"
            },
            "total": {"$sum": 1},
            "usd_count": {
                "$sum": {"$cond": [{"$eq": ["$financial_currency", "USD"]}, 1, 0]}
            },
            "non_usd_count": {
                "$sum": {"$cond": [
                    {"$and": [
                        {"$ne": ["$financial_currency", "USD"]},
                        {"$ne": ["$financial_currency", None]}
                    ]},
                    1, 0
                ]}
            },
            "null_count": {
                "$sum": {"$cond": [{"$eq": ["$financial_currency", None]}, 1, 0]}
            }
        }},
        {"$match": {"_id.industry": {"$ne": None}}}
    ]
    
    ticker_counts = {}
    async for doc in db.tracked_tickers.aggregate(pipeline):
        industry = doc["_id"]["industry"]
        ticker_counts[industry] = {
            "sector": doc["_id"]["sector"],
            "total": doc["total"],
            "usd": doc["usd_count"],
            "non_usd": doc["non_usd_count"],
            "null": doc["null_count"]
        }
    
    # Step 2: Get stored peer medians
    # Source: peer_benchmarks collection
    peer_medians = {}
    async for doc in db.peer_benchmarks.find({"industry": {"$ne": None}}):
        industry = doc.get("industry")
        benchmarks = doc.get("benchmarks", {})
        peer_medians[industry] = {
            "pe_median": benchmarks.get("pe_median"),
            "ps_median": benchmarks.get("ps_median"),
            "pb_median": benchmarks.get("pb_median"),
            "ev_ebitda_median": benchmarks.get("ev_ebitda_median"),
            "peer_count_total": doc.get("peer_count_total", 0),
            "peer_count_used": doc.get("peer_count_used", 0),
            "computed_at": doc.get("computed_at", "N/A")
        }
    
    # Step 3: Merge data and build report rows
    report_rows = []
    for industry, counts in ticker_counts.items():
        medians = peer_medians.get(industry, {})
        
        row = {
            "sector": counts["sector"] or "N/A",
            "industry": industry,
            "total": counts["total"],
            "usd": counts["usd"],
            "non_usd": counts["non_usd"],
            "null": counts["null"],
            "pe_median": medians.get("pe_median"),
            "ps_median": medians.get("ps_median"),
            "pb_median": medians.get("pb_median"),
            "ev_ebitda_median": medians.get("ev_ebitda_median"),
            "peers_insufficient": counts["usd"] < 3
        }
        report_rows.append(row)
    
    # Step 4: Sort by non_usd descending
    report_rows.sort(key=lambda x: x["non_usd"], reverse=True)
    
    # Step 5: Print top 20 + totals
    print("-" * 140)
    header = f"{'Sector':<28} {'Industry':<40} {'Total':>6} {'USD':>5} {'Non-USD':>8} {'NULL':>5} {'P/E':>8} {'P/S':>8} {'P/B':>8} {'EV/EBITDA':>10} {'Insuff':>6}"
    print(header)
    print("-" * 140)
    
    # Format helper
    def fmt_median(val):
        return f"{val:.2f}" if val is not None else "N/A"
    
    def fmt_flag(val):
        return "YES" if val else ""
    
    # Top 20 by exclusion
    for row in report_rows[:20]:
        sector_trunc = row['sector'][:27] if len(row['sector']) > 27 else row['sector']
        industry_trunc = row['industry'][:39] if len(row['industry']) > 39 else row['industry']
        line = f"{sector_trunc:<28} {industry_trunc:<40} {row['total']:>6} {row['usd']:>5} {row['non_usd']:>8} {row['null']:>5} {fmt_median(row['pe_median']):>8} {fmt_median(row['ps_median']):>8} {fmt_median(row['pb_median']):>8} {fmt_median(row['ev_ebitda_median']):>10} {fmt_flag(row['peers_insufficient']):>6}"
        print(line)
    
    print("-" * 140)
    
    # Totals
    totals = {
        "total": sum(r["total"] for r in report_rows),
        "usd": sum(r["usd"] for r in report_rows),
        "non_usd": sum(r["non_usd"] for r in report_rows),
        "null": sum(r["null"] for r in report_rows),
        "industries": len(report_rows),
        "insufficient": sum(1 for r in report_rows if r["peers_insufficient"])
    }
    
    print(f"{'TOTALS':<28} {'Industries: ' + str(totals['industries']):<40} {totals['total']:>6} {totals['usd']:>5} {totals['non_usd']:>8} {totals['null']:>5}")
    print()
    print(f"Industries with Peers Insufficient (< 3 USD tickers): {totals['insufficient']}")
    print()
    
    # Step 6: Summary statistics
    print("=" * 140)
    print("SUMMARY STATISTICS")
    print("=" * 140)
    print(f"Total Industries:                    {totals['industries']}")
    print(f"Total Visible Tickers:               {totals['total']}")
    print(f"  - USD (included in medians):       {totals['usd']} ({totals['usd']/totals['total']*100:.1f}%)")
    print(f"  - Non-USD (excluded):              {totals['non_usd']} ({totals['non_usd']/totals['total']*100:.1f}%)")
    print(f"  - NULL currency (excluded):        {totals['null']} ({totals['null']/totals['total']*100:.1f}%)")
    print(f"Industries with < 3 USD peers:       {totals['insufficient']}")
    print()
    
    # Top 10 non-USD currencies
    print("TOP NON-USD CURRENCIES (from tracked_tickers.financial_currency):")
    currency_pipeline = [
        {"$match": {"is_visible": True, "financial_currency": {"$nin": [None, "USD"]}}},
        {"$group": {"_id": "$financial_currency", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 10}
    ]
    async for doc in db.tracked_tickers.aggregate(currency_pipeline):
        print(f"  {doc['_id']}: {doc['count']}")
    
    print()
    
    # Industries with highest impact (most excluded as % of total)
    print("INDUSTRIES WITH HIGHEST EXCLUSION RATE (Non-USD + NULL as % of Total):")
    impact_rows = sorted(report_rows, key=lambda x: (x['non_usd'] + x['null']) / max(x['total'], 1), reverse=True)[:10]
    for row in impact_rows:
        excluded_pct = (row['non_usd'] + row['null']) / max(row['total'], 1) * 100
        print(f"  {row['industry'][:45]:<45} {excluded_pct:>5.1f}% excluded ({row['non_usd']} non-USD + {row['null']} NULL of {row['total']} total)")
    
    print()
    print("=" * 140)
    print("END OF REPORT")
    print("=" * 140)
    
    client.close()


if __name__ == "__main__":
    asyncio.run(run_audit())
