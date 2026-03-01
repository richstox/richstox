#!/usr/bin/env python3
"""
RICHSTOX Peer Benchmarks Full Audit Report
============================================
READ-ONLY audit of peer benchmark computation coverage across all sectors and industries.

Methodology (matches production):
- USD-only filter (financial_currency: "USD")
- NO exclude-self (this is corpus health, not per-ticker view)
- Winsorization: 1-99 percentile
- Simple median (not weighted)

Output:
- peer_benchmarks_sectors.csv / .json
- peer_benchmarks_industries.csv / .json
- peer_benchmarks_summary.json
"""

import os
import sys
import json
import csv
import statistics
from datetime import datetime, timezone
from collections import defaultdict
from pymongo import MongoClient

# Config
MONGO_URL = os.environ.get("MONGO_URL", "mongodb://localhost:27017")
DB_NAME = os.environ.get("DB_NAME", "richstox_prod")
OUTPUT_DIR = "/app/backend/scripts"
MIN_PEERS_FOR_VALID_MEDIAN = 3

METRICS = ["pe", "ps", "pb", "ev_ebitda", "ev_revenue"]

def connect_db():
    client = MongoClient(MONGO_URL)
    return client[DB_NAME]

def winsorize_and_median(values):
    """
    Apply 1-99 percentile winsorization and compute simple median.
    Returns (median, was_insufficient) tuple.
    """
    if len(values) < MIN_PEERS_FOR_VALID_MEDIAN:
        # Insufficient peers - return None median
        if values:
            return statistics.median(values), True  # Still compute but flag as insufficient
        return None, True
    
    values_sorted = sorted(values)
    n = len(values_sorted)
    
    # 1st and 99th percentile indices
    p1_idx = max(0, int(n * 0.01))
    p99_idx = min(n - 1, int(n * 0.99))
    p1, p99 = values_sorted[p1_idx], values_sorted[p99_idx]
    
    # Winsorize: clamp values to [p1, p99]
    values_winsorized = [max(p1, min(p99, v)) for v in values]
    
    return statistics.median(values_winsorized), False

def compute_benchmark_stats(group_data):
    """
    Compute median and exclusion stats for a group of tickers.
    Returns dict with stats per metric.
    """
    stats = {}
    
    for metric in METRICS:
        values = []
        excluded_missing = 0
        excluded_non_positive = 0
        
        for t in group_data:
            val = t.get(metric)
            if val is None:
                excluded_missing += 1
            elif val <= 0:
                excluded_non_positive += 1
            else:
                values.append(val)
        
        # Compute median with winsorization
        median, insufficient = winsorize_and_median(values)
        
        stats[metric] = {
            "count": len(values),
            "median": round(median, 4) if median is not None else None,
            "excluded_missing": excluded_missing,
            "excluded_non_positive": excluded_non_positive,
            "excluded_insufficient_peers": 1 if insufficient and len(values) > 0 else 0,
        }
    
    return stats

def flatten_stats(stats, peer_count_total):
    """Flatten stats dict into a flat row for CSV."""
    row = {"peer_count_total": peer_count_total}
    
    for metric in METRICS:
        m_stats = stats.get(metric, {})
        row[f"{metric}_count"] = m_stats.get("count", 0)
        row[f"{metric}_median"] = m_stats.get("median") if m_stats.get("median") is not None else ""
        row[f"{metric}_excluded_missing"] = m_stats.get("excluded_missing", 0)
        row[f"{metric}_excluded_non_positive"] = m_stats.get("excluded_non_positive", 0)
        row[f"{metric}_excluded_insufficient_peers"] = m_stats.get("excluded_insufficient_peers", 0)
    
    return row

def calculate_exclusion_rate(row):
    """Calculate overall exclusion rate for a row."""
    total_possible = row.get("peer_count_total", 0) * len(METRICS)
    if total_possible == 0:
        return 0.0
    
    total_excluded = 0
    for metric in METRICS:
        total_excluded += row.get(f"{metric}_excluded_missing", 0)
        total_excluded += row.get(f"{metric}_excluded_non_positive", 0)
    
    return (total_excluded / total_possible) * 100

def get_primary_exclusion_reason(row):
    """Determine primary exclusion reason for a row."""
    missing_total = sum(row.get(f"{m}_excluded_missing", 0) for m in METRICS)
    non_positive_total = sum(row.get(f"{m}_excluded_non_positive", 0) for m in METRICS)
    
    if non_positive_total > missing_total:
        return "non_positive_value"
    elif missing_total > 0:
        return "missing_raw_data"
    else:
        return "none"

def get_min_peer_count(row):
    """Get minimum peer count across all metrics."""
    counts = [row.get(f"{m}_count", 0) for m in METRICS]
    return min(counts) if counts else 0

def get_affected_metrics(row, threshold=3):
    """Get list of metrics with peer_count below threshold."""
    affected = []
    for m in METRICS:
        if row.get(f"{m}_count", 0) < threshold:
            affected.append(m)
    return affected

def run_audit():
    """Run full peer benchmarks audit."""
    db = connect_db()
    
    print("=" * 70)
    print("PEER BENCHMARKS FULL AUDIT REPORT")
    print("=" * 70)
    print(f"Started: {datetime.now(timezone.utc).isoformat()}")
    print()
    
    # 1. Load all visible tickers
    print("Loading tickers...")
    all_visible = db.tracked_tickers.count_documents({"is_visible": True})
    
    # USD-only filter (matches production)
    tickers = list(db.tracked_tickers.find(
        {"is_visible": True, "financial_currency": "USD"},
        {"ticker": 1, "sector": 1, "industry": 1}
    ))
    total_usd = len(tickers)
    print(f"  Total visible: {all_visible}")
    print(f"  Total USD (used): {total_usd}")
    
    # 2. Load valuation metrics from cache
    print("Loading valuation metrics...")
    valuations = {}
    for doc in db.ticker_valuations_cache.find({}, {"ticker": 1, "current_metrics": 1}):
        valuations[doc["ticker"]] = doc.get("current_metrics", {})
    print(f"  Loaded metrics for {len(valuations)} tickers")
    
    # 3. Group by sector and industry
    print("Grouping tickers...")
    sector_groups = defaultdict(list)
    industry_groups = defaultdict(list)
    
    for t in tickers:
        ticker = t["ticker"]
        sector = t.get("sector")
        industry = t.get("industry")
        metrics = valuations.get(ticker, {})
        
        ticker_data = {
            "ticker": ticker,
            "pe": metrics.get("pe"),
            "ps": metrics.get("ps"),
            "pb": metrics.get("pb"),
            "ev_ebitda": metrics.get("ev_ebitda"),
            "ev_revenue": metrics.get("ev_revenue"),
        }
        
        if sector:
            sector_groups[sector].append(ticker_data)
        if industry and sector:
            industry_groups[(industry, sector)].append(ticker_data)
    
    print(f"  Sectors: {len(sector_groups)}")
    print(f"  Industries: {len(industry_groups)}")
    
    # 4. Compute sector benchmarks
    print("Computing sector benchmarks...")
    sector_report = []
    for sector, data in sorted(sector_groups.items()):
        stats = compute_benchmark_stats(data)
        row = {"sector": sector}
        row.update(flatten_stats(stats, len(data)))
        sector_report.append(row)
    
    # 5. Compute industry benchmarks
    print("Computing industry benchmarks...")
    industry_report = []
    for (industry, sector), data in sorted(industry_groups.items()):
        stats = compute_benchmark_stats(data)
        row = {"industry": industry, "sector": sector}
        row.update(flatten_stats(stats, len(data)))
        industry_report.append(row)
    
    # 6. Compute risk lists
    print("Computing risk analysis...")
    
    # Lowest peer count industries
    lowest_peer_count = sorted(
        industry_report,
        key=lambda x: get_min_peer_count(x)
    )[:20]
    
    lowest_peer_list = []
    for row in lowest_peer_count:
        min_count = get_min_peer_count(row)
        affected = get_affected_metrics(row, threshold=MIN_PEERS_FOR_VALID_MEDIAN)
        lowest_peer_list.append({
            "industry": row["industry"],
            "sector": row["sector"],
            "min_peer_count": min_count,
            "metrics_affected": affected
        })
    
    # Highest exclusion rate industries
    for row in industry_report:
        row["_exclusion_rate"] = calculate_exclusion_rate(row)
        row["_primary_reason"] = get_primary_exclusion_reason(row)
    
    highest_exclusion = sorted(
        industry_report,
        key=lambda x: x["_exclusion_rate"],
        reverse=True
    )[:20]
    
    highest_exclusion_list = []
    for row in highest_exclusion:
        highest_exclusion_list.append({
            "industry": row["industry"],
            "sector": row["sector"],
            "exclusion_rate_pct": round(row["_exclusion_rate"], 1),
            "primary_reason": row["_primary_reason"]
        })
    
    # Remove temp fields
    for row in industry_report:
        row.pop("_exclusion_rate", None)
        row.pop("_primary_reason", None)
    
    # 7. Save reports
    print("Saving reports...")
    
    # CSV headers
    sector_fields = ["sector", "peer_count_total"]
    industry_fields = ["industry", "sector", "peer_count_total"]
    for m in METRICS:
        for suffix in ["_count", "_median", "_excluded_missing", "_excluded_non_positive", "_excluded_insufficient_peers"]:
            sector_fields.append(f"{m}{suffix}")
            industry_fields.append(f"{m}{suffix}")
    
    # Sector CSV
    with open(f"{OUTPUT_DIR}/peer_benchmarks_sectors.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=sector_fields)
        writer.writeheader()
        writer.writerows(sector_report)
    
    # Sector JSON
    with open(f"{OUTPUT_DIR}/peer_benchmarks_sectors.json", "w") as f:
        json.dump(sector_report, f, indent=2)
    
    # Industry CSV
    with open(f"{OUTPUT_DIR}/peer_benchmarks_industries.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=industry_fields)
        writer.writeheader()
        writer.writerows(industry_report)
    
    # Industry JSON
    with open(f"{OUTPUT_DIR}/peer_benchmarks_industries.json", "w") as f:
        json.dump(industry_report, f, indent=2)
    
    # Summary JSON
    summary = {
        "audit_timestamp": datetime.now(timezone.utc).isoformat(),
        "total_visible_tickers": all_visible,
        "total_usd_tickers": total_usd,
        "total_sectors": len(sector_groups),
        "total_industries": len(industry_groups),
        "lowest_peer_count_industries": lowest_peer_list,
        "highest_exclusion_rate_industries": highest_exclusion_list
    }
    with open(f"{OUTPUT_DIR}/peer_benchmarks_summary.json", "w") as f:
        json.dump(summary, f, indent=2)
    
    # 8. Print console summary
    print()
    print("=" * 70)
    print("AUDIT RESULTS")
    print("=" * 70)
    print()
    print(f"Total visible tickers: {all_visible:,}")
    print(f"Total USD tickers: {total_usd:,}")
    print(f"Total sectors: {len(sector_groups)}")
    print(f"Total industries: {len(industry_groups)}")
    print()
    
    print("TOP 20 INDUSTRIES WITH LOWEST PEER COUNT:")
    for i, item in enumerate(lowest_peer_list, 1):
        affected_str = ", ".join(item["metrics_affected"]) if item["metrics_affected"] else "none"
        print(f"  {i:2}. {item['industry']} ({item['sector']}): min_count={item['min_peer_count']}, affected=[{affected_str}]")
    print()
    
    print("TOP 20 INDUSTRIES WITH HIGHEST EXCLUSION RATES:")
    for i, item in enumerate(highest_exclusion_list, 1):
        print(f"  {i:2}. {item['industry']} ({item['sector']}): {item['exclusion_rate_pct']:.1f}% excluded (primary: {item['primary_reason']})")
    print()
    
    print("FILES SAVED:")
    print(f"  - {OUTPUT_DIR}/peer_benchmarks_sectors.csv")
    print(f"  - {OUTPUT_DIR}/peer_benchmarks_sectors.json")
    print(f"  - {OUTPUT_DIR}/peer_benchmarks_industries.csv")
    print(f"  - {OUTPUT_DIR}/peer_benchmarks_industries.json")
    print(f"  - {OUTPUT_DIR}/peer_benchmarks_summary.json")
    print()
    print(f"Completed: {datetime.now(timezone.utc).isoformat()}")
    
    return summary, sector_report, industry_report

if __name__ == "__main__":
    run_audit()
