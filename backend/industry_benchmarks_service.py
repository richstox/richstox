"""
RICHSTOX Industry Benchmarks Service
====================================
Computes and stores industry benchmark data for peer comparison.

Aggregation Job:
- Iterates through all active tickers
- Groups by industry
- Computes median for key metrics per industry (min 5 companies)

Collections:
- industry_benchmarks: Stores median metrics per industry

Metrics computed:
- P/E, P/S, P/B, EV/EBITDA, EV/Revenue
- Dividend Yield, Net Margin, Profit Margin
"""

import logging
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional
import statistics

logger = logging.getLogger("richstox.benchmarks")

# Minimum companies required to compute industry benchmark
MIN_COMPANIES_FOR_BENCHMARK = 5

# Metrics to compute benchmarks for
BENCHMARK_METRICS = [
    "pe_ratio",
    "ps_ratio", 
    "pb_ratio",
    "ev_ebitda",
    "ev_revenue",
    "dividend_yield",
    "profit_margin",
    "net_margin_ttm",
    "roe",
    "roa",
]


def compute_median(values: List[float], require_min: bool = True) -> Optional[float]:
    """Compute median, filtering out None and invalid values.
    
    Args:
        values: List of numeric values
        require_min: If True, requires MIN_COMPANIES_FOR_BENCHMARK. If False, computes with any data.
    """
    valid = [v for v in values if v is not None and isinstance(v, (int, float)) and not (isinstance(v, float) and (v != v))]  # filter NaN
    if len(valid) == 0:
        return None
    # Still compute median even with fewer companies - frontend will show "Limited peer set"
    return statistics.median(valid)


def compute_percentiles(values: List[float]) -> Dict[str, Optional[float]]:
    """Compute 25th, 50th (median), 75th percentiles.
    
    Now computes for any number of valid values (frontend shows "Limited peer set" if count < 5).
    """
    valid = sorted([v for v in values if v is not None and isinstance(v, (int, float)) and not (isinstance(v, float) and (v != v))])
    n = len(valid)
    
    if n == 0:
        return {"p25": None, "median": None, "p75": None}
    
    # Calculate percentiles even with small sample - frontend will indicate limited data
    return {
        "p25": valid[int(n * 0.25)] if n >= 4 else valid[0],
        "median": statistics.median(valid),
        "p75": valid[int(n * 0.75)] if n >= 4 else valid[-1],
    }


async def compute_industry_benchmarks(db) -> Dict[str, Any]:
    """
    Compute industry benchmarks from company_fundamentals_cache.
    
    Groups companies by industry, computes median for each metric.
    Only industries with MIN_COMPANIES_FOR_BENCHMARK+ companies get benchmarks.
    
    Returns:
        Summary of computed benchmarks.
    """
    now = datetime.now(timezone.utc)
    
    # Fetch all companies with fundamentals
    cursor = db.company_fundamentals_cache.find(
        {"industry": {"$ne": None}},
        {
            "_id": 0,
            "ticker": 1,
            "industry": 1,
            "sector": 1,
            **{metric: 1 for metric in BENCHMARK_METRICS}
        }
    )
    
    companies = await cursor.to_list(length=10000)
    
    if not companies:
        return {"error": "No companies with industry data found", "benchmarks_created": 0}
    
    # Group by industry
    by_industry: Dict[str, List[Dict]] = {}
    for company in companies:
        industry = company.get("industry")
        if not industry:
            continue
        if industry not in by_industry:
            by_industry[industry] = []
        by_industry[industry].append(company)
    
    # Compute benchmarks for each industry
    benchmarks_created = 0
    benchmarks_skipped = 0
    industry_details = []
    
    for industry, industry_companies in by_industry.items():
        company_count = len(industry_companies)
        
        if company_count < MIN_COMPANIES_FOR_BENCHMARK:
            benchmarks_skipped += 1
            continue
        
        # Get sector (most common in this industry)
        sectors = [c.get("sector") for c in industry_companies if c.get("sector")]
        sector = max(set(sectors), key=sectors.count) if sectors else None
        
        # Compute median for each metric
        benchmark_doc = {
            "industry": industry,
            "sector": sector,
            "company_count": company_count,
            "tickers": [c["ticker"] for c in industry_companies],
        }
        
        for metric in BENCHMARK_METRICS:
            values = [c.get(metric) for c in industry_companies]
            percentiles = compute_percentiles(values)
            
            benchmark_doc[f"{metric}_median"] = percentiles["median"]
            benchmark_doc[f"{metric}_p25"] = percentiles["p25"]
            benchmark_doc[f"{metric}_p75"] = percentiles["p75"]
            
            # Count how many companies have valid data for this metric
            valid_count = len([v for v in values if v is not None and isinstance(v, (int, float))])
            benchmark_doc[f"{metric}_count"] = valid_count
        
        benchmark_doc["created_at"] = now
        benchmark_doc["updated_at"] = now
        
        # Upsert to database
        await db.industry_benchmarks.update_one(
            {"industry": industry},
            {"$set": benchmark_doc},
            upsert=True
        )
        
        benchmarks_created += 1
        industry_details.append({
            "industry": industry,
            "sector": sector,
            "companies": company_count,
        })
    
    # Create indexes
    await db.industry_benchmarks.create_index("industry", unique=True)
    await db.industry_benchmarks.create_index("sector")
    
    logger.info(f"Computed benchmarks for {benchmarks_created} industries (skipped {benchmarks_skipped} with <{MIN_COMPANIES_FOR_BENCHMARK} companies)")
    
    return {
        "benchmarks_created": benchmarks_created,
        "benchmarks_skipped": benchmarks_skipped,
        "total_companies": len(companies),
        "total_industries": len(by_industry),
        "industries": sorted(industry_details, key=lambda x: x["companies"], reverse=True)[:20],
        "computed_at": now.isoformat(),
    }


async def get_industry_benchmark(db, industry: str) -> Optional[Dict[str, Any]]:
    """
    Get benchmark data for a specific industry.
    """
    benchmark = await db.industry_benchmarks.find_one(
        {"industry": industry},
        {"_id": 0}
    )
    return benchmark


async def get_benchmark_stats(db) -> Dict[str, Any]:
    """Get statistics about industry benchmarks."""
    total = await db.industry_benchmarks.count_documents({})
    
    if total == 0:
        return {
            "total_industries": 0,
            "message": "No benchmarks computed yet. Run /admin/benchmarks/compute first."
        }
    
    # Get top industries by company count
    cursor = db.industry_benchmarks.find(
        {},
        {"_id": 0, "industry": 1, "sector": 1, "company_count": 1}
    ).sort("company_count", -1).limit(10)
    
    top_industries = await cursor.to_list(length=10)
    
    # Get sectors
    sectors = await db.industry_benchmarks.distinct("sector")
    
    return {
        "total_industries": total,
        "total_sectors": len(sectors),
        "sectors": sectors,
        "top_industries_by_company_count": top_industries,
    }


def compute_valuation_score(
    company_metrics: Dict[str, Any],
    benchmark_metrics: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Compute Valuation Score (0-100) by comparing company metrics to industry benchmarks.
    
    Algorithm:
    - Base score: 50
    - For each metric:
        - If company value < industry median * 0.9 → +10 (undervalued)
        - If company value > industry median * 1.1 → -10 (overvalued)
        - Otherwise → 0 (in line)
    - Final score clamped to [0, 100]
    
    Returns:
        {
            "score": 0-100,
            "status": "below_peers" | "above_peers" | "in_line",
            "status_label": "Below peers" | "Above peers" | "In line",
            "metrics_comparison": {...},
            "net_adjustments": int
        }
    """
    base_score = 50
    adjustments = 0
    net_adjustments = 0  # Count of + vs - metrics
    
    comparison_metrics = [
        ("pe_ratio", "pe_ratio_median", "lower_better"),
        ("ps_ratio", "ps_ratio_median", "lower_better"),
        ("pb_ratio", "pb_ratio_median", "lower_better"),
        ("ev_ebitda", "ev_ebitda_median", "lower_better"),
        ("ev_revenue", "ev_revenue_median", "lower_better"),
        ("dividend_yield", "dividend_yield_median", "higher_better"),
        ("net_margin_ttm", "net_margin_ttm_median", "higher_better"),
        ("profit_margin", "profit_margin_median", "higher_better"),
    ]
    
    metrics_comparison = {}
    
    for company_key, benchmark_key, direction in comparison_metrics:
        company_val = company_metrics.get(company_key)
        benchmark_val = benchmark_metrics.get(benchmark_key)
        
        if company_val is None or benchmark_val is None or benchmark_val == 0:
            metrics_comparison[company_key] = {
                "company_value": company_val,
                "benchmark_value": benchmark_val,
                "deviation_pct": None,
                "status": "no_data",
                "adjustment": 0,
            }
            continue
        
        # Calculate deviation percentage
        deviation_pct = ((company_val - benchmark_val) / abs(benchmark_val)) * 100
        
        # Determine if this is favorable or unfavorable
        if direction == "lower_better":
            # For valuation ratios, lower is better (undervalued)
            if company_val < benchmark_val * 0.9:
                adjustment = 10
                status = "below_peers"
                net_adjustments += 1
            elif company_val > benchmark_val * 1.1:
                adjustment = -10
                status = "above_peers"
                net_adjustments -= 1
            else:
                adjustment = 0
                status = "in_line"
        else:
            # For profitability metrics, higher is better
            if company_val > benchmark_val * 1.1:
                adjustment = 10
                status = "above_peers"
                net_adjustments += 1
            elif company_val < benchmark_val * 0.9:
                adjustment = -10
                status = "below_peers"
                net_adjustments -= 1
            else:
                adjustment = 0
                status = "in_line"
        
        adjustments += adjustment
        
        metrics_comparison[company_key] = {
            "company_value": round(company_val, 2) if company_val else None,
            "benchmark_value": round(benchmark_val, 2) if benchmark_val else None,
            "deviation_pct": round(deviation_pct, 1),
            "status": status,
            "adjustment": adjustment,
            "direction": direction,
        }
    
    # Calculate final score
    final_score = max(0, min(100, base_score + adjustments))
    
    # Determine overall status
    # score > 60 means mostly undervalued (good)
    # score < 40 means mostly overvalued
    # 40-60 means fairly valued / in line
    if final_score > 60:
        overall_status = "below_peers"
        status_label = "Below peers"
    elif final_score < 40:
        overall_status = "above_peers"
        status_label = "Above peers"
    else:
        overall_status = "in_line"
        status_label = "In line"
    
    return {
        "score": final_score,
        "status": overall_status,
        "status_label": status_label,
        "net_adjustments": net_adjustments,
        "metrics_comparison": metrics_comparison,
    }


def compute_gradient_color(
    company_val: float,
    benchmark_val: float,
    direction: str = "lower_better"
) -> Dict[str, Any]:
    """
    Compute soft gradient color based on deviation from benchmark.
    
    Color semantics:
    - Light green → Below peer median (cheaper valuation for lower_better metrics)
    - Neutral grey/white → In line with peer median
    - Light red → Above peer median (higher valuation for lower_better metrics)
    
    Intensity rule:
    - 0-10% deviation = almost white (barely visible)
    - 10-25% deviation = light pastel
    - 25-50% deviation = medium tone
    - 50%+ deviation = more saturated (but still muted)
    
    Returns:
        {
            "deviation_pct": float,
            "intensity": "none" | "low" | "medium" | "high",
            "color_class": "neutral" | "positive" | "negative",
            "rgb": "rgb(r, g, b)"
        }
    """
    if company_val is None or benchmark_val is None or benchmark_val == 0:
        return {
            "deviation_pct": None,
            "intensity": "none",
            "color_class": "neutral",
            "rgb": "rgb(255, 255, 255)"
        }
    
    deviation_pct = ((company_val - benchmark_val) / abs(benchmark_val)) * 100
    abs_deviation = abs(deviation_pct)
    
    # Determine if this is positive or negative
    if direction == "lower_better":
        # For valuation ratios: below benchmark is good (green), above is concerning (red)
        is_positive = deviation_pct < 0
    else:
        # For profitability: above benchmark is good (green), below is concerning (red)
        is_positive = deviation_pct > 0
    
    # Determine intensity
    if abs_deviation < 10:
        intensity = "none"
        alpha = 0.05
    elif abs_deviation < 25:
        intensity = "low"
        alpha = 0.15
    elif abs_deviation < 50:
        intensity = "medium"
        alpha = 0.3
    else:
        intensity = "high"
        alpha = 0.5
    
    # Generate RGB (soft pastels)
    if intensity == "none":
        rgb = "rgb(255, 255, 255)"
        color_class = "neutral"
    elif is_positive:
        # Soft green: rgb(200, 230, 200) at full intensity
        r = int(255 - (55 * alpha * 2))
        g = int(255 - (25 * alpha * 2))
        b = int(255 - (55 * alpha * 2))
        rgb = f"rgb({r}, {g}, {b})"
        color_class = "positive"
    else:
        # Soft red: rgb(230, 200, 200) at full intensity
        r = int(255 - (25 * alpha * 2))
        g = int(255 - (55 * alpha * 2))
        b = int(255 - (55 * alpha * 2))
        rgb = f"rgb({r}, {g}, {b})"
        color_class = "negative"
    
    # Determine direction (above/below peers)
    # For lower_better: below benchmark means deviation_pct < 0
    # For higher_better: above benchmark means deviation_pct > 0
    if abs_deviation < 10:
        dir_status = "in_line"
    elif deviation_pct > 0:
        dir_status = "above"
    else:
        dir_status = "below"
    
    return {
        "deviation_pct": round(deviation_pct, 1),
        "intensity": intensity,
        "color_class": color_class,
        "rgb": rgb,
        "direction": dir_status,
    }
