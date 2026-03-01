"""
RICHSTOX Benchmarks & Calculations Service
==========================================
Implements P0 calculations:
1. P/E local calculation (price / eps_ttm)
2. Net Margin TTM (from financials_cache)
3. Dividend Yield TTM (from dividend_history)
4. Industry Benchmarks (median values per industry)
5. Valuation Score (0-100 gauge)

=============================================================================
RAW FACTS ONLY. No precomputed metrics from EODHD.
=============================================================================

All metrics (P/E, margins, yields, beta, 52W, MAs) MUST be computed locally
from raw financial statements + price data. Never use precomputed values
from EODHD Highlights/Technicals sections.

Color gradient strategy (soft, Google Sheets style):
- Light green: Below peer median (cheaper valuation)
- Neutral: In line with peer median
- Light red: Above peer median (higher valuation)
"""

import os
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional, List
import statistics

logger = logging.getLogger("richstox.benchmarks")


# ============================================================================
# COLOR GRADIENT CALCULATION
# ============================================================================

def calculate_gradient_color(value: float, peer_median: float, lower_is_better: bool = True) -> Dict[str, Any]:
    """
    Calculate soft gradient color based on deviation from peer median.
    
    Returns:
        {
            "color": "green" | "neutral" | "red",
            "intensity": 0-100 (0=white, 100=saturated),
            "deviation_pct": float,
            "label": "Below peers" | "In line" | "Above peers"
        }
    
    Color semantics (for metrics where lower = better, like P/E):
    - Below peer median → green (cheaper)
    - Above peer median → red (more expensive)
    
    For metrics where higher = better (like Net Margin):
    - Above peer median → green (better)
    - Below peer median → red (worse)
    """
    if value is None or peer_median is None or peer_median == 0:
        return {
            "color": "neutral",
            "intensity": 0,
            "deviation_pct": None,
            "label": "No peer data"
        }
    
    deviation_pct = ((value - peer_median) / abs(peer_median)) * 100
    abs_deviation = abs(deviation_pct)
    
    # Calculate intensity (0-100)
    # 0-10% deviation = 0-20 intensity (barely visible)
    # 10-25% deviation = 20-50 intensity (light pastel)
    # 25-50% deviation = 50-75 intensity (medium)
    # 50%+ deviation = 75-100 intensity (saturated but muted)
    if abs_deviation <= 10:
        intensity = abs_deviation * 2  # 0-20
    elif abs_deviation <= 25:
        intensity = 20 + (abs_deviation - 10) * 2  # 20-50
    elif abs_deviation <= 50:
        intensity = 50 + (abs_deviation - 25)  # 50-75
    else:
        intensity = min(75 + (abs_deviation - 50) * 0.5, 100)  # 75-100
    
    intensity = int(intensity)
    
    # Determine color based on direction
    if lower_is_better:
        # For P/E, P/S, P/B, EV/EBITDA - lower is better
        if deviation_pct < -10:
            color = "green"
            label = "Below peers"
        elif deviation_pct > 10:
            color = "red"
            label = "Above peers"
        else:
            color = "neutral"
            label = "In line"
    else:
        # For Net Margin, Dividend Yield - higher is better
        if deviation_pct > 10:
            color = "green"
            label = "Above peers"
        elif deviation_pct < -10:
            color = "red"
            label = "Below peers"
        else:
            color = "neutral"
            label = "In line"
    
    return {
        "color": color,
        "intensity": intensity,
        "deviation_pct": round(deviation_pct, 1),
        "label": label
    }


# ============================================================================
# TTM CALCULATIONS
# ============================================================================

async def calculate_net_margin_ttm(db, ticker: str) -> Optional[float]:
    """
    Calculate Net Margin TTM from last 4 quarters of financials.
    Net Margin = (sum(net_income_4Q) / sum(revenue_4Q)) * 100
    """
    ticker_full = ticker if ticker.endswith(".US") else f"{ticker}.US"
    
    # Get last 4 quarters
    quarters = await db.financials_cache.find(
        {"ticker": ticker_full, "period_type": "quarterly"},
        {"revenue": 1, "net_income": 1, "period_date": 1}
    ).sort("period_date", -1).limit(4).to_list(4)
    
    if len(quarters) < 4:
        return None
    
    total_revenue = sum(q.get("revenue") or 0 for q in quarters)
    total_net_income = sum(q.get("net_income") or 0 for q in quarters)
    
    if total_revenue == 0:
        return None
    
    margin = (total_net_income / total_revenue) * 100
    return round(max(-100, min(100, margin)), 2)  # Clamp


async def calculate_dividend_yield_ttm(db, ticker: str, current_price: float) -> Optional[float]:
    """
    Calculate Dividend Yield TTM from dividend_history.
    Dividend Yield = (sum(dividends_last_365_days) / current_price) * 100
    """
    if not current_price or current_price <= 0:
        return None
    
    ticker_full = ticker if ticker.endswith(".US") else f"{ticker}.US"
    one_year_ago = datetime.now(timezone.utc) - timedelta(days=365)
    
    # Get dividends from last year
    dividends = await db.dividend_history.find(
        {
            "ticker": ticker_full,
            "ex_date": {"$gte": one_year_ago.strftime("%Y-%m-%d")}
        },
        {"amount": 1}
    ).to_list(100)
    
    if not dividends:
        # Fallback: use forward dividend from fundamentals
        return None
    
    total_dividends = sum(d.get("amount") or 0 for d in dividends)
    
    if total_dividends <= 0:
        return 0.0
    
    yield_ttm = (total_dividends / current_price) * 100
    return round(yield_ttm, 4)


def calculate_pe_ratio_local(price: float, eps_ttm: float) -> Optional[float]:
    """
    Calculate P/E ratio locally from current price and EPS TTM.
    """
    if not price or not eps_ttm or eps_ttm <= 0:
        return None
    return round(price / eps_ttm, 2)


# ============================================================================
# INDUSTRY BENCHMARKS
# ============================================================================

async def build_industry_benchmarks(db) -> Dict[str, Any]:
    """
    Build industry_benchmarks table by aggregating median values
    for each industry from company_fundamentals_cache.
    
    Minimum 5 companies per industry required.
    """
    now = datetime.now(timezone.utc)
    
    result = {
        "started_at": now.isoformat(),
        "industries_processed": 0,
        "industries_with_data": 0,
        "total_tickers_used": 0,
        "skipped_insufficient_data": [],
    }
    
    # Get all unique industries with active tickers
    pipeline = [
        {"$match": {"industry": {"$ne": None, "$ne": ""}}},
        {"$group": {
            "_id": "$industry",
            "count": {"$sum": 1},
            "tickers": {"$push": "$ticker"}
        }},
        {"$match": {"count": {"$gte": 5}}},  # Min 5 companies
        {"$sort": {"count": -1}}
    ]
    
    industries = await db.company_fundamentals_cache.aggregate(pipeline).to_list(None)
    
    for industry_doc in industries:
        industry = industry_doc["_id"]
        tickers = industry_doc["tickers"]
        
        result["industries_processed"] += 1
        
        # Get all fundamentals for this industry
        companies = await db.company_fundamentals_cache.find(
            {"ticker": {"$in": tickers}},
            {
                "ticker": 1,
                "pe_ratio": 1,
                "ps_ratio": 1,
                "pb_ratio": 1,
                "ev_ebitda": 1,
                "ev_revenue": 1,
                "dividend_yield": 1,
                "profit_margin": 1,
                "beta": 1,
            }
        ).to_list(None)
        
        # Calculate medians for each metric
        def get_median(values):
            valid = [v for v in values if v is not None and v > 0]
            if len(valid) < 3:
                return None
            return round(statistics.median(valid), 4)
        
        pe_values = [c.get("pe_ratio") for c in companies]
        ps_values = [c.get("ps_ratio") for c in companies]
        pb_values = [c.get("pb_ratio") for c in companies]
        ev_ebitda_values = [c.get("ev_ebitda") for c in companies]
        ev_revenue_values = [c.get("ev_revenue") for c in companies]
        div_yield_values = [c.get("dividend_yield") for c in companies]
        margin_values = [c.get("profit_margin") for c in companies]
        beta_values = [c.get("beta") for c in companies]
        
        benchmark_doc = {
            "industry": industry,
            "company_count": len(companies),
            "pe_median": get_median(pe_values),
            "ps_median": get_median(ps_values),
            "pb_median": get_median(pb_values),
            "ev_ebitda_median": get_median(ev_ebitda_values),
            "ev_revenue_median": get_median(ev_revenue_values),
            "dividend_yield_median": get_median(div_yield_values),
            "profit_margin_median": get_median(margin_values),
            "beta_median": get_median(beta_values),
            "tickers_sample": tickers[:10],  # First 10 for reference
            "updated_at": now,
        }
        
        # Upsert
        await db.industry_benchmarks.update_one(
            {"industry": industry},
            {"$set": benchmark_doc},
            upsert=True
        )
        
        result["industries_with_data"] += 1
        result["total_tickers_used"] += len(companies)
    
    # Log industries with insufficient data
    insufficient = await db.company_fundamentals_cache.aggregate([
        {"$match": {"industry": {"$ne": None, "$ne": ""}}},
        {"$group": {"_id": "$industry", "count": {"$sum": 1}}},
        {"$match": {"count": {"$lt": 5}}},
    ]).to_list(None)
    
    result["skipped_insufficient_data"] = [
        {"industry": i["_id"], "count": i["count"]} 
        for i in insufficient
    ]
    
    result["finished_at"] = datetime.now(timezone.utc).isoformat()
    
    logger.info(
        f"Industry benchmarks built: {result['industries_with_data']} industries, "
        f"{result['total_tickers_used']} tickers used"
    )
    
    return result


async def get_industry_benchmark(db, industry: str) -> Optional[Dict[str, Any]]:
    """Get benchmark for a specific industry."""
    if not industry:
        return None
    
    benchmark = await db.industry_benchmarks.find_one(
        {"industry": industry},
        {"_id": 0}
    )
    
    return benchmark


# ============================================================================
# VALUATION SCORE (0-100 Gauge)
# ============================================================================

def calculate_valuation_score(
    company: Dict[str, Any],
    benchmark: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Calculate Valuation Score (0-100) based on comparison to industry benchmarks.
    
    Algorithm:
    - Base score = 50
    - For each metric, compare to industry median:
      - val < median * 0.9 → +10 (undervalued)
      - val > median * 1.1 → -10 (overvalued)
      - else → 0 (neutral)
    - Clamp final score to [0, 100]
    
    Status:
    - score > 60 → "Below peers" (potentially undervalued)
    - score < 40 → "Above peers" (potentially overvalued)
    - 40-60 → "In line"
    """
    if not benchmark:
        return {
            "score": None,
            "status": "No benchmark",
            "label": "Insufficient peer data",
            "details": {}
        }
    
    base_score = 50
    adjustments = []
    details = {}
    
    # Metrics where LOWER is better (valuation ratios)
    lower_better_metrics = [
        ("pe_ratio", "pe_median", "P/E"),
        ("ps_ratio", "ps_median", "P/S"),
        ("pb_ratio", "pb_median", "P/B"),
        ("ev_ebitda", "ev_ebitda_median", "EV/EBITDA"),
        ("ev_revenue", "ev_revenue_median", "EV/Revenue"),
    ]
    
    # Metrics where HIGHER is better
    higher_better_metrics = [
        ("dividend_yield", "dividend_yield_median", "Div Yield"),
        ("profit_margin", "profit_margin_median", "Net Margin"),
    ]
    
    for company_key, benchmark_key, label in lower_better_metrics:
        val = company.get(company_key)
        median = benchmark.get(benchmark_key)
        
        if val is not None and median is not None and median > 0:
            if val < median * 0.9:
                adjustments.append(10)
                details[label] = {"adjustment": +10, "reason": "Below peers"}
            elif val > median * 1.1:
                adjustments.append(-10)
                details[label] = {"adjustment": -10, "reason": "Above peers"}
            else:
                adjustments.append(0)
                details[label] = {"adjustment": 0, "reason": "In line"}
    
    for company_key, benchmark_key, label in higher_better_metrics:
        val = company.get(company_key)
        median = benchmark.get(benchmark_key)
        
        if val is not None and median is not None and median > 0:
            if val > median * 1.1:
                adjustments.append(10)
                details[label] = {"adjustment": +10, "reason": "Above peers"}
            elif val < median * 0.9:
                adjustments.append(-10)
                details[label] = {"adjustment": -10, "reason": "Below peers"}
            else:
                adjustments.append(0)
                details[label] = {"adjustment": 0, "reason": "In line"}
    
    if not adjustments:
        return {
            "score": None,
            "status": "No data",
            "label": "Insufficient metric data",
            "details": {}
        }
    
    final_score = base_score + sum(adjustments)
    final_score = max(0, min(100, final_score))  # Clamp
    
    # Determine status
    if final_score > 60:
        status = "below_peers"
        label = "Below peers"
    elif final_score < 40:
        status = "above_peers"
        label = "Above peers"
    else:
        status = "in_line"
        label = "In line"
    
    return {
        "score": final_score,
        "status": status,
        "label": label,
        "details": details,
        "peer_count": benchmark.get("company_count", 0)
    }


# ============================================================================
# ENRICHED STOCK OVERVIEW
# ============================================================================

async def get_enriched_stock_overview(
    db,
    ticker: str,
    lite: bool = True
) -> Dict[str, Any]:
    """
    Get stock overview with all P0 calculations and benchmark comparisons.
    
    Includes:
    - Company fundamentals
    - P/E calculated locally
    - Net Margin TTM
    - Dividend Yield TTM (if dividend_history available)
    - Industry benchmark comparison
    - Valuation Score (0-100)
    - Gradient colors for each metric
    """
    ticker_upper = ticker.upper()
    ticker_full = ticker_upper if ticker_upper.endswith(".US") else f"{ticker_upper}.US"
    
    # Get company fundamentals
    company = await db.company_fundamentals_cache.find_one(
        {"ticker": ticker_full},
        {"_id": 0}
    )
    
    if not company:
        company = await db.company_fundamentals_cache.find_one(
            {"code": ticker_upper},
            {"_id": 0}
        )
    
    if not company:
        return {"error": "Ticker not found", "ticker": ticker_full}
    
    # Get latest price
    latest_price_doc = await db.stock_prices.find_one(
        {"ticker": ticker_full},
        {"_id": 0},
        sort=[("date", -1)]
    )
    
    current_price = None
    price_data = None
    
    if latest_price_doc:
        current_price = latest_price_doc.get("adjusted_close") or latest_price_doc.get("close_price")
        
        # Get previous price for change calculation
        prev_price_doc = await db.stock_prices.find_one(
            {"ticker": ticker_full, "date": {"$lt": latest_price_doc.get("date")}},
            {"_id": 0},
            sort=[("date", -1)]
        )
        
        previous_price = current_price
        if prev_price_doc:
            previous_price = prev_price_doc.get("adjusted_close") or prev_price_doc.get("close_price") or current_price
        
        price_data = {
            "last_close": current_price,
            "previous_close": previous_price,
            "change": round(current_price - previous_price, 2) if current_price and previous_price else 0,
            "change_pct": round(((current_price - previous_price) / previous_price) * 100, 2) if previous_price else 0,
            "date": latest_price_doc.get("date"),
        }
    
    # Calculate P/E locally - NEVER use EODHD precomputed value
    # RAW FACTS ONLY: compute from price and eps_ttm from financial statements
    eps_ttm = company.get("eps_ttm")
    pe_local = calculate_pe_ratio_local(current_price, eps_ttm) if current_price and eps_ttm else None
    
    # Calculate Net Margin TTM - from raw financial statements
    net_margin_ttm = await calculate_net_margin_ttm(db, ticker_full)
    
    # Calculate Dividend Yield TTM - from dividend_history collection
    # RAW FACTS ONLY: never use EODHD dividend_yield
    div_yield_ttm = await calculate_dividend_yield_ttm(db, ticker_full, current_price)
    
    # Get industry benchmark
    industry = company.get("industry")
    benchmark = await get_industry_benchmark(db, industry)
    
    # Calculate Valuation Score
    # RAW FACTS ONLY: all metrics computed locally, no EODHD precomputed values
    valuation_data = {
        "pe_ratio": pe_local,
        "ps_ratio": None,  # TODO: compute locally from price and revenue
        "pb_ratio": None,  # TODO: compute locally from price and book value
        "ev_ebitda": None,  # TODO: compute locally
        "ev_revenue": None,  # TODO: compute locally
        "dividend_yield": div_yield_ttm,
        "profit_margin": net_margin_ttm,  # computed locally from financial statements
    }
    
    valuation_score = calculate_valuation_score(valuation_data, benchmark)
    
    # Build metrics with gradient colors
    metrics = {}
    
    if benchmark:
        # P/E
        metrics["pe_ratio"] = {
            "value": pe_local,
            "peer_median": benchmark.get("pe_median"),
            **calculate_gradient_color(pe_local, benchmark.get("pe_median"), lower_is_better=True)
        }
        
        # P/S
        metrics["ps_ratio"] = {
            "value": company.get("ps_ratio"),
            "peer_median": benchmark.get("ps_median"),
            **calculate_gradient_color(company.get("ps_ratio"), benchmark.get("ps_median"), lower_is_better=True)
        }
        
        # P/B
        metrics["pb_ratio"] = {
            "value": company.get("pb_ratio"),
            "peer_median": benchmark.get("pb_median"),
            **calculate_gradient_color(company.get("pb_ratio"), benchmark.get("pb_median"), lower_is_better=True)
        }
        
        # EV/EBITDA
        metrics["ev_ebitda"] = {
            "value": company.get("ev_ebitda"),
            "peer_median": benchmark.get("ev_ebitda_median"),
            **calculate_gradient_color(company.get("ev_ebitda"), benchmark.get("ev_ebitda_median"), lower_is_better=True)
        }
        
        # EV/Revenue
        metrics["ev_revenue"] = {
            "value": company.get("ev_revenue"),
            "peer_median": benchmark.get("ev_revenue_median"),
            **calculate_gradient_color(company.get("ev_revenue"), benchmark.get("ev_revenue_median"), lower_is_better=True)
        }
        
        # Dividend Yield (higher is better)
        metrics["dividend_yield_ttm"] = {
            "value": div_yield_ttm,
            "peer_median": benchmark.get("dividend_yield_median"),
            **calculate_gradient_color(div_yield_ttm, benchmark.get("dividend_yield_median"), lower_is_better=False)
        }
        
        # Net Margin (higher is better)
        metrics["net_margin_ttm"] = {
            "value": net_margin_ttm,
            "peer_median": benchmark.get("profit_margin_median"),
            **calculate_gradient_color(net_margin_ttm, benchmark.get("profit_margin_median"), lower_is_better=False)
        }
    else:
        # No benchmark - just values without comparison
        metrics = {
            "pe_ratio": {"value": pe_local, "peer_median": None, "color": "neutral", "intensity": 0, "label": "No peer data"},
            "ps_ratio": {"value": company.get("ps_ratio"), "peer_median": None, "color": "neutral", "intensity": 0, "label": "No peer data"},
            "pb_ratio": {"value": company.get("pb_ratio"), "peer_median": None, "color": "neutral", "intensity": 0, "label": "No peer data"},
            "ev_ebitda": {"value": company.get("ev_ebitda"), "peer_median": None, "color": "neutral", "intensity": 0, "label": "No peer data"},
            "ev_revenue": {"value": company.get("ev_revenue"), "peer_median": None, "color": "neutral", "intensity": 0, "label": "No peer data"},
            "dividend_yield_ttm": {"value": div_yield_ttm, "peer_median": None, "color": "neutral", "intensity": 0, "label": "No peer data"},
            "net_margin_ttm": {"value": net_margin_ttm, "peer_median": None, "color": "neutral", "intensity": 0, "label": "No peer data"},
        }
    
    # Build response
    response = {
        "ticker": ticker_full,
        "company": {
            "code": company.get("code"),
            "name": company.get("name"),
            "exchange": company.get("exchange"),
            "sector": company.get("sector"),
            "industry": company.get("industry"),
            "description": company.get("description"),
            "website": company.get("website"),
            "logo_url": company.get("logo_url"),
            "full_time_employees": company.get("full_time_employees"),
            "ipo_date": company.get("ipo_date"),
            "city": company.get("city"),
            "state": company.get("state"),
            "country_name": company.get("country_name"),
            "market_cap": company.get("market_cap"),
            "eps_ttm": eps_ttm,
            "beta": company.get("beta"),
            "fifty_two_week_high": company.get("fifty_two_week_high"),
            "fifty_two_week_low": company.get("fifty_two_week_low"),
            "pct_insiders": company.get("pct_insiders"),
            "pct_institutions": company.get("pct_institutions"),
        },
        "price": price_data,
        "valuation": {
            "score": valuation_score,
            "metrics": metrics,
            "peer_context": {
                "industry": industry,
                "peer_count": benchmark.get("company_count") if benchmark else 0,
                "has_benchmark": benchmark is not None,
            }
        },
        "lite_mode": lite,
    }
    
    # Add full data if not lite mode
    if not lite:
        # Financials
        quarterly = await db.financials_cache.find(
            {"ticker": ticker_full, "period_type": "quarterly"},
            {"_id": 0}
        ).sort("period_date", -1).limit(8).to_list(8)
        
        annual = await db.financials_cache.find(
            {"ticker": ticker_full, "period_type": "annual"},
            {"_id": 0}
        ).sort("period_date", -1).limit(4).to_list(4)
        
        response["financials"] = {"quarterly": quarterly, "annual": annual}
        
        # Earnings
        earnings = await db.earnings_history_cache.find(
            {"ticker": ticker_full},
            {"_id": 0}
        ).sort("quarter_date", -1).limit(12).to_list(12)
        
        response["earnings"] = earnings
        
        # Insider
        insider = await db.insider_activity_cache.find_one(
            {"ticker": ticker_full},
            {"_id": 0}
        )
        
        response["insider_activity"] = insider
    
    return response
