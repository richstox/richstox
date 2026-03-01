"""
RICHSTOX Data Gaps Tracking Service
====================================
Tracks and reports missing data across all collections.

Collections monitored:
- company_fundamentals_cache (fundamentals)
- stock_prices (price)
- dividend_history (dividends)
- financials_cache (financials)
- earnings_history_cache (earnings)

Features:
- Real-time gap detection during stock-overview calls
- Admin endpoints for gap reports
- Daily summary generation
"""

import logging
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional
from enum import Enum

logger = logging.getLogger("richstox.data_gaps")


class DataField(str, Enum):
    """Critical data fields to track."""
    FUNDAMENTALS = "fundamentals"
    PRICE = "price"
    DIVIDENDS = "dividends"
    FINANCIALS = "financials"
    EARNINGS = "earnings"
    INSIDER = "insider"
    BENCHMARK = "benchmark"


async def log_data_gap(
    db,
    ticker: str,
    field: DataField,
    details: str = None
) -> None:
    """
    Log a data gap for a ticker.
    
    Args:
        db: MongoDB database
        ticker: Stock ticker
        field: Which data field is missing
        details: Optional details about the gap
    """
    ticker_full = ticker if ticker.endswith(".US") else f"{ticker}.US"
    now = datetime.now(timezone.utc)
    
    await db.data_gaps.update_one(
        {"ticker": ticker_full, "field": field.value},
        {
            "$set": {
                "ticker": ticker_full,
                "field": field.value,
                "details": details,
                "updated_at": now,
            },
            "$setOnInsert": {
                "created_at": now,
            }
        },
        upsert=True
    )


async def clear_data_gap(
    db,
    ticker: str,
    field: DataField
) -> None:
    """
    Clear a data gap when data becomes available.
    """
    ticker_full = ticker if ticker.endswith(".US") else f"{ticker}.US"
    await db.data_gaps.delete_one({"ticker": ticker_full, "field": field.value})


async def check_and_log_gaps(
    db,
    ticker: str,
    data: Dict[str, Any]
) -> Dict[str, bool]:
    """
    Check stock-overview response for gaps and log them.
    
    Args:
        db: MongoDB database
        ticker: Stock ticker
        data: The stock-overview response data
    
    Returns:
        Dictionary of field -> has_data boolean
    """
    ticker_full = ticker if ticker.endswith(".US") else f"{ticker}.US"
    gaps = {}
    
    # Check fundamentals
    company = data.get("company", {})
    has_fundamentals = company and company.get("name") is not None
    gaps["fundamentals"] = has_fundamentals
    if not has_fundamentals:
        await log_data_gap(db, ticker_full, DataField.FUNDAMENTALS, "Missing company data")
    else:
        await clear_data_gap(db, ticker_full, DataField.FUNDAMENTALS)
    
    # Check price
    price = data.get("price")
    has_price = price and price.get("last_close") is not None
    gaps["price"] = has_price
    if not has_price:
        await log_data_gap(db, ticker_full, DataField.PRICE, "Missing price data")
    else:
        await clear_data_gap(db, ticker_full, DataField.PRICE)
    
    # Check key metrics
    metrics = data.get("key_metrics", {})
    has_metrics = metrics and metrics.get("market_cap") is not None
    if not has_metrics:
        await log_data_gap(db, ticker_full, DataField.FUNDAMENTALS, "Missing key metrics")
    
    # Check financials (only in full mode)
    financials = data.get("financials")
    if financials is not None:  # Only check if not lite mode
        has_financials = financials and (
            (financials.get("quarterly") and len(financials["quarterly"]) > 0) or
            (financials.get("annual") and len(financials["annual"]) > 0)
        )
        gaps["financials"] = has_financials
        if not has_financials:
            await log_data_gap(db, ticker_full, DataField.FINANCIALS, "Missing financials data")
        else:
            await clear_data_gap(db, ticker_full, DataField.FINANCIALS)
    
    # Check earnings (only in full mode)
    earnings = data.get("earnings")
    if earnings is not None:
        has_earnings = earnings and len(earnings) > 0
        gaps["earnings"] = has_earnings
        if not has_earnings:
            await log_data_gap(db, ticker_full, DataField.EARNINGS, "Missing earnings data")
        else:
            await clear_data_gap(db, ticker_full, DataField.EARNINGS)
    
    # Check dividends (only in full mode)
    dividends = data.get("dividends")
    if dividends is not None:
        has_dividends = dividends and dividends.get("status") != "no_dividends"
        gaps["dividends"] = has_dividends
        # Don't log as gap if stock simply doesn't pay dividends
    
    # Check benchmark
    has_benchmark = data.get("has_benchmark", False)
    gaps["benchmark"] = has_benchmark
    if not has_benchmark:
        industry = company.get("industry") if company else None
        await log_data_gap(db, ticker_full, DataField.BENCHMARK, f"No benchmark for industry: {industry}")
    else:
        await clear_data_gap(db, ticker_full, DataField.BENCHMARK)
    
    return gaps


async def get_data_gaps_by_field(
    db,
    field: str,
    limit: int = 100
) -> Dict[str, Any]:
    """
    Get all tickers missing a specific data field.
    
    Args:
        db: MongoDB database
        field: Data field to check (fundamentals|price|dividends|financials|earnings|benchmark)
        limit: Maximum tickers to return
    
    Returns:
        Summary with affected tickers
    """
    cursor = db.data_gaps.find(
        {"field": field},
        {"_id": 0}
    ).sort("updated_at", -1).limit(limit)
    
    gaps = await cursor.to_list(length=limit)
    total = await db.data_gaps.count_documents({"field": field})
    
    return {
        "field": field,
        "total_gaps": total,
        "showing": len(gaps),
        "tickers": [g["ticker"] for g in gaps],
        "details": gaps,
    }


async def get_data_gaps_summary(db) -> Dict[str, Any]:
    """
    Get summary of all data gaps across all fields.
    
    Returns:
        Complete data gaps report.
    """
    # Count gaps by field
    pipeline = [
        {"$group": {"_id": "$field", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
    ]
    
    field_counts = await db.data_gaps.aggregate(pipeline).to_list(length=20)
    
    # Get total active tickers
    total_tickers = await db.company_fundamentals_cache.count_documents({})
    
    # Get gaps breakdown
    gaps_by_field = {item["_id"]: item["count"] for item in field_counts}
    
    # Calculate coverage percentages
    coverage = {}
    for field in DataField:
        gap_count = gaps_by_field.get(field.value, 0)
        covered = total_tickers - gap_count
        coverage[field.value] = {
            "total": total_tickers,
            "covered": covered,
            "gaps": gap_count,
            "coverage_pct": round((covered / total_tickers * 100), 2) if total_tickers > 0 else 0
        }
    
    # Get sample of worst affected tickers (most gaps)
    ticker_gaps_pipeline = [
        {"$group": {"_id": "$ticker", "gap_count": {"$sum": 1}, "fields": {"$push": "$field"}}},
        {"$sort": {"gap_count": -1}},
        {"$limit": 20}
    ]
    
    worst_tickers = await db.data_gaps.aggregate(ticker_gaps_pipeline).to_list(length=20)
    
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_tickers_in_db": total_tickers,
        "coverage_by_field": coverage,
        "worst_affected_tickers": worst_tickers,
        "total_gap_entries": await db.data_gaps.count_documents({}),
    }


async def scan_all_tickers_for_gaps(db, limit: int = None) -> Dict[str, Any]:
    """
    Scan all tickers in the database and identify data gaps.
    This is a comprehensive scan - use sparingly.
    
    Args:
        db: MongoDB database
        limit: Optional limit on tickers to scan
    
    Returns:
        Scan results summary.
    """
    # Get all tickers from fundamentals cache
    query = {}
    cursor = db.company_fundamentals_cache.find(
        query,
        {"_id": 0, "ticker": 1, "code": 1, "name": 1, "industry": 1}
    )
    
    if limit:
        cursor = cursor.limit(limit)
    
    tickers = await cursor.to_list(length=limit or 10000)
    
    results = {
        "scanned": 0,
        "missing_price": [],
        "missing_financials": [],
        "missing_earnings": [],
        "missing_dividends": [],
        "missing_benchmark": [],
    }
    
    for ticker_doc in tickers:
        ticker = ticker_doc.get("ticker") or ticker_doc.get("code")
        if not ticker:
            continue
        
        ticker_full = ticker if ticker.endswith(".US") else f"{ticker}.US"
        results["scanned"] += 1
        
        # Check price
        price = await db.stock_prices.find_one({"ticker": ticker_full})
        if not price:
            results["missing_price"].append(ticker_full)
            await log_data_gap(db, ticker_full, DataField.PRICE, "No price data in stock_prices")
        
        # Check financials
        financials = await db.financials_cache.find_one({"ticker": ticker_full})
        if not financials:
            results["missing_financials"].append(ticker_full)
            await log_data_gap(db, ticker_full, DataField.FINANCIALS, "No data in financials_cache")
        
        # Check earnings
        earnings = await db.earnings_history_cache.find_one({"ticker": ticker_full})
        if not earnings:
            results["missing_earnings"].append(ticker_full)
            await log_data_gap(db, ticker_full, DataField.EARNINGS, "No data in earnings_history_cache")
        
        # Check benchmark
        industry = ticker_doc.get("industry")
        if industry:
            benchmark = await db.industry_benchmarks.find_one({"industry": industry})
            if not benchmark:
                results["missing_benchmark"].append(ticker_full)
                await log_data_gap(db, ticker_full, DataField.BENCHMARK, f"No benchmark for: {industry}")
    
    # Create indexes
    await db.data_gaps.create_index("field")
    await db.data_gaps.create_index("ticker")
    await db.data_gaps.create_index([("field", 1), ("ticker", 1)], unique=True)
    
    return {
        "tickers_scanned": results["scanned"],
        "missing_price_count": len(results["missing_price"]),
        "missing_financials_count": len(results["missing_financials"]),
        "missing_earnings_count": len(results["missing_earnings"]),
        "missing_benchmark_count": len(results["missing_benchmark"]),
        "sample_missing_price": results["missing_price"][:10],
        "sample_missing_financials": results["missing_financials"][:10],
    }


async def generate_daily_report(db) -> Dict[str, Any]:
    """
    Generate daily data gaps report for logging/alerting.
    """
    summary = await get_data_gaps_summary(db)
    
    # Store report in ops_job_runs
    report = {
        "job_type": "data_gaps_daily_report",
        "started_at": datetime.now(timezone.utc),
        "finished_at": datetime.now(timezone.utc),
        "status": "completed",
        "result": summary,
    }
    
    await db.ops_job_runs.insert_one(report)
    
    logger.info(f"Daily data gaps report: {summary['total_gap_entries']} total gaps across {summary['total_tickers_in_db']} tickers")
    
    return summary
