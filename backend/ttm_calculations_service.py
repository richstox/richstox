"""
RICHSTOX TTM Calculations Service
=================================
Computes Trailing Twelve Month (TTM) metrics from quarterly data.

=============================================================================
RAW FACTS ONLY. No precomputed metrics from EODHD.
=============================================================================

All metrics MUST be computed locally from raw financial statements.
Never use precomputed values from EODHD Highlights/Technicals sections.

Calculated metrics:
- Net Margin TTM: (sum(last 4Q net_income) / sum(last 4Q revenue)) * 100
- EPS TTM: sum(last 4Q diluted_eps)
- Revenue TTM: sum(last 4Q revenue)
- Net Income TTM: sum(last 4Q net_income)
- P/E Ratio (local): current_price / eps_ttm
"""

import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List

logger = logging.getLogger("richstox.ttm")


async def calculate_ttm_metrics(db, ticker: str) -> Dict[str, Any]:
    """
    Calculate TTM metrics from quarterly financials.
    
    Args:
        db: MongoDB database
        ticker: Stock ticker (e.g., "AAPL" or "AAPL.US")
    
    Returns:
        Dictionary with TTM metrics.
    """
    ticker_full = ticker if ticker.endswith(".US") else f"{ticker}.US"
    
    # Get last 4 quarters of financial data
    cursor = db.financials_cache.find(
        {"ticker": ticker_full, "period_type": "quarterly"},
        {"_id": 0}
    ).sort("period_date", -1).limit(4)
    
    quarters = await cursor.to_list(length=4)
    
    result = {
        "ticker": ticker_full,
        "quarters_available": len(quarters),
        "revenue_ttm": None,
        "net_income_ttm": None,
        "net_margin_ttm": None,
        "eps_ttm": None,
        "ebitda_ttm": None,
        "operating_income_ttm": None,
        "gross_profit_ttm": None,
        "free_cash_flow_ttm": None,
        "quarters_used": [],
    }
    
    if len(quarters) < 4:
        logger.warning(f"Only {len(quarters)} quarters available for {ticker_full}, need 4 for TTM")
        # Still try to calculate with available data
        if len(quarters) == 0:
            return result
    
    # Sum up the quarterly values
    revenues = []
    net_incomes = []
    eps_values = []
    ebitdas = []
    operating_incomes = []
    gross_profits = []
    free_cash_flows = []
    
    def safe_numeric(val):
        """Convert value to float, handling strings and None."""
        if val is None:
            return None
        try:
            return float(val)
        except (ValueError, TypeError):
            return None
    
    for q in quarters:
        result["quarters_used"].append(q.get("period_date"))
        
        rev = safe_numeric(q.get("revenue"))
        if rev is not None:
            revenues.append(rev)
        
        ni = safe_numeric(q.get("net_income"))
        if ni is not None:
            net_incomes.append(ni)
        
        eps = safe_numeric(q.get("diluted_eps"))
        if eps is not None:
            eps_values.append(eps)
        
        ebitda = safe_numeric(q.get("ebitda"))
        if ebitda is not None:
            ebitdas.append(ebitda)
        
        oi = safe_numeric(q.get("operating_income"))
        if oi is not None:
            operating_incomes.append(oi)
        
        gp = safe_numeric(q.get("gross_profit"))
        if gp is not None:
            gross_profits.append(gp)
        
        fcf = safe_numeric(q.get("free_cash_flow"))
        if fcf is not None:
            free_cash_flows.append(fcf)
    
    # Calculate TTM values
    if len(revenues) == 4:
        result["revenue_ttm"] = sum(revenues)
    
    if len(net_incomes) == 4:
        result["net_income_ttm"] = sum(net_incomes)
    
    if len(eps_values) == 4:
        result["eps_ttm"] = sum(eps_values)
    
    if len(ebitdas) == 4:
        result["ebitda_ttm"] = sum(ebitdas)
    
    if len(operating_incomes) == 4:
        result["operating_income_ttm"] = sum(operating_incomes)
    
    if len(gross_profits) == 4:
        result["gross_profit_ttm"] = sum(gross_profits)
    
    if len(free_cash_flows) == 4:
        result["free_cash_flow_ttm"] = sum(free_cash_flows)
    
    # Calculate Net Margin TTM
    if result["net_income_ttm"] is not None and result["revenue_ttm"] is not None and result["revenue_ttm"] != 0:
        result["net_margin_ttm"] = round((result["net_income_ttm"] / result["revenue_ttm"]) * 100, 2)
    
    return result


async def calculate_local_pe_ratio(current_price: float, eps_ttm: float) -> Optional[float]:
    """
    Calculate P/E ratio locally using current price and EPS TTM.
    
    Formula: price_last_close / eps_ttm
    
    Args:
        current_price: Latest stock price
        eps_ttm: Trailing 12-month EPS
    
    Returns:
        P/E ratio or None if invalid inputs
    """
    if current_price is None or eps_ttm is None or eps_ttm <= 0:
        return None
    
    return round(current_price / eps_ttm, 2)


async def update_company_ttm_metrics(db, ticker: str) -> Dict[str, Any]:
    """
    Calculate TTM metrics and update company_fundamentals_cache.
    
    Args:
        db: MongoDB database
        ticker: Stock ticker
    
    Returns:
        Summary of update operation.
    """
    ticker_full = ticker if ticker.endswith(".US") else f"{ticker}.US"
    
    # Calculate TTM metrics
    ttm = await calculate_ttm_metrics(db, ticker)
    
    update_fields = {
        "updated_at": datetime.now(timezone.utc),
    }
    
    if ttm["net_margin_ttm"] is not None:
        update_fields["net_margin_ttm"] = ttm["net_margin_ttm"]
    
    # Update the document
    result = await db.company_fundamentals_cache.update_one(
        {"ticker": ticker_full},
        {"$set": update_fields}
    )
    
    return {
        "ticker": ticker_full,
        "updated": result.modified_count > 0,
        "net_margin_ttm": ttm.get("net_margin_ttm"),
        "eps_ttm": ttm.get("eps_ttm"),
        "quarters_used": ttm.get("quarters_used", []),
    }


async def batch_update_ttm_metrics(db, limit: int = 500) -> Dict[str, Any]:
    """
    Update TTM metrics for all tickers in company_fundamentals_cache.
    
    Args:
        db: MongoDB database
        limit: Maximum number of tickers to process
    
    Returns:
        Summary of batch operation.
    """
    # Get all tickers
    cursor = db.company_fundamentals_cache.find(
        {},
        {"ticker": 1, "_id": 0}
    ).limit(limit)
    
    tickers = await cursor.to_list(length=limit)
    
    result = {
        "started_at": datetime.now(timezone.utc).isoformat(),
        "total_tickers": len(tickers),
        "updated": 0,
        "no_data": 0,
        "errors": 0,
    }
    
    for doc in tickers:
        ticker = doc.get("ticker")
        if not ticker:
            continue
        
        try:
            update_result = await update_company_ttm_metrics(db, ticker)
            if update_result.get("updated"):
                result["updated"] += 1
            elif update_result.get("net_margin_ttm") is None:
                result["no_data"] += 1
        except Exception as e:
            logger.error(f"Error updating TTM for {ticker}: {e}")
            result["errors"] += 1
    
    result["finished_at"] = datetime.now(timezone.utc).isoformat()
    
    return result


async def get_enhanced_stock_metrics(
    db,
    ticker: str,
    current_price: float = None
) -> Dict[str, Any]:
    """
    Get all enhanced metrics for a stock including TTM calculations.
    
    This is the main function used by stock-overview endpoint.
    
    Args:
        db: MongoDB database
        ticker: Stock ticker
        current_price: Current stock price (for local P/E calculation)
    
    Returns:
        Complete metrics dictionary with TTM values, local calculations, and peer comparison data.
    """
    ticker_full = ticker if ticker.endswith(".US") else f"{ticker}.US"
    
    # Get company fundamentals
    company = await db.company_fundamentals_cache.find_one(
        {"ticker": ticker_full},
        {"_id": 0}
    )
    
    if not company:
        return {"error": f"No fundamentals for {ticker_full}"}
    
    # Calculate fresh TTM metrics
    ttm = await calculate_ttm_metrics(db, ticker_full)
    
    # Get industry benchmark
    industry = company.get("industry")
    benchmark = None
    if industry:
        benchmark = await db.industry_benchmarks.find_one(
            {"industry": industry},
            {"_id": 0}
        )
    
    # Use provided price or from company cache
    price = current_price or company.get("price_last_close")
    
    # Calculate local P/E
    eps_ttm = ttm.get("eps_ttm") or company.get("eps_ttm")
    local_pe = None
    if price and eps_ttm and eps_ttm > 0:
        local_pe = round(price / eps_ttm, 2)
    
    # Build enhanced metrics
    metrics = {
        "ticker": ticker_full,
        
        # Basic info from company fundamentals
        "name": company.get("name"),
        "sector": company.get("sector"),
        "industry": industry,
        
        # Price
        "price_last_close": price,
        
        # Key metrics (prefer local calculations over EODHD)
        "market_cap": company.get("market_cap"),
        "enterprise_value": company.get("enterprise_value"),
        
        # EPS & P/E (local calculation)
        "eps_ttm": eps_ttm,
        "pe_ratio": local_pe or company.get("pe_ratio"),  # Prefer local, fallback to EODHD
        "pe_ratio_source": "local" if local_pe else "eodhd",
        
        # Other valuation ratios (from EODHD)
        "ps_ratio": company.get("ps_ratio"),
        "pb_ratio": company.get("pb_ratio"),
        "ev_ebitda": company.get("ev_ebitda"),
        "ev_revenue": company.get("ev_revenue"),
        "peg_ratio": company.get("peg_ratio"),
        
        # Profitability (TTM calculated locally)
        "net_margin_ttm": ttm.get("net_margin_ttm") or company.get("net_margin_ttm"),
        "profit_margin": company.get("profit_margin"),
        "operating_margin": company.get("operating_margin"),
        "roe": company.get("roe"),
        "roa": company.get("roa"),
        
        # TTM financial values
        "revenue_ttm": ttm.get("revenue_ttm") or company.get("revenue_ttm"),
        "net_income_ttm": ttm.get("net_income_ttm"),
        "ebitda_ttm": ttm.get("ebitda_ttm"),
        
        # Dividends (yield will be calculated separately from dividend_history)
        "dividend_yield": company.get("dividend_yield"),
        "dividend_yield_ttm": company.get("dividend_yield_ttm"),  # Updated by dividend sync
        "payout_ratio": company.get("payout_ratio"),
        "ex_dividend_date": company.get("ex_dividend_date"),
        
        # Risk metrics
        "beta": company.get("beta"),
        "fifty_two_week_high": company.get("fifty_two_week_high"),
        "fifty_two_week_low": company.get("fifty_two_week_low"),
        
        # Ownership
        "pct_insiders": company.get("pct_insiders"),
        "pct_institutions": company.get("pct_institutions"),
        
        # TTM calculation metadata
        "ttm_quarters_used": ttm.get("quarters_used", []),
        
        # Benchmark data (if available)
        "has_benchmark": benchmark is not None,
        "benchmark_industry": benchmark.get("industry") if benchmark else None,
        "benchmark_company_count": benchmark.get("company_count") if benchmark else None,
    }
    
    # Add benchmark medians if available
    if benchmark:
        metrics["benchmark_pe_median"] = benchmark.get("pe_ratio_median")
        metrics["benchmark_ps_median"] = benchmark.get("ps_ratio_median")
        metrics["benchmark_pb_median"] = benchmark.get("pb_ratio_median")
        metrics["benchmark_ev_ebitda_median"] = benchmark.get("ev_ebitda_median")
        metrics["benchmark_ev_revenue_median"] = benchmark.get("ev_revenue_median")
        metrics["benchmark_dividend_yield_median"] = benchmark.get("dividend_yield_median")
        metrics["benchmark_net_margin_median"] = benchmark.get("net_margin_ttm_median")
        metrics["benchmark_profit_margin_median"] = benchmark.get("profit_margin_median")
    
    return metrics
