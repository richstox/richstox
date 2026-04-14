# ⚠️ BINDING RULE: VISIBLE UNIVERSE FILTER
# ============================================
# The ONLY runtime filter for ticker visibility is: is_visible == True
# NO ad-hoc filters allowed (exchange, suffix, sector, industry, asset_type, etc.)
# Violation = data integrity breach = users see wrong tickers
# 
# Use VISIBLE_UNIVERSE_QUERY constant defined in server.py line 60
# or whitelist_service.py line 710
#
# If you need to filter tickers, add a NEW field to tracked_tickers schema
# and get explicit user approval FIRST.
# ============================================

# ================================================================================
# SCHEDULER-ONLY SERVICE - DO NOT IMPORT IN RUNTIME ROUTES
# ================================================================================
# This file computes daily key metrics for all visible tickers.
# Called by scheduler.py - Job A (runs first, before peer medians)
#
# Collections:
#   - ticker_key_metrics_daily: Per-ticker metrics (one row per ticker per day)
#   - peer_medians_daily: Industry/sector/market medians (computed after Job A)
# ================================================================================

"""
Key Metrics Service
===================
Computes and stores daily valuation metrics for all visible tickers.

Job A: compute_daily_key_metrics() - runs at 05:00 Prague time
Job B: compute_daily_peer_medians() - runs at 05:30 Prague time (after Job A)
"""

import os
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional
import statistics

logger = logging.getLogger("richstox.key_metrics")

# Minimum peer count before fallback
MIN_PEER_COUNT = 5

# FIX-2: Minimum dividend payers required for median_payers calculation
MIN_DIVIDEND_PAYERS = 5

# REMOVED: MIN_MARKET_CAP filter - now using all peers with winsorization
# MIN_MARKET_CAP = 100_000_000  # No longer used

# Winsorization percentiles (clip outliers)
WINSORIZE_LOW = 1   # 1st percentile
WINSORIZE_HIGH = 99  # 99th percentile

# =============================================================================
# N/A REASON CODES (standardized)
# =============================================================================
NA_REASON_CODES = {
    "unprofitable": "Unprofitable",           # Negative earnings/EBITDA
    "missing_shares": "Missing Data",          # No shares_outstanding
    "missing_debt_cash": "Missing Data",       # EV inputs incomplete
    "missing_revenue": "Missing Data",         # No revenue data
    "missing_book_value": "Missing Data",      # No book value data
    "insufficient_history": "Limited History", # < N data points
    "negative_value": "Negative Value",        # Calculated value is negative
}


def get_na_reason(code: str) -> str:
    """Get display text for N/A reason code."""
    return NA_REASON_CODES.get(code, "N/A")


def winsorize(values: List[float], low_pct: int = 1, high_pct: int = 99) -> List[float]:
    """
    Winsorize a list of values by clipping at percentiles.
    
    Args:
        values: List of numeric values
        low_pct: Lower percentile to clip at (default 1%)
        high_pct: Upper percentile to clip at (default 99%)
    
    Returns:
        Winsorized list with outliers clipped
    """
    if not values or len(values) < 3:
        return values
    
    sorted_vals = sorted(values)
    n = len(sorted_vals)
    
    low_idx = max(0, int(n * low_pct / 100))
    high_idx = min(n - 1, int(n * high_pct / 100))
    
    low_bound = sorted_vals[low_idx]
    high_bound = sorted_vals[high_idx]
    
    return [max(low_bound, min(high_bound, v)) for v in values]


def cap_weighted_median(values: List[float], weights: List[float]) -> Optional[float]:
    """
    Calculate cap-weighted median.
    
    Args:
        values: List of metric values
        weights: List of market cap weights (same length as values)
    
    Returns:
        Cap-weighted median value
    """
    if not values or not weights or len(values) != len(weights):
        return None
    
    # Remove any pairs with None or zero weight
    pairs = [(v, w) for v, w in zip(values, weights) if v is not None and w and w > 0]
    
    if not pairs:
        return None
    
    # Sort by value
    pairs.sort(key=lambda x: x[0])
    
    values_sorted = [p[0] for p in pairs]
    weights_sorted = [p[1] for p in pairs]
    
    # Calculate cumulative weights
    total_weight = sum(weights_sorted)
    if total_weight <= 0:
        return None
    
    cumsum = 0
    median_idx = 0
    
    for i, w in enumerate(weights_sorted):
        cumsum += w
        if cumsum >= total_weight / 2:
            median_idx = i
            break
    
    return values_sorted[median_idx]


async def compute_daily_key_metrics(db) -> Dict[str, Any]:
    """
    Job A: Compute and store key metrics for all visible tickers.
    
    Stores to ticker_key_metrics_daily collection.
    Must run BEFORE compute_daily_peer_medians().
    
    Schema for ticker_key_metrics_daily:
    {
        "ticker": "AAPL.US",
        "date": "2026-02-22",
        "price": 264.58,
        "shares_outstanding": 15000000000,
        "market_cap": 3968700000000,
        "pe_ttm": 33.5,
        "ps_ttm": 8.9,
        "pb": 42.5,
        "ev_ebitda_ttm": 25.3,
        "ev_revenue_ttm": 8.1,
        "sector": "Technology",
        "industry": "Consumer Electronics",
        "computed_at": "2026-02-22T05:00:00Z"
    }
    """
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    
    logger.info(f"Job A: Starting daily key metrics computation for {today}")
    
    # Get all visible tickers
    visible_tickers = await db.tracked_tickers.find(
        {"is_visible": True},
        {"_id": 0, "ticker": 1, "sector": 1, "industry": 1}
    ).to_list(length=10000)
    
    logger.info(f"Processing {len(visible_tickers)} visible tickers")
    
    processed = 0
    errors = 0
    
    for ticker_doc in visible_tickers:
        try:
            ticker = ticker_doc["ticker"]
            sector = ticker_doc.get("sector")
            industry = ticker_doc.get("industry")
            
            if not sector or not industry:
                continue
            
            # Compute metrics for this ticker
            metrics = await compute_ticker_metrics(db, ticker)
            
            if metrics is None:
                continue
            
            # Build document
            doc = {
                "ticker": ticker,
                "date": today,
                "price": metrics.get("price"),
                "shares_outstanding": metrics.get("shares_outstanding"),
                "market_cap": metrics.get("market_cap"),
                "pe_ttm": metrics.get("pe_ttm"),
                "ps_ttm": metrics.get("ps_ttm"),
                "pb": metrics.get("pb"),
                "ev_ebitda_ttm": metrics.get("ev_ebitda_ttm"),
                "ev_revenue_ttm": metrics.get("ev_revenue_ttm"),
                "sector": sector,
                "industry": industry,
                "computed_at": datetime.now(timezone.utc).isoformat()
            }
            
            # Upsert
            await db.ticker_key_metrics_daily.update_one(
                {"ticker": ticker, "date": today},
                {"$set": doc},
                upsert=True
            )
            
            processed += 1
            
        except Exception as e:
            logger.error(f"Error processing {ticker_doc.get('ticker')}: {e}")
            errors += 1
    
    # Create indexes
    await db.ticker_key_metrics_daily.create_index([("ticker", 1), ("date", -1)])
    await db.ticker_key_metrics_daily.create_index([("date", 1)])
    await db.ticker_key_metrics_daily.create_index([("industry", 1), ("date", 1)])
    await db.ticker_key_metrics_daily.create_index([("sector", 1), ("date", 1)])
    
    logger.info(f"Job A complete: {processed} tickers processed, {errors} errors")
    
    return {
        "status": "success",
        "date": today,
        "processed": processed,
        "errors": errors
    }


async def compute_ticker_metrics(db, ticker: str) -> Optional[Dict[str, Any]]:
    """
    Compute all key metrics for a single ticker from raw DB data.
    
    Returns None if essential data is missing.
    """
    symbol = ticker.replace(".US", "").upper()
    
    # Get latest price
    price_doc = await db.stock_prices.find_one(
        {"ticker": ticker},
        {"_id": 0, "close": 1, "date": 1},
        sort=[("date", -1)]
    )
    
    if not price_doc or not price_doc.get("close"):
        return None
    
    price = price_doc["close"]
    
    # Get fundamentals
    fundamentals = await db.company_fundamentals_cache.find_one(
        {"symbol": symbol},
        {"_id": 0}
    )
    
    if not fundamentals:
        return None
    
    # Extract shares outstanding
    shares_outstanding = extract_shares_outstanding(fundamentals)
    
    if not shares_outstanding:
        return None
    
    market_cap = price * shares_outstanding
    
    # Compute valuation multiples
    pe_ttm = compute_pe_ttm(fundamentals, price)
    ps_ttm = compute_ps_ttm(fundamentals, market_cap)
    pb = compute_pb(fundamentals, market_cap)
    ev_ebitda_ttm = compute_ev_ebitda_ttm(fundamentals, market_cap)
    ev_revenue_ttm = compute_ev_revenue_ttm(fundamentals, market_cap)
    
    return {
        "price": price,
        "shares_outstanding": shares_outstanding,
        "market_cap": market_cap,
        "pe_ttm": pe_ttm,
        "ps_ttm": ps_ttm,
        "pb": pb,
        "ev_ebitda_ttm": ev_ebitda_ttm,
        "ev_revenue_ttm": ev_revenue_ttm
    }


def extract_shares_outstanding(fundamentals: dict) -> Optional[float]:
    """
    Extract shares outstanding from fundamentals.
    
    Checks multiple locations in order of preference:
    1. shares_outstanding (direct value)
    2. shares_outstanding_quarterly (latest quarter)
    3. shares_outstanding_annual (latest year)
    """
    # Method 1: Direct shares_outstanding field
    shares = fundamentals.get("shares_outstanding")
    if shares:
        try:
            if isinstance(shares, (int, float)):
                return float(shares)
            elif isinstance(shares, dict):
                shares_val = shares.get("shares") or shares.get("value")
                if shares_val:
                    return float(shares_val)
            elif isinstance(shares, list) and len(shares) > 0:
                shares_val = shares[0].get("shares") if isinstance(shares[0], dict) else shares[0]
                if shares_val:
                    return float(shares_val)
        except (ValueError, TypeError):
            pass
    
    # Method 2: shares_outstanding_quarterly (get latest by dateFormatted)
    shares_q = fundamentals.get("shares_outstanding_quarterly", {})
    if shares_q and isinstance(shares_q, dict):
        try:
            # Find latest quarter by dateFormatted
            latest_entry = None
            latest_date = ""
            for key, entry in shares_q.items():
                if isinstance(entry, dict):
                    date_str = entry.get("dateFormatted", "")
                    if date_str > latest_date:
                        latest_date = date_str
                        latest_entry = entry
            
            if latest_entry:
                shares_val = latest_entry.get("shares")
                if shares_val:
                    return float(shares_val)
        except (ValueError, TypeError):
            pass
    
    # Method 3: shares_outstanding_annual (get latest by dateFormatted)
    shares_a = fundamentals.get("shares_outstanding_annual", {})
    if shares_a and isinstance(shares_a, dict):
        try:
            # Find latest year by dateFormatted
            latest_entry = None
            latest_date = ""
            for key, entry in shares_a.items():
                if isinstance(entry, dict):
                    date_str = entry.get("dateFormatted", "")
                    if date_str > latest_date:
                        latest_date = date_str
                        latest_entry = entry
            
            if latest_entry:
                shares_val = latest_entry.get("shares")
                if shares_val:
                    return float(shares_val)
        except (ValueError, TypeError):
            pass
    
    return None


def safe_float(val) -> float:
    """Safely convert value to float, return 0 if not possible."""
    if val is None:
        return 0
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0


def compute_pe_ttm(fundamentals: dict, price: float) -> Optional[float]:
    """
    Compute P/E TTM using multiple methods:
    1. earnings_history.epsActual (last 4 quarters) - most reliable
    2. dilutedEPS from income_statement_quarterly
    3. netIncome / shares_outstanding
    """
    # Method 1: Use earnings_history (most reliable EPS data)
    earnings_h = fundamentals.get("earnings_history", {})
    if earnings_h:
        try:
            # Sort by date and get latest 4 quarters with actual EPS
            sorted_quarters = []
            for key, entry in earnings_h.items():
                if isinstance(entry, dict) and entry.get("epsActual") is not None:
                    date_str = entry.get("date", key)
                    sorted_quarters.append((date_str, entry))
            
            sorted_quarters.sort(key=lambda x: x[0], reverse=True)
            
            # Get latest 4 with actual data
            valid_quarters = [q for q in sorted_quarters if q[1].get("epsActual") is not None][:4]
            
            if len(valid_quarters) >= 4:
                eps_ttm = sum(safe_float(q[1].get("epsActual")) for q in valid_quarters)
                if eps_ttm > 0:
                    pe = price / eps_ttm
                    if 0 < pe < 1000:  # Sanity check
                        return round(pe, 2)
        except Exception:
            pass
    
    # Method 2: Use dilutedEPS from income_statement_quarterly
    try:
        income = fundamentals.get("income_statement_quarterly", {})
        if income:
            sorted_quarters = sorted(income.keys(), reverse=True)[:4]
            if len(sorted_quarters) >= 4:
                eps_ttm = 0
                for q in sorted_quarters:
                    eps = safe_float(income[q].get("dilutedEPS"))
                    eps_ttm += eps
                
                if eps_ttm > 0:
                    pe = price / eps_ttm
                    if 0 < pe < 1000:
                        return round(pe, 2)
    except Exception:
        pass
    
    # Method 3: Compute from netIncome and shares_outstanding
    try:
        income = fundamentals.get("income_statement_quarterly", {})
        shares = extract_shares_outstanding(fundamentals)
        
        if income and shares and shares > 0:
            sorted_quarters = sorted(income.keys(), reverse=True)[:4]
            if len(sorted_quarters) >= 4:
                net_income_ttm = 0
                for q in sorted_quarters:
                    ni = safe_float(income[q].get("netIncome"))
                    net_income_ttm += ni
                
                if net_income_ttm > 0:
                    eps_ttm = net_income_ttm / shares
                    pe = price / eps_ttm
                    if 0 < pe < 1000:
                        return round(pe, 2)
    except Exception:
        pass
    
    return None


def compute_ps_ttm(fundamentals: dict, market_cap: float) -> Optional[float]:
    """Compute P/S TTM from last 4 quarters of revenue."""
    try:
        income = fundamentals.get("income_statement_quarterly", {})
        if not income:
            return None
        
        sorted_quarters = sorted(income.keys(), reverse=True)[:4]
        if len(sorted_quarters) < 4:
            return None
        
        revenue_ttm = 0
        for q in sorted_quarters:
            revenue = safe_float(income[q].get("totalRevenue"))
            revenue_ttm += revenue
        
        if revenue_ttm > 0:
            ps = market_cap / revenue_ttm
            if 0 < ps < 500:  # Sanity check
                return round(ps, 2)
    except Exception:
        pass
    return None


def compute_pb(fundamentals: dict, market_cap: float) -> Optional[float]:
    """Compute P/B from latest balance sheet book value."""
    try:
        balance = fundamentals.get("balance_sheet_quarterly", {})
        if not balance:
            return None
        
        latest_q = sorted(balance.keys(), reverse=True)[0]
        latest_bs = balance[latest_q]
        
        # Book value = Total Assets - Total Liabilities
        # Or use totalStockholderEquity if available
        book_value = safe_float(latest_bs.get("totalStockholderEquity"))
        
        if not book_value:
            total_assets = safe_float(latest_bs.get("totalAssets"))
            total_liab = safe_float(latest_bs.get("totalLiab") or latest_bs.get("totalLiabilities"))
            book_value = total_assets - total_liab
        
        if book_value > 0:
            pb = market_cap / book_value
            if 0 < pb < 500:  # Sanity check
                return round(pb, 2)
    except Exception:
        pass
    return None


def compute_ev_ebitda_ttm(fundamentals: dict, market_cap: float) -> Optional[float]:
    """Compute EV/EBITDA TTM."""
    try:
        balance = fundamentals.get("balance_sheet_quarterly", {})
        income = fundamentals.get("income_statement_quarterly", {})
        cash_flow = fundamentals.get("cash_flow_quarterly", {})
        
        if not balance or not income:
            return None
        
        # Get latest balance sheet for debt/cash
        latest_q = sorted(balance.keys(), reverse=True)[0]
        latest_bs = balance[latest_q]
        
        short_debt = safe_float(latest_bs.get("shortLongTermDebt"))
        long_debt = safe_float(latest_bs.get("longTermDebt"))
        total_debt = short_debt + long_debt
        cash = safe_float(latest_bs.get("cash") or latest_bs.get("cashAndCashEquivalentsAtCarryingValue"))
        
        ev = market_cap + total_debt - cash
        
        # Calculate EBITDA TTM
        sorted_quarters = sorted(income.keys(), reverse=True)[:4]
        if len(sorted_quarters) < 4:
            return None
        
        ebitda_ttm = 0
        for q in sorted_quarters:
            op_income = safe_float(income[q].get("operatingIncome"))
            cf_q = cash_flow.get(q, {}) if cash_flow else {}
            depreciation = safe_float(cf_q.get("depreciation") or cf_q.get("depreciationAndAmortization"))
            ebitda_ttm += op_income + depreciation
        
        if ebitda_ttm > 0:
            ratio = ev / ebitda_ttm
            if 0 < ratio < 500:  # Sanity check
                return round(ratio, 2)
    except Exception:
        pass
    return None


def compute_ev_revenue_ttm(fundamentals: dict, market_cap: float) -> Optional[float]:
    """Compute EV/Revenue TTM."""
    try:
        balance = fundamentals.get("balance_sheet_quarterly", {})
        income = fundamentals.get("income_statement_quarterly", {})
        
        if not balance or not income:
            return None
        
        # Get latest balance sheet for debt/cash
        latest_q = sorted(balance.keys(), reverse=True)[0]
        latest_bs = balance[latest_q]
        
        short_debt = safe_float(latest_bs.get("shortLongTermDebt"))
        long_debt = safe_float(latest_bs.get("longTermDebt"))
        total_debt = short_debt + long_debt
        cash = safe_float(latest_bs.get("cash") or latest_bs.get("cashAndCashEquivalentsAtCarryingValue"))
        
        ev = market_cap + total_debt - cash
        
        # Calculate Revenue TTM
        sorted_quarters = sorted(income.keys(), reverse=True)[:4]
        if len(sorted_quarters) < 4:
            return None
        
        revenue_ttm = 0
        for q in sorted_quarters:
            revenue = safe_float(income[q].get("totalRevenue"))
            revenue_ttm += revenue
        
        if revenue_ttm > 0:
            ratio = ev / revenue_ttm
            if 0 < ratio < 100:  # Sanity check
                return round(ratio, 2)
    except Exception:
        pass
    return None


async def compute_daily_peer_medians(db) -> Dict[str, Any]:
    """
    Job B: Compute and store peer medians from ticker_key_metrics_daily.
    
    Uses WINSORIZED CAP-WEIGHTED MEDIAN:
    - NO market cap filter (all peers included)
    - Outliers clipped at 1st/99th percentile
    - Median weighted by market cap
    
    Must run AFTER compute_daily_key_metrics() (Job A).
    
    Schema for peer_medians_daily:
    {
        "date": "2026-02-22",
        "group_type": "industry",  # "industry", "sector", or "market"
        "group_name": "Consumer Electronics",
        "metric": "pe_ttm",
        "median_value": 25.3,
        "peer_count": 15,  # Total peers in industry
        "computed_at": "2026-02-22T05:30:00Z"
    }
    """
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    
    logger.info(f"Job B: Starting peer medians computation for {today}")
    
    # Get all metrics from today (NO market cap filter)
    all_metrics = await db.ticker_key_metrics_daily.find(
        {"date": today},
        {"_id": 0}
    ).to_list(length=10000)
    
    if not all_metrics:
        logger.warning("No metrics found for today. Job A may not have run yet.")
        return {"status": "error", "reason": "no_data"}
    
    logger.info(f"Computing medians from {len(all_metrics)} tickers (no market cap filter)")
    
    # Metrics to compute
    metric_names = ["pe_ttm", "ps_ttm", "pb", "ev_ebitda_ttm", "ev_revenue_ttm"]
    
    # Group by industry (with market cap for weighting)
    industries = {}
    sectors = {}
    market_data = {m: {"values": [], "weights": []} for m in metric_names}
    
    for m in all_metrics:
        industry = m.get("industry")
        sector = m.get("sector")
        market_cap = m.get("market_cap", 0) or 0
        
        if industry:
            if industry not in industries:
                industries[industry] = {metric: {"values": [], "weights": []} for metric in metric_names}
            for metric in metric_names:
                val = m.get(metric)
                if val is not None and val > 0:
                    industries[industry][metric]["values"].append(val)
                    industries[industry][metric]["weights"].append(market_cap)
        
        if sector:
            if sector not in sectors:
                sectors[sector] = {metric: {"values": [], "weights": []} for metric in metric_names}
            for metric in metric_names:
                val = m.get(metric)
                if val is not None and val > 0:
                    sectors[sector][metric]["values"].append(val)
                    sectors[sector][metric]["weights"].append(market_cap)
        
        # Market-wide
        for metric in metric_names:
            val = m.get(metric)
            if val is not None and val > 0:
                market_data[metric]["values"].append(val)
                market_data[metric]["weights"].append(market_cap)
    
    # Store medians
    stored = 0
    
    # Industry medians (winsorized + cap-weighted)
    for industry, metrics in industries.items():
        for metric, data in metrics.items():
            values = data["values"]
            weights = data["weights"]
            
            if len(values) >= MIN_PEER_COUNT:
                # Winsorize outliers
                winsorized_values = winsorize(values, WINSORIZE_LOW, WINSORIZE_HIGH)
                
                # Calculate cap-weighted median
                median_val = cap_weighted_median(winsorized_values, weights)
                
                if median_val is not None:
                    await db.peer_medians_daily.update_one(
                        {"date": today, "group_type": "industry", "group_name": industry, "metric": metric},
                        {"$set": {
                            "date": today,
                            "group_type": "industry",
                            "group_name": industry,
                            "metric": metric,
                            "median_value": round(median_val, 2),
                            "peer_count": len(values),  # Total peers in industry
                            "computed_at": datetime.now(timezone.utc).isoformat()
                        }},
                        upsert=True
                    )
                    stored += 1
    
    # Sector medians (winsorized + cap-weighted)
    for sector, metrics in sectors.items():
        for metric, data in metrics.items():
            values = data["values"]
            weights = data["weights"]
            
            if len(values) >= MIN_PEER_COUNT:
                winsorized_values = winsorize(values, WINSORIZE_LOW, WINSORIZE_HIGH)
                median_val = cap_weighted_median(winsorized_values, weights)
                
                if median_val is not None:
                    await db.peer_medians_daily.update_one(
                        {"date": today, "group_type": "sector", "group_name": sector, "metric": metric},
                        {"$set": {
                            "date": today,
                            "group_type": "sector",
                            "group_name": sector,
                            "metric": metric,
                            "median_value": round(median_val, 2),
                            "peer_count": len(values),
                            "computed_at": datetime.now(timezone.utc).isoformat()
                        }},
                        upsert=True
                    )
                    stored += 1
    
    # Market medians (winsorized + cap-weighted)
    for metric, data in market_data.items():
        values = data["values"]
        weights = data["weights"]
        
        if len(values) >= MIN_PEER_COUNT:
            winsorized_values = winsorize(values, WINSORIZE_LOW, WINSORIZE_HIGH)
            median_val = cap_weighted_median(winsorized_values, weights)
            
            if median_val is not None:
                await db.peer_medians_daily.update_one(
                    {"date": today, "group_type": "market", "group_name": "US", "metric": metric},
                    {"$set": {
                        "date": today,
                        "group_type": "market",
                        "group_name": "US",
                        "metric": metric,
                        "median_value": round(median_val, 2),
                        "peer_count": len(values),
                        "computed_at": datetime.now(timezone.utc).isoformat()
                    }},
                    upsert=True
                )
                stored += 1
    
    # Create indexes
    await db.peer_medians_daily.create_index([("date", 1), ("group_type", 1), ("group_name", 1), ("metric", 1)])
    
    logger.info(f"Job B complete: {stored} medians stored")
    
    return {
        "status": "success",
        "date": today,
        "medians_stored": stored,
        "industries": len(industries),
        "sectors": len(sectors)
    }


async def get_peer_median_from_daily(db, industry: str, sector: str, metric: str, date: str = None) -> tuple:
    """
    Get peer median for a metric, with fallback logic:
    1. Industry (if peer_count >= MIN_PEER_COUNT)
    2. Sector (if peer_count >= MIN_PEER_COUNT)
    3. Market
    
    Returns: (median_value, peer_count, group_type)
    """
    if date is None:
        date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    
    # Try industry first
    if industry:
        doc = await db.peer_medians_daily.find_one(
            {"date": date, "group_type": "industry", "group_name": industry, "metric": metric},
            {"_id": 0}
        )
        if doc and doc.get("peer_count", 0) >= MIN_PEER_COUNT:
            return (doc["median_value"], doc["peer_count"], "industry")
    
    # Fallback to sector
    if sector:
        doc = await db.peer_medians_daily.find_one(
            {"date": date, "group_type": "sector", "group_name": sector, "metric": metric},
            {"_id": 0}
        )
        if doc and doc.get("peer_count", 0) >= MIN_PEER_COUNT:
            return (doc["median_value"], doc["peer_count"], "sector")
    
    # Fallback to market
    doc = await db.peer_medians_daily.find_one(
        {"date": date, "group_type": "market", "group_name": "US", "metric": metric},
        {"_id": 0}
    )
    if doc:
        return (doc["median_value"], doc["peer_count"], "market")
    
    return (None, 0, None)


async def get_5y_median_for_ticker(db, ticker: str, metric: str) -> Optional[float]:
    """
    Compute 5Y median for a specific ticker and metric from historical data.
    
    Uses weekly snapshots (every 7th record) for efficiency.
    Returns median of historical values (robust to outliers).
    """
    five_years_ago = (datetime.now(timezone.utc) - timedelta(days=5*365)).strftime("%Y-%m-%d")
    
    # Get historical values for this ticker and metric
    cursor = db.ticker_key_metrics_daily.find(
        {"ticker": ticker, "date": {"$gte": five_years_ago}, metric: {"$ne": None, "$gt": 0}},
        {"_id": 0, metric: 1, "date": 1}
    ).sort("date", -1)
    
    values = []
    async for doc in cursor:
        val = doc.get(metric)
        if val and val > 0:
            values.append(val)
    
    if len(values) >= 10:  # Need at least 10 data points
        return round(statistics.median(values), 2)
    
    return None



# =============================================================================
# PEER BENCHMARKS V2 - READS FROM EMBEDDED FUNDAMENTALS
# =============================================================================
# BINDING: This version reads from tracked_tickers.fundamentals (embedded)
# NOT from ticker_key_metrics_daily (which is empty)
# =============================================================================

async def compute_peer_benchmarks_v2(db) -> Dict[str, Any]:
    """
    Job: Compute and store peer benchmarks from embedded fundamentals.
    
    BINDING: Reads from tracked_tickers.fundamentals directly.
    
    For each unique Industry (and fallback Sector):
    - Query all visible tickers in that Industry/Sector
    - Compute their current 5 metrics from embedded fundamentals
    - Apply winsorization (1st/99th percentile)
    - Calculate cap-weighted median for each metric
    - Store result in peer_benchmarks collection
    
    Schema for peer_benchmarks:
    {
        _id: ObjectId,
        industry: "Consumer Electronics",
        sector: "Technology",
        peer_count: 47,
        benchmarks: {
            pe_median: 25.3,
            ps_median: 8.1,
            pb_median: 12.4,
            ev_ebitda_median: 18.2,
            ev_revenue_median: 7.9
        },
        computed_at: "2026-02-26T04:30:00Z"
    }
    """
    logger.info("Starting peer benchmarks computation from embedded fundamentals...")
    
    start_time = datetime.now(timezone.utc)
    
    # Helper functions
    def safe_float(val):
        if val is None:
            return None
        try:
            return float(val)
        except:
            return None
    
    def get_ttm_sum(quarterly_data: dict, field: str):
        if not quarterly_data:
            return None
        sorted_quarters = sorted(quarterly_data.keys(), reverse=True)[:4]
        if len(sorted_quarters) < 4:
            return None
        total = 0
        for q in sorted_quarters:
            val = safe_float(quarterly_data[q].get(field))
            if val is not None:
                total += val
        return total if total != 0 else None
    
    def get_latest_value(quarterly_data: dict, field: str):
        if not quarterly_data:
            return None
        sorted_quarters = sorted(quarterly_data.keys(), reverse=True)
        for q in sorted_quarters:
            val = safe_float(quarterly_data[q].get(field))
            if val is not None:
                return val
        return None
    
    def winsorize_values(values: list, low_pct: float = 1, high_pct: float = 99) -> list:
        """Clip outliers at given percentiles."""
        if len(values) < 3:
            return values
        sorted_vals = sorted(values)
        low_idx = max(0, int(len(sorted_vals) * low_pct / 100))
        high_idx = min(len(sorted_vals) - 1, int(len(sorted_vals) * high_pct / 100))
        low_val = sorted_vals[low_idx]
        high_val = sorted_vals[high_idx]
        return [max(low_val, min(high_val, v)) for v in values]
    
    def weighted_median(values: list, weights: list) -> float:
        """Calculate weighted median."""
        if not values or not weights:
            return None
        if len(values) != len(weights):
            return statistics.median(values) if values else None
        
        # Pair and sort by value
        pairs = sorted(zip(values, weights), key=lambda x: x[0])
        
        total_weight = sum(weights)
        if total_weight == 0:
            return statistics.median(values)
        
        cumsum = 0
        for val, weight in pairs:
            cumsum += weight
            if cumsum >= total_weight / 2:
                return val
        
        return pairs[-1][0] if pairs else None
    
    # Get all visible tickers - ONLY metadata, not full fundamentals (too large)
    ticker_list = await db.tracked_tickers.find(
        {"is_visible": True, "fundamentals": {"$exists": True, "$ne": None}},
        {"_id": 0, "ticker": 1, "sector": 1, "industry": 1}
    ).to_list(length=10000)
    
    logger.info(f"Found {len(ticker_list)} visible tickers with fundamentals")
    
    # Get latest prices for all tickers
    latest_prices = {}
    cursor = db.stock_prices.aggregate([
        {"$match": {"ticker": {"$in": [t["ticker"] for t in ticker_list]}}},
        {"$sort": {"date": -1}},
        {"$group": {"_id": "$ticker", "close": {"$first": "$close"}, "adjusted_close": {"$first": "$adjusted_close"}}}
    ])
    async for doc in cursor:
        price = doc.get("adjusted_close") or doc.get("close")
        if price:
            latest_prices[doc["_id"]] = price
    
    logger.info(f"Got prices for {len(latest_prices)} tickers")
    
    # Compute metrics for each ticker - process in batches to avoid memory issues
    ticker_metrics = []
    batch_size = 100
    
    for batch_start in range(0, len(ticker_list), batch_size):
        batch_tickers = ticker_list[batch_start:batch_start + batch_size]
        batch_ticker_names = [t["ticker"] for t in batch_tickers]
        
        # Fetch fundamentals for this batch only
        fundamentals_batch = {}
        cursor = db.tracked_tickers.find(
            {"ticker": {"$in": batch_ticker_names}},
            {"_id": 0, "ticker": 1, "fundamentals": 1}
        )
        async for doc in cursor:
            fundamentals_batch[doc["ticker"]] = doc.get("fundamentals", {})
        
        for t in batch_tickers:
            ticker = t.get("ticker")
            sector = t.get("sector")
            industry = t.get("industry")
            fundamentals = fundamentals_batch.get(ticker, {})
            current_price = latest_prices.get(ticker)
            
            if not current_price or not fundamentals:
                continue
            
            # Extract data from embedded fundamentals
            shares_stats = fundamentals.get("SharesStats", {})
            outstanding_shares_data = fundamentals.get("outstandingShares", {})
            financials_data = fundamentals.get("Financials", {})
            earnings = fundamentals.get("Earnings", {})
            
            # Get shares outstanding
            shares = safe_float(shares_stats.get("SharesOutstanding"))
            if not shares and outstanding_shares_data:
                annual = outstanding_shares_data.get("annual", {})
                if annual:
                    latest_key = sorted(annual.keys(), reverse=True)[0] if annual else None
                    if latest_key:
                        shares = safe_float(annual[latest_key].get("shares"))
            
            if not shares:
                continue
            
            # Market cap
            market_cap = current_price * shares
            if market_cap <= 0:
                continue
            
            # Financial statements
            income_stmt = financials_data.get("Income_Statement", {})
            quarterly_income = income_stmt.get("quarterly", {})
            balance_sheet = financials_data.get("Balance_Sheet", {})
            quarterly_balance = balance_sheet.get("quarterly", {})
            cash_flow_stmt = financials_data.get("Cash_Flow", {})
            quarterly_cashflow = cash_flow_stmt.get("quarterly", {})
        
        # TTM values
        revenue_ttm = get_ttm_sum(quarterly_income, "totalRevenue")
        net_income_ttm = get_ttm_sum(quarterly_income, "netIncome")
        ebitda_ttm = get_ttm_sum(quarterly_income, "ebitda")
        
        if not ebitda_ttm:
            operating_income = get_ttm_sum(quarterly_income, "operatingIncome")
            depreciation = get_ttm_sum(quarterly_cashflow, "depreciation")
            if operating_income:
                ebitda_ttm = operating_income + abs(depreciation or 0)
        
        # Balance sheet items
        total_equity = get_latest_value(quarterly_balance, "totalStockholderEquity")
        total_debt = get_latest_value(quarterly_balance, "totalDebt")
        if not total_debt:
            short_term = get_latest_value(quarterly_balance, "shortTermDebt") or 0
            long_term = get_latest_value(quarterly_balance, "longTermDebt") or 0
            total_debt = short_term + long_term if (short_term or long_term) else 0
        cash = get_latest_value(quarterly_balance, "cash") or get_latest_value(quarterly_balance, "cashAndShortTermInvestments") or 0
        
        # EPS TTM
        eps_ttm = None
        earnings_history = earnings.get("History", {})
        if earnings_history:
            sorted_quarters = sorted(earnings_history.keys(), reverse=True)[:4]
            eps_values = [safe_float(earnings_history[q].get("epsActual")) for q in sorted_quarters if safe_float(earnings_history[q].get("epsActual")) is not None]
            if len(eps_values) >= 4:
                eps_ttm = sum(eps_values)
        if not eps_ttm and net_income_ttm and shares:
            eps_ttm = net_income_ttm / shares
        
        # Enterprise value
        enterprise_value = market_cap + (total_debt or 0) - cash
        
        # Compute 5 metrics
        metrics = {
            "ticker": ticker,
            "sector": sector,
            "industry": industry,
            "market_cap": market_cap,
        }
        
        # P/E
        if eps_ttm and eps_ttm > 0:
            metrics["pe"] = current_price / eps_ttm
        
        # P/S
        if revenue_ttm and revenue_ttm > 0:
            metrics["ps"] = market_cap / revenue_ttm
        
        # P/B
        if total_equity and total_equity > 0:
            metrics["pb"] = market_cap / total_equity
        
        # EV/EBITDA
        if ebitda_ttm and ebitda_ttm > 0 and enterprise_value:
            metrics["ev_ebitda"] = enterprise_value / ebitda_ttm
        
        # EV/Revenue
        if revenue_ttm and revenue_ttm > 0 and enterprise_value:
            metrics["ev_revenue"] = enterprise_value / revenue_ttm
        
        ticker_metrics.append(metrics)
    
    logger.info(f"Computed metrics for {len(ticker_metrics)} tickers")
    
    # Group by industry
    industries = {}
    for m in ticker_metrics:
        industry = m.get("industry")
        sector = m.get("sector")
        
        if industry:
            if industry not in industries:
                industries[industry] = {
                    "sector": sector,
                    "tickers": []
                }
            industries[industry]["tickers"].append(m)
    
    # Compute and store benchmarks
    stored = 0
    metric_names = ["pe", "ps", "pb", "ev_ebitda", "ev_revenue"]
    
    for industry, data in industries.items():
        sector = data["sector"]
        tickers_data = data["tickers"]
        
        if len(tickers_data) < MIN_PEER_COUNT:
            continue
        
        benchmarks = {}
        
        for metric in metric_names:
            values = [t.get(metric) for t in tickers_data if t.get(metric) is not None and t.get(metric) > 0]
            weights = [t.get("market_cap", 1) for t in tickers_data if t.get(metric) is not None and t.get(metric) > 0]
            
            if len(values) >= MIN_PEER_COUNT:
                # Winsorize outliers
                winsorized = winsorize_values(values)
                
                # Cap-weighted median
                median_val = weighted_median(winsorized, weights)
                
                if median_val is not None:
                    benchmarks[f"{metric}_median"] = round(median_val, 2)
        
        if benchmarks:
            await db.peer_benchmarks.update_one(
                {"industry": industry},
                {"$set": {
                    "industry": industry,
                    "sector": sector,
                    "peer_count": len(tickers_data),
                    "benchmarks": benchmarks,
                    "computed_at": datetime.now(timezone.utc).isoformat()
                }},
                upsert=True
            )
            stored += 1
    
    # Also compute sector-level benchmarks (as fallback)
    sectors = {}
    for m in ticker_metrics:
        sector = m.get("sector")
        if sector:
            if sector not in sectors:
                sectors[sector] = []
            sectors[sector].append(m)
    
    for sector, tickers_data in sectors.items():
        if len(tickers_data) < MIN_PEER_COUNT:
            continue
        
        benchmarks = {}
        
        for metric in metric_names:
            values = [t.get(metric) for t in tickers_data if t.get(metric) is not None and t.get(metric) > 0]
            weights = [t.get("market_cap", 1) for t in tickers_data if t.get(metric) is not None and t.get(metric) > 0]
            
            if len(values) >= MIN_PEER_COUNT:
                winsorized = winsorize_values(values)
                median_val = weighted_median(winsorized, weights)
                
                if median_val is not None:
                    benchmarks[f"{metric}_median"] = round(median_val, 2)
        
        if benchmarks:
            await db.peer_benchmarks.update_one(
                {"sector": sector, "industry": None},
                {"$set": {
                    "industry": None,
                    "sector": sector,
                    "peer_count": len(tickers_data),
                    "benchmarks": benchmarks,
                    "computed_at": datetime.now(timezone.utc).isoformat()
                }},
                upsert=True
            )
            stored += 1
    
    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    
    logger.info(f"Peer benchmarks completed: {stored} industry/sector benchmarks stored in {elapsed:.1f}s")
    
    return {
        "status": "success",
        "tickers_processed": len(ticker_metrics),
        "industries_stored": len([i for i in industries.values() if len(i["tickers"]) >= MIN_PEER_COUNT]),
        "sectors_stored": len([s for s in sectors.values() if len(s) >= MIN_PEER_COUNT]),
        "total_benchmarks": stored,
        "elapsed_seconds": round(elapsed, 1)
    }


# =============================================================================
# PEER BENCHMARKS V3 - USD-ONLY FILTER (BINDING FIX)
# =============================================================================
# BINDING: Filters out tickers with currency mismatch (non-USD financials)
# Detection: P/S < 0.05 indicates financials in foreign currency (JPY, KRW, etc.)
# =============================================================================

# Threshold for currency mismatch detection
CURRENCY_MISMATCH_PS_THRESHOLD = 0.05  # P/S below this indicates non-USD financials
MIN_PEERS_FOR_INDUSTRY = 12  # Minimum peers before fallback to sector

async def compute_peer_benchmarks_v3(db) -> Dict[str, Any]:
    """
    Job: Compute peer benchmarks with USD-only filter.
    
    BINDING FIX (P1 Policy): Excludes tickers where financial_currency != "USD".
    
    Uses the new `financial_currency` field populated by currency extraction
    from fundamentals.Financials.*.currency_symbol (with fallback to nested
    quarterly/yearly entries).
    
    Non-USD tickers still have their own metrics calculated, but they are
    NOT included in the pool of peers used to calculate sector/industry medians.
    
    Schema for peer_benchmarks:
    {
        _id: ObjectId,
        industry: "Consumer Electronics",
        sector: "Technology",
        currency_filter: "USD_only",
        peer_count_total: 16,      # Before filter
        peer_count_usd: 13,        # After filter (USD-only)
        excluded_tickers: [{ticker, currency, reason}],
        benchmarks: {
            pe_median: 25.3,
            ps_median: 8.1,
            ...
        },
        computed_at: "2026-02-26T04:30:00Z"
    }
    """
    logger.info("Starting peer benchmarks V3 computation (P1 Policy: USD-only filter)...")
    
    start_time = datetime.now(timezone.utc)
    
    # Helper functions
    def safe_float(val):
        if val is None:
            return None
        try:
            return float(val)
        except:
            return None
    
    def get_ttm_sum(quarterly_data: dict, field: str):
        if not quarterly_data:
            return None
        sorted_quarters = sorted(quarterly_data.keys(), reverse=True)[:4]
        if len(sorted_quarters) < 4:
            return None
        total = 0
        for q in sorted_quarters:
            val = safe_float(quarterly_data[q].get(field))
            if val is not None:
                total += val
        return total if total != 0 else None
    
    def get_latest_value(quarterly_data: dict, field: str):
        if not quarterly_data:
            return None
        for q in sorted(quarterly_data.keys(), reverse=True):
            val = safe_float(quarterly_data[q].get(field))
            if val is not None:
                return val
        return None
    
    def winsorize_values(values: list, low_pct: float = 1, high_pct: float = 99) -> list:
        if len(values) < 3:
            return values
        sorted_vals = sorted(values)
        low_idx = max(0, int(len(sorted_vals) * low_pct / 100))
        high_idx = min(len(sorted_vals) - 1, int(len(sorted_vals) * high_pct / 100))
        return [max(sorted_vals[low_idx], min(sorted_vals[high_idx], v)) for v in values]
    
    def weighted_median(values: list, weights: list) -> float:
        if not values or not weights or len(values) != len(weights):
            return statistics.median(values) if values else None
        pairs = sorted(zip(values, weights), key=lambda x: x[0])
        total_weight = sum(weights)
        if total_weight == 0:
            return statistics.median(values)
        cumsum = 0
        for val, weight in pairs:
            cumsum += weight
            if cumsum >= total_weight / 2:
                return val
        return pairs[-1][0] if pairs else None
    
    # Get all visible tickers with their financial_currency
    ticker_list = await db.tracked_tickers.find(
        {"is_visible": True, "fundamentals": {"$exists": True, "$ne": None}},
        {"_id": 0, "ticker": 1, "sector": 1, "industry": 1, "financial_currency": 1}
    ).to_list(length=10000)
    
    # Separate USD and non-USD tickers upfront (P1 Policy)
    usd_tickers = [t for t in ticker_list if t.get("financial_currency") == "USD"]
    non_usd_tickers = [t for t in ticker_list if t.get("financial_currency") and t.get("financial_currency") != "USD"]
    null_currency_tickers = [t for t in ticker_list if not t.get("financial_currency")]
    
    logger.info(f"Found {len(ticker_list)} visible tickers with fundamentals")
    logger.info(f"  USD tickers: {len(usd_tickers)} (will be used for peer medians)")
    logger.info(f"  Non-USD tickers: {len(non_usd_tickers)} (excluded from peer medians)")
    logger.info(f"  NULL currency tickers: {len(null_currency_tickers)} (excluded from peer medians)")
    
    # Get latest prices
    latest_prices = {}
    cursor = db.stock_prices.aggregate([
        {"$match": {"ticker": {"$in": [t["ticker"] for t in ticker_list]}}},
        {"$sort": {"date": -1}},
        {"$group": {"_id": "$ticker", "price": {"$first": "$adjusted_close"}}}
    ])
    async for doc in cursor:
        if doc.get("price"):
            latest_prices[doc["_id"]] = doc["price"]
    
    logger.info(f"Got prices for {len(latest_prices)} tickers")
    
    # Compute metrics for each ticker in batches
    ticker_metrics = []
    excluded_currency_mismatch = []
    batch_size = 100
    
    for batch_start in range(0, len(ticker_list), batch_size):
        batch_tickers = ticker_list[batch_start:batch_start + batch_size]
        batch_ticker_names = [t["ticker"] for t in batch_tickers]
        
        # Fetch fundamentals for this batch
        fundamentals_batch = {}
        cursor = db.tracked_tickers.find(
            {"ticker": {"$in": batch_ticker_names}},
            {"_id": 0, "ticker": 1, "fundamentals": 1}
        )
        async for doc in cursor:
            fundamentals_batch[doc["ticker"]] = doc.get("fundamentals", {})
        
        for t in batch_tickers:
            ticker = t.get("ticker")
            sector = t.get("sector")
            industry = t.get("industry")
            financial_currency = t.get("financial_currency")  # P1 Policy: Use persisted currency
            fundamentals = fundamentals_batch.get(ticker, {})
            current_price = latest_prices.get(ticker)
            
            if not current_price or not fundamentals:
                continue
            
            # P1 Policy: Track non-USD tickers for exclusion from peer medians
            is_usd = financial_currency == "USD"
            if not is_usd:
                excluded_currency_mismatch.append({
                    "ticker": ticker,
                    "industry": industry,
                    "currency": financial_currency,
                    "reason": f"financial_currency={financial_currency} (non-USD excluded from peer medians)"
                })
            
            # Extract shares outstanding
            shares_stats = fundamentals.get("SharesStats", {})
            shares = safe_float(shares_stats.get("SharesOutstanding"))
            if not shares:
                outstanding = fundamentals.get("outstandingShares", {}).get("annual", {})
                if outstanding:
                    latest_key = sorted(outstanding.keys(), reverse=True)[0] if outstanding else None
                    if latest_key:
                        shares = safe_float(outstanding[latest_key].get("shares"))
            
            if not shares or shares <= 0:
                continue
            
            market_cap = current_price * shares
            if market_cap <= 0:
                continue
            
            # Financial statements
            financials_data = fundamentals.get("Financials", {})
            quarterly_income = financials_data.get("Income_Statement", {}).get("quarterly", {})
            quarterly_balance = financials_data.get("Balance_Sheet", {}).get("quarterly", {})
            quarterly_cashflow = financials_data.get("Cash_Flow", {}).get("quarterly", {})
            earnings = fundamentals.get("Earnings", {})
            
            # TTM values
            revenue_ttm = get_ttm_sum(quarterly_income, "totalRevenue")
            net_income_ttm = get_ttm_sum(quarterly_income, "netIncome")
            ebitda_ttm = get_ttm_sum(quarterly_income, "ebitda")
            
            if not ebitda_ttm:
                operating_income = get_ttm_sum(quarterly_income, "operatingIncome")
                depreciation = get_ttm_sum(quarterly_cashflow, "depreciation")
                if operating_income:
                    ebitda_ttm = operating_income + abs(depreciation or 0)
            
            # Balance sheet
            total_equity = get_latest_value(quarterly_balance, "totalStockholderEquity")
            total_debt = get_latest_value(quarterly_balance, "totalDebt")
            if not total_debt:
                short_term = get_latest_value(quarterly_balance, "shortTermDebt") or 0
                long_term = get_latest_value(quarterly_balance, "longTermDebt") or 0
                total_debt = short_term + long_term if (short_term or long_term) else 0
            cash = get_latest_value(quarterly_balance, "cash") or get_latest_value(quarterly_balance, "cashAndShortTermInvestments") or 0
            
            # EPS TTM
            eps_ttm = None
            earnings_history = earnings.get("History", {})
            if earnings_history:
                sorted_q = sorted(earnings_history.keys(), reverse=True)[:4]
                eps_values = [safe_float(earnings_history[q].get("epsActual")) for q in sorted_q 
                             if safe_float(earnings_history[q].get("epsActual")) is not None]
                if len(eps_values) >= 3:
                    eps_ttm = sum(eps_values)
            if not eps_ttm and net_income_ttm and shares:
                eps_ttm = net_income_ttm / shares
            
            enterprise_value = market_cap + (total_debt or 0) - cash
            
            # Compute metrics
            metrics = {
                "ticker": ticker,
                "sector": sector,
                "industry": industry,
                "market_cap": market_cap,
            }
            
            # P/E
            if eps_ttm and eps_ttm > 0:
                metrics["pe"] = current_price / eps_ttm
            
            # P/S (critical for currency mismatch detection)
            ps_value = None
            if revenue_ttm and revenue_ttm > 0:
                ps_value = market_cap / revenue_ttm
                metrics["ps"] = ps_value
            
            # P/B
            if total_equity and total_equity > 0:
                metrics["pb"] = market_cap / total_equity
            
            # EV/EBITDA
            if ebitda_ttm and ebitda_ttm > 0 and enterprise_value > 0:
                metrics["ev_ebitda"] = enterprise_value / ebitda_ttm
            
            # EV/Revenue
            if revenue_ttm and revenue_ttm > 0 and enterprise_value > 0:
                metrics["ev_revenue"] = enterprise_value / revenue_ttm
            
            # Dividend Yield (ForwardAnnualDividendYield)
            # P1 FIX: Add to peer_benchmarks for consistent infrastructure
            splits_dividends = fundamentals.get("SplitsDividends", {})
            forward_div_yield = safe_float(splits_dividends.get("ForwardAnnualDividendYield"))
            if forward_div_yield is not None and forward_div_yield >= 0:
                # EODHD returns decimal (0.025 = 2.5%), convert to percentage
                metrics["dividend_yield"] = forward_div_yield * 100
            
            # ── STEP 4 Key Metrics ──────────────────────────────────────
            # Net Margin (TTM) = net_income_ttm / revenue_ttm * 100
            if net_income_ttm is not None and revenue_ttm and revenue_ttm > 0:
                metrics["net_margin_ttm"] = (net_income_ttm / revenue_ttm) * 100
            
            # Free Cash Flow Yield = (operating_cf_ttm - capex_ttm) / market_cap * 100
            operating_cf_ttm = get_ttm_sum(quarterly_cashflow, "totalCashFromOperatingActivities")
            if not operating_cf_ttm:
                operating_cf_ttm = get_ttm_sum(quarterly_cashflow, "operatingCashflow")
            capex_ttm = get_ttm_sum(quarterly_cashflow, "capitalExpenditures")
            if operating_cf_ttm is not None and market_cap > 0:
                # EODHD stores capex as negative; abs() ensures subtraction is correct
                fcf_ttm = operating_cf_ttm - abs(capex_ttm or 0)
                metrics["fcf_yield"] = (fcf_ttm / market_cap) * 100
            
            # Net Debt / EBITDA  (net_debt can be negative if cash > debt)
            net_debt = (total_debt or 0) - cash
            if ebitda_ttm and ebitda_ttm > 0:
                metrics["net_debt_ebitda"] = net_debt / ebitda_ttm
            
            # Revenue Growth (3Y CAGR) from annual Income_Statement
            annual_income = financials_data.get("Income_Statement", {}).get("yearly", {})
            if annual_income:
                sorted_years = sorted(annual_income.keys(), reverse=True)
                if len(sorted_years) >= 4:
                    rev_current = safe_float(annual_income[sorted_years[0]].get("totalRevenue"))
                    rev_3y_ago = safe_float(annual_income[sorted_years[3]].get("totalRevenue"))
                    if rev_current and rev_3y_ago and rev_3y_ago > 0 and rev_current > 0:
                        metrics["revenue_growth_3y"] = ((rev_current / rev_3y_ago) ** (1.0 / 3.0) - 1) * 100
            
            # ROE = net_income_ttm / total_equity * 100
            if net_income_ttm is not None and total_equity and total_equity > 0:
                metrics["roe"] = (net_income_ttm / total_equity) * 100
            
            # CURRENCY MISMATCH DETECTION - P1 Policy using persisted financial_currency
            # Non-USD tickers are excluded from peer median calculations
            metrics["currency_mismatch"] = not is_usd
            metrics["financial_currency"] = financial_currency
            
            ticker_metrics.append(metrics)
    
    logger.info(f"Computed metrics for {len(ticker_metrics)} tickers")
    logger.info(f"P1 Policy: {len(excluded_currency_mismatch)} non-USD tickers excluded from peer medians")
    
    # Group by industry (separating USD-only)
    industries = {}
    for m in ticker_metrics:
        industry = m.get("industry")
        sector = m.get("sector")
        
        if industry:
            if industry not in industries:
                industries[industry] = {
                    "sector": sector,
                    "all_tickers": [],
                    "usd_only_tickers": []
                }
            industries[industry]["all_tickers"].append(m)
            if not m.get("currency_mismatch"):
                industries[industry]["usd_only_tickers"].append(m)
    
    # Compute and store benchmarks (USD-only) with SORTED metric_values lists
    # FIX-2: Dual dividend medians (median_all + median_payers)
    stored = 0
    non_dividend_metrics = ["pe", "ps", "pb", "ev_ebitda", "ev_revenue"]
    # STEP 4 Key Metrics for admin overview (allow negative values for some)
    step4_metric_keys = ["pe", "net_margin_ttm", "fcf_yield", "net_debt_ebitda", "revenue_growth_3y", "dividend_yield", "roe"]
    # Metrics that can be negative (don't filter > 0)
    step4_allow_negative = {"net_margin_ttm", "fcf_yield", "net_debt_ebitda", "revenue_growth_3y", "roe"}
    
    for industry, data in industries.items():
        sector = data["sector"]
        all_tickers = data["all_tickers"]
        usd_tickers = data["usd_only_tickers"]
        
        # Get excluded tickers for this industry
        excluded = [t["ticker"] for t in all_tickers if t.get("currency_mismatch")]
        
        # Use USD-only tickers for benchmark calculation
        tickers_data = usd_tickers
        
        if len(tickers_data) < MIN_PEER_COUNT:
            continue
        
        # Build metric_values with sorted parallel arrays
        metric_values = {}
        metrics_count = {}
        benchmarks = {}
        
        # Process non-dividend metrics first
        for metric in non_dividend_metrics:
            pairs = [
                (t["ticker"], t[metric]) 
                for t in tickers_data 
                if t.get(metric) is not None and t.get(metric) > 0
            ]
            
            metrics_count[metric] = len(pairs)
            
            if len(pairs) >= MIN_PEER_COUNT:
                pairs.sort(key=lambda x: x[1])
                metric_values[metric] = {
                    "tickers": [p[0] for p in pairs],
                    "values": [round(p[1], 4) for p in pairs]
                }
                values = metric_values[metric]["values"]
                n = len(values)
                if n % 2 == 1:
                    benchmarks[f"{metric}_median"] = round(values[n // 2], 2)
                else:
                    benchmarks[f"{metric}_median"] = round((values[n // 2 - 1] + values[n // 2]) / 2, 2)
                
                assert len(metric_values[metric]["tickers"]) == len(metric_values[metric]["values"]), \
                    f"Array length mismatch for {industry}/{metric}"
        
        # FIX-2: DUAL DIVIDEND MEDIANS
        # median_all: all peers (including 0% non-payers)
        # median_payers: only payers (yield > 0), requires MIN_DIVIDEND_PAYERS
        pairs_all = [
            (t["ticker"], t["dividend_yield"]) 
            for t in tickers_data 
            if t.get("dividend_yield") is not None and t.get("dividend_yield") >= 0
        ]
        pairs_payers = [
            (t["ticker"], t["dividend_yield"]) 
            for t in tickers_data 
            if t.get("dividend_yield") is not None and t.get("dividend_yield") > 0
        ]
        
        dividend_peer_count = len(pairs_all)
        dividend_payers_count = len(pairs_payers)
        metrics_count["dividend_yield"] = dividend_peer_count
        
        # Compute median_all
        dividend_median_all = None
        dividend_level_all = None
        if len(pairs_all) >= MIN_PEER_COUNT:
            pairs_all.sort(key=lambda x: x[1])
            values_all = [round(p[1], 4) for p in pairs_all]
            metric_values["dividend_yield_all"] = {
                "tickers": [p[0] for p in pairs_all],
                "values": values_all
            }
            n = len(values_all)
            if n % 2 == 1:
                dividend_median_all = round(values_all[n // 2], 2)
            else:
                dividend_median_all = round((values_all[n // 2 - 1] + values_all[n // 2]) / 2, 2)
            dividend_level_all = "industry"
        
        # Compute median_payers (requires MIN_DIVIDEND_PAYERS)
        dividend_median_payers = None
        dividend_level_payers = None
        if len(pairs_payers) >= MIN_DIVIDEND_PAYERS:
            pairs_payers.sort(key=lambda x: x[1])
            values_payers = [round(p[1], 4) for p in pairs_payers]
            metric_values["dividend_yield_payers"] = {
                "tickers": [p[0] for p in pairs_payers],
                "values": values_payers
            }
            n = len(values_payers)
            if n % 2 == 1:
                dividend_median_payers = round(values_payers[n // 2], 2)
            else:
                dividend_median_payers = round((values_payers[n // 2 - 1] + values_payers[n // 2]) / 2, 2)
            dividend_level_payers = "industry"
        
        # Store dividend data in benchmarks (backward compat + new fields)
        benchmarks["dividend_yield_median"] = dividend_median_all  # Backward compat
        benchmarks["dividend_yield_median_all"] = dividend_median_all
        benchmarks["dividend_yield_median_payers"] = dividend_median_payers
        
        # ── STEP 4: Compute medians for the 7 admin Key Metrics ─────
        step4 = {}
        for mk in step4_metric_keys:
            if mk == "dividend_yield":
                # Use the already-computed dividend_median_all
                if dividend_median_all is not None:
                    step4["dividend_yield_ttm"] = {"median": dividend_median_all, "n_used": dividend_peer_count}
                continue
            if mk in step4_allow_negative:
                pairs_s4 = [t[mk] for t in tickers_data if t.get(mk) is not None]
            else:
                pairs_s4 = [t[mk] for t in tickers_data if t.get(mk) is not None and t.get(mk) > 0]
            if len(pairs_s4) >= MIN_PEER_COUNT:
                pairs_s4 = winsorize_values(pairs_s4)
                pairs_s4.sort()
                n_s4 = len(pairs_s4)
                if n_s4 % 2 == 1:
                    med = round(pairs_s4[n_s4 // 2], 2)
                else:
                    med = round((pairs_s4[n_s4 // 2 - 1] + pairs_s4[n_s4 // 2]) / 2, 2)
                # Map key to spec name (pe -> pe_ttm)
                out_key = "pe_ttm" if mk == "pe" else mk
                step4[out_key] = {"median": med, "n_used": n_s4}
        
        if metric_values:
            await db.peer_benchmarks.update_one(
                {"industry": industry},
                {"$set": {
                    "industry": industry,
                    "sector": sector,
                    "currency_filter": "USD_only",
                    "peer_count_total": len(all_tickers),
                    "peer_count_used": len(usd_tickers),
                    "excluded_tickers": excluded,
                    "metrics_count": metrics_count,
                    "metric_values": metric_values,
                    "benchmarks": benchmarks,
                    "step4_medians": step4,
                    # FIX-2: Dividend-specific fields
                    "dividend_peer_count": dividend_peer_count,
                    "dividend_payers_count": dividend_payers_count,
                    "dividend_median_level_all": dividend_level_all,
                    "dividend_median_level_payers": dividend_level_payers,
                    "computed_at": datetime.now(timezone.utc).isoformat()
                }},
                upsert=True
            )
            stored += 1
    
    # =========================================================================
    # SECTOR-LEVEL BENCHMARKS (USD-only, for fallback when industry < 3 peers)
    # FIX-2: Also compute dual dividend medians for sector fallback
    # =========================================================================
    MAX_PEERS_PER_METRIC = 1000  # Hard guardrail for document size
    
    sectors = {}
    for m in ticker_metrics:
        sector = m.get("sector")
        if sector and not m.get("currency_mismatch"):
            if sector not in sectors:
                sectors[sector] = []
            sectors[sector].append(m)
    
    sector_stored = 0
    for sector, usd_tickers in sectors.items():
        if len(usd_tickers) < MIN_PEER_COUNT:
            continue
        
        metric_values = {}
        metrics_count = {}
        benchmarks = {}
        
        # Process non-dividend metrics
        for metric in non_dividend_metrics:
            pairs = [
                (t["ticker"], t[metric]) 
                for t in usd_tickers 
                if t.get(metric) is not None and t.get(metric) > 0
            ]
            
            metrics_count[metric] = len(pairs)
            
            if len(pairs) >= MIN_PEER_COUNT:
                pairs.sort(key=lambda x: x[1])
                
                # Hard guardrail: truncate if too large
                if len(pairs) > MAX_PEERS_PER_METRIC:
                    logger.warning(f"Sector {sector} has {len(pairs)} peers for {metric}, truncating")
                    start = (len(pairs) - MAX_PEERS_PER_METRIC) // 2
                    pairs = pairs[start:start + MAX_PEERS_PER_METRIC]
                
                metric_values[metric] = {
                    "tickers": [p[0] for p in pairs],
                    "values": [round(p[1], 4) for p in pairs]
                }
                values = metric_values[metric]["values"]
                n = len(values)
                if n % 2 == 1:
                    benchmarks[f"{metric}_median"] = round(values[n // 2], 2)
                else:
                    benchmarks[f"{metric}_median"] = round((values[n // 2 - 1] + values[n // 2]) / 2, 2)
                
                assert len(metric_values[metric]["tickers"]) == len(metric_values[metric]["values"]), \
                    f"Array length mismatch for sector {sector}/{metric}"
        
        # FIX-2: DUAL DIVIDEND MEDIANS for sector
        pairs_all = [
            (t["ticker"], t["dividend_yield"]) 
            for t in usd_tickers 
            if t.get("dividend_yield") is not None and t.get("dividend_yield") >= 0
        ]
        pairs_payers = [
            (t["ticker"], t["dividend_yield"]) 
            for t in usd_tickers 
            if t.get("dividend_yield") is not None and t.get("dividend_yield") > 0
        ]
        
        dividend_peer_count = len(pairs_all)
        dividend_payers_count = len(pairs_payers)
        metrics_count["dividend_yield"] = dividend_peer_count
        
        # Compute median_all for sector
        dividend_median_all = None
        dividend_level_all = None
        if len(pairs_all) >= MIN_PEER_COUNT:
            pairs_all.sort(key=lambda x: x[1])
            if len(pairs_all) > MAX_PEERS_PER_METRIC:
                start = (len(pairs_all) - MAX_PEERS_PER_METRIC) // 2
                pairs_all = pairs_all[start:start + MAX_PEERS_PER_METRIC]
            values_all = [round(p[1], 4) for p in pairs_all]
            metric_values["dividend_yield_all"] = {
                "tickers": [p[0] for p in pairs_all],
                "values": values_all
            }
            n = len(values_all)
            if n % 2 == 1:
                dividend_median_all = round(values_all[n // 2], 2)
            else:
                dividend_median_all = round((values_all[n // 2 - 1] + values_all[n // 2]) / 2, 2)
            dividend_level_all = "sector"
        
        # Compute median_payers for sector
        dividend_median_payers = None
        dividend_level_payers = None
        if len(pairs_payers) >= MIN_DIVIDEND_PAYERS:
            pairs_payers.sort(key=lambda x: x[1])
            if len(pairs_payers) > MAX_PEERS_PER_METRIC:
                start = (len(pairs_payers) - MAX_PEERS_PER_METRIC) // 2
                pairs_payers = pairs_payers[start:start + MAX_PEERS_PER_METRIC]
            values_payers = [round(p[1], 4) for p in pairs_payers]
            metric_values["dividend_yield_payers"] = {
                "tickers": [p[0] for p in pairs_payers],
                "values": values_payers
            }
            n = len(values_payers)
            if n % 2 == 1:
                dividend_median_payers = round(values_payers[n // 2], 2)
            else:
                dividend_median_payers = round((values_payers[n // 2 - 1] + values_payers[n // 2]) / 2, 2)
            dividend_level_payers = "sector"
        
        benchmarks["dividend_yield_median"] = dividend_median_all
        benchmarks["dividend_yield_median_all"] = dividend_median_all
        benchmarks["dividend_yield_median_payers"] = dividend_median_payers
        
        # ── STEP 4: Compute medians for the 7 admin Key Metrics (sector) ─
        step4 = {}
        for mk in step4_metric_keys:
            if mk == "dividend_yield":
                if dividend_median_all is not None:
                    step4["dividend_yield_ttm"] = {"median": dividend_median_all, "n_used": dividend_peer_count}
                continue
            if mk in step4_allow_negative:
                pairs_s4 = [t[mk] for t in usd_tickers if t.get(mk) is not None]
            else:
                pairs_s4 = [t[mk] for t in usd_tickers if t.get(mk) is not None and t.get(mk) > 0]
            if len(pairs_s4) >= MIN_PEER_COUNT:
                pairs_s4 = winsorize_values(pairs_s4)
                pairs_s4.sort()
                n_s4 = len(pairs_s4)
                if n_s4 % 2 == 1:
                    med = round(pairs_s4[n_s4 // 2], 2)
                else:
                    med = round((pairs_s4[n_s4 // 2 - 1] + pairs_s4[n_s4 // 2]) / 2, 2)
                out_key = "pe_ttm" if mk == "pe" else mk
                step4[out_key] = {"median": med, "n_used": n_s4}
        
        if metric_values:
            await db.peer_benchmarks.update_one(
                {"sector": sector, "industry": None},
                {"$set": {
                    "industry": None,
                    "sector": sector,
                    "currency_filter": "USD_only",
                    "peer_count": len(usd_tickers),
                    "metrics_count": metrics_count,
                    "metric_values": metric_values,
                    "benchmarks": benchmarks,
                    "step4_medians": step4,
                    # FIX-2: Dividend-specific fields
                    "dividend_peer_count": dividend_peer_count,
                    "dividend_payers_count": dividend_payers_count,
                    "dividend_median_level_all": dividend_level_all,
                    "dividend_median_level_payers": dividend_level_payers,
                    "computed_at": datetime.now(timezone.utc).isoformat()
                }},
                upsert=True
            )
            sector_stored += 1
    
    logger.info(f"Stored {sector_stored} sector-level benchmarks")
    
    # =========================================================================
    # MARKET-LEVEL BENCHMARKS (USD-only, final fallback)
    # FIX-2: Also compute dual dividend medians for market fallback
    # =========================================================================
    all_usd_tickers = [m for m in ticker_metrics if not m.get("currency_mismatch")]
    
    if len(all_usd_tickers) >= MIN_PEER_COUNT:
        metric_values = {}
        metrics_count = {}
        benchmarks = {}
        
        # Process non-dividend metrics for market level
        for metric in non_dividend_metrics:
            pairs = [
                (t["ticker"], t[metric]) 
                for t in all_usd_tickers 
                if t.get(metric) is not None and t.get(metric) > 0
            ]
            
            metrics_count[metric] = len(pairs)
            
            if len(pairs) >= MIN_PEER_COUNT:
                pairs.sort(key=lambda x: x[1])
                
                if len(pairs) > MAX_PEERS_PER_METRIC:
                    start = (len(pairs) - MAX_PEERS_PER_METRIC) // 2
                    pairs = pairs[start:start + MAX_PEERS_PER_METRIC]
                
                metric_values[metric] = {
                    "tickers": [p[0] for p in pairs],
                    "values": [round(p[1], 4) for p in pairs]
                }
                values = metric_values[metric]["values"]
                n = len(values)
                if n % 2 == 1:
                    benchmarks[f"{metric}_median"] = round(values[n // 2], 2)
                else:
                    benchmarks[f"{metric}_median"] = round((values[n // 2 - 1] + values[n // 2]) / 2, 2)
        
        # FIX-2: DUAL DIVIDEND MEDIANS for market
        pairs_all = [
            (t["ticker"], t["dividend_yield"]) 
            for t in all_usd_tickers 
            if t.get("dividend_yield") is not None and t.get("dividend_yield") >= 0
        ]
        pairs_payers = [
            (t["ticker"], t["dividend_yield"]) 
            for t in all_usd_tickers 
            if t.get("dividend_yield") is not None and t.get("dividend_yield") > 0
        ]
        
        dividend_peer_count = len(pairs_all)
        dividend_payers_count = len(pairs_payers)
        metrics_count["dividend_yield"] = dividend_peer_count
        
        # Compute median_all for market
        dividend_median_all = None
        dividend_level_all = None
        if len(pairs_all) >= MIN_PEER_COUNT:
            pairs_all.sort(key=lambda x: x[1])
            if len(pairs_all) > MAX_PEERS_PER_METRIC:
                start = (len(pairs_all) - MAX_PEERS_PER_METRIC) // 2
                pairs_all = pairs_all[start:start + MAX_PEERS_PER_METRIC]
            values_all = [round(p[1], 4) for p in pairs_all]
            metric_values["dividend_yield_all"] = {
                "tickers": [p[0] for p in pairs_all],
                "values": values_all
            }
            n = len(values_all)
            if n % 2 == 1:
                dividend_median_all = round(values_all[n // 2], 2)
            else:
                dividend_median_all = round((values_all[n // 2 - 1] + values_all[n // 2]) / 2, 2)
            dividend_level_all = "market"
        
        # Compute median_payers for market
        dividend_median_payers = None
        dividend_level_payers = None
        if len(pairs_payers) >= MIN_DIVIDEND_PAYERS:
            pairs_payers.sort(key=lambda x: x[1])
            if len(pairs_payers) > MAX_PEERS_PER_METRIC:
                start = (len(pairs_payers) - MAX_PEERS_PER_METRIC) // 2
                pairs_payers = pairs_payers[start:start + MAX_PEERS_PER_METRIC]
            values_payers = [round(p[1], 4) for p in pairs_payers]
            metric_values["dividend_yield_payers"] = {
                "tickers": [p[0] for p in pairs_payers],
                "values": values_payers
            }
            n = len(values_payers)
            if n % 2 == 1:
                dividend_median_payers = round(values_payers[n // 2], 2)
            else:
                dividend_median_payers = round((values_payers[n // 2 - 1] + values_payers[n // 2]) / 2, 2)
            dividend_level_payers = "market"
        
        benchmarks["dividend_yield_median"] = dividend_median_all
        benchmarks["dividend_yield_median_all"] = dividend_median_all
        benchmarks["dividend_yield_median_payers"] = dividend_median_payers
        
        # ── STEP 4: Compute medians for the 7 admin Key Metrics (market) ─
        step4 = {}
        for mk in step4_metric_keys:
            if mk == "dividend_yield":
                if dividend_median_all is not None:
                    step4["dividend_yield_ttm"] = {"median": dividend_median_all, "n_used": dividend_peer_count}
                continue
            if mk in step4_allow_negative:
                pairs_s4 = [t[mk] for t in all_usd_tickers if t.get(mk) is not None]
            else:
                pairs_s4 = [t[mk] for t in all_usd_tickers if t.get(mk) is not None and t.get(mk) > 0]
            if len(pairs_s4) >= MIN_PEER_COUNT:
                pairs_s4 = winsorize_values(pairs_s4)
                pairs_s4.sort()
                n_s4 = len(pairs_s4)
                if n_s4 % 2 == 1:
                    med = round(pairs_s4[n_s4 // 2], 2)
                else:
                    med = round((pairs_s4[n_s4 // 2 - 1] + pairs_s4[n_s4 // 2]) / 2, 2)
                out_key = "pe_ttm" if mk == "pe" else mk
                step4[out_key] = {"median": med, "n_used": n_s4}
        
        if metric_values:
            await db.peer_benchmarks.update_one(
                {"sector": None, "industry": None},
                {"$set": {
                    "industry": None,
                    "sector": None,
                    "level": "market",
                    "currency_filter": "USD_only",
                    "peer_count": len(all_usd_tickers),
                    "metrics_count": metrics_count,
                    "metric_values": metric_values,
                    "benchmarks": benchmarks,
                    "step4_medians": step4,
                    # FIX-2: Dividend-specific fields
                    "dividend_peer_count": dividend_peer_count,
                    "dividend_payers_count": dividend_payers_count,
                    "dividend_median_level_all": dividend_level_all,
                    "dividend_median_level_payers": dividend_level_payers,
                    "computed_at": datetime.now(timezone.utc).isoformat()
                }},
                upsert=True
            )
            logger.info(f"Stored market-level benchmarks (peer_count={len(all_usd_tickers)})")
    
    # =========================================================================
    # FIX-2: FALLBACK PASS - Update industries missing payers median
    # Industries with < MIN_DIVIDEND_PAYERS get their payers median from sector/market
    # =========================================================================
    logger.info("Starting fallback pass for dividend_median_payers...")
    
    # Load sector medians for fallback (sector docs have industry=None)
    sector_medians = {}
    cursor = db.peer_benchmarks.find({"sector": {"$ne": None}, "industry": None})
    async for doc in cursor:
        sector = doc["sector"]
        sector_medians[sector] = {
            "median_payers": doc.get("benchmarks", {}).get("dividend_yield_median_payers"),
            "payers_count": doc.get("dividend_payers_count", 0)
        }
    
    # Load market median (sector=None AND industry=None)
    market_doc = await db.peer_benchmarks.find_one({"sector": None, "industry": None})
    market_median_payers = market_doc.get("benchmarks", {}).get("dividend_yield_median_payers") if market_doc else None
    market_payers_count = market_doc.get("dividend_payers_count", 0) if market_doc else 0
    
    # Update industries with missing payers median
    fallback_updates = 0
    cursor = db.peer_benchmarks.find({"industry": {"$ne": None}})
    async for doc in cursor:
        industry = doc["industry"]
        sector = doc.get("sector")
        current_level_payers = doc.get("dividend_median_level_payers")
        
        # Skip if already has valid payers median at industry level
        if current_level_payers == "industry":
            continue
        
        # Try sector fallback first
        fallback_median = None
        fallback_level = None
        
        if sector and sector in sector_medians:
            sector_data = sector_medians[sector]
            if sector_data["median_payers"] is not None and sector_data["payers_count"] >= MIN_DIVIDEND_PAYERS:
                fallback_median = sector_data["median_payers"]
                fallback_level = "sector"
        
        # Try market fallback if sector didn't work
        if fallback_median is None and market_median_payers is not None and market_payers_count >= MIN_DIVIDEND_PAYERS:
            fallback_median = market_median_payers
            fallback_level = "market"
        
        # Update document with fallback
        if fallback_median is not None:
            await db.peer_benchmarks.update_one(
                {"industry": industry},
                {"$set": {
                    "benchmarks.dividend_yield_median_payers": fallback_median,
                    "dividend_median_level_payers": fallback_level
                    # Note: dividend_payers_count stays at industry level (shows local count)
                }}
            )
            fallback_updates += 1
    
    logger.info(f"Fallback pass complete: {fallback_updates} industries updated with sector/market payers median")
    
    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    
    logger.info(f"Peer benchmarks V3 completed: {stored} benchmarks in {elapsed:.1f}s")
    logger.info(f"Excluded {len(excluded_currency_mismatch)} tickers due to currency mismatch")
    
    return {
        "status": "success",
        "tickers_processed": len(ticker_metrics),
        "tickers_excluded_currency": len(excluded_currency_mismatch),
        "excluded_details": excluded_currency_mismatch[:20],  # Top 20 for logging
        "industries_stored": stored,
        "elapsed_seconds": round(elapsed, 1)
    }
