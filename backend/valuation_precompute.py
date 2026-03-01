# =============================================================================
# RICHSTOX VALUATION PRE-COMPUTATION JOB
# =============================================================================
# BINDING: Pre-computes valuations for ALL visible tickers nightly.
# Results stored in ticker_valuations_cache for < 200ms API response.
# =============================================================================

import asyncio
import statistics
from datetime import datetime, timezone
from motor.motor_asyncio import AsyncIOMotorClient
from typing import Dict, Any, Optional, List
import logging
import os

logger = logging.getLogger("richstox.valuation")

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def safe_float(val) -> Optional[float]:
    """Safely convert value to float."""
    if val is None:
        return None
    try:
        f = float(val)
        return f if f != 0 else None
    except (ValueError, TypeError):
        return None


def get_ttm_sum(quarterly_data: dict, field: str) -> Optional[float]:
    """Get TTM sum from last 4 quarters."""
    if not quarterly_data:
        return None
    sorted_quarters = sorted(quarterly_data.keys(), reverse=True)[:4]
    if len(sorted_quarters) < 4:
        return None
    
    total = 0
    valid_count = 0
    for q in sorted_quarters:
        val = safe_float(quarterly_data[q].get(field))
        if val is not None:
            total += val
            valid_count += 1
    
    return total if valid_count >= 3 else None  # Allow 1 missing quarter


def get_latest_value(quarterly_data: dict, field: str) -> Optional[float]:
    """Get latest non-null value from quarterly data."""
    if not quarterly_data:
        return None
    for q in sorted(quarterly_data.keys(), reverse=True):
        val = safe_float(quarterly_data[q].get(field))
        if val is not None:
            return val
    return None


# =============================================================================
# VALUATION MATH GUARDRAILS (P0 - 2026-02-27)
# =============================================================================
# EPSILON = 1e-3 (minimum meaningful denominator)
# MAX_CAP = 10000 (maximum meaningful multiple)
# =============================================================================

class MetricStatus:
    """Status codes for valuation metric computation."""
    OK = "ok"
    MISSING_RAW_DATA = "missing_raw_data"
    NEAR_ZERO_DENOMINATOR = "near_zero_denominator"
    NON_POSITIVE_VALUE = "non_positive_value"
    MISSING_SHARES = "missing_shares"
    EXTREME_OUTLIER = "extreme_outlier"


def safe_divide(
    numerator: float,
    denominator: float,
    require_positive_denominator: bool = True,
    epsilon: float = 1e-3,
    max_cap: float = 10000,
) -> tuple:
    """
    Safe division with guardrails for valuation metrics.
    
    Args:
        numerator: The numerator (e.g., price, market_cap, EV)
        denominator: The denominator (e.g., eps_ttm, revenue_ttm, ebitda_ttm)
        require_positive_denominator: If True, denominator must be > 0
        epsilon: Minimum absolute value for denominator
        max_cap: Maximum allowed result value
    
    Returns:
        (value, status): tuple of (float|None, MetricStatus)
    """
    # 1. Missing data
    if denominator is None or numerator is None:
        return None, MetricStatus.MISSING_RAW_DATA
    
    # 2. Near-zero floating point trap
    if abs(denominator) < epsilon:
        return None, MetricStatus.NEAR_ZERO_DENOMINATOR
    
    # 3. Non-positive check
    if require_positive_denominator and denominator <= 0:
        return None, MetricStatus.NON_POSITIVE_VALUE
    
    # 4. Math is now guaranteed safe
    result = numerator / denominator
    
    # 5. Extreme Outlier check
    if result > max_cap:
        return None, MetricStatus.EXTREME_OUTLIER
    
    return round(result, 4), MetricStatus.OK


# =============================================================================
# MAIN PRE-COMPUTATION FUNCTION
# =============================================================================

async def precompute_ticker_valuations(db) -> Dict[str, Any]:
    """
    Pre-compute valuation metrics for ALL visible tickers.
    
    BINDING: Stores results in ticker_valuations_cache collection.
    
    Schema:
    {
        "_id": "AAPL.US",
        "current_metrics": {
            "pe": 33.9,
            "ps": 9.2,
            "pb": 45.3,
            "ev_ebitda": 26.4,
            "ev_revenue": 9.3
        },
        "eval_vs_peers": "more_expensive",
        "eval_vs_5y": "more_expensive",
        "computed_at": "2026-02-26T04:00:00Z"
    }
    """
    logger.info("Starting ticker valuations pre-computation...")
    start_time = datetime.now(timezone.utc)
    
    # Get all visible tickers (just metadata first)
    ticker_list = await db.tracked_tickers.find(
        {"is_visible": True},
        {"_id": 0, "ticker": 1, "sector": 1, "industry": 1}
    ).to_list(length=10000)
    
    logger.info(f"Found {len(ticker_list)} visible tickers")
    
    # Get latest prices for all tickers
    prices = {}
    cursor = db.stock_prices.aggregate([
        {"$sort": {"date": -1}},
        {"$group": {"_id": "$ticker", "price": {"$first": "$adjusted_close"}}}
    ])
    async for doc in cursor:
        if doc.get("price"):
            prices[doc["_id"]] = doc["price"]
    
    logger.info(f"Got prices for {len(prices)} tickers")
    
    # Load peer benchmarks for comparison
    peer_benchmarks = {}
    cursor = db.peer_benchmarks.find({})
    async for doc in cursor:
        key = doc.get("industry") or f"sector:{doc.get('sector')}"
        if key:
            peer_benchmarks[key] = doc.get("benchmarks", {})
    
    logger.info(f"Loaded {len(peer_benchmarks)} peer benchmarks")
    
    # Process tickers in batches
    batch_size = 100
    processed = 0
    stored = 0
    
    for batch_start in range(0, len(ticker_list), batch_size):
        batch = ticker_list[batch_start:batch_start + batch_size]
        batch_tickers = [t["ticker"] for t in batch]
        
        # Fetch fundamentals for this batch
        fundamentals_map = {}
        cursor = db.tracked_tickers.find(
            {"ticker": {"$in": batch_tickers}},
            {"_id": 0, "ticker": 1, "fundamentals": 1}
        )
        async for doc in cursor:
            fundamentals_map[doc["ticker"]] = doc.get("fundamentals", {})
        
        # Process each ticker in batch
        for t in batch:
            ticker = t["ticker"]
            sector = t.get("sector")
            industry = t.get("industry")
            price = prices.get(ticker)
            fund = fundamentals_map.get(ticker, {})
            
            processed += 1
            
            if not price or not fund:
                continue
            
            # Extract data
            shares_stats = fund.get("SharesStats", {})
            financials = fund.get("Financials", {})
            earnings = fund.get("Earnings", {})
            
            # Shares outstanding
            shares = safe_float(shares_stats.get("SharesOutstanding"))
            if not shares:
                outstanding = fund.get("outstandingShares", {}).get("annual", {})
                if outstanding:
                    latest_year = sorted(outstanding.keys(), reverse=True)[0] if outstanding else None
                    if latest_year:
                        shares = safe_float(outstanding[latest_year].get("shares"))
            
            if not shares:
                continue
            
            # Market cap
            market_cap = price * shares
            
            # Financial statements
            income_stmt = financials.get("Income_Statement", {})
            quarterly_income = income_stmt.get("quarterly", {})
            balance_sheet = financials.get("Balance_Sheet", {})
            quarterly_balance = balance_sheet.get("quarterly", {})
            cash_flow = financials.get("Cash_Flow", {})
            quarterly_cashflow = cash_flow.get("quarterly", {})
            
            # TTM values from Income Statement
            revenue_ttm = get_ttm_sum(quarterly_income, "totalRevenue")
            net_income_ttm = get_ttm_sum(quarterly_income, "netIncome")
            ebitda_ttm = get_ttm_sum(quarterly_income, "ebitda")
            
            # If EBITDA not available, compute from operating income + D&A
            if not ebitda_ttm:
                operating_income = get_ttm_sum(quarterly_income, "operatingIncome")
                depreciation = get_ttm_sum(quarterly_cashflow, "depreciation")
                if operating_income:
                    ebitda_ttm = operating_income + abs(depreciation or 0)
            
            # Balance sheet values (latest)
            # BINDING: Correct key is "totalStockholderEquity" not "equity"
            total_equity = get_latest_value(quarterly_balance, "totalStockholderEquity")
            
            total_debt = get_latest_value(quarterly_balance, "totalDebt")
            if not total_debt:
                short_term = get_latest_value(quarterly_balance, "shortTermDebt") or 0
                long_term = get_latest_value(quarterly_balance, "longTermDebt") or 0
                total_debt = short_term + long_term if (short_term or long_term) else 0
            
            cash = get_latest_value(quarterly_balance, "cash")
            if not cash:
                cash = get_latest_value(quarterly_balance, "cashAndShortTermInvestments") or 0
            
            # EPS TTM from Earnings History (more accurate than computed)
            eps_ttm = None
            earnings_history = earnings.get("History", {})
            if earnings_history:
                sorted_q = sorted(earnings_history.keys(), reverse=True)[:4]
                eps_values = []
                for q in sorted_q:
                    eps_val = safe_float(earnings_history[q].get("epsActual"))
                    if eps_val is not None and eps_val != 0:
                        eps_values.append(eps_val)
                
                if len(eps_values) >= 3:  # Allow 1 missing quarter
                    eps_ttm = sum(eps_values)
            
            # Fallback: compute EPS from net income / shares
            if not eps_ttm and net_income_ttm and shares:
                eps_ttm = net_income_ttm / shares
            
            # Enterprise Value
            enterprise_value = market_cap + (total_debt or 0) - (cash or 0)
            
            # =================================================================
            # COMPUTE 5 METRICS WITH GUARDRAILS (P0 - 2026-02-27)
            # =================================================================
            metrics = {}
            metric_statuses = {}
            
            # SHARES GUARD: Check shares before market_cap/EV dependent metrics
            shares_valid = shares is not None and shares > 0
            market_cap_valid = shares_valid and market_cap is not None and market_cap > 0
            ev_valid = market_cap_valid and enterprise_value is not None
            
            # -----------------------------------------------------------------
            # P/E (Price to Earnings)
            # Denominator: eps_ttm (requires positive)
            # Does NOT require shares (price / eps_ttm)
            # -----------------------------------------------------------------
            pe_val, pe_status = safe_divide(price, eps_ttm, require_positive_denominator=True)
            if pe_val is not None:
                metrics["pe"] = pe_val
            metric_statuses["pe"] = pe_status
            
            # -----------------------------------------------------------------
            # P/S (Price to Sales = Market Cap / Revenue TTM)
            # REQUIRES valid market_cap (which requires shares)
            # -----------------------------------------------------------------
            if not market_cap_valid:
                ps_val, ps_status = None, MetricStatus.MISSING_SHARES
            else:
                ps_val, ps_status = safe_divide(market_cap, revenue_ttm, require_positive_denominator=True)
            if ps_val is not None:
                metrics["ps"] = ps_val
            metric_statuses["ps"] = ps_status
            
            # -----------------------------------------------------------------
            # P/B (Price to Book = Market Cap / Total Equity)
            # REQUIRES valid market_cap
            # -----------------------------------------------------------------
            if not market_cap_valid:
                pb_val, pb_status = None, MetricStatus.MISSING_SHARES
            else:
                pb_val, pb_status = safe_divide(market_cap, total_equity, require_positive_denominator=True)
            if pb_val is not None:
                metrics["pb"] = pb_val
            metric_statuses["pb"] = pb_status
            
            # -----------------------------------------------------------------
            # EV/EBITDA (Enterprise Value / EBITDA TTM)
            # REQUIRES valid EV (which requires market_cap → shares)
            # -----------------------------------------------------------------
            if not ev_valid:
                ev_ebitda_val, ev_ebitda_status = None, MetricStatus.MISSING_SHARES
            else:
                ev_ebitda_val, ev_ebitda_status = safe_divide(enterprise_value, ebitda_ttm, require_positive_denominator=True)
            if ev_ebitda_val is not None:
                metrics["ev_ebitda"] = ev_ebitda_val
            metric_statuses["ev_ebitda"] = ev_ebitda_status
            
            # -----------------------------------------------------------------
            # EV/Revenue (Enterprise Value / Revenue TTM)
            # REQUIRES valid EV
            # -----------------------------------------------------------------
            if not ev_valid:
                ev_revenue_val, ev_revenue_status = None, MetricStatus.MISSING_SHARES
            else:
                ev_revenue_val, ev_revenue_status = safe_divide(enterprise_value, revenue_ttm, require_positive_denominator=True)
            if ev_revenue_val is not None:
                metrics["ev_revenue"] = ev_revenue_val
            metric_statuses["ev_revenue"] = ev_revenue_status
            
            if not metrics:
                continue
            
            # =================================================================
            # COMPARE VS PEERS
            # =================================================================
            peer_bench = peer_benchmarks.get(industry) or peer_benchmarks.get(f"sector:{sector}") or {}
            
            vs_peers = {}
            for metric_name, current_val in metrics.items():
                peer_key = f"{metric_name}_median"
                peer_val = peer_bench.get(peer_key)
                
                if peer_val and peer_val > 0:
                    ratio = current_val / peer_val
                    if ratio < 0.85:
                        vs_peers[metric_name] = "cheaper"
                    elif ratio > 1.15:
                        vs_peers[metric_name] = "more_expensive"
                    else:
                        vs_peers[metric_name] = "around"
            
            # Overall vs peers (majority rule)
            if vs_peers:
                counts = {"cheaper": 0, "around": 0, "more_expensive": 0}
                for v in vs_peers.values():
                    counts[v] += 1
                
                total = sum(counts.values())
                if counts["more_expensive"] / total >= 0.6:
                    eval_vs_peers = "more_expensive"
                elif counts["cheaper"] / total >= 0.6:
                    eval_vs_peers = "cheaper"
                else:
                    eval_vs_peers = "around"
            else:
                eval_vs_peers = None
            
            # =================================================================
            # STORE IN CACHE
            # =================================================================
            cache_doc = {
                "_id": ticker,
                "ticker": ticker,
                "sector": sector,
                "industry": industry,
                "current_price": price,
                "market_cap": round(market_cap, 0),
                "current_metrics": metrics,
                "metric_statuses": metric_statuses,  # P0: Store guardrail statuses
                "peer_benchmarks": {f"{k}_median": v for k, v in peer_bench.items()} if peer_bench else None,
                "vs_peers": vs_peers if vs_peers else None,
                "eval_vs_peers": eval_vs_peers,
                "eval_vs_5y": None,  # TODO: Implement 5Y average comparison
                # P0 FIX: Store raw_inputs for Net Debt/EBITDA calculation in mobile_data
                "raw_inputs": {
                    "ebitda_ttm": ebitda_ttm,
                    "total_debt": total_debt,
                    "cash": cash,
                    "enterprise_value": enterprise_value if ev_valid else None,
                    "shares_outstanding": shares,
                },
                "computed_at": datetime.now(timezone.utc).isoformat()
            }
            
            await db.ticker_valuations_cache.replace_one(
                {"_id": ticker},
                cache_doc,
                upsert=True
            )
            stored += 1
        
        if processed % 500 == 0:
            logger.info(f"  Processed {processed}/{len(ticker_list)} tickers, stored {stored}")
    
    # Create index for fast lookups
    await db.ticker_valuations_cache.create_index("ticker", unique=True)
    
    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    
    logger.info(f"Valuation pre-computation completed: {stored} tickers in {elapsed:.1f}s")
    
    return {
        "status": "success",
        "tickers_processed": processed,
        "tickers_stored": stored,
        "elapsed_seconds": round(elapsed, 1)
    }


# =============================================================================
# ALSO UPDATE PEER BENCHMARKS WITH CORRECT KEYS
# =============================================================================

async def compute_peer_benchmarks_fixed(db) -> Dict[str, Any]:
    """
    Compute peer benchmarks with CORRECT field mappings.
    
    BINDING: Uses correct keys:
    - totalStockholderEquity (not "equity")
    - epsActual from Earnings.History
    """
    logger.info("Starting peer benchmarks computation (FIXED keys)...")
    start_time = datetime.now(timezone.utc)
    
    # Get all unique industries
    industries = await db.tracked_tickers.aggregate([
        {"$match": {"is_visible": True, "industry": {"$ne": None}}},
        {"$group": {"_id": "$industry", "sector": {"$first": "$sector"}, "count": {"$sum": 1}}},
        {"$match": {"count": {"$gte": 5}}},
        {"$sort": {"count": -1}}
    ]).to_list(length=500)
    
    logger.info(f"Found {len(industries)} industries with 5+ tickers")
    
    # Get latest prices
    prices = {}
    cursor = db.stock_prices.aggregate([
        {"$sort": {"date": -1}},
        {"$group": {"_id": "$ticker", "price": {"$first": "$adjusted_close"}}}
    ])
    async for doc in cursor:
        if doc.get("price"):
            prices[doc["_id"]] = doc["price"]
    
    stored = 0
    
    for ind_data in industries:
        industry = ind_data["_id"]
        sector = ind_data["sector"]
        
        # Get tickers in this industry with required fields
        tickers = await db.tracked_tickers.find(
            {"is_visible": True, "industry": industry, "fundamentals": {"$exists": True}},
            {
                "_id": 0, 
                "ticker": 1, 
                "fundamentals.SharesStats.SharesOutstanding": 1,
                "fundamentals.Financials.Income_Statement.quarterly": 1,
                "fundamentals.Financials.Balance_Sheet.quarterly": 1,
                "fundamentals.Earnings.History": 1
            }
        ).to_list(length=300)
        
        pe_values, ps_values, pb_values, ev_ebitda_values, ev_revenue_values = [], [], [], [], []
        
        for t in tickers:
            ticker = t.get("ticker")
            price = prices.get(ticker)
            if not price:
                continue
            
            fund = t.get("fundamentals", {})
            shares = safe_float(fund.get("SharesStats", {}).get("SharesOutstanding"))
            if not shares:
                continue
            
            market_cap = price * shares
            if market_cap <= 0:
                continue
            
            # Income statement TTM
            income = fund.get("Financials", {}).get("Income_Statement", {}).get("quarterly", {})
            if income:
                sorted_q = sorted(income.keys(), reverse=True)[:4]
                if len(sorted_q) >= 4:
                    revenue_ttm = sum(safe_float(income[q].get("totalRevenue")) or 0 for q in sorted_q)
                    ebitda_ttm = sum(safe_float(income[q].get("ebitda")) or 0 for q in sorted_q)
                    
                    if revenue_ttm > 0:
                        ps_values.append(market_cap / revenue_ttm)
                        ev_revenue_values.append(market_cap / revenue_ttm)  # Simplified EV
                    if ebitda_ttm > 0:
                        ev_ebitda_values.append(market_cap / ebitda_ttm)
            
            # EPS from Earnings.History (CORRECT)
            earnings = fund.get("Earnings", {}).get("History", {})
            if earnings:
                sorted_q = sorted(earnings.keys(), reverse=True)[:4]
                eps_vals = [safe_float(earnings[q].get("epsActual")) for q in sorted_q 
                           if safe_float(earnings[q].get("epsActual")) and safe_float(earnings[q].get("epsActual")) != 0]
                if len(eps_vals) >= 3:
                    eps_ttm = sum(eps_vals)
                    if eps_ttm > 0:
                        pe_values.append(price / eps_ttm)
            
            # Book value from Balance Sheet (CORRECT KEY: totalStockholderEquity)
            balance = fund.get("Financials", {}).get("Balance_Sheet", {}).get("quarterly", {})
            if balance:
                sorted_q = sorted(balance.keys(), reverse=True)
                for q in sorted_q:
                    equity = safe_float(balance[q].get("totalStockholderEquity"))
                    if equity and equity > 0:
                        pb_values.append(market_cap / equity)
                        break
        
        # Calculate medians
        benchmarks = {}
        if len(pe_values) >= 5:
            benchmarks["pe_median"] = round(statistics.median(pe_values), 2)
        if len(ps_values) >= 5:
            benchmarks["ps_median"] = round(statistics.median(ps_values), 2)
        if len(pb_values) >= 5:
            benchmarks["pb_median"] = round(statistics.median(pb_values), 2)
        if len(ev_ebitda_values) >= 5:
            benchmarks["ev_ebitda_median"] = round(statistics.median(ev_ebitda_values), 2)
        if len(ev_revenue_values) >= 5:
            benchmarks["ev_revenue_median"] = round(statistics.median(ev_revenue_values), 2)
        
        if benchmarks:
            await db.peer_benchmarks.update_one(
                {"industry": industry},
                {"$set": {
                    "industry": industry,
                    "sector": sector,
                    "peer_count": len(tickers),
                    "metrics_count": {
                        "pe": len(pe_values),
                        "ps": len(ps_values),
                        "pb": len(pb_values),
                        "ev_ebitda": len(ev_ebitda_values),
                        "ev_revenue": len(ev_revenue_values)
                    },
                    "benchmarks": benchmarks,
                    "computed_at": datetime.now(timezone.utc).isoformat()
                }},
                upsert=True
            )
            stored += 1
    
    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logger.info(f"Peer benchmarks completed: {stored} stored in {elapsed:.1f}s")
    
    return {"status": "success", "industries_stored": stored, "elapsed_seconds": round(elapsed, 1)}


# =============================================================================
# MAIN - Run both jobs
# =============================================================================

async def run_full_precompute():
    """Run both peer benchmarks and ticker valuations pre-computation."""
    client = AsyncIOMotorClient(os.environ.get('MONGO_URL', 'mongodb://localhost:27017'))
    db = client['richstox_prod']
    
    # 1. First compute peer benchmarks with fixed keys
    result1 = await compute_peer_benchmarks_fixed(db)
    print(f"Peer benchmarks: {result1}")
    
    # 2. Then compute ticker valuations
    result2 = await precompute_ticker_valuations(db)
    print(f"Ticker valuations: {result2}")
    
    client.close()
    return {"peer_benchmarks": result1, "ticker_valuations": result2}


if __name__ == "__main__":
    asyncio.run(run_full_precompute())
