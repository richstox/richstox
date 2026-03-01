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

"""
RICHSTOX Local Metrics Calculator
=================================
Computes all metrics locally from RAW FACTS.

=============================================================================
RAW FACTS ONLY. No precomputed metrics from EODHD.
=============================================================================

All metrics are calculated from:
- stock_prices (OHLCV data)
- company_fundamentals_cache (raw financial statements)
- dividend_history (dividend payments)

Never use precomputed P/E, EV/EBITDA, beta, etc. from EODHD.
"""

import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional, List, Tuple
import math

logger = logging.getLogger("richstox.metrics")


# =============================================================================
# REALITY CHECK METRICS (MAX history - since IPO)
# =============================================================================

async def calculate_reality_check_max(db, ticker: str) -> Dict[str, Any]:
    """
    Calculate Reality Check metrics for MAX history (since IPO or first available price).
    
    Returns:
        - total_return_pct: Total return including dividends (reinvested)
        - max_drawdown_pct: Worst drop from peak
        - cagr_pct: Compound Annual Growth Rate
        - benchmark_cagr_pct: S&P 500 TR CAGR for SAME period (after clamping)
        - benchmark_start_date: Actual start date used for benchmark comparison
        - start_date: First available date for ticker
        - end_date: Last available date
        - years: Number of years
    
    BENCHMARK COMPARISON:
    - Uses SP500TR.INDX from DB (Total Return Index, includes dividends)
    - Clamps to earliest benchmark date (1988-01-04)
    - Returns CAGR (annualized), NOT total return (easier to compare)
    """
    ticker_full = ticker if ticker.endswith(".US") else f"{ticker}.US"
    
    # Benchmark config
    SP500TR_TICKER = "SP500TR.INDX"
    SP500TR_EARLIEST = "1988-01-04"
    SANITY_MAX_TOTAL_RETURN = 50000  # 50,000% max for sanity check
    
    # Get all prices for ticker (ascending by date)
    prices = await db.stock_prices.find(
        {"ticker": ticker_full},
        {"_id": 0, "date": 1, "close": 1, "adjusted_close": 1}
    ).sort("date", 1).to_list(length=20000)
    
    if len(prices) < 2:
        return None
    
    start_date = prices[0]["date"]
    end_date = prices[-1]["date"]
    start_price = prices[0].get("adjusted_close") or prices[0]["close"]
    end_price = prices[-1].get("adjusted_close") or prices[-1]["close"]
    
    # Calculate years
    try:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        years = (end_dt - start_dt).days / 365.25
    except:
        years = 1
    
    if years < 0.1:
        years = 0.1  # Minimum to avoid division issues
    
    # Total Return (using adjusted_close which includes dividends)
    if start_price and start_price > 0:
        total_return_pct = ((end_price / start_price) - 1) * 100
    else:
        total_return_pct = 0
    
    # CAGR (Compound Annual Growth Rate)
    if start_price and start_price > 0 and years > 0:
        cagr_pct = (pow(end_price / start_price, 1 / years) - 1) * 100
    else:
        cagr_pct = 0
    
    # Max Drawdown (Worst Drop)
    max_drawdown_pct = calculate_max_drawdown([p.get("adjusted_close") or p["close"] for p in prices])
    
    # ==========================================================================
    # BENCHMARK COMPARISON (SP500TR.INDX)
    # CRITICAL: Compare SAME period for both ticker and benchmark
    # ==========================================================================
    benchmark_cagr_pct = None
    benchmark_start_date = None
    
    # Clamp start date to benchmark availability
    comparison_start = max(start_date, SP500TR_EARLIEST)
    
    # Get ticker price at clamped start date
    ticker_at_start = await db.stock_prices.find_one(
        {"ticker": ticker_full, "date": {"$gte": comparison_start}},
        {"_id": 0, "close": 1, "adjusted_close": 1, "date": 1},
        sort=[("date", 1)]
    )
    
    # Get SP500TR at clamped start date
    sp500_at_start = await db.stock_prices.find_one(
        {"ticker": SP500TR_TICKER, "date": {"$gte": comparison_start}},
        {"_id": 0, "close": 1, "date": 1},
        sort=[("date", 1)]
    )
    
    # Get SP500TR at end date
    sp500_at_end = await db.stock_prices.find_one(
        {"ticker": SP500TR_TICKER, "date": {"$lte": end_date}},
        {"_id": 0, "close": 1, "date": 1},
        sort=[("date", -1)]
    )
    
    if sp500_at_start and sp500_at_end and ticker_at_start:
        sp500_start_price = sp500_at_start.get("close")
        sp500_end_price = sp500_at_end.get("close")
        benchmark_start_date = sp500_at_start.get("date")
        
        if sp500_start_price and sp500_start_price > 0 and sp500_end_price:
            # Calculate SP500TR total return for the period
            sp500_total_return = ((sp500_end_price / sp500_start_price) - 1) * 100
            
            # Sanity check
            if sp500_total_return > SANITY_MAX_TOTAL_RETURN:
                logger.warning(f"SP500TR sanity check failed: {sp500_total_return:.1f}% > {SANITY_MAX_TOTAL_RETURN}%")
                benchmark_cagr_pct = None
            else:
                # Calculate years for benchmark period
                try:
                    bench_start_dt = datetime.strptime(benchmark_start_date, "%Y-%m-%d")
                    bench_end_dt = datetime.strptime(end_date, "%Y-%m-%d")
                    bench_years = (bench_end_dt - bench_start_dt).days / 365.25
                except:
                    bench_years = years
                
                if bench_years > 0.1:
                    # Return CAGR (annualized) for easier comparison
                    benchmark_cagr_pct = (pow(sp500_end_price / sp500_start_price, 1 / bench_years) - 1) * 100
    
    # Calculate Efficiency Score: total_return / abs(max_drawdown)
    efficiency_score = None
    if max_drawdown_pct and abs(max_drawdown_pct) > 0:
        efficiency_score = total_return_pct / abs(max_drawdown_pct)
    
    # Calculate Relative Outperformance (ratio-based)
    # outperformance_pct = (stock_cagr / sp500_cagr - 1) * 100
    outperformance_pct = None
    if cagr_pct is not None and benchmark_cagr_pct is not None and benchmark_cagr_pct > 0:
        outperformance_pct = ((cagr_pct / benchmark_cagr_pct) - 1) * 100
    
    return {
        "total_return_pct": round(total_return_pct, 1),
        "max_drawdown_pct": round(max_drawdown_pct, 1),
        "cagr_pct": round(cagr_pct, 1),
        "benchmark_cagr_pct": round(benchmark_cagr_pct, 1) if benchmark_cagr_pct is not None else None,
        "outperformance_pct": round(outperformance_pct, 1) if outperformance_pct is not None else None,
        "efficiency_score": round(efficiency_score, 1) if efficiency_score is not None else None,
        "benchmark_start_date": benchmark_start_date,
        "start_date": start_date,
        "end_date": end_date,
        "years": round(years, 1),
    }


def calculate_max_drawdown(prices: List[float]) -> float:
    """Calculate maximum drawdown from a list of prices."""
    if not prices or len(prices) < 2:
        return 0
    
    peak = prices[0]
    max_dd = 0
    
    for price in prices:
        if price > peak:
            peak = price
        dd = ((peak - price) / peak) * 100 if peak > 0 else 0
        if dd > max_dd:
            max_dd = dd
    
    return max_dd


async def calculate_sp500_return(db, start_date: str, end_date: str) -> Optional[float]:
    """
    Calculate S&P 500 Total Return for given period.
    Uses SP500TR.INDX (S&P 500 Total Return Index) from EODHD.
    Data available from 1988-01-04.
    """
    sp500_ticker = "SP500TR.INDX"
    
    # If start_date is before data availability, use earliest available date
    earliest_date = "1988-01-04"
    if start_date < earliest_date:
        start_date = earliest_date
    
    start_price_doc = await db.stock_prices.find_one(
        {"ticker": sp500_ticker, "date": {"$gte": start_date}},
        {"_id": 0, "close": 1, "date": 1},
        sort=[("date", 1)]
    )
    
    end_price_doc = await db.stock_prices.find_one(
        {"ticker": sp500_ticker, "date": {"$lte": end_date}},
        {"_id": 0, "close": 1, "date": 1},
        sort=[("date", -1)]
    )
    
    if not start_price_doc or not end_price_doc:
        return None
    
    start_price = start_price_doc.get("close")
    end_price = end_price_doc.get("close")
    
    if start_price and start_price > 0 and end_price:
        return ((end_price / start_price) - 1) * 100
    return None


# =============================================================================
# PERIOD STATS (for chart period selector)
# =============================================================================

async def calculate_period_stats(db, ticker: str, period: str) -> Dict[str, Any]:
    """
    Calculate stats for a specific period.
    
    Args:
        period: "3M", "6M", "YTD", "1Y", "3Y", "5Y"
    
    Returns:
        - profit_pct: Total return for period
        - max_drawdown_pct: Max drawdown for period
        - cagr_pct: Annualized return (CAGR)
        - benchmark_total_pct: SP500TR total return for SAME period (for Period Stats, show total %)
    """
    ticker_full = ticker if ticker.endswith(".US") else f"{ticker}.US"
    
    # Benchmark config
    SP500TR_TICKER = "SP500TR.INDX"
    
    # Calculate start date based on period
    now = datetime.now(timezone.utc)
    
    if period == "3M":
        start_dt = now - timedelta(days=90)
        years = 0.25
    elif period == "6M":
        start_dt = now - timedelta(days=180)
        years = 0.5
    elif period == "YTD":
        start_dt = datetime(now.year, 1, 1, tzinfo=timezone.utc)
        years = (now - start_dt).days / 365.0
    elif period == "1Y":
        start_dt = now - timedelta(days=365)
        years = 1.0
    elif period == "3Y":
        start_dt = now - timedelta(days=365 * 3)
        years = 3.0
    elif period == "5Y":
        start_dt = now - timedelta(days=365 * 5)
        years = 5.0
    elif period == "MAX":
        # P1 CRITICAL FIX: Get earliest available price date from database
        earliest = await db.stock_prices.find_one(
            {"ticker": ticker_full},
            {"_id": 0, "date": 1},
            sort=[("date", 1)]
        )
        if earliest:
            start_dt = datetime.strptime(earliest["date"], "%Y-%m-%d").replace(tzinfo=timezone.utc)
        else:
            start_dt = now - timedelta(days=365 * 20)  # Fallback 20 years
        years = (now - start_dt).days / 365.0
    else:
        start_dt = now - timedelta(days=365)  # Default 1Y
        years = 1.0
    
    start_date = start_dt.strftime("%Y-%m-%d")
    end_date = now.strftime("%Y-%m-%d")
    
    # Get prices for period
    prices = await db.stock_prices.find(
        {"ticker": ticker_full, "date": {"$gte": start_date}},
        {"_id": 0, "date": 1, "close": 1, "adjusted_close": 1}
    ).sort("date", 1).to_list(length=None if period == "MAX" else 2000)  # No limit for MAX
    
    if len(prices) < 2:
        return None
    
    actual_start_date = prices[0]["date"]
    actual_end_date = prices[-1]["date"]
    start_price = prices[0].get("adjusted_close") or prices[0]["close"]
    end_price = prices[-1].get("adjusted_close") or prices[-1]["close"]
    
    # Profit (Total Return)
    profit_pct = ((end_price / start_price) - 1) * 100 if start_price > 0 else 0
    
    # CAGR (Annualized return)
    cagr_pct = None
    if start_price > 0 and years > 0:
        total_return = end_price / start_price
        if total_return > 0:
            cagr_pct = (pow(total_return, 1.0 / years) - 1) * 100
    
    # Max Drawdown
    max_dd = calculate_max_drawdown([p.get("adjusted_close") or p["close"] for p in prices])
    
    # ==========================================================================
    # BENCHMARK (SP500TR.INDX) - SAME period, return TOTAL % (not CAGR)
    # ==========================================================================
    benchmark_total_pct = None
    
    # Get SP500TR at period start
    sp500_at_start = await db.stock_prices.find_one(
        {"ticker": SP500TR_TICKER, "date": {"$gte": actual_start_date}},
        {"_id": 0, "close": 1, "date": 1},
        sort=[("date", 1)]
    )
    
    # Get SP500TR at period end
    sp500_at_end = await db.stock_prices.find_one(
        {"ticker": SP500TR_TICKER, "date": {"$lte": actual_end_date}},
        {"_id": 0, "close": 1, "date": 1},
        sort=[("date", -1)]
    )
    
    if sp500_at_start and sp500_at_end:
        sp500_start_price = sp500_at_start.get("close")
        sp500_end_price = sp500_at_end.get("close")
        
        if sp500_start_price and sp500_start_price > 0 and sp500_end_price:
            # For Period Stats, show TOTAL return % (not annualized)
            benchmark_total_pct = ((sp500_end_price / sp500_start_price) - 1) * 100
    
    # Calculate Relative Outperformance (ratio-based) for short periods
    # outperformance_pct = (stock_total / sp500_total - 1) * 100
    outperformance_pct = None
    if profit_pct is not None and benchmark_total_pct is not None and benchmark_total_pct > 0:
        # Convert percentages to multipliers: (1 + profit_pct/100) / (1 + benchmark/100) - 1
        stock_multiplier = 1 + (profit_pct / 100)
        bench_multiplier = 1 + (benchmark_total_pct / 100)
        if bench_multiplier > 0:
            outperformance_pct = ((stock_multiplier / bench_multiplier) - 1) * 100
    
    return {
        "period": period,
        "profit_pct": round(profit_pct, 1),
        "max_drawdown_pct": round(max_dd, 1),
        "cagr_pct": round(cagr_pct, 1) if cagr_pct is not None else None,
        "benchmark_total_pct": round(benchmark_total_pct, 1) if benchmark_total_pct is not None else None,
        "outperformance_pct": round(outperformance_pct, 1) if outperformance_pct is not None else None,
        "start_date": actual_start_date,
        "end_date": actual_end_date,
    }


# =============================================================================
# VALUATION (Local compute from RAW FACTS)
# =============================================================================

async def calculate_local_pe(db, ticker: str, current_price: float) -> Optional[float]:
    """
    Calculate P/E ratio locally from raw financial statements.
    
    P/E = Current Price / EPS_TTM
    EPS_TTM = Sum of last 4 quarters diluted EPS
    """
    ticker_full = ticker if ticker.endswith(".US") else f"{ticker}.US"
    symbol = ticker.replace(".US", "").upper()
    
    # Get raw financial data
    fundamentals = await db.company_fundamentals_cache.find_one(
        {"symbol": symbol},
        {"_id": 0, "income_statement_quarterly": 1, "earnings_history": 1, "shares_outstanding_quarterly": 1}
    )
    
    if not fundamentals:
        return None
    
    # Try to get EPS from earnings_history first
    earnings = fundamentals.get("earnings_history", {})
    if earnings:
        # Get last 4 quarters EPS
        sorted_quarters = sorted(earnings.keys(), reverse=True)[:4]
        eps_values = []
        for q in sorted_quarters:
            eps = earnings[q].get("epsActual")
            if eps is not None:
                eps_values.append(eps)
        
        if len(eps_values) >= 4:
            eps_ttm = sum(eps_values)
            if eps_ttm > 0 and current_price:
                return current_price / eps_ttm
    
    # Fallback: Calculate from income statement
    income = fundamentals.get("income_statement_quarterly", {})
    shares = fundamentals.get("shares_outstanding_quarterly", [])
    
    if income:
        sorted_quarters = sorted(income.keys(), reverse=True)[:4]
        net_income_ttm = 0
        
        for q in sorted_quarters:
            ni = income[q].get("netIncome")
            if ni:
                # Ensure numeric type
                try:
                    ni_val = float(ni)
                    net_income_ttm += ni_val
                except (ValueError, TypeError):
                    pass
        
        # Get shares outstanding
        shares_out = None
        if shares:
            try:
                if isinstance(shares, dict):
                    # shares is a dict with quarter dates as keys
                    latest_key = sorted(shares.keys(), reverse=True)[0] if shares else None
                    if latest_key:
                        shares_val = shares[latest_key].get("commonSharesOutstanding") or shares[latest_key].get("shares")
                        shares_out = float(shares_val) if shares_val else None
                elif isinstance(shares, list) and len(shares) > 0:
                    # shares is a list
                    shares_val = shares[0].get("shares") if isinstance(shares[0], dict) else shares[0]
                    shares_out = float(shares_val) if shares_val else None
            except (ValueError, TypeError, KeyError, IndexError):
                shares_out = None
        
        if net_income_ttm > 0 and shares_out and shares_out > 0 and current_price:
            eps_ttm = net_income_ttm / shares_out
            return current_price / eps_ttm
    
    return None


async def calculate_local_ev_ebitda(db, ticker: str, current_price: float) -> Optional[float]:
    """
    Calculate EV/EBITDA locally from raw financial statements.
    
    EV = Market Cap + Total Debt - Cash
    EBITDA = Operating Income + Depreciation (TTM)
    """
    symbol = ticker.replace(".US", "").upper()
    
    fundamentals = await db.company_fundamentals_cache.find_one(
        {"symbol": symbol},
        {"_id": 0, "balance_sheet_quarterly": 1, "income_statement_quarterly": 1, 
         "cash_flow_quarterly": 1, "shares_outstanding_quarterly": 1}
    )
    
    if not fundamentals:
        return None
    
    # Get shares outstanding for market cap
    shares = fundamentals.get("shares_outstanding_quarterly", {})
    shares_out = None
    try:
        if isinstance(shares, dict) and shares:
            latest_key = sorted(shares.keys(), reverse=True)[0]
            shares_val = shares[latest_key].get("commonSharesOutstanding") or shares[latest_key].get("shares")
            shares_out = float(shares_val) if shares_val else None
        elif isinstance(shares, list) and len(shares) > 0:
            shares_val = shares[0].get("shares") if isinstance(shares[0], dict) else shares[0]
            shares_out = float(shares_val) if shares_val else None
    except (ValueError, TypeError, KeyError, IndexError):
        shares_out = None
    
    if not shares_out or not current_price:
        return None
    
    market_cap = current_price * shares_out
    
    # Get latest balance sheet for debt and cash
    balance = fundamentals.get("balance_sheet_quarterly", {})
    if not balance:
        return None
    
    latest_bs = balance.get(sorted(balance.keys(), reverse=True)[0], {}) if balance else {}
    
    # Safe float conversion for debt/cash values
    def safe_float(val):
        if val is None:
            return 0
        try:
            return float(val)
        except (ValueError, TypeError):
            return 0
    
    short_debt = safe_float(latest_bs.get("shortLongTermDebt"))
    long_debt = safe_float(latest_bs.get("longTermDebt"))
    total_debt = short_debt + long_debt
    cash = safe_float(latest_bs.get("cash") or latest_bs.get("cashAndCashEquivalentsAtCarryingValue"))
    
    ev = market_cap + total_debt - cash
    
    # Calculate EBITDA TTM
    income = fundamentals.get("income_statement_quarterly", {})
    cash_flow = fundamentals.get("cash_flow_quarterly", {})
    
    if not income:
        return None
    
    sorted_quarters = sorted(income.keys(), reverse=True)[:4]
    ebitda_ttm = 0
    
    for q in sorted_quarters:
        op_income = safe_float(income[q].get("operatingIncome"))
        # Get depreciation from cash flow
        cf_q = cash_flow.get(q, {}) if cash_flow else {}
        depreciation = safe_float(cf_q.get("depreciation") or cf_q.get("depreciationAndAmortization"))
        ebitda_ttm += op_income + depreciation
    
    if ebitda_ttm > 0:
        return ev / ebitda_ttm
    
    return None


async def get_peer_median(db, ticker: str, metric: str = "pe_ratio") -> Tuple[Optional[float], int, str]:
    """
    Get peer median for comparison.
    
    Args:
        metric: "pe_ratio" or "ev_ebitda"
    
    Returns:
        (median_value, peer_count, comparison_type)
        comparison_type: "industry" or "sector"
    """
    symbol = ticker.replace(".US", "").upper()
    
    # Get ticker's industry and sector
    tracked = await db.tracked_tickers.find_one(
        {"ticker": f"{symbol}.US"},
        {"_id": 0, "industry": 1, "sector": 1}
    )
    
    if not tracked:
        return None, 0, "none"
    
    industry = tracked.get("industry")
    sector = tracked.get("sector")
    
    PEER_THRESHOLD = 12
    
    # Try industry first
    if industry:
        industry_peers = await db.tracked_tickers.find(
            {"is_visible": True, "industry": industry, "ticker": {"$ne": f"{symbol}.US"}},
            {"_id": 0, "ticker": 1}
        ).to_list(length=200)
        
        if len(industry_peers) >= PEER_THRESHOLD:
            median = await calculate_peer_metric_median(db, [p["ticker"] for p in industry_peers], metric)
            return median, len(industry_peers), "industry"
    
    # Fallback to sector
    if sector:
        sector_peers = await db.tracked_tickers.find(
            {"is_visible": True, "sector": sector, "ticker": {"$ne": f"{symbol}.US"}},
            {"_id": 0, "ticker": 1}
        ).to_list(length=500)
        
        median = await calculate_peer_metric_median(db, [p["ticker"] for p in sector_peers], metric)
        return median, len(sector_peers), "sector"
    
    return None, 0, "none"


async def calculate_peer_metric_median(db, peer_tickers: List[str], metric: str) -> Optional[float]:
    """Calculate median of a metric for peer tickers."""
    values = []
    
    for ticker in peer_tickers[:50]:  # Limit to 50 for performance
        # Get current price
        price_doc = await db.stock_prices.find_one(
            {"ticker": ticker},
            {"_id": 0, "close": 1},
            sort=[("date", -1)]
        )
        
        if not price_doc:
            continue
        
        current_price = price_doc["close"]
        
        # Get fundamentals for this peer
        symbol = ticker.replace(".US", "").upper()
        fundamentals = await db.company_fundamentals_cache.find_one(
            {"symbol": symbol},
            {"_id": 0}
        )
        
        val = None
        
        if metric == "pe_ratio":
            val = await calculate_local_pe(db, ticker, current_price)
        elif metric == "ev_ebitda":
            val = await calculate_local_ev_ebitda(db, ticker, current_price)
        elif metric == "ps_ratio" and fundamentals:
            val = await compute_ps_ratio(db, ticker, current_price, fundamentals)
        elif metric == "pb_ratio" and fundamentals:
            val = await compute_pb_ratio(db, ticker, current_price, fundamentals)
        elif metric == "ev_revenue" and fundamentals:
            val = await compute_ev_revenue_ratio(db, ticker, current_price, fundamentals)
        
        if val and val > 0 and val < 1000:  # Filter outliers
            values.append(val)
    
    if not values:
        return None
    
    values.sort()
    n = len(values)
    if n % 2 == 0:
        return (values[n//2 - 1] + values[n//2]) / 2
    return values[n//2]


async def get_5y_average_pe(db, ticker: str) -> Optional[float]:
    """
    Calculate 5-year average P/E from historical data.
    
    Uses quarterly EPS and price data to compute historical P/E.
    """
    symbol = ticker.replace(".US", "").upper()
    ticker_full = f"{symbol}.US"
    
    # Get earnings history
    fundamentals = await db.company_fundamentals_cache.find_one(
        {"symbol": symbol},
        {"_id": 0, "earnings_history": 1}
    )
    
    if not fundamentals or not fundamentals.get("earnings_history"):
        return None
    
    earnings = fundamentals["earnings_history"]
    
    # Get last 20 quarters (5 years)
    sorted_quarters = sorted(earnings.keys(), reverse=True)[:20]
    
    pe_values = []
    
    for q_date in sorted_quarters:
        q_data = earnings[q_date]
        eps = q_data.get("epsActual")
        
        if not eps or eps <= 0:
            continue
        
        # Get price for that quarter end
        price_doc = await db.stock_prices.find_one(
            {"ticker": ticker_full, "date": {"$lte": q_date}},
            {"_id": 0, "close": 1},
            sort=[("date", -1)]
        )
        
        if price_doc:
            # Annualize EPS (multiply by 4 for quarterly)
            eps_annual = eps * 4
            pe = price_doc["close"] / eps_annual
            if 0 < pe < 200:  # Filter outliers
                pe_values.append(pe)
    
    if len(pe_values) < 4:
        return None
    
    return sum(pe_values) / len(pe_values)


async def get_valuation_comparison(db, ticker: str, current_price: float) -> Dict[str, Any]:
    """
    DEPRECATED - use get_valuation_overview instead.
    Kept for backward compatibility.
    """
    return await get_valuation_overview(db, ticker, current_price)


async def compute_5y_metric_averages(db, ticker: str) -> Dict[str, Any]:
    """
    Compute 5Y arithmetic mean for valuation multiples from ticker_key_metrics_daily.
    
    P2 REQUIREMENT: Always return structure, never hide.
    If insufficient data, return na_reason instead of omitting.
    
    Returns:
        {
            "pe": {"avg_5y": float|None, "data_points": int, "na_reason": str|None},
            "ps": {"avg_5y": float|None, "data_points": int, "na_reason": str|None},
            "pb": {"avg_5y": float|None, "data_points": int, "na_reason": str|None},
            "ev_ebitda": {"avg_5y": float|None, "data_points": int, "na_reason": str|None},
            "ev_revenue": {"avg_5y": float|None, "data_points": int, "na_reason": str|None},
            "overall_vs_5y": "cheaper"|"around"|"more_expensive"|None,
            "history_available": bool
        }
    """
    # Mapping from our metric names to DB field names
    metric_mapping = {
        "pe": "pe_ttm",
        "ps": "ps_ttm", 
        "pb": "pb",
        "ev_ebitda": "ev_ebitda_ttm",
        "ev_revenue": "ev_revenue_ttm"
    }
    
    result = {}
    
    for metric_key, db_field in metric_mapping.items():
        # Compute average from ticker_key_metrics_daily
        pipeline = [
            {"$match": {"ticker": ticker, db_field: {"$type": "number", "$gt": 0}}},
            {"$group": {
                "_id": None,
                "avg": {"$avg": f"${db_field}"},
                "count": {"$sum": 1}
            }}
        ]
        
        agg_result = await db.ticker_key_metrics_daily.aggregate(pipeline).to_list(1)
        
        if agg_result and agg_result[0]["count"] >= 12:  # Need at least 12 months (1 year) of data
            result[metric_key] = {
                "avg_5y": round(agg_result[0]["avg"], 2),
                "data_points": agg_result[0]["count"],
                "na_reason": None
            }
        elif agg_result and agg_result[0]["count"] > 0:
            # Some data but not enough
            result[metric_key] = {
                "avg_5y": round(agg_result[0]["avg"], 2),
                "data_points": agg_result[0]["count"],
                "na_reason": "insufficient_history"
            }
        else:
            # No numeric data at all
            result[metric_key] = {
                "avg_5y": None,
                "data_points": 0,
                "na_reason": "no_history" if metric_key in ["pe", "ev_ebitda"] else "missing_data"
            }
    
    # Check if we have meaningful history
    metrics_with_history = sum(1 for m in result.values() if m["avg_5y"] is not None and m["data_points"] >= 12)
    result["history_available"] = metrics_with_history >= 2  # Need at least 2 metrics with history
    
    return result


async def get_valuation_overview(db, ticker: str, current_price: float) -> Dict[str, Any]:
    """
    Compute OVERALL valuation badge using up to 5 locally computed multiples.
    
    Multiples:
    - P/E = price / EPS_TTM
    - P/S = market_cap / revenue_TTM
    - P/B = market_cap / book_value
    - EV/EBITDA = (market_cap + debt - cash) / EBITDA_TTM
    - EV/Revenue = (market_cap + debt - cash) / revenue_TTM
    
    Benchmarks:
    - vs peers: industry median (fallback sector if peers < 12)
    - vs 5Y average: average of last 20 quarterly computed points
    
    Classification per metric:
    - Cheaper: metric < 0.85 × benchmark
    - Around: 0.85-1.15 × benchmark  
    - More expensive: metric > 1.15 × benchmark
    
    Overall badge (majority rule):
    - If ≥60% are "more_expensive" → Overall = more_expensive
    - If ≥60% are "cheaper" → Overall = cheaper
    - Else → around
    """
    ticker_full = ticker if ticker.endswith(".US") else f"{ticker}.US"
    symbol = ticker.replace(".US", "").upper()
    
    # Thresholds for transparency
    thresholds = {
        "cheaper": 0.85,
        "expensive": 1.15,
        "majority": 0.6
    }
    
    # Get raw fundamentals from DB
    fundamentals = await db.company_fundamentals_cache.find_one(
        {"symbol": symbol},
        {"_id": 0}
    )
    
    if not fundamentals:
        return {
            "available": False,
            "reason": "No fundamental data available",
            "disclaimer": "Context only, not advice.",
            "thresholds": thresholds
        }
    
    # Get ticker info for peer lookup
    ticker_info = await db.tracked_tickers.find_one(
        {"ticker": ticker_full},
        {"_id": 0, "sector": 1, "industry": 1}
    )
    
    sector = ticker_info.get("sector") if ticker_info else None
    industry = ticker_info.get("industry") if ticker_info else None
    
    # Compute all 5 metrics locally from raw data
    # P0 REQUIREMENT: ALL 5 metrics must be present, even if N/A
    metrics = {}
    
    # Helper function to classify a metric
    def classify_metric(current_val, benchmark_val):
        if current_val is None or benchmark_val is None or benchmark_val <= 0:
            return None
        ratio = current_val / benchmark_val
        if ratio < thresholds["cheaper"]:
            return "cheaper"
        elif ratio > thresholds["expensive"]:
            return "more_expensive"
        else:
            return "around"
    
    # 1. P/E Ratio - ALWAYS include, even if N/A
    pe_current = await calculate_local_pe(db, ticker, current_price)
    pe_peer_median, pe_peer_count, pe_peer_type = await get_peer_median_for_metric(db, ticker, sector, industry, "pe_ratio")
    pe_5y_avg = await get_5y_average_pe(db, ticker)
    
    metrics["pe"] = {
        "name": "P/E",
        "current": round(pe_current, 1) if pe_current else None,
        "peer_median": round(pe_peer_median, 1) if pe_peer_median else None,
        "peer_count": pe_peer_count,
        "peer_type": pe_peer_type,
        "avg_5y": round(pe_5y_avg, 1) if pe_5y_avg else None,
        "vs_peers": classify_metric(pe_current, pe_peer_median),
        "vs_5y": classify_metric(pe_current, pe_5y_avg),
        "benchmark_used": {
            "peers": pe_peer_median is not None,
            "5y": pe_5y_avg is not None
        },
        "na_reason": "unprofitable" if not pe_current else None
    }
    
    # 2. P/S Ratio (Price to Sales) - ALWAYS include
    ps_data = await compute_ps_ratio(db, ticker, current_price, fundamentals)
    ps_peer_median, ps_peer_count, ps_peer_type = await get_peer_median_for_metric(db, ticker, sector, industry, "ps_ratio")
    ps_5y_avg = await get_5y_average_for_metric(db, ticker, "ps_ratio")
    
    metrics["ps"] = {
        "name": "P/S",
        "current": round(ps_data, 1) if ps_data else None,
        "peer_median": round(ps_peer_median, 1) if ps_peer_median else None,
        "peer_count": ps_peer_count,
        "peer_type": ps_peer_type,
        "avg_5y": round(ps_5y_avg, 1) if ps_5y_avg else None,
        "vs_peers": classify_metric(ps_data, ps_peer_median),
        "vs_5y": classify_metric(ps_data, ps_5y_avg),
        "benchmark_used": {
            "peers": ps_peer_median is not None,
            "5y": ps_5y_avg is not None
        },
        "na_reason": "missing_data" if not ps_data else None
    }
    
    # 3. P/B Ratio (Price to Book) - ALWAYS include
    pb_data = await compute_pb_ratio(db, ticker, current_price, fundamentals)
    pb_peer_median, pb_peer_count, pb_peer_type = await get_peer_median_for_metric(db, ticker, sector, industry, "pb_ratio")
    pb_5y_avg = await get_5y_average_for_metric(db, ticker, "pb_ratio")
    
    metrics["pb"] = {
        "name": "P/B",
        "current": round(pb_data, 1) if pb_data else None,
        "peer_median": round(pb_peer_median, 1) if pb_peer_median else None,
        "peer_count": pb_peer_count,
        "peer_type": pb_peer_type,
        "avg_5y": round(pb_5y_avg, 1) if pb_5y_avg else None,
        "vs_peers": classify_metric(pb_data, pb_peer_median),
        "vs_5y": classify_metric(pb_data, pb_5y_avg),
        "benchmark_used": {
            "peers": pb_peer_median is not None,
            "5y": pb_5y_avg is not None
        },
        "na_reason": "missing_data" if not pb_data else None
    }
    
    # 4. EV/EBITDA - ALWAYS include
    ev_ebitda_data = await compute_ev_ebitda_ratio(db, ticker, current_price, fundamentals)
    ev_ebitda_peer_median, ev_ebitda_peer_count, ev_ebitda_peer_type = await get_peer_median_for_metric(db, ticker, sector, industry, "ev_ebitda")
    ev_ebitda_5y_avg = await get_5y_average_for_metric(db, ticker, "ev_ebitda")
    
    metrics["ev_ebitda"] = {
        "name": "EV/EBITDA",
        "current": round(ev_ebitda_data, 1) if ev_ebitda_data else None,
        "peer_median": round(ev_ebitda_peer_median, 1) if ev_ebitda_peer_median else None,
        "peer_count": ev_ebitda_peer_count,
        "peer_type": ev_ebitda_peer_type,
        "avg_5y": round(ev_ebitda_5y_avg, 1) if ev_ebitda_5y_avg else None,
        "vs_peers": classify_metric(ev_ebitda_data, ev_ebitda_peer_median),
        "vs_5y": classify_metric(ev_ebitda_data, ev_ebitda_5y_avg),
        "benchmark_used": {
            "peers": ev_ebitda_peer_median is not None,
            "5y": ev_ebitda_5y_avg is not None
        },
        "na_reason": "unprofitable" if not ev_ebitda_data else None
    }
    
    # 5. EV/Revenue - ALWAYS include
    ev_revenue_data = await compute_ev_revenue_ratio(db, ticker, current_price, fundamentals)
    ev_revenue_peer_median, ev_revenue_peer_count, ev_revenue_peer_type = await get_peer_median_for_metric(db, ticker, sector, industry, "ev_revenue")
    ev_revenue_5y_avg = await get_5y_average_for_metric(db, ticker, "ev_revenue")
    
    metrics["ev_revenue"] = {
        "name": "EV/Revenue",
        "current": round(ev_revenue_data, 1) if ev_revenue_data else None,
        "peer_median": round(ev_revenue_peer_median, 1) if ev_revenue_peer_median else None,
        "peer_count": ev_revenue_peer_count,
        "peer_type": ev_revenue_peer_type,
        "avg_5y": round(ev_revenue_5y_avg, 1) if ev_revenue_5y_avg else None,
        "vs_peers": classify_metric(ev_revenue_data, ev_revenue_peer_median),
        "vs_5y": classify_metric(ev_revenue_data, ev_revenue_5y_avg),
        "benchmark_used": {
            "peers": ev_revenue_peer_median is not None,
            "5y": ev_revenue_5y_avg is not None
        },
        "na_reason": "missing_data" if not ev_revenue_data else None
    }
    
    # Count available metrics (those with current value)
    metrics_with_values = sum(1 for m in metrics.values() if m.get('current') is not None)
    metrics_used = metrics_with_values
    
    if metrics_used == 0:
        return {
            "available": False,
            "reason": "Unable to compute any valuation metrics from available data",
            "disclaimer": "Context only, not advice.",
            "thresholds": thresholds
        }
    
    # Compute overall badges using majority rule
    def compute_overall(comparison_key):
        classifications = [m.get(comparison_key) for m in metrics.values() if m.get(comparison_key)]
        if not classifications:
            return None
        
        total = len(classifications)
        cheaper_count = sum(1 for c in classifications if c == "cheaper")
        expensive_count = sum(1 for c in classifications if c == "more_expensive")
        
        if expensive_count / total >= thresholds["majority"]:
            return "more_expensive"
        elif cheaper_count / total >= thresholds["majority"]:
            return "cheaper"
        else:
            return "around"
    
    overall_vs_peers = compute_overall("vs_peers")
    overall_vs_5y = compute_overall("vs_5y")
    
    # Get peer info
    peer_count = 0
    peer_type = "industry"
    if "pe" in metrics and metrics["pe"].get("peer_median"):
        _, peer_count, peer_type = await get_peer_median(db, ticker, "pe_ratio")
    
    return {
        "available": True,
        "overall_vs_peers": overall_vs_peers,
        "overall_vs_5y_avg": overall_vs_5y,
        "metrics_used": metrics_used,
        "metrics": metrics,
        "peer_count": peer_count,
        "peer_type": peer_type,
        # P2: vs 5Y average summary for UI display
        "history_5y": {
            "available": overall_vs_5y is not None,
            "overall": overall_vs_5y,
            "metrics_with_history": sum(1 for m in metrics.values() if m.get("avg_5y") is not None),
            "na_reason": "insufficient_history" if overall_vs_5y is None else None,
        },
        "disclaimer": "Context only, not advice.",
        "thresholds": thresholds
    }


# =============================================================================
# HELPER FUNCTIONS FOR VALUATION METRICS (LOCAL COMPUTE FROM RAW DATA)
# =============================================================================

async def compute_ps_ratio(db, ticker: str, current_price: float, fundamentals: dict) -> Optional[float]:
    """
    P/S = Market Cap / Revenue TTM
    Revenue TTM = Sum of last 4 quarters revenue from income statement
    """
    ticker_full = ticker if ticker.endswith(".US") else f"{ticker}.US"
    
    # Get shares outstanding
    shares_out = await get_shares_outstanding(db, ticker, fundamentals)
    if not shares_out or shares_out <= 0:
        return None
    
    market_cap = current_price * shares_out
    
    # Get Revenue TTM from income statement
    income = fundamentals.get("income_statement_quarterly", {})
    if not income:
        return None
    
    sorted_quarters = sorted(income.keys(), reverse=True)[:4]
    revenue_ttm = 0
    
    for q in sorted_quarters:
        rev = income[q].get("totalRevenue") or income[q].get("revenue")
        if rev:
            try:
                revenue_ttm += float(rev)
            except (ValueError, TypeError):
                pass
    
    if revenue_ttm <= 0:
        return None
    
    return market_cap / revenue_ttm


async def compute_pb_ratio(db, ticker: str, current_price: float, fundamentals: dict) -> Optional[float]:
    """
    P/B = Market Cap / Book Value
    Book Value = Total Equity from most recent balance sheet
    """
    ticker_full = ticker if ticker.endswith(".US") else f"{ticker}.US"
    
    # Get shares outstanding
    shares_out = await get_shares_outstanding(db, ticker, fundamentals)
    if not shares_out or shares_out <= 0:
        return None
    
    market_cap = current_price * shares_out
    
    # Get Book Value from balance sheet
    balance = fundamentals.get("balance_sheet_quarterly", {})
    if not balance:
        return None
    
    sorted_quarters = sorted(balance.keys(), reverse=True)
    book_value = None
    
    for q in sorted_quarters:
        bv = balance[q].get("totalStockholderEquity") or balance[q].get("totalEquity")
        if bv:
            try:
                book_value = float(bv)
                break
            except (ValueError, TypeError):
                pass
    
    if not book_value or book_value <= 0:
        return None
    
    return market_cap / book_value


async def compute_ev_ebitda_ratio(db, ticker: str, current_price: float, fundamentals: dict) -> Optional[float]:
    """
    EV/EBITDA = (Market Cap + Total Debt - Cash) / EBITDA TTM
    """
    ticker_full = ticker if ticker.endswith(".US") else f"{ticker}.US"
    
    # Get shares outstanding
    shares_out = await get_shares_outstanding(db, ticker, fundamentals)
    if not shares_out or shares_out <= 0:
        return None
    
    market_cap = current_price * shares_out
    
    # Get debt and cash from balance sheet
    balance = fundamentals.get("balance_sheet_quarterly", {})
    if not balance:
        return None
    
    sorted_quarters = sorted(balance.keys(), reverse=True)
    total_debt = 0
    cash = 0
    
    for q in sorted_quarters:
        debt = balance[q].get("totalDebt") or balance[q].get("longTermDebt", 0)
        cash_val = balance[q].get("cashAndCashEquivalents") or balance[q].get("cash", 0)
        try:
            total_debt = float(debt) if debt else 0
            cash = float(cash_val) if cash_val else 0
            break
        except (ValueError, TypeError):
            pass
    
    ev = market_cap + total_debt - cash
    
    # Get EBITDA TTM from income statement
    income = fundamentals.get("income_statement_quarterly", {})
    if not income:
        return None
    
    sorted_quarters = sorted(income.keys(), reverse=True)[:4]
    ebitda_ttm = 0
    
    for q in sorted_quarters:
        ebitda = income[q].get("ebitda")
        if ebitda:
            try:
                ebitda_ttm += float(ebitda)
            except (ValueError, TypeError):
                pass
    
    if ebitda_ttm <= 0:
        return None
    
    return ev / ebitda_ttm


async def compute_ev_revenue_ratio(db, ticker: str, current_price: float, fundamentals: dict) -> Optional[float]:
    """
    EV/Revenue = (Market Cap + Total Debt - Cash) / Revenue TTM
    """
    ticker_full = ticker if ticker.endswith(".US") else f"{ticker}.US"
    
    # Get shares outstanding
    shares_out = await get_shares_outstanding(db, ticker, fundamentals)
    if not shares_out or shares_out <= 0:
        return None
    
    market_cap = current_price * shares_out
    
    # Get debt and cash from balance sheet
    balance = fundamentals.get("balance_sheet_quarterly", {})
    total_debt = 0
    cash = 0
    
    if balance:
        sorted_quarters = sorted(balance.keys(), reverse=True)
        for q in sorted_quarters:
            debt = balance[q].get("totalDebt") or balance[q].get("longTermDebt", 0)
            cash_val = balance[q].get("cashAndCashEquivalents") or balance[q].get("cash", 0)
            try:
                total_debt = float(debt) if debt else 0
                cash = float(cash_val) if cash_val else 0
                break
            except (ValueError, TypeError):
                pass
    
    ev = market_cap + total_debt - cash
    
    # Get Revenue TTM from income statement
    income = fundamentals.get("income_statement_quarterly", {})
    if not income:
        return None
    
    sorted_quarters = sorted(income.keys(), reverse=True)[:4]
    revenue_ttm = 0
    
    for q in sorted_quarters:
        rev = income[q].get("totalRevenue") or income[q].get("revenue")
        if rev:
            try:
                revenue_ttm += float(rev)
            except (ValueError, TypeError):
                pass
    
    if revenue_ttm <= 0:
        return None
    
    return ev / revenue_ttm


async def get_shares_outstanding(db, ticker: str, fundamentals: dict) -> Optional[float]:
    """Get shares outstanding from fundamentals."""
    # First try direct shares_outstanding field (number)
    shares = fundamentals.get("shares_outstanding")
    
    if shares is not None:
        try:
            return float(shares)
        except (ValueError, TypeError):
            pass
    
    # Try shares_outstanding_quarterly (dict format)
    shares = fundamentals.get("shares_outstanding_quarterly", {})
    
    if isinstance(shares, dict) and shares:
        latest_key = sorted(shares.keys(), reverse=True)[0]
        shares_val = shares[latest_key].get("commonSharesOutstanding") or shares[latest_key].get("shares")
        try:
            return float(shares_val) if shares_val else None
        except (ValueError, TypeError):
            return None
    elif isinstance(shares, list) and len(shares) > 0:
        shares_val = shares[0].get("shares") if isinstance(shares[0], dict) else shares[0]
        try:
            return float(shares_val) if shares_val else None
        except (ValueError, TypeError):
            return None
    
    return None


async def get_peer_median_for_metric(db, ticker: str, sector: str, industry: str, metric: str) -> tuple:
    """
    Get peer median for a specific metric with EXCLUDE-SELF logic.
    
    NEW ALGORITHM (P0 Fix for mega-cap self-reference):
    1. Get industry peer_benchmarks document with metric_values lists
    2. Exclude self ticker from the list
    3. Winsorize remaining values (1-99 percentile)
    4. Compute simple median (not weighted)
    5. If < 3 peers remain, fallback to sector
    
    Returns: (median_value, peer_count, group_type or reason)
    """
    import statistics
    
    MIN_PEER_COUNT = 3  # Minimum peers after exclude-self
    
    # Map metric names to cache field names
    metric_map = {
        "pe_ratio": "pe",
        "ps_ratio": "ps",
        "pb_ratio": "pb",
        "ev_ebitda": "ev_ebitda",
        "ev_revenue": "ev_revenue"
    }
    
    cache_field = metric_map.get(metric, metric)
    ticker_full = ticker if ticker.endswith(".US") else f"{ticker}.US"
    
    def compute_simple_median_exclude_self(tickers: list, values: list, exclude_ticker: str) -> tuple:
        """
        Compute simple median with exclude-self and winsorization.
        
        Order of operations:
        1. Exclude self ticker
        2. Winsorize (1-99 percentile)
        3. Simple median
        """
        if not tickers or not values or len(tickers) != len(values):
            return None, 0
        
        # STEP 1: Exclude self (maintain sorted order)
        filtered = [(t, v) for t, v in zip(tickers, values) if t != exclude_ticker]
        
        if len(filtered) < MIN_PEER_COUNT:
            return None, len(filtered)
        
        # Values remain sorted after exclusion
        filtered_values = [v for _, v in filtered]
        peer_count = len(filtered_values)
        
        # STEP 2: Winsorize (1-99 percentile)
        low_idx = max(0, int(peer_count * 1 / 100))
        high_idx = min(peer_count - 1, int(peer_count * 99 / 100))
        low_bound = filtered_values[low_idx]
        high_bound = filtered_values[high_idx]
        
        winsorized = [max(low_bound, min(high_bound, v)) for v in filtered_values]
        winsorized.sort()  # Re-sort after winsorization
        
        # STEP 3: Simple median (index-based)
        n = len(winsorized)
        if n % 2 == 1:
            median_val = winsorized[n // 2]
        else:
            median_val = (winsorized[n // 2 - 1] + winsorized[n // 2]) / 2
        
        return median_val, peer_count
    
    # Try industry first
    if industry:
        doc = await db.peer_benchmarks.find_one(
            {"industry": industry},
            {"_id": 0, "metric_values": 1, "peer_count_used": 1, "peer_count": 1}
        )
        if doc:
            metric_data = doc.get("metric_values", {}).get(cache_field, {})
            tickers_list = metric_data.get("tickers", [])
            values_list = metric_data.get("values", [])
            
            if tickers_list and values_list:
                median_val, peer_count = compute_simple_median_exclude_self(
                    tickers_list, values_list, ticker_full
                )
                if median_val is not None:
                    return (median_val, peer_count, "industry")
    
    # Fallback to sector
    if sector:
        doc = await db.peer_benchmarks.find_one(
            {"sector": sector, "industry": None},
            {"_id": 0, "metric_values": 1, "peer_count": 1}
        )
        if doc:
            metric_data = doc.get("metric_values", {}).get(cache_field, {})
            tickers_list = metric_data.get("tickers", [])
            values_list = metric_data.get("values", [])
            
            if tickers_list and values_list:
                median_val, peer_count = compute_simple_median_exclude_self(
                    tickers_list, values_list, ticker_full
                )
                if median_val is not None:
                    return (median_val, peer_count, "sector")
    
    # No data available
    return (None, 0, "no_data")


async def get_5y_average_for_metric(db, ticker: str, metric: str) -> Optional[float]:
    """
    Get 5-year arithmetic mean for a specific valuation metric.
    
    P2 REQUIREMENT: Read from ticker_key_metrics_daily collection.
    
    Args:
        ticker: e.g. "XXII.US"
        metric: "ps_ratio", "pb_ratio", "ev_ebitda", "ev_revenue"
    
    Returns:
        5Y average value or None if insufficient data
    """
    ticker_full = ticker if ticker.endswith(".US") else f"{ticker}.US"
    
    # Map metric name to DB field
    field_mapping = {
        "ps_ratio": "ps_ttm",
        "pb_ratio": "pb",
        "ev_ebitda": "ev_ebitda_ttm",
        "ev_revenue": "ev_revenue_ttm",
        "pe_ratio": "pe_ttm",
    }
    
    db_field = field_mapping.get(metric, metric)
    
    # Compute average from ticker_key_metrics_daily (last 60 months)
    pipeline = [
        {"$match": {
            "ticker": ticker_full,
            db_field: {"$type": "number", "$gt": 0}
        }},
        {"$group": {
            "_id": None,
            "avg": {"$avg": f"${db_field}"},
            "count": {"$sum": 1}
        }}
    ]
    
    result = await db.ticker_key_metrics_daily.aggregate(pipeline).to_list(1)
    
    # Need at least 12 data points (1 year) for meaningful average
    if result and result[0]["count"] >= 12:
        return result[0]["avg"]
    
    return None


# =============================================================================
# HYBRID 7 KEY METRICS
# =============================================================================
# Master Prompt requires these 7 metrics (in order):
# 1. Market Cap
# 2. Shares Outstanding
# 3. Net Margin (TTM)
# 4. Free Cash Flow Yield
# 5. Net Debt / EBITDA
# 6. Revenue Growth (3Y CAGR)
# 7. Dividend Yield (TTM)
# =============================================================================

async def calculate_hybrid_7_metrics(db, ticker: str, current_price: float) -> Dict[str, Any]:
    """
    Calculate the Hybrid 7 Key Metrics for ticker detail page.
    
    Returns dict with 7 metrics, each containing:
    - value: numeric value or None
    - formatted: formatted string for display
    - na_reason: reason code if N/A (unprofitable, missing_data, etc.)
    """
    symbol = ticker.replace(".US", "").upper()
    
    # Get fundamentals
    fund = await db.company_fundamentals_cache.find_one(
        {"symbol": symbol},
        {"_id": 0}
    )
    
    if not fund:
        return _empty_hybrid_7("missing_data")
    
    # Extract shares outstanding
    shares = _extract_shares(fund)
    
    # Market Cap
    market_cap = None
    market_cap_formatted = None
    if current_price and shares:
        market_cap = current_price * shares
        market_cap_formatted = _format_large_number(market_cap)
    
    # Get latest income statement (TTM from quarterly)
    income_q = fund.get("income_statement_quarterly", {})
    sorted_quarters = sorted(income_q.keys(), reverse=True)[:4] if income_q else []
    
    # Net Margin TTM
    net_margin = None
    net_margin_reason = None
    if len(sorted_quarters) >= 4:
        total_revenue = sum(_safe_float(income_q[q].get("totalRevenue")) for q in sorted_quarters)
        total_net_income = sum(_safe_float(income_q[q].get("netIncome")) for q in sorted_quarters)
        
        if total_revenue and total_revenue > 0:
            net_margin = (total_net_income / total_revenue) * 100
            if net_margin < 0:
                net_margin_reason = "unprofitable"
        else:
            net_margin_reason = "missing_revenue"
    else:
        net_margin_reason = "insufficient_history"
    
    # Free Cash Flow Yield
    fcf_yield = None
    fcf_yield_reason = None
    cash_flow_q = fund.get("cash_flow_quarterly", {})
    cf_quarters = sorted(cash_flow_q.keys(), reverse=True)[:4] if cash_flow_q else []
    
    if len(cf_quarters) >= 4 and market_cap:
        total_fcf = 0
        for q in cf_quarters:
            operating_cf = _safe_float(cash_flow_q[q].get("operatingCashFlow") or cash_flow_q[q].get("totalCashFromOperatingActivities"))
            capex = _safe_float(cash_flow_q[q].get("capitalExpenditures"))
            if operating_cf is not None:
                total_fcf += operating_cf - abs(capex) if capex else operating_cf
        
        if market_cap > 0:
            fcf_yield = (total_fcf / market_cap) * 100
            if fcf_yield < 0:
                fcf_yield_reason = "negative_fcf"
    else:
        fcf_yield_reason = "insufficient_history"
    
    # Net Debt / EBITDA
    net_debt_ebitda = None
    net_debt_ebitda_reason = None
    
    balance = fund.get("balance_sheet_quarterly", {})
    balance_quarters = sorted(balance.keys(), reverse=True)[:1] if balance else []
    
    if balance_quarters:
        latest_balance = balance[balance_quarters[0]]
        total_debt = _safe_float(latest_balance.get("shortTermDebt", 0)) + _safe_float(latest_balance.get("longTermDebt", 0))
        cash = _safe_float(latest_balance.get("cash", 0)) + _safe_float(latest_balance.get("shortTermInvestments", 0))
        net_debt = total_debt - cash
        
        # Calculate EBITDA TTM
        ebitda_ttm = 0
        if len(sorted_quarters) >= 4:
            for q in sorted_quarters:
                ebit = _safe_float(income_q[q].get("operatingIncome") or income_q[q].get("ebit"))
                da = _safe_float(income_q[q].get("depreciationAndAmortization", 0))
                ebitda_ttm += (ebit or 0) + da
        
        if ebitda_ttm > 0:
            net_debt_ebitda = net_debt / ebitda_ttm
        elif ebitda_ttm < 0:
            net_debt_ebitda_reason = "unprofitable"
        else:
            net_debt_ebitda_reason = "missing_data"
    else:
        net_debt_ebitda_reason = "missing_data"
    
    # Revenue Growth 3Y CAGR - COMPUTED FROM RAW FINANCIALS_CACHE
    revenue_growth_3y = None
    revenue_growth_reason = None
    
    # Get annual income statements from financials_cache
    ticker_full = f"{symbol}.US"
    annual_income = await db.financials_cache.find(
        {"ticker": ticker_full, "period_type": "annual", "statement_type": "Income_Statement"},
        {"_id": 0, "date": 1, "data": 1}
    ).sort("date", -1).to_list(length=4)
    
    if len(annual_income) >= 4:
        revenue_current = _safe_float(annual_income[0]["data"].get("totalRevenue"))
        revenue_3y_ago = _safe_float(annual_income[3]["data"].get("totalRevenue"))
        
        if revenue_current and revenue_3y_ago and revenue_3y_ago > 0:
            revenue_growth_3y = ((revenue_current / revenue_3y_ago) ** (1/3) - 1) * 100
        else:
            revenue_growth_reason = "missing_revenue"
    else:
        revenue_growth_reason = "insufficient_history"
    
    # Dividend Yield TTM - COMPUTED FROM RAW DIVIDEND DATA
    # Formula: (Sum of dividends in last 365 days) / current_price * 100
    dividend_yield = None
    dividend_yield_reason = None
    
    if current_price and current_price > 0:
        ticker_full = f"{symbol}.US"
        one_year_ago = (datetime.now(timezone.utc) - timedelta(days=365)).strftime("%Y-%m-%d")
        
        # Get dividend payments from last 365 days
        dividend_cursor = db.dividend_history.find(
            {"ticker": ticker_full, "date": {"$gte": one_year_ago}},
            {"_id": 0, "value": 1}
        )
        
        total_dividends = 0
        async for div in dividend_cursor:
            div_val = _safe_float(div.get("value"))
            if div_val:
                total_dividends += div_val
        
        if total_dividends > 0:
            dividend_yield = (total_dividends / current_price) * 100
        else:
            dividend_yield = 0  # No dividends = 0% yield
    else:
        dividend_yield_reason = "missing_data"
    
    return {
        "market_cap": {
            "name": "Market Cap",
            "value": market_cap,
            "formatted": market_cap_formatted,
            "na_reason": "missing_data" if not market_cap else None
        },
        "shares_outstanding": {
            "name": "Shares Outstanding",
            "value": shares,
            "formatted": _format_shares(shares),
            "na_reason": "missing_shares" if not shares else None
        },
        "net_margin_ttm": {
            "name": "Net Margin (TTM)",
            "value": round(net_margin, 2) if net_margin is not None else None,
            "formatted": f"{net_margin:.1f}%" if net_margin is not None else None,
            "na_reason": net_margin_reason
        },
        "fcf_yield": {
            "name": "Free Cash Flow Yield",
            "value": round(fcf_yield, 2) if fcf_yield is not None else None,
            "formatted": f"{fcf_yield:.1f}%" if fcf_yield is not None else None,
            "na_reason": fcf_yield_reason
        },
        "net_debt_ebitda": {
            "name": "Net Debt / EBITDA",
            "value": round(net_debt_ebitda, 2) if net_debt_ebitda is not None else None,
            "formatted": f"{net_debt_ebitda:.1f}x" if net_debt_ebitda is not None else None,
            "na_reason": net_debt_ebitda_reason
        },
        "revenue_growth_3y": {
            "name": "Revenue Growth (3Y CAGR)",
            "value": round(revenue_growth_3y, 2) if revenue_growth_3y is not None else None,
            "formatted": f"{revenue_growth_3y:+.1f}%" if revenue_growth_3y is not None else None,
            "na_reason": revenue_growth_reason
        },
        "dividend_yield_ttm": {
            "name": "Dividend Yield (TTM)",
            "value": round(dividend_yield, 2) if dividend_yield is not None else None,
            "formatted": f"{dividend_yield:.2f}%" if dividend_yield is not None else None,
            "na_reason": dividend_yield_reason
        }
    }


def _empty_hybrid_7(reason: str) -> Dict[str, Any]:
    """Return empty Hybrid 7 structure with given reason."""
    metrics = [
        "market_cap", "shares_outstanding", "net_margin_ttm",
        "fcf_yield", "net_debt_ebitda", "revenue_growth_3y", "dividend_yield_ttm"
    ]
    names = [
        "Market Cap", "Shares Outstanding", "Net Margin (TTM)",
        "Free Cash Flow Yield", "Net Debt / EBITDA", "Revenue Growth (3Y CAGR)", "Dividend Yield (TTM)"
    ]
    return {
        m: {"name": n, "value": None, "formatted": None, "na_reason": reason}
        for m, n in zip(metrics, names)
    }


def _extract_shares(fund: dict) -> Optional[float]:
    """Extract shares outstanding from fundamentals."""
    # Direct field
    shares = fund.get("shares_outstanding")
    if shares:
        if isinstance(shares, (int, float)):
            return float(shares)
        if isinstance(shares, dict):
            return _safe_float(shares.get("shares") or shares.get("value"))
    
    # From quarterly
    shares_q = fund.get("shares_outstanding_quarterly", {})
    if shares_q:
        latest_date = ""
        latest_val = None
        for key, entry in shares_q.items():
            if isinstance(entry, dict):
                date_str = entry.get("dateFormatted", "")
                if date_str > latest_date:
                    latest_date = date_str
                    latest_val = _safe_float(entry.get("shares"))
        if latest_val:
            return latest_val
    
    # From annual
    shares_a = fund.get("shares_outstanding_annual", {})
    if shares_a:
        latest_date = ""
        latest_val = None
        for key, entry in shares_a.items():
            if isinstance(entry, dict):
                date_str = entry.get("dateFormatted", "")
                if date_str > latest_date:
                    latest_date = date_str
                    latest_val = _safe_float(entry.get("shares"))
        if latest_val:
            return latest_val
    
    return None


def _safe_float(val, default: float = 0) -> float:
    """Safely convert value to float, returns default (0) if not possible."""
    if val is None:
        return default
    try:
        result = float(val)
        return result if result is not None else default
    except (ValueError, TypeError):
        return default


def _format_large_number(val: Optional[float], prefix: str = "$") -> Optional[str]:
    """Format large number with T/B/M suffix."""
    if val is None:
        return None
    if val >= 1e12:
        return f"{prefix}{val/1e12:.2f}T"
    if val >= 1e9:
        return f"{prefix}{val/1e9:.1f}B"
    if val >= 1e6:
        return f"{prefix}{val/1e6:.0f}M"
    return f"{prefix}{val:,.0f}"


def _format_shares(val: Optional[float]) -> Optional[str]:
    """Format shares outstanding (no dollar sign)."""
    if val is None:
        return None
    if val >= 1e9:
        return f"{val/1e9:.1f}B"
    if val >= 1e6:
        return f"{val/1e6:.0f}M"
    if val >= 1e3:
        return f"{val/1e3:.0f}K"
    return f"{val:,.0f}"


async def get_industry_peer_count(db, ticker: str) -> Dict[str, int]:
    """
    Get total industry peers count for valuation transparency.
    
    Returns:
        {
            "total_industry_peers": count of all visible tickers in same industry,
            "industry": industry name
        }
    """
    symbol = ticker.replace(".US", "").upper()
    
    tracked = await db.tracked_tickers.find_one(
        {"ticker": f"{symbol}.US"},
        {"_id": 0, "industry": 1}
    )
    
    if not tracked or not tracked.get("industry"):
        return {"total_industry_peers": 0, "industry": None}
    
    industry = tracked["industry"]
    
    count = await db.tracked_tickers.count_documents({
        "is_visible": True,
        "industry": industry
    })
    
    return {
        "total_industry_peers": count,
        "industry": industry
    }


async def get_peer_transparency(db, ticker: str) -> Dict[str, Any]:
    """
    Get peer transparency data for valuation metrics.
    
    Returns:
        {
            "total_industry_peers": All visible tickers in same industry,
            "valid_metric_peers": Dict of metric -> count with valid data,
            "industry": industry name,
            "group_type": "industry" | "sector" | "market"
        }
    
    P0 Requirement: UI shows "vs 12 industry peers / 6 with valid data"
    """
    from datetime import timezone
    
    symbol = ticker.replace(".US", "").upper()
    ticker_full = f"{symbol}.US"
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    
    # Get ticker's industry and sector
    tracked = await db.tracked_tickers.find_one(
        {"ticker": ticker_full},
        {"_id": 0, "industry": 1, "sector": 1}
    )
    
    if not tracked:
        return {
            "total_industry_peers": 0,
            "valid_metric_peers": {},
            "industry": None,
            "group_type": None
        }
    
    industry = tracked.get("industry")
    sector = tracked.get("sector")
    
    # Count total peers in industry
    total_industry_peers = 0
    if industry:
        total_industry_peers = await db.tracked_tickers.count_documents({
            "is_visible": True,
            "industry": industry
        })
    
    # Get valid metric counts from peer_medians_daily
    metrics = ["pe_ttm", "ps_ttm", "pb", "ev_ebitda_ttm", "ev_revenue_ttm"]
    valid_metric_peers = {}
    group_type = None
    
    for metric in metrics:
        # Try industry first
        if industry:
            doc = await db.peer_medians_daily.find_one(
                {"date": today, "group_type": "industry", "group_name": industry, "metric": metric},
                {"_id": 0, "peer_count": 1}
            )
            if doc and doc.get("peer_count", 0) >= 5:
                valid_metric_peers[metric] = doc["peer_count"]
                group_type = "industry"
                continue
        
        # Fallback to sector
        if sector:
            doc = await db.peer_medians_daily.find_one(
                {"date": today, "group_type": "sector", "group_name": sector, "metric": metric},
                {"_id": 0, "peer_count": 1}
            )
            if doc and doc.get("peer_count", 0) >= 5:
                valid_metric_peers[metric] = doc["peer_count"]
                if not group_type:
                    group_type = "sector"
                continue
        
        # Fallback to market
        doc = await db.peer_medians_daily.find_one(
            {"date": today, "group_type": "market", "group_name": "US", "metric": metric},
            {"_id": 0, "peer_count": 1}
        )
        if doc:
            valid_metric_peers[metric] = doc["peer_count"]
            if not group_type:
                group_type = "market"
    
    return {
        "total_industry_peers": total_industry_peers,
        "valid_metric_peers": valid_metric_peers,
        "industry": industry,
        "group_type": group_type or "industry"
    }



# =============================================================================
# VALUATION OVERVIEW V2 - READS FROM EMBEDDED FUNDAMENTALS
# =============================================================================
# BINDING: This version reads from tracked_tickers.fundamentals (embedded)
# NOT from company_fundamentals_cache (deprecated/empty)
# =============================================================================

async def get_valuation_overview_v2(db, ticker: str, current_price: float) -> Dict[str, Any]:
    """
    Compute OVERALL valuation badge using 5 locally computed multiples.
    BINDING: Reads from embedded tracked_tickers.fundamentals.
    
    Multiples:
    - P/E = price / EPS_TTM
    - P/S = market_cap / revenue_TTM
    - P/B = market_cap / book_value
    - EV/EBITDA = (market_cap + debt - cash) / EBITDA_TTM
    - EV/Revenue = (market_cap + debt - cash) / revenue_TTM
    
    Dual-Pillar Benchmarks:
    - Pillar A (vs Peers): industry median (fallback sector if peers < 5)
    - Pillar B (vs 5Y Average): company's own 5-year quarterly average
    
    Classification per metric:
    - Cheaper: metric < 0.85 × benchmark
    - Around: 0.85-1.15 × benchmark  
    - More expensive: metric > 1.15 × benchmark
    
    Overall badge (majority rule per pillar):
    - If ≥60% (3 out of 5) are "more_expensive" → Overall = more_expensive
    - If ≥60% (3 out of 5) are "cheaper" → Overall = cheaper
    - Else → around
    """
    ticker_full = ticker if ticker.endswith(".US") else f"{ticker}.US"
    symbol = ticker.replace(".US", "").upper()
    
    # Thresholds for transparency
    thresholds = {
        "cheaper": 0.85,
        "expensive": 1.15,
        "majority": 0.6  # 3 out of 5 = 60%
    }
    
    # BINDING: Get embedded fundamentals from tracked_tickers
    tracked = await db.tracked_tickers.find_one(
        {"ticker": ticker_full},
        {"_id": 0, "fundamentals": 1, "sector": 1, "industry": 1, "name": 1}
    )
    
    if not tracked or not tracked.get("fundamentals"):
        return {
            "available": False,
            "reason": "No fundamental data available",
            "disclaimer": "Context only, not advice.",
            "thresholds": thresholds
        }
    
    fundamentals = tracked.get("fundamentals", {})
    sector = tracked.get("sector")
    industry = tracked.get("industry")
    
    # Extract data from embedded fundamentals
    general = fundamentals.get("General", {})
    financials = fundamentals.get("Financials", {})
    shares_stats = fundamentals.get("SharesStats", {})
    outstanding_shares = fundamentals.get("outstandingShares", {})
    earnings = fundamentals.get("Earnings", {})
    
    # Get shares outstanding
    shares_outstanding = None
    if shares_stats:
        shares_outstanding = shares_stats.get("SharesOutstanding")
    if not shares_outstanding and outstanding_shares:
        # Try to get from outstandingShares.annual or quarterly
        annual = outstanding_shares.get("annual", {})
        if annual:
            latest_key = sorted(annual.keys(), reverse=True)[0] if annual else None
            if latest_key:
                shares_outstanding = annual[latest_key].get("shares")
    
    # Convert to float
    if shares_outstanding:
        try:
            shares_outstanding = float(shares_outstanding)
        except:
            shares_outstanding = None
    
    # Compute market cap
    market_cap = None
    if shares_outstanding and current_price:
        market_cap = shares_outstanding * current_price
    
    # Get Income Statement for TTM calculations
    income_stmt = financials.get("Income_Statement", {})
    quarterly_income = income_stmt.get("quarterly", {})
    
    # Get Balance Sheet for book value, debt, cash
    balance_sheet = financials.get("Balance_Sheet", {})
    quarterly_balance = balance_sheet.get("quarterly", {})
    
    # Get Cash Flow for additional data
    cash_flow = financials.get("Cash_Flow", {})
    quarterly_cashflow = cash_flow.get("quarterly", {})
    
    # Helper: Get TTM sum from quarterly data
    def get_ttm_sum(quarterly_data: dict, field: str) -> Optional[float]:
        if not quarterly_data:
            return None
        sorted_quarters = sorted(quarterly_data.keys(), reverse=True)[:4]
        if len(sorted_quarters) < 4:
            return None
        total = 0
        for q in sorted_quarters:
            val = quarterly_data[q].get(field)
            if val is not None:
                try:
                    total += float(val)
                except:
                    pass
        return total if total != 0 else None
    
    # Helper: Get latest value from quarterly data
    def get_latest_value(quarterly_data: dict, field: str) -> Optional[float]:
        if not quarterly_data:
            return None
        sorted_quarters = sorted(quarterly_data.keys(), reverse=True)
        for q in sorted_quarters:
            val = quarterly_data[q].get(field)
            if val is not None:
                try:
                    return float(val)
                except:
                    pass
        return None
    
    # Compute TTM values
    revenue_ttm = get_ttm_sum(quarterly_income, "totalRevenue")
    net_income_ttm = get_ttm_sum(quarterly_income, "netIncome")
    ebitda_ttm = get_ttm_sum(quarterly_income, "ebitda")
    
    # If EBITDA not available, compute from operating income + D&A
    if not ebitda_ttm:
        operating_income_ttm = get_ttm_sum(quarterly_income, "operatingIncome")
        depreciation_ttm = get_ttm_sum(quarterly_cashflow, "depreciation")
        if operating_income_ttm and depreciation_ttm:
            ebitda_ttm = operating_income_ttm + abs(depreciation_ttm)
    
    # Get balance sheet items (latest)
    total_equity = get_latest_value(quarterly_balance, "totalStockholderEquity")
    total_debt = get_latest_value(quarterly_balance, "totalDebt")
    if not total_debt:
        short_term = get_latest_value(quarterly_balance, "shortTermDebt") or 0
        long_term = get_latest_value(quarterly_balance, "longTermDebt") or 0
        total_debt = short_term + long_term if (short_term or long_term) else None
    
    cash = get_latest_value(quarterly_balance, "cash")
    if not cash:
        cash = get_latest_value(quarterly_balance, "cashAndShortTermInvestments")
    
    # Compute EPS TTM
    eps_ttm = None
    earnings_history = earnings.get("History", {})
    if earnings_history:
        sorted_quarters = sorted(earnings_history.keys(), reverse=True)[:4]
        eps_values = []
        for q in sorted_quarters:
            eps = earnings_history[q].get("epsActual")
            if eps is not None:
                try:
                    eps_values.append(float(eps))
                except:
                    pass
        if len(eps_values) >= 4:
            eps_ttm = sum(eps_values)
    
    # Fallback: compute EPS from net income / shares
    if not eps_ttm and net_income_ttm and shares_outstanding:
        eps_ttm = net_income_ttm / shares_outstanding
    
    # Compute Enterprise Value
    enterprise_value = None
    if market_cap is not None:
        debt_val = total_debt or 0
        cash_val = cash or 0
        enterprise_value = market_cap + debt_val - cash_val
    
    # Helper function to classify a metric
    def classify_metric(current_val, benchmark_val):
        if current_val is None or benchmark_val is None or benchmark_val <= 0:
            return None
        ratio = current_val / benchmark_val
        if ratio < thresholds["cheaper"]:
            return "cheaper"
        elif ratio > thresholds["expensive"]:
            return "more_expensive"
        else:
            return "around"
    
    # Compute all 5 metrics
    metrics = {}
    
    # 1. P/E Ratio
    pe_current = None
    if eps_ttm and eps_ttm > 0 and current_price:
        pe_current = current_price / eps_ttm
    
    pe_peer_median, pe_peer_count, pe_peer_type = await get_peer_median_for_metric(db, ticker, sector, industry, "pe_ratio")
    pe_5y_avg = await get_5y_average_pe(db, ticker)
    
    metrics["pe"] = {
        "name": "P/E",
        "current": round(pe_current, 1) if pe_current else None,
        "peer_median": round(pe_peer_median, 1) if pe_peer_median else None,
        "peer_count": pe_peer_count,
        "peer_type": pe_peer_type,
        "avg_5y": round(pe_5y_avg, 1) if pe_5y_avg else None,
        "vs_peers": classify_metric(pe_current, pe_peer_median),
        "vs_5y": classify_metric(pe_current, pe_5y_avg),
        "na_reason": "unprofitable" if not pe_current else None
    }
    
    # 2. P/S Ratio (Price to Sales)
    ps_current = None
    if market_cap and revenue_ttm and revenue_ttm > 0:
        ps_current = market_cap / revenue_ttm
    
    ps_peer_median, ps_peer_count, ps_peer_type = await get_peer_median_for_metric(db, ticker, sector, industry, "ps_ratio")
    ps_5y_avg = await get_5y_average_for_metric(db, ticker, "ps_ratio")
    
    metrics["ps"] = {
        "name": "P/S",
        "current": round(ps_current, 1) if ps_current else None,
        "peer_median": round(ps_peer_median, 1) if ps_peer_median else None,
        "peer_count": ps_peer_count,
        "peer_type": ps_peer_type,
        "avg_5y": round(ps_5y_avg, 1) if ps_5y_avg else None,
        "vs_peers": classify_metric(ps_current, ps_peer_median),
        "vs_5y": classify_metric(ps_current, ps_5y_avg),
        "na_reason": "missing_revenue" if not ps_current else None
    }
    
    # 3. P/B Ratio (Price to Book)
    pb_current = None
    if market_cap and total_equity and total_equity > 0:
        pb_current = market_cap / total_equity
    
    pb_peer_median, pb_peer_count, pb_peer_type = await get_peer_median_for_metric(db, ticker, sector, industry, "pb_ratio")
    pb_5y_avg = await get_5y_average_for_metric(db, ticker, "pb_ratio")
    
    metrics["pb"] = {
        "name": "P/B",
        "current": round(pb_current, 1) if pb_current else None,
        "peer_median": round(pb_peer_median, 1) if pb_peer_median else None,
        "peer_count": pb_peer_count,
        "peer_type": pb_peer_type,
        "avg_5y": round(pb_5y_avg, 1) if pb_5y_avg else None,
        "vs_peers": classify_metric(pb_current, pb_peer_median),
        "vs_5y": classify_metric(pb_current, pb_5y_avg),
        "na_reason": "negative_equity" if not pb_current else None
    }
    
    # 4. EV/EBITDA
    ev_ebitda_current = None
    if enterprise_value and ebitda_ttm and ebitda_ttm > 0:
        ev_ebitda_current = enterprise_value / ebitda_ttm
    
    ev_ebitda_peer_median, ev_ebitda_peer_count, ev_ebitda_peer_type = await get_peer_median_for_metric(db, ticker, sector, industry, "ev_ebitda")
    ev_ebitda_5y_avg = await get_5y_average_for_metric(db, ticker, "ev_ebitda")
    
    metrics["ev_ebitda"] = {
        "name": "EV/EBITDA",
        "current": round(ev_ebitda_current, 1) if ev_ebitda_current else None,
        "peer_median": round(ev_ebitda_peer_median, 1) if ev_ebitda_peer_median else None,
        "peer_count": ev_ebitda_peer_count,
        "peer_type": ev_ebitda_peer_type,
        "avg_5y": round(ev_ebitda_5y_avg, 1) if ev_ebitda_5y_avg else None,
        "vs_peers": classify_metric(ev_ebitda_current, ev_ebitda_peer_median),
        "vs_5y": classify_metric(ev_ebitda_current, ev_ebitda_5y_avg),
        "na_reason": "negative_ebitda" if not ev_ebitda_current else None
    }
    
    # 5. EV/Revenue
    ev_revenue_current = None
    if enterprise_value and revenue_ttm and revenue_ttm > 0:
        ev_revenue_current = enterprise_value / revenue_ttm
    
    ev_revenue_peer_median, ev_revenue_peer_count, ev_revenue_peer_type = await get_peer_median_for_metric(db, ticker, sector, industry, "ev_revenue")
    ev_revenue_5y_avg = await get_5y_average_for_metric(db, ticker, "ev_revenue")
    
    metrics["ev_revenue"] = {
        "name": "EV/Revenue",
        "current": round(ev_revenue_current, 1) if ev_revenue_current else None,
        "peer_median": round(ev_revenue_peer_median, 1) if ev_revenue_peer_median else None,
        "peer_count": ev_revenue_peer_count,
        "peer_type": ev_revenue_peer_type,
        "avg_5y": round(ev_revenue_5y_avg, 1) if ev_revenue_5y_avg else None,
        "vs_peers": classify_metric(ev_revenue_current, ev_revenue_peer_median),
        "vs_5y": classify_metric(ev_revenue_current, ev_revenue_5y_avg),
        "na_reason": "missing_revenue" if not ev_revenue_current else None
    }
    
    # Count available metrics (those with current value)
    metrics_with_values = sum(1 for m in metrics.values() if m.get('current') is not None)
    
    if metrics_with_values == 0:
        return {
            "available": False,
            "reason": "Unable to compute any valuation metrics from available data",
            "disclaimer": "Context only, not advice.",
            "thresholds": thresholds,
            "debug": {
                "shares_outstanding": shares_outstanding,
                "market_cap": market_cap,
                "revenue_ttm": revenue_ttm,
                "eps_ttm": eps_ttm,
                "total_equity": total_equity
            }
        }
    
    # Compute overall badges using majority rule (≥60% = 3 out of 5)
    def compute_overall(comparison_key):
        classifications = [m.get(comparison_key) for m in metrics.values() if m.get(comparison_key)]
        if not classifications:
            return None
        
        total = len(classifications)
        cheaper_count = sum(1 for c in classifications if c == "cheaper")
        expensive_count = sum(1 for c in classifications if c == "more_expensive")
        
        if expensive_count / total >= thresholds["majority"]:
            return "more_expensive"
        elif cheaper_count / total >= thresholds["majority"]:
            return "cheaper"
        else:
            return "around"
    
    overall_vs_peers = compute_overall("vs_peers")
    overall_vs_5y = compute_overall("vs_5y")
    
    # Get peer info summary
    peer_count = pe_peer_count or ps_peer_count or 0
    peer_type = pe_peer_type or ps_peer_type or "industry"
    
    return {
        "available": True,
        # Pillar A: vs Peers
        "overall_vs_peers": overall_vs_peers,
        "peer_count": peer_count,
        "peer_type": peer_type,
        # Pillar B: vs 5Y History
        "overall_vs_5y_avg": overall_vs_5y,
        "history_5y": {
            "available": overall_vs_5y is not None,
            "overall": overall_vs_5y,
            "metrics_with_history": sum(1 for m in metrics.values() if m.get("avg_5y") is not None),
            "na_reason": "insufficient_history" if overall_vs_5y is None else None,
        },
        # All 5 metrics detail (for expanded view)
        "metrics_used": metrics_with_values,
        "metrics": metrics,
        # Computed values (for transparency)
        "computed_values": {
            "market_cap": round(market_cap, 0) if market_cap else None,
            "enterprise_value": round(enterprise_value, 0) if enterprise_value else None,
            "revenue_ttm": round(revenue_ttm, 0) if revenue_ttm else None,
            "ebitda_ttm": round(ebitda_ttm, 0) if ebitda_ttm else None,
            "eps_ttm": round(eps_ttm, 2) if eps_ttm else None,
            "shares_outstanding": round(shares_outstanding, 0) if shares_outstanding else None,
        },
        "disclaimer": "Context only, not advice.",
        "thresholds": thresholds
    }



# =============================================================================
# HYBRID 7 KEY METRICS V2 - READS FROM EMBEDDED FUNDAMENTALS
# =============================================================================

def _safe_float(val):
    """Safely convert value to float."""
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None

def _format_large_number(num: float) -> str:
    """Format large numbers with B/M suffix."""
    if num is None:
        return None
    if num >= 1e12:
        return f"${num/1e12:.1f}T"
    elif num >= 1e9:
        return f"${num/1e9:.1f}B"
    elif num >= 1e6:
        return f"${num/1e6:.1f}M"
    else:
        return f"${num:,.0f}"

def _empty_hybrid_7_v2(reason: str) -> Dict[str, Any]:
    """Return empty hybrid 7 metrics dict with a reason."""
    metrics = {
        "market_cap": {"name": "Market Cap", "value": None, "formatted": None, "na_reason": reason},
        "shares_outstanding": {"name": "Shares Outstanding", "value": None, "formatted": None, "na_reason": reason},
        "net_margin_ttm": {"name": "Net Margin (TTM)", "value": None, "formatted": None, "na_reason": reason},
        "fcf_yield": {"name": "Free Cash Flow Yield", "value": None, "formatted": None, "na_reason": reason},
        "net_debt_ebitda": {"name": "Net Debt / EBITDA", "value": None, "formatted": None, "na_reason": reason},
        "revenue_growth_3y": {"name": "Revenue Growth (3Y CAGR)", "value": None, "formatted": None, "na_reason": reason},
        "dividend_yield_ttm": {"name": "Dividend Yield (TTM)", "value": None, "formatted": None, "na_reason": reason},
    }
    return metrics


async def calculate_hybrid_7_metrics_v2(db, ticker: str, current_price: float) -> Dict[str, Any]:
    """
    Calculate the Hybrid 7 Key Metrics for ticker detail page.
    BINDING: Reads from embedded tracked_tickers.fundamentals.
    
    Returns dict with 7 metrics, each containing:
    - value: numeric value or None
    - formatted: formatted string for display
    - na_reason: reason code if N/A (unprofitable, missing_data, etc.)
    """
    ticker_full = ticker if ticker.endswith(".US") else f"{ticker}.US"
    
    # BINDING: Get embedded fundamentals from tracked_tickers
    tracked = await db.tracked_tickers.find_one(
        {"ticker": ticker_full},
        {"_id": 0, "fundamentals": 1}
    )
    
    if not tracked or not tracked.get("fundamentals"):
        return _empty_hybrid_7_v2("missing_data")
    
    fundamentals = tracked.get("fundamentals", {})
    shares_stats = fundamentals.get("SharesStats", {})
    outstanding_shares = fundamentals.get("outstandingShares", {})
    financials = fundamentals.get("Financials", {})
    splits_dividends = fundamentals.get("SplitsDividends", {})
    
    # Get shares outstanding
    shares = None
    if shares_stats:
        shares = _safe_float(shares_stats.get("SharesOutstanding"))
    if not shares and outstanding_shares:
        annual = outstanding_shares.get("annual", {})
        if annual:
            latest_key = sorted(annual.keys(), reverse=True)[0] if annual else None
            if latest_key:
                shares = _safe_float(annual[latest_key].get("shares"))
    
    # Market Cap
    market_cap = None
    market_cap_formatted = None
    if current_price and shares:
        market_cap = current_price * shares
        market_cap_formatted = _format_large_number(market_cap)
    
    # Get financial statements
    income_stmt = financials.get("Income_Statement", {})
    quarterly_income = income_stmt.get("quarterly", {})
    yearly_income = income_stmt.get("yearly", {})
    
    balance_sheet = financials.get("Balance_Sheet", {})
    quarterly_balance = balance_sheet.get("quarterly", {})
    
    cash_flow = financials.get("Cash_Flow", {})
    quarterly_cashflow = cash_flow.get("quarterly", {})
    
    # Helper: Get sorted quarters (last N)
    def get_sorted_quarters(data: dict, n: int = 4):
        if not data:
            return []
        return sorted(data.keys(), reverse=True)[:n]
    
    # Net Margin TTM
    net_margin = None
    net_margin_reason = None
    sorted_quarters = get_sorted_quarters(quarterly_income, 4)
    
    if len(sorted_quarters) >= 4:
        total_revenue = sum(_safe_float(quarterly_income[q].get("totalRevenue")) or 0 for q in sorted_quarters)
        total_net_income = sum(_safe_float(quarterly_income[q].get("netIncome")) or 0 for q in sorted_quarters)
        
        if total_revenue and total_revenue > 0:
            net_margin = (total_net_income / total_revenue) * 100
            if net_margin < 0:
                net_margin_reason = "unprofitable"
        else:
            net_margin_reason = "missing_revenue"
    else:
        net_margin_reason = "insufficient_history"
    
    # Free Cash Flow Yield
    fcf_yield = None
    fcf_yield_reason = None
    cf_quarters = get_sorted_quarters(quarterly_cashflow, 4)
    
    if len(cf_quarters) >= 4 and market_cap:
        total_fcf = 0
        for q in cf_quarters:
            operating_cf = _safe_float(quarterly_cashflow[q].get("totalCashFromOperatingActivities"))
            if operating_cf is None:
                operating_cf = _safe_float(quarterly_cashflow[q].get("operatingCashFlow"))
            capex = _safe_float(quarterly_cashflow[q].get("capitalExpenditures")) or 0
            if operating_cf is not None:
                total_fcf += operating_cf - abs(capex)
        
        if market_cap > 0:
            fcf_yield = (total_fcf / market_cap) * 100
            if fcf_yield < 0:
                fcf_yield_reason = "negative_fcf"
    else:
        fcf_yield_reason = "insufficient_history"
    
    # Net Debt / EBITDA
    net_debt_ebitda = None
    net_debt_ebitda_reason = None
    
    balance_quarters = get_sorted_quarters(quarterly_balance, 1)
    income_quarters_4 = get_sorted_quarters(quarterly_income, 4)
    
    if balance_quarters and len(income_quarters_4) >= 4:
        latest_balance = quarterly_balance[balance_quarters[0]]
        
        # Get debt and cash
        total_debt = _safe_float(latest_balance.get("totalDebt"))
        if total_debt is None:
            short_term = _safe_float(latest_balance.get("shortTermDebt")) or 0
            long_term = _safe_float(latest_balance.get("longTermDebt")) or 0
            total_debt = short_term + long_term if (short_term or long_term) else None
        
        cash = _safe_float(latest_balance.get("cash"))
        if cash is None:
            cash = _safe_float(latest_balance.get("cashAndShortTermInvestments"))
        
        # Get EBITDA TTM
        ebitda_ttm = sum(_safe_float(quarterly_income[q].get("ebitda")) or 0 for q in income_quarters_4)
        if not ebitda_ttm:
            operating_income = sum(_safe_float(quarterly_income[q].get("operatingIncome")) or 0 for q in income_quarters_4)
            depreciation = sum(_safe_float(quarterly_cashflow[q].get("depreciation")) or 0 for q in cf_quarters[:4]) if cf_quarters else 0
            if operating_income:
                ebitda_ttm = operating_income + abs(depreciation)
        
        if total_debt is not None and cash is not None and ebitda_ttm and ebitda_ttm > 0:
            net_debt = total_debt - cash
            net_debt_ebitda = net_debt / ebitda_ttm
        elif ebitda_ttm and ebitda_ttm <= 0:
            net_debt_ebitda_reason = "negative_ebitda"
        else:
            net_debt_ebitda_reason = "missing_data"
    else:
        net_debt_ebitda_reason = "insufficient_history"
    
    # Revenue Growth (3Y CAGR)
    revenue_growth_3y = None
    revenue_growth_reason = None
    
    sorted_years = sorted(yearly_income.keys(), reverse=True) if yearly_income else []
    if len(sorted_years) >= 4:  # Need current + 3 years ago
        current_year = sorted_years[0]
        three_years_ago = sorted_years[3]
        
        current_revenue = _safe_float(yearly_income[current_year].get("totalRevenue"))
        past_revenue = _safe_float(yearly_income[three_years_ago].get("totalRevenue"))
        
        if current_revenue and past_revenue and past_revenue > 0:
            # CAGR = (end/start)^(1/years) - 1
            revenue_growth_3y = ((current_revenue / past_revenue) ** (1/3) - 1) * 100
        else:
            revenue_growth_reason = "missing_data"
    else:
        revenue_growth_reason = "insufficient_history"
    
    # Dividend Yield TTM
    dividend_yield = None
    dividend_yield_reason = None
    
    div_per_share = _safe_float(splits_dividends.get("DividendPerShareTTM"))
    fwd_annual_div = _safe_float(splits_dividends.get("ForwardAnnualDividendRate"))
    
    if current_price and current_price > 0:
        if fwd_annual_div and fwd_annual_div > 0:
            dividend_yield = (fwd_annual_div / current_price) * 100
        elif div_per_share and div_per_share > 0:
            dividend_yield = (div_per_share / current_price) * 100
        else:
            dividend_yield_reason = "no_dividend"
    else:
        dividend_yield_reason = "missing_price"
    
    return {
        "market_cap": {
            "name": "Market Cap",
            "value": market_cap,
            "formatted": market_cap_formatted,
            "na_reason": "missing_data" if not market_cap else None
        },
        "shares_outstanding": {
            "name": "Shares Outstanding",
            "value": shares,
            "formatted": f"{shares/1e9:.2f}B" if shares and shares >= 1e9 else (f"{shares/1e6:.0f}M" if shares else None),
            "na_reason": "missing_data" if not shares else None
        },
        "net_margin_ttm": {
            "name": "Net Margin (TTM)",
            "value": round(net_margin, 1) if net_margin is not None else None,
            "formatted": f"{net_margin:.1f}%" if net_margin is not None else None,
            "na_reason": net_margin_reason
        },
        "fcf_yield": {
            "name": "Free Cash Flow Yield",
            "value": round(fcf_yield, 1) if fcf_yield is not None else None,
            "formatted": f"{fcf_yield:.1f}%" if fcf_yield is not None else None,
            "na_reason": fcf_yield_reason
        },
        "net_debt_ebitda": {
            "name": "Net Debt / EBITDA",
            "value": round(net_debt_ebitda, 1) if net_debt_ebitda is not None else None,
            "formatted": f"{net_debt_ebitda:.1f}x" if net_debt_ebitda is not None else None,
            "na_reason": net_debt_ebitda_reason
        },
        "revenue_growth_3y": {
            "name": "Revenue Growth (3Y CAGR)",
            "value": round(revenue_growth_3y, 1) if revenue_growth_3y is not None else None,
            "formatted": f"{revenue_growth_3y:+.1f}%" if revenue_growth_3y is not None else None,
            "na_reason": revenue_growth_reason
        },
        "dividend_yield_ttm": {
            "name": "Dividend Yield (TTM)",
            "value": round(dividend_yield, 2) if dividend_yield is not None else None,
            "formatted": f"{dividend_yield:.2f}%" if dividend_yield is not None else None,
            "na_reason": dividend_yield_reason
        },
    }
