"""
Peer Median Math Debug Script
=============================
Verifies peer median calculations end-to-end for a given industry.

Replicates EXACTLY the production logic from compute_peer_benchmarks_v3():
1. Select visible tickers with same industry and USD-only filter
2. Compute raw metrics from fundamentals (P/E, P/S, P/B, EV/EBITDA, EV/Revenue)
3. Apply winsorization and weighted median (market_cap weighted)
4. Compare computed vs stored medians

Data Sources (same as production):
- tracked_tickers: ticker, industry, sector, financial_currency, fundamentals
- stock_prices: latest adjusted_close for price
- peer_benchmarks: stored medians to compare against

Median Rule (for even counts):
- Uses WEIGHTED median (market_cap as weight)
- Returns first value where cumulative weight >= total_weight / 2

Run with: python scripts/debug_peer_math.py "Insurance - Diversified"
         python scripts/debug_peer_math.py "Consumer Electronics"
"""

import asyncio
import os
import sys
import statistics
from datetime import datetime, timezone
from typing import Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv
load_dotenv()


def safe_float(val) -> Optional[float]:
    """Safely convert to float."""
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def get_ttm_sum(quarterly_data: dict, field: str) -> Optional[float]:
    """Get TTM (trailing 12 months) sum from quarterly data."""
    if not quarterly_data:
        return None
    sorted_quarters = sorted(quarterly_data.keys(), reverse=True)[:4]
    if len(sorted_quarters) < 4:
        return None
    total = 0
    for q in sorted_quarters:
        val = safe_float(quarterly_data[q].get(field))
        if val is None:
            return None
        total += val
    return total


def get_latest_value(quarterly_data: dict, field: str) -> Optional[float]:
    """Get most recent value from quarterly data."""
    if not quarterly_data:
        return None
    sorted_quarters = sorted(quarterly_data.keys(), reverse=True)
    for q in sorted_quarters:
        val = safe_float(quarterly_data[q].get(field))
        if val is not None:
            return val
    return None


def winsorize_values(values: list, low_pct: float = 1, high_pct: float = 99) -> list:
    """
    Winsorize values by capping at percentile bounds.
    Same logic as production compute_peer_benchmarks_v3.
    """
    if len(values) < 3:
        return values
    sorted_vals = sorted(values)
    low_idx = max(0, int(len(sorted_vals) * low_pct / 100))
    high_idx = min(len(sorted_vals) - 1, int(len(sorted_vals) * high_pct / 100))
    return [max(sorted_vals[low_idx], min(sorted_vals[high_idx], v)) for v in values]


def weighted_median(values: list, weights: list) -> Optional[float]:
    """
    Compute weighted median.
    Same logic as production compute_peer_benchmarks_v3.
    
    Rule: Returns first value where cumulative weight >= total_weight / 2
    (effectively the lower of the two middle values for even counts)
    """
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


async def debug_peer_math(industry: str):
    """
    Debug peer median math for a specific industry.
    READ-ONLY - no database writes.
    """
    client = AsyncIOMotorClient(os.environ.get('MONGO_URL'))
    db = client[os.environ.get('DB_NAME', 'richstox_prod')]
    
    print("=" * 100)
    print(f"PEER MEDIAN MATH DEBUG: {industry}")
    print("=" * 100)
    print(f"Timestamp: {datetime.now(timezone.utc).isoformat()}")
    print()
    
    # =========================================================================
    # STEP 1: Select peer universe (exactly like production)
    # =========================================================================
    print("STEP 1: SELECT PEER UNIVERSE")
    print("-" * 100)
    print(f"Source: tracked_tickers collection")
    print(f"Filter: is_visible=True, industry='{industry}', financial_currency='USD'")
    print()
    
    # Get all tickers in this industry
    all_industry_tickers = await db.tracked_tickers.find(
        {"is_visible": True, "industry": industry, "fundamentals": {"$exists": True, "$ne": None}},
        {"_id": 0, "ticker": 1, "financial_currency": 1}
    ).to_list(length=1000)
    
    usd_tickers = [t for t in all_industry_tickers if t.get("financial_currency") == "USD"]
    non_usd_tickers = [t for t in all_industry_tickers if t.get("financial_currency") and t.get("financial_currency") != "USD"]
    null_tickers = [t for t in all_industry_tickers if not t.get("financial_currency")]
    
    print(f"Total industry tickers: {len(all_industry_tickers)}")
    print(f"  USD (included): {len(usd_tickers)}")
    print(f"  Non-USD (excluded): {len(non_usd_tickers)} - {[t['ticker'] for t in non_usd_tickers]}")
    print(f"  NULL currency (excluded): {len(null_tickers)} - {[t['ticker'] for t in null_tickers]}")
    print()
    
    # =========================================================================
    # STEP 2: Get latest prices
    # =========================================================================
    ticker_names = [t["ticker"] for t in usd_tickers]
    
    latest_prices = {}
    cursor = db.stock_prices.aggregate([
        {"$match": {"ticker": {"$in": ticker_names}}},
        {"$sort": {"date": -1}},
        {"$group": {"_id": "$ticker", "price": {"$first": "$adjusted_close"}, "date": {"$first": "$date"}}}
    ])
    async for doc in cursor:
        if doc.get("price"):
            latest_prices[doc["_id"]] = {"price": doc["price"], "date": doc["date"]}
    
    print(f"Prices fetched: {len(latest_prices)} tickers")
    print()
    
    # =========================================================================
    # STEP 3: Compute raw metrics for each peer (exactly like production)
    # =========================================================================
    print("STEP 2: COMPUTE RAW METRICS")
    print("-" * 100)
    print("Source: tracked_tickers.fundamentals (Financials, SharesStats, Earnings)")
    print("Metrics: P/E, P/S, P/B, EV/EBITDA, EV/Revenue")
    print()
    
    peer_data = []
    
    for ticker_doc in usd_tickers:
        ticker = ticker_doc["ticker"]
        
        # Fetch fundamentals
        full_doc = await db.tracked_tickers.find_one(
            {"ticker": ticker},
            {"_id": 0, "ticker": 1, "fundamentals": 1}
        )
        if not full_doc:
            continue
        
        fundamentals = full_doc.get("fundamentals", {})
        price_info = latest_prices.get(ticker)
        
        if not price_info or not fundamentals:
            continue
        
        current_price = price_info["price"]
        
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
        data = {
            "ticker": ticker,
            "price": current_price,
            "shares": shares,
            "market_cap": market_cap,
            "enterprise_value": enterprise_value,
            "eps_ttm": eps_ttm,
            "revenue_ttm": revenue_ttm,
            "ebitda_ttm": ebitda_ttm,
            "total_equity": total_equity,
        }
        
        # P/E
        if eps_ttm and eps_ttm > 0:
            data["pe"] = current_price / eps_ttm
        
        # P/S
        if revenue_ttm and revenue_ttm > 0:
            data["ps"] = market_cap / revenue_ttm
        
        # P/B
        if total_equity and total_equity > 0:
            data["pb"] = market_cap / total_equity
        
        # EV/EBITDA
        if ebitda_ttm and ebitda_ttm > 0 and enterprise_value > 0:
            data["ev_ebitda"] = enterprise_value / ebitda_ttm
        
        # EV/Revenue
        if revenue_ttm and revenue_ttm > 0 and enterprise_value > 0:
            data["ev_revenue"] = enterprise_value / revenue_ttm
        
        peer_data.append(data)
    
    # Print peer table
    print(f"{'Ticker':<12} {'Price':>10} {'MktCap':>14} {'MktCap%':>8} {'P/E':>10} {'P/S':>10} {'P/B':>10} {'EV/EBITDA':>12} {'EV/Rev':>10}")
    print("-" * 110)
    
    total_mktcap = sum(p.get("market_cap", 0) for p in peer_data)
    for p in sorted(peer_data, key=lambda x: x.get("market_cap", 0), reverse=True):
        pe = f"{p['pe']:.4f}" if p.get('pe') else "N/A"
        ps = f"{p['ps']:.4f}" if p.get('ps') else "N/A"
        pb = f"{p['pb']:.4f}" if p.get('pb') else "N/A"
        ev_eb = f"{p['ev_ebitda']:.4f}" if p.get('ev_ebitda') else "N/A"
        ev_rev = f"{p['ev_revenue']:.4f}" if p.get('ev_revenue') else "N/A"
        mktcap_pct = (p['market_cap'] / total_mktcap * 100) if total_mktcap > 0 else 0
        print(f"{p['ticker']:<12} {p['price']:>10.2f} {p['market_cap']/1e9:>12.2f}B {mktcap_pct:>7.1f}% {pe:>10} {ps:>10} {pb:>10} {ev_eb:>12} {ev_rev:>10}")
    
    print("-" * 110)
    print(f"{'TOTAL':<12} {'':>10} {total_mktcap/1e9:>12.2f}B {100.0:>7.1f}%")
    print()
    
    # =========================================================================
    # STEP 4: Recompute medians (exactly like production)
    # =========================================================================
    print("STEP 3: RECOMPUTE MEDIANS")
    print("-" * 100)
    print("Method: Winsorization (1st-99th percentile) + Weighted Median (market_cap)")
    print()
    
    metric_names = ["pe", "ps", "pb", "ev_ebitda", "ev_revenue"]
    computed_medians = {}
    
    for metric in metric_names:
        values = [p.get(metric) for p in peer_data if p.get(metric) is not None and p.get(metric) > 0]
        weights = [p.get("market_cap", 1) for p in peer_data if p.get(metric) is not None and p.get(metric) > 0]
        tickers_with_metric = [p.get("ticker") for p in peer_data if p.get(metric) is not None and p.get(metric) > 0]
        
        if len(values) >= 3:
            # Apply winsorization
            winsorized = winsorize_values(values)
            
            # Compute weighted median
            median_val = weighted_median(winsorized, weights)
            
            if median_val is not None:
                computed_medians[f"{metric}_median"] = round(median_val, 2)
                
                # Show detailed breakdown
                print(f"{metric.upper()} ({len(values)} values):")
                print(f"  Tickers: {tickers_with_metric}")
                print(f"  Raw values: {[round(v, 4) for v in values]}")
                print(f"  Winsorized: {[round(v, 4) for v in winsorized]}")
                print(f"  Weights (MktCap $B): {[round(w/1e9, 2) for w in weights]}")
                
                # Show weighted median calculation step-by-step
                pairs = sorted(zip(winsorized, weights, tickers_with_metric), key=lambda x: x[0])
                total_weight = sum(weights)
                print(f"  Total weight: ${total_weight/1e9:.2f}B, 50% threshold: ${total_weight/2/1e9:.2f}B")
                print(f"  Sorted (value, weight, ticker):")
                cumsum = 0
                for val, weight, tick in pairs:
                    cumsum += weight
                    marker = " <-- MEDIAN" if cumsum >= total_weight / 2 and median_val == val else ""
                    print(f"    {tick}: {val:.4f}, ${weight/1e9:.2f}B, cumsum=${cumsum/1e9:.2f}B{marker}")
                    if cumsum >= total_weight / 2 and median_val == val:
                        break
                
                print(f"  => Weighted Median: {median_val:.4f} (rounded: {round(median_val, 2)})")
                print()
        else:
            print(f"{metric.upper()}: Insufficient data (only {len(values)} valid values, need >= 3)")
            print()
    
    # =========================================================================
    # STEP 5: Compare with stored medians
    # =========================================================================
    print("STEP 4: COMPARE VS STORED")
    print("-" * 100)
    
    stored_doc = await db.peer_benchmarks.find_one({"industry": industry}, {"_id": 0})
    
    if stored_doc:
        stored_benchmarks = stored_doc.get("benchmarks", {})
        peer_count_total = stored_doc.get("peer_count_total", stored_doc.get("peer_count", 0))
        peer_count_used = stored_doc.get("peer_count_used", stored_doc.get("peer_count", 0))
        computed_at = stored_doc.get("computed_at")
        
        print(f"Stored document:")
        print(f"  peer_count_total: {peer_count_total}")
        print(f"  peer_count_used: {peer_count_used}")
        print(f"  computed_at: {computed_at}")
        print()
        
        print(f"{'Metric':<15} {'Computed':>12} {'Stored':>12} {'Diff':>12} {'Status':<10}")
        print("-" * 65)
        
        all_match = True
        for metric in metric_names:
            key = f"{metric}_median"
            computed = computed_medians.get(key)
            stored = stored_benchmarks.get(key)
            
            if computed is not None and stored is not None:
                diff = computed - stored
                status = "OK" if abs(diff) < 0.01 else "MISMATCH"
                if status == "MISMATCH":
                    all_match = False
                print(f"{key:<15} {computed:>12.2f} {stored:>12.2f} {diff:>+12.2f} {status:<10}")
            elif computed is None and stored is None:
                print(f"{key:<15} {'N/A':>12} {'N/A':>12} {'':>12} {'OK (both N/A)':<10}")
            else:
                all_match = False
                print(f"{key:<15} {str(computed) if computed else 'N/A':>12} {str(stored) if stored else 'N/A':>12} {'':>12} {'MISMATCH':<10}")
        
        print()
        print(f"VERDICT: {'ALL MEDIANS MATCH' if all_match else 'MISMATCH DETECTED'}")
    else:
        print(f"ERROR: No peer_benchmarks document found for industry '{industry}'")
    
    print()
    print("=" * 100)
    print("END OF DEBUG")
    print("=" * 100)
    
    client.close()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python scripts/debug_peer_math.py <industry>")
        print("Example: python scripts/debug_peer_math.py 'Insurance - Diversified'")
        sys.exit(1)
    
    industry = sys.argv[1]
    asyncio.run(debug_peer_math(industry))
