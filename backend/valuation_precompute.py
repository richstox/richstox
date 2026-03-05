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
from typing import Dict, Any, Optional, List, Tuple
import logging
import os
from bisect import bisect_right

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
# TIMESERIES HELPERS (M-06)
# =============================================================================

def _sorted_date_keys(section: dict, limit: int) -> List[str]:
    """Return latest date keys (YYYY-MM-DD) sorted descending."""
    if not isinstance(section, dict) or not section:
        return []
    return sorted([k for k in section.keys() if isinstance(k, str)], reverse=True)[:limit]


def _latest_value_on_or_before(section: dict, field: str, period_end: str) -> Optional[float]:
    """Get latest numeric value from statement section at or before period_end."""
    if not isinstance(section, dict) or not section:
        return None
    for key in sorted(section.keys(), reverse=True):
        if key <= period_end:
            val = safe_float((section.get(key) or {}).get(field))
            if val is not None:
                return val
    return None


def _build_price_index(price_rows: List[dict]) -> Tuple[List[str], List[float]]:
    """
    Build ascending date/value arrays for binary-search alignment.
    Keeps only rows with numeric positive price.
    """
    items: List[Tuple[str, float]] = []
    for row in price_rows:
        date = row.get("date")
        price = safe_float(row.get("adjusted_close")) or safe_float(row.get("close"))
        if not date or price is None or price <= 0:
            continue
        items.append((date, price))
    items.sort(key=lambda x: x[0])  # ascending
    return [d for d, _ in items], [p for _, p in items]


def _price_on_or_before(price_dates: List[str], price_values: List[float], period_end: str) -> Tuple[Optional[float], Optional[str]]:
    """
    Period alignment rule:
    price_used = close from nearest previous trading day (<= period_end).
    """
    if not price_dates:
        return None, None
    idx = bisect_right(price_dates, period_end) - 1
    if idx < 0:
        return None, None
    return price_values[idx], price_dates[idx]


def _extract_shares_as_of(fund: dict, period_end: str) -> Optional[float]:
    """Get shares outstanding as-of period_end from raw facts only."""
    shares_stats = fund.get("SharesStats", {}) or {}
    shares_direct = safe_float(shares_stats.get("SharesOutstanding"))

    outstanding = fund.get("outstandingShares", {}) or {}
    candidates: List[Tuple[str, float]] = []

    # Dict format: {"annual": {"2024-12-31": {"shares": ...}}}
    for bucket in ("quarterly", "annual"):
        block = outstanding.get(bucket, {})
        if isinstance(block, dict):
            for date_key, payload in block.items():
                shares_val = safe_float((payload or {}).get("shares"))
                if shares_val is not None:
                    candidates.append((str(date_key), shares_val))
        elif isinstance(block, list):
            for payload in block:
                if not isinstance(payload, dict):
                    continue
                date_key = payload.get("date") or payload.get("period") or payload.get("reportedDate")
                shares_val = safe_float(payload.get("shares"))
                if date_key and shares_val is not None:
                    candidates.append((str(date_key), shares_val))

    # Prefer period-aligned shares first
    aligned = [(d, s) for d, s in candidates if d <= period_end]
    if aligned:
        aligned.sort(key=lambda x: x[0], reverse=True)
        return aligned[0][1]

    # Fallback to latest known shares
    if candidates:
        candidates.sort(key=lambda x: x[0], reverse=True)
        return candidates[0][1]

    return shares_direct


def _ttm_sum_as_of(quarterly: dict, field: str, period_end: str) -> Optional[float]:
    """
    Get TTM sum ending at period_end from quarterly statement.
    Requires at least 3 valid quarters out of 4.
    """
    if not isinstance(quarterly, dict) or not quarterly:
        return None
    eligible = [k for k in sorted(quarterly.keys(), reverse=True) if k <= period_end]
    window = eligible[:4]
    if len(window) < 4:
        return None
    total = 0.0
    valid = 0
    for q in window:
        v = safe_float((quarterly.get(q) or {}).get(field))
        if v is not None:
            total += v
            valid += 1
    return total if valid >= 3 else None


def _eps_ttm_as_of(earnings_history: dict, period_end: str) -> Optional[float]:
    """Compute EPS TTM from earnings history up to period_end."""
    if not isinstance(earnings_history, dict) or not earnings_history:
        return None
    eligible = [k for k in sorted(earnings_history.keys(), reverse=True) if k <= period_end]
    window = eligible[:4]
    if len(window) < 4:
        return None
    vals = []
    for key in window:
        v = safe_float((earnings_history.get(key) or {}).get("epsActual"))
        if v is not None:
            vals.append(v)
    if len(vals) >= 3:
        return sum(vals)
    return None


def _classify_relative(current: Optional[float], benchmark: Optional[float]) -> Optional[str]:
    if current is None or benchmark is None or benchmark <= 0:
        return None
    ratio = current / benchmark
    if ratio < 0.85:
        return "cheaper"
    if ratio > 1.15:
        return "more_expensive"
    return "around"


def _majority_badge(classifications: List[str]) -> Optional[str]:
    if not classifications:
        return None
    cheaper_count = sum(1 for x in classifications if x == "cheaper")
    expensive_count = sum(1 for x in classifications if x == "more_expensive")
    total = len(classifications)
    if expensive_count / total >= 0.6:
        return "more_expensive"
    if cheaper_count / total >= 0.6:
        return "cheaper"
    return "around"


def _compute_point_metrics(
    *,
    price_used: Optional[float],
    shares_used: Optional[float],
    net_income_used: Optional[float],
    revenue_used: Optional[float],
    book_value_used: Optional[float],
    ebitda_used: Optional[float],
    total_debt_used: Optional[float],
    cash_used: Optional[float],
) -> Tuple[Dict[str, Optional[float]], Dict[str, str], Dict[str, Any]]:
    """
    Compute valuation point metrics with persisted reason codes and audit inputs.
    """
    market_cap_used = None
    if price_used is not None and shares_used is not None and shares_used > 0:
        market_cap_used = price_used * shares_used

    ev_used = None
    if market_cap_used is not None and total_debt_used is not None and cash_used is not None:
        ev_used = market_cap_used + total_debt_used - cash_used

    metrics: Dict[str, Optional[float]] = {
        "pe": None,
        "ps": None,
        "pb": None,
        "ev_ebitda": None,
        "ev_revenue": None,
    }
    statuses: Dict[str, str] = {}

    # P/E
    if shares_used is None or shares_used <= 0:
        statuses["pe"] = MetricStatus.MISSING_SHARES
    elif net_income_used is None:
        statuses["pe"] = MetricStatus.MISSING_RAW_DATA
    else:
        eps_used = net_income_used / shares_used if shares_used else None
        pe_val, pe_status = safe_divide(
            price_used,
            eps_used,
            require_positive_denominator=True,
        )
        metrics["pe"] = pe_val
        statuses["pe"] = pe_status

    # P/S
    if market_cap_used is None:
        statuses["ps"] = MetricStatus.MISSING_SHARES
    else:
        ps_val, ps_status = safe_divide(
            market_cap_used,
            revenue_used,
            require_positive_denominator=True,
        )
        metrics["ps"] = ps_val
        statuses["ps"] = ps_status

    # P/B
    if market_cap_used is None:
        statuses["pb"] = MetricStatus.MISSING_SHARES
    else:
        pb_val, pb_status = safe_divide(
            market_cap_used,
            book_value_used,
            require_positive_denominator=True,
        )
        metrics["pb"] = pb_val
        statuses["pb"] = pb_status

    # EV/EBITDA
    if ev_used is None:
        statuses["ev_ebitda"] = MetricStatus.MISSING_SHARES if market_cap_used is None else MetricStatus.MISSING_RAW_DATA
    else:
        ev_ebitda_val, ev_ebitda_status = safe_divide(
            ev_used,
            ebitda_used,
            require_positive_denominator=True,
        )
        metrics["ev_ebitda"] = ev_ebitda_val
        statuses["ev_ebitda"] = ev_ebitda_status

    # EV/Revenue
    if ev_used is None:
        statuses["ev_revenue"] = MetricStatus.MISSING_SHARES if market_cap_used is None else MetricStatus.MISSING_RAW_DATA
    else:
        ev_revenue_val, ev_revenue_status = safe_divide(
            ev_used,
            revenue_used,
            require_positive_denominator=True,
        )
        metrics["ev_revenue"] = ev_revenue_val
        statuses["ev_revenue"] = ev_revenue_status

    inputs_used = {
        "price_used": price_used,
        "shares_used": shares_used,
        "market_cap_used": market_cap_used,
        "net_income_used": net_income_used,
        "revenue_used": revenue_used,
        "book_value_used": book_value_used,
        "ebitda_used": ebitda_used,
        "ev_used": ev_used,
        "fx_used": None,  # Reserved for future FX normalization
        # Backward-compatible aliases for existing API payload usage
        "enterprise_value": ev_used,
        "total_debt": total_debt_used,
        "cash": cash_used,
        "shares_outstanding": shares_used,
    }
    return metrics, statuses, inputs_used


# =============================================================================
# MAIN PRE-COMPUTATION FUNCTION
# =============================================================================

async def precompute_ticker_valuations(db) -> Dict[str, Any]:
    """
    M-06: Pre-compute local-only valuation time-series for ALL visible tickers.

    Source of truth:
      - ticker_valuation_timeseries (quarterly + annual points)

    Materialized latest view:
      - ticker_valuations_cache (derived ONLY from ticker_valuation_timeseries)
    """
    logger.info("Starting M-06 valuation pre-computation (timeseries + latest view)...")
    start_time = datetime.now(timezone.utc)
    
    # Get all visible tickers (metadata only)
    ticker_list = await db.tracked_tickers.find(
        {"is_visible": True},
        {"_id": 0, "ticker": 1, "sector": 1, "industry": 1}
    ).to_list(length=10000)
    
    logger.info(f"Found {len(ticker_list)} visible tickers")

    # Load peer benchmarks for comparison
    peer_benchmarks = {}
    cursor = db.peer_benchmarks.find({})
    async for doc in cursor:
        # Support both shapes:
        #   1) {"benchmarks": {"pe_median": ...}}
        #   2) {"pe_median": ...}
        benchmarks = doc.get("benchmarks", {})
        if not benchmarks:
            benchmarks = {
                "pe_median": doc.get("pe_median"),
                "ps_median": doc.get("ps_median"),
                "pb_median": doc.get("pb_median"),
                "ev_ebitda_median": doc.get("ev_ebitda_median"),
                "ev_revenue_median": doc.get("ev_revenue_median"),
            }
        key = doc.get("industry") or f"sector:{doc.get('sector')}"
        if key:
            peer_benchmarks[key] = benchmarks
    
    logger.info(f"Loaded {len(peer_benchmarks)} peer benchmarks")

    # Ensure indexes for idempotent time-series upserts
    await db.ticker_valuation_timeseries.create_index(
        [("ticker", 1), ("period_type", 1), ("period_end", 1)],
        unique=True
    )
    await db.ticker_valuations_cache.create_index("ticker", unique=True)

    # Process tickers in batches
    batch_size = 100
    processed = 0
    latest_views_stored = 0
    points_stored = 0
    quarterly_points_stored = 0
    annual_points_stored = 0
    
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

        # Fetch 5Y price history for batch (for period alignment)
        price_rows_map: Dict[str, List[dict]] = {ticker: [] for ticker in batch_tickers}
        price_cursor = db.stock_prices.find(
            {"ticker": {"$in": batch_tickers}},
            {"_id": 0, "ticker": 1, "date": 1, "adjusted_close": 1, "close": 1}
        )
        async for row in price_cursor:
            ticker = row.get("ticker")
            if ticker in price_rows_map:
                price_rows_map[ticker].append(row)

        # Process each ticker in batch
        for t in batch:
            ticker = t["ticker"]
            sector = t.get("sector")
            industry = t.get("industry")
            fund = fundamentals_map.get(ticker, {})
            price_rows = price_rows_map.get(ticker, [])
            
            processed += 1
            
            if not fund or not price_rows:
                continue

            price_dates, price_values = _build_price_index(price_rows)
            if not price_dates:
                continue

            financials = fund.get("Financials", {})
            earnings = fund.get("Earnings", {})
            income_stmt = financials.get("Income_Statement", {})
            quarterly_income = income_stmt.get("quarterly", {})
            yearly_income = income_stmt.get("yearly", {})
            balance_sheet = financials.get("Balance_Sheet", {})
            quarterly_balance = balance_sheet.get("quarterly", {})
            earnings_history = earnings.get("History", {})
            cash_flow = financials.get("Cash_Flow", {})
            quarterly_cashflow = cash_flow.get("quarterly", {})
            yearly_balance = balance_sheet.get("yearly", {})
            yearly_cashflow = cash_flow.get("yearly", {})
            yearly_earnings = earnings.get("Annual", {})

            quarterly_periods = _sorted_date_keys(quarterly_income, 20)
            annual_periods = _sorted_date_keys(yearly_income, 5)

            quarterly_points: List[dict] = []
            annual_points: List[dict] = []

            # ---- Quarterly (20Q) points ----
            for period_end in quarterly_periods:
                price_used, price_date_used = _price_on_or_before(price_dates, price_values, period_end)
                shares_used = _extract_shares_as_of(fund, period_end)
                net_income_used = _ttm_sum_as_of(quarterly_income, "netIncome", period_end)
                revenue_used = _ttm_sum_as_of(quarterly_income, "totalRevenue", period_end)
                ebitda_used = _ttm_sum_as_of(quarterly_income, "ebitda", period_end)

                if ebitda_used is None:
                    op_income = _ttm_sum_as_of(quarterly_income, "operatingIncome", period_end)
                    depreciation = _ttm_sum_as_of(quarterly_cashflow, "depreciation", period_end) or 0
                    if op_income is not None:
                        ebitda_used = op_income + abs(depreciation)

                book_value_used = _latest_value_on_or_before(quarterly_balance, "totalStockholderEquity", period_end)
                total_debt_used = _latest_value_on_or_before(quarterly_balance, "totalDebt", period_end)
                if total_debt_used is None:
                    short_term = _latest_value_on_or_before(quarterly_balance, "shortTermDebt", period_end) or 0
                    long_term = _latest_value_on_or_before(quarterly_balance, "longTermDebt", period_end) or 0
                    total_debt_used = short_term + long_term if (short_term or long_term) else None
                cash_used = _latest_value_on_or_before(quarterly_balance, "cash", period_end)
                if cash_used is None:
                    cash_used = _latest_value_on_or_before(quarterly_balance, "cashAndShortTermInvestments", period_end)

                # EPS TTM from earnings history, fallback net_income/shares handled in metric compute
                eps_ttm = _eps_ttm_as_of(earnings_history, period_end)
                if eps_ttm is not None and shares_used is not None:
                    # Convert EPS back to net income equivalent so one input contract stays consistent
                    net_income_for_pe = eps_ttm * shares_used
                else:
                    net_income_for_pe = net_income_used

                metrics, statuses, inputs_used = _compute_point_metrics(
                    price_used=price_used,
                    shares_used=shares_used,
                    net_income_used=net_income_for_pe,
                    revenue_used=revenue_used,
                    book_value_used=book_value_used,
                    ebitda_used=ebitda_used,
                    total_debt_used=total_debt_used,
                    cash_used=cash_used,
                )

                point_doc = {
                    "ticker": ticker,
                    "period_type": "quarterly",
                    "period_end": period_end,
                    "price_date_used": price_date_used,
                    "metrics": metrics,
                    "metric_statuses": statuses,
                    "reason_codes": {k: (None if v == MetricStatus.OK else v) for k, v in statuses.items()},
                    "inputs_used": inputs_used,
                    "sector": sector,
                    "industry": industry,
                    "computed_at": datetime.now(timezone.utc).isoformat(),
                    "source": "local_raw_facts",
                }
                await db.ticker_valuation_timeseries.replace_one(
                    {"ticker": ticker, "period_type": "quarterly", "period_end": period_end},
                    point_doc,
                    upsert=True
                )
                quarterly_points.append(point_doc)
                quarterly_points_stored += 1
                points_stored += 1

            # ---- Annual (5Y) points ----
            for period_end in annual_periods:
                price_used, price_date_used = _price_on_or_before(price_dates, price_values, period_end)
                shares_used = _extract_shares_as_of(fund, period_end)
                net_income_used = safe_float((yearly_income.get(period_end) or {}).get("netIncome"))
                revenue_used = safe_float((yearly_income.get(period_end) or {}).get("totalRevenue"))
                ebitda_used = safe_float((yearly_income.get(period_end) or {}).get("ebitda"))
                if ebitda_used is None:
                    op_income = safe_float((yearly_income.get(period_end) or {}).get("operatingIncome"))
                    depreciation = safe_float((yearly_cashflow.get(period_end) or {}).get("depreciation")) or 0
                    if op_income is not None:
                        ebitda_used = op_income + abs(depreciation)

                book_value_used = safe_float((yearly_balance.get(period_end) or {}).get("totalStockholderEquity"))
                total_debt_used = safe_float((yearly_balance.get(period_end) or {}).get("totalDebt"))
                if total_debt_used is None:
                    short_term = safe_float((yearly_balance.get(period_end) or {}).get("shortTermDebt")) or 0
                    long_term = safe_float((yearly_balance.get(period_end) or {}).get("longTermDebt")) or 0
                    total_debt_used = short_term + long_term if (short_term or long_term) else None
                cash_used = safe_float((yearly_balance.get(period_end) or {}).get("cash"))
                if cash_used is None:
                    cash_used = safe_float((yearly_balance.get(period_end) or {}).get("cashAndShortTermInvestments"))

                # If annual earnings exists for same period, prefer it for PE component
                earnings_annual_val = safe_float((yearly_earnings.get(period_end) or {}).get("epsActual"))
                if earnings_annual_val is not None and shares_used is not None:
                    net_income_for_pe = earnings_annual_val * shares_used
                else:
                    net_income_for_pe = net_income_used

                metrics, statuses, inputs_used = _compute_point_metrics(
                    price_used=price_used,
                    shares_used=shares_used,
                    net_income_used=net_income_for_pe,
                    revenue_used=revenue_used,
                    book_value_used=book_value_used,
                    ebitda_used=ebitda_used,
                    total_debt_used=total_debt_used,
                    cash_used=cash_used,
                )

                point_doc = {
                    "ticker": ticker,
                    "period_type": "annual",
                    "period_end": period_end,
                    "price_date_used": price_date_used,
                    "metrics": metrics,
                    "metric_statuses": statuses,
                    "reason_codes": {k: (None if v == MetricStatus.OK else v) for k, v in statuses.items()},
                    "inputs_used": inputs_used,
                    "sector": sector,
                    "industry": industry,
                    "computed_at": datetime.now(timezone.utc).isoformat(),
                    "source": "local_raw_facts",
                }
                await db.ticker_valuation_timeseries.replace_one(
                    {"ticker": ticker, "period_type": "annual", "period_end": period_end},
                    point_doc,
                    upsert=True
                )
                annual_points.append(point_doc)
                annual_points_stored += 1
                points_stored += 1

            if not quarterly_points and not annual_points:
                continue

            # -----------------------------------------------------------------
            # Materialized latest view (derived only from timeseries)
            # -----------------------------------------------------------------
            latest_point = quarterly_points[0] if quarterly_points else annual_points[0]
            latest_metrics_full = latest_point.get("metrics", {})
            latest_metrics = {k: v for k, v in latest_metrics_full.items() if v is not None}
            latest_statuses = latest_point.get("metric_statuses", {})
            latest_inputs = latest_point.get("inputs_used", {})

            peer_bench = peer_benchmarks.get(industry) or peer_benchmarks.get(f"sector:{sector}") or {}
            vs_peers = {}
            for metric_name, current_val in latest_metrics.items():
                peer_val = safe_float(peer_bench.get(f"{metric_name}_median"))
                cls = _classify_relative(current_val, peer_val)
                if cls:
                    vs_peers[metric_name] = cls
            eval_vs_peers = _majority_badge(list(vs_peers.values()))

            # 5Y comparison from quarterly source-of-truth points (excluding latest)
            history_pool = quarterly_points[1:] if len(quarterly_points) > 1 else quarterly_points
            metric_avgs: Dict[str, Optional[float]] = {}
            vs_5y = {}
            for metric_name in ["pe", "ps", "pb", "ev_ebitda", "ev_revenue"]:
                vals = [safe_float((p.get("metrics") or {}).get(metric_name)) for p in history_pool]
                vals = [v for v in vals if v is not None]
                metric_avgs[metric_name] = round(sum(vals) / len(vals), 4) if vals else None
                cls = _classify_relative(latest_metrics.get(metric_name), metric_avgs[metric_name])
                if cls:
                    vs_5y[metric_name] = cls
            eval_vs_5y = _majority_badge(list(vs_5y.values()))

            cache_doc = {
                "_id": ticker,
                "ticker": ticker,
                "sector": sector,
                "industry": industry,
                "period_type": latest_point.get("period_type"),
                "period_end": latest_point.get("period_end"),
                "price_date_used": latest_point.get("price_date_used"),
                "current_price": latest_inputs.get("price_used"),
                "market_cap": round(latest_inputs.get("market_cap_used"), 0) if latest_inputs.get("market_cap_used") else None,
                "current_metrics": latest_metrics,
                "metric_statuses": latest_statuses,
                "peer_benchmarks": {f"{k}_median": v for k, v in peer_bench.items()} if peer_bench else None,
                "vs_peers": vs_peers if vs_peers else None,
                "eval_vs_peers": eval_vs_peers,
                "vs_5y": vs_5y if vs_5y else None,
                "metric_5y_averages": metric_avgs,
                "eval_vs_5y": eval_vs_5y,
                # Backward-compatible raw_inputs shape expected by server
                "raw_inputs": {
                    "ebitda_ttm": latest_inputs.get("ebitda_used"),
                    "total_debt": latest_inputs.get("total_debt"),
                    "cash": latest_inputs.get("cash"),
                    "enterprise_value": latest_inputs.get("enterprise_value"),
                    "shares_outstanding": latest_inputs.get("shares_outstanding"),
                },
                "timeseries_source": {
                    "collection": "ticker_valuation_timeseries",
                    "latest_period_type": latest_point.get("period_type"),
                    "latest_period_end": latest_point.get("period_end"),
                    "quarterly_points": len(quarterly_points),
                    "annual_points": len(annual_points),
                },
                "computed_at": datetime.now(timezone.utc).isoformat(),
                "source": "materialized_from_timeseries",
            }

            await db.ticker_valuations_cache.replace_one(
                {"_id": ticker},
                cache_doc,
                upsert=True
            )
            latest_views_stored += 1

        if processed % 500 == 0:
            logger.info(
                f"  Processed {processed}/{len(ticker_list)} tickers, "
                f"timeseries_points={points_stored}, latest_views={latest_views_stored}"
            )
    
    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    
    logger.info(
        f"M-06 valuation pre-computation completed: latest_views={latest_views_stored}, "
        f"timeseries_points={points_stored} in {elapsed:.1f}s"
    )
    
    return {
        "status": "success",
        "tickers_processed": processed,
        "tickers_stored": latest_views_stored,
        "timeseries_points_stored": points_stored,
        "quarterly_points_stored": quarterly_points_stored,
        "annual_points_stored": annual_points_stored,
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
