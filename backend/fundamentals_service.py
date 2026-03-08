"""
RICHSTOX Fundamentals Service
=============================
Fetches, parses, and stores EODHD fundamentals data in normalized tables.

Tables:
- company_fundamentals_cache: Key company metrics (expanded)
- financials_cache: Income/Balance/CashFlow metrics per period
- earnings_history_cache: Quarterly EPS data
- insider_activity_cache: Aggregated 6-month insider activity

Schema is NORMALIZED (not JSONB) for query speed and indexing.
"""

import os
import logging
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional
import httpx
from provider_debug_service import upsert_provider_debug_snapshot

logger = logging.getLogger("richstox.fundamentals")

EODHD_BASE_URL = "https://eodhd.com/api"
EODHD_API_KEY = os.getenv("EODHD_API_KEY", "")

# Pilot tickers for initial testing
PILOT_TICKERS = [
    "AAPL", "MSFT", "JNJ", "NVDA", "TSLA", "GOOGL", "AMZN", 
    "META", "NFLX", "AVGO", "COST", "ADBE", "ASML", "LRCX", "CDNS"
]


async def fetch_fundamentals_from_eodhd(ticker: str) -> Optional[Dict[str, Any]]:
    """
    Fetch full fundamentals from EODHD API.
    Cost: 10 credits per request.
    """
    if not EODHD_API_KEY:
        logger.error("EODHD_API_KEY not configured")
        return None
    
    # Ensure .US suffix
    ticker_full = ticker if ticker.endswith(".US") else f"{ticker}.US"
    
    url = f"{EODHD_BASE_URL}/fundamentals/{ticker_full}"
    params = {"api_token": EODHD_API_KEY, "fmt": "json"}
    
    try:
        async with httpx.AsyncClient(timeout=60) as client:
            response = await client.get(url, params=params)
            
            if response.status_code == 404:
                logger.warning(f"No fundamentals for {ticker}")
                return None
            
            response.raise_for_status()
            data = response.json()
            
            if not data or not data.get("General"):
                logger.warning(f"Empty fundamentals for {ticker}")
                return None
            
            return data
    except Exception as e:
        logger.error(f"Failed to fetch fundamentals for {ticker}: {e}")
        return None


def parse_company_fundamentals(ticker: str, data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Parse EODHD fundamentals into company_fundamentals_cache schema.
    Includes ALL fields needed for stock detail page.
    Handles missing/null data gracefully.
    """
    general = data.get("General") or {}
    shares = data.get("SharesStats") or {}
    splits_div = data.get("SplitsDividends") or {}
    address = general.get("AddressData") or {}

    now = datetime.now(timezone.utc)

    # Normalise sector/industry at parse time: strip whitespace, collapse empty → None
    sector_raw   = (general.get("Sector")   or "").strip()
    industry_raw = (general.get("Industry") or "").strip()

    # CRITICAL DEBUG: log exactly what EODHD returned so parser issues are visible
    logger.critical(
        f"PARSER DEBUG: Ticker {ticker} -> "
        f"Sector: '{sector_raw}', Industry: '{industry_raw}', "
        f"HasClass: {bool(sector_raw and industry_raw)}"
    )

    # Calculate EPS TTM from earnings history
    eps_ttm = None
    earnings_data = data.get("Earnings") or {}
    earnings_history = earnings_data.get("History") or {}
    if earnings_history:
        # Get last 4 quarters
        quarters = sorted(earnings_history.values(), key=lambda x: x.get("reportDate", "") if x else "", reverse=True)[:4]
        eps_values = [q.get("epsActual") for q in quarters if q and q.get("epsActual") is not None]
        if len(eps_values) == 4:
            eps_ttm = sum(eps_values)
    
    return {
        "ticker": ticker if ticker.endswith(".US") else f"{ticker}.US",
        "code": general.get("Code") or ticker.replace(".US", ""),
        
        # Identity
        "name": general.get("Name") or "",
        "exchange": general.get("Exchange") or "",
        "currency_code": general.get("CurrencyCode") or "USD",
        "country_iso": general.get("CountryISO") or "US",
        "country_name": general.get("CountryName") or "USA",
        
        # Classification — stripped and None-normalised at source
        "sector":       sector_raw   or None,
        "industry":     industry_raw or None,
        "gic_sector":   general.get("GicSector"),
        "gic_industry": general.get("GicIndustry"),
        "security_type": general.get("Type") or "Common Stock",
        "asset_type":    general.get("Type") or "Common Stock",  # Alias for ETF filtering
        
        # Company details
        "description": (general.get("Description") or "")[:2000],
        "website": general.get("WebURL"),
        "logo_url": general.get("LogoURL"),
        "full_time_employees": general.get("FullTimeEmployees"),
        "ipo_date": general.get("IPODate"),
        "fiscal_year_end": general.get("FiscalYearEnd"),
        "is_delisted": general.get("IsDelisted") or False,
        
        # Address
        "address": general.get("Address"),
        "city": address.get("City"),
        "state": address.get("State"),
        "zip_code": address.get("ZIP"),
        
        # RAW-FACTS policy: provider-computed metrics are intentionally not stored
        # in production cache and stay available only in provider_debug_snapshot.
        "market_cap": None,
        "enterprise_value": None,
        "pe_ratio": None,
        "eps_ttm": eps_ttm,
        "ps_ratio": None,
        "pb_ratio": None,
        "ev_ebitda": None,
        "ev_revenue": None,
        "peg_ratio": None,
        "forward_pe": None,
        "trailing_pe": None,
        
        # Profitability
        "profit_margin": None,
        "operating_margin": None,
        "gross_margin": None,
        "net_margin_ttm": None,
        "roe": None,
        "roa": None,
        
        # Growth
        "revenue_ttm": None,
        "revenue_per_share": None,
        "quarterly_revenue_growth": None,
        "quarterly_earnings_growth": None,
        
        # Dividends
        "dividend_yield": None,
        "dividend_yield_ttm": None,
        "dividend_share": None,
        "forward_dividend_rate": splits_div.get("ForwardAnnualDividendRate"),
        "forward_dividend_yield": splits_div.get("ForwardAnnualDividendYield"),
        "payout_ratio": splits_div.get("PayoutRatio"),
        "ex_dividend_date": splits_div.get("ExDividendDate"),
        "dividend_date": splits_div.get("DividendDate"),
        
        # Shares
        "shares_outstanding": shares.get("SharesOutstanding"),
        "shares_float": shares.get("SharesFloat"),
        "pct_insiders": shares.get("PercentInsiders"),
        "pct_institutions": shares.get("PercentInstitutions"),
        
        # Technicals
        "beta": None,
        "fifty_two_week_high": None,
        "fifty_two_week_low": None,
        "fifty_day_ma": None,
        "two_hundred_day_ma": None,
        
        # Book value
        "book_value": None,
        "ebitda": None,
        
        # Price (set by price sync)
        "price_last_close": None,
        "price_updated_at": None,
        
        # Metadata
        "eodhd_updated_at": general.get("UpdatedAt"),
        "created_at": now,
        "updated_at": now,
    }


def parse_financials(ticker: str, data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Parse EODHD financials into normalized rows.
    One row per (ticker, period_type, period_date).

    EODHD payload keys: "yearly" and "quarterly" (NOT "annual").
    period_type stored in DB: "annual" (from "yearly") and "quarterly".
    """
    financials = data.get("Financials", {})
    rows = []
    now = datetime.now(timezone.utc)
    ticker_full = ticker if ticker.endswith(".US") else f"{ticker}.US"

    # EODHD uses "yearly" in the payload; we normalise to "annual" in the DB.
    eodhd_to_db_period = {"yearly": "annual", "quarterly": "quarterly"}

    for eodhd_period_key, db_period_type in eodhd_to_db_period.items():
        # Income Statement
        income  = financials.get("Income_Statement", {}).get(eodhd_period_key, {})
        balance = financials.get("Balance_Sheet",    {}).get(eodhd_period_key, {})
        cashflow = financials.get("Cash_Flow",        {}).get(eodhd_period_key, {})

        # Get all dates from all statements
        all_dates = set()
        all_dates.update(income.keys())
        all_dates.update(balance.keys())
        all_dates.update(cashflow.keys())

        for period_date in sorted(all_dates, reverse=True)[:20]:  # Keep last 20 periods
            if not period_date or period_date == "0":
                continue

            inc = income.get(period_date) or {}
            bal = balance.get(period_date) or {}
            cf  = cashflow.get(period_date) or {}

            row = {
                "ticker": ticker_full,
                "period_type": db_period_type,
                "period_date": period_date,

                # Income Statement
                "revenue":             inc.get("totalRevenue"),
                "cost_of_revenue":     inc.get("costOfRevenue"),
                "gross_profit":        inc.get("grossProfit"),
                "operating_income":    inc.get("operatingIncome"),
                "operating_expenses":  inc.get("totalOperatingExpenses"),
                "net_income":          inc.get("netIncome"),
                "ebitda":              inc.get("ebitda"),
                "ebit":                inc.get("ebit"),
                "interest_expense":    inc.get("interestExpense"),
                "income_tax_expense":  inc.get("incomeTaxExpense"),
                "diluted_eps":         inc.get("dilutedEPS"),

                # Balance Sheet
                "total_assets":              bal.get("totalAssets"),
                "total_liabilities":         bal.get("totalLiab"),
                "total_equity":              bal.get("totalStockholderEquity"),
                "total_debt":                bal.get("shortLongTermDebt"),
                "cash_and_equivalents":      bal.get("cash"),
                "short_term_investments":    bal.get("shortTermInvestments"),
                "total_current_assets":      bal.get("totalCurrentAssets"),
                "total_current_liabilities": bal.get("totalCurrentLiabilities"),
                "retained_earnings":         bal.get("retainedEarnings"),

                # Cash Flow
                "operating_cash_flow":   cf.get("totalCashFromOperatingActivities"),
                "investing_cash_flow":   cf.get("totalCashflowsFromInvestingActivities"),
                "financing_cash_flow":   cf.get("totalCashFromFinancingActivities"),
                "capital_expenditures":  cf.get("capitalExpenditures"),
                "free_cash_flow":        cf.get("freeCashFlow"),
                "dividends_paid":        cf.get("dividendsPaid"),

                "created_at": now,
                "updated_at": now,
            }

            # Only add if we have some financial data (not just metadata fields)
            _meta = {"ticker", "period_type", "period_date", "created_at", "updated_at"}
            if any(v is not None for k, v in row.items() if k not in _meta):
                rows.append(row)

    return rows


def parse_earnings_history(ticker: str, data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Parse EODHD earnings history into normalized rows.
    One row per quarter.
    """
    earnings = data.get("Earnings", {}).get("History", {})
    rows = []
    now = datetime.now(timezone.utc)
    ticker_full = ticker if ticker.endswith(".US") else f"{ticker}.US"
    
    for key, quarter in earnings.items():
        report_date = quarter.get("reportDate")
        if not report_date:
            continue
        
        reported_eps = quarter.get("epsActual")
        estimated_eps = quarter.get("epsEstimate")
        
        # Calculate surprise
        surprise_pct = None
        if reported_eps is not None and estimated_eps is not None and estimated_eps != 0:
            surprise_pct = ((reported_eps - estimated_eps) / abs(estimated_eps)) * 100
        elif quarter.get("surprisePercent") is not None:
            surprise_pct = quarter.get("surprisePercent")
        
        rows.append({
            "ticker": ticker_full,
            "quarter_date": report_date,
            "reported_eps": reported_eps,
            "estimated_eps": estimated_eps,
            "eps_difference": quarter.get("epsDifference"),
            "surprise_pct": surprise_pct,
            "beat_miss": "beat" if (surprise_pct and surprise_pct > 0) else ("miss" if (surprise_pct and surprise_pct < 0) else None),
            "created_at": now,
            "updated_at": now,
        })
    
    # Sort by date descending and keep last 32 quarters (8 years)
    rows.sort(key=lambda x: x["quarter_date"], reverse=True)
    return rows[:32]


def parse_insider_activity(ticker: str, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Parse EODHD insider transactions and aggregate into 6-month summary.
    """
    transactions = data.get("InsiderTransactions", {})
    now = datetime.now(timezone.utc)
    ticker_full = ticker if ticker.endswith(".US") else f"{ticker}.US"
    
    if not transactions:
        return None
    
    six_months_ago = now - timedelta(days=180)
    
    buyers = set()
    sellers = set()
    total_buy_value = 0
    total_sell_value = 0
    total_buy_shares = 0
    total_sell_shares = 0
    last_activity_date = None
    
    for key, txn in transactions.items():
        txn_date_str = txn.get("transactionDate") or txn.get("date")
        if not txn_date_str:
            continue
        
        try:
            txn_date = datetime.strptime(txn_date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except:
            continue
        
        # Only last 6 months
        if txn_date < six_months_ago:
            continue
        
        # Track last activity
        if last_activity_date is None or txn_date_str > last_activity_date:
            last_activity_date = txn_date_str
        
        owner_name = txn.get("ownerName", "Unknown")
        txn_code = txn.get("transactionCode", "").upper()
        amount = txn.get("transactionAmount") or 0
        price = txn.get("transactionPrice") or 0
        acquired = txn.get("transactionAcquiredDisposed", "").upper()
        
        # Determine buy vs sell
        is_buy = txn_code == "P" or acquired == "A"
        is_sell = txn_code == "S" or acquired == "D"
        
        if is_buy and amount > 0:
            buyers.add(owner_name)
            total_buy_shares += amount
            total_buy_value += amount * price
        elif is_sell and amount > 0:
            sellers.add(owner_name)
            total_sell_shares += amount
            total_sell_value += amount * price
    
    net_value = total_buy_value - total_sell_value
    
    # Determine status
    if net_value > 10000:  # Significant net buying
        status = "net_buying"
    elif net_value < -10000:  # Significant net selling
        status = "net_selling"
    else:
        status = "neutral"
    
    return {
        "ticker": ticker_full,
        "buyers_count": len(buyers),
        "sellers_count": len(sellers),
        "total_buy_shares_6m": total_buy_shares,
        "total_sell_shares_6m": total_sell_shares,
        "total_buy_value_6m": total_buy_value,
        "total_sell_value_6m": total_sell_value,
        "net_value_6m": net_value,
        "avg_buy_price": total_buy_value / total_buy_shares if total_buy_shares > 0 else None,
        "avg_sell_price": total_sell_value / total_sell_shares if total_sell_shares > 0 else None,
        "last_activity_date": last_activity_date,
        "status": status,
        "created_at": now,
        "updated_at": now,
    }


async def sync_ticker_fundamentals(
    db,
    ticker: str,
    force: bool = False
) -> Dict[str, Any]:
    """
    Fetch and store fundamentals for a single ticker.
    
    Updates:
    - company_fundamentals_cache
    - financials_cache
    - earnings_history_cache
    - insider_activity_cache
    
    Also activates ticker in tracked_tickers if successful.
    """
    ticker_upper = ticker.upper()
    ticker_full = ticker_upper if ticker_upper.endswith(".US") else f"{ticker_upper}.US"
    now = datetime.now(timezone.utc)
    
    result = {
        "ticker": ticker_full,
        "success": False,
        "company_fundamentals": False,
        "financials": 0,
        "earnings": 0,
        "insider_activity": False,
        "error": None,
    }
    
    # Fetch from EODHD
    data = await fetch_fundamentals_from_eodhd(ticker_upper)
    
    if not data:
        result["error"] = "No fundamentals data from EODHD"
        return result
    
    try:
        await upsert_provider_debug_snapshot(
            db=db,
            ticker=ticker_full,
            raw_payload=data,
            source_job="manual_fundamentals_sync",
        )

        # 1. Parse and store company fundamentals
        company_doc = parse_company_fundamentals(ticker_upper, data)
        await db.company_fundamentals_cache.update_one(
            {"ticker": ticker_full},
            {"$set": company_doc},
            upsert=True
        )
        result["company_fundamentals"] = True
        
        # 2. Parse and store financials
        financials_rows = parse_financials(ticker_upper, data)
        if financials_rows:
            # Delete old data for this ticker
            await db.financials_cache.delete_many({"ticker": ticker_full})
            # Insert new
            await db.financials_cache.insert_many(financials_rows)
            result["financials"] = len(financials_rows)
        
        # 3. Parse and store earnings history
        earnings_rows = parse_earnings_history(ticker_upper, data)
        if earnings_rows:
            await db.earnings_history_cache.delete_many({"ticker": ticker_full})
            await db.earnings_history_cache.insert_many(earnings_rows)
            result["earnings"] = len(earnings_rows)
        
        # 4. Parse and store insider activity
        insider_doc = parse_insider_activity(ticker_upper, data)
        if insider_doc:
            await db.insider_activity_cache.update_one(
                {"ticker": ticker_full},
                {"$set": insider_doc},
                upsert=True
            )
            result["insider_activity"] = True
        
        # 5. Activate ticker in tracked_tickers
        sector   = (company_doc.get("sector")   or "").strip()
        industry = (company_doc.get("industry") or "").strip()
        has_classification = bool(sector and industry)

        await db.tracked_tickers.update_one(
            {"ticker": ticker_full},
            {
                "$set": {
                    "status": "active",
                    "is_active": True,
                    "name": company_doc.get("name"),
                    "sector":            sector   or None,
                    "industry":          industry or None,
                    "has_classification": has_classification,
                    "fundamentals_updated_at": now,
                    "updated_at": now,
                }
            },
            upsert=True
        )
        
        result["success"] = True
        logger.info(f"Synced fundamentals for {ticker_full}: financials={result['financials']}, earnings={result['earnings']}")
        
    except Exception as e:
        result["error"] = str(e)
        logger.error(f"Error syncing fundamentals for {ticker_full}: {e}")
    
    return result


async def sync_pilot_fundamentals(db, dry_run: bool = False) -> Dict[str, Any]:
    """
    Sync fundamentals for pilot batch of 15 tickers.
    """
    result = {
        "started_at": datetime.now(timezone.utc).isoformat(),
        "dry_run": dry_run,
        "tickers": PILOT_TICKERS,
        "total": len(PILOT_TICKERS),
        "success": 0,
        "failed": 0,
        "results": [],
        "api_calls_used": 0,
    }
    
    if dry_run:
        result["message"] = f"Would sync {len(PILOT_TICKERS)} tickers"
        result["finished_at"] = datetime.now(timezone.utc).isoformat()
        return result
    
    for ticker in PILOT_TICKERS:
        ticker_result = await sync_ticker_fundamentals(db, ticker)
        result["results"].append(ticker_result)
        result["api_calls_used"] += 10  # Each fundamentals call = 10 credits
        
        if ticker_result["success"]:
            result["success"] += 1
        else:
            result["failed"] += 1
    
    result["finished_at"] = datetime.now(timezone.utc).isoformat()
    return result


async def get_fundamentals_stats(db) -> Dict[str, Any]:
    """Get statistics about fundamentals data."""
    company_count = await db.company_fundamentals_cache.count_documents({})
    financials_count = await db.financials_cache.count_documents({})
    earnings_count = await db.earnings_history_cache.count_documents({})
    insider_count = await db.insider_activity_cache.count_documents({})
    
    # Unique tickers in each table
    company_tickers = await db.company_fundamentals_cache.distinct("ticker")
    financials_tickers = await db.financials_cache.distinct("ticker")
    earnings_tickers = await db.earnings_history_cache.distinct("ticker")
    insider_tickers = await db.insider_activity_cache.distinct("ticker")
    
    return {
        "company_fundamentals_cache": {
            "rows": company_count,
            "tickers": len(company_tickers),
        },
        "financials_cache": {
            "rows": financials_count,
            "tickers": len(financials_tickers),
        },
        "earnings_history_cache": {
            "rows": earnings_count,
            "tickers": len(earnings_tickers),
        },
        "insider_activity_cache": {
            "rows": insider_count,
            "tickers": len(insider_tickers),
        },
    }
