# ==============================================================================
# 🛡️ SCHEDULER-ONLY FILE - EXTERNAL API CALLS ALLOWED
# ==============================================================================
# This file is in ALLOWLIST: /app/scripts/audit_external_calls.py
# May call: eodhd.com/api/* (via httpx)
# Context: ONLY from scheduler.py jobs or admin backfill endpoints
# Runtime API endpoints MUST NOT import from this file
# ==============================================================================
#
# ================================================================================
# UNIVERSE SYSTEM — PERMANENT & BINDING FOR ALL FUTURE INSTANCES
# ================================================================================
# ALLOWED EODHD API ENDPOINTS (ONLY THESE 3):
# 1. SEED:         https://eodhd.com/api/exchange-symbol-list/{NYSE|NASDAQ}
# 2. PRICES:       https://eodhd.com/api/eod-bulk-last-day/US
# 3. FUNDAMENTALS: https://eodhd.com/api/fundamentals/{TICKER}.US  <-- THIS SERVICE
#
# VISIBLE UNIVERSE RULE:
# is_visible = is_seeded && has_price_data && has_classification
#
# APP RUNTIME NEVER CALLS EODHD. All data comes from MongoDB only.
# This service is ONLY called by scheduler/backfill jobs.
#
# Any deviation requires explicit written approval from Richard (kurtarichard@gmail.com).
# ================================================================================

"""
RICHSTOX Raw Facts Fundamentals Backfill
=========================================
ONE-TIME backfill storing ONLY RAW FACTS from EODHD fundamentals.

EODHD ENDPOINT: https://eodhd.com/api/fundamentals/{TICKER}.US

=============================================================================
RAW FACTS ONLY. No precomputed metrics.
=============================================================================

STORED:
  - Identity: symbol, primary_ticker, name, exchange, country_iso, currency_code
  - Classification: sector, industry, gic_* fields
  - Display: logo_url, website, description, ipo_date, employees
  - RAW Financial Statements: Income Statement, Balance Sheet, Cash Flow
  - Shares Outstanding History

NOT STORED (must be computed locally):
  - P/E, PEG, forward PE, trailing PE
  - Dividend yield, payout ratio
  - Margins (profit, operating, gross)
  - ROE, ROA, beta
  - 52-week high/low, moving averages (50/200 MA)
  - Any valuation or technical precomputed fields

=============================================================================
"""

import asyncio
import os
import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional
import httpx
from motor.motor_asyncio import AsyncIOMotorClient

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("backfill_raw")

MONGO_URL = os.environ.get("MONGO_URL", "mongodb://localhost:27017")
DB_NAME = os.environ.get("DB_NAME", "richstox")
EODHD_API_KEY = os.environ.get("EODHD_API_KEY", "")

REQUESTS_PER_SECOND = 5
BATCH_LOG_INTERVAL = 100

# =============================================================================
# RAW FACTS ONLY. No precomputed metrics.
# FORBIDDEN FIELDS - these must be computed locally, never stored from EODHD:
#   PERatio, PEGRatio, ForwardPE, TrailingPE, DividendYield, PayoutRatio,
#   ProfitMargin, OperatingMarginTTM, GrossProfitTTM, ReturnOnEquityTTM,
#   ReturnOnAssetsTTM, Beta, 52WeekHigh, 52WeekLow, 50DayMA, 200DayMA,
#   ShortRatio, ShortPercent, WallStreetTargetPrice, any Valuation fields
# =============================================================================

FORBIDDEN_FIELDS = {
    # Valuation metrics - compute locally
    "PERatio", "PEGRatio", "ForwardPE", "TrailingPE", "PriceBookMRQ",
    "PriceSalesTTM", "EnterpriseValue", "EnterpriseValueRevenue", 
    "EnterpriseValueEbitda",
    
    # Dividend metrics - compute locally from dividend history
    "DividendYield", "PayoutRatio", "ForwardAnnualDividendYield",
    
    # Profitability metrics - compute locally from financials
    "ProfitMargin", "OperatingMarginTTM", "GrossProfitTTM",
    "ReturnOnEquityTTM", "ReturnOnAssetsTTM", "EBITDA",
    
    # Technical metrics - compute locally from price data
    "Beta", "52WeekHigh", "52WeekLow", "50DayMA", "200DayMA",
    "ShortRatio", "ShortPercent", "SharesShort", "SharesShortPriorMonth",
    
    # Other precomputed
    "WallStreetTargetPrice", "MarketCapitalization", "MarketCapitalizationMln",
    "BookValue", "EarningsShare", "DilutedEpsTTM", "RevenueTTM",
    "RevenuePerShareTTM", "QuarterlyRevenueGrowthYOY", "QuarterlyEarningsGrowthYOY",
}


def extract_raw_facts(data: Dict[str, Any], ticker: str) -> Optional[Dict[str, Any]]:
    """
    Extract ONLY raw facts from EODHD fundamentals response.
    
    RAW FACTS ONLY. No precomputed metrics.
    All valuation/technical metrics must be computed locally.
    """
    if not data or not isinstance(data, dict):
        return None
    
    general = data.get("General") or {}
    if not general:
        return None
    
    # Base symbol (without .US suffix)
    symbol = ticker.replace(".US", "").upper()
    
    # Get raw financial statements
    financials = data.get("Financials") or {}
    earnings = data.get("Earnings") or {}
    outstanding_shares = data.get("outstandingShares") or {}
    
    result = {
        # =================================================================
        # A) IDENTITY & MAPPING (required)
        # =================================================================
        "symbol": symbol,
        "primary_ticker": general.get("PrimaryTicker", ticker),
        "name": general.get("Name", ""),
        "exchange": general.get("Exchange", ""),
        "country_iso": general.get("CountryISO", "US"),
        "currency_code": general.get("CurrencyCode", "USD"),
        "asset_type": general.get("Type", ""),
        "is_delisted": general.get("IsDelisted", False),
        "delisted_date": general.get("DelistedDate"),
        
        # =================================================================
        # B) CLASSIFICATION (required for filters)
        # =================================================================
        "sector": general.get("Sector", ""),
        "industry": general.get("Industry", ""),
        "gic_sector": general.get("GicSector", ""),
        "gic_group": general.get("GicGroup", ""),
        "gic_industry": general.get("GicIndustry", ""),
        "gic_sub_industry": general.get("GicSubIndustry", ""),
        
        # =================================================================
        # C) DISPLAY FACTS (required for UI)
        # =================================================================
        "logo_url": general.get("LogoURL", ""),
        "website": general.get("WebURL", ""),
        "description": general.get("Description", ""),
        "ipo_date": general.get("IPODate", ""),
        "employees": general.get("FullTimeEmployees"),
        "address": general.get("Address", ""),
        "phone": general.get("Phone", ""),
        "fiscal_year_end": general.get("FiscalYearEnd", ""),
        
        # Identifiers (optional, for data linking)
        "cik": general.get("CIK", ""),
        "cusip": general.get("CUSIP", ""),
        "isin": general.get("ISIN", ""),
        
        # =================================================================
        # D) RAW FINANCIAL STATEMENTS (required for local calculations)
        # Store as-is, no derived metrics
        # =================================================================
        
        # Income Statement - raw data for computing margins, EPS, etc.
        "income_statement_quarterly": financials.get("Income_Statement", {}).get("quarterly", {}),
        "income_statement_yearly": financials.get("Income_Statement", {}).get("yearly", {}),
        
        # Balance Sheet - raw data for computing book value, equity ratios, etc.
        "balance_sheet_quarterly": financials.get("Balance_Sheet", {}).get("quarterly", {}),
        "balance_sheet_yearly": financials.get("Balance_Sheet", {}).get("yearly", {}),
        
        # Cash Flow - raw data for free cash flow, etc.
        "cash_flow_quarterly": financials.get("Cash_Flow", {}).get("quarterly", {}),
        "cash_flow_yearly": financials.get("Cash_Flow", {}).get("yearly", {}),
        
        # Shares Outstanding History - for computing market cap
        "shares_outstanding_annual": outstanding_shares.get("annual", []),
        "shares_outstanding_quarterly": outstanding_shares.get("quarterly", []),
        
        # Earnings History (raw EPS data for computing P/E locally)
        "earnings_history": earnings.get("History", {}),
        "earnings_annual": earnings.get("Annual", {}),
        
        # =================================================================
        # META
        # =================================================================
        "last_updated_at": datetime.now(timezone.utc),
        "data_source": "EODHD",
        "backfill_version": "raw_facts_v1",
    }
    
    return result


def extract_tracked_ticker_fields(raw_data: Dict[str, Any]) -> Dict[str, Any]:
    """Extract minimal fields for tracked_tickers collection."""
    return {
        "sector": raw_data.get("sector", ""),
        "industry": raw_data.get("industry", ""),
        "name": raw_data.get("name", ""),
        "logo_url": raw_data.get("logo_url", ""),
        "website": raw_data.get("website", ""),
        "ipo_date": raw_data.get("ipo_date", ""),
        "asset_type": raw_data.get("asset_type", ""),
        "exchange": raw_data.get("exchange", ""),
        "fundamentals_status": "raw_facts",
        "fundamentals_updated_at": datetime.now(timezone.utc),
    }


async def run_raw_facts_backfill(
    db,
    limit: Optional[int] = None,
    dry_run: bool = False,
    start_from: Optional[str] = None
) -> Dict[str, Any]:
    """
    Run RAW FACTS ONLY fundamentals backfill.
    
    =============================================================================
    RAW FACTS ONLY. No precomputed metrics.
    =============================================================================
    """
    if not EODHD_API_KEY:
        raise ValueError("EODHD_API_KEY not configured!")
    
    job_started = datetime.now(timezone.utc)
    job_id = f"backfill_raw_facts_{job_started.strftime('%Y%m%d_%H%M%S')}"
    
    logger.info("=" * 70)
    logger.info("RAW FACTS ONLY BACKFILL - No precomputed metrics")
    logger.info("=" * 70)
    logger.info(f"Job ID: {job_id}")
    logger.info(f"Dry run: {dry_run}, Limit: {limit or 'ALL'}")
    
    # Universe query (binding)
    universe_query = {
        "$or": [
            {"is_whitelisted": True},
            {"is_whitelisted": {"$exists": False}},
        ],
        "asset_type": "Common Stock",
        "exchange": {"$in": ["NYSE", "NASDAQ"]},
        "is_active": True,
    }
    
    if start_from:
        universe_query["ticker"] = {"$gte": start_from}
    
    total_universe = await db.tracked_tickers.count_documents(universe_query)
    logger.info(f"Universe size: {total_universe} tickers")
    
    cursor = db.tracked_tickers.find(
        universe_query,
        {"ticker": 1, "_id": 0}
    ).sort("ticker", 1)
    
    if limit:
        cursor = cursor.limit(limit)
    
    tickers = [doc["ticker"] for doc in await cursor.to_list(length=limit or total_universe)]
    logger.info(f"Will process: {len(tickers)} tickers")
    
    if not tickers:
        return {"status": "nothing_to_do", "job_id": job_id}
    
    stats = {
        "total_tickers": len(tickers),
        "processed": 0,
        "updated": 0,
        "not_found": 0,
        "not_common_stock": 0,
        "failed": 0,
        "api_calls": 0,
    }
    
    if not dry_run:
        await db.ops_job_runs.insert_one({
            "job_type": "backfill_raw_facts",
            "job_id": job_id,
            "started_at": job_started,
            "status": "running",
            "total_tickers": len(tickers),
        })
    
    async with httpx.AsyncClient(timeout=60) as http_client:
        for i, ticker in enumerate(tickers):
            try:
                # Rate limiting
                if i > 0 and i % REQUESTS_PER_SECOND == 0:
                    await asyncio.sleep(1)
                
                # Progress logging
                if i > 0 and i % BATCH_LOG_INTERVAL == 0:
                    elapsed = (datetime.now(timezone.utc) - job_started).total_seconds()
                    rate = i / elapsed if elapsed > 0 else 0
                    remaining = (len(tickers) - i) / rate if rate > 0 else 0
                    logger.info(
                        f"PROGRESS: {i}/{len(tickers)} ({100*i/len(tickers):.1f}%) | "
                        f"Updated: {stats['updated']} | Rate: {rate:.1f}/sec | ETA: {remaining/60:.1f} min"
                    )
                
                ticker_full = ticker if ticker.endswith(".US") else f"{ticker}.US"
                
                url = f"https://eodhd.com/api/fundamentals/{ticker_full}"
                params = {"api_token": EODHD_API_KEY, "fmt": "json"}
                
                response = await http_client.get(url, params=params)
                stats["api_calls"] += 1
                stats["processed"] += 1
                
                if response.status_code == 404:
                    stats["not_found"] += 1
                    continue
                
                if response.status_code != 200:
                    stats["failed"] += 1
                    continue
                
                data = response.json()
                
                # Extract RAW FACTS ONLY
                raw_facts = extract_raw_facts(data, ticker_full)
                
                if not raw_facts:
                    stats["not_found"] += 1
                    continue
                
                # Check asset type
                asset_type = raw_facts.get("asset_type", "")
                if asset_type and asset_type != "Common Stock":
                    stats["not_common_stock"] += 1
                    if not dry_run:
                        await db.tracked_tickers.update_one(
                            {"ticker": ticker},
                            {"$set": {
                                "asset_type": asset_type,
                                "is_active": False,
                                "deactivation_reason": f"asset_type={asset_type}",
                                "updated_at": datetime.now(timezone.utc),
                            }}
                        )
                    continue
                
                if not dry_run:
                    # Store raw facts and REMOVE forbidden precomputed metrics
                    symbol = raw_facts["symbol"]
                    
                    # Fields to explicitly remove (forbidden precomputed metrics)
                    unset_fields = {
                        # Valuation metrics
                        "pe_ratio": "", "peg_ratio": "", "forward_pe": "", "trailing_pe": "",
                        "price_book_mrq": "", "price_sales_ttm": "",
                        "enterprise_value": "", "enterprise_value_revenue": "", "enterprise_value_ebitda": "",
                        # Dividend metrics
                        "dividend_yield": "", "payout_ratio": "", "forward_annual_dividend_yield": "",
                        "forward_annual_dividend_rate": "",
                        # Profitability metrics
                        "profit_margin": "", "operating_margin_ttm": "", "gross_profit_ttm": "",
                        "return_on_equity_ttm": "", "return_on_assets_ttm": "", "ebitda": "",
                        # Technical metrics
                        "beta": "", "52_week_high": "", "52_week_low": "", "50_day_ma": "", "200_day_ma": "",
                        "short_ratio": "", "short_percent": "", "shares_short": "", "shares_short_prior_month": "",
                        "short_ratio_stats": "", "short_percent_outstanding": "",
                        # Other precomputed
                        "wall_street_target_price": "", "market_cap": "", "market_cap_mln": "",
                        "book_value": "", "eps": "", "eps_ttm": "", "diluted_eps_ttm": "",
                        "revenue_ttm": "", "revenue_per_share_ttm": "",
                        "quarterly_revenue_growth_yoy": "", "quarterly_earnings_growth_yoy": "",
                        # Old complete backfill fields
                        "analyst_target_price": "", "analyst_strong_buy": "", "analyst_buy": "",
                        "analyst_hold": "", "analyst_sell": "", "analyst_strong_sell": "", "analyst_rating": "",
                        "esg_total_score": "", "esg_environment_score": "", "esg_social_score": "", "esg_governance_score": "",
                        "eps_estimate_current_year": "", "eps_estimate_next_year": "",
                        "eps_estimate_next_quarter": "", "eps_estimate_current_quarter": "",
                        "dividend_share": "", "dividend_date": "", "ex_dividend_date": "",
                        "last_split_factor": "", "last_split_date": "",
                        "shares_outstanding": "", "shares_float": "",
                        "percent_insiders": "", "percent_institutions": "",
                        "shares_short_stats": "",
                        "institutions_count": "", "funds_count": "",
                        "top_institutional_holders": "", "top_fund_holders": "",
                        "insider_transactions": "",
                        "outstanding_shares_annual": "", "outstanding_shares_quarterly": "",
                        "earnings_trend": "",
                        "most_recent_quarter": "",
                        # Old naming conventions
                        "financials_balance_sheet_quarterly": "", "financials_balance_sheet_yearly": "",
                        "financials_cash_flow_quarterly": "", "financials_cash_flow_yearly": "",
                        "financials_income_statement_quarterly": "", "financials_income_statement_yearly": "",
                    }
                    
                    await db.company_fundamentals_cache.update_one(
                        {"symbol": symbol},
                        {
                            "$set": raw_facts,
                            "$unset": unset_fields,
                        },
                        upsert=True
                    )
                    
                    # Update tracked_tickers (minimal)
                    tracked_fields = extract_tracked_ticker_fields(raw_facts)
                    await db.tracked_tickers.update_one(
                        {"ticker": ticker},
                        {"$set": tracked_fields}
                    )
                
                stats["updated"] += 1
                
            except Exception as e:
                stats["failed"] += 1
                logger.error(f"Error processing {ticker}: {e}")
    
    job_completed = datetime.now(timezone.utc)
    duration_seconds = (job_completed - job_started).total_seconds()
    
    logger.info("=" * 70)
    logger.info(f"RAW FACTS BACKFILL COMPLETE: {job_id}")
    logger.info(f"Duration: {duration_seconds:.1f}s ({duration_seconds/60:.1f} min)")
    logger.info(f"Stats: {stats}")
    logger.info("=" * 70)
    
    if not dry_run:
        await db.ops_job_runs.update_one(
            {"job_id": job_id},
            {"$set": {
                "completed_at": job_completed,
                "duration_seconds": duration_seconds,
                "status": "completed",
                "stats": stats,
            }}
        )
    
    return {
        "status": "completed",
        "job_id": job_id,
        "duration_seconds": duration_seconds,
        "stats": stats,
    }


async def verify_raw_facts_backfill(db) -> Dict[str, Any]:
    """Verify raw facts backfill results."""
    universe_query = {
        "$or": [
            {"is_whitelisted": True},
            {"is_whitelisted": {"$exists": False}},
        ],
        "asset_type": "Common Stock",
        "exchange": {"$in": ["NYSE", "NASDAQ"]},
        "is_active": True,
    }
    
    universe_count = await db.tracked_tickers.count_documents(universe_query)
    
    missing_sector = await db.tracked_tickers.count_documents({
        **universe_query,
        "$or": [{"sector": None}, {"sector": ""}, {"sector": {"$exists": False}}]
    })
    
    missing_industry = await db.tracked_tickers.count_documents({
        **universe_query,
        "$or": [{"industry": None}, {"industry": ""}, {"industry": {"$exists": False}}]
    })
    
    missing_logo = await db.tracked_tickers.count_documents({
        **universe_query,
        "$or": [{"logo_url": None}, {"logo_url": ""}, {"logo_url": {"$exists": False}}]
    })
    
    raw_facts_count = await db.company_fundamentals_cache.count_documents({
        "backfill_version": "raw_facts_v1"
    })
    
    # Check that forbidden fields are not present
    has_forbidden = await db.company_fundamentals_cache.count_documents({
        "backfill_version": "raw_facts_v1",
        "$or": [
            {"pe_ratio": {"$exists": True}},
            {"beta": {"$exists": True}},
            {"dividend_yield": {"$exists": True}},
            {"52_week_high": {"$exists": True}},
            {"profit_margin": {"$exists": True}},
        ]
    })
    
    return {
        "universe_count": universe_count,
        "missing_sector": missing_sector,
        "missing_industry": missing_industry,
        "missing_logo_url": missing_logo,
        "raw_facts_cached": raw_facts_count,
        "has_forbidden_fields": has_forbidden,
        "forbidden_fields_check": "PASS" if has_forbidden == 0 else "FAIL",
    }


if __name__ == "__main__":
    import argparse
    import json
    
    parser = argparse.ArgumentParser(description="Raw facts fundamentals backfill")
    parser.add_argument("--limit", type=int, help="Limit number of tickers")
    parser.add_argument("--dry-run", action="store_true", help="Don't write to DB")
    parser.add_argument("--start-from", type=str, help="Start from ticker")
    parser.add_argument("--verify-only", action="store_true", help="Only verify")
    args = parser.parse_args()
    
    async def main():
        client = AsyncIOMotorClient(MONGO_URL)
        db = client[DB_NAME]
        
        if args.verify_only:
            result = await verify_raw_facts_backfill(db)
        else:
            result = await run_raw_facts_backfill(
                db, limit=args.limit, dry_run=args.dry_run, start_from=args.start_from
            )
            if not args.dry_run:
                verify = await verify_raw_facts_backfill(db)
                result["verification"] = verify
        
        print(json.dumps(result, indent=2, default=str))
        client.close()
    
    asyncio.run(main())
