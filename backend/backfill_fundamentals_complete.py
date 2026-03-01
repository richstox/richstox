# ==============================================================================
# 🛡️ SCHEDULER-ONLY FILE - EXTERNAL API CALLS ALLOWED
# ==============================================================================
# This file is in ALLOWLIST: /app/scripts/audit_external_calls.py
# May call: eodhd.com/api/* (via httpx)
# Context: ONLY from scheduler.py jobs or admin backfill endpoints
# Runtime API endpoints MUST NOT import from this file
# ==============================================================================
"""
RICHSTOX Complete Fundamentals Backfill
========================================
ONE-TIME comprehensive backfill to fetch and store ALL fundamentals data.
This script fetches EVERYTHING from EODHD fundamentals endpoint.

Cost: 1 ticker = 10 API credits. ~6096 tickers = 60,960 credits.
DO NOT re-run unless absolutely necessary.

Usage:
    python backfill_fundamentals_complete.py [--limit N] [--dry-run]
    
Or via API:
    POST /api/admin/backfill-fundamentals-complete
"""

import asyncio
import os
import sys
import argparse
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
import httpx
from motor.motor_asyncio import AsyncIOMotorClient

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("backfill_complete")

# P0 Phase 3: Use unified config module (no fallback defaults for production safety)
from config import get_mongo_url, get_db_name, EODHD_API_KEY as CONFIG_EODHD_KEY

MONGO_URL = get_mongo_url()
DB_NAME = get_db_name()
EODHD_API_KEY = CONFIG_EODHD_KEY or os.environ.get("EODHD_API_KEY", "")

# Rate limiting - EODHD allows 5 req/sec
REQUESTS_PER_SECOND = 5
BATCH_LOG_INTERVAL = 100


def extract_all_fundamentals(data: Dict[str, Any], ticker: str) -> Optional[Dict[str, Any]]:
    """
    Extract ALL available fields from EODHD fundamentals response.
    This is the COMPLETE extraction - nothing should be missing.
    
    Returns None if data is invalid or empty.
    """
    if not data or not isinstance(data, dict):
        return None
    
    # Safely get all sections with empty dict defaults
    general = data.get("General") or {}
    highlights = data.get("Highlights") or {}
    technicals = data.get("Technicals") or {}
    valuation = data.get("Valuation") or {}
    shares_stats = data.get("SharesStats") or {}
    splits_dividends = data.get("SplitsDividends") or {}
    analyst_ratings = data.get("AnalystRatings") or {}
    holders = data.get("Holders") or {}
    insider_transactions = data.get("InsiderTransactions") or {}
    esg_scores = data.get("ESGScores") or {}
    outstandingShares = data.get("outstandingShares") or {}
    earnings = data.get("Earnings") or {}
    financials = data.get("Financials") or {}
    
    # If no General section, this ticker has no useful data
    if not general:
        return None
    
    # Base symbol (without .US suffix)
    symbol = ticker.replace(".US", "").upper()
    
    # ==========================================================================
    # P0 PHASE 3: RAW-INPUTS-ONLY STORAGE
    # ==========================================================================
    # BINDING RULE: We compute EVERYTHING that is computable by us.
    # Store ONLY raw inputs needed for local calculations.
    # DO NOT store provider-computed metrics/ratios/aggregates.
    # ==========================================================================
    
    result = {
        # === IDENTIFIERS (Primary Key) ===
        "symbol": symbol,  # Canonical key (AAPL, not AAPL.US)
        "ticker": ticker,  # Full ticker with suffix (AAPL.US)
        
        # === A) ENTITY PROFILE (Static Facts) ===
        "code": general.get("Code", symbol),
        "name": general.get("Name", ""),
        "primary_ticker": general.get("PrimaryTicker", ""),
        "exchange": general.get("Exchange", ""),
        "currency_code": general.get("CurrencyCode", "USD"),
        "currency_name": general.get("CurrencyName", ""),
        "currency_symbol": general.get("CurrencySymbol", ""),
        "country_name": general.get("CountryName", ""),
        "country_iso": general.get("CountryISO", "US"),
        
        # Asset Type - CRITICAL for filtering
        "asset_type": general.get("Type", ""),
        
        # Sector/Industry + GICS
        "sector": general.get("Sector", ""),
        "industry": general.get("Industry", ""),
        "gic_sector": general.get("GicSector", ""),
        "gic_group": general.get("GicGroup", ""),
        "gic_industry": general.get("GicIndustry", ""),
        "gic_sub_industry": general.get("GicSubIndustry", ""),
        
        # Description, website, logo
        "description": general.get("Description", ""),
        "logo_url": general.get("LogoURL", ""),
        "website": general.get("WebURL", ""),
        
        # Full HQ/Contact
        "address": general.get("Address", ""),
        "phone": general.get("Phone", ""),
        "full_time_employees": general.get("FullTimeEmployees"),
        
        # Listing Info
        "fiscal_year_end": general.get("FiscalYearEnd", ""),
        "ipo_date": general.get("IPODate", ""),
        "is_delisted": general.get("IsDelisted", False),
        "delisted_date": general.get("DelistedDate"),
        "home_category": general.get("HomeCategory", ""),
        
        # Identifiers (ISIN, CUSIP, CIK, LEI)
        "cik": general.get("CIK", ""),
        "cusip": general.get("CUSIP", ""),
        "isin": general.get("ISIN", ""),
        "lei": general.get("LEI", ""),
        "employer_id": general.get("EmployerIdNumber", ""),
        
        # 1) OFFICERS (raw list) - MUST SAVE
        "officers": list(general.get("Officers", {}).values()) if isinstance(general.get("Officers"), dict) else [],
        
        # === C) SHARES (Raw) ===
        "shares_outstanding": shares_stats.get("SharesOutstanding"),
        "shares_float": shares_stats.get("SharesFloat"),
        
        # === D) CORPORATE ACTIONS (Raw Events) ===
        # Dividend events (raw)
        "dividend_date": splits_dividends.get("DividendDate"),
        "ex_dividend_date": splits_dividends.get("ExDividendDate"),
        "forward_annual_dividend_rate": splits_dividends.get("ForwardAnnualDividendRate"),
        
        # Split events (raw)
        "last_split_factor": splits_dividends.get("LastSplitFactor"),
        "last_split_date": splits_dividends.get("LastSplitDate"),
        
        # === ANALYST RATINGS (Raw counts - not computed) ===
        "analyst_target_price": analyst_ratings.get("TargetPrice"),
        "analyst_strong_buy": analyst_ratings.get("StrongBuy"),
        "analyst_buy": analyst_ratings.get("Buy"),
        "analyst_hold": analyst_ratings.get("Hold"),
        "analyst_sell": analyst_ratings.get("Sell"),
        "analyst_strong_sell": analyst_ratings.get("StrongSell"),
        
        # === ESG SCORES (if available - raw scores) ===
        "esg_total_score": esg_scores.get("TotalEsg") if esg_scores else None,
        "esg_environment_score": esg_scores.get("EnvironmentScore") if esg_scores else None,
        "esg_social_score": esg_scores.get("SocialScore") if esg_scores else None,
        "esg_governance_score": esg_scores.get("GovernanceScore") if esg_scores else None,
        
        # === OUTSTANDING SHARES HISTORY (Raw time series) ===
        "outstanding_shares_annual": outstandingShares.get("annual", []) if outstandingShares else [],
        "outstanding_shares_quarterly": outstandingShares.get("quarterly", []) if outstandingShares else [],
        
        # === HOLDERS (Raw counts + top holders) ===
        "institutions_count": len(holders.get("Institutions", {}).keys()) if holders.get("Institutions") else 0,
        "funds_count": len(holders.get("Funds", {}).keys()) if holders.get("Funds") else 0,
        "top_institutional_holders": list(holders.get("Institutions", {}).values())[:10] if holders.get("Institutions") else [],
        "top_fund_holders": list(holders.get("Funds", {}).values())[:10] if holders.get("Funds") else [],
        
        # === INSIDER TRANSACTIONS (Raw events, last 10) ===
        "insider_transactions": list(insider_transactions.values())[:10] if isinstance(insider_transactions, dict) else [],
        
        # === B) EARNINGS (Raw History + Trend) ===
        "earnings_history": earnings.get("History", {}) if earnings else {},
        "earnings_trend": earnings.get("Trend", {}) if earnings else {},
        "earnings_annual": earnings.get("Annual", {}) if earnings else {},
        
        # === B) FINANCIALS (Raw time series - full statements) ===
        "financials_balance_sheet_quarterly": financials.get("Balance_Sheet", {}).get("quarterly", {}) if financials else {},
        "financials_balance_sheet_yearly": financials.get("Balance_Sheet", {}).get("yearly", {}) if financials else {},
        "financials_cash_flow_quarterly": financials.get("Cash_Flow", {}).get("quarterly", {}) if financials else {},
        "financials_cash_flow_yearly": financials.get("Cash_Flow", {}).get("yearly", {}) if financials else {},
        "financials_income_statement_quarterly": financials.get("Income_Statement", {}).get("quarterly", {}) if financials else {},
        "financials_income_statement_yearly": financials.get("Income_Statement", {}).get("yearly", {}) if financials else {},
        
        # === META ===
        "last_updated_at": datetime.now(timezone.utc),
        "data_source": "EODHD",
        "backfill_version": "raw_inputs_only_v2",  # P0 Phase 3 - with config.py
        
        # ==========================================================================
        # STRIPPED (Provider-computed metrics) - DO NOT STORE:
        # - market_cap, market_cap_mln (we compute: price × shares)
        # - pe_ratio, peg_ratio, trailing_pe, forward_pe (we compute)
        # - pb_ratio, ps_ratio (we compute)
        # - enterprise_value, ev_ebitda, ev_revenue (we compute)
        # - dividend_yield, forward_dividend_yield, payout_ratio (we compute)
        # - profit_margin, operating_margin, roa, roe (we compute from statements)
        # - beta, 52w_high, 52w_low, 50d_ma, 200d_ma (we compute from prices)
        # - eps_ttm, revenue_ttm, ebitda, gross_profit_ttm (we compute from statements)
        # - book_value (we compute from balance sheet)
        # - revenue_per_share, diluted_eps_ttm (we compute)
        # ==========================================================================
    }
    
    return result

# ==============================================================================
# ARCHIVED: Lines 219-326 moved to:
# /app/backend/_archive/backfill_fundamentals_complete_deadcode_219_326.py
# Reason: Unreachable code (after return statement) + RAW-INPUTS-ONLY policy
# ==============================================================================


def extract_tracked_ticker_fields(full_data: Dict[str, Any]) -> Dict[str, Any]:
    """Extract fields to update in tracked_tickers collection."""
    return {
        "sector": full_data.get("sector", ""),
        "industry": full_data.get("industry", ""),
        "name": full_data.get("name", ""),
        "logo_url": full_data.get("logo_url", ""),
        "website": full_data.get("website", ""),
        "ipo_date": full_data.get("ipo_date", ""),
        "asset_type": full_data.get("asset_type", ""),
        "country": full_data.get("country_iso", "US"),
        "is_delisted": full_data.get("is_delisted", False),
        "fundamentals_status": "complete",
        "fundamentals_updated_at": datetime.now(timezone.utc),
    }


async def run_complete_backfill(
    db,
    limit: Optional[int] = None,
    dry_run: bool = False,
    start_from: Optional[str] = None
) -> Dict[str, Any]:
    """
    Run COMPLETE fundamentals backfill for all universe tickers.
    
    Args:
        db: MongoDB database instance
        limit: Optional limit on number of tickers to process
        dry_run: If True, don't write to database
        start_from: Optional ticker to start from (for resuming)
    
    Returns:
        Dict with job status and statistics
    """
    if not EODHD_API_KEY:
        raise ValueError("EODHD_API_KEY not configured!")
    
    job_started = datetime.now(timezone.utc)
    job_id = f"backfill_fundamentals_complete_{job_started.strftime('%Y%m%d_%H%M%S')}"
    
    logger.info("=" * 70)
    logger.info(f"STARTING COMPLETE FUNDAMENTALS BACKFILL: {job_id}")
    logger.info(f"Dry run: {dry_run}, Limit: {limit or 'ALL'}, Start from: {start_from or 'beginning'}")
    logger.info("=" * 70)
    
    # Universe query - all active tickers
    universe_query = {
        "is_active": True,
        "exchange": {"$in": ["NYSE", "NASDAQ"]},
        "asset_type": "Common Stock",
    }
    
    # If start_from is specified, add to query
    if start_from:
        universe_query["ticker"] = {"$gte": start_from}
    
    total_universe = await db.tracked_tickers.count_documents(universe_query)
    logger.info(f"Universe size: {total_universe} tickers")
    
    # Get all tickers to process
    cursor = db.tracked_tickers.find(
        universe_query,
        {"ticker": 1, "_id": 0}
    ).sort("ticker", 1)
    
    if limit:
        cursor = cursor.limit(limit)
    
    tickers = [doc["ticker"] for doc in await cursor.to_list(length=limit or total_universe)]
    logger.info(f"Will process: {len(tickers)} tickers")
    
    if not tickers:
        return {
            "status": "nothing_to_do",
            "message": "No tickers to process",
            "job_id": job_id,
        }
    
    # Statistics
    stats = {
        "total_tickers": len(tickers),
        "processed": 0,
        "updated": 0,
        "not_found": 0,
        "not_common_stock": 0,
        "failed": 0,
        "api_calls": 0,
    }
    
    # P0 Phase 3: Inventory snapshot BEFORE backfill
    inventory_before = None
    if not dry_run:
        inventory_before = {
            "total_fundamentals": await db.company_fundamentals_cache.count_documents({}),
            "with_officers": await db.company_fundamentals_cache.count_documents({"officers": {"$exists": True, "$ne": []}}),
            "raw_inputs_v2": await db.company_fundamentals_cache.count_documents({"backfill_version": "raw_inputs_only_v2"}),
            "with_isin": await db.company_fundamentals_cache.count_documents({"isin": {"$exists": True, "$ne": ""}}),
            "with_statements": await db.company_fundamentals_cache.count_documents({
                "financials_income_statement_yearly": {"$exists": True, "$ne": {}}
            }),
        }
        logger.info(f"Inventory BEFORE: {inventory_before}")
    
    # Store job start in ops_job_runs
    if not dry_run:
        await db.ops_job_runs.insert_one({
            "job_type": "backfill_fundamentals_complete",
            "job_id": job_id,
            "started_at": job_started,
            "status": "running",
            "total_tickers": len(tickers),
            "triggered_by": "manual_admin",
            "inventory_snapshot_before": inventory_before,
        })
    
    # Process tickers
    async with httpx.AsyncClient(timeout=60) as http_client:
        for i, ticker in enumerate(tickers):
            try:
                # Rate limiting: 5 req/sec
                if i > 0 and i % REQUESTS_PER_SECOND == 0:
                    await asyncio.sleep(1)
                
                # Progress logging every N tickers
                if i > 0 and i % BATCH_LOG_INTERVAL == 0:
                    elapsed = (datetime.now(timezone.utc) - job_started).total_seconds()
                    rate = i / elapsed if elapsed > 0 else 0
                    remaining = (len(tickers) - i) / rate if rate > 0 else 0
                    logger.info(
                        f"PROGRESS: {i}/{len(tickers)} ({100*i/len(tickers):.1f}%) | "
                        f"Updated: {stats['updated']} | Not found: {stats['not_found']} | "
                        f"Rate: {rate:.1f}/sec | ETA: {remaining/60:.1f} min"
                    )
                
                # Ensure .US suffix
                ticker_full = ticker if ticker.endswith(".US") else f"{ticker}.US"
                
                # Fetch fundamentals
                url = f"https://eodhd.com/api/fundamentals/{ticker_full}"
                params = {"api_token": EODHD_API_KEY, "fmt": "json"}
                
                response = await http_client.get(url, params=params)
                stats["api_calls"] += 1
                stats["processed"] += 1
                
                if response.status_code == 404:
                    stats["not_found"] += 1
                    logger.debug(f"404 Not found: {ticker_full}")
                    continue
                
                if response.status_code != 200:
                    stats["failed"] += 1
                    logger.warning(f"API error {response.status_code} for {ticker_full}")
                    continue
                
                data = response.json()
                
                if not data or "General" not in data:
                    stats["not_found"] += 1
                    logger.debug(f"No data for {ticker_full}")
                    continue
                
                # Extract ALL fields
                full_fundamentals = extract_all_fundamentals(data, ticker_full)
                
                # Check if Common Stock - if not, mark for deactivation
                asset_type = full_fundamentals.get("asset_type", "")
                if asset_type and asset_type != "Common Stock":
                    stats["not_common_stock"] += 1
                    logger.info(f"NOT Common Stock: {ticker_full} -> {asset_type}")
                    
                    if not dry_run:
                        # Deactivate from universe
                        await db.tracked_tickers.update_one(
                            {"ticker": ticker},
                            {"$set": {
                                "asset_type": asset_type,
                                "is_active": False,  # Remove from universe
                                "deactivation_reason": f"asset_type={asset_type}",
                                "updated_at": datetime.now(timezone.utc),
                            }}
                        )
                    continue
                
                # Store complete fundamentals
                if not dry_run:
                    # Upsert to company_fundamentals_cache using symbol as key
                    symbol = full_fundamentals["symbol"]
                    await db.company_fundamentals_cache.update_one(
                        {"symbol": symbol},
                        {"$set": full_fundamentals},
                        upsert=True
                    )
                    
                    # Update tracked_tickers with essential fields
                    tracked_fields = extract_tracked_ticker_fields(full_fundamentals)
                    await db.tracked_tickers.update_one(
                        {"ticker": ticker},
                        {"$set": tracked_fields}
                    )
                
                stats["updated"] += 1
                
            except Exception as e:
                stats["failed"] += 1
                logger.error(f"Error processing {ticker}: {e}")
    
    # Job completion
    job_completed = datetime.now(timezone.utc)
    duration_seconds = (job_completed - job_started).total_seconds()
    
    logger.info("=" * 70)
    logger.info(f"BACKFILL COMPLETE: {job_id}")
    logger.info(f"Duration: {duration_seconds:.1f}s ({duration_seconds/60:.1f} min)")
    logger.info(f"Stats: {stats}")
    logger.info("=" * 70)
    
    # Update job status
    if not dry_run:
        # P0 Phase 3: Inventory snapshot AFTER backfill
        inventory_after = {
            "total_fundamentals": await db.company_fundamentals_cache.count_documents({}),
            "with_officers": await db.company_fundamentals_cache.count_documents({"officers": {"$exists": True, "$ne": []}}),
            "raw_inputs_v2": await db.company_fundamentals_cache.count_documents({"backfill_version": "raw_inputs_only_v2"}),
            "with_isin": await db.company_fundamentals_cache.count_documents({"isin": {"$exists": True, "$ne": ""}}),
            "with_statements": await db.company_fundamentals_cache.count_documents({
                "financials_income_statement_yearly": {"$exists": True, "$ne": {}}
            }),
        }
        logger.info(f"Inventory AFTER: {inventory_after}")
        
        await db.ops_job_runs.update_one(
            {"job_id": job_id},
            {"$set": {
                "completed_at": job_completed,
                "duration_seconds": duration_seconds,
                "status": "completed",
                "stats": stats,
                "inventory_snapshot_after": inventory_after,
            }}
        )
    
    # Create unique index on company_fundamentals_cache.symbol
    if not dry_run:
        try:
            await db.company_fundamentals_cache.create_index("symbol", unique=True)
            logger.info("Created unique index on company_fundamentals_cache.symbol")
        except Exception as e:
            logger.warning(f"Index already exists or error: {e}")
    
    return {
        "status": "completed",
        "job_id": job_id,
        "duration_seconds": duration_seconds,
        "stats": stats,
    }


async def verify_backfill(db) -> Dict[str, Any]:
    """
    Verify backfill results - report the 5 key metrics.
    """
    universe_query = {
        "is_active": True,
        "exchange": {"$in": ["NYSE", "NASDAQ"]},
        "asset_type": "Common Stock",
    }
    
    universe_count = await db.tracked_tickers.count_documents(universe_query)
    
    # 1. Missing sector
    missing_sector = await db.tracked_tickers.count_documents({
        **universe_query,
        "$or": [
            {"sector": None},
            {"sector": ""},
            {"sector": {"$exists": False}}
        ]
    })
    
    # 2. Missing industry
    missing_industry = await db.tracked_tickers.count_documents({
        **universe_query,
        "$or": [
            {"industry": None},
            {"industry": ""},
            {"industry": {"$exists": False}}
        ]
    })
    
    # 3. Missing logo_url
    missing_logo = await db.tracked_tickers.count_documents({
        **universe_query,
        "$or": [
            {"logo_url": None},
            {"logo_url": ""},
            {"logo_url": {"$exists": False}}
        ]
    })
    
    # 4. Top 20 missing by market cap (from company_fundamentals_cache)
    top_missing = await db.company_fundamentals_cache.find(
        {
            "$or": [
                {"sector": None},
                {"sector": ""},
            ]
        },
        {"symbol": 1, "name": 1, "market_cap": 1, "_id": 0}
    ).sort("market_cap", -1).limit(20).to_list(length=20)
    
    # 5. Total records in company_fundamentals_cache
    total_cache = await db.company_fundamentals_cache.count_documents({})
    
    # Bonus: Check for complete fundamentals
    complete_fundamentals = await db.company_fundamentals_cache.count_documents({
        "backfill_version": "complete_v1"
    })
    
    return {
        "universe_count": universe_count,
        "verification": {
            "missing_sector": {
                "count": missing_sector,
                "percent": round(100 * missing_sector / universe_count, 2) if universe_count > 0 else 0
            },
            "missing_industry": {
                "count": missing_industry,
                "percent": round(100 * missing_industry / universe_count, 2) if universe_count > 0 else 0
            },
            "missing_logo_url": {
                "count": missing_logo,
                "percent": round(100 * missing_logo / universe_count, 2) if universe_count > 0 else 0
            },
            "top_20_missing_by_market_cap": top_missing,
            "total_fundamentals_cached": total_cache,
            "complete_fundamentals_cached": complete_fundamentals,
        }
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Complete fundamentals backfill")
    parser.add_argument("--limit", type=int, help="Limit number of tickers")
    parser.add_argument("--dry-run", action="store_true", help="Don't write to DB")
    parser.add_argument("--start-from", type=str, help="Start from ticker (for resuming)")
    parser.add_argument("--verify-only", action="store_true", help="Only run verification")
    args = parser.parse_args()
    
    async def main():
        client = AsyncIOMotorClient(MONGO_URL)
        db = client[DB_NAME]
        
        if args.verify_only:
            result = await verify_backfill(db)
            print(json.dumps(result, indent=2, default=str))
        else:
            result = await run_complete_backfill(
                db,
                limit=args.limit,
                dry_run=args.dry_run,
                start_from=args.start_from
            )
            print(json.dumps(result, indent=2, default=str))
            
            # Run verification after backfill
            if not args.dry_run:
                verify = await verify_backfill(db)
                print("\n=== VERIFICATION ===")
                print(json.dumps(verify, indent=2, default=str))
        
        client.close()
    
    import json
    asyncio.run(main())
