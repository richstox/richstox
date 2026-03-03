# ==============================================================================
# 🛡️ SCHEDULER-ONLY FILE - EXTERNAL API CALLS ALLOWED
# ==============================================================================
# This file is in ALLOWLIST: /app/scripts/audit_external_calls.py
# May call: eodhd.com/api/* (via httpx)
# Context: ONLY from scheduler.py jobs or admin backfill endpoints
# Runtime API endpoints MUST NOT import from this file
# ==============================================================================
#
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
# UNIVERSE SYSTEM — PERMANENT & BINDING FOR ALL FUTURE INSTANCES
# ================================================================================
# This is the ONLY way the app defines its ticker universe. No exceptions.
# No agent, fork, or future instance may deviate from this.
#
# ALLOWED EODHD API ENDPOINTS (ONLY THESE 3):
# 1. SEED:         https://eodhd.com/api/exchange-symbol-list/{NYSE|NASDAQ}
# 2. PRICES:       https://eodhd.com/api/eod-bulk-last-day/US
# 3. FUNDAMENTALS: https://eodhd.com/api/fundamentals/{TICKER}.US
#
# VISIBLE UNIVERSE RULE:
# is_visible = is_seeded && has_price_data && has_classification
# Where:
#   - is_seeded: NYSE/NASDAQ + Type == "Common Stock"
#   - has_price_data: appears in daily bulk prices
#   - has_classification: sector AND industry are non-empty
#
# APP RUNTIME NEVER CALLS EODHD. All data comes from MongoDB only.
#
# Any deviation requires explicit written approval from Richard (kurtarichard@gmail.com).
# ================================================================================

"""
RICHSTOX Whitelist Service - Ticker Pipeline
==============================================
Manages the canonical list of trackable tickers (tracked_tickers collection).

CRITICAL DEFINITIONS:
- is_visible = ticker visible in app (is_seeded && has_price_data && has_classification)
- is_seeded = from NYSE/NASDAQ Common Stock list
- has_price_data = appears in daily bulk prices
- has_classification = sector AND industry non-empty

VISIBLE UNIVERSE:
- is_visible = true (single source of truth)

Pipeline:
1. sync-ticker-whitelist: Fetch candidates from EODHD exchange-symbol-list
   -> Creates entries in tracked_tickers with is_active=false, fundamentals_status='pending'
   -> Queues fundamentals_events for each new ticker

2. process-fundamentals-events: Fetches fundamentals for pending tickers
   -> Updates fundamentals_status='ok'/'missing'/'error'
   -> Fills sector, industry, etc.
   -> Does NOT change is_active

3. price_ingestion: Fetches daily prices
   -> Sets has_price_data=true after successful price save
   -> is_active is derived from has_price_data

4. Only tickers with is_active=true are visible/searchable in the app
"""

import os
import logging
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
import httpx

from visibility_rules import get_canonical_sieve_query

logger = logging.getLogger("richstox.whitelist")

EODHD_BASE_URL = "https://eodhd.com/api"
EODHD_API_KEY = os.getenv("EODHD_API_KEY", "")

# Exchanges to sync
SUPPORTED_EXCHANGES = ["NYSE", "NASDAQ"]

# Filter criteria for whitelist candidates
WHITELIST_FILTERS = {
    "type": "Common Stock",
    "currency": "USD",
}

# Fields to extract from fundamentals for company_fundamentals_cache
FUNDAMENTALS_FIELDS = [
    "General.Code",
    "General.Name", 
    "General.Exchange",
    "General.Sector",
    "General.Industry",
    "General.Description",
    "General.LogoURL",
    "General.WebURL",
    "General.IPODate",
    "General.IsDelisted",
    "Highlights.MarketCapitalization",
    "Highlights.PERatio",
    "Highlights.EarningsShare",
    "Highlights.DividendYield",
    "Highlights.BookValue",
    "Highlights.RevenueTTM",
    "Technicals.Beta",
    "Technicals.52WeekHigh",
    "Technicals.52WeekLow",
    "SharesStats.SharesOutstanding",
]


async def fetch_exchange_symbols(exchange: str) -> List[Dict[str, Any]]:
    """Fetch all symbols from EODHD exchange-symbol-list endpoint."""
    if not EODHD_API_KEY:
        logger.error("EODHD_API_KEY not configured")
        return []
    
    url = f"{EODHD_BASE_URL}/exchange-symbol-list/{exchange}"
    params = {"api_token": EODHD_API_KEY, "fmt": "json"}
    
    try:
        async with httpx.AsyncClient(timeout=120) as client:
            response = await client.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            logger.info(f"Fetched {len(data)} symbols from {exchange}")
            return data
    except Exception as e:
        logger.error(f"Failed to fetch symbols from {exchange}: {e}")
        return []


async def fetch_fundamentals(ticker: str) -> Optional[Dict[str, Any]]:
    """
    Fetch fundamentals for a single ticker from EODHD.
    Returns None if no fundamentals available.
    
    Cost: 10 API calls per request
    """
    if not EODHD_API_KEY:
        return None
    
    # Ensure .US suffix
    if not ticker.endswith(".US"):
        ticker = f"{ticker}.US"
    
    url = f"{EODHD_BASE_URL}/fundamentals/{ticker}"
    params = {"api_token": EODHD_API_KEY, "fmt": "json"}
    
    try:
        async with httpx.AsyncClient(timeout=60) as client:
            response = await client.get(url, params=params)
            
            if response.status_code == 404:
                logger.debug(f"No fundamentals for {ticker}")
                return None
            
            response.raise_for_status()
            data = response.json()
            
            # Check if we got valid data (not empty)
            if not data or not data.get("General"):
                logger.debug(f"Empty fundamentals for {ticker}")
                return None
            
            return data
    except Exception as e:
        logger.error(f"Failed to fetch fundamentals for {ticker}: {e}")
        return None


def filter_whitelist_candidates(symbols: List[Dict[str, Any]], exchange: str) -> List[Dict[str, Any]]:
    """Filter symbols to only include whitelist candidates."""
    candidates = []
    
    # Patterns to exclude (warrants, preferred, units, rights, etc.)
    EXCLUDE_PATTERNS = [
        "-WT", "-WS", "-WI",  # Warrants
        "-U", "-UN",          # Units
        "-P-", "-PA", "-PB", "-PC", "-PD", "-PE", "-PF", "-PG", "-PH", "-PI", "-PJ",  # Preferred
        "-R", "-RI",          # Rights
        ".A", ".B", ".C",     # Class shares with dots
    ]
    
    for sym in symbols:
        code = sym.get("Code", "").strip()
        if not code:
            continue
        
        # Skip tickers with dots (usually ADRs, preferred shares, units)
        if "." in code:
            continue
        
        # Skip tickers matching exclude patterns
        skip = False
        for pattern in EXCLUDE_PATTERNS:
            if pattern in code.upper():
                skip = True
                break
        if skip:
            continue
        
        if sym.get("Type") != WHITELIST_FILTERS["type"]:
            continue
        
        if sym.get("Currency") != WHITELIST_FILTERS["currency"]:
            continue
        
        candidates.append({
            "ticker": f"{code}.US",
            "code": code,
            "name": sym.get("Name", code),
            "exchange": exchange,
            "isin": sym.get("Isin", ""),
            "type": sym.get("Type"),
            "currency": sym.get("Currency"),
            "country": sym.get("Country", "USA"),
        })
    
    logger.info(f"Filtered {len(candidates)} candidates from {exchange}")
    return candidates


def extract_fundamentals_cache(fundamentals: Dict[str, Any], ticker: str) -> Dict[str, Any]:
    """
    Extract relevant fields from EODHD fundamentals response
    to store in company_fundamentals_cache.
    """
    general = fundamentals.get("General", {})
    highlights = fundamentals.get("Highlights", {})
    technicals = fundamentals.get("Technicals", {})
    shares = fundamentals.get("SharesStats", {})
    valuation = fundamentals.get("Valuation", {})
    
    return {
        "ticker": ticker,
        "code": general.get("Code", ticker.replace(".US", "")),
        "name": general.get("Name", ""),
        "exchange": general.get("Exchange", ""),
        "sector": general.get("Sector", ""),
        "industry": general.get("Industry", ""),
        "description": (general.get("Description") or "")[:1000],  # Truncate
        "logo_url": general.get("LogoURL", ""),
        "web_url": general.get("WebURL", ""),
        "ipo_date": general.get("IPODate", ""),
        "is_delisted": general.get("IsDelisted", False),
        "country": general.get("CountryName", "USA"),
        "currency": general.get("CurrencyCode", "USD"),
        
        # =======================================================================
        # RAW FACTS ONLY. No precomputed metrics.
        # These EODHD precomputed values are FORBIDDEN and must not be stored:
        #   - pe_ratio, peg_ratio, dividend_yield, profit_margin, ROE, ROA
        #   - beta, 52W high/low, 50/200 MA
        #   - market_cap, EPS (must compute from price * shares, net_income/shares)
        # Compute all metrics locally from raw financial statements + prices.
        # =======================================================================
        
        # Only identity and classification facts stored here
        # Shares data (raw fact, needed for market cap calculation)
        "shares_outstanding": shares.get("SharesOutstanding"),
        "shares_float": shares.get("SharesFloat"),
        
        "updated_at": datetime.now(timezone.utc),
    }


async def sync_ticker_whitelist(
    db,
    dry_run: bool = False,
    exchanges: Optional[List[str]] = None
) -> Dict[str, Any]:
    """
    Synchronize the tracked_tickers collection with EODHD exchange-symbol-list.
    
    This creates CANDIDATES only. Tickers get status='pending_fundamentals'.
    They become 'active' only after fundamentals are fetched successfully.
    """
    exchanges = exchanges or SUPPORTED_EXCHANGES
    now = datetime.now(timezone.utc)
    
    result = {
        "started_at": now.isoformat(),
        "dry_run": dry_run,
        "exchanges": exchanges,
        "fetched": 0,
        "filtered": 0,
        "added_pending": 0,
        "already_exists": 0,
        "reactivated": 0,
        "deactivated": 0,
        "fundamentals_events_created": 0,
        "errors": [],
    }
    
    # Collect all candidates
    all_candidates = []
    for exchange in exchanges:
        symbols = await fetch_exchange_symbols(exchange)
        result["fetched"] += len(symbols)
        
        if not symbols:
            result["errors"].append(f"No symbols returned from {exchange}")
            continue
        
        candidates = filter_whitelist_candidates(symbols, exchange)
        result["filtered"] += len(candidates)
        all_candidates.extend(candidates)
    
    if not all_candidates:
        result["errors"].append("No candidates after filtering")
        result["finished_at"] = datetime.now(timezone.utc).isoformat()
        return result
    
    candidate_tickers = {c["ticker"] for c in all_candidates}
    logger.info(f"Total whitelist candidates: {len(candidate_tickers)}")
    
    if dry_run:
        # Count what would happen
        existing = await db.tracked_tickers.find(
            {"ticker": {"$in": list(candidate_tickers)}},
            {"ticker": 1, "status": 1}
        ).to_list(None)
        
        existing_map = {t["ticker"]: t.get("status") for t in existing}
        
        for c in all_candidates:
            ticker = c["ticker"]
            if ticker not in existing_map:
                result["added_pending"] += 1
            else:
                result["already_exists"] += 1
        
        result["finished_at"] = datetime.now(timezone.utc).isoformat()
        return result
    
    # LIVE MODE
    # BULK UPSERT tracked_tickers v batchích po 500
    from pymongo import UpdateOne
    BATCH_SIZE = 500

    ticker_ops = []
    for candidate in all_candidates:
        ticker = candidate["ticker"]
        ticker_ops.append(UpdateOne(
            {"ticker": ticker},
            {
                "$set": {
                    "last_seen_date": now,
                    "updated_at": now,
                },
                "$setOnInsert": {
                    "ticker": ticker,
                    "code": candidate["code"],
                    "symbol": candidate["code"],
                    "primary_ticker": ticker,
                    "name": candidate["name"],
                    "exchange": candidate["exchange"],
                    "isin": candidate["isin"],
                    "type": candidate["type"],
                    "asset_type": candidate["type"],
                    "currency": candidate["currency"],
                    "country": candidate["country"],
                    "is_whitelisted": True,
                    "is_active": False,
                    "has_price_data": False,
                    "fundamentals_status": "pending",
                    "status": "pending_fundamentals",
                    "first_seen_date": now,
                    "classification_source": "exchange_symbol_list",
                    "created_at": now,
                }
            },
            upsert=True
        ))

    # Zpracovat v batchích, sbírat indexy nových tickerů přes upserted_ids
    new_ticker_indices = []
    for i in range(0, len(ticker_ops), BATCH_SIZE):
        batch = ticker_ops[i:i + BATCH_SIZE]
        try:
            bulk_result = await db.tracked_tickers.bulk_write(batch, ordered=False)
            result["added_pending"] += bulk_result.upserted_count
            result["already_exists"] += bulk_result.matched_count
            # upserted_ids je dict {index_v_batchi: _id}
            for batch_idx in bulk_result.upserted_ids:
                new_ticker_indices.append(i + batch_idx)
        except Exception as e:
            logger.error(f"Bulk write batch {i // BATCH_SIZE} failed: {e}")

    # Bulk insert fundamentals_events jen pro nové tickery
    if new_ticker_indices:
        event_docs = [
            {
                "ticker": all_candidates[idx]["ticker"],
                "event_type": "initial_sync",
                "status": "pending",
                "created_at": now,
            }
            for idx in new_ticker_indices
        ]
        try:
            await db.fundamentals_events.insert_many(event_docs, ordered=False)
            result["fundamentals_events_created"] += len(event_docs)
        except Exception as e:
            logger.error(f"fundamentals_events insert_many failed: {e}")
    
    # Deactivate tickers not in current EODHD list
    deactivate_result = await db.tracked_tickers.update_many(
        {
            "ticker": {"$nin": list(candidate_tickers)},
            "status": "active",
            "classification_source": "exchange_symbol_list",
        },
        {
            "$set": {
                "status": "delisted",
                "is_active": False,
                "deactivated_at": now,
                "updated_at": now,
            }
        }
    )
    result["deactivated"] = deactivate_result.modified_count
    
    result["finished_at"] = datetime.now(timezone.utc).isoformat()
    
    logger.info(
        f"Whitelist sync complete: "
        f"added_pending={result['added_pending']}, "
        f"already_exists={result['already_exists']}, "
        f"events_created={result['fundamentals_events_created']}"
    )
    
    return result


async def process_fundamentals_events(
    db,
    batch_size: int = 50,
    dry_run: bool = False
) -> Dict[str, Any]:
    """
    Process pending fundamentals events.
    
    For each pending event:
    1. Fetch fundamentals from EODHD
    2. If successful: update tracked_tickers status='active', is_active=true
    3. Store data in company_fundamentals_cache
    4. If failed: update status='no_fundamentals', is_active=false
    """
    now = datetime.now(timezone.utc)
    
    result = {
        "started_at": now.isoformat(),
        "dry_run": dry_run,
        "batch_size": batch_size,
        "processed": 0,
        "activated": 0,
        "no_fundamentals": 0,
        "errors": 0,
        "api_calls_used": 0,  # Each fundamental = 10 API calls
    }
    
    # Get pending events
    pending_events = await db.fundamentals_events.find(
        {"status": "pending"}
    ).limit(batch_size).to_list(batch_size)
    
    result["pending_count"] = len(pending_events)
    
    if not pending_events:
        result["finished_at"] = datetime.now(timezone.utc).isoformat()
        return result
    
    if dry_run:
        result["would_process"] = len(pending_events)
        result["finished_at"] = datetime.now(timezone.utc).isoformat()
        return result
    
    for event in pending_events:
        ticker = event["ticker"]
        event_id = event["_id"]
        
        # Fetch fundamentals
        fundamentals = await fetch_fundamentals(ticker)
        result["api_calls_used"] += 10
        result["processed"] += 1
        
        if fundamentals:
            # Extract and cache fundamentals
            cache_doc = extract_fundamentals_cache(fundamentals, ticker)
            
            # Upsert into company_fundamentals_cache
            await db.company_fundamentals_cache.update_one(
                {"ticker": ticker},
                {"$set": cache_doc},
                upsert=True
            )
            
            # Update fundamentals status - does NOT change is_active
            # is_active is controlled by has_price_data (set by price ingestion)
            await db.tracked_tickers.update_one(
                {"ticker": ticker},
                {"$set": {
                    "fundamentals_status": "ok",
                    "status": "active",  # Legacy field
                    "name": cache_doc.get("name") or ticker,
                    "sector": cache_doc.get("sector"),
                    "industry": cache_doc.get("industry"),
                    "logo_url": cache_doc.get("logo_url"),
                    "fundamentals_updated_at": now,
                    "updated_at": now,
                }}
            )
            
            # Mark event as processed
            await db.fundamentals_events.update_one(
                {"_id": event_id},
                {"$set": {
                    "status": "completed",
                    "processed_at": now,
                }}
            )
            
            result["activated"] += 1
            logger.debug(f"Fundamentals OK for {ticker}")
        else:
            # No fundamentals available - does NOT change is_active
            await db.tracked_tickers.update_one(
                {"ticker": ticker},
                {"$set": {
                    "fundamentals_status": "missing",
                    "status": "no_fundamentals",  # Legacy field
                    "updated_at": now,
                }}
            )
            
            await db.fundamentals_events.update_one(
                {"_id": event_id},
                {"$set": {
                    "status": "no_data",
                    "processed_at": now,
                    "error": "No fundamentals available from EODHD",
                }}
            )
            
            result["no_fundamentals"] += 1
            logger.debug(f"No fundamentals for {ticker}")
    
    result["finished_at"] = datetime.now(timezone.utc).isoformat()
    
    logger.info(
        f"Fundamentals processing complete: "
        f"activated={result['activated']}, "
        f"no_fundamentals={result['no_fundamentals']}, "
        f"api_calls={result['api_calls_used']}"
    )
    
    return result


async def get_whitelist_stats(db) -> Dict[str, Any]:
    """
    Get current whitelist statistics.
    
    =============================================================================
    VISIBLE UNIVERSE RULE (PERMANENT)
    =============================================================================
    Uses is_visible=true as the only visibility filter.
    is_visible = is_seeded && has_price_data && has_classification
    =============================================================================
    """
    # VISIBLE UNIVERSE QUERY
    visible_filter = {"is_visible": True}
    
    total = await db.tracked_tickers.count_documents({})
    
    # Visible universe
    visible = await db.tracked_tickers.count_documents(visible_filter)
    
    # With fundamentals (status=active within visible)
    with_fundamentals = await db.tracked_tickers.count_documents({
        **visible_filter,
        "status": "active"
    })
    
    # Pending fundamentals (within visible)
    pending_fundamentals = await db.tracked_tickers.count_documents({
        **visible_filter,
        "status": "pending_fundamentals"
    })
    
    # Legacy counts
    no_fundamentals = await db.tracked_tickers.count_documents({"status": "no_fundamentals"})
    delisted = await db.tracked_tickers.count_documents({"status": "delisted"})
    
    # Pending fundamentals events
    pending_events = await db.fundamentals_events.count_documents({"status": "pending"})
    
    # Count by exchange (visible only)
    pipeline = [
        {"$match": visible_filter},
        {"$group": {"_id": "$exchange", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
    ]
    by_exchange = await db.tracked_tickers.aggregate(pipeline).to_list(None)
    
    # Fundamentals cache count
    fundamentals_cached = await db.company_fundamentals_cache.count_documents({})
    
    return {
        "total": total,
        "visible": visible,  # VISIBLE UNIVERSE (is_visible=true)
        "with_fundamentals": with_fundamentals,
        "pending_fundamentals": pending_fundamentals,
        "active": with_fundamentals,  # Legacy alias
        "no_fundamentals": no_fundamentals,
        "delisted": delisted,
        "pending_events": pending_events,
        "fundamentals_cached": fundamentals_cached,
        "by_exchange": {item["_id"]: item["count"] for item in by_exchange},
    }


async def search_whitelist(
    db,
    query: str,
    limit: int = 20
) -> List[Dict[str, Any]]:
    """
    Search the whitelist for tickers matching a query.
    
    RANKING ORDER (deterministic):
    1) Exact ticker match (ticker == query.upper() + ".US") → rank 0
    2) Ticker prefix matches (ticker startswith query) → rank 1
    3) Name prefix matches (name startswith query) → rank 2
    4) Name contains matches (name contains query) → rank 3
    
    Within each rank tier, results are sorted alphabetically by ticker.
    """
    if not query or len(query) < 1:
        return []
    
    query_upper = query.upper()
    query_with_suffix = f"{query_upper}.US"
    
    # CANONICAL SOURCE OF TRUTH: Same query as Universe Funnel
    visible_filter = get_canonical_sieve_query()
    
    # Search conditions: ticker prefix OR name contains
    search_or = {
        "$or": [
            {"ticker": {"$regex": f"^{query_upper}", "$options": "i"}},
            {"name": {"$regex": query, "$options": "i"}},
        ]
    }
    
    # Aggregation pipeline with ranking
    pipeline = [
        # Stage 1: Match canonical sieve + search conditions
        {"$match": {"$and": [visible_filter, search_or]}},
        
        # Stage 2: Add rank field for deterministic sorting
        {"$addFields": {
            "rank": {
                "$switch": {
                    "branches": [
                        # Rank 0: Exact ticker match (e.g., "GE.US" for query "GE")
                        {
                            "case": {"$eq": [{"$toUpper": "$ticker"}, query_with_suffix]},
                            "then": 0
                        },
                        # Rank 1: Ticker prefix match (e.g., "GEVO.US" for query "GE")
                        {
                            "case": {"$regexMatch": {"input": "$ticker", "regex": f"^{query_upper}", "options": "i"}},
                            "then": 1
                        },
                        # Rank 2: Name prefix match (e.g., "General Electric" for query "General")
                        {
                            "case": {"$regexMatch": {"input": "$name", "regex": f"^{query}", "options": "i"}},
                            "then": 2
                        },
                    ],
                    # Rank 3: Name contains match (default)
                    "default": 3
                }
            }
        }},
        
        # Stage 3: Sort by rank (ascending), then ticker (alphabetical)
        {"$sort": {"rank": 1, "ticker": 1}},
        
        # Stage 4: Limit results
        {"$limit": limit},
        
        # Stage 5: Project only needed fields
        {"$project": {
            "_id": 0,
            "ticker": 1,
            "name": 1,
            "exchange": 1,
            "sector": 1,
            "industry": 1,
            "asset_type": 1,
            "status": 1,
            "safety_type": 1,
            "logo": "$fundamentals.General.LogoURL",
            "rank": 1,
        }}
    ]
    
    results = await db.tracked_tickers.aggregate(pipeline).to_list(None)
    
    # Format results
    formatted = []
    for r in results:
        ticker = r.get("ticker", "")
        ticker_code = ticker.replace(".US", "") if ticker else ""
        
        # Build safety info
        safety_type = r.get("safety_type", "standard")
        safety_info = {
            "type": safety_type,
            "badge_text": {
                "standard": None,
                "spac_shell": "SPAC / Shell Co",
                "recent_ipo": "Recent IPO"
            }.get(safety_type),
            "badge_color": {
                "standard": None,
                "spac_shell": "amber",
                "recent_ipo": "blue"
            }.get(safety_type),
        }
        
        formatted.append({
            "ticker": ticker_code,
            "name": r.get("name") or ticker_code,
            "exchange": r.get("exchange", "US"),
            "sector": r.get("sector"),
            "industry": r.get("industry"),
            "asset_type": r.get("asset_type", "Common Stock"),
            "fundamentals_pending": r.get("status") != "active",
            "safety": safety_info,
            "logo": r.get("logo"),
        })
    
    return formatted


async def is_ticker_in_whitelist(db, ticker: str) -> bool:
    """
    Check if a ticker is in the active whitelist.
    LOVABLE LOGIC: Visibility = is_active=true AND exchange IN ('NYSE', 'NASDAQ') AND asset_type='Common Stock'
    """
    if not ticker.endswith(".US"):
        ticker = f"{ticker}.US"
    
    # SINGLE SOURCE OF TRUTH: Use is_visible only
    doc = await db.tracked_tickers.find_one(
        {
            "ticker": ticker,
            "is_visible": True,
        },
        {"_id": 1}
    )
    
    return doc is not None


# DEPRECATED - Use VISIBLE_UNIVERSE_QUERY instead
# Kept for backwards compatibility but should not be used
LOVABLE_VISIBILITY_FILTER = {"is_visible": True}


# SINGLE SOURCE OF TRUTH (mirrors server.py constant)
VISIBLE_UNIVERSE_QUERY = {"is_visible": True}



async def get_fundamentals_batch(db, tickers: List[str]) -> Dict[str, Dict[str, Any]]:
    """
    Get fundamentals data for multiple tickers from cache.
    Returns dict mapping ticker -> fundamentals data.
    
    NO API calls - only reads from company_fundamentals_cache.
    """
    if not tickers:
        return {}
    
    result = {}
    cursor = db.company_fundamentals_cache.find(
        {"ticker": {"$in": tickers}},
        {"ticker": 1, "logo_url": 1, "name": 1, "sector": 1, "industry": 1, "_id": 0}
    )
    
    async for doc in cursor:
        result[doc["ticker"]] = doc
    
    return result
