"""
Universe Counts Service (P48)
=============================
Single source of truth for universe/funnel counts.
Used by BOTH Admin Panel and Talk filters.

CANONICAL FUNNEL DEFINITION:
1. seeded_us_total - All tickers from NYSE + NASDAQ (Sunday seed)
2. seeded_common_stock - Type == "Common Stock"
3. active_with_price_data - has_price_data == true
4. with_classification - sector AND industry present
5. passes_visibility_rule - shares_outstanding > 0 OR safety_type exception
6. visible_tickers - is_visible == true (final customer view)

GUARD: Each step must be <= previous step (monotonic decreasing).

BINDING: Do not change without Richard's approval.
"""

from datetime import datetime, timezone
from typing import Dict, Any, List, Tuple
from zoneinfo import ZoneInfo

PRAGUE_TZ = ZoneInfo("Europe/Prague")

# Import canonical visibility query from visibility_rules (DATA SUPREMACY MANIFESTO v1.0)
from visibility_rules import VISIBLE_TICKERS_QUERY, get_canonical_sieve_query

# Keep backward compatibility alias
VISIBLE_UNIVERSE_QUERY = VISIBLE_TICKERS_QUERY


async def get_universe_counts(db) -> Dict[str, Any]:
    """
    Get canonical universe/funnel counts.
    Single source of truth for Admin Panel + Talk.
    
    Returns:
        Dictionary with funnel steps and metadata
    """
    now_prague = datetime.now(PRAGUE_TZ)
    
    # =========================================================================
    # FUNNEL STEP QUERIES (DATA SUPREMACY MANIFESTO v1.0)
    # SEEDING: exchange ∈ {NYSE, NASDAQ} AND asset_type == "Common Stock"
    # ACTIVITY: has_price_data == true
    # QUALITY: sector AND industry present
    # STATUS: is_delisted != true
    # =========================================================================
    
    # Step 1: All seeded US tickers (NYSE + NASDAQ only)
    step1_query = {"exchange": {"$in": ["NYSE", "NASDAQ"]}}
    seeded_us_total = await db.tracked_tickers.count_documents(step1_query)
    
    # Breakdown by exchange (for info)
    nyse_count = await db.tracked_tickers.count_documents({"exchange": "NYSE"})
    nasdaq_count = await db.tracked_tickers.count_documents({"exchange": "NASDAQ"})
    
    # Step 2: Common Stock only (use asset_type, not type)
    step2_query = {**step1_query, "asset_type": "Common Stock"}
    seeded_common_stock = await db.tracked_tickers.count_documents(step2_query)
    
    # Step 3: With price data (ACTIVITY)
    step3_query = {**step2_query, "has_price_data": True}
    active_with_price_data = await db.tracked_tickers.count_documents(step3_query)
    
    # Step 4: With classification - sector AND industry present (QUALITY)
    step4_query = {
        **step3_query,
        "sector": {"$nin": [None, ""]},
        "industry": {"$nin": [None, ""]}
    }
    with_classification = await db.tracked_tickers.count_documents(step4_query)
    
    # Step 5: Passes visibility rule (STATUS - not delisted)
    # This is the canonical sieve from DATA SUPREMACY MANIFESTO v1.0
    step5_query = {
        **step4_query,
        "is_delisted": {"$ne": True}
    }
    passes_visibility_rule = await db.tracked_tickers.count_documents(step5_query)
    
    # Step 6: Visible tickers (is_visible=true) - this is the final customer-facing count
    # This SHOULD equal step 5 if visibility logic is consistent
    visible_tickers = await db.tracked_tickers.count_documents(VISIBLE_TICKERS_QUERY)
    
    # =========================================================================
    # BUILD FUNNEL STEPS
    # =========================================================================
    
    funnel_steps = [
        {
            "step": 1,
            "name": "Seeded US (NYSE+NASDAQ)",
            "count": seeded_us_total,
            "query": "exchange in [NYSE, NASDAQ]",
            "source_job": "universe_seed",
            "breakdown": f"NYSE: {nyse_count}, NASDAQ: {nasdaq_count}"
        },
        {
            "step": 2,
            "name": "Common Stock",
            "count": seeded_common_stock,
            "query": "type == Common Stock",
            "source_job": "universe_seed",
        },
        {
            "step": 3,
            "name": "With Price Data",
            "count": active_with_price_data,
            "query": "has_price_data == true",
            "source_job": "price_sync",
        },
        {
            "step": 4,
            "name": "With Classification",
            "count": with_classification,
            "query": "sector AND industry present",
            "source_job": "fundamentals_sync",
        },
        {
            "step": 5,
            "name": "Passes Visibility Rule",
            "count": passes_visibility_rule,
            "query": "shares_outstanding > 0 OR safety_type exception",
            "source_job": "visibility_check",
        },
        {
            "step": 6,
            "name": "Visible Tickers (Customer View)",
            "count": visible_tickers,
            "query": "is_visible == true",
            "source_job": "is_visible flag",
            "note": "Same as Talk filter total_count"
        },
    ]
    
    # =========================================================================
    # CONSISTENCY CHECKS
    # =========================================================================
    
    inconsistencies = []
    
    # Check monotonic decreasing (each step <= previous)
    for i in range(1, len(funnel_steps)):
        prev_count = funnel_steps[i-1]["count"]
        curr_count = funnel_steps[i]["count"]
        if curr_count > prev_count:
            inconsistencies.append({
                "type": "funnel_increase",
                "step": funnel_steps[i]["step"],
                "name": funnel_steps[i]["name"],
                "count": curr_count,
                "prev_step": funnel_steps[i-1]["step"],
                "prev_name": funnel_steps[i-1]["name"],
                "prev_count": prev_count,
                "message": f"Step {funnel_steps[i]['step']} ({curr_count}) > Step {funnel_steps[i-1]['step']} ({prev_count})"
            })
            funnel_steps[i]["warning"] = f"Exceeds step {funnel_steps[i-1]['step']} ({prev_count})"
    
    # Check if visible_tickers matches passes_visibility_rule
    # They should be equal if visibility logic is correctly computed
    visibility_mismatch = abs(visible_tickers - passes_visibility_rule)
    if visibility_mismatch > 0:
        inconsistencies.append({
            "type": "visibility_mismatch",
            "visible_tickers": visible_tickers,
            "passes_visibility_rule": passes_visibility_rule,
            "diff": visibility_mismatch,
            "message": f"is_visible ({visible_tickers}) != visibility rule ({passes_visibility_rule}). Diff: {visibility_mismatch}"
        })
    
    return {
        "generated_at": now_prague.isoformat(),
        "funnel_steps": funnel_steps,
        "inconsistencies": inconsistencies,
        "has_inconsistency": len(inconsistencies) > 0,
        
        # Quick access to key counts
        "counts": {
            "seeded_us_total": seeded_us_total,
            "nyse": nyse_count,
            "nasdaq": nasdaq_count,
            "common_stock": seeded_common_stock,
            "with_price_data": active_with_price_data,
            "with_classification": with_classification,
            "passes_visibility_rule": passes_visibility_rule,
            "visible_tickers": visible_tickers,
        },
        
        # For Talk filters compatibility
        "visible_universe_count": visible_tickers,
    }


async def get_exchange_counts(db) -> List[Dict[str, Any]]:
    """
    Get exchange counts for Talk filters (using visible universe).
    """
    counts = []
    async for doc in db.tracked_tickers.aggregate([
        {"$match": VISIBLE_UNIVERSE_QUERY},
        {"$group": {"_id": "$exchange", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
    ]):
        if doc["_id"]:
            counts.append({"value": doc["_id"], "count": doc["count"]})
    return counts


async def get_sector_counts(db) -> List[Dict[str, Any]]:
    """
    Get sector counts for Talk filters (using visible universe).
    """
    counts = []
    async for doc in db.tracked_tickers.aggregate([
        {"$match": {**VISIBLE_UNIVERSE_QUERY, "sector": {"$ne": None, "$ne": ""}}},
        {"$group": {"_id": "$sector", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
    ]):
        if doc["_id"] and doc["_id"].strip():
            counts.append({"value": doc["_id"].strip(), "count": doc["count"]})
    return counts


async def get_industry_counts(db) -> List[Dict[str, Any]]:
    """
    Get industry counts for Talk filters (using visible universe).
    """
    counts = []
    async for doc in db.tracked_tickers.aggregate([
        {"$match": {**VISIBLE_UNIVERSE_QUERY, "industry": {"$ne": None, "$ne": ""}}},
        {"$group": {"_id": "$industry", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
    ]):
        if doc["_id"] and doc["_id"].strip():
            counts.append({"value": doc["_id"].strip(), "count": doc["count"]})
    return counts
