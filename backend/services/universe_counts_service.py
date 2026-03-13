"""
Universe Counts Service (P48)
=============================
Single source of truth for universe/funnel counts.
Used by BOTH Admin Panel and Talk filters.

CANONICAL ADMIN PIPELINE DEFINITION:
1. step1_seeded_total - Step 1 DB-backed seed universe (NYSE/NASDAQ Common Stock)
2. active_with_price_data - Step 2 output
3. with_classification - Step 3 output
4. passes_visibility_rule - Step 4 rule output
5. visible_tickers - final customer-visible universe

RAW EXCHANGE UNIVERSE:
- step1_raw_exchange_total = all NYSE + NASDAQ tracked_tickers before Common Stock scoping
- kept as metadata for Step 1 auditability / backward compatibility

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
    
    # Build step queries
    raw_exchange_query = {"exchange": {"$in": ["NYSE", "NASDAQ"]}}
    step1_query = {**raw_exchange_query, "asset_type": "Common Stock"}
    step3_query = {**step1_query, "has_price_data": True}
    step4_query = {
        **step3_query,
        "sector": {"$nin": [None, ""]},
        "industry": {"$nin": [None, ""]}
    }
    step5_query = {**step4_query, "is_delisted": {"$ne": True}}

    # =========================================================================
    # SINGLE ROUND-TRIP: $facet runs all 8 counts in parallel on the server
    # Replaces 8x sequential count_documents (~400ms) with 1 aggregation (~50ms)
    # =========================================================================
    # Step 3 "up-to-date" output rule (ticker-level):
    #   fundamentals_status='complete'
    #   AND needs_fundamentals_refresh != True
    #   AND fundamentals_updated_at not null/missing
    step3_output_query = {
        **step3_query,
        "fundamentals_status": "complete",
        "needs_fundamentals_refresh": {"$ne": True},
        "fundamentals_updated_at": {"$nin": [None, ""], "$exists": True},
    }
    step4_visible_query = {**step4_query, "is_visible": True}
    facet_result = await db.tracked_tickers.aggregate([{"$facet": {
        "raw_exchange":   [{"$match": raw_exchange_query},       {"$count": "n"}],
        "seeded":         [{"$match": step1_query},              {"$count": "n"}],
        "nyse":           [{"$match": {"exchange": "NYSE"}},     {"$count": "n"}],
        "nasdaq":         [{"$match": {"exchange": "NASDAQ"}},   {"$count": "n"}],
        "price":          [{"$match": step3_query},              {"$count": "n"}],
        "step3_output":   [{"$match": step3_output_query},       {"$count": "n"}],
        "classified":     [{"$match": step4_query},              {"$count": "n"}],
        "step4_visible":  [{"$match": step4_visible_query},      {"$count": "n"}],
        "visibility":     [{"$match": step5_query},              {"$count": "n"}],
        "visible":        [{"$match": VISIBLE_TICKERS_QUERY},    {"$count": "n"}],
    }}]).to_list(1)

    f = facet_result[0] if facet_result else {}
    def _n(key: str) -> int:
        return (f.get(key) or [{}])[0].get("n", 0)

    step1_raw_exchange_total = _n("raw_exchange")
    seeded_us_total        = _n("seeded")
    nyse_count             = _n("nyse")
    nasdaq_count           = _n("nasdaq")
    active_with_price_data = _n("price")
    step3_output_total     = _n("step3_output")
    with_classification    = _n("classified")
    step4_visible_total    = _n("step4_visible")
    passes_visibility_rule = _n("visibility")
    visible_tickers        = _n("visible")
    
    # =========================================================================
    # BUILD FUNNEL STEPS
    # =========================================================================
    
    funnel_steps = [
        {
            "step": 1,
            "name": "Universe Seed",
            "count": seeded_us_total,
            "query": "exchange in [NYSE, NASDAQ] AND asset_type == Common Stock",
            "source_job": "universe_seed",
            "breakdown": f"raw exchange total: {step1_raw_exchange_total}, NYSE: {nyse_count}, NASDAQ: {nasdaq_count}"
        },
        {
            "step": 2,
            "name": "With Price Data",
            "count": active_with_price_data,
            "query": "has_price_data == true",
            "source_job": "price_sync",
        },
        {
            "step": 3,
            "name": "With Classification",
            "count": with_classification,
            "query": "sector AND industry present",
            "source_job": "fundamentals_sync",
        },
        {
            "step": 4,
            "name": "Passes Visibility Rule",
            "count": passes_visibility_rule,
            "query": "shares_outstanding > 0 OR safety_type exception",
            "source_job": "visibility_check",
        },
        {
            "step": 5,
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
            "step1_raw_exchange_total": step1_raw_exchange_total,
            "step1_seeded_total": seeded_us_total,
            "seeded_us_total": seeded_us_total,
            "nyse": nyse_count,
            "nasdaq": nasdaq_count,
            "common_stock": seeded_us_total,
            "with_price_data": active_with_price_data,
            "step3_input_total": active_with_price_data,
            "step3_output_total": step3_output_total,
            "step3_filtered_out_total": max(active_with_price_data - step3_output_total, 0),
            "with_classification": with_classification,
            # Step 4 canonical funnel: visible scoped to classified universe.
            "step4_visible_total": step4_visible_total,
            "step4_filtered_out_total": max(with_classification - step4_visible_total, 0),
            "passes_visibility_rule": passes_visibility_rule,
            "visible_tickers": visible_tickers,
        },

        # Step 3 ticker-level funnel (input/output/filtered_out).
        # "Up-to-date" = fundamentals_status='complete'
        #               AND needs_fundamentals_refresh != True
        #               AND fundamentals_updated_at not null/missing
        "step3_funnel": {
            "input_total": active_with_price_data,
            "output_total": step3_output_total,
            "filtered_out_total": max(active_with_price_data - step3_output_total, 0),
        },

        # Step 4 canonical funnel: visible scoped to classified universe.
        "step4_funnel": {
            "input_total": with_classification,
            "output_total": step4_visible_total,
            "filtered_out_total": max(with_classification - step4_visible_total, 0),
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
