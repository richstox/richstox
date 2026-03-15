"""
Universe Counts Service (P48)
=============================
Single source of truth for universe/funnel counts.
Used by BOTH Admin Panel and Talk filters.

CANONICAL FUNNEL DEFINITION:
  seeded     - exchange ∈ {NYSE, NASDAQ} AND asset_type == "Common Stock"
  with_price - seeded AND has_price_data == true
  classified - with_price AND fundamentals_status == "complete"  (step3 output)
  visible    - classified AND is_visible == true (scoped for monotonicity)

GUARD: Each step must be <= previous step (monotonic decreasing).

BINDING: Do not change without Richard's approval.
"""

from datetime import datetime
from typing import Dict, Any, List
from zoneinfo import ZoneInfo

PRAGUE_TZ = ZoneInfo("Europe/Prague")

# Import canonical visibility query from visibility_rules (DATA SUPREMACY MANIFESTO v1.0)
from visibility_rules import VISIBLE_TICKERS_QUERY

# Keep backward compatibility alias
VISIBLE_UNIVERSE_QUERY = VISIBLE_TICKERS_QUERY


async def get_universe_counts(db) -> Dict[str, Any]:
    """
    Get canonical universe/funnel counts.
    Single source of truth for Admin Panel + Talk.

    Returns:
        Dictionary with funnel steps and metadata.
        Key counts:
          counts.seeded     - seeded universe (NYSE/NASDAQ Common Stock)
          counts.with_price - seeded tickers with current price data
          counts.classified - with_price tickers where fundamentals_status=="complete"
          counts.visible    - classified tickers where is_visible==true
    """
    now_prague = datetime.now(PRAGUE_TZ)

    # =========================================================================
    # FUNNEL STEP QUERIES (each is a strict superset of the previous)
    # seeded:     exchange ∈ {NYSE, NASDAQ} AND asset_type == "Common Stock"
    # with_price: seeded AND has_price_data == true
    # classified: with_price AND fundamentals_status == "complete"  (step3 output)
    # visible:    classified AND is_visible == true  (scoped for monotonicity)
    # =========================================================================
    seeded_query = {
        "exchange":   {"$in": ["NYSE", "NASDAQ"]},
        "asset_type": "Common Stock",
    }
    with_price_query = {**seeded_query, "has_price_data": True}
    classified_query = {**with_price_query, "fundamentals_status": "complete"}
    # Visible is scoped to classified so that visible <= classified always holds.
    visible_query = {**classified_query, "is_visible": True}

    # =========================================================================
    # SINGLE ROUND-TRIP: $facet runs all counts in parallel on the server
    # =========================================================================
    facet_result = await db.tracked_tickers.aggregate([{"$facet": {
        # NYSE/NASDAQ breakdown scoped to seeded so counts never exceed seeded_total
        "nyse":       [{"$match": {"exchange": "NYSE",   "asset_type": "Common Stock"}}, {"$count": "n"}],
        "nasdaq":     [{"$match": {"exchange": "NASDAQ", "asset_type": "Common Stock"}}, {"$count": "n"}],
        "seeded":     [{"$match": seeded_query},     {"$count": "n"}],
        "with_price": [{"$match": with_price_query}, {"$count": "n"}],
        "classified": [{"$match": classified_query}, {"$count": "n"}],
        "visible":    [{"$match": visible_query},    {"$count": "n"}],
    }}]).to_list(1)

    f = facet_result[0] if facet_result else {}

    def _n(key: str) -> int:
        return (f.get(key) or [{}])[0].get("n", 0)

    nyse_count       = _n("nyse")
    nasdaq_count     = _n("nasdaq")
    seeded_total     = _n("seeded")
    with_price_total = _n("with_price")
    classified_total = _n("classified")
    visible_total    = _n("visible")

    # =========================================================================
    # BUILD FUNNEL STEPS
    # =========================================================================
    funnel_steps = [
        {
            "step": 1,
            "name": "Seeded Universe (Common Stock)",
            "count": seeded_total,
            "query": "exchange in [NYSE, NASDAQ] AND asset_type == 'Common Stock'",
            "source_job": "universe_seed",
            "breakdown": f"NYSE: {nyse_count}, NASDAQ: {nasdaq_count}",
        },
        {
            "step": 2,
            "name": "With Price Data",
            "count": with_price_total,
            "query": "seeded AND has_price_data == true",
            "source_job": "price_sync",
        },
        {
            "step": 3,
            "name": "Visible Tickers (Customer View)",
            "count": visible_total,
            "query": "classified AND is_visible == true",
            "source_job": "fundamentals_sync",
            "note": "Fundamentals + visibility gates (delisted, shares, currency)",
        },
    ]

    # =========================================================================
    # CONSISTENCY CHECKS
    # =========================================================================
    inconsistencies = []

    # Check monotonic decreasing: each step count must be <= previous step count.
    for i in range(1, len(funnel_steps)):
        prev = funnel_steps[i - 1]
        curr = funnel_steps[i]
        if curr["count"] > prev["count"]:
            inconsistencies.append({
                "type": "funnel_increase",
                "step": curr["step"],
                "name": curr["name"],
                "count": curr["count"],
                "prev_step": prev["step"],
                "prev_name": prev["name"],
                "prev_count": prev["count"],
                "message": (
                    f"Step {curr['step']} ({curr['count']}) > "
                    f"Step {prev['step']} ({prev['count']})"
                ),
            })
            curr["warning"] = f"Exceeds step {prev['step']} ({prev['count']})"

    return {
        "generated_at": now_prague.isoformat(),
        "funnel_steps": funnel_steps,
        "inconsistencies": inconsistencies,
        "has_inconsistency": len(inconsistencies) > 0,

        # Primary counts — unambiguous, consistent field names.
        # These are the authoritative values for Admin UI and Talk.
        "counts": {
            # Canonical funnel
            "seeded":     seeded_total,      # exchange∈{NYSE,NASDAQ} AND asset_type=="Common Stock"
            "with_price": with_price_total,  # seeded AND has_price_data
            "classified": classified_total,  # with_price AND fundamentals_status=="complete"
            "visible":    visible_total,     # classified AND is_visible==true

            # Exchange breakdown (audit; scoped to seeded)
            "nyse":   nyse_count,
            "nasdaq": nasdaq_count,

            # Backward-compatibility aliases (do not use in new code)
            "common_stock":        seeded_total,
            "with_price_data":     with_price_total,
            "with_classification": classified_total,
            "visible_tickers":     visible_total,
        },

        # Step 3 ticker-level funnel (fundamentals sync):
        #   input  = with_price tickers entering step 3
        #   output = classified tickers (fundamentals_status=="complete")
        "step3_funnel": {
            "input_total":        with_price_total,
            "output_total":       classified_total,
            "filtered_out_total": max(with_price_total - classified_total, 0),
        },

        # For Talk filters compatibility
        "visible_universe_count": visible_total,
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
