"""
Universe Counts Service (P48)
=============================
Single source of truth for universe/funnel counts.
Used by BOTH Admin Panel and Talk filters.

CANONICAL FUNNEL DEFINITION:
  raw        - All tickers from NYSE + NASDAQ exchanges (raw exchange universe)
  seeded     - NYSE/NASDAQ Common Stock (is_seeded == true / step1 definition)
  with_price - seeded + has_price_data == true
  classified - with_price + fundamentals_status == "complete"  (step3 output)
  visible    - classified + is_visible == true (scoped to classified for monotonicity)

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
        Dictionary with funnel steps and metadata.
        Key counts:
          counts.raw        - NYSE+NASDAQ raw exchange total
          counts.seeded     - seeded universe (Common Stock, is_seeded=True)
          counts.with_price - seeded tickers with current price data
          counts.classified - with_price tickers where fundamentals_status=="complete"
          counts.visible    - classified tickers where is_visible==true
    """
    now_prague = datetime.now(PRAGUE_TZ)

    # =========================================================================
    # FUNNEL STEP QUERIES
    # raw:        exchange ∈ {NYSE, NASDAQ}  (all asset types)
    # seeded:     is_seeded == True          (NYSE/NASDAQ Common Stock seed)
    # with_price: seeded + has_price_data
    # classified: with_price + fundamentals_status == "complete"  (step3 output)
    # visible:    classified + is_visible == True  (scoped for monotonicity)
    # =========================================================================
    raw_query        = {"exchange": {"$in": ["NYSE", "NASDAQ"]}}
    seeded_query     = {"is_seeded": True}
    with_price_query = {"is_seeded": True, "has_price_data": True}
    classified_query = {
        "is_seeded": True,
        "has_price_data": True,
        "fundamentals_status": "complete",
    }
    # Visible is scoped to classified so that visible <= classified always holds.
    visible_query    = {
        "is_seeded": True,
        "has_price_data": True,
        "fundamentals_status": "complete",
        "is_visible": True,
    }

    # =========================================================================
    # SINGLE ROUND-TRIP: $facet runs all counts in parallel on the server
    # =========================================================================
    facet_result = await db.tracked_tickers.aggregate([{"$facet": {
        "raw":          [{"$match": raw_query},        {"$count": "n"}],
        # NYSE/NASDAQ breakdown scoped to seeded so counts never exceed seeded_total
        "nyse":         [{"$match": {"is_seeded": True, "exchange": "NYSE"}},    {"$count": "n"}],
        "nasdaq":       [{"$match": {"is_seeded": True, "exchange": "NASDAQ"}},  {"$count": "n"}],
        "seeded":       [{"$match": seeded_query},     {"$count": "n"}],
        "with_price":   [{"$match": with_price_query}, {"$count": "n"}],
        "classified":   [{"$match": classified_query}, {"$count": "n"}],
        "visible":      [{"$match": visible_query},    {"$count": "n"}],
    }}]).to_list(1)

    f = facet_result[0] if facet_result else {}

    def _n(key: str) -> int:
        return (f.get(key) or [{}])[0].get("n", 0)

    raw_total        = _n("raw")
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
            "step": 0,
            "name": "Raw Exchange Universe (NYSE+NASDAQ)",
            "count": raw_total,
            "query": "exchange in [NYSE, NASDAQ]",
            "source_job": "universe_seed",
            "breakdown": f"NYSE: {nyse_count}, NASDAQ: {nasdaq_count}",
            "note": "Audit metadata only — includes all asset types",
        },
        {
            "step": 1,
            "name": "Seeded Universe (Common Stock)",
            "count": seeded_total,
            "query": "is_seeded == true",
            "source_job": "universe_seed",
        },
        {
            "step": 2,
            "name": "With Price Data",
            "count": with_price_total,
            "query": "is_seeded == true AND has_price_data == true",
            "source_job": "price_sync",
        },
        {
            "step": 3,
            "name": "With Fundamentals (Classified)",
            "count": classified_total,
            "query": "is_seeded == true AND has_price_data == true AND fundamentals_status == 'complete'",
            "source_job": "fundamentals_sync",
        },
        {
            "step": 4,
            "name": "Visible Tickers (Customer View)",
            "count": visible_total,
            "query": "classified == true AND is_visible == true",
            "source_job": "compute_visible_universe",
            "note": "All 7 visibility gates satisfied",
        },
    ]

    # =========================================================================
    # CONSISTENCY CHECKS
    # =========================================================================
    inconsistencies = []

    # Check monotonic decreasing: each step count must be <= previous step count.
    # raw (step 0) is expected to be >= seeded (step 1) since it includes all asset types.
    # Any step that increases over the previous is flagged as an inconsistency.
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
            # Funnel
            "raw":          raw_total,        # NYSE+NASDAQ all asset types
            "seeded":       seeded_total,      # is_seeded=True (Common Stock)
            "with_price":   with_price_total,  # seeded + has_price_data
            "classified":   classified_total,  # with_price + fundamentals_status=="complete"
            "visible":      visible_total,     # classified + is_visible=True

            # Exchange breakdown (audit; scoped to seeded)
            "nyse":   nyse_count,
            "nasdaq": nasdaq_count,

            # Backward-compatibility aliases (do not use in new code)
            # NOTE: seeded_us_total previously counted all NYSE+NASDAQ (all asset types).
            #       New code should use 'seeded' (Common Stock only) or 'raw' (all asset types).
            "seeded_us_total":      raw_total,       # legacy: all NYSE+NASDAQ regardless of type
            "common_stock":         seeded_total,    # legacy: Common Stock count (= seeded)
            "with_price_data":      with_price_total,
            "with_classification":  classified_total,
            "visible_tickers":      visible_total,
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
