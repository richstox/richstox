"""
Credit Log Service
==================
Logs every EODHD API call to the credit_logs collection for full auditability.

Schema: credit_logs
  request_id   : str      (UUID)
  job_name     : str      (e.g. "step2_splits", "price_backfill")
  operation    : str      (e.g. "bulk_splits", "price_history", "fundamentals")
  ticker       : str|None (None for bulk/exchange-wide calls)
  credits_used : int      (1 for price, 10 for fundamentals, 1 for bulk calls)
  duration_ms  : int      (wall-clock ms for the HTTP request)
  called_at    : datetime (UTC)
  api_endpoint : str      (full URL without api_token)
  http_status  : int      (200, 404, 429, ...)
  status       : str      ("success" | "error" | "429" | "mock")
"""

import uuid
import logging
from datetime import datetime, timezone, date
from typing import Optional, Dict, Any

logger = logging.getLogger("richstox.credit_log")


async def log_api_credit(
    db,
    *,
    job_name: str,
    operation: str,
    api_endpoint: str,
    credits_used: int,
    http_status: int,
    status: str,
    duration_ms: int,
    ticker: Optional[str] = None,
) -> None:
    """Insert one credit log record. Fire-and-forget — never raises."""
    try:
        await db.credit_logs.insert_one({
            "request_id": str(uuid.uuid4()),
            "job_name": job_name,
            "operation": operation,
            "ticker": ticker,
            "credits_used": credits_used,
            "duration_ms": duration_ms,
            "called_at": datetime.now(timezone.utc),
            "api_endpoint": api_endpoint,
            "http_status": http_status,
            "status": status,
        })
    except Exception as e:
        logger.warning(f"Failed to write credit log: {e}")


async def get_daily_credit_usage(db) -> Dict[str, Any]:
    """
    Return today's total credit consumption from credit_logs.
    Uses UTC midnight as the day boundary.
    """
    today_start = datetime.combine(date.today(), datetime.min.time()).replace(tzinfo=timezone.utc)

    pipeline = [
        {"$match": {"called_at": {"$gte": today_start}}},
        {
            "$group": {
                "_id": "$status",
                "total_credits": {"$sum": "$credits_used"},
                "total_calls": {"$sum": 1},
            }
        },
    ]
    rows = await db.credit_logs.aggregate(pipeline).to_list(20)

    summary: Dict[str, Any] = {
        "total_credits": 0,
        "total_calls": 0,
        "success_credits": 0,
        "error_calls": 0,
        "mock_calls": 0,
        "date": date.today().isoformat(),
    }
    for row in rows:
        s = row.get("_id") or "unknown"
        credits = row.get("total_credits", 0)
        calls = row.get("total_calls", 0)
        summary["total_credits"] += credits
        summary["total_calls"] += calls
        if s == "success":
            summary["success_credits"] = credits
        elif s == "error":
            summary["error_calls"] = calls
        elif s == "mock":
            summary["mock_calls"] = calls

    return summary


async def get_pipeline_sync_status(db) -> Dict[str, Any]:
    """
    Aggregate canonical pipeline/freshness state from tracked_tickers.
    Used by Admin Pipeline dashboard.
    """
    seeded_query = {
        "exchange": {"$in": ["NYSE", "NASDAQ"]},
        "asset_type": "Common Stock",
    }
    priced_query = {**seeded_query, "has_price_data": True}
    fundamentals_current_query = {
        **priced_query,
        "fundamentals_status": "complete",
        "needs_fundamentals_refresh": {"$ne": True},
        "fundamentals_updated_at": {"$nin": [None, ""], "$exists": True},
        "sector": {"$nin": [None, ""]},
        "industry": {"$nin": [None, ""]},
    }
    visible_query = {"is_visible": True}

    facet_result = await db.tracked_tickers.aggregate([
        {"$match": seeded_query},
        {"$facet": {
            "seeded_total":             [{"$count": "n"}],
            "visible_total":            [{"$match": {"is_visible": True}}, {"$count": "n"}],
            "missing_price_data":       [{"$match": {"has_price_data": {"$ne": True}}}, {"$count": "n"}],
            "missing_classification":   [{
                "$match": {
                    "has_price_data": True,
                    "$or": [
                        {"sector": {"$in": [None, ""]}},
                        {"industry": {"$in": [None, ""]}},
                        {"has_classification": {"$ne": True}},
                    ],
                },
            }, {"$count": "n"}],
            "pending_fundamentals":     [{
                "$match": {
                    "has_price_data": True,
                    "$or": [
                        {"fundamentals_status": {"$in": [None, "pending", "processing", "error"]}},
                        {"fundamentals_updated_at": {"$in": [None, ""]}},
                        {"fundamentals_updated_at": {"$exists": False}},
                    ],
                },
            }, {"$count": "n"}],
            "price_complete":           [{"$match": {**visible_query, "price_history_complete": True}}, {"$count": "n"}],
            "fundamentals_complete":    [{"$match": {**visible_query, **{
                "fundamentals_status": "complete",
                "needs_fundamentals_refresh": {"$ne": True},
                "fundamentals_updated_at": {"$nin": [None, ""], "$exists": True},
            }}}, {"$count": "n"}],
            "needs_price_redownload":   [{"$match": {"needs_price_redownload": True}},  {"$count": "n"}],
            "needs_fundamentals_refresh": [{"$match": {"needs_fundamentals_refresh": True}}, {"$count": "n"}],
            "non_visible_reasons": [
                {"$match": {"is_visible": {"$ne": True}, "visibility_failed_reason": {"$nin": [None, ""]}}},
                {"$group": {"_id": "$visibility_failed_reason", "count": {"$sum": 1}}},
            ],
        }},
    ]).to_list(1)

    f = facet_result[0] if facet_result else {}

    def _n(key: str) -> int:
        return (f.get(key) or [{}])[0].get("n", 0)

    seeded_total = _n("seeded_total")
    visible_total = _n("visible_total")
    price_complete = _n("price_complete")
    fundamentals_complete = _n("fundamentals_complete")
    missing_price_data = _n("missing_price_data")
    missing_classification = _n("missing_classification")
    pending_fundamentals = _n("pending_fundamentals")
    non_visible_total = max(seeded_total - visible_total, 0)
    visibility_failed_reasons = {
        row.get("_id"): row.get("count", 0)
        for row in (f.get("non_visible_reasons") or [])
        if row.get("_id")
    }

    price_freshest = await db.tracked_tickers.find_one(
        {**priced_query, "price_data_current_through": {"$nin": [None, ""]}},
        {"_id": 0, "price_data_current_through": 1},
        sort=[("price_data_current_through", -1)],
    )
    fundamentals_freshest = await db.tracked_tickers.find_one(
        fundamentals_current_query,
        {"_id": 0, "fundamentals_updated_at": 1},
        sort=[("fundamentals_updated_at", -1)],
    )
    oldest_price_refresh = await db.tracked_tickers.find_one(
        {**seeded_query, "needs_price_redownload": True, "price_refresh_requested_at": {"$ne": None}},
        {"_id": 0, "price_refresh_requested_at": 1},
        sort=[("price_refresh_requested_at", 1)],
    )
    oldest_fund_refresh = await db.tracked_tickers.find_one(
        {**seeded_query, "needs_fundamentals_refresh": True, "fundamentals_refresh_requested_at": {"$ne": None}},
        {"_id": 0, "fundamentals_refresh_requested_at": 1},
        sort=[("fundamentals_refresh_requested_at", 1)],
    )

    credits = await get_daily_credit_usage(db)

    return {
        "seeded_tickers": seeded_total,
        "visible_tickers": visible_total,
        "non_visible_tickers": non_visible_total,
        "missing_price_data": missing_price_data,
        "missing_classification": missing_classification,
        "pending_fundamentals": pending_fundamentals,
        "visibility_failed_reasons": visibility_failed_reasons,
        "price_data_updated_through": (price_freshest or {}).get("price_data_current_through"),
        "fundamentals_updated_through": (fundamentals_freshest or {}).get("fundamentals_updated_at"),
        "oldest_price_refresh_requested_at": (oldest_price_refresh or {}).get("price_refresh_requested_at"),
        "oldest_fundamentals_refresh_requested_at": (oldest_fund_refresh or {}).get("fundamentals_refresh_requested_at"),
        "total_visible_tickers": visible_total,
        "price_history_complete": price_complete,
        "price_history_pct": round(price_complete / visible_total * 100, 1) if visible_total else 0,
        "fundamentals_complete": fundamentals_complete,
        "fundamentals_pct": round(fundamentals_complete / visible_total * 100, 1) if visible_total else 0,
        "needs_price_redownload": _n("needs_price_redownload"),
        "needs_fundamentals_refresh": _n("needs_fundamentals_refresh"),
        "credits_today": credits["total_credits"],
        "credits_limit": 100_000,
        "credits_pct": round(credits["total_credits"] / 100_000 * 100, 1),
    }
