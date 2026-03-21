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
    Aggregate price_history and fundamentals completion status from tracked_tickers.
    Used by Admin Pipeline dashboard.
    """
    base_query = {
        "exchange": {"$in": ["NYSE", "NASDAQ"]},
        "asset_type": "Common Stock",
        "is_seeded": True,
        "has_price_data": True,
    }

    facet_result = await db.tracked_tickers.aggregate([
        {"$match": base_query},
        {"$facet": {
            "total":                    [{"$count": "n"}],
            "price_complete":           [{"$match": {"price_history_complete": True}},  {"$count": "n"}],
            "fundamentals_complete":    [{"$match": {"fundamentals_complete": True}},   {"$count": "n"}],
            "needs_price_redownload":   [{"$match": {"needs_price_redownload": True}},  {"$count": "n"}],
            "needs_fundamentals_refresh": [{"$match": {"needs_fundamentals_refresh": True}}, {"$count": "n"}],
        }},
    ]).to_list(1)

    f = facet_result[0] if facet_result else {}

    def _n(key: str) -> int:
        return (f.get(key) or [{}])[0].get("n", 0)

    total = _n("total")
    price_complete = _n("price_complete")
    fundamentals_complete = _n("fundamentals_complete")

    credits = await get_daily_credit_usage(db)
    pending_refresh_count = await db.tracked_tickers.count_documents(
        {"needs_fundamentals_refresh": True}
    )
    pending_events_audit = await db.fundamentals_events.count_documents({"status": "pending"})

    # Per-event-type pending counts for Step 2 detector cards
    pending_by_type_cursor = db.fundamentals_events.aggregate([
        {"$match": {"status": "pending"}},
        {"$group": {"_id": "$event_type", "count": {"$sum": 1}}},
    ])
    pending_by_type_raw = {doc["_id"]: doc["count"] async for doc in pending_by_type_cursor}
    pending_event_counts = {
        "split": pending_by_type_raw.get("split", 0),
        "dividend": pending_by_type_raw.get("dividend", 0),
        "earnings": pending_by_type_raw.get("earnings", 0),
    }

    return {
        "total_visible_tickers": total,
        "price_history_complete": price_complete,
        "price_history_pct": round(price_complete / total * 100, 1) if total else 0,
        "fundamentals_complete": fundamentals_complete,
        "fundamentals_pct": round(fundamentals_complete / total * 100, 1) if total else 0,
        "needs_price_redownload": _n("needs_price_redownload"),
        "needs_fundamentals_refresh": pending_refresh_count,
        "pending_events_audit": pending_events_audit,
        "pending_event_counts": pending_event_counts,
        "credits_today": credits["total_credits"],
        "credits_limit": 100_000,
        "credits_pct": round(credits["total_credits"] / 100_000 * 100, 1),
    }
