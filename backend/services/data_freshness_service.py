"""
Data Freshness Service
======================
Returns freshness metrics for the admin pipeline dashboard:
  1. Events watermark status (last_events_checked_date)
  2. Fundamentals age distribution (company_fundamentals_cache)
  3. Pending fundamentals events queue depth
"""

from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

PRAGUE_TZ = ZoneInfo("Europe/Prague")


async def get_data_freshness(db) -> dict:
    """Aggregate data-freshness metrics from MongoDB."""
    events_watermark = await _get_events_watermark(db)
    fundamentals_age = await _get_fundamentals_age(db)
    pending_events = await _get_pending_events(db)
    return {
        "events_watermark": events_watermark,
        "fundamentals_age": fundamentals_age,
        "pending_events": pending_events,
    }


# ── B1: Events watermark status ─────────────────────────────────────────────

async def _get_events_watermark(db) -> dict:
    watermark_doc = await db.ops_config.find_one({"key": "last_events_checked_date"})
    if not watermark_doc or not watermark_doc.get("value"):
        return {"date": None, "days_behind": None, "status": "unknown"}

    watermark_date_str = watermark_doc["value"]
    watermark = date.fromisoformat(watermark_date_str)
    today = datetime.now(PRAGUE_TZ).date()

    # Count weekdays (trading days) between watermark+1 and today
    days_behind = sum(
        1
        for d in range((today - watermark).days)
        if (watermark + timedelta(days=d + 1)).weekday() < 5
    )

    if days_behind <= 1:
        status = "current"
    elif days_behind <= 5:
        status = "behind"
    else:
        status = "stale"

    return {
        "date": watermark_date_str,
        "days_behind": days_behind,
        "status": status,
    }


# ── B2: Fundamentals age distribution ───────────────────────────────────────

async def _get_fundamentals_age(db) -> dict:
    now = datetime.now(timezone.utc)
    week_ago = now - timedelta(days=7)
    month_ago = now - timedelta(days=30)

    pipeline = [
        {"$facet": {
            "total": [{"$count": "n"}],
            "fresh_7d": [
                {"$match": {"fundamentals_updated_at": {"$gte": week_ago}}},
                {"$count": "n"},
            ],
            "stale_7_30d": [
                {"$match": {"fundamentals_updated_at": {"$gte": month_ago, "$lt": week_ago}}},
                {"$count": "n"},
            ],
            "stale_30d_plus": [
                {"$match": {"fundamentals_updated_at": {"$lt": month_ago}}},
                {"$count": "n"},
            ],
            "never": [
                {"$match": {"$or": [
                    {"fundamentals_updated_at": None},
                    {"fundamentals_updated_at": {"$exists": False}},
                ]}},
                {"$count": "n"},
            ],
            "oldest": [
                {"$match": {"fundamentals_updated_at": {"$exists": True, "$ne": None}}},
                {"$sort": {"fundamentals_updated_at": 1}},
                {"$limit": 1},
                {"$project": {"_id": 0, "ticker": 1, "fundamentals_updated_at": 1}},
            ],
            "newest": [
                {"$match": {"fundamentals_updated_at": {"$exists": True, "$ne": None}}},
                {"$sort": {"fundamentals_updated_at": -1}},
                {"$limit": 1},
                {"$project": {"_id": 0, "ticker": 1, "fundamentals_updated_at": 1}},
            ],
        }}
    ]

    result = await db.company_fundamentals_cache.aggregate(pipeline).to_list(1)
    r = result[0] if result else {}

    def _n(key: str) -> int:
        return (r.get(key) or [{}])[0].get("n", 0)

    total = _n("total")

    def _pct(count: int) -> float:
        return round(count / total * 100, 1) if total else 0.0

    fresh = _n("fresh_7d")
    stale_7_30 = _n("stale_7_30d")
    stale_30 = _n("stale_30d_plus")
    never = _n("never")

    oldest_list = r.get("oldest") or []
    newest_list = r.get("newest") or []

    def _edge(lst: list) -> dict | None:
        if not lst:
            return None
        doc = lst[0]
        ts = doc.get("fundamentals_updated_at")
        return {
            "ticker": doc.get("ticker"),
            "date": ts.isoformat() if ts else None,
        }

    return {
        "total": total,
        "fresh_7d": {"count": fresh, "pct": _pct(fresh)},
        "stale_7_30d": {"count": stale_7_30, "pct": _pct(stale_7_30)},
        "stale_30d_plus": {"count": stale_30, "pct": _pct(stale_30)},
        "never_synced": {"count": never, "pct": _pct(never)},
        "oldest": _edge(oldest_list),
        "newest": _edge(newest_list),
    }


# ── B3: Pending fundamentals events queue ────────────────────────────────────

async def _get_pending_events(db) -> dict:
    pending_count = await db.fundamentals_events.count_documents({"status": "pending"})

    oldest_pending = await db.fundamentals_events.find_one(
        {"status": "pending"},
        {"_id": 0, "ticker": 1, "event_type": 1, "created_at": 1},
        sort=[("created_at", 1)],
    )

    type_breakdown: dict[str, int] = {}
    async for doc in db.fundamentals_events.aggregate([
        {"$match": {"status": "pending"}},
        {"$group": {"_id": "$event_type", "count": {"$sum": 1}}},
    ]):
        type_breakdown[doc["_id"]] = doc["count"]

    oldest_created_at = None
    oldest_ticker = None
    if oldest_pending:
        ts = oldest_pending.get("created_at")
        oldest_created_at = ts.isoformat() if ts else None
        oldest_ticker = oldest_pending.get("ticker")

    return {
        "count": pending_count,
        "oldest_created_at": oldest_created_at,
        "oldest_ticker": oldest_ticker,
        "by_type": type_breakdown,
    }
