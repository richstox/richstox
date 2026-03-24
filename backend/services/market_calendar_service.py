"""
Market Calendar Service
=======================
Single source of truth for US trading days, market status, and session info.

Architecture:
- Persisted calendar rows in ``market_calendar`` MongoDB collection
- Runtime functions use only DB data + current time (no external API calls)
- Calendar refresh fetches EODHD exchange-details and generates rows
- Designed for future multi-market support (market key = "US" for now)

Session config (hardcoded conventions):
- Pre-market open:  04:00 ET
- Regular open:     from stored calendar row (default 09:30 ET)
- Regular close:    from stored calendar row or early close override (default 16:00 ET)
- After-hours close: 20:00 ET
- Timezone:         America/New_York
"""

import logging
import os
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Dict, List, Optional

import httpx
from zoneinfo import ZoneInfo

logger = logging.getLogger("richstox.market_calendar_service")

# ─── Constants ────────────────────────────────────────────────────────────────

EODHD_API_KEY = os.environ.get("EODHD_API_KEY", "")
EODHD_BASE_URL = "https://eodhd.com/api"

COLLECTION = "market_calendar"

# Session conventions (not sourced from API)
PRE_MARKET_OPEN = time(4, 0)       # 04:00 ET
AFTER_HOURS_CLOSE = time(20, 0)    # 20:00 ET

# Defaults when exchange details are unavailable
DEFAULT_REGULAR_OPEN = "09:30"
DEFAULT_REGULAR_CLOSE = "16:00"
DEFAULT_TIMEZONE = "America/New_York"

# Working days: Mon=0 .. Fri=4
DEFAULT_WORKING_DAYS = {0, 1, 2, 3, 4}

NY_TZ = ZoneInfo("America/New_York")


# ─── Index setup ──────────────────────────────────────────────────────────────

async def ensure_indexes(db) -> None:
    """Create indexes for the market_calendar collection."""
    coll = db[COLLECTION]
    await coll.create_index(
        [("market", 1), ("date", 1)],
        unique=True,
        name="market_date_unique",
    )
    await coll.create_index(
        [("market", 1), ("is_trading_day", 1), ("date", -1)],
        name="market_trading_day_lookup",
    )


# ─── Calendar Refresh (EODHD fetch + row generation) ─────────────────────────

async def refresh_market_calendar(
    db,
    market: str = "US",
    *,
    years_ahead: int = 2,
) -> Dict[str, Any]:
    """
    Fetch EODHD exchange-details for the given market and regenerate
    calendar rows for the date range [today - 2 years .. today + years_ahead].

    Returns summary dict with rows_written, api_timing, etc.
    """
    import time as _time

    api_start = _time.monotonic()
    exchange_details = await _fetch_exchange_details(market)
    api_elapsed_ms = round((_time.monotonic() - api_start) * 1000)

    # Parse exchange details
    tz_name = DEFAULT_TIMEZONE
    regular_open = DEFAULT_REGULAR_OPEN
    regular_close = DEFAULT_REGULAR_CLOSE
    holidays: Dict[str, str] = {}       # date_str -> holiday_name
    early_close_dates: Dict[str, str] = {}  # date_str -> early_close_time
    working_days = DEFAULT_WORKING_DAYS

    if exchange_details:
        tz_name = exchange_details.get("Timezone") or DEFAULT_TIMEZONE

        trading_hours = exchange_details.get("TradingHours") or {}
        if isinstance(trading_hours, dict):
            regular_open = trading_hours.get("Open") or DEFAULT_REGULAR_OPEN
            regular_close = trading_hours.get("Close") or DEFAULT_REGULAR_CLOSE
        elif isinstance(trading_hours, str):
            # Some payloads return "09:30-16:00" string
            parts = trading_hours.split("-")
            if len(parts) == 2:
                regular_open = parts[0].strip()
                regular_close = parts[1].strip()

        # Parse holidays
        raw_holidays = exchange_details.get("ExchangeHolidays") or {}
        if isinstance(raw_holidays, dict):
            for date_str, info in raw_holidays.items():
                name = info if isinstance(info, str) else (
                    info.get("Holiday") or info.get("Name") or info.get("name") or "Holiday"
                    if isinstance(info, dict) else "Holiday"
                )
                holidays[date_str] = name
        elif isinstance(raw_holidays, list):
            for item in raw_holidays:
                if isinstance(item, dict):
                    d = item.get("Date") or item.get("date") or ""
                    n = item.get("Holiday") or item.get("Name") or item.get("name") or "Holiday"
                    if d:
                        holidays[d] = n

        # Parse early close days
        raw_early = exchange_details.get("ExchangeEarlyCloseDays") or {}
        if isinstance(raw_early, dict):
            for date_str, close_time in raw_early.items():
                if isinstance(close_time, str):
                    early_close_dates[date_str] = close_time
                elif isinstance(close_time, dict):
                    early_close_dates[date_str] = (
                        close_time.get("Close") or close_time.get("close") or ""
                    )
        elif isinstance(raw_early, list):
            for item in raw_early:
                if isinstance(item, dict):
                    d = item.get("Date") or item.get("date") or ""
                    c = item.get("Close") or item.get("close") or ""
                    if d and c:
                        early_close_dates[d] = c

        # Parse working days
        raw_wd = exchange_details.get("WorkingDays")
        if isinstance(raw_wd, str):
            _day_map = {
                "Monday": 0, "Tuesday": 1, "Wednesday": 2,
                "Thursday": 3, "Friday": 4, "Saturday": 5, "Sunday": 6,
            }
            parsed = set()
            for part in raw_wd.split(","):
                day_name = part.strip()
                if day_name in _day_map:
                    parsed.add(_day_map[day_name])
            if parsed:
                working_days = parsed

    # Generate calendar rows
    today = date.today()
    start_date = today - timedelta(days=365 * 2)
    end_date = today + timedelta(days=365 * years_ahead)

    now_utc = datetime.now(timezone.utc)
    rows_written = 0
    bulk_ops = []

    current = start_date
    while current <= end_date:
        date_str = current.isoformat()
        is_weekend = current.weekday() not in working_days
        is_holiday = date_str in holidays
        is_trading = not is_weekend and not is_holiday

        early_close = early_close_dates.get(date_str)
        close_time = early_close if (is_trading and early_close) else regular_close

        doc = {
            "market": market,
            "date": date_str,
            "is_trading_day": is_trading,
            "trading_hours": {
                "open": regular_open,
                "close": close_time,
            } if is_trading else None,
            "holiday_name": holidays.get(date_str) if is_holiday else None,
            "early_close_time": early_close if (is_trading and early_close) else None,
            "timezone": tz_name,
            "updated_at": now_utc,
        }

        from pymongo import UpdateOne
        bulk_ops.append(
            UpdateOne(
                {"market": market, "date": date_str},
                {"$set": doc, "$setOnInsert": {"created_at": now_utc}},
                upsert=True,
            )
        )

        if len(bulk_ops) >= 500:
            result = await db[COLLECTION].bulk_write(bulk_ops, ordered=False)
            rows_written += result.upserted_count + result.modified_count
            bulk_ops = []

        current += timedelta(days=1)

    if bulk_ops:
        result = await db[COLLECTION].bulk_write(bulk_ops, ordered=False)
        rows_written += result.upserted_count + result.modified_count

    total_days = (end_date - start_date).days + 1
    summary = {
        "market": market,
        "status": "success",
        "rows_written": rows_written,
        "total_days_generated": total_days,
        "date_range": {"from": start_date.isoformat(), "to": end_date.isoformat()},
        "holidays_count": len(holidays),
        "early_close_count": len(early_close_dates),
        "api_elapsed_ms": api_elapsed_ms,
        "exchange_details_available": exchange_details is not None,
    }
    logger.info(
        "Market calendar refresh complete: market=%s rows=%d range=%s..%s",
        market, rows_written, start_date.isoformat(), end_date.isoformat(),
    )
    return summary


async def _fetch_exchange_details(market: str) -> Optional[Dict[str, Any]]:
    """
    Fetch EODHD exchange-details for the given market.
    Returns parsed JSON dict or None on failure.
    """
    if not EODHD_API_KEY:
        logger.warning("EODHD_API_KEY not set — using defaults for calendar generation")
        return None

    exchange_code = _market_to_exchange_code(market)
    url = f"{EODHD_BASE_URL}/exchange-details/{exchange_code}"
    params = {"api_token": EODHD_API_KEY, "fmt": "json"}

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            logger.info(
                "Fetched exchange details for %s (%s): HTTP %d",
                market, exchange_code, response.status_code,
            )
            return data
    except Exception as exc:
        logger.error("Failed to fetch exchange details for %s: %s", market, exc)
        return None


def _market_to_exchange_code(market: str) -> str:
    """Map market key to EODHD exchange code."""
    _MAP = {
        "US": "US",
        # Future: "LSE": "LSE", "TSE": "TSE"
    }
    return _MAP.get(market, market)


# ─── Runtime Query Functions ──────────────────────────────────────────────────
# These use ONLY persisted DB data + current time.
# No external API calls.

async def is_trading_day(db, date_str: str, market: str = "US") -> bool:
    """
    Returns whether the given date is a trading day.

    market_calendar is the single source of truth.  If no calendar row
    exists for the requested date, this returns ``False`` (fail-closed)
    rather than guessing via weekday heuristic.  This ensures we never
    silently approximate holidays or treat unsynced dates as trading days.

    Callers that need a different fallback should handle the ``None`` case
    via :func:`is_trading_day_or_none`.
    """
    doc = await db[COLLECTION].find_one(
        {"market": market, "date": date_str},
        {"is_trading_day": 1, "_id": 0},
    )
    if doc is not None:
        return bool(doc.get("is_trading_day", False))

    # No calendar row — fail closed.  Do NOT fall back to weekday heuristic.
    # market_calendar must be the only runtime source of truth.
    logger.debug(
        "is_trading_day: no calendar row for market=%s date=%s — returning False",
        market, date_str,
    )
    return False


async def last_n_trading_days(
    db, n: int, market: str = "US", *, before_date: Optional[str] = None,
) -> List[str]:
    """
    Returns the last N trading days in reverse chronological order.

    If before_date is provided, returns N trading days strictly before that date.
    Otherwise, returns N trading days up to and including today.
    """
    if before_date:
        date_filter = {"$lt": before_date}
    else:
        date_filter = {"$lte": date.today().isoformat()}

    docs = await db[COLLECTION].find(
        {"market": market, "is_trading_day": True, "date": date_filter},
        {"date": 1, "_id": 0},
    ).sort("date", -1).limit(n).to_list(n)

    return [d["date"] for d in docs]


async def last_n_completed_trading_days(
    db, n: int, market: str = "US",
) -> List[str]:
    """
    Returns the last N completed trading days only.

    A trading day is "completed" when the regular session has fully closed
    in the market's timezone.  If today is a trading day but the regular
    session has not yet ended, today is NOT included.

    For market="US" the effective timezone is America/New_York.
    The close time is read from the persisted market_calendar row for that
    exact date (respecting early close days).  If no calendar row exists
    for today, today is excluded (fail-closed).
    """
    now_et = datetime.now(NY_TZ)
    today_str = now_et.date().isoformat()

    # Determine if today's regular session is complete using persisted data
    today_doc = await db[COLLECTION].find_one(
        {"market": market, "date": today_str},
        {"is_trading_day": 1, "trading_hours": 1, "_id": 0},
    )

    include_today = False
    if today_doc and today_doc.get("is_trading_day"):
        # Close time MUST come from the calendar row — this correctly
        # handles early close days without any hardcoded fallback.
        trading_hours = today_doc.get("trading_hours")
        if trading_hours and trading_hours.get("close"):
            close_str = trading_hours["close"]
            try:
                close_h, close_m = map(int, close_str.split(":"))
                close_time = time(close_h, close_m)
                if now_et.time() >= close_time:
                    include_today = True
            except (ValueError, TypeError):
                # Malformed close time — don't include today (fail-closed)
                pass
        # If trading_hours or close is missing from the row, don't include
        # today.  This is fail-closed: we only include today if we can
        # confirm the session has ended.
    # If today is not a trading day or has no calendar row, don't include it

    if include_today:
        date_filter = {"$lte": today_str}
    else:
        date_filter = {"$lt": today_str}

    docs = await db[COLLECTION].find(
        {"market": market, "is_trading_day": True, "date": date_filter},
        {"date": 1, "_id": 0},
    ).sort("date", -1).limit(n).to_list(n)

    return [d["date"] for d in docs]


async def market_status_now(db, market: str = "US") -> Dict[str, Any]:
    """
    Return current market status including state, next transition, and label.

    States: CLOSED, PRE_MARKET, REGULAR, AFTER_HOURS
    """
    now_et = datetime.now(NY_TZ)
    today_str = now_et.date().isoformat()

    today_doc = await db[COLLECTION].find_one(
        {"market": market, "date": today_str},
        {"is_trading_day": 1, "trading_hours": 1, "early_close_time": 1, "_id": 0},
    )

    is_today_trading = bool(today_doc and today_doc.get("is_trading_day"))

    # Parse session times
    if is_today_trading:
        hours = today_doc.get("trading_hours") or {}
        open_str = hours.get("open", DEFAULT_REGULAR_OPEN)
        close_str = hours.get("close", DEFAULT_REGULAR_CLOSE)
    else:
        open_str = DEFAULT_REGULAR_OPEN
        close_str = DEFAULT_REGULAR_CLOSE

    try:
        oh, om = map(int, open_str.split(":"))
        regular_open = time(oh, om)
    except (ValueError, TypeError):
        regular_open = time(9, 30)

    try:
        ch, cm = map(int, close_str.split(":"))
        regular_close = time(ch, cm)
    except (ValueError, TypeError):
        regular_close = time(16, 0)

    now_time = now_et.time()

    # Determine state
    if not is_today_trading:
        state = "CLOSED"
        # Find next trading day
        next_td = await _next_trading_day(db, today_str, market)
        if next_td:
            next_open_dt = _make_et_datetime(next_td, PRE_MARKET_OPEN)
            label = _format_countdown("Opens", now_et, next_open_dt)
            next_transition = "PRE_MARKET"
            next_transition_at = next_open_dt.isoformat()
            seconds = max(0, int((next_open_dt - now_et).total_seconds()))
        else:
            label = "Market closed"
            next_transition = None
            next_transition_at = None
            seconds = 0
    elif now_time < PRE_MARKET_OPEN:
        state = "CLOSED"
        next_open_dt = _make_et_datetime(today_str, PRE_MARKET_OPEN)
        label = _format_countdown("Opens", now_et, next_open_dt)
        next_transition = "PRE_MARKET"
        next_transition_at = next_open_dt.isoformat()
        seconds = max(0, int((next_open_dt - now_et).total_seconds()))
    elif now_time < regular_open:
        state = "PRE_MARKET"
        next_open_dt = _make_et_datetime(today_str, regular_open)
        label = _format_countdown("Opens", now_et, next_open_dt)
        next_transition = "REGULAR"
        next_transition_at = next_open_dt.isoformat()
        seconds = max(0, int((next_open_dt - now_et).total_seconds()))
    elif now_time < regular_close:
        state = "REGULAR"
        next_close_dt = _make_et_datetime(today_str, regular_close)
        label = _format_countdown("Closes", now_et, next_close_dt)
        next_transition = "AFTER_HOURS"
        next_transition_at = next_close_dt.isoformat()
        seconds = max(0, int((next_close_dt - now_et).total_seconds()))
    elif now_time < AFTER_HOURS_CLOSE:
        state = "AFTER_HOURS"
        next_ah_dt = _make_et_datetime(today_str, AFTER_HOURS_CLOSE)
        label = _format_countdown("After-hours ends", now_et, next_ah_dt)
        next_transition = "CLOSED"
        next_transition_at = next_ah_dt.isoformat()
        seconds = max(0, int((next_ah_dt - now_et).total_seconds()))
    else:
        state = "CLOSED"
        next_td = await _next_trading_day(db, today_str, market)
        if next_td:
            next_open_dt = _make_et_datetime(next_td, PRE_MARKET_OPEN)
            label = _format_countdown("Opens", now_et, next_open_dt)
            next_transition = "PRE_MARKET"
            next_transition_at = next_open_dt.isoformat()
            seconds = max(0, int((next_open_dt - now_et).total_seconds()))
        else:
            label = "Market closed"
            next_transition = None
            next_transition_at = None
            seconds = 0

    return {
        "state": state,
        "next_transition": next_transition,
        "next_transition_at": next_transition_at,
        "time_to_next_transition_seconds": seconds,
        "label": label,
        "is_trading_day": is_today_trading,
        "trading_hours": {
            "open": open_str,
            "close": close_str,
        } if is_today_trading else None,
        "timezone": DEFAULT_TIMEZONE,
    }


async def market_open_closed_now(db, market: str = "US") -> Dict[str, Any]:
    """
    Simple open/closed check with time to close.

    Semantics: ``is_open`` means the **regular** market session is currently
    active (state == "REGULAR").  PRE_MARKET and AFTER_HOURS are NOT
    considered "open" for this endpoint — this matches the standard
    interpretation used by ticker detail pages and market status UIs.
    """
    status = await market_status_now(db, market)
    # is_open = regular session only (not pre-market or after-hours)
    is_open = status["state"] == "REGULAR"
    time_to_close = 0
    if is_open:
        time_to_close = status.get("time_to_next_transition_seconds", 0)
    return {
        "is_open": is_open,
        "state": status["state"],
        "time_to_close_seconds": time_to_close,
    }


# ─── Helpers ──────────────────────────────────────────────────────────────────

async def _next_trading_day(db, after_date: str, market: str) -> Optional[str]:
    """Find the next trading day strictly after the given date."""
    doc = await db[COLLECTION].find_one(
        {"market": market, "is_trading_day": True, "date": {"$gt": after_date}},
        {"date": 1, "_id": 0},
        sort=[("date", 1)],
    )
    return doc["date"] if doc else None


def _make_et_datetime(date_str: str, t: time) -> datetime:
    """Create a timezone-aware datetime in America/New_York."""
    d = date.fromisoformat(date_str)
    return datetime.combine(d, t, tzinfo=NY_TZ)


def _format_countdown(prefix: str, now: datetime, target: datetime) -> str:
    """Format a human-readable countdown label."""
    delta = target - now
    total_seconds = max(0, int(delta.total_seconds()))
    if total_seconds <= 0:
        return "Market closed"
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    if hours > 0:
        return f"{prefix} in {hours}h {minutes}m"
    return f"{prefix} in {minutes}m"


# ─── Admin metric helper ─────────────────────────────────────────────────────

async def get_last_10_completed_trading_days_health(db, market: str = "US") -> Dict[str, Any]:
    """
    OPS HEALTH metric: for each of the last 10 completed trading days,
    check if price_sync successfully processed that date.

    Returns:
    {
        "days": [
            {"date": "2026-03-23", "ok": true},
            {"date": "2026-03-20", "ok": true},
            ...
        ],
        "ok_count": 10,
        "missing_count": 0,
        "status": "green" | "yellow" | "red"
    }

    A day D is "ok" if ops_job_runs has at least one price_sync run where
    details.price_bulk_gapfill.days[] contains an entry with:
      - processed_date = D
      - status = "success"
      - rows_written > 0
    """
    # 1. Get last 10 completed trading days from market calendar
    completed_days = await last_n_completed_trading_days(db, 10, market)

    if not completed_days:
        return {
            "days": [],
            "ok_count": 0,
            "missing_count": 0,
            "status": "yellow",
        }

    # 2. Collect all successfully processed dates from ops_job_runs
    processed_dates = await _get_bulk_processed_dates_set(db)

    # 3. Check each completed trading day
    days_result = []
    missing_dates = []
    ok_count = 0
    for d in completed_days:
        ok = d in processed_dates
        days_result.append({"date": d, "ok": ok})
        if ok:
            ok_count += 1
        else:
            missing_dates.append(d)

    missing_count = len(missing_dates)

    # Status logic: green = all OK, yellow = 1-2 missing, red = 3+ missing
    if missing_count == 0:
        status = "green"
    elif missing_count <= 2:
        status = "yellow"
    else:
        status = "red"

    return {
        "days": days_result,
        "ok_count": ok_count,
        "missing_count": missing_count,
        "missing_dates": missing_dates,
        "status": status,
    }


async def _get_bulk_processed_dates_set(db) -> set:
    """
    Collect all dates successfully processed by price_sync
    from ops_job_runs.details.price_bulk_gapfill.days[].

    A date is considered processed if at least one run has an entry with:
      - processed_date = D
      - status = "success"
      - rows_written > 0

    We scan the most recent 50 successful price_sync runs (the $match
    filters to status=success/completed, so failed runs don't consume
    from this budget).  Each run typically covers 1-3 days of gapfill,
    so 50 runs safely covers 50+ distinct dates — well beyond the 10
    completed trading days this metric needs.  The limit exists only to
    bound the aggregation pipeline; it cannot cause the "last 10
    completed trading days" metric to miss a date unless the system has
    gone 50+ successful pipeline runs without covering it (which would
    already indicate a real gap).
    """
    pipeline = [
        {"$match": {
            "job_name": "price_sync",
            "status": {"$in": ["success", "completed"]},
            "details.price_bulk_gapfill.days": {"$exists": True},
        }},
        {"$sort": {"finished_at": -1}},
        {"$limit": 50},
        {"$project": {"_id": 0, "details.price_bulk_gapfill.days": 1}},
    ]

    dates: set = set()
    async for doc in db.ops_job_runs.aggregate(pipeline):
        details = doc.get("details") or {}
        gapfill = details.get("price_bulk_gapfill") or {}
        for day in gapfill.get("days", []):
            if (
                day.get("status") == "success"
                and day.get("processed_date")
                and (day.get("rows_written") or 0) > 0
            ):
                dates.add(day["processed_date"])

    return dates
