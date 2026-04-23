# ==============================================================================
# 🛡️ SCHEDULER-ONLY FILE - EXTERNAL API CALLS ALLOWED
# ==============================================================================
# This file is in ALLOWLIST: /app/scripts/audit_external_calls.py
# May call: eodhd.com/api/* (via httpx)
# Context: ONLY from scheduler.py jobs or admin backfill endpoints
# Runtime API endpoints MUST NOT import from this file
# ==============================================================================
"""
RICHSTOX Dividend History Service
=================================
Fetches and stores historical dividend data from EODHD.

Collection:
- dividend_history: Stores all dividend payments per ticker

Used for:
- Calculating Dividend Yield TTM
- Dividends tab visualization (annual chart, YoY growth)
"""

import os
import logging
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional
from collections import Counter
from statistics import median, pstdev
from zoneinfo import ZoneInfo
import httpx
from pymongo import UpdateOne

logger = logging.getLogger("richstox.dividends")

EODHD_BASE_URL = "https://eodhd.com/api"
EODHD_API_KEY = os.getenv("EODHD_API_KEY", "")
UPCOMING_DIVIDEND_SOURCE = "eodhd_dividend_calendar"
# Product requirement: keep a rolling 90-day horizon for upcoming ex-dividend UX.
UPCOMING_DIVIDEND_WINDOW_DAYS = 90

UPCOMING_EARNINGS_SOURCE = "eodhd_earnings_calendar"
# Same 90-day horizon as dividends.
UPCOMING_EARNINGS_WINDOW_DAYS = 90

PRAGUE_TZ_NAME = "Europe/Prague"
MIN_PROVIDER_CONSENSUS_COUNT = 2
PROVIDER_CONSENSUS_RATIO = 0.6
MAX_FREQUENCY_CONSISTENCY_RATIO = 0.35
MAX_FREQUENCY_RELATIVE_ERROR = 0.35

_FREQUENCY_MAP = {
    "m": "Monthly",
    "month": "Monthly",
    "monthly": "Monthly",
    "q": "Quarterly",
    "quarter": "Quarterly",
    "quarterly": "Quarterly",
    "s": "Semiannual",
    "semiannual": "Semiannual",
    "semi-annual": "Semiannual",
    "biannual": "Semiannual",
    "half-year": "Semiannual",
    "half yearly": "Semiannual",
    "irregular": "Irregular",
    "special": "Special",
}


def _parse_date_ymd(value: Any) -> Optional[str]:
    if not value or not isinstance(value, str):
        return None
    raw = value.strip()
    if not raw:
        return None
    if len(raw) >= 10:
        raw = raw[:10]
    try:
        datetime.strptime(raw, "%Y-%m-%d")
        return raw
    except ValueError:
        return None


def _normalize_ticker_symbol(value: Any) -> Optional[str]:
    if not value or not isinstance(value, str):
        return None
    symbol = value.strip().upper()
    if not symbol:
        return None
    return symbol if "." in symbol else f"{symbol}.US"


def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not (parsed > 0):
        return None
    return round(parsed, 6)


def _normalize_frequency_label(raw: Any) -> Optional[str]:
    if not raw or not isinstance(raw, str):
        return None
    normalized = raw.strip().lower().replace("_", " ").replace("-", " ")
    normalized = " ".join(normalized.split())
    if normalized in _FREQUENCY_MAP:
        return _FREQUENCY_MAP[normalized]
    if "quarter" in normalized:
        return "Quarterly"
    if "semi" in normalized or "biannual" in normalized or "half" in normalized:
        return "Semiannual"
    if "month" in normalized:
        return "Monthly"
    if "irregular" in normalized:
        return "Irregular"
    if "special" in normalized:
        return "Special"
    return None


def _event_flags(div: Dict[str, Any]) -> tuple[bool, bool]:
    """Return tuple (is_special, is_irregular) inferred from event metadata fields."""
    div_type = str(div.get("dividend_type") or div.get("type") or "").lower()
    period = str(div.get("period") or div.get("frequency") or "").lower()
    is_special = bool(div.get("is_special")) or "special" in div_type or "special" in period
    is_irregular = bool(div.get("is_irregular")) or "irregular" in div_type or "irregular" in period
    return is_special, is_irregular


async def fetch_dividends_from_eodhd(ticker: str, from_date: str = None) -> List[Dict[str, Any]]:
    """
    Fetch dividend history from EODHD API.
    Cost: 1 credit per request.
    
    Args:
        ticker: Stock ticker (e.g., "AAPL" or "AAPL.US")
        from_date: Start date (YYYY-MM-DD), defaults to 10 years ago
    
    Returns:
        List of dividend records
    """
    if not EODHD_API_KEY:
        logger.error("EODHD_API_KEY not configured")
        return []
    
    ticker_full = ticker if ticker.endswith(".US") else f"{ticker}.US"
    
    if not from_date:
        from_date = (datetime.now() - timedelta(days=365 * 10)).strftime("%Y-%m-%d")
    
    url = f"{EODHD_BASE_URL}/div/{ticker_full}"
    params = {
        "api_token": EODHD_API_KEY,
        "fmt": "json",
        "from": from_date,
    }
    
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.get(url, params=params)
            
            if response.status_code == 404:
                logger.info(f"No dividends for {ticker}")
                return []
            
            response.raise_for_status()
            data = response.json()
            
            if not isinstance(data, list):
                return []
            
            return data
    except Exception as e:
        logger.error(f"Failed to fetch dividends for {ticker}: {e}")
        return []


def parse_dividend_records(ticker: str, dividends: List[Dict]) -> List[Dict[str, Any]]:
    """
    Parse EODHD dividend data into normalized records.
    """
    now = datetime.now(timezone.utc)
    ticker_full = ticker if ticker.endswith(".US") else f"{ticker}.US"
    
    records = []
    for div in dividends:
        ex_date = _parse_date_ymd(div.get("date") or div.get("exDate") or div.get("ex_date"))
        if not ex_date:
            continue

        amount = _safe_float(div.get("value") or div.get("dividend") or div.get("amount"))
        if amount is None:
            continue
        period_raw = div.get("period") or div.get("frequency")
        type_raw = div.get("type") or div.get("dividend_type")
        frequency_label = _normalize_frequency_label(period_raw) or _normalize_frequency_label(type_raw)
        is_special, is_irregular = _event_flags({
            "period": period_raw,
            "dividend_type": type_raw,
            "is_special": div.get("is_special"),
            "is_irregular": div.get("is_irregular"),
        })

        records.append({
            "ticker": ticker_full,
            "ex_date": ex_date,
            "payment_date": _parse_date_ymd(div.get("paymentDate") or div.get("payment_date") or div.get("payDate")),
            "record_date": _parse_date_ymd(div.get("recordDate") or div.get("record_date")),
            "declaration_date": _parse_date_ymd(div.get("declarationDate") or div.get("declaration_date")),
            "amount": amount,
            "unadjusted_amount": div.get("unadjustedValue"),
            "currency": (div.get("currency") or "USD"),
            "period": period_raw,
            "dividend_type": type_raw,
            "frequency_label": frequency_label,
            "is_special": is_special,
            "is_irregular": is_irregular,
            "created_at": now,
        })
    
    return records


async def sync_ticker_dividends(db, ticker: str) -> Dict[str, Any]:
    """
    Sync dividend history for a single ticker.
    
    Returns:
        Summary of sync operation.
    """
    ticker_upper = ticker.upper()
    ticker_full = ticker_upper if ticker_upper.endswith(".US") else f"{ticker_upper}.US"
    
    result = {
        "ticker": ticker_full,
        "success": False,
        "dividends_synced": 0,
        "error": None,
    }
    
    # Fetch from EODHD
    dividends = await fetch_dividends_from_eodhd(ticker_upper)
    
    if not dividends:
        result["message"] = "No dividend data (stock may not pay dividends)"
        result["success"] = True  # Not an error, just no dividends
        return result
    
    try:
        records = parse_dividend_records(ticker_upper, dividends)
        
        if records:
            # Delete old records and insert new
            await db.dividend_history.delete_many({"ticker": ticker_full})
            await db.dividend_history.insert_many(records)
            result["dividends_synced"] = len(records)
        
        result["success"] = True
        logger.info(f"Synced {len(records)} dividends for {ticker_full}")
        
    except Exception as e:
        result["error"] = str(e)
        logger.error(f"Error syncing dividends for {ticker_full}: {e}")
    
    return result


async def sync_batch_dividends(
    db,
    tickers: List[str],
    delay_between_requests: float = 0.2
) -> Dict[str, Any]:
    """
    Sync dividend history for multiple tickers.
    
    Args:
        db: MongoDB database
        tickers: List of tickers to sync
        delay_between_requests: Delay in seconds between API calls
    
    Returns:
        Summary of batch operation.
    """
    import asyncio
    
    result = {
        "started_at": datetime.now(timezone.utc).isoformat(),
        "total_tickers": len(tickers),
        "success": 0,
        "failed": 0,
        "total_dividends": 0,
        "api_calls": 0,
    }
    
    for ticker in tickers:
        ticker_result = await sync_ticker_dividends(db, ticker)
        result["api_calls"] += 1
        
        if ticker_result["success"]:
            result["success"] += 1
            result["total_dividends"] += ticker_result.get("dividends_synced", 0)
        else:
            result["failed"] += 1
        
        await asyncio.sleep(delay_between_requests)
    
    result["finished_at"] = datetime.now(timezone.utc).isoformat()
    
    # Create index
    await db.dividend_history.create_index([("ticker", 1), ("ex_date", -1)])
    await db.dividend_history.create_index("ex_date")
    
    return result


async def calculate_dividend_yield_ttm(db, ticker: str, current_price: float) -> Optional[float]:
    """
    Calculate trailing 12-month dividend yield.
    
    Formula: sum(dividends_last_365_days) / current_price * 100
    
    Supports both field formats:
    - Legacy: ex_date, amount
    - Backfill: date, value
    
    Returns:
        Dividend yield as percentage, or None if no data.
    """
    ticker_full = ticker if ticker.endswith(".US") else f"{ticker}.US"
    
    one_year_ago = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
    
    # Query using $or to support both field formats
    cursor = db.dividend_history.find({
        "ticker": ticker_full,
        "$or": [
            {"ex_date": {"$gte": one_year_ago}},
            {"date": {"$gte": one_year_ago}}
        ]
    })
    
    dividends = await cursor.to_list(length=100)
    
    if not dividends:
        return None
    
    # Sum amounts using either field name
    total_dividends = sum(
        d.get("amount") or d.get("value") or 0 
        for d in dividends
    )
    
    if current_price <= 0 or total_dividends <= 0:
        return None
    
    dividend_yield_ttm = (total_dividends / current_price) * 100
    return round(dividend_yield_ttm, 4)


def _detect_dividend_frequency(events: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Detect dividend frequency from the last 12 months of ex-dated events.

    Preference order:
    1) reliable provider metadata labels (Monthly/Quarterly/Semiannual),
    2) inferred cadence from interval consistency between regular events,
    3) Special/Irregular fallback when cadence is not comparable.
    """
    now = datetime.now(timezone.utc)
    lookback_start = now - timedelta(days=365)

    enriched = []
    for event in events:
        ex_date = _parse_date_ymd(event.get("ex_date"))
        if not ex_date:
            continue
        ex_dt = datetime.strptime(ex_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        if ex_dt < lookback_start or ex_dt > now + timedelta(days=1):
            continue
        is_special, is_irregular = _event_flags(event)
        enriched.append({
            **event,
            "ex_date": ex_date,
            "ex_dt": ex_dt,
            "is_special": is_special,
            "is_irregular": is_irregular,
        })

    has_special = any(e["is_special"] for e in enriched)
    has_irregular = any(e["is_irregular"] for e in enriched)
    regular_events = [e for e in enriched if not e["is_special"] and not e["is_irregular"]]

    if len(regular_events) == 0:
        if has_special and not has_irregular:
            return {"label": "Special", "source": "event_flags", "has_special": True, "has_irregular": False}
        if has_irregular:
            return {"label": "Irregular", "source": "event_flags", "has_special": has_special, "has_irregular": True}
        return {"label": "Irregular", "source": "insufficient_data", "has_special": False, "has_irregular": False}

    provider_labels = []
    for event in regular_events:
        provider_label = (
            _normalize_frequency_label(event.get("frequency_label"))
            or _normalize_frequency_label(event.get("period"))
            or _normalize_frequency_label(event.get("dividend_type"))
            or _normalize_frequency_label(event.get("frequency"))
            or _normalize_frequency_label(event.get("type"))
        )
        if provider_label in {"Monthly", "Quarterly", "Semiannual"}:
            provider_labels.append(provider_label)

    if provider_labels:
        counts = Counter(provider_labels)
        top_label, top_count = counts.most_common(1)[0]
        if (
            top_count >= MIN_PROVIDER_CONSENSUS_COUNT
            and top_count / len(provider_labels) >= PROVIDER_CONSENSUS_RATIO
        ):
            return {
                "label": top_label,
                "source": "provider_metadata",
                "has_special": has_special,
                "has_irregular": has_irregular,
            }

    if len(regular_events) < 2:
        return {
            "label": "Irregular",
            "source": "insufficient_data",
            "has_special": has_special,
            "has_irregular": has_irregular,
        }

    regular_sorted = sorted(regular_events, key=lambda x: x["ex_dt"])
    intervals = []
    for idx in range(1, len(regular_sorted)):
        diff = (regular_sorted[idx]["ex_dt"] - regular_sorted[idx - 1]["ex_dt"]).days
        if diff > 0:
            intervals.append(diff)

    if not intervals:
        return {
            "label": "Irregular",
            "source": "insufficient_data",
            "has_special": has_special,
            "has_irregular": has_irregular,
        }

    interval_mean = sum(intervals) / len(intervals)
    interval_median = median(intervals)
    interval_stdev = pstdev(intervals) if len(intervals) > 1 else 0.0
    consistency_ratio = interval_stdev / interval_mean if interval_mean > 0 else 1.0

    # Approximate Gregorian average spacing (365.25-day year), rounded.
    expected = {
        "Monthly": 30.4,
        "Quarterly": 91.3,
        "Semiannual": 182.6,
    }
    best_label = min(expected.keys(), key=lambda k: abs(interval_median - expected[k]))
    relative_error = abs(interval_median - expected[best_label]) / expected[best_label]

    if (
        consistency_ratio <= MAX_FREQUENCY_CONSISTENCY_RATIO
        and relative_error <= MAX_FREQUENCY_RELATIVE_ERROR
    ):
        return {
            "label": best_label,
            "source": "inferred",
            "has_special": has_special,
            "has_irregular": has_irregular,
        }

    return {
        "label": "Irregular",
        "source": "inferred",
        "has_special": has_special,
        "has_irregular": has_irregular,
    }


def _select_next_upcoming_event(events: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    candidates = []
    for row in events:
        ex_date = _parse_date_ymd(row.get("next_ex_date") or row.get("ex_date") or row.get("exDate") or row.get("date"))
        if not ex_date or ex_date < today:
            continue
        amount = _safe_float(row.get("next_dividend_amount") or row.get("dividend") or row.get("amount") or row.get("value"))
        event = {
            "next_ex_date": ex_date,
            "next_pay_date": _parse_date_ymd(row.get("next_pay_date") or row.get("paymentDate") or row.get("payment_date") or row.get("payDate")),
            "next_dividend_amount": amount,
            "next_dividend_currency": row.get("next_dividend_currency") or row.get("currency"),
            "dividend_type": row.get("type") or row.get("dividend_type"),
            "period": row.get("period") or row.get("frequency"),
        }
        is_special, is_irregular = _event_flags(event)
        event["is_special"] = is_special
        event["is_irregular"] = is_irregular
        event["event_type_label"] = "Special dividend" if is_special else ("Irregular dividend" if is_irregular else None)
        candidates.append(event)
    if not candidates:
        return None
    candidates.sort(key=lambda e: e["next_ex_date"])
    return candidates[0]


async def create_upcoming_dividends_indexes(db) -> None:
    """Create indexes for the upcoming_dividends collection.

    Called once at server startup, not on every job run.
    Motor/PyMongo make create_index idempotent so it is safe to call on each
    cold start without rebuilding existing indexes.
    """
    await db.upcoming_dividends.create_index(
        [("ticker", 1)], unique=True, name="upcoming_dividends_ticker_unique"
    )
    await db.upcoming_dividends.create_index(
        [("next_ex_date", 1)], name="upcoming_dividends_next_ex_date"
    )


async def sync_upcoming_dividend_calendar_for_visible_tickers(db) -> Dict[str, Any]:
    if not EODHD_API_KEY:
        logger.error("[dividend_upcoming_calendar] EODHD_API_KEY not configured")
        return {"success": False, "error": "EODHD_API_KEY not configured"}

    now = datetime.now(timezone.utc)
    window_start = now.strftime("%Y-%m-%d")
    window_end = (now + timedelta(days=UPCOMING_DIVIDEND_WINDOW_DAYS)).strftime("%Y-%m-%d")

    # Canonical universe source: tracked_tickers.is_visible.
    visible_tickers_raw = await db.tracked_tickers.distinct("ticker", {"is_visible": True})
    visible_tickers = {_normalize_ticker_symbol(t) for t in visible_tickers_raw if _normalize_ticker_symbol(t)}

    url = f"{EODHD_BASE_URL}/calendar/dividends"
    params = {
        "api_token": EODHD_API_KEY,
        "fmt": "json",
        "from": window_start,
        "to": window_end,
    }

    async with httpx.AsyncClient(timeout=45) as client:
        response = await client.get(url, params=params)
        response.raise_for_status()
        payload = response.json()

    if isinstance(payload, dict):
        rows = payload.get("dividends") or payload.get("data") or []
    elif isinstance(payload, list):
        rows = payload
    else:
        rows = []

    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        ticker = _normalize_ticker_symbol(
            row.get("Code")
            or row.get("code")
            or row.get("symbol")
            or row.get("ticker")
        )
        if not ticker:
            continue
        if visible_tickers and ticker not in visible_tickers:
            continue
        grouped.setdefault(ticker, []).append(row)

    write_ops: List[UpdateOne] = []
    for ticker, ticker_rows in grouped.items():
        next_event = _select_next_upcoming_event(ticker_rows)
        if not next_event:
            continue
        write_ops.append(
            UpdateOne(
                {"ticker": ticker},
                {"$set": {
                    "ticker": ticker,
                    "next_ex_date": next_event["next_ex_date"],
                    "next_pay_date": next_event.get("next_pay_date"),
                    "next_dividend_amount": next_event.get("next_dividend_amount"),
                    "next_dividend_currency": next_event.get("next_dividend_currency"),
                    "dividend_type": next_event.get("dividend_type"),
                    "period": next_event.get("period"),
                    "is_special": bool(next_event.get("is_special")),
                    "is_irregular": bool(next_event.get("is_irregular")),
                    "source": UPCOMING_DIVIDEND_SOURCE,
                    "fetched_at": now,
                    "window_start": window_start,
                    "window_end": window_end,
                }},
                upsert=True,
            )
        )

    null_tickers = sorted(visible_tickers - set(grouped.keys()))
    for ticker in null_tickers:
        write_ops.append(
            UpdateOne(
                {"ticker": ticker},
                {"$set": {
                    "ticker": ticker,
                    "next_ex_date": None,
                    "next_pay_date": None,
                    "next_dividend_amount": None,
                    "next_dividend_currency": None,
                    "dividend_type": None,
                    "period": None,
                    "is_special": False,
                    "is_irregular": False,
                    "source": UPCOMING_DIVIDEND_SOURCE,
                    "fetched_at": now,
                    "window_start": window_start,
                    "window_end": window_end,
                }},
                upsert=True,
            )
        )

    if write_ops:
        await db.upcoming_dividends.bulk_write(write_ops, ordered=False)

    return {
        "success": True,
        "source": UPCOMING_DIVIDEND_SOURCE,
        "window_start": window_start,
        "window_end": window_end,
        "visible_tickers": len(visible_tickers),
        "tickers_with_upcoming": len(grouped),
        "tickers_without_upcoming": len(null_tickers),
        "records_written": len(write_ops),
    }


async def get_dividend_history_for_ticker(
    db,
    ticker: str,
    years: int = 10
) -> Dict[str, Any]:
    """
    Get dividend history with annual aggregation for UI.
    
    Supports both field formats:
    - Legacy: ex_date, amount (from sync_ticker_dividends)
    - Backfill: date, value (from backfill_dividends.py)
    
    Returns:
        {
            "ticker": str,
            "annual_dividends": [...],  # For bar chart
            "history": [...],           # All dividend records
            "recent_payments": [...],   # Last 8 payments
            "yoy_growth": float,        # Year-over-year growth %
            "status": "growing" | "stable" | "declining" | "no_dividends"
        }
    """
    ticker_full = ticker if ticker.endswith(".US") else f"{ticker}.US"
    now = datetime.now(timezone.utc)
    
    from_date = (now - timedelta(days=365 * years)).strftime("%Y-%m-%d")
    
    # Query using $or to support both field formats
    cursor = db.dividend_history.find(
        {
            "ticker": ticker_full,
            "$or": [
                {"ex_date": {"$gte": from_date}},
                {"date": {"$gte": from_date}}
            ]
        },
        {"_id": 0}
    ).sort([("ex_date", -1), ("date", -1)])
    
    dividends = await cursor.to_list(length=500)
    
    if not dividends:
        upcoming = await db.upcoming_dividends.find_one({"ticker": ticker_full}, {"_id": 0})
        return {
            "ticker": ticker_full,
            "annual_dividends": [],
            "history": [],
            "recent_payments": [],
            "next_dividend": {
                "next_ex_date": upcoming.get("next_ex_date") if upcoming else None,
                "next_pay_date": upcoming.get("next_pay_date") if upcoming else None,
                "next_dividend_amount": upcoming.get("next_dividend_amount") if upcoming else None,
                "next_dividend_currency": upcoming.get("next_dividend_currency") if upcoming else None,
                "event_type_label": (
                    "Special dividend" if upcoming and upcoming.get("is_special")
                    else ("Irregular dividend" if upcoming and upcoming.get("is_irregular") else None)
                ),
            },
            "frequency": {
                "label": "Irregular",
                "source": "insufficient_data",
                "has_special": False,
                "has_irregular": False,
            },
            "yoy_growth": None,
            "status": "no_dividends",
            "total_years": 0,
        }
    
    # Normalize records to consistent format
    normalized = []
    for div in dividends:
        # Get date from either field
        div_date = div.get("ex_date") or div.get("date")
        # Get amount from either field
        amount = div.get("amount") or div.get("value") or 0
        
        if div_date and amount > 0:
            period_raw = div.get("period") or div.get("frequency")
            type_raw = div.get("dividend_type") or div.get("type")
            is_special, is_irregular = _event_flags({
                "period": period_raw,
                "dividend_type": type_raw,
                "is_special": div.get("is_special"),
                "is_irregular": div.get("is_irregular"),
            })
            normalized.append({
                "ex_date": div_date,
                "amount": amount,
                "payment_date": div.get("payment_date") or div.get("paymentDate"),
                "currency": div.get("currency", "USD"),
                "period": period_raw,
                "dividend_type": type_raw,
                "frequency_label": _normalize_frequency_label(div.get("frequency_label")) or _normalize_frequency_label(period_raw),
                "is_special": is_special,
                "is_irregular": is_irregular,
                "event_type_label": "Special dividend" if is_special else ("Irregular dividend" if is_irregular else None),
            })
    
    # Sort by date descending
    normalized.sort(key=lambda x: x["ex_date"], reverse=True)
    
    if not normalized:
        upcoming = await db.upcoming_dividends.find_one({"ticker": ticker_full}, {"_id": 0})
        return {
            "ticker": ticker_full,
            "annual_dividends": [],
            "history": [],
            "recent_payments": [],
            "next_dividend": {
                "next_ex_date": upcoming.get("next_ex_date") if upcoming else None,
                "next_pay_date": upcoming.get("next_pay_date") if upcoming else None,
                "next_dividend_amount": upcoming.get("next_dividend_amount") if upcoming else None,
                "next_dividend_currency": upcoming.get("next_dividend_currency") if upcoming else None,
                "event_type_label": (
                    "Special dividend" if upcoming and upcoming.get("is_special")
                    else ("Irregular dividend" if upcoming and upcoming.get("is_irregular") else None)
                ),
            },
            "frequency": {
                "label": "Irregular",
                "source": "insufficient_data",
                "has_special": False,
                "has_irregular": False,
            },
            "yoy_growth": None,
            "status": "no_dividends",
            "total_years": 0,
        }
    
    # Group by year
    by_year = {}
    current_year = now.year
    
    for div in normalized:
        year = div["ex_date"][:4]
        year_int = int(year)
        if year_int not in by_year:
            by_year[year_int] = []
        by_year[year_int].append(div)
    
    # Build annual totals
    annual_dividends = []
    for year in sorted(by_year.keys()):
        year_divs = by_year[year]
        total = sum(d["amount"] for d in year_divs)
        count = len(year_divs)
        
        annual_dividends.append({
            "year": year,
            "total": round(total, 4),
            "payment_count": count,
            "is_partial": year == current_year,
        })
    
    # Calculate YoY growth (compare last complete year to previous)
    yoy_growth = None
    if len(annual_dividends) >= 2:
        # Find last two complete years
        complete_years = [a for a in annual_dividends if not a["is_partial"]]
        if len(complete_years) >= 2:
            latest = complete_years[-1]["total"]
            previous = complete_years[-2]["total"]
            if previous > 0:
                yoy_growth = round(((latest - previous) / previous) * 100, 2)
    
    # Determine status
    if not annual_dividends:
        status = "no_dividends"
    elif yoy_growth is not None:
        if yoy_growth > 5:
            status = "growing"
        elif yoy_growth < -5:
            status = "declining"
        else:
            status = "stable"
    else:
        status = "stable"
    
    frequency = _detect_dividend_frequency(normalized)
    display_currency = None
    for item in normalized:
        currency = item.get("currency")
        if isinstance(currency, str) and currency.strip():
            display_currency = currency.strip().upper()
            break

    today = now.strftime("%Y-%m-%d")
    upcoming_history_event = next(
        (
            event for event in normalized
            if event["ex_date"] >= today
        ),
        None,
    )

    upcoming = await db.upcoming_dividends.find_one({"ticker": ticker_full}, {"_id": 0})
    next_dividend = {
        "next_ex_date": (
            upcoming.get("next_ex_date")
            if upcoming and upcoming.get("next_ex_date")
            else (upcoming_history_event.get("ex_date") if upcoming_history_event else None)
        ),
        "next_pay_date": (
            upcoming.get("next_pay_date")
            if upcoming and upcoming.get("next_pay_date")
            else (upcoming_history_event.get("payment_date") if upcoming_history_event else None)
        ),
        "next_dividend_amount": (
            upcoming.get("next_dividend_amount")
            if upcoming and upcoming.get("next_dividend_amount") is not None
            else (upcoming_history_event.get("amount") if upcoming_history_event else None)
        ),
        "next_dividend_currency": (
            upcoming.get("next_dividend_currency")
            if upcoming and upcoming.get("next_dividend_currency")
            else (upcoming_history_event.get("currency") if upcoming_history_event else display_currency)
        ),
        "event_type_label": (
            "Special dividend" if upcoming and upcoming.get("is_special")
            else ("Irregular dividend" if upcoming and upcoming.get("is_irregular") else (
                upcoming_history_event.get("event_type_label") if upcoming_history_event else None
            ))
        ),
        "source": upcoming.get("source") if upcoming else ("dividend_history" if upcoming_history_event else None),
        "fetched_at": upcoming.get("fetched_at") if upcoming else None,
        "window_start": upcoming.get("window_start") if upcoming else None,
        "window_end": upcoming.get("window_end") if upcoming else None,
    }

    return {
        "ticker": ticker_full,
        "annual_dividends": annual_dividends,
        "history": normalized,
        "recent_payments": normalized[:10],  # Last 10 payments
        "next_dividend": next_dividend,
        "frequency": frequency,
        "display_currency": display_currency,
        "yoy_growth": yoy_growth,
        "status": status,
        "total_years": len(annual_dividends),
    }


async def get_dividend_stats(db) -> Dict[str, Any]:
    """Get statistics about dividend_history collection."""
    total_records = await db.dividend_history.count_documents({})
    unique_tickers = await db.dividend_history.distinct("ticker")
    
    return {
        "total_records": total_records,
        "unique_tickers": len(unique_tickers),
        "sample_tickers": unique_tickers[:10] if unique_tickers else [],
    }


# ---------------------------------------------------------------------------
# Daily automated sync — called from scheduler.py
# ---------------------------------------------------------------------------
DIVIDEND_RESYNC_DAYS = 7  # Re-fetch dividend history every 7 days


async def sync_dividends_for_visible_tickers(db) -> Dict[str, Any]:
    """
    Daily job: sync dividend_history for all visible tickers.

    Logic:
    1. Get all visible tickers from tracked_tickers.
    2. Skip tickers whose dividends_synced_at is < DIVIDEND_RESYNC_DAYS old.
    3. For remaining tickers, call sync_ticker_dividends and stamp
       dividends_synced_at + dividends_sync_status on tracked_tickers.

    Returns summary dict suitable for ops logging.
    """
    import asyncio

    now = datetime.now(timezone.utc)
    resync_cutoff = now - timedelta(days=DIVIDEND_RESYNC_DAYS)

    # All visible tickers
    cursor = db.tracked_tickers.find(
        {"is_visible": True},
        {"_id": 0, "ticker": 1, "dividends_synced_at": 1},
    )
    all_visible = await cursor.to_list(length=10_000)

    # Filter to those needing (re-)sync
    pending = []
    for doc in all_visible:
        last_sync = doc.get("dividends_synced_at")
        if last_sync is not None and getattr(last_sync, "tzinfo", None) is None:
            # MongoDB returns naive datetimes — treat as UTC to match resync_cutoff.
            last_sync = last_sync.replace(tzinfo=timezone.utc)
        if last_sync is None or last_sync < resync_cutoff:
            pending.append(doc["ticker"])

    summary: Dict[str, Any] = {
        "started_at": now.isoformat(),
        "total_visible": len(all_visible),
        "pending_sync": len(pending),
        "synced_ok": 0,
        "synced_fail": 0,
        "total_dividends_written": 0,
    }

    logger.info(
        f"[dividend_sync] Starting: {len(pending)} of {len(all_visible)} "
        f"visible tickers need sync (resync_days={DIVIDEND_RESYNC_DAYS})"
    )

    for ticker in pending:
        try:
            result = await sync_ticker_dividends(db, ticker)
            status = "ok" if result["success"] else "error"
            divs_written = result.get("dividends_synced", 0)

            await db.tracked_tickers.update_one(
                {"ticker": ticker},
                {"$set": {
                    "dividends_synced_at": now,
                    "dividends_sync_status": status,
                }},
            )

            if result["success"]:
                summary["synced_ok"] += 1
                summary["total_dividends_written"] += divs_written
            else:
                summary["synced_fail"] += 1
                logger.warning(f"[dividend_sync] {ticker}: {result.get('error')}")

        except Exception as exc:
            summary["synced_fail"] += 1
            logger.error(f"[dividend_sync] {ticker} unhandled error: {exc}")

        # Rate-limit: 0.25 s between EODHD calls (≈4 req/s)
        await asyncio.sleep(0.25)

    summary["finished_at"] = datetime.now(timezone.utc).isoformat()
    logger.info(
        f"[dividend_sync] Done: ok={summary['synced_ok']}, "
        f"fail={summary['synced_fail']}, dividends={summary['total_dividends_written']}"
    )
    return summary


# ==============================================================================
# UPCOMING EARNINGS CALENDAR — mirrors upcoming_dividends pattern exactly
# ==============================================================================

async def create_upcoming_earnings_indexes(db) -> None:
    """Create indexes for the upcoming_earnings collection.

    Called once at server startup, not on every job run.
    Motor/PyMongo make create_index idempotent so it is safe to call on each
    cold start without rebuilding existing indexes.
    """
    await db.upcoming_earnings.create_index(
        [("ticker", 1)], unique=True, name="upcoming_earnings_ticker_unique"
    )
    await db.upcoming_earnings.create_index(
        [("report_date", 1)], name="upcoming_earnings_report_date"
    )


def _parse_earnings_report_date(row: Dict[str, Any]) -> Optional[str]:
    """Extract and normalise the report date from an EODHD earnings calendar row."""
    raw = (
        row.get("report_date")
        or row.get("date")
        or row.get("reportDate")
    )
    return _parse_date_ymd(raw)


async def sync_upcoming_earnings_calendar_for_visible_tickers(db) -> Dict[str, Any]:
    """Daily job: fetch EODHD /calendar/earnings and persist one doc per visible
    ticker into the upcoming_earnings collection.

    Pattern mirrors sync_upcoming_dividend_calendar_for_visible_tickers exactly:
    - One document per visible ticker (upsert).
    - Visible tickers with no upcoming earnings get all payload fields set to None.
    - Visibility definition: tracked_tickers.is_visible = True.
    - Does NOT touch scheduler_service.py Step 2.6 (_detect_earnings_candidates_eodhd),
      which is a separate pipeline stage for flagging fundamentals refresh.
    """
    if not EODHD_API_KEY:
        logger.error("[earnings_upcoming_calendar] EODHD_API_KEY not configured")
        return {"success": False, "error": "EODHD_API_KEY not configured"}

    now = datetime.now(timezone.utc)
    window_start = now.strftime("%Y-%m-%d")
    window_end = (now + timedelta(days=UPCOMING_EARNINGS_WINDOW_DAYS)).strftime("%Y-%m-%d")

    # Canonical universe source: tracked_tickers.is_visible (same as dividend calendar).
    visible_tickers_raw = await db.tracked_tickers.distinct("ticker", {"is_visible": True})
    visible_tickers = {_normalize_ticker_symbol(t) for t in visible_tickers_raw if _normalize_ticker_symbol(t)}

    url = f"{EODHD_BASE_URL}/calendar/earnings"
    params = {
        "api_token": EODHD_API_KEY,
        "fmt": "json",
        "from": window_start,
        "to": window_end,
    }

    async with httpx.AsyncClient(timeout=45) as client:
        response = await client.get(url, params=params)
        response.raise_for_status()
        payload = response.json()

    if isinstance(payload, dict):
        rows = payload.get("earnings") or payload.get("data") or []
    elif isinstance(payload, list):
        rows = payload
    else:
        rows = []

    # Group rows by normalised ticker symbol; keep only the nearest upcoming event.
    grouped: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        ticker = _normalize_ticker_symbol(
            row.get("code")
            or row.get("Code")
            or row.get("symbol")
            or row.get("ticker")
        )
        if not ticker:
            continue
        if visible_tickers and ticker not in visible_tickers:
            continue
        report_date = _parse_earnings_report_date(row)
        if not report_date:
            continue
        # Keep the nearest (earliest) upcoming report per ticker.
        if ticker not in grouped or report_date < grouped[ticker]["_report_date"]:
            grouped[ticker] = {
                "_report_date": report_date,
                "report_date": report_date,
                "fiscal_period_end": _parse_date_ymd(
                    row.get("fiscal_date_ending")
                    or row.get("fiscalDateEnding")
                    or row.get("date_period")
                ),
                "before_after_market": row.get("before_after_market")
                    or row.get("Before_After_Market")
                    or row.get("beforeAfterMarket")
                    or None,
                "currency": row.get("currency") or row.get("Currency") or None,
                "estimate": _safe_float(
                    row.get("estimate")
                    or row.get("epsMean")
                    or row.get("epsEstimate")
                ),
            }

    write_ops: List[UpdateOne] = []

    for ticker, event in grouped.items():
        write_ops.append(
            UpdateOne(
                {"ticker": ticker},
                {"$set": {
                    "ticker": ticker,
                    "report_date": event["report_date"],
                    "fiscal_period_end": event["fiscal_period_end"],
                    "before_after_market": event["before_after_market"],
                    "currency": event["currency"],
                    "estimate": event["estimate"],
                    "source": UPCOMING_EARNINGS_SOURCE,
                    "fetched_at": now,
                    "window_start": window_start,
                    "window_end": window_end,
                }},
                upsert=True,
            )
        )

    null_tickers = sorted(visible_tickers - set(grouped.keys()))
    for ticker in null_tickers:
        write_ops.append(
            UpdateOne(
                {"ticker": ticker},
                {"$set": {
                    "ticker": ticker,
                    "report_date": None,
                    "fiscal_period_end": None,
                    "before_after_market": None,
                    "currency": None,
                    "estimate": None,
                    "source": UPCOMING_EARNINGS_SOURCE,
                    "fetched_at": now,
                    "window_start": window_start,
                    "window_end": window_end,
                }},
                upsert=True,
            )
        )

    if write_ops:
        await db.upcoming_earnings.bulk_write(write_ops, ordered=False)

    return {
        "success": True,
        "source": UPCOMING_EARNINGS_SOURCE,
        "window_start": window_start,
        "window_end": window_end,
        "visible_tickers": len(visible_tickers),
        "tickers_with_upcoming": len(grouped),
        "tickers_without_upcoming": len(null_tickers),
        "records_written": len(write_ops),
    }


async def get_earnings_for_ticker(db, ticker: str) -> Dict[str, Any]:
    """Return structured earnings history + upcoming for the given ticker.

    Served by GET /v1/ticker/{ticker}/earnings.

    Classification rules (Prague date-only, no UTC comparison):
      is_upcoming = True  when:
        quarter_date > today_prague
        OR (quarter_date <= today_prague AND reported_eps is None)
      is_upcoming = False when:
        quarter_date <= today_prague AND reported_eps is not None

      show_badge = True only when ALL of:
        is_upcoming = False
        reported_eps is not None
        estimated_eps is not None
        estimated_eps != 0

    beat_miss is NOT returned (redundant; sign of surprise_pct is sufficient).
    Upcoming rows never carry surprise_pct.
    History rows with show_badge = False carry surprise_pct = None.
    before_after_market, fiscal_period_end, currency are always None for history rows.
    """
    ticker_upper = ticker.upper()
    ticker_full = ticker_upper if ticker_upper.endswith(".US") else f"{ticker_upper}.US"

    today_prague = datetime.now(ZoneInfo(PRAGUE_TZ_NAME)).strftime("%Y-%m-%d")

    # --- metadata: pull default_currency from tracked_tickers ---
    tracked = await db.tracked_tickers.find_one(
        {"ticker": ticker_full},
        {"_id": 0, "financial_currency": 1},
    )
    default_currency = (tracked or {}).get("financial_currency") or "USD"

    # --- upcoming earnings from dedicated collection ---
    upcoming_doc = await db.upcoming_earnings.find_one(
        {"ticker": ticker_full}, {"_id": 0}
    )
    upcoming_earnings = None
    if upcoming_doc and upcoming_doc.get("report_date"):
        upcoming_earnings = {
            "report_date": upcoming_doc["report_date"],
            "fiscal_period_end": upcoming_doc.get("fiscal_period_end"),
            "before_after_market": upcoming_doc.get("before_after_market"),
            "currency": upcoming_doc.get("currency"),
            "estimate": upcoming_doc.get("estimate"),
        }

    # --- earnings history from earnings_history_cache ---
    raw_rows = await db.earnings_history_cache.find(
        {"ticker": ticker_full},
        {"_id": 0},
    ).sort("quarter_date", -1).limit(32).to_list(32)

    earnings_history = []
    for row in raw_rows:
        quarter_date = row.get("quarter_date") or ""
        reported_eps = row.get("reported_eps")
        estimated_eps = row.get("estimated_eps")

        # Classification (Prague date-only)
        if quarter_date > today_prague or reported_eps is None:
            is_upcoming = True
        else:
            is_upcoming = False

        # Badge eligibility
        show_badge = (
            not is_upcoming
            and reported_eps is not None
            and estimated_eps is not None
            and estimated_eps != 0
        )

        # surprise_pct: only meaningful when show_badge is True
        surprise_pct = row.get("surprise_pct") if show_badge else None

        earnings_history.append({
            "quarter_date": quarter_date,
            "fiscal_period_end": None,       # not stored in earnings_history_cache
            "reported_eps": reported_eps,
            "estimated_eps": estimated_eps,
            "currency": None,                # not stored in earnings_history_cache
            "before_after_market": None,     # not supported for history rows
            "surprise_pct": surprise_pct,
            "is_upcoming": is_upcoming,
            "show_badge": show_badge,
        })

    return {
        "ticker": ticker_full,
        "metadata": {
            "default_currency": default_currency,
            "currencies": [default_currency],
            "default_frequency": "Quarterly",
            "frequencies": ["Quarterly"],
        },
        "upcoming_earnings": upcoming_earnings,
        "earnings_history": earnings_history,
    }
