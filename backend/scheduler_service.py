"""
RICHSTOX Scheduler Service
===========================
DO NOT CHANGE WITHOUT RICHARD APPROVAL (kurtarichard@gmail.com)
This scheduler/data pipeline is IMMUTABLE.

Handles scheduled tasks with:
- Mon-Sat schedule (Europe/Prague timezone)
- Kill switch protection (ops_config collection)
- Job logging to ops_job_runs
- Manual admin endpoints always work (bypass kill switch)

Schedule (Europe/Prague timezone):
- 03:00 (Mon-Sat): Step 1 Universe Seed
- Step 2 auto-runs after Step 1 completion: price sync + event detectors
- Step 3 auto-runs after Step 2 completion: fundamentals sync from pending events
- 04:15: SP500TR benchmark update
- 04:45: Price backfill (newly activated tickers, gaps, corporate actions)
- 05:00: PAIN cache refresh (max drawdown for all visible tickers)
- 05:00: Parallel backfill ALL (1,000 tickers/day)
- 05:30: Key metrics + peer medians
- 13:00: News & sentiment refresh (followed/watchlisted tickers)

Kill Switch:
- Stored in ops_config collection: {key: "scheduler_enabled", value: true/false}
- Only affects scheduled jobs, NOT manual admin API calls
"""

import os
import time
import logging
from datetime import datetime, timezone, timedelta, date
from typing import Dict, Any, Optional, List, Callable, Awaitable
import asyncio
from zoneinfo import ZoneInfo
import httpx
from pymongo import UpdateOne
from pymongo.errors import DuplicateKeyError

from services.admin_jobs_service import is_cancel_requested

logger = logging.getLogger("richstox.scheduler")

# Constants
SCHEDULER_CONFIG_KEY = "scheduler_enabled"
SEED_QUERY = {"exchange": {"$in": ["NYSE", "NASDAQ"]}, "asset_type": "Common Stock", "is_seeded": True}
# Canonical Step 3 universe — tickers that are seeded and have price data.
# has_price_data is True for tickers in today's bulk OR with existing stock_prices.
# This is the exact filter used by universe_counts_service step3_query and
# is the source of truth for "Tickers with prices" on the Step 3 pipeline card.
STEP3_QUERY = {**SEED_QUERY, "has_price_data": True}
PRAGUE_TZ = ZoneInfo("Europe/Prague")
STEP2_REPORT_STEP = "Step 2 - Price Sync"
EVENTS_WATERMARK_KEY = "last_events_checked_date"
REMEDIATION_WATCHDOG_TIMEOUT_SECONDS = 300
REMEDIATION_HEARTBEAT_SECONDS = 10
FUNDAMENTALS_SYNC_LOCK_ID = "fundamentals_sync"
FUNDAMENTALS_SYNC_HEARTBEAT_SECONDS = 10
FUNDAMENTALS_SYNC_LOCK_LEASE_SECONDS = 60
FUNDAMENTALS_SYNC_ZOMBIE_TIMEOUT_SECONDS = 300
FUNDAMENTALS_SYNC_TICKER_TIMEOUT_SECONDS = 120
# Logo backfill: tickers with logo_status="absent" fetched before this cutoff
# must be re-attempted — earlier runs used the wrong CDN base
# (eodhistoricaldata.com instead of eodhd.com).
LOGO_CDN_FIX_CUTOFF = datetime(2026, 4, 4, tzinfo=timezone.utc)
LOGO_BACKFILL_CONCURRENCY = 10
LOGO_BACKFILL_TICKER_TIMEOUT = 30
LOGO_BACKFILL_VISIBLE_LIMIT = 500
CANCEL_REQUESTED_STUCK_SECONDS = 600
PRICE_SYNC_STUCK_TIMEOUT_SECONDS = 1800  # 30 minutes
PRICE_SYNC_LOCK_ID = "price_sync"
PRICE_SYNC_LOCK_LEASE_SECONDS = 60
PRICE_SYNC_HEARTBEAT_SECONDS = 10
PRICE_SYNC_ACTIVE_PHASES = {
    "bulk_catchup",
    "2.1_bulk_catchup",
    "2.2_split",
    "2.4_dividend",
    "2.6_earnings",
}
STEP3_PHASE_C_CONCURRENCY_DEFAULT = 3
STEP3_PHASE_C_CONCURRENCY_MAX = 12
STEP3_PHASE_C_BATCH_SIZE_DEFAULT = 50

EODHD_BASE_URL = "https://eodhd.com/api"
EODHD_API_KEY = os.getenv("EODHD_API_KEY", "")
_FUNDAMENTALS_EVENTS_INDEX_DONE = False
_FUNDAMENTALS_EVENTS_INDEX_LOCK: Optional[asyncio.Lock] = None
_OPS_LOCKS_TTL_INDEX_DONE = False
_OPS_LOCKS_TTL_INDEX_LOCK: Optional[asyncio.Lock] = None
# Delay between detector phases so the frontend (polling every ~2 s) can observe
# intermediate 2.2 / 2.4 / 2.6 progress updates before the run completes.
_DETECTOR_PHASE_POLL_DELAY = 0.5
MIN_BULK_ROWS_SANITY_CHECK = 4000
MIN_BULK_MATCHED_SEEDED_SANITY_CHECK = 4000
STEP2_SANITY_THRESHOLD_USED = (
    f"matched_seeded_tickers_count >= {MIN_BULK_MATCHED_SEEDED_SANITY_CHECK}"
)
MAX_BULK_GAPFILL_DAYS_HISTORY = 60
# Max 1 full-history redownload per ticker per this many days.
_REDOWNLOAD_COOLDOWN_DAYS = 7

# Gap remediation: reasons that indicate the gap is NOT-APPLICABLE
# (ticker absent or close=0 — not a true data gap).
# Shared between remediate_gap_date and run_bulk_gapfill_remediation.
_NOT_APPLICABLE_REASONS = frozenset({
    "not_in_bulk_not_in_api",
    "bulk_found_but_close_is_zero",
    "bulk_close_zero_api_returned_empty",
    "api_returned_only_zero_price",
})


def _ensure_utc(dt: Optional[datetime]) -> Optional[datetime]:
    """Normalize a datetime read from Mongo: attach UTC if naive, convert otherwise."""
    if dt is None:
        return None
    if not isinstance(dt, datetime):
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def _to_prague_iso(dt: Optional[datetime]) -> Optional[str]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(PRAGUE_TZ).isoformat()


async def get_scheduler_enabled(db) -> bool:
    """
    Check if scheduler is enabled (kill switch NOT engaged).
    
    Returns True if scheduler should run.
    Returns False if kill switch is engaged.
    """
    config = await db.ops_config.find_one({"key": SCHEDULER_CONFIG_KEY})
    
    if not config:
        # Default to enabled if no config exists
        return True
    
    return config.get("value", True)


async def set_scheduler_enabled(db, enabled: bool) -> Dict[str, Any]:
    """
    Set scheduler enabled state (kill switch).
    
    Args:
        db: MongoDB database
        enabled: True to enable scheduler, False to engage kill switch
    
    Returns:
        Updated config document
    """
    now = datetime.now(timezone.utc)
    
    await db.ops_config.update_one(
        {"key": SCHEDULER_CONFIG_KEY},
        {
            "$set": {
                "key": SCHEDULER_CONFIG_KEY,
                "value": enabled,
                "updated_at": now,
                "updated_by": "api",
            },
            "$setOnInsert": {
                "created_at": now,
            }
        },
        upsert=True
    )

    logger.info(f"Scheduler {'ENABLED' if enabled else 'DISABLED (kill switch engaged)'}")
    
    return {
        "key": SCHEDULER_CONFIG_KEY,
        "value": enabled,
        "updated_at": now.isoformat(),
        "message": f"Scheduler {'enabled' if enabled else 'disabled (kill switch engaged)'}"
    }


async def log_scheduled_job(
    db,
    job_name: str,
    status: str,
    details: Dict[str, Any],
    started_at: datetime,
    finished_at: Optional[datetime] = None,
    error: Optional[str] = None
) -> str:
    """
    Log a scheduled job run to ops_job_runs.
    
    Returns:
        Job ID (string)
    """
    now = datetime.now(timezone.utc)
    
    # Calculate duration correctly
    end_time = finished_at if finished_at else now
    duration = (end_time - started_at).total_seconds() if started_at else 0
    
    doc = {
        "job_name": job_name,
        "source": "scheduler",  # Distinguish from manual runs
        "status": status,
        "details": details,
        "started_at": started_at,
        "finished_at": end_time,
        "started_at_prague": _to_prague_iso(started_at),
        "finished_at_prague": _to_prague_iso(end_time),
        "log_timezone": "Europe/Prague",
        "duration_seconds": duration,
        "error": error,
        "created_at": now,
    }
    
    result = await db.ops_job_runs.insert_one(doc)
    return str(result.inserted_id)


def _as_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _pick_price(row: dict) -> Optional[float]:
    adjusted = _as_float((row or {}).get("adjusted_close"))
    if adjusted is not None and adjusted > 0:
        return adjusted
    close = _as_float((row or {}).get("close"))
    if close is not None and close > 0:
        return close
    return None


async def _ensure_fundamentals_events_index(db) -> None:
    """
    Ensure partial unique index for pending fundamentals_events.
    Guarded to run only once per process.
    """
    global _FUNDAMENTALS_EVENTS_INDEX_DONE
    global _FUNDAMENTALS_EVENTS_INDEX_LOCK
    if _FUNDAMENTALS_EVENTS_INDEX_LOCK is None:
        _FUNDAMENTALS_EVENTS_INDEX_LOCK = asyncio.Lock()
    async with _FUNDAMENTALS_EVENTS_INDEX_LOCK:
        if _FUNDAMENTALS_EVENTS_INDEX_DONE:
            return
        await db.fundamentals_events.create_index(
            [("ticker", 1), ("event_type", 1)],
            name="fundamentals_events_pending_ticker_event_type",
            unique=True,
            partialFilterExpression={"status": "pending"},
        )
        _FUNDAMENTALS_EVENTS_INDEX_DONE = True


async def _enqueue_fundamentals_events(
    db,
    event_type: str,
    tickers: List[str],
    now: datetime,
    source_job: str,
    detector_step: str,
    detected_date: Optional[str] = None,
) -> Dict[str, int]:
    """
    Enqueue fundamentals events idempotently.
    Creates one pending event per (ticker, event_type) while pending exists.

    On repeated same-day runs, tickers whose events for the same
    (event_type, detected_date) are already completed/skipped are excluded
    to prevent re-enqueue and unnecessary Step 3 reprocessing.
    """
    await _ensure_fundamentals_events_index(db)

    normalized = sorted({t for t in tickers if t})
    if not normalized:
        return {"new_inserts": 0, "skipped_existing": 0}

    # Skip tickers whose events for this detected_date were already processed
    # by Step 3 (completed/skipped).  Prevents re-enqueue on same-day reruns.
    already_processed: set = set()
    if detected_date:
        cursor = db.fundamentals_events.find(
            {
                "ticker": {"$in": normalized},
                "event_type": event_type,
                "status": {"$in": ["completed", "skipped"]},
                "detected_date": detected_date,
            },
            {"ticker": 1},
        )
        async for doc in cursor:
            already_processed.add(doc["ticker"])

    to_enqueue = [t for t in normalized if t not in already_processed]

    ops: List[UpdateOne] = []
    for ticker in to_enqueue:
        ops.append(
            UpdateOne(
                {
                    "ticker": ticker,
                    "event_type": event_type,
                    # Only enqueue a new pending item; existing non-pending docs
                    # should not be flipped back to pending.
                    "status": "pending",
                },
                {
                    "$setOnInsert": {
                        # TODO: remove legacy source_job after downstream fundamentals processors/reports
                        # drop the dependency (track in scheduler ops backlog).
                        "source_job": source_job,
                        "created_at": now,
                    },
                    # Refresh metadata on every enqueue attempt to reflect the latest detector run.
                    "$set": {
                        "detected_date": detected_date,
                        "source": source_job,
                        "detector_step": detector_step,
                        "updated_at": now,
                    },
                },
                upsert=True,
            )
        )

    ENQUEUE_BATCH_SIZE = 500
    new_inserts = 0
    for i in range(0, len(ops), ENQUEUE_BATCH_SIZE):
        batch = ops[i:i + ENQUEUE_BATCH_SIZE]
        try:
            batch_result = await db.fundamentals_events.bulk_write(batch, ordered=False)
            new_inserts += len(batch_result.upserted_ids or {})
        except Exception as exc:
            logger.error(f"_enqueue_fundamentals_events batch {i//ENQUEUE_BATCH_SIZE} failed: {exc}")

    # pending_matched: tickers already in 'pending' state (upsert matched, no insert).
    pending_matched = len(to_enqueue) - new_inserts
    if to_enqueue:
        await db.tracked_tickers.update_many(
            {"ticker": {"$in": to_enqueue}},
            {"$set": {"needs_fundamentals_refresh": True, "updated_at": now}},
        )

    # Undo the detector's needs_fundamentals_refresh=True for tickers already
    # processed today.  The detector functions set the flag before this call;
    # resetting it here prevents Step 3 from re-processing completed work.
    # Guard with fundamentals_status='complete' so we only reset tickers that
    # were fully synced — never tickers still awaiting first classification.
    if already_processed:
        await db.tracked_tickers.update_many(
            {
                "ticker": {"$in": list(already_processed)},
                "fundamentals_status": "complete",
            },
            {"$set": {"needs_fundamentals_refresh": False, "updated_at": now}},
        )

    return {
        "new_inserts": new_inserts,
        "skipped_existing": pending_matched + len(already_processed),
    }


async def _fetch_eodhd_bulk(
    endpoint: str, params: Dict[str, Any]
) -> tuple:
    """
    Execute one EODHD bulk API call.
    Returns (data_list, http_status, duration_ms).
    In MOCK mode (no API key) returns ([], 0, 0) with status "mock".
    """
    if not EODHD_API_KEY:
        return [], 0, 0, "mock"

    url = f"{EODHD_BASE_URL}/{endpoint}"
    all_params = {"api_token": EODHD_API_KEY, "fmt": "json", **params}
    t0 = time.monotonic()
    try:
        async with httpx.AsyncClient(timeout=60) as client:
            resp = await client.get(url, params=all_params)
        duration_ms = int((time.monotonic() - t0) * 1000)
        if resp.status_code == 429:
            return [], 429, duration_ms, "429"
        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, list):
            data = data.get("earnings", []) if isinstance(data, dict) else []
        return data, resp.status_code, duration_ms, "success"
    except Exception as exc:
        duration_ms = int((time.monotonic() - t0) * 1000)
        logger.error(f"EODHD bulk call failed [{endpoint}]: {exc}")
        return [], 0, duration_ms, "error"


async def _detect_split_candidates_eodhd(db, today_str: str) -> Dict[str, Any]:
    """
    Step 2.2: Fetch today's splits from EODHD bulk splits endpoint.
    API: GET /api/eod-bulk-last-day/US?type=splits&date=TODAY
    Credits: 1
    Flags set (flagging only — NO deletion):
      needs_price_redownload = true
      needs_fundamentals_refresh = true
      price_history_complete = false
      price_history_status = "pending"
      last_split_detected = today
    """
    from credit_log_service import log_api_credit

    endpoint = "eod-bulk-last-day/US"
    api_url = f"{EODHD_BASE_URL}/{endpoint}?type=splits&date={today_str}&api_token=YOUR_API_TOKEN&fmt=json"

    data, http_status, duration_ms, call_status = await _fetch_eodhd_bulk(
        endpoint, {"type": "splits", "date": today_str}
    )

    await log_api_credit(
        db,
        job_name="step2_splits",
        operation="bulk_splits",
        api_endpoint=api_url,
        credits_used=1,
        http_status=http_status,
        status=call_status,
        duration_ms=duration_ms,
    )

    if call_status == "mock":
        return {
            "mock_mode": True,
            "api_endpoint": api_url,
            "raw_count": 0,
            "universe_count": 0,
            "flagged_count": 0,
            "tickers": [],
        }

    # Build set of codes from EODHD response — field is "code" (without .US)
    split_codes = {
        str(row.get("code", "")).upper()
        for row in data
        if row.get("code")
    }

    if not split_codes:
        return {
            "mock_mode": False,
            "api_endpoint": api_url,
            "raw_count": len(data),
            "universe_count": 0,
            "flagged_count": 0,
            "tickers": [],
        }

    # Find which of these are in our seeded universe
    seed_docs = await db.tracked_tickers.find(
        {**SEED_QUERY, "has_price_data": True},
        {"_id": 0, "ticker": 1},
    ).to_list(None)

    # tracked_tickers stores tickers as "AAPL.US"
    universe_set = {d["ticker"].replace(".US", "") for d in seed_docs if d.get("ticker")}
    in_universe = [code for code in split_codes if code in universe_set]

    flagged = 0
    if in_universe:
        tickers_us = [f"{t}.US" for t in in_universe]
        result = await db.tracked_tickers.update_many(
            {
                "ticker": {"$in": tickers_us},
                # Idempotency: skip tickers already remediated for today's split.
                # A ticker with last_split_detected==today AND price_history_complete==True
                # has already been fully re-downloaded and should not be re-flagged.
                "$or": [
                    {"last_split_detected": {"$ne": today_str}},
                    {"price_history_complete": {"$ne": True}},
                ],
            },
            {"$set": {
                "needs_price_redownload": True,
                "needs_fundamentals_refresh": True,
                "price_history_complete": False,
                "price_history_status": "pending",
                "last_split_detected": today_str,
                # Clear computed fields alongside proof markers
                "history_download_completed": False,
                "gap_free_since_history_download": False,
            },
            "$unset": {
                "history_download_proven_at": "",
                "history_download_proven_anchor": "",
            }},
        )
        flagged = result.modified_count

    return {
        "mock_mode": False,
        "api_endpoint": api_url,
        "raw_count": len(data),
        "universe_count": len(in_universe),
        "flagged_count": flagged,
        "tickers": in_universe[:50],
    }


async def _detect_dividend_candidates_eodhd(db, today_str: str) -> Dict[str, Any]:
    """
    Step 2.4: Fetch today's ex-dividend events from EODHD bulk dividends endpoint.
    API: GET /api/eod-bulk-last-day/US?type=dividends&date=TODAY
    Credits: 1
    Flags set:
      needs_fundamentals_refresh = true
      last_dividend_detected = today
    """
    from credit_log_service import log_api_credit

    endpoint = "eod-bulk-last-day/US"
    api_url = f"{EODHD_BASE_URL}/{endpoint}?type=dividends&date={today_str}&api_token=YOUR_API_TOKEN&fmt=json"

    data, http_status, duration_ms, call_status = await _fetch_eodhd_bulk(
        endpoint, {"type": "dividends", "date": today_str}
    )

    await log_api_credit(
        db,
        job_name="step2_dividends",
        operation="bulk_dividends",
        api_endpoint=api_url,
        credits_used=1,
        http_status=http_status,
        status=call_status,
        duration_ms=duration_ms,
    )

    if call_status == "mock":
        return {
            "mock_mode": True,
            "api_endpoint": api_url,
            "raw_count": 0,
            "universe_count": 0,
            "flagged_count": 0,
            "tickers": [],
        }

    div_codes = {
        str(row.get("code", "")).upper()
        for row in data
        if row.get("code")
    }

    if not div_codes:
        return {
            "mock_mode": False,
            "api_endpoint": api_url,
            "raw_count": len(data),
            "universe_count": 0,
            "flagged_count": 0,
            "tickers": [],
        }

    seed_docs = await db.tracked_tickers.find(
        {**SEED_QUERY, "has_price_data": True},
        {"_id": 0, "ticker": 1},
    ).to_list(None)
    universe_set = {d["ticker"].replace(".US", "") for d in seed_docs if d.get("ticker")}
    in_universe = [code for code in div_codes if code in universe_set]

    flagged = 0
    if in_universe:
        tickers_us = [f"{t}.US" for t in in_universe]
        result = await db.tracked_tickers.update_many(
            {
                "ticker": {"$in": tickers_us},
                # Idempotency: skip tickers already remediated for today's dividend.
                # A ticker with last_dividend_detected==today AND price_history_complete==True
                # has already been fully re-downloaded and should not be re-flagged.
                "$or": [
                    {"last_dividend_detected": {"$ne": today_str}},
                    {"price_history_complete": {"$ne": True}},
                ],
            },
            {"$set": {
                "needs_fundamentals_refresh": True,
                "needs_price_redownload": True,
                "price_history_complete": False,
                "price_history_status": "pending",
                "last_dividend_detected": today_str,
                # Clear computed fields alongside proof markers
                "history_download_completed": False,
                "gap_free_since_history_download": False,
            },
            "$unset": {
                "history_download_proven_at": "",
                "history_download_proven_anchor": "",
            }},
        )
        flagged = result.modified_count

    return {
        "mock_mode": False,
        "api_endpoint": api_url,
        "raw_count": len(data),
        "universe_count": len(in_universe),
        "flagged_count": flagged,
        "tickers": in_universe[:50],
    }


async def _detect_earnings_candidates_eodhd(db, today_str: str, from_date: Optional[str] = None) -> Dict[str, Any]:
    """
    Step 2.6: Fetch earnings from EODHD earnings calendar.
    API: GET /api/calendar/earnings?from=FROM&to=TODAY
    Supports catchup: from_date can be earlier than today to cover missed days.
    Credits: 1 per call regardless of date range.
    Flags set:
      needs_fundamentals_refresh = true
      last_earnings_detected = today
    """
    from credit_log_service import log_api_credit

    start = from_date or today_str
    endpoint = "calendar/earnings"
    api_url = f"{EODHD_BASE_URL}/{endpoint}?from={start}&to={today_str}&api_token=YOUR_API_TOKEN&fmt=json"

    data, http_status, duration_ms, call_status = await _fetch_eodhd_bulk(
        endpoint, {"from": start, "to": today_str}
    )

    await log_api_credit(
        db,
        job_name="step2_earnings",
        operation="earnings_calendar",
        api_endpoint=api_url,
        credits_used=1,
        http_status=http_status,
        status=call_status,
        duration_ms=duration_ms,
    )

    if call_status == "mock":
        return {
            "mock_mode": True,
            "api_endpoint": api_url,
            "raw_count": 0,
            "universe_count": 0,
            "flagged_count": 0,
            "tickers": [],
        }

    # Earnings calendar response: list of objects with "code" field like "AAPL.US"
    earnings_codes = set()
    for row in data:
        code = str(row.get("code", "")).upper()
        # Strip exchange suffix if present
        code = code.replace(".US", "").replace(".NYSE", "").replace(".NASDAQ", "")
        if code:
            earnings_codes.add(code)

    if not earnings_codes:
        return {
            "mock_mode": False,
            "api_endpoint": api_url,
            "raw_count": len(data),
            "universe_count": 0,
            "flagged_count": 0,
            "tickers": [],
        }

    seed_docs = await db.tracked_tickers.find(
        {**SEED_QUERY, "has_price_data": True},
        {"_id": 0, "ticker": 1},
    ).to_list(None)
    universe_set = {d["ticker"].replace(".US", "") for d in seed_docs if d.get("ticker")}
    in_universe = [code for code in earnings_codes if code in universe_set]

    flagged = 0
    if in_universe:
        tickers_us = [f"{t}.US" for t in in_universe]
        result = await db.tracked_tickers.update_many(
            {"ticker": {"$in": tickers_us}},
            {"$set": {
                "needs_fundamentals_refresh": True,
                "last_earnings_detected": today_str,
            }},
        )
        flagged = result.modified_count

    return {
        "mock_mode": False,
        "api_endpoint": api_url,
        "raw_count": len(data),
        "universe_count": len(in_universe),
        "flagged_count": flagged,
        "tickers": in_universe[:50],
    }


async def _get_or_init_events_watermark(db, today_dt: date) -> date:
    """
    Persistent Step 2 events watermark in ops_config.

    If missing, bootstrap to max(latest Step 1 completion date, today-30).
    Watermark initialization is a one-time bootstrap; afterwards we advance per-day
    after successful processing to avoid skipped days.
    """
    cfg = await db.ops_config.find_one({"key": EVENTS_WATERMARK_KEY})
    if cfg and cfg.get("value"):
        try:
            return datetime.fromisoformat(cfg["value"]).date()
        except Exception:
            logger.warning(f"Invalid {EVENTS_WATERMARK_KEY} in ops_config; reinitializing")

    latest_step1 = await db.ops_job_runs.find_one(
        {"job_name": "universe_seed", "status": {"$in": ["success", "completed"]}},
        {"finished_at": 1},
        sort=[("finished_at", -1)],
    )
    step1_date: Optional[date] = None
    if latest_step1 and latest_step1.get("finished_at"):
        finished_at = latest_step1["finished_at"]
        if hasattr(finished_at, "tzinfo") and finished_at.tzinfo is None:
            finished_at = finished_at.replace(tzinfo=timezone.utc)
        step1_date = finished_at.astimezone(PRAGUE_TZ).date()

    bootstrap_floor = today_dt - timedelta(days=30)
    init_date = step1_date if step1_date and step1_date > bootstrap_floor else bootstrap_floor

    await db.ops_config.update_one(
        {"key": EVENTS_WATERMARK_KEY},
        {
            "$set": {
                "key": EVENTS_WATERMARK_KEY,
                "value": init_date.isoformat(),
                "updated_at": datetime.now(timezone.utc),
            }
        },
        upsert=True,
    )
    return init_date


async def _set_events_watermark(db, new_date: date) -> None:
    await db.ops_config.update_one(
        {"key": EVENTS_WATERMARK_KEY},
        {"$set": {"value": new_date.isoformat(), "updated_at": datetime.now(timezone.utc)}},
        upsert=True,
    )


async def _get_missed_trading_dates(db, today_dt: date) -> List[date]:
    """
    Return list of trading dates (date objects) to process from (watermark+1 … today), skipping weekends.
    Never skips weekdays; any backlog remains queued for the next run.
    """
    watermark = await _get_or_init_events_watermark(db, today_dt)
    missed: List[date] = []
    current = watermark + timedelta(days=1)
    while current <= today_dt:
        if current.weekday() < 5:
            missed.append(current)
        current += timedelta(days=1)
    return missed


async def _remediate_price_redownload(
    db,
    tickers_us: List[str],
    now: datetime,
    *,
    progress_cb: Optional[Callable[[str], Awaitable[None]]] = None,
    exclusion_run_id: Optional[str] = None,
    exclusion_report_date: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Execute scoped full price history re-download for split/dividend flagged tickers.

    Uses the same fetch-and-upsert logic as backfill_prices_full_history but
    scoped to the supplied tickers only.  After a successful re-download the
    `needs_price_redownload` flag is cleared on the ticker document.

    Idempotent: re-running for the same tickers is safe (upsert on ticker+date).

    Returns:
        {"succeeded": int, "failed": int, "tickers_processed": int}
    """
    from price_ingestion_service import backfill_ticker_prices

    succeeded = 0
    failed = 0
    unique_tickers = list(dict.fromkeys(t for t in tickers_us if t))
    failures: List[Dict[str, Any]] = []
    start_ts = time.monotonic()
    last_heartbeat = start_ts
    total = len(unique_tickers)
    processed = 0
    names_map: Dict[str, Optional[str]] = {}
    if unique_tickers:
        docs = await db.tracked_tickers.find(
            {"ticker": {"$in": unique_tickers}},
            {"ticker": 1, "name": 1, "_id": 0},
        ).to_list(None)
        names_map = {d.get("ticker"): d.get("name") for d in docs if d.get("ticker")}

    def _failure_record(ticker_full: str, reason: str) -> Dict[str, Any]:
        return {
            "ticker": ticker_full.replace(".US", ""),
            "name": names_map.get(ticker_full),
            "reason": reason,
        }

    for idx, ticker_us in enumerate(unique_tickers):
        try:
            result = await backfill_ticker_prices(db, ticker_us)
            if result.get("success"):
                records_upserted = result.get("records_upserted", 0)
                if records_upserted > 0:
                    succeeded += 1
                    # Derive anchor from backfill result date_range
                    _dr = result.get("date_range") or {}
                    _anchor = _dr.get("to")
                    _proof_fields: Dict[str, Any] = {
                        "needs_price_redownload": False,
                        "price_history_complete": True,
                        "price_history_complete_as_of": _anchor,
                        "price_history_status": "complete",
                        "updated_at": now,
                        # Strict proof marker — canonical source for history_download_completed
                        "history_download_proven_at": now,
                        "history_download_proven_anchor": _anchor,
                        # Computed fields — kept in sync so dashboard facet reads work
                        "history_download_completed": True,
                        "gap_free_since_history_download": True,
                    }
                    await db.tracked_tickers.update_one(
                        {"ticker": ticker_us},
                        {"$set": _proof_fields},
                    )
                else:
                    failed += 1
                    msg = (
                        "success=True but records_upserted=0 — leaving "
                        "needs_price_redownload=True for retry"
                    )
                    failures.append(_failure_record(ticker_us, msg))
                    logger.warning(f"_remediate_price_redownload: {ticker_us} {msg}")
            else:
                failed += 1
                err_msg = result.get("error") or result.get("message") or "Unknown error"
                failures.append(_failure_record(ticker_us, err_msg))
                logger.warning(
                    f"_remediate_price_redownload: {ticker_us} backfill returned success=False — {err_msg}"
                )
        except Exception as exc:
            failed += 1
            failures.append(_failure_record(ticker_us, str(exc)))
            logger.error(f"_remediate_price_redownload: {ticker_us} failed — {exc}")

        processed += 1
        now_ts = time.monotonic()
        elapsed = now_ts - start_ts
        since_last_heartbeat = now_ts - last_heartbeat
        if (processed % 5 == 0) or (since_last_heartbeat >= REMEDIATION_HEARTBEAT_SECONDS):
            if progress_cb:
                await progress_cb(
                    f"2.7 Remediating split/dividend tickers: {processed}/{total} "
                    f"(✓{succeeded} ✗{failed})"
                )
            last_heartbeat = now_ts

        if elapsed > REMEDIATION_WATCHDOG_TIMEOUT_SECONDS:
            remaining = unique_tickers[idx + 1:]
            failed += len(remaining)
            for rem in remaining:
                failures.append(
                    _failure_record(
                        rem,
                        f"Skipped due to remediation watchdog timeout (>{REMEDIATION_WATCHDOG_TIMEOUT_SECONDS}s)",
                    )
                )
            logger.warning(
                f"_remediate_price_redownload: watchdog triggered after {REMEDIATION_WATCHDOG_TIMEOUT_SECONDS}s, "
                f"stopping with {len(remaining)} remaining ticker(s) unprocessed"
            )
            break

    if failures:
        await _append_step2_exclusions(db, exclusion_run_id, exclusion_report_date, failures)

    return {
        "succeeded": succeeded,
        "failed": failed,
        "tickers_processed": processed,
        "watchdog_triggered": processed < total,
    }


async def run_step2_event_detectors(
    db,
    progress_cb: Optional[Callable[[str], Awaitable[None]]] = None,
    exclusion_meta: Optional[Dict[str, Any]] = None,
    cancel_check: Optional[Callable[[], Awaitable[bool]]] = None,
    processed_date: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Execute Step 2 sub-steps with REAL EODHD API calls.
    2.2: bulk splits → needs_price_redownload + needs_fundamentals_refresh
    2.4: bulk dividends → needs_fundamentals_refresh
    2.6: earnings calendar (full missed range) → needs_fundamentals_refresh

    After all detectors run, tickers flagged by 2.2/2.4 (needs_price_redownload)
    are immediately remediated via a scoped full price history re-download
    (_remediate_price_redownload).  Earnings-only tickers (2.6) are handled by
    Step 3 fundamentals refresh via the fundamentals_events queue.

    GAP DETECTION: If Step 2 missed multiple days, iterates through all missed
    trading dates for splits and dividends. Earnings uses a date range call.

    cancel_check: optional async callable — if it returns True the detectors
    stop processing further dates and return with cancelled=True.
    """
    today_dt = datetime.now(PRAGUE_TZ).date()
    today_str = today_dt.strftime("%Y-%m-%d")
    now = datetime.now(timezone.utc)

    async def _p(msg: str) -> None:
        if progress_cb:
            await progress_cb(msg)

    # When provided by Step 2.1 bulk payload, use the exact processed_date for
    # all detector windows to stay aligned with provider-latest bulk ingest.
    if processed_date:
        try:
            missed_date_objs = [datetime.fromisoformat(processed_date).date()]
        except Exception:
            logger.warning(
                "Step 2 detectors received invalid processed_date=%r; falling back to watermark catchup",
                processed_date,
            )
            missed_date_objs = await _get_missed_trading_dates(db, today_dt)
            if today_dt not in missed_date_objs:
                missed_date_objs.append(today_dt)
    else:
        # Determine all missed trading dates since last successful run
        missed_date_objs = await _get_missed_trading_dates(db, today_dt)
        if today_dt not in missed_date_objs:
            missed_date_objs.append(today_dt)
    missed_dates = [d.strftime("%Y-%m-%d") for d in missed_date_objs]

    logger.info(f"Step 2 detectors: processing {len(missed_dates)} date(s): {missed_dates}")

    split_raw_total = 0
    split_all_in_universe: List[str] = []
    split_flagged_total = 0
    split_endpoints: List[str] = []
    div_raw_total = 0
    div_all_in_universe: List[str] = []
    div_flagged_total = 0
    div_endpoints: List[str] = []
    earnings_all: Dict[str, Any] = {
        "mock_mode": not bool(EODHD_API_KEY),
        "api_endpoints_all": [],
        "dates_checked": [],
        "raw_count": 0,
        "universe_count": 0,
        "flagged_count": 0,
        "tickers": [],
    }
    processed_dates: List[str] = []
    _cancelled = False

    for i, date_obj in enumerate(missed_date_objs):
        # Check for cancellation before each day's batch of detector calls
        if cancel_check and await cancel_check():
            logger.info("Step 2 event detectors cancelled mid-run after %d/%d dates", i, len(missed_dates))
            _cancelled = True
            break

        date_str = date_obj.strftime("%Y-%m-%d")
        await _p(f"2.2 Split detector: calling EODHD for {date_str} ({i+1}/{len(missed_dates)})…")
        s = await _detect_split_candidates_eodhd(db, date_str)
        split_raw_total += s.get("raw_count", 0)
        split_all_in_universe.extend(s.get("tickers", []))
        split_flagged_total += s.get("flagged_count", 0)
        if s.get("api_endpoint"):
            split_endpoints.append(s["api_endpoint"])
        await asyncio.sleep(_DETECTOR_PHASE_POLL_DELAY)

        await _p(f"2.4 Dividend detector: calling EODHD for {date_str} ({i+1}/{len(missed_dates)})…")
        d = await _detect_dividend_candidates_eodhd(db, date_str)
        div_raw_total += d.get("raw_count", 0)
        div_all_in_universe.extend(d.get("tickers", []))
        div_flagged_total += d.get("flagged_count", 0)
        if d.get("api_endpoint"):
            div_endpoints.append(d["api_endpoint"])
        await asyncio.sleep(_DETECTOR_PHASE_POLL_DELAY)

        await _p(f"2.6 Earnings detector: calling EODHD calendar {date_str} ({i+1}/{len(missed_dates)})…")
        earnings = await _detect_earnings_candidates_eodhd(db, date_str, from_date=date_str)
        earnings_all["api_endpoints_all"].append(earnings.get("api_endpoint"))
        earnings_all["raw_count"] += earnings.get("raw_count", 0)
        earnings_all["universe_count"] += earnings.get("universe_count", 0)
        earnings_all["flagged_count"] += earnings.get("flagged_count", 0)
        earnings_all["tickers"].extend(earnings.get("tickers", []))
        earnings_all["dates_checked"].append(date_str)

        # Advance watermark only after the full detector set for the day succeeds
        await _set_events_watermark(db, date_obj)
        processed_dates.append(date_str)

    split_all_in_universe = list(dict.fromkeys(split_all_in_universe))  # dedup
    div_all_in_universe = list(dict.fromkeys(div_all_in_universe))

    split = {
        "mock_mode": not bool(EODHD_API_KEY),
        "api_endpoint": split_endpoints[0] if split_endpoints else f"{EODHD_BASE_URL}/eod-bulk-last-day/US?type=splits&date={today_str}&api_token=YOUR_API_TOKEN&fmt=json",
        "api_endpoints_all": split_endpoints,
        "dates_checked": processed_dates,
        "raw_count": split_raw_total,
        "universe_count": len(split_all_in_universe),
        "flagged_count": split_flagged_total,
        "tickers": split_all_in_universe[:50],
    }
    dividend = {
        "mock_mode": not bool(EODHD_API_KEY),
        "api_endpoint": div_endpoints[0] if div_endpoints else f"{EODHD_BASE_URL}/eod-bulk-last-day/US?type=dividends&date={today_str}&api_token=YOUR_API_TOKEN&fmt=json",
        "api_endpoints_all": div_endpoints,
        "dates_checked": processed_dates,
        "raw_count": div_raw_total,
        "universe_count": len(div_all_in_universe),
        "flagged_count": div_flagged_total,
        "tickers": div_all_in_universe[:50],
    }
    audit_endpoint_dates = processed_dates or missed_dates or [today_str]
    earnings = {
        "mock_mode": earnings_all.get("mock_mode", False),
        "api_endpoint": (earnings_all.get("api_endpoints_all") or [
            f"{EODHD_BASE_URL}/calendar/earnings?"
            f"from={audit_endpoint_dates[0]}"
            f"&to={audit_endpoint_dates[-1]}"
            f"&api_token=YOUR_API_TOKEN&fmt=json"
        ])[0],
        "api_endpoints_all": earnings_all.get("api_endpoints_all") or [],
        "dates_checked": processed_dates,
        "raw_count": earnings_all.get("raw_count", 0),
        "universe_count": earnings_all.get("universe_count", 0),
        "flagged_count": earnings_all.get("flagged_count", 0),
        "tickers": (earnings_all.get("tickers") or [])[:50],
    }

    # Still enqueue to fundamentals_events collection for Step 3 (backwards compat)
    # Skip enqueueing if cancelled mid-run (partial data may be incomplete)
    all_flagged: List[str] = []
    price_redownload_tickers: List[str] = []  # split + dividend tickers needing full price refresh
    enqueue_stats = {
        "split": {"new_inserts": 0, "skipped_existing": 0},
        "dividend": {"new_inserts": 0, "skipped_existing": 0},
        "earnings": {"new_inserts": 0, "skipped_existing": 0},
    }
    if not _cancelled:
        for result, event_type, step in [
            (split, "split", "2.2"),
            (dividend, "dividend", "2.4"),
            (earnings, "earnings", "2.6"),
        ]:
            tickers = [f"{t}.US" for t in result.get("tickers", [])]
            if tickers:
                enqueue_res = await _enqueue_fundamentals_events(
                    db,
                    event_type=event_type,
                    tickers=tickers,
                    now=now,
                    source_job="price_sync",
                    detector_step=step,
                    detected_date=today_str,
                )
                enqueue_stats[event_type] = {
                    "new_inserts": enqueue_res.get("new_inserts", 0),
                    "skipped_existing": enqueue_res.get("skipped_existing", 0),
                }
                all_flagged.extend(tickers)
                # Split and dividend detectors set needs_price_redownload=True.
                # Collect unique tickers for immediate scoped price re-download.
                # if step in ("2.2", "2.4"):
                #    price_redownload_tickers.extend(tickers)

    # Deduplicate price_redownload_tickers (split + dividend may overlap).
    price_redownload_tickers = list(dict.fromkeys(price_redownload_tickers))

    # B) Auto-remediation: execute scoped full price history re-download for
    # tickers flagged by split/dividend detectors.  Uses the same logic as
    # backfill_prices_full_history but scoped to flagged tickers only.
    # Earnings-only tickers are handled by Step 3 fundamentals refresh.
    remediation_stats = {
        "price_redownload_triggered": len(price_redownload_tickers),
        "price_redownload_succeeded": 0,
        "price_redownload_failed": 0,
    }
    if price_redownload_tickers:
        await _p(
            f"2.7 Remediating {len(price_redownload_tickers)} split/dividend ticker(s): "
            "full price history re-download…"
        )
        meta = exclusion_meta or {}
        exclusion_run_id = meta.get("run_id")
        exclusion_report_date = meta.get("report_date")
        redownload_result = await _remediate_price_redownload(
            db,
            price_redownload_tickers,
            now,
            progress_cb=progress_cb,
            exclusion_run_id=exclusion_run_id,
            exclusion_report_date=exclusion_report_date,
        )
        remediation_stats["price_redownload_succeeded"] = redownload_result.get("succeeded", 0)
        remediation_stats["price_redownload_failed"] = redownload_result.get("failed", 0)
        logger.info(
            f"Step 2 price remediation: {remediation_stats['price_redownload_succeeded']} "
            f"succeeded, {remediation_stats['price_redownload_failed']} failed "
            f"out of {len(price_redownload_tickers)} tickers"
        )

    split_new = enqueue_stats["split"]["new_inserts"]
    div_new = enqueue_stats["dividend"]["new_inserts"]
    earn_new = enqueue_stats["earnings"]["new_inserts"]
    split_skip = enqueue_stats["split"]["skipped_existing"]
    div_skip = enqueue_stats["dividend"]["skipped_existing"]
    earn_skip = enqueue_stats["earnings"]["skipped_existing"]
    enqueued_total = sum(v.get("new_inserts", 0) for v in enqueue_stats.values())
    skipped_total = sum(v.get("skipped_existing", 0) for v in enqueue_stats.values())

    return {
        "step_2_2_split": {
            "mock_mode": split.get("mock_mode", False),
            "api_endpoint": split.get("api_endpoint", ""),
            "raw_count": split.get("raw_count", 0),
            "universe_count": split.get("universe_count", 0),
            "flagged_count": split.get("flagged_count", 0),
            "tickers_sample": split.get("tickers", [])[:10],
            "dates_checked": split.get("dates_checked", []),
            "verified_through_date": split.get("dates_checked", [])[-1] if split.get("dates_checked") else today_str,
            "fundamentals_events_enqueued_new": split_new,
            "fundamentals_events_enqueued_skipped_existing": split_skip,
        },
        "step_2_4_dividend": {
            "mock_mode": dividend.get("mock_mode", False),
            "api_endpoint": dividend.get("api_endpoint", ""),
            "raw_count": dividend.get("raw_count", 0),
            "universe_count": dividend.get("universe_count", 0),
            "flagged_count": dividend.get("flagged_count", 0),
            "tickers_sample": dividend.get("tickers", [])[:10],
            "dates_checked": dividend.get("dates_checked", []),
            "verified_through_date": dividend.get("dates_checked", [])[-1] if dividend.get("dates_checked") else today_str,
            "fundamentals_events_enqueued_new": div_new,
            "fundamentals_events_enqueued_skipped_existing": div_skip,
        },
        "step_2_6_earnings": {
            "mock_mode": earnings.get("mock_mode", False),
            "api_endpoint": earnings.get("api_endpoint", ""),
            "raw_count": earnings.get("raw_count", 0),
            "universe_count": earnings.get("universe_count", 0),
            "flagged_count": earnings.get("flagged_count", 0),
            "tickers_sample": earnings.get("tickers", [])[:10],
            "dates_checked": earnings.get("dates_checked", []),
            "verified_through_date": earnings.get("dates_checked", [])[-1] if earnings.get("dates_checked") else today_str,
            "fundamentals_events_enqueued_new": earn_new,
            "fundamentals_events_enqueued_skipped_existing": earn_skip,
        },
        "enqueued_total": enqueued_total,
        "skipped_total": skipped_total,
        "remediation": remediation_stats,
        "today": today_str,
        "cancelled": _cancelled,
    }


async def run_daily_price_sync(
    db,
    ignore_kill_switch: bool = False,
    parent_run_id: Optional[str] = None,
    chain_run_id: Optional[str] = None,
    run_doc_id: Optional[Any] = None,
    cancel_check: Optional[Callable[[], Awaitable[bool]]] = None,
) -> Dict[str, Any]:
    """
    Run daily price sync job with BULK CATCHUP.

    Phases:
    - Phase A: bulk last-day catchup → set has_price_data flags; track progress.
    - Phase B: detect splits/dividends/earnings since last check → set needs_* flags.
    - Phase C: download adjusted price history for split/dividend tickers.

    parent_run_id: exclusion_report_run_id of the preceding universe_seed run.
    chain_run_id: chain identifier shared across all steps in the pipeline run.
    Both are required; seeded_total is fetched strictly from the matching
    universe_seed ops_job_runs document. Fails with "missing_seeded_total_for_chain"
    if the document is not found or seeded_total is absent.

    run_doc_id: if provided, reuse an externally-inserted ops_job_runs sentinel
    instead of creating a new one (used by the admin full-pipeline chain).
    cancel_check: optional async callable that returns True if cancellation was
    requested (used by the full pipeline chain orchestrator).
    """
    from price_ingestion_service import (
        run_daily_bulk_catchup,
        _read_price_bulk_state,
        _write_price_bulk_state,
    )

    started_at = datetime.now(timezone.utc)
    job_name = "price_sync"
    await _finalize_stuck_price_sync_runs(db, started_at)

    logger.info(f"Starting {job_name} with gap detection and bulk catchup")

    # Use an externally-inserted sentinel (chain orchestrator) or create our own.
    if run_doc_id is not None:
        _running_doc_id = run_doc_id
        # Persist chain context to the externally-provided sentinel on start.
        # Uses dot-notation $set so existing fields are never overwritten.
        _ctx_result = await db.ops_job_runs.update_one(
            {"_id": _running_doc_id},
            {"$set": {
                "details.parent_run_id": parent_run_id,
                "details.chain_run_id": chain_run_id,
            }},
        )
        if _ctx_result.matched_count == 0:
            logger.warning(
                f"{job_name}: sentinel doc not found for run_doc_id={_running_doc_id}; "
                "chain context could not be persisted"
            )
    else:
        # Insert "running" sentinel so the frontend poll detects the job started
        _running_doc_id = (await db.ops_job_runs.insert_one({
            "job_name": job_name,
            "status": "running",
            "started_at": started_at,
            "source": "scheduler",
            "details": {"parent_run_id": parent_run_id, "chain_run_id": chain_run_id},
            "phase": "2.1_bulk_catchup",
            "progress_processed": 0,
            "progress_total": 0,
            "progress_pct": 0,
        })).inserted_id

    async def _is_cancelled() -> bool:
        """Check chain cancel callback and this run doc's cancel_requested status."""
        if cancel_check and await cancel_check():
            return True
        return await is_cancel_requested(db, _running_doc_id)

    # ── Single-flight lock: skip if another price_sync is already running ───
    _lock_owner_run_id = str(_running_doc_id)
    _lock_acquired = await _acquire_price_sync_lock(db, _lock_owner_run_id, started_at)
    if not _lock_acquired:
        finished_at = datetime.now(timezone.utc)
        await db.ops_job_runs.update_one(
            {"_id": _running_doc_id},
            {"$set": {
                "status": "skipped",
                "finished_at": finished_at,
                "started_at_prague": _to_prague_iso(started_at),
                "finished_at_prague": _to_prague_iso(finished_at),
                "log_timezone": "Europe/Prague",
                "error": "single_flight_lock_held",
                "progress": "Skipped: price_sync already running",
                "details.lock_id": PRICE_SYNC_LOCK_ID,
                "details.owner_run_id": _lock_owner_run_id,
            }},
        )
        logger.warning(f"{job_name}: single-flight lock held, skipping run")
        return {
            "job_name": job_name,
            "status": "skipped",
            "reason": "single_flight_lock_held",
            "started_at": started_at.isoformat(),
        }

    _heartbeat_stop = asyncio.Event()
    _heartbeat_task: Optional[asyncio.Task] = None

    async def _price_sync_heartbeat_worker() -> None:
        while not _heartbeat_stop.is_set():
            try:
                await asyncio.wait_for(_heartbeat_stop.wait(), timeout=PRICE_SYNC_HEARTBEAT_SECONDS)
                return  # stop event was set
            except asyncio.TimeoutError:
                pass  # normal: interval elapsed, do heartbeat
            try:
                hb_now = datetime.now(timezone.utc)
                await _heartbeat_price_sync_lock(db, _lock_owner_run_id, hb_now)
                await db.ops_job_runs.update_one(
                    {"_id": _running_doc_id},
                    {"$set": {"heartbeat_at": hb_now}},
                )
            except asyncio.CancelledError:
                return
            except Exception as _hb_exc:
                logger.debug(f"price_sync heartbeat error (best-effort): {_hb_exc}")

    async def _release_price_sync_resources() -> None:
        nonlocal _heartbeat_task, _lock_acquired
        _heartbeat_stop.set()
        if _heartbeat_task is not None:
            _heartbeat_task.cancel()
            await asyncio.gather(_heartbeat_task, return_exceptions=True)
            _heartbeat_task = None
        if _lock_acquired:
            await _release_price_sync_lock(db, _lock_owner_run_id)
            _lock_acquired = False

    _heartbeat_task = asyncio.create_task(_price_sync_heartbeat_worker())

    try:
        # Check kill switch (manual endpoints can explicitly bypass)
        if (not ignore_kill_switch) and (not await get_scheduler_enabled(db)):
            logger.warning(f"{job_name} skipped: kill switch engaged")
            await db.ops_job_runs.update_one(
                {"_id": _running_doc_id}, {"$set": {"status": "skipped"}}
            )
            return {
                "job_name": job_name,
                "status": "skipped",
                "reason": "kill_switch_engaged",
                "started_at": started_at.isoformat(),
            }

        # Check for cancel request before starting
        if await _is_cancelled():
            logger.info(f"{job_name} cancelled before start")
            _cancelled_at = datetime.now(timezone.utc)
            await db.ops_job_runs.update_one(
                {"_id": _running_doc_id}, {"$set": {
                    "status": "cancelled",
                    "finished_at": _cancelled_at,
                    "finished_at_prague": _to_prague_iso(_cancelled_at),
                    "phase": "stopped",
                    "cancelled_at": _cancelled_at,
                }}
            )
            return {
                "job_name": job_name,
                "status": "cancelled",
                "exclusion_report_run_id": None,
                "started_at": started_at.isoformat(),
                "finished_at": _cancelled_at.isoformat(),
            }

        # Progress helper: update running sentinel so UI poll can show live status.
        # Sets both text progress and structured numeric fields.
        async def _progress(
            msg: str,
            *,
            processed: Optional[int] = None,
            total: Optional[int] = None,
            phase: Optional[str] = None,
        ) -> None:
            fields: Dict[str, Any] = {"progress": msg}
            if phase is not None:
                fields["phase"] = phase
            if total is not None:
                fields["progress_total"] = total
            if processed is not None:
                fields["progress_processed"] = processed
                if total:
                    fields["progress_pct"] = min(round(100 * processed / total), 100)
            await db.ops_job_runs.update_one(
                {"_id": _running_doc_id},
                {"$set": fields},
            )

        # ── Phase A: bulk price catchup + has_price_data flags ────────────────
        # Strictly fetch seeded_total from the matching universe_seed run.
        # Requires both parent_run_id (exclusion_report_run_id of universe_seed)
        # and chain_run_id.  Fails loudly if not found — no fallbacks.
        if not parent_run_id or not chain_run_id:
            raise RuntimeError(
                "price_sync requires both parent_run_id and chain_run_id; "
                "missing_seeded_total_for_chain"
            )

        _parent_doc = await db.ops_job_runs.find_one(
            {
                "job_name": "universe_seed",
                "details.exclusion_report_run_id": parent_run_id,
                "details.chain_run_id": chain_run_id,
            },
            {"details.seeded_total": 1},
        )
        if not _parent_doc:
            raise RuntimeError("missing_seeded_total_for_chain")
        _seeded_val = (_parent_doc.get("details") or {}).get("seeded_total")
        if _seeded_val is None:
            raise RuntimeError("missing_seeded_total_for_chain")
        progress_total_step2 = int(_seeded_val)
        logger.info(
            f"{job_name}: seeded_total={progress_total_step2} sourced from universe_seed "
            f"(parent_run_id={parent_run_id}, chain_run_id={chain_run_id})"
        )

        # Run the bulk catchup with gap detection, streaming per-batch progress
        async def _bulk_progress(done: int, total: int, _: str) -> None:
            await _progress(
                f"2.1 Bulk price sync: {done} / {total} tickers",
                processed=done,
                total=total,
                phase="2.1_bulk_catchup",
            )

        # Load seeded ticker set so bulk catchup filters against the exact
        # same universe that Step 1 produced (respects suffix/dot filters).
        _seeded_docs = await db.tracked_tickers.find(
            SEED_QUERY, {"_id": 0, "ticker": 1}
        ).to_list(None)
        _seeded_set = {d["ticker"] for d in _seeded_docs if d.get("ticker")}
        min_bulk_rows_ok = MIN_BULK_ROWS_SANITY_CHECK
        min_matched_seeded_tickers_ok = MIN_BULK_MATCHED_SEEDED_SANITY_CHECK
        sanity_threshold_used = STEP2_SANITY_THRESHOLD_USED
        target_end_date = datetime.now(PRAGUE_TZ).date()

        # ── Determine last_closing_day (LCD) from market_calendar ─────────
        # LCD is the authoritative date used for the bulk URL and
        # processed_date.  It is the most recent trading day that has
        # already fully closed — obtained directly from the calendar via
        # get_last_closing_day() (strictly before today in Prague time).
        #
        # The calendar is the single source of truth for holidays and
        # weekends.  There is NO backward-iteration / speculative
        # probing of the EODHD bulk API.  If the calendar says the last
        # closed trading day is 2026-04-02 (because 2026-04-03 is Good
        # Friday), we ask EODHD for exactly that date.
        # Requirement: market_calendar MUST be populated and fresh.
        from services.market_calendar_service import (
            get_last_closing_day as _get_last_closing_day,
            is_calendar_fresh as _is_calendar_fresh,
        )
        from price_ingestion_service import (
            fetch_bulk_eod_latest as _fetch_bulk_eod_latest,
        )
        _calendar_fresh = await _is_calendar_fresh(db)
        if not _calendar_fresh:
            raise RuntimeError("market_calendar_missing_or_stale")
        _last_closing_day = await _get_last_closing_day(
            db, "US", as_of_date=target_end_date.isoformat(),
        )
        if not _last_closing_day:
            raise RuntimeError("market_calendar_missing_or_stale")

        await _progress(
            f"2.1 Fetching bulk EOD for last closed trading day: {_last_closing_day}",
            phase="2.1_bulk_catchup",
        )

        # Pre-fetch bulk data for the last closing day so we can pass it
        # to run_daily_bulk_catchup (avoids a redundant API call).
        _lcd_bulk_data: Optional[list] = None
        _v_data, _v_fetched = await _fetch_bulk_eod_latest(
            "US", include_meta=True, for_date=_last_closing_day,
        )
        if _v_data:
            _lcd_bulk_data = _v_data

        logger.info(
            f"{job_name}: last_closing_day={_last_closing_day} "
            f"(as_of={target_end_date.isoformat()})"
        )
        _bulk_url_display = (
            f"https://eodhd.com/api/eod-bulk-last-day/US?date={_last_closing_day}&api_token=YOUR_API_TOKEN&fmt=json"
        )

        price_bulk_state = await _read_price_bulk_state(db)
        watermark_before = (
            (price_bulk_state or {}).get("global_last_bulk_date_processed")
            if isinstance(price_bulk_state, dict)
            else None
        )

        if watermark_before:
            try:
                # Validate stored watermark format for telemetry/debugging only.
                _ = datetime.fromisoformat(str(watermark_before))
            except Exception:
                logger.warning(
                    f"{job_name}: invalid pipeline_state.price_bulk.global_last_bulk_date_processed={watermark_before!r}; ignoring"
                )
                watermark_before = None

        should_attempt_bulk_fetch = progress_total_step2 > 0

        days: List[Dict[str, Any]] = []
        await db.ops_job_runs.update_one(
            {"_id": _running_doc_id},
            {"$set": {"details.price_bulk_gapfill": {
                "watermark_before": watermark_before,
                "target_end_date": target_end_date.strftime("%Y-%m-%d"),
                "last_closing_day": _last_closing_day,
                "min_bulk_rows_ok": min_bulk_rows_ok,
                "min_matched_seeded_tickers_ok": min_matched_seeded_tickers_ok,
                "sanity_threshold_used": sanity_threshold_used,
                "bulk_url_used": _bulk_url_display,
                "ticker_samples": {
                    "bulk_rows_sample": [],
                    "bulk_rows_normalized_sample": [],
                    "seeded_tickers_sample": [],
                    "seeded_tickers_normalized_sample": [],
                },
                "days": days,
            }}},
        )
        result_gapfill = {
            "watermark_before": watermark_before,
            "target_end_date": target_end_date.strftime("%Y-%m-%d"),
            "last_closing_day": _last_closing_day,
            "min_bulk_rows_ok": min_bulk_rows_ok,
            "min_matched_seeded_tickers_ok": min_matched_seeded_tickers_ok,
            "sanity_threshold_used": sanity_threshold_used,
            "bulk_url_used": _bulk_url_display,
            "ticker_samples": {
                "bulk_rows_sample": [],
                "bulk_rows_normalized_sample": [],
                "seeded_tickers_sample": [],
                "seeded_tickers_normalized_sample": [],
            },
            "days": days,
        }
        if not should_attempt_bulk_fetch:
            result_gapfill["skip_reason"] = (
                "bulk fetch skipped: no seeded tickers available for Step 2"
            )
            await db.ops_job_runs.update_one(
                {"_id": _running_doc_id},
                {"$set": {"details.price_bulk_gapfill.skip_reason": result_gapfill["skip_reason"]}},
            )

        result: Dict[str, Any] = {
            "status": "success",
            "dates_processed": 0,
            "records_upserted": 0,
            "api_calls": 0,
            "bulk_fetch_executed": False,
            "raw_row_count": 0,
            "rows_written": 0,
            "price_bulk_gapfill_days_count": 0,
            "bulk_writes": 0,
            "bulk_url_used": _bulk_url_display,
            "last_closing_day": _last_closing_day,
            "tickers_with_price": [],
            "price_bulk_gapfill": result_gapfill,
            "matched_seeded_tickers_count": 0,
            "match_ratio": 0.0,
            "sanity_threshold_used": sanity_threshold_used,
        }
        _tickers_with_price_set: set = set()
        bulk_attempts = [None] if should_attempt_bulk_fetch else []
        if should_attempt_bulk_fetch:
            await _progress(
                f"2.1 Fetching prices for {_last_closing_day} (bulk)…",
                processed=0,
                total=progress_total_step2,
                phase="2.1_bulk_catchup",
            )
        for idx, _ in enumerate(bulk_attempts):
            await _progress(
                f"2.1 Bulk gapfill {idx + 1}/{len(bulk_attempts)} "
                f"(date={_last_closing_day})…",
                phase="2.1_bulk_catchup",
            )
            should_append_day = True
            day: Dict[str, Any] = {
                "bulk_date": _last_closing_day,
                "processed_date": _last_closing_day,
                "unique_dates": [],
                "bulk_url_used": _bulk_url_display,
                "status": "error",
                "rows_written": 0,
                "advanced_watermark": False,
                "error": None,
            }
            try:
                day_result = await run_daily_bulk_catchup(
                    db,
                    progress_cb=_bulk_progress,
                    seeded_tickers_override=_seeded_set,
                    latest_trading_day=_last_closing_day,
                    bulk_data_override=_lcd_bulk_data,
                )

                # ── Non-trading-day skip: treat as successful no-op ─────────
                # When market_calendar says the EODHD date is a holiday/weekend,
                # run_daily_bulk_catchup returns status="skipped" with
                # skipped_reason="non_trading_day".  Treat this as a successful
                # completion so the scheduler advances state and does NOT retry.
                if day_result.get("skipped_reason") == "non_trading_day":
                    _ntd_date = day_result.get("date", "unknown")
                    logger.info(
                        f"{job_name}: bulk data date {_ntd_date} is not a trading day — "
                        "skipping price sync (no writes, marking as completed)"
                    )
                    should_append_day = False
                    result["status"] = "completed"
                    result["bulk_fetch_executed"] = True
                    result["api_calls"] = 1
                    result["raw_row_count"] = day_result.get("raw_row_count", 0)
                    result["skipped_reason"] = "non_trading_day"
                    result["skipped_date"] = _ntd_date
                    await _progress(
                        f"Skipped: {_ntd_date} is not a US trading day (holiday/weekend)",
                        phase="completed",
                    )
                    break

                day_bulk_fetch_executed = bool(day_result.get("bulk_fetch_executed"))
                result["bulk_fetch_executed"] = day_bulk_fetch_executed
                result["api_calls"] = 1 if result["bulk_fetch_executed"] else 0
                result["raw_row_count"] = day_result.get("raw_row_count", 0)
                if not day_bulk_fetch_executed:
                    should_append_day = False
                    day["status"] = "error"
                    day["error"] = "bulk fetch not executed"
                    result["status"] = "error"
                    break
                day["bulk_url_used"] = day_result.get("bulk_url_used", day["bulk_url_used"])
                result_gapfill["ticker_samples"] = day_result.get(
                    "ticker_samples",
                    result_gapfill.get("ticker_samples", {}),
                )
                await db.ops_job_runs.update_one(
                    {"_id": _running_doc_id},
                    {"$set": {"details.price_bulk_gapfill.ticker_samples": result_gapfill["ticker_samples"]}},
                )
                # Use the actual date from the bulk payload (date_seen)
                # instead of blindly trusting the calendar's last_closing_day.
                # If the calendar is correct both should always be equal,
                # but this provides defence-in-depth.
                _actual_date = (
                    day_result.get("processed_date")
                    or day_result.get("date")
                    or _last_closing_day
                )
                day["processed_date"] = _actual_date
                day["bulk_date"] = _actual_date
                day["unique_dates"] = day_result.get("unique_dates", [])
                result["bulk_writes"] += day_result.get("bulk_writes", 0)

                if len(day["unique_dates"]) != 1:
                    _ud = day["unique_dates"]
                    day["status"] = "error"
                    day["error"] = (
                        f"bulk payload has no dates (empty payload) for date={_last_closing_day}"
                        if len(_ud) == 0
                        else f"bulk payload contains multiple dates ({_ud}) for date={_last_closing_day}"
                    )
                    result["status"] = "error"
                    days.append(day)
                    if len(days) > MAX_BULK_GAPFILL_DAYS_HISTORY:
                        days = days[-MAX_BULK_GAPFILL_DAYS_HISTORY:]
                    await db.ops_job_runs.update_one(
                        {"_id": _running_doc_id},
                        {"$set": {"details.price_bulk_gapfill.days": days}},
                    )
                    result_gapfill["days"] = days
                    break

                processed_date = day.get("processed_date")
                rows_written = (
                    day_result.get("records_upserted", 0)
                    if processed_date
                    else 0
                )
                day["rows_written"] = rows_written
                result["rows_written"] = rows_written
                _day_tickers_with_price_data = day_result.get("tickers_with_price_data")
                _day_matched_price_tickers_raw = day_result.get("matched_price_tickers_raw")
                if _day_tickers_with_price_data is not None:
                    matched_seeded_tickers_count = int(_day_tickers_with_price_data)
                elif _day_matched_price_tickers_raw is not None:
                    matched_seeded_tickers_count = int(_day_matched_price_tickers_raw)
                else:
                    matched_seeded_tickers_count = len(day_result.get("tickers_with_price") or [])
                match_ratio = (
                    (matched_seeded_tickers_count / progress_total_step2)
                    if progress_total_step2 > 0
                    else 0.0
                )
                day["matched_seeded_tickers_count"] = matched_seeded_tickers_count
                day["match_ratio"] = match_ratio
                day["sanity_threshold_used"] = sanity_threshold_used
                result["matched_seeded_tickers_count"] = matched_seeded_tickers_count
                result["match_ratio"] = match_ratio
                day_ok = matched_seeded_tickers_count >= min_matched_seeded_tickers_ok
                if day_ok:
                    now_utc = datetime.now(timezone.utc)
                    await _write_price_bulk_state(db, processed_date, now_utc)
                    day["status"] = "success"
                    day["advanced_watermark"] = True
                    result["dates_processed"] += day_result.get("dates_processed", 0)
                    result["records_upserted"] += day_result.get("records_upserted", 0)
                    _tickers_with_price_set.update(day_result.get("tickers_with_price", []))
                else:
                    day["status"] = "failed_sanity"
                    day["error"] = (
                        "bulk match sanity failed: "
                        f"matched_seeded_tickers_count={matched_seeded_tickers_count} "
                        f"< min_matched_seeded_tickers_ok={min_matched_seeded_tickers_ok} "
                        f"(match_ratio={match_ratio:.4f}) for processed_date={processed_date}"
                    )
                    result["status"] = "error"
            except Exception as exc:
                day["status"] = "error"
                day["error"] = str(exc)
                result["status"] = "error"

            if should_append_day:
                days.append(day)
                if len(days) > MAX_BULK_GAPFILL_DAYS_HISTORY:
                    days = days[-MAX_BULK_GAPFILL_DAYS_HISTORY:]
                await db.ops_job_runs.update_one(
                    {"_id": _running_doc_id},
                    {"$set": {"details.price_bulk_gapfill.days": days}},
                )
                result_gapfill["days"] = days
            if day["status"] != "success":
                break

        result["tickers_with_price"] = sorted(_tickers_with_price_set)
        result["api_calls"] = 1 if result.get("bulk_fetch_executed") else 0
        result["price_bulk_gapfill_days_count"] = len(days)

        # ── Early exit on Phase A error (sanity failure / bulk exception) ─────
        # When result["status"] is "error" after the bulk loop, finalize the
        # ops_job_runs doc immediately instead of falling through to Phase B
        # where downstream code may raise and leave the doc stuck at "running".
        if result.get("status") == "error":
            _err_msg = ""
            if days:
                _err_msg = days[-1].get("error") or ""
            if not _err_msg:
                _err_msg = "Phase A bulk price sync failed"
            _phase_a_err_at = datetime.now(timezone.utc)
            await db.ops_job_runs.update_one(
                {"_id": _running_doc_id},
                {"$set": {
                    "status": "error",
                    "finished_at": _phase_a_err_at,
                    "finished_at_prague": _to_prague_iso(_phase_a_err_at),
                    "log_timezone": "Europe/Prague",
                    "phase": "2.1_bulk_catchup",
                    "error": _err_msg,
                    "error_message": _err_msg,
                    "details": {
                        **result,
                        "price_bulk_gapfill": result_gapfill,
                        "parent_run_id": parent_run_id,
                        "chain_run_id": chain_run_id,
                    },
                }},
            )
            logger.error(f"{job_name}: Phase A error — {_err_msg}")
            return {
                "job_name": job_name,
                "status": "error",
                "error": _err_msg,
                "records_upserted": result.get("records_upserted", 0),
                "dates_processed": result.get("dates_processed", 0),
                "api_calls": result.get("api_calls", 0),
                "started_at": started_at.isoformat(),
                "finished_at": _phase_a_err_at.isoformat(),
            }

        if should_attempt_bulk_fetch and (not result.get("bulk_fetch_executed", False)):
            _bulk_guard_msg = (
                "Step 2 bulk guard triggered: no bulk fetch executed "
                f"(api_calls={result.get('api_calls', 0)}, "
                f"bulk_fetch_executed={result.get('bulk_fetch_executed', False)}, "
                f"days={len(days)}). "
                "Aborting run to prevent false success."
            )
            _bulk_guard_finished_at = datetime.now(timezone.utc)
            await db.ops_job_runs.update_one(
                {"_id": _running_doc_id},
                {"$set": {
                    "status": "error",
                    "finished_at": _bulk_guard_finished_at,
                    "finished_at_prague": _to_prague_iso(_bulk_guard_finished_at),
                    "phase": "2.1_bulk_catchup",
                    "error": _bulk_guard_msg,
                    "details": {
                        **result,
                        "parent_run_id": parent_run_id,
                        "chain_run_id": chain_run_id,
                    },
                }},
            )
            logger.error(f"{job_name}: {_bulk_guard_msg}")
            return {
                "job_name": job_name,
                "status": "error",
                "error": _bulk_guard_msg,
                "records_upserted": result.get("records_upserted", 0),
                "dates_processed": result.get("dates_processed", 0),
                "api_calls": result.get("api_calls", 0),
                "started_at": started_at.isoformat(),
                "finished_at": _bulk_guard_finished_at.isoformat(),
            }

        # ── Non-trading-day early exit ────────────────────────────────────────
        # When the bulk catchup detected a holiday/weekend date from EODHD,
        # skip Phases A-tail / B / C entirely.  Return "completed" so the
        # scheduler marks the step as done and does not retry.
        #
        # IMPORTANT: we still call sync_has_price_data_flags (DB-fallback mode)
        # so that has_price_data reflects the *last real trading day* stored in
        # stock_prices.  Without this, a pipeline run on a holiday after a fresh
        # deploy (or first-ever run) would leave all tickers with
        # has_price_data=False and Step 3 would process 0 tickers.
        if result.get("skipped_reason") == "non_trading_day":
            _ntd_date = result.get("skipped_date", "unknown")
            logger.info(
                f"{job_name}: {_ntd_date} is not a trading day — "
                "syncing has_price_data flags from stock_prices (DB fallback)"
            )
            _ntd_flag_summary = await sync_has_price_data_flags(
                db, include_exclusions=True, tickers_with_price=None,
            )
            logger.info(
                f"{job_name}: non-trading-day flag sync: "
                f"{_ntd_flag_summary['with_price_data']}/{_ntd_flag_summary['seeded_total']} "
                "tickers with price data (from stock_prices DB)"
            )

            _ntd_finished = datetime.now(timezone.utc)
            await db.ops_job_runs.update_one(
                {"_id": _running_doc_id},
                {"$set": {
                    "status": "completed",
                    "finished_at": _ntd_finished,
                    "finished_at_prague": _to_prague_iso(_ntd_finished),
                    "phase": "completed",
                    "progress": (
                        f"No price sync needed: {_ntd_date} is not a US trading day "
                        "(holiday/weekend per market_calendar). "
                        f"has_price_data flags synced from DB: "
                        f"{_ntd_flag_summary['with_price_data']}/{_ntd_flag_summary['seeded_total']}"
                    ),
                    "details": {
                        **result,
                        "parent_run_id": parent_run_id,
                        "chain_run_id": chain_run_id,
                        "non_trading_day_flag_sync": _ntd_flag_summary,
                    },
                }},
            )
            logger.info(
                f"{job_name}: completed (non-trading-day skip for {_ntd_date})"
            )
            return {
                "job_name": job_name,
                "status": "completed",
                "skipped_reason": "non_trading_day",
                "skipped_date": _ntd_date,
                "records_upserted": 0,
                "dates_processed": 0,
                "api_calls": result.get("api_calls", 0),
                "started_at": started_at.isoformat(),
                "finished_at": _ntd_finished.isoformat(),
                "non_trading_day_flag_sync": _ntd_flag_summary,
            }

        await _progress(
            f"2.1 Prices synced: {result.get('records_upserted', 0)} records "
            f"across {result.get('dates_processed', 0)} date(s). "
            "Updating has_price_data flags…",
            phase="2.1_bulk_catchup",
        )

        # Canonical Step 2 behavior: update has_price_data flags after bulk ingest
        _bulk_tickers_with_price = result.get("tickers_with_price", None)
        price_flag_summary = await sync_has_price_data_flags(
            db, include_exclusions=True,
            tickers_with_price=_bulk_tickers_with_price,
            bulk_date=_last_closing_day,
        )
        seeded_total = price_flag_summary["seeded_total"]
        with_price = price_flag_summary["with_price_data"]
        _result_matched_seeded_tickers_count = result.get("matched_seeded_tickers_count")
        _result_tickers_with_price = result.get("tickers_with_price")
        if _result_matched_seeded_tickers_count is not None:
            matched_seeded_tickers_count = int(_result_matched_seeded_tickers_count)
        elif _result_tickers_with_price is not None:
            matched_seeded_tickers_count = len(_result_tickers_with_price)
        else:
            matched_seeded_tickers_count = int(price_flag_summary.get("matched_price_tickers_raw", 0))
        match_ratio = (
            (matched_seeded_tickers_count / seeded_total)
            if seeded_total > 0
            else 0.0
        )
        result["matched_seeded_tickers_count"] = matched_seeded_tickers_count
        result["match_ratio"] = match_ratio
        result["sanity_threshold_used"] = sanity_threshold_used
        result["tickers_seeded_total"] = seeded_total
        result["tickers_with_price_data"] = matched_seeded_tickers_count
        result["tickers_without_price_data"] = max(seeded_total - matched_seeded_tickers_count, 0)
        result["matched_price_tickers_raw"] = matched_seeded_tickers_count
        result.update(
            await save_price_sync_exclusion_report(
                db,
                rows=price_flag_summary.get("exclusions", []),
                now=datetime.now(timezone.utc),
            )
        )
        if not result.get("exclusion_report_run_id"):
            missing_run_id_msg = (
                "Step 2 price sync result missing required exclusion_report_run_id field"
            )
            raise RuntimeError(missing_run_id_msg)

        await _progress(
            "2.2 Running split/dividend/earnings detectors…",
            processed=with_price,
            total=seeded_total,
            phase="2.2_split",
        )

        # ── Stop check between Phase A and Phase B ────────────────────────────
        if await _is_cancelled():
            logger.info(f"{job_name} cancelled after Phase A (bulk catchup)")
            _cancelled_at = datetime.now(timezone.utc)
            await db.ops_job_runs.update_one(
                {"_id": _running_doc_id},
                {"$set": {
                    "status": "cancelled",
                    "finished_at": _cancelled_at,
                    "finished_at_prague": _to_prague_iso(_cancelled_at),
                    "phase": "stopped",
                    "details": {
                        "dates_processed": result.get("dates_processed", 0),
                        "records_upserted": result.get("records_upserted", 0),
                        "rows_written": result.get("rows_written", 0),
                        "raw_row_count": result.get("raw_row_count", 0),
                        "bulk_fetch_executed": result.get("bulk_fetch_executed", False),
                        "api_calls": result.get("api_calls", 0),
                        "price_bulk_gapfill_days_count": result.get("price_bulk_gapfill_days_count", 0),
                        "bulk_url_used": result.get("bulk_url_used"),
                        "tickers_seeded_total": seeded_total,
                        "tickers_with_price_data": with_price,
                        "exclusion_report_run_id": result.get("exclusion_report_run_id"),
                        "price_bulk_gapfill": result.get("price_bulk_gapfill", {}),
                        "parent_run_id": parent_run_id,
                        "chain_run_id": chain_run_id,
                        "stop_reason": "cancel_requested_after_phase_a",
                    },
                }},
            )
            return {
                "job_name": job_name,
                "status": "cancelled",
                "exclusion_report_run_id": result.get("exclusion_report_run_id"),
                "tickers_seeded_total": seeded_total,
                "tickers_with_price_data": with_price,
                "started_at": started_at.isoformat(),
                "finished_at": _cancelled_at.isoformat(),
            }

        # ── Phase B+C: event detectors + price history remediation ────────────
        await _progress(
            "2.2 Running split/dividend/earnings detectors…",
            processed=with_price,
            total=seeded_total,
            phase="2.2_split",
        )

        # Derive the canonical phase label from the detector message prefix.
        _DETECTOR_PHASE_MAP = {
            "2.2": "2.2_split",
            "2.4": "2.4_dividend",
            "2.6": "2.6_earnings",
            "2.7": "2.6_earnings",  # price-redownload remediation follows earnings
        }

        async def _detector_progress(msg: str) -> None:
            prefix = msg[:3] if len(msg) >= 3 else ""
            ph = _DETECTOR_PHASE_MAP.get(prefix, "2.2_split")
            await _progress(msg, processed=with_price, total=seeded_total, phase=ph)

        # Step 2.2 / 2.4 / 2.6 detectors -> enqueue fundamentals refresh events.
        event_detector_summary = await run_step2_event_detectors(
            db,
            progress_cb=_detector_progress,
            exclusion_meta={
                "run_id": result.get("exclusion_report_run_id"),
                "report_date": result.get("exclusion_report_date"),
            },
            cancel_check=_is_cancelled,
            processed_date=(days[0].get("processed_date") if days else None),
        )
        result["event_detectors"] = event_detector_summary
        result["fundamentals_events_enqueued"] = event_detector_summary.get("enqueued_total", 0)
        result["fundamentals_events_enqueued_skipped_existing"] = event_detector_summary.get("skipped_total", 0)

        # Check if detectors were cancelled mid-run
        if event_detector_summary.get("cancelled"):
            logger.info(f"{job_name} cancelled during event detection phase")
            _cancelled_at = datetime.now(timezone.utc)
            await db.ops_job_runs.update_one(
                {"_id": _running_doc_id},
                {"$set": {
                    "status": "cancelled",
                    "finished_at": _cancelled_at,
                    "finished_at_prague": _to_prague_iso(_cancelled_at),
                    "phase": "stopped",
                    "details": {
                        "dates_processed": result.get("dates_processed", 0),
                        "records_upserted": result.get("records_upserted", 0),
                        "rows_written": result.get("rows_written", 0),
                        "raw_row_count": result.get("raw_row_count", 0),
                        "bulk_fetch_executed": result.get("bulk_fetch_executed", False),
                        "api_calls": result.get("api_calls", 0),
                        "price_bulk_gapfill_days_count": result.get("price_bulk_gapfill_days_count", 0),
                        "bulk_url_used": result.get("bulk_url_used"),
                        "tickers_seeded_total": seeded_total,
                        "tickers_with_price_data": result.get("tickers_with_price_data", with_price),
                        "tickers_without_price_data": result.get("tickers_without_price_data", 0),
                        "matched_price_tickers_raw": result.get("matched_price_tickers_raw", 0),
                        "matched_seeded_tickers_count": result.get("matched_seeded_tickers_count", 0),
                        "match_ratio": result.get("match_ratio", 0.0),
                        "sanity_threshold_used": result.get("sanity_threshold_used", sanity_threshold_used),
                        "event_detectors": event_detector_summary,
                        "fundamentals_events_enqueued": result.get("fundamentals_events_enqueued", 0),
                        "fundamentals_events_enqueued_skipped_existing": result.get("fundamentals_events_enqueued_skipped_existing", 0),
                        "exclusion_report_run_id": result.get("exclusion_report_run_id"),
                        "price_bulk_gapfill": result.get("price_bulk_gapfill", {}),
                        "parent_run_id": parent_run_id,
                        "chain_run_id": chain_run_id,
                        "stop_reason": "cancel_requested_during_phase_bc",
                    },
                }},
            )
            return {
                "job_name": job_name,
                "status": "cancelled",
                "exclusion_report_run_id": result.get("exclusion_report_run_id"),
                "tickers_seeded_total": seeded_total,
                "tickers_with_price_data": with_price,
                "fundamentals_events_enqueued": result.get("fundamentals_events_enqueued", 0),
                "fundamentals_events_enqueued_skipped_existing": result.get("fundamentals_events_enqueued_skipped_existing", 0),
                "started_at": started_at.isoformat(),
                "finished_at": _cancelled_at.isoformat(),
            }

        await _progress(
            f"Done: {with_price} tickers with prices. "
            f"{result.get('fundamentals_events_enqueued', 0)} fundamentals events enqueued.",
            processed=with_price,
            total=seeded_total,
            phase="completed",
        )

        # Log to ops_job_runs
        finished_at = datetime.now(timezone.utc)
        await db.ops_job_runs.update_one(
            {"_id": _running_doc_id},
            {"$set": {
                "finished_at": finished_at,
                "started_at_prague": _to_prague_iso(started_at),
                "finished_at_prague": _to_prague_iso(finished_at),
                "log_timezone": "Europe/Prague",
                "status": result.get("status", "completed"),
                "phase": "completed",
                "progress_processed": with_price,
                "progress_total": seeded_total,
                "progress_pct": min(round(100 * with_price / seeded_total), 100) if seeded_total else 0,
                    "details": {
                        "dates_processed": result.get("dates_processed", 0),
                        "records_upserted": result.get("records_upserted", 0),
                        "rows_written": result.get("rows_written", 0),
                        "raw_row_count": result.get("raw_row_count", 0),
                        "bulk_fetch_executed": result.get("bulk_fetch_executed", False),
                        "bulk_url_used": result.get("bulk_url_used"),
                        "tickers_seeded_total": result.get("tickers_seeded_total", 0),
                        "tickers_with_price_data": result.get("tickers_with_price_data", 0),
                    "tickers_without_price_data": result.get("tickers_without_price_data", 0),
                    "matched_price_tickers_raw": result.get("matched_price_tickers_raw", 0),
                    "matched_seeded_tickers_count": result.get("matched_seeded_tickers_count", 0),
                    "match_ratio": result.get("match_ratio", 0.0),
                    "sanity_threshold_used": result.get("sanity_threshold_used", sanity_threshold_used),
                    "exclusion_report_rows": result.get("exclusion_report_rows", 0),
                    "api_calls": result.get("api_calls", 0),
                    "bulk_writes": result.get("bulk_writes", 0),
                    "price_bulk_gapfill_days_count": result.get("price_bulk_gapfill_days_count", 0),
                    "fundamentals_events_enqueued": result.get("fundamentals_events_enqueued", 0),
                    "fundamentals_events_enqueued_skipped_existing": result.get("fundamentals_events_enqueued_skipped_existing", 0),
                    "event_detectors": result.get("event_detectors", {}),
                    "exclusion_report_run_id": result.get("exclusion_report_run_id"),
                    "price_bulk_gapfill": result.get("price_bulk_gapfill", {}),
                    "parent_run_id": parent_run_id,
                    "chain_run_id": chain_run_id,
                },
            }}
        )
        
        logger.info(f"{job_name} completed: {result.get('records_upserted', 0)} records, "
                   f"{result.get('dates_processed', 0)} gap dates processed, "
                   f"{result.get('api_calls', 0)} API calls")
        
        return {
            "job_name": job_name,
            "status": result.get("status", "completed"),
            "records_upserted": result.get("records_upserted", 0),
            "rows_written": result.get("rows_written", 0),
            "raw_row_count": result.get("raw_row_count", 0),
            "tickers_seeded_total": result.get("tickers_seeded_total", 0),
            "tickers_with_price_data": result.get("tickers_with_price_data", 0),
            "tickers_without_price_data": result.get("tickers_without_price_data", 0),
            "matched_price_tickers_raw": result.get("matched_price_tickers_raw", 0),
            "matched_seeded_tickers_count": result.get("matched_seeded_tickers_count", 0),
            "match_ratio": result.get("match_ratio", 0.0),
            "sanity_threshold_used": result.get("sanity_threshold_used", sanity_threshold_used),
            "exclusion_report_rows": result.get("exclusion_report_rows", 0),
            "exclusion_report_run_id": result.get("exclusion_report_run_id"),
            "dates_processed": result.get("dates_processed", 0),
            "api_calls": result.get("api_calls", 0),
            "fundamentals_events_enqueued": result.get("fundamentals_events_enqueued", 0),
            "fundamentals_events_enqueued_skipped_existing": result.get("fundamentals_events_enqueued_skipped_existing", 0),
            "event_detectors": result.get("event_detectors", {}),
            "started_at": started_at.isoformat(),
            "finished_at": datetime.now(timezone.utc).isoformat(),
        }

    except Exception as e:
        error_msg = str(e)
        logger.error(f"{job_name} error: {error_msg}")

        _fail_finished_at = datetime.now(timezone.utc)
        await db.ops_job_runs.update_one(
            {"_id": _running_doc_id},
            {"$set": {
                "status": "error",
                "finished_at": _fail_finished_at,
                "finished_at_prague": _to_prague_iso(_fail_finished_at),
                "error": error_msg,
                "error_message": error_msg,
                "details.parent_run_id": parent_run_id,
                "details.chain_run_id": chain_run_id,
            }},
        )

        return {
            "job_name": job_name,
            "status": "error",
            "error": error_msg,
            "exclusion_report_run_id": None,
            "records_upserted": 0,
            "dates_processed": 0,
            "started_at": started_at.isoformat(),
            "finished_at": _fail_finished_at.isoformat(),
        }
    finally:
        # ── Safety net: guarantee ops_job_runs is never left at "running" ─────
        try:
            _still_running = await db.ops_job_runs.find_one(
                {"_id": _running_doc_id, "status": "running"},
                {"_id": 1},
            )
            if _still_running:
                _safety_at = datetime.now(timezone.utc)
                await db.ops_job_runs.update_one(
                    {"_id": _running_doc_id, "status": "running"},
                    {"$set": {
                        "status": "error",
                        "finished_at": _safety_at,
                        "finished_at_prague": _to_prague_iso(_safety_at),
                        "log_timezone": "Europe/Prague",
                        "error": "terminated_without_final_status",
                        "error_message": "terminated_without_final_status",
                    }},
                )
                logger.error(
                    f"{job_name}: finally safety net — forced status='error' "
                    f"(doc was still 'running' at exit)"
                )
        except Exception as _safety_exc:
            logger.error(
                f"{job_name}: finally safety net DB update failed: {_safety_exc}"
            )
        await _release_price_sync_resources()


async def sync_has_price_data_flags(db, include_exclusions: bool = False, tickers_with_price: Optional[List[str]] = None, bulk_date: Optional[str] = None) -> Dict[str, Any]:
    """
    Recompute price-related flags for seeded US Common Stock universe.

    Sets THREE fields on each tracked_ticker document:

    - **has_latest_bulk_close**: True ONLY if the ticker appeared in the
      processed bulk for this run with close > 0.  Informational — tracks
      whether the ticker is "active" in today's EODHD bulk feed.

    - **has_price_data** (visibility gate): True ONLY if the ticker
      appeared in today's bulk with close > 0.  Identical to
      has_latest_bulk_close.  A ticker NOT in the latest EODHD bulk
      report is NOT visible — period.

    - **has_price_history**: True if the ticker has ANY historical record
      in ``stock_prices`` with close > 0 (from Phase C, manual backfill,
      or previous bulk runs).  Informational only — does NOT gate
      visibility.

    Side-effects:

    - **gap_free_exclusions**: When *bulk_date* is provided and tickers
      are NOT in today's bulk, writes ``gap_free_exclusions`` entries
      (date=bulk_date, reason="not_in_bulk_data") so they are excluded
      from the gap-free metric and shown as data_notices in the chart.

    - **Re-download trigger**: Tickers that were absent from yesterday's
      bulk (has_latest_bulk_close=False) but appear in today's bulk AND
      had ``price_history_complete=True`` are flagged with
      ``needs_price_redownload=True`` so Phase C re-downloads their full
      price history on the next run.

    When *tickers_with_price* is a non-empty list (normal trading day),
    flags are reset-and-set from that list exclusively.

    When *tickers_with_price* is an empty list (non-trading day / fetch
    failure), we skip the flag reset entirely so previous-day values persist.

    When *tickers_with_price* is None (legacy / manual call), we fall back
    to querying ``stock_prices`` — both flags get the same value.
    """
    seeded_docs = await db.tracked_tickers.find(
        SEED_QUERY,
        {"_id": 0, "ticker": 1, "name": 1}
    ).to_list(None)
    seeded_tickers = [d.get("ticker") for d in seeded_docs if d.get("ticker")]
    name_by_ticker = {
        d.get("ticker"): (d.get("name") or "(unknown)")
        for d in seeded_docs
        if d.get("ticker")
    }
    seeded_total = len(seeded_tickers)
    if seeded_total == 0:
        base: Dict[str, Any] = {
            "seeded_total": 0,
            "with_latest_bulk_close": 0,
            "without_any_price": 0,
            "with_price_data": 0,
            "without_price_data": 0,
            "matched_price_tickers_raw": 0,
        }
        if include_exclusions:
            base["exclusions"] = []
        return base

    seeded_set = set(seeded_tickers)
    from price_ingestion_service import _normalize_step2_ticker

    # ── Tracks whether we should write bulk-close flags this run ─────────
    skip_bulk_flag_reset = False
    any_price_set: Set[str] = set()  # for exclusion-report "close=0" distinction
    returning_tickers: Set[str] = set()  # tickers absent yesterday, back today
    _excl_written = 0
    _returning_cooldown_skipped = 0
    _returning_no_gap_skipped = 0

    if tickers_with_price is not None and len(tickers_with_price) > 0:
        # ── Normal trading day: bulk data available ──────────────────────
        bulk_close_set = {
            _normalize_step2_ticker(t)
            for t in tickers_with_price
            if _normalize_step2_ticker(t)
        } & seeded_set
        any_price_set = bulk_close_set  # same set when sourced from bulk feed
        matched_raw = len(bulk_close_set)

    elif tickers_with_price is not None and len(tickers_with_price) == 0:
        # ── Empty bulk (non-trading day or fetch failure) ────────────────
        # Do NOT reset flags — preserve previous trading day's values.
        logger.warning(
            "[sync_has_price_data_flags] tickers_with_price is empty but "
            "%d tickers are seeded — skipping flag reset to preserve "
            "previous trading day values",
            seeded_total,
        )
        bulk_close_set: Set[str] = set()
        matched_raw = 0
        skip_bulk_flag_reset = True

    else:
        # ── Legacy fallback (tickers_with_price is None): query stock_prices
        seeded_codes = [t[:-3] if t.endswith(".US") else t for t in seeded_tickers]
        ticker_candidates = list(set(seeded_tickers) | set(seeded_codes))
        valid_price_query = {
            "ticker": {"$in": ticker_candidates},
            "$or": [
                {"close": {"$gt": 0}},
                {"adjusted_close": {"$gt": 0}},
            ],
        }
        raw_price_tickers = await db.stock_prices.distinct("ticker", valid_price_query)
        raw_any_price_tickers = await db.stock_prices.distinct(
            "ticker",
            {"ticker": {"$in": ticker_candidates}}
        )

        normalized_price_tickers = {
            _normalize_step2_ticker(t)
            for t in raw_price_tickers
            if _normalize_step2_ticker(t)
        }
        normalized_any_price_tickers = {
            _normalize_step2_ticker(t)
            for t in raw_any_price_tickers
            if _normalize_step2_ticker(t)
        }
        bulk_close_set = normalized_price_tickers & seeded_set
        any_price_set = normalized_any_price_tickers & seeded_set
        matched_raw = len(bulk_close_set)

    # ── Reset & set has_latest_bulk_close + has_price_data (visibility gate) ──
    if not skip_bulk_flag_reset:
        now_ts = datetime.now(timezone.utc)

        # ── Detect "returning" tickers: absent from yesterday's bulk but
        # present in today's AND had completed history before.
        # Guards:
        #   1. Proven gap window: last_seen_in_bulk_date < bulk_date (actual
        #      trading-day gap; not just a single-day flicker).
        #   2. 7-day cooldown: full_history_downloaded_at must be > 7 days ago
        #      (or absent) — prevents unnecessary re-download churn.
        if bulk_close_set:
            _returning_cursor = await db.tracked_tickers.find(
                {
                    "ticker": {"$in": list(bulk_close_set)},
                    "has_latest_bulk_close": {"$ne": True},
                    "price_history_complete": True,
                },
                {"_id": 0, "ticker": 1,
                 "last_seen_in_bulk_date": 1,
                 "full_history_downloaded_at": 1},
            ).to_list(None)

            _cooldown_cutoff = now_ts - timedelta(days=_REDOWNLOAD_COOLDOWN_DAYS)
            for _rdoc in _returning_cursor:
                _rticker = _rdoc["ticker"]
                _last_seen = _rdoc.get("last_seen_in_bulk_date")
                _last_dl = _ensure_utc(_rdoc.get("full_history_downloaded_at"))

                # Guard 1: proven gap window — last_seen_in_bulk_date must
                # exist and be strictly before today's bulk_date.
                if bulk_date and _last_seen and _last_seen >= bulk_date:
                    _returning_no_gap_skipped += 1
                    continue

                # Guard 2: 7-day cooldown on full-history re-downloads.
                if _last_dl and _last_dl > _cooldown_cutoff:
                    _returning_cooldown_skipped += 1
                    continue

                returning_tickers.add(_rticker)

        # Reset ALL seeded tickers — both bulk-close and history flags
        await db.tracked_tickers.update_many(
            {"ticker": {"$in": seeded_tickers}},
            {"$set": {
                "has_latest_bulk_close": False,
                "has_price_data": False,
                "has_price_history": False,
                "updated_at": now_ts,
            }},
        )
        # Set True for tickers present in this bulk with close > 0
        if bulk_close_set:
            _bulk_set_fields: Dict[str, Any] = {
                "has_latest_bulk_close": True,
                "has_price_data": True,
                "has_price_history": True,  # bulk tickers have at least today's price
                "updated_at": now_ts,
            }
            if bulk_date:
                _bulk_set_fields["last_seen_in_bulk_date"] = bulk_date
            await db.tracked_tickers.update_many(
                {"ticker": {"$in": list(bulk_close_set)}},
                {"$set": _bulk_set_fields},
            )

        # ── Flag returning tickers for Phase C re-download ───────────────
        # Only tickers that passed both guards (gap window + cooldown) are
        # flagged.  Their historical data may be stale, so Phase C must
        # re-download the complete price history (one API call per ticker).
        if returning_tickers:
            await db.tracked_tickers.update_many(
                {"ticker": {"$in": list(returning_tickers)}},
                {"$set": {
                    "needs_price_redownload": True,
                    "price_history_complete": False,
                    "price_history_status": "pending_redownload_returning",
                    "updated_at": now_ts,
                }},
            )
            logger.info(
                "[sync_has_price_data_flags] %d tickers returning to bulk "
                "after absence → needs_price_redownload=True "
                "(Phase C will re-download full history) "
                "[cooldown_skipped=%d, no_gap_window_skipped=%d]",
                len(returning_tickers),
                _returning_cooldown_skipped,
                _returning_no_gap_skipped,
            )

        # No preservation: tickers NOT in today's bulk get has_price_data=False.
        # Visibility requires presence in the latest EODHD bulk with close > 0.

        # ── Auto-populate gap_free_exclusions for not-in-bulk tickers ────
        # A ticker absent from EODHD bulk is NOT a gap — it simply wasn't
        # in the feed (halted, delisted, no trade, provider omission).
        # Write exclusion entries so the gap-free metric ignores them and
        # the chart endpoint can show data_notices to customers.
        not_in_bulk = seeded_set - bulk_close_set
        if not_in_bulk and bulk_date:
            from pymongo import UpdateOne as _ExclUpdateOne
            _excl_ops = [
                _ExclUpdateOne(
                    {"ticker": t, "date": bulk_date},
                    {"$set": {
                        "ticker": t,
                        "date": bulk_date,
                        "reason": "not_in_bulk_data",
                        "bulk_found": False,
                        "updated_at": now_ts,
                    }},
                    upsert=True,
                )
                for t in not_in_bulk
            ]
            try:
                await db.gap_free_exclusions.bulk_write(_excl_ops, ordered=False)
                _excl_written = len(_excl_ops)
                logger.info(
                    "[sync_has_price_data_flags] wrote %d gap_free_exclusions "
                    "for tickers not in bulk (date=%s)",
                    _excl_written, bulk_date,
                )
            except Exception as _excl_err:
                logger.warning(
                    "[sync_has_price_data_flags] failed to write gap_free_exclusions: %s",
                    _excl_err,
                )

    with_bulk_close = len(bulk_close_set)
    summary: Dict[str, Any] = {
        "seeded_total": seeded_total,
        "with_latest_bulk_close": with_bulk_close,
        "without_any_price": max(seeded_total - with_bulk_close, 0),
        "matched_price_tickers_raw": matched_raw,
        "skipped_flag_reset": skip_bulk_flag_reset,
        # has_price_data = bulk tickers ONLY (no preservation)
        "with_price_data": with_bulk_close,
        "without_price_data": max(seeded_total - with_bulk_close, 0),
        "returning_tickers_flagged_for_redownload": len(returning_tickers),
        "returning_tickers_cooldown_skipped": _returning_cooldown_skipped,
        "returning_tickers_no_gap_window_skipped": _returning_no_gap_skipped,
        "gap_free_exclusions_written": _excl_written,
    }
    if include_exclusions:
        exclusions: List[Dict[str, Any]] = []
        for ticker in sorted(seeded_set - bulk_close_set):
            reason = (
                "Close/adjusted_close missing or zero"
                if ticker in any_price_set
                else "Ticker not present in bulk data"
            )
            exclusions.append({
                "ticker": ticker.replace(".US", ""),
                "name": name_by_ticker.get(ticker, "(unknown)"),
                "step": STEP2_REPORT_STEP,
                "reason": reason,
            })
        summary["exclusions"] = exclusions
    return summary


async def save_price_sync_exclusion_report(
    db,
    rows: List[Dict[str, Any]],
    now: datetime,
) -> Dict[str, Any]:
    """
    Save Step 2 exclusion rows to shared pipeline exclusion report collection.
    Rewrites only Step 2 rows for report_date, keeping other step rows intact.
    """
    report_date = now.astimezone(PRAGUE_TZ).strftime("%Y-%m-%d")
    run_id = f"price_sync_{now.strftime('%Y%m%d_%H%M%S')}"

    await db.pipeline_exclusion_report.delete_many({
        "report_date": report_date,
        "step": STEP2_REPORT_STEP,
    })

    if not rows:
        return {
            "exclusion_report_date": report_date,
            "exclusion_report_rows": 0,
            "exclusion_report_run_id": run_id,
        }

    docs = []
    for row in rows:
        docs.append({
            "ticker": row.get("ticker", "(unknown)"),
            "name": row.get("name"),
            "step": row.get("step", STEP2_REPORT_STEP),
            "reason": row.get("reason", "Unknown"),
            "report_date": report_date,
            "source_job": "price_sync",
            "run_id": run_id,
            "created_at": now,
        })

    await db.pipeline_exclusion_report.insert_many(docs, ordered=False)
    return {
        "exclusion_report_date": report_date,
        "exclusion_report_rows": len(docs),
        "exclusion_report_run_id": run_id,
    }


async def _append_step2_exclusions(
    db,
    run_id: Optional[str],
    report_date: Optional[str],
    rows: List[Dict[str, Any]],
) -> None:
    """
    Append rows to existing Step 2 exclusion report (same run_id).
    """
    if not run_id or not report_date or not rows:
        return
    docs = []
    now = datetime.now(timezone.utc)
    for row in rows:
        docs.append({
            "ticker": row.get("ticker", "(unknown)"),
            "name": row.get("name", "(unknown)"),
            "step": STEP2_REPORT_STEP,
            "reason": row.get("reason", "Unknown"),
            "report_date": report_date,
            "source_job": "price_sync",
            "run_id": run_id,
            "created_at": now,
        })
    if docs:
        await db.pipeline_exclusion_report.insert_many(docs, ordered=False)


STEP3_REPORT_STEP = "Step 3 - Fundamentals Sync"
_VISIBILITY_EXCLUSION_REASONS = {"DELISTED", "MISSING_SHARES", "MISSING_FINANCIAL_CURRENCY"}


async def save_step3_exclusion_report(db, now: datetime) -> Dict[str, Any]:
    """
    Write Step 3 exclusion rows: STEP3_QUERY tickers that are NOT up-to-date
    after the sync run.  One row per ticker, primary reason wins.

    Priority order (first matching reason wins per ticker):
      1. fundamentals_status == 'error'           → "Fundamentals sync error"
      2. fundamentals_status not in ('complete')
         AND not null/missing                      → "Fundamentals not synced"
      3. fundamentals_status is null/missing       → "Fundamentals not synced"
      4. needs_fundamentals_refresh == True        → "Stale fundamentals (refresh pending)"
      5. sector or industry missing/empty          → "Classification missing"

    "Up-to-date" means: fundamentals_status='complete'
                        AND needs_fundamentals_refresh != True
                        AND fundamentals_updated_at is not null
    """
    report_date = now.astimezone(PRAGUE_TZ).strftime("%Y-%m-%d")
    run_id = f"fundamentals_sync_{now.strftime('%Y%m%d_%H%M%S')}"

    docs_cursor = db.tracked_tickers.find(
        {
            **SEED_QUERY,
            "has_price_data": True,
            "$or": [
                # not complete
                {"fundamentals_status": {"$ne": "complete"}},
                # complete but refresh needed
                {"needs_fundamentals_refresh": True},
                # complete, no refresh, but updated_at missing
                {"fundamentals_updated_at": {"$in": [None, ""]}},
                {"fundamentals_updated_at": {"$exists": False}},
                # complete, no refresh, but classification missing
                {"sector": {"$in": [None, ""]}},
                {"industry": {"$in": [None, ""]}},
            ],
        },
        {
            "_id": 0, "ticker": 1, "name": 1,
            "fundamentals_status": 1, "needs_fundamentals_refresh": 1,
            "fundamentals_updated_at": 1, "sector": 1, "industry": 1,
        },
    )

    await db.pipeline_exclusion_report.delete_many(
        {"report_date": report_date, "step": STEP3_REPORT_STEP}
    )

    docs = []
    async for doc in docs_cursor:
        ticker = doc.get("ticker", "(unknown)")
        fstatus = doc.get("fundamentals_status")
        needs_refresh = doc.get("needs_fundamentals_refresh") is True
        updated_at = doc.get("fundamentals_updated_at")
        sector = (doc.get("sector") or "").strip()
        industry = (doc.get("industry") or "").strip()

        if fstatus == "error":
            reason = "Fundamentals sync error"
        elif fstatus != "complete":
            reason = "Fundamentals not synced"
        elif needs_refresh:
            reason = "Stale fundamentals (refresh pending)"
        elif not updated_at:
            reason = "Fundamentals not synced"
        elif not sector or not industry:
            reason = "Classification missing"
        else:
            # Passes all checks — should not appear here, skip defensively.
            continue

        docs.append({
            "ticker": ticker,
            "name": doc.get("name", ""),
            "step": STEP3_REPORT_STEP,
            "reason": reason,
            "report_date": report_date,
            "source_job": "fundamentals_sync",
            "run_id": run_id,
            "created_at": now,
        })

    if docs:
        await db.pipeline_exclusion_report.insert_many(docs, ordered=False)

    return {"step3_exclusion_rows": len(docs), "report_date": report_date, "exclusion_report_run_id": run_id}


async def save_step3_visibility_exclusion_report(db, now: datetime) -> Dict[str, Any]:
    """
    Write Step 3 visibility-gate exclusion rows for classified tickers that
    fail one of the 3 visibility gates added during Step 3 (DELISTED,
    MISSING_SHARES, MISSING_FINANCIAL_CURRENCY).

    Uses the CORRECT classified query matching universe_counts_service.py:
      seeded + has_price_data + fundamentals_status=="complete"

    Rows are written with step = "Step 3 - Fundamentals Sync" so they
    appear alongside other Step 3 exclusion rows in the CSV.

    Only the 3 visibility-specific reasons are written here — NOT_SEEDED,
    NO_PRICE_DATA, MISSING_SECTOR, MISSING_INDUSTRY are already handled
    by Steps 1–3 exclusion reports.
    """
    report_date = now.astimezone(PRAGUE_TZ).strftime("%Y-%m-%d")
    run_id = f"fundamentals_visibility_{now.strftime('%Y%m%d_%H%M%S')}"

    # Correct classified query matching universe_counts_service.py
    _classified_query = {
        "exchange": {"$in": ["NYSE", "NASDAQ"]},
        "asset_type": "Common Stock",
        "is_seeded": True,
        "has_price_data": True,
        "fundamentals_status": "complete",
    }

    _filtered_query = {**_classified_query, "is_visible": {"$ne": True}}

    classified_count = await db.tracked_tickers.count_documents(_classified_query)
    visible_count = await db.tracked_tickers.count_documents(
        {**_classified_query, "is_visible": True}
    )
    filtered_query_count = await db.tracked_tickers.count_documents(_filtered_query)

    _reason_labels = {
        "DELISTED":                   "Ticker is delisted",
        "MISSING_SHARES":             "Shares outstanding missing or zero",
        "MISSING_FINANCIAL_CURRENCY": "Financial currency missing",
    }

    docs_cursor = db.tracked_tickers.find(
        _filtered_query,
        {"_id": 0, "ticker": 1, "name": 1, "visibility_failed_reason": 1,
         "shares_outstanding": 1},
    )

    # Delete ONLY the visibility-gate rows for this date, not all Step 3 rows
    _visibility_reason_labels = list(_reason_labels.values())
    await db.pipeline_exclusion_report.delete_many(
        {
            "report_date": report_date,
            "step": STEP3_REPORT_STEP,
            "reason": {"$in": _visibility_reason_labels},
        }
    )

    docs = []
    async for doc in docs_cursor:
        raw_reason = doc.get("visibility_failed_reason") or "UNKNOWN"
        # Only write rows for the 3 visibility-specific reasons.
        # Other reasons (NOT_SEEDED, NO_PRICE_DATA, MISSING_SECTOR, MISSING_INDUSTRY)
        # are already covered by Steps 1–3 exclusion reports.
        if raw_reason not in _VISIBILITY_EXCLUSION_REASONS:
            continue
        row = {
            "ticker":      doc.get("ticker", "(unknown)"),
            "name":        doc.get("name", ""),
            "step":        STEP3_REPORT_STEP,
            "reason":      _reason_labels.get(raw_reason, raw_reason),
            "report_date": report_date,
            "source_job":  "fundamentals_sync",
            "run_id":      run_id,
            "created_at":  now,
        }
        if raw_reason == "MISSING_SHARES":
            row["_debug"] = {
                "shares_outstanding_value": doc.get("shares_outstanding"),
                "field_path": "tracked_tickers.shares_outstanding",
            }
        docs.append(row)

    if docs:
        await db.pipeline_exclusion_report.insert_many(docs, ordered=False)

    rows_written = len(docs)
    return {
        "visibility_exclusion_rows": rows_written,
        "exclusion_report_run_id": run_id,
        "report_date": report_date,
        "_debug": {
            "classified_count": classified_count,
            "visible_count":    visible_count,
            "rows_written":     rows_written,
            "filtered_query_count": filtered_query_count,
        },
    }


async def _deduplicate_pending_events(db, now: datetime) -> Dict[str, int]:
    """
    Mark duplicate pending fundamentals_events as 'deduped', keeping only the
    most-recent event per (ticker, event_type) pair.

    Strategy:
      1. Snapshot pending_before_dedup via count_documents BEFORE touching anything.
      2. Stream aggregate results (grouped by (ticker, event_type)); for each group
         collect the dupe _ids (all except the newest).
      3. Flush to update_many in chunks of CHUNK_SIZE to avoid building one
         unbounded Python list — memory-safe regardless of queue size.

    No documents are deleted — 'deduped' is a permanent audit status.
    Returns: {"pending_before_dedup": N, "deduped_event_count": M}
    """
    CHUNK_SIZE = 5_000

    pending_before_dedup = await db.fundamentals_events.count_documents({"status": "pending"})

    pipeline = [
        {"$match": {"status": "pending"}},
        {"$sort": {"created_at": -1}},
        {
            "$group": {
                "_id": {"ticker": "$ticker", "event_type": "$event_type"},
                "keep_id": {"$first": "$_id"},
                "all_ids": {"$push": "$_id"},
            }
        },
        {
            "$project": {
                "dupe_ids": {
                    "$filter": {
                        "input": "$all_ids",
                        "as": "eid",
                        "cond": {"$ne": ["$$eid", "$keep_id"]},
                    }
                }
            }
        },
        {"$match": {"dupe_ids.0": {"$exists": True}}},
    ]

    chunk: list = []
    deduped_event_count = 0

    async for doc in db.fundamentals_events.aggregate(pipeline):
        chunk.extend(doc["dupe_ids"])
        if len(chunk) >= CHUNK_SIZE:
            res = await db.fundamentals_events.update_many(
                {"_id": {"$in": chunk}},
                {"$set": {"status": "deduped", "deduped_at": now}},
            )
            deduped_event_count += res.modified_count
            chunk = []

    if chunk:
        res = await db.fundamentals_events.update_many(
            {"_id": {"$in": chunk}},
            {"$set": {"status": "deduped", "deduped_at": now}},
        )
        deduped_event_count += res.modified_count

    if deduped_event_count:
        logger.info(f"_deduplicate_pending_events: marked {deduped_event_count} duplicate events as deduped")
    return {"pending_before_dedup": pending_before_dedup, "deduped_event_count": deduped_event_count}


async def _skip_already_complete_tickers(
    db, ticker_full_list: List[str], now: datetime
) -> Dict[str, Any]:
    """
    For tickers that are already fundamentals_status='complete' and do NOT have
    needs_fundamentals_refresh=True, mark their pending events as 'skipped'.

    Returns:
        {
            "skipped_ticker_count": int,   # distinct tickers skipped
            "skipped_event_count": int,    # individual events updated
            "skipped_tickers": [...]       # list (for queue_stats audit)
        }
    """
    if not ticker_full_list:
        return {"skipped_ticker_count": 0, "skipped_event_count": 0, "skipped_tickers": []}

    skippable: List[str] = await db.tracked_tickers.distinct(
        "ticker",
        {
            "ticker": {"$in": ticker_full_list},
            "fundamentals_status": "complete",
            "needs_fundamentals_refresh": {"$ne": True},
        },
    )

    if not skippable:
        return {"skipped_ticker_count": 0, "skipped_event_count": 0, "skipped_tickers": []}

    result = await db.fundamentals_events.update_many(
        {"ticker": {"$in": skippable}, "status": "pending"},
        {"$set": {"status": "skipped", "skipped_reason": "already_complete", "skipped_at": now}},
    )
    skipped_events = result.modified_count
    logger.info(
        f"_skip_already_complete_tickers: skipped {len(skippable)} tickers "
        f"({skipped_events} events) — fundamentals_status=complete, no refresh flag"
    )
    return {
        "skipped_ticker_count": len(skippable),
        "skipped_event_count": skipped_events,
        "skipped_tickers": skippable,
    }


async def _reconcile_logo_completeness(db) -> Dict[str, Any]:
    """Reset stale-complete tickers whose logo_status is unresolved.

    After the logo requirement was added to the completeness gate, tickers
    that were synced *before* logos existed may still carry
    ``fundamentals_status="complete"`` despite having no ``logo_status`` (or
    ``logo_status="error"``).  The skip-gate would then skip them forever.

    This function:
      1. Finds tracked_tickers with fundamentals_status="complete" and
         needs_fundamentals_refresh != True.
      2. Cross-checks company_fundamentals_cache: any ticker whose cache doc
         has no ``logo_status`` or ``logo_status`` not in ("present","absent")
         is considered stale-complete.
      3. Resets ``needs_fundamentals_refresh=True`` and
         ``fundamentals_status="partial"`` so Step 3 reprocesses them.

    Returns ``{"reset_count": int, "reset_tickers": [...]}``.
    """
    # 1. Find "complete" tickers that are not already flagged for refresh
    complete_tickers: List[str] = await db.tracked_tickers.distinct(
        "ticker",
        {
            **SEED_QUERY,
            "has_price_data": True,
            "fundamentals_status": "complete",
            "needs_fundamentals_refresh": {"$ne": True},
        },
    )
    if not complete_tickers:
        return {"reset_count": 0, "reset_tickers": []}

    # 2. Check which of those tickers have logo_status *truly* resolved in the
    #    cache.  Resolution requires:
    #      a) logo_status in ("present", "absent") AND logo_fetched_at is set, AND
    #      b) for "absent": logo_fetched_at >= LOGO_CDN_FIX_CUTOFF (stale results
    #         from the old broken CDN base are NOT considered resolved).
    resolved_tickers: List[str] = await db.company_fundamentals_cache.distinct(
        "ticker",
        {
            "ticker": {"$in": complete_tickers},
            "logo_fetched_at": {"$exists": True, "$ne": None},
            "$or": [
                {"logo_status": "present"},
                {"logo_status": "absent", "logo_fetched_at": {"$gte": LOGO_CDN_FIX_CUTOFF}},
            ],
        },
    )
    resolved_set = set(resolved_tickers)
    stale_tickers = [t for t in complete_tickers if t not in resolved_set]

    if not stale_tickers:
        return {"reset_count": 0, "reset_tickers": []}

    # 3. Reset flags so Step 3 reprocesses these tickers (downloads logos)
    now = datetime.now(timezone.utc)
    await db.tracked_tickers.update_many(
        {"ticker": {"$in": stale_tickers}},
        {"$set": {
            "fundamentals_status": "partial",
            "fundamentals_complete": False,
            "needs_fundamentals_refresh": True,
            "updated_at": now,
        }},
    )
    logger.info(
        f"_reconcile_logo_completeness: reset {len(stale_tickers)} tickers "
        "to partial/needs_refresh (logo_status not resolved in cache)"
    )
    return {"reset_count": len(stale_tickers), "reset_tickers": stale_tickers}


async def _build_logo_backfill_worklist(
    db,
    exclude_tickers: Optional[set] = None,
    limit: int = LOGO_BACKFILL_VISIBLE_LIMIT,
) -> List[dict]:
    """Return ``company_fundamentals_cache`` docs needing logo (re-)download.

    Criteria (any match → included):
      • ``logo_status`` field missing       — never attempted
      • ``logo_status = "error"``            — transient failure
      • ``logo_status = "absent"``           — no logo found previously

    Scoped to **visible** tickers (``status="active"``, ``is_visible=True``)
    because only those appear in the UI.
    Tickers in *exclude_tickers* (already processed in Phase A main) are skipped.
    At most *limit* tickers are returned per run.
    """
    visible_query = {"status": "active", "is_visible": True}
    eligible: List[str] = await db.tracked_tickers.distinct("ticker", visible_query)
    if not eligible:
        logger.info("_build_logo_backfill_worklist: eligible=0 (no visible active tickers)")
        return []

    query: dict = {
        "ticker": {"$in": eligible},
        "$or": [
            {"logo_status": {"$exists": False}},
            {"logo_status": "error"},
            {"logo_status": "absent"},
        ],
    }

    docs = await db.company_fundamentals_cache.find(
        query, {"ticker": 1, "logo_url": 1, "logo_status": 1, "_id": 0}
    ).to_list(None)

    if exclude_tickers:
        docs = [d for d in docs if d.get("ticker") not in exclude_tickers]

    if limit and len(docs) > limit:
        docs = docs[:limit]

    logger.info(
        "_build_logo_backfill_worklist: visible_eligible=%d matched=%d exclude=%d limit=%d",
        len(eligible), len(docs), len(exclude_tickers) if exclude_tickers else 0, limit,
    )
    return docs


async def purge_orphaned_fundamentals_events(db) -> Dict[str, Any]:
    """
    Delete fundamentals_events whose ticker is not in the canonical Step 3
    universe (STEP3_QUERY: NYSE/NASDAQ Common Stock with price data).

    Tickers in tracked_tickers are stored with the .US suffix (e.g. "AAPL.US"),
    matching the format used in fundamentals_events.ticker.

    Safety: distinct() returns at most ~6-7k items for our universe — well under
    MongoDB's 16 MB document limit.  If the eligible list ever exceeds 10,000
    we skip the purge and log a warning rather than building a dangerously large
    $nin list.
    """
    MAX_SAFE_ELIGIBLE = 10_000

    eligible: list = await db.tracked_tickers.distinct("ticker", STEP3_QUERY)

    if not eligible:
        logger.warning("purge_orphaned_fundamentals_events: eligible list is empty — skipping purge to avoid deleting everything")
        return {"skipped": True, "reason": "empty_eligible_list"}

    if len(eligible) > MAX_SAFE_ELIGIBLE:
        logger.warning(
            f"purge_orphaned_fundamentals_events: eligible list too large ({len(eligible)}) "
            f"— skipping purge (threshold={MAX_SAFE_ELIGIBLE})"
        )
        return {"skipped": True, "reason": "eligible_list_too_large", "count": len(eligible)}

    result = await db.fundamentals_events.delete_many(
        {"ticker": {"$nin": eligible}}
    )
    deleted = result.deleted_count
    if deleted:
        logger.info(f"purge_orphaned_fundamentals_events: deleted {deleted} orphaned events")
    return {"deleted": deleted, "eligible_count": len(eligible)}


async def _finalize_zombie_fundamentals_runs(db, now: datetime) -> int:
    """Finalize stale fundamentals_sync runs so they do not stay running forever."""
    stale_before = now - timedelta(seconds=FUNDAMENTALS_SYNC_ZOMBIE_TIMEOUT_SECONDS)
    running_zombie_result = await db.ops_job_runs.update_many(
        {
            "job_name": "fundamentals_sync",
            "status": "running",
            "$or": [
                {"heartbeat_at": {"$exists": False}},
                {"heartbeat_at": None},
                {"heartbeat_at": {"$lt": stale_before}},
            ],
        },
        {"$set": {
            "status": "cancelled",
            "cancelled_at": now,
            "finished_at": now,
            "finished_at_prague": _to_prague_iso(now),
            "log_timezone": "Europe/Prague",
            "details.zombie_finalized": True,
            "details.zombie_reason": "stale_heartbeat_or_missing",
        }},
    )
    cancel_stale_before = now - timedelta(seconds=CANCEL_REQUESTED_STUCK_SECONDS)
    cancel_zombie_result = await db.ops_job_runs.update_many(
        {
            "job_name": "fundamentals_sync",
            "status": "cancel_requested",
            "$or": [
                {"heartbeat_at": {"$exists": False}},
                {"heartbeat_at": None},
                {"heartbeat_at": {"$lt": cancel_stale_before}},
            ],
        },
        {"$set": {
            "status": "cancelled",
            "cancelled_at": now,
            "finished_at": now,
            "finished_at_prague": _to_prague_iso(now),
            "log_timezone": "Europe/Prague",
            "details.zombie_finalized": True,
            "details.zombie_reason": "cancel_requested_stale_heartbeat",
        }},
    )
    if running_zombie_result.modified_count:
        logger.warning(
            f"fundamentals_sync: finalized {running_zombie_result.modified_count} zombie run(s) "
            f"(heartbeat cutoff={stale_before.isoformat()})"
        )
    if cancel_zombie_result.modified_count:
        logger.warning(
            f"fundamentals_sync: finalized {cancel_zombie_result.modified_count} cancel_requested run(s) "
            f"(heartbeat cutoff={cancel_stale_before.isoformat()})"
        )

    # ── Fix stale telemetry: any phase still "running" is incorrect for a
    #    zombie-finalized run (process died).  Patch them to "error" so the
    #    dashboard does not show a ghost "running" phase on a cancelled run.
    total_zombified = running_zombie_result.modified_count + cancel_zombie_result.modified_count
    if total_zombified:
        async for doc in db.ops_job_runs.find({
            "job_name": "fundamentals_sync",
            "details.zombie_finalized": True,
            "status": "cancelled",
            "$or": [
                {"details.step3_telemetry.phases.A.status": "running"},
                {"details.step3_telemetry.phases.B.status": "running"},
                {"details.step3_telemetry.phases.C.status": "running"},
            ],
        }):
            fix_fields: Dict[str, Any] = {}
            phases = (doc.get("details") or {}).get("step3_telemetry", {}).get("phases", {})
            for pk in ("A", "B", "C"):
                if (phases.get(pk) or {}).get("status") == "running":
                    fix_fields[f"details.step3_telemetry.phases.{pk}.status"] = "error"
                    fix_fields[f"details.step3_telemetry.phases.{pk}.message"] = "Process terminated (zombie finalized)"
            if fix_fields:
                await db.ops_job_runs.update_one({"_id": doc["_id"]}, {"$set": fix_fields})
                logger.info(
                    f"fundamentals_sync: patched zombie telemetry for run {doc['_id']} "
                    f"(phases fixed: {list(fix_fields.keys())})"
                )

    return total_zombified


async def _finalize_stuck_price_sync_runs(db, now: datetime) -> int:
    """
    Finalize price_sync runs that are still marked running but no active phase exists.
    These stale docs should not keep the admin UI in "running" state forever.
    """
    running_zombie_result = await db.ops_job_runs.update_many(
        {
            "job_name": "price_sync",
            "status": "running",
            "$or": [
                {"phase": {"$exists": False}},
                {"phase": None},
                {"phase": {"$nin": list(PRICE_SYNC_ACTIVE_PHASES)}},
            ],
        },
        {"$set": {
            "status": "cancelled",
            "cancelled_at": now,
            "finished_at": now,
            "finished_at_prague": _to_prague_iso(now),
            "log_timezone": "Europe/Prague",
            "details.zombie_finalized": True,
            "details.zombie_reason": "stale_running_no_active_phase",
        }},
    )
    cancel_stale_before = now - timedelta(seconds=CANCEL_REQUESTED_STUCK_SECONDS)
    cancel_zombie_result = await db.ops_job_runs.update_many(
        {
            "job_name": "price_sync",
            "status": "cancel_requested",
            "$or": [
                {"heartbeat_at": {"$exists": False}},
                {"heartbeat_at": None},
                {"heartbeat_at": {"$lt": cancel_stale_before}},
            ],
        },
        {"$set": {
            "status": "cancelled",
            "cancelled_at": now,
            "finished_at": now,
            "finished_at_prague": _to_prague_iso(now),
            "log_timezone": "Europe/Prague",
            "details.zombie_finalized": True,
            "details.zombie_reason": "cancel_requested_stale_heartbeat",
        }},
    )
    if running_zombie_result.modified_count:
        logger.warning(
            f"price_sync: finalized {running_zombie_result.modified_count} stale running run(s) "
            "(missing or non-active phase)"
        )
    if cancel_zombie_result.modified_count:
        logger.warning(
            f"price_sync: finalized {cancel_zombie_result.modified_count} cancel_requested run(s) "
            f"(heartbeat cutoff={cancel_stale_before.isoformat()})"
        )
    # Watchdog: finalize runs stuck in "running" for > 30 minutes regardless
    # of phase.  This catches runs where the process died mid-phase (e.g.
    # sanity failure + exception left the doc at status="running" with an
    # active phase like "2.1_bulk_catchup").
    _stuck_cutoff = now - timedelta(seconds=PRICE_SYNC_STUCK_TIMEOUT_SECONDS)
    stuck_timeout_result = await db.ops_job_runs.update_many(
        {
            "job_name": "price_sync",
            "status": "running",
            "started_at": {"$lt": _stuck_cutoff},
        },
        {"$set": {
            "status": "error",
            "finished_at": now,
            "finished_at_prague": _to_prague_iso(now),
            "log_timezone": "Europe/Prague",
            "error": f"watchdog: running > {PRICE_SYNC_STUCK_TIMEOUT_SECONDS}s",
            "error_message": f"watchdog: running > {PRICE_SYNC_STUCK_TIMEOUT_SECONDS}s",
            "details.zombie_finalized": True,
            "details.zombie_reason": "stuck_timeout_watchdog",
        }},
    )
    if stuck_timeout_result.modified_count:
        logger.warning(
            f"price_sync: watchdog finalized {stuck_timeout_result.modified_count} run(s) "
            f"stuck running > {PRICE_SYNC_STUCK_TIMEOUT_SECONDS}s "
            f"(cutoff={_stuck_cutoff.isoformat()})"
        )
    return (
        running_zombie_result.modified_count
        + cancel_zombie_result.modified_count
        + stuck_timeout_result.modified_count
    )


async def _finalize_orphaned_chain_runs(db, now: datetime) -> int:
    """
    Finalize pipeline_chain_runs stuck in 'running' whose child jobs are all
    terminal (no ops_job_runs with status running/cancel_requested for that
    chain).  This closes the gap where zombie job finalization updates
    ops_job_runs but never propagates to the parent chain document.
    """
    stale_before = now - timedelta(seconds=FUNDAMENTALS_SYNC_ZOMBIE_TIMEOUT_SECONDS)
    finalized = 0
    async for chain_doc in db.pipeline_chain_runs.find({
        "status": "running",
        "started_at": {"$lt": stale_before},
    }):
        chain_id = chain_doc.get("chain_run_id")
        if not chain_id:
            continue
        active_job = await db.ops_job_runs.find_one({
            "details.chain_run_id": chain_id,
            "status": {"$in": ["running", "cancel_requested"]},
        })
        if active_job:
            continue
        result = await db.pipeline_chain_runs.update_one(
            {"chain_run_id": chain_id, "status": "running"},
            {"$set": {
                "status": "cancelled",
                "finished_at": now,
                "finished_at_prague": _to_prague_iso(now),
                "error": "orphaned_chain_no_active_jobs",
            }},
        )
        if result.modified_count:
            finalized += 1
            logger.warning(
                f"pipeline_chain_runs: finalized orphaned chain {chain_id} "
                f"(no active jobs, started_at < {stale_before.isoformat()})"
            )
    return finalized


async def finalize_stuck_admin_job_runs(
    db,
    *,
    now: Optional[datetime] = None,
    job_names: Optional[List[str]] = None,
) -> Dict[str, int]:
    """
    Finalize stale running docs for admin pipeline jobs so UI status stays truthful.
    Also finalizes orphaned pipeline_chain_runs whose child jobs are all terminal.
    """
    if now is None:
        now = datetime.now(timezone.utc)
    targets = set(job_names or ["price_sync", "fundamentals_sync"])
    finalized = {"price_sync": 0, "fundamentals_sync": 0}
    if "price_sync" in targets:
        finalized["price_sync"] = await _finalize_stuck_price_sync_runs(db, now)
    if "fundamentals_sync" in targets:
        finalized["fundamentals_sync"] = await _finalize_zombie_fundamentals_runs(db, now)
    finalized["orphaned_chains"] = await _finalize_orphaned_chain_runs(db, now)
    return finalized


async def _ensure_ops_locks_ttl_index(db) -> None:
    """Ensure TTL index for ops_locks.expires_at exists."""
    global _OPS_LOCKS_TTL_INDEX_DONE
    global _OPS_LOCKS_TTL_INDEX_LOCK
    if _OPS_LOCKS_TTL_INDEX_LOCK is None:
        _OPS_LOCKS_TTL_INDEX_LOCK = asyncio.Lock()
    async with _OPS_LOCKS_TTL_INDEX_LOCK:
        if _OPS_LOCKS_TTL_INDEX_DONE:
            return
        await db.ops_locks.create_index(
            [("expires_at", 1)],
            name="ops_locks_expires_at_ttl",
            expireAfterSeconds=0,
        )
        _OPS_LOCKS_TTL_INDEX_DONE = True


async def _acquire_fundamentals_sync_lock(db, owner_run_id: str, now: datetime) -> bool:
    """Acquire distributed single-flight lock for fundamentals_sync."""
    await _ensure_ops_locks_ttl_index(db)
    lease_expires_at = now + timedelta(seconds=FUNDAMENTALS_SYNC_LOCK_LEASE_SECONDS)

    reusable = await db.ops_locks.update_one(
        {
            "_id": FUNDAMENTALS_SYNC_LOCK_ID,
            "$or": [
                {"owner_run_id": owner_run_id},
                {"expires_at": {"$lte": now}},
            ],
        },
        {"$set": {
            "owner_run_id": owner_run_id,
            "acquired_at": now,
            "heartbeat_at": now,
            "expires_at": lease_expires_at,
        }},
    )
    if reusable.matched_count:
        return True

    try:
        await db.ops_locks.insert_one({
            "_id": FUNDAMENTALS_SYNC_LOCK_ID,
            "owner_run_id": owner_run_id,
            "acquired_at": now,
            "heartbeat_at": now,
            "expires_at": lease_expires_at,
        })
        return True
    except DuplicateKeyError:
        return False


async def _heartbeat_fundamentals_sync_lock(db, owner_run_id: str, now: datetime) -> None:
    """Refresh lock lease and run heartbeat while fundamentals_sync is running."""
    await db.ops_locks.update_one(
        {"_id": FUNDAMENTALS_SYNC_LOCK_ID, "owner_run_id": owner_run_id},
        {"$set": {
            "heartbeat_at": now,
            "expires_at": now + timedelta(seconds=FUNDAMENTALS_SYNC_LOCK_LEASE_SECONDS),
        }},
    )


async def _release_fundamentals_sync_lock(db, owner_run_id: str) -> None:
    await db.ops_locks.delete_one({
        "_id": FUNDAMENTALS_SYNC_LOCK_ID,
        "owner_run_id": owner_run_id,
    })


# ── Price sync single-flight lock (mirrors fundamentals_sync lock) ──────────

async def _acquire_price_sync_lock(db, owner_run_id: str, now: datetime) -> bool:
    """Acquire distributed single-flight lock for price_sync."""
    await _ensure_ops_locks_ttl_index(db)
    lease_expires_at = now + timedelta(seconds=PRICE_SYNC_LOCK_LEASE_SECONDS)

    reusable = await db.ops_locks.update_one(
        {
            "_id": PRICE_SYNC_LOCK_ID,
            "$or": [
                {"owner_run_id": owner_run_id},
                {"expires_at": {"$lte": now}},
            ],
        },
        {"$set": {
            "owner_run_id": owner_run_id,
            "acquired_at": now,
            "heartbeat_at": now,
            "expires_at": lease_expires_at,
        }},
    )
    if reusable.matched_count:
        return True

    try:
        await db.ops_locks.insert_one({
            "_id": PRICE_SYNC_LOCK_ID,
            "owner_run_id": owner_run_id,
            "acquired_at": now,
            "heartbeat_at": now,
            "expires_at": lease_expires_at,
        })
        return True
    except DuplicateKeyError:
        return False


async def _heartbeat_price_sync_lock(db, owner_run_id: str, now: datetime) -> None:
    """Refresh lock lease while price_sync is running."""
    await db.ops_locks.update_one(
        {"_id": PRICE_SYNC_LOCK_ID, "owner_run_id": owner_run_id},
        {"$set": {
            "heartbeat_at": now,
            "expires_at": now + timedelta(seconds=PRICE_SYNC_LOCK_LEASE_SECONDS),
        }},
    )


async def _release_price_sync_lock(db, owner_run_id: str) -> None:
    await db.ops_locks.delete_one({
        "_id": PRICE_SYNC_LOCK_ID,
        "owner_run_id": owner_run_id,
    })


async def run_fundamentals_changes_sync(db, batch_size: int = 50, ignore_kill_switch: bool = False, parent_run_id: Optional[str] = None, cancel_check: Optional[Callable[[], Awaitable[bool]]] = None, chain_run_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Run fundamentals sync for tickers with changes/events.
    Processes tickers in parallel (up to 20 concurrent) for speed.
    With batch_size=2000: processes all 6,435 tickers in ~15-20 minutes.

    chain_run_id: chain identifier shared across all steps in the pipeline run.
    Stored in the sentinel doc's details so the chain orchestrator can find and
    finalize this doc on cancellation/failure.
    """
    from batch_jobs_service import sync_single_ticker_fundamentals
    from visibility_rules import recompute_visibility_all

    started_at = datetime.now(timezone.utc)
    job_name = "fundamentals_sync"

    logger.info(f"Starting {job_name}")
    await _finalize_zombie_fundamentals_runs(db, started_at)

    def _build_phase(name: str) -> Dict[str, Any]:
        return {
            "name": name,
            "status": "idle",
            "processed": 0,
            "total": None,
            "pct": None,
            "message": None,
        }

    step3_telemetry: Dict[str, Any] = {
        "active_phase": "A",
        "updated_at_prague": _to_prague_iso(started_at),
        "phases": {
            "A": _build_phase("Fundamentals"),
            "B": _build_phase("Visibility"),
            "C": _build_phase("PriceHistory"),
        },
        "visible_present_logo_example_found": False,
        "visible_present_logo_example_ticker": None,
    }

    _telemetry_last_write_monotonic = 0.0
    _telemetry_last_processed = 0

    # Running sentinel so frontend poll detects job start immediately
    _running_doc_id = (await db.ops_job_runs.insert_one({
        "job_name": job_name,
        "status": "running",
        "started_at": started_at,
        "heartbeat_at": started_at,
        "source": "scheduler",
        "progress": "Queuing tickers for fundamentals sync…",
        "details": {
            "parent_run_id": parent_run_id,
            "chain_run_id": chain_run_id,
            "step3_telemetry": step3_telemetry,
        },
    })).inserted_id
    _lock_owner_run_id = str(_running_doc_id)
    _lock_acquired = await _acquire_fundamentals_sync_lock(db, _lock_owner_run_id, started_at)
    if not _lock_acquired:
        finished_at = datetime.now(timezone.utc)
        await db.ops_job_runs.update_one(
            {"_id": _running_doc_id},
            {"$set": {
                "status": "skipped",
                "finished_at": finished_at,
                "started_at_prague": _to_prague_iso(started_at),
                "finished_at_prague": _to_prague_iso(finished_at),
                "log_timezone": "Europe/Prague",
                "error": "single_flight_lock_held",
                "progress": "Skipped: fundamentals_sync already running",
                "details.lock_id": FUNDAMENTALS_SYNC_LOCK_ID,
                "details.owner_run_id": _lock_owner_run_id,
            }},
        )
        logger.warning(f"{job_name}: single-flight lock held, skipping run")
        return {
            "job_name": job_name,
            "status": "skipped",
            "reason": "single_flight_lock_held",
            "started_at": started_at.isoformat(),
        }

    _heartbeat_stop = asyncio.Event()
    _heartbeat_task: Optional[asyncio.Task] = None
    _last_ticker_started: Optional[str] = None
    _last_ticker_finished: Optional[str] = None
    _heartbeat_last_error: Optional[str] = None

    def _details_updated_fields(now_utc: datetime) -> Dict[str, Any]:
        return {
            "details.updated_at": now_utc,
            "details.updated_at_prague": _to_prague_iso(now_utc),
        }

    async def _release_single_flight_resources() -> None:
        nonlocal _heartbeat_task, _lock_acquired
        _heartbeat_stop.set()
        if _heartbeat_task is not None:
            _heartbeat_task.cancel()
            await asyncio.gather(_heartbeat_task, return_exceptions=True)
            _heartbeat_task = None
        if _lock_acquired:
            await _release_fundamentals_sync_lock(db, _lock_owner_run_id)
            _lock_acquired = False

    async def _heartbeat_worker() -> None:
        nonlocal _heartbeat_last_error
        while not _heartbeat_stop.is_set():
            await asyncio.sleep(FUNDAMENTALS_SYNC_HEARTBEAT_SECONDS)
            heartbeat_now = datetime.now(timezone.utc)
            try:
                heartbeat_result = await asyncio.wait_for(
                    db.ops_job_runs.update_one(
                        {"_id": _running_doc_id, "status": "running"},
                        {"$set": {
                            "heartbeat_at": heartbeat_now,
                            "details.last_ticker_started": _last_ticker_started,
                            "details.last_ticker_finished": _last_ticker_finished,
                            "details.heartbeat_last_error": _heartbeat_last_error,
                            **_details_updated_fields(heartbeat_now),
                        }},
                    ),
                    timeout=10,
                )
                if heartbeat_result.modified_count == 0:
                    # Exit if the run is no longer active (externally finalized/status changed).
                    logger.info(f"{job_name}: heartbeat worker stopped (run no longer active)")
                    break
                _heartbeat_last_error = None  # clear on success
            except Exception as hb_exc:
                _heartbeat_last_error = f"{type(hb_exc).__name__}: {hb_exc}"
                logger.warning(
                    f"{job_name}: heartbeat write failed, will retry next cycle: {hb_exc}"
                )
                continue
            try:
                await asyncio.wait_for(
                    _heartbeat_fundamentals_sync_lock(db, _lock_owner_run_id, heartbeat_now),
                    timeout=10,
                )
            except Exception as lock_exc:
                logger.warning(f"{job_name}: heartbeat lock refresh failed: {lock_exc}")

    _heartbeat_task = asyncio.create_task(_heartbeat_worker())

    async def _is_cancelled() -> bool:
        """Check optional chain cancel callback and ops_job_runs cancel_requested flag."""
        if cancel_check and await cancel_check():
            return True
        return await is_cancel_requested(db, _running_doc_id)

    async def _finalize_cancelled(
        *,
        stop_reason: str,
        progress_msg: str = "Cancelled by admin",
        processed: int = 0,
        total: int = 0,
        extra_details: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        _heartbeat_stop.set()
        cancelled_at = datetime.now(timezone.utc)
        pct = 0
        if total > 0:
            pct = min(100, round(processed / total * 100))
        details_payload: Dict[str, Any] = {
            "parent_run_id": parent_run_id,
            "chain_run_id": chain_run_id,
            "step3_telemetry": step3_telemetry,
            "stop_reason": stop_reason,
        }
        if extra_details:
            details_payload.update(extra_details)
        await db.ops_job_runs.update_one(
            {"_id": _running_doc_id},
            {"$set": {
                "status": "cancelled",
                "finished_at": cancelled_at,
                "finished_at_prague": _to_prague_iso(cancelled_at),
                "log_timezone": "Europe/Prague",
                "progress": progress_msg,
                "progress_processed": processed,
                "progress_total": total,
                "progress_pct": pct,
                "heartbeat_at": cancelled_at,
                "cancelled_at": cancelled_at,
                "details": details_payload,
                **_details_updated_fields(cancelled_at),
            }},
        )
        await _release_single_flight_resources()
        return {
            "job_name": job_name,
            "status": "cancelled",
            "started_at": started_at.isoformat(),
            "finished_at": cancelled_at.isoformat(),
        }

    async def _write_step3_telemetry(*, force: bool = False, throttle_processed: Optional[int] = None) -> None:
        nonlocal _telemetry_last_write_monotonic, _telemetry_last_processed
        now_monotonic = time.monotonic()
        if _telemetry_last_write_monotonic > 0 and (now_monotonic - _telemetry_last_write_monotonic) < 1.0:
            return
        if not force:
            processed_now = throttle_processed if throttle_processed is not None else 0
            if (processed_now - _telemetry_last_processed) < 10:
                return
            _telemetry_last_processed = processed_now
        now_utc = datetime.now(timezone.utc)
        step3_telemetry["updated_at_prague"] = _to_prague_iso(now_utc)
        active_phase = step3_telemetry.get("active_phase") or "A"
        active = (step3_telemetry.get("phases") or {}).get(active_phase, {})
        progress_msg = active.get("message")
        progress_processed = active.get("processed", 0)
        progress_total = active.get("total")
        progress_pct = active.get("pct")
        try:
            await asyncio.wait_for(
                db.ops_job_runs.update_one(
                    {"_id": _running_doc_id},
                    {"$set": {
                        "details.step3_telemetry": step3_telemetry,
                        "details.last_ticker_started": _last_ticker_started,
                        "details.last_ticker_finished": _last_ticker_finished,
                        "progress": progress_msg,
                        "progress_processed": progress_processed,
                        "progress_total": progress_total,
                        "progress_pct": progress_pct,
                        "heartbeat_at": now_utc,
                        **_details_updated_fields(now_utc),
                    }},
                ),
                timeout=30,
            )
        except Exception as telem_exc:
            logger.warning(f"{job_name}: telemetry write failed (skipping): {telem_exc}")
            return
        _telemetry_last_write_monotonic = now_monotonic
        _telemetry_last_processed = int(active.get("processed", 0))

    def _phase_update(
        phase_key: str,
        *,
        status: Optional[str] = None,
        processed: Optional[int] = None,
        total: Optional[int] = None,
        message: Optional[str] = None,
        activate: bool = False,
    ) -> None:
        phase = step3_telemetry["phases"][phase_key]
        if activate:
            step3_telemetry["active_phase"] = phase_key
        if status is not None:
            phase["status"] = status
        if processed is not None:
            phase["processed"] = int(processed)
        if total is not None:
            phase["total"] = int(total)
        if message is not None:
            phase["message"] = message
        if phase.get("total") is None or phase.get("total", 0) <= 0:
            phase["pct"] = None
        else:
            phase["pct"] = round((phase.get("processed", 0) / phase["total"]) * 100, 2)

    async def _progress(msg: str) -> None:
        now_utc = datetime.now(timezone.utc)
        await db.ops_job_runs.update_one(
            {"_id": _running_doc_id},
            {"$set": {
                "progress": msg,
                "heartbeat_at": now_utc,
                **_details_updated_fields(now_utc),
            }},
        )

    # Initialized up front so the exception handler can safely reference them even
    # if a failure occurs before queue construction. done_count is incremented in
    # the asyncio.as_completed loop when each ticker finishes processing.
    done_count = 0
    # Populated from tracked_tickers.needs_fundamentals_refresh before processing
    # (fundamentals_events remains audit-only for Step 3 selection).
    tickers_to_sync: List[str] = []

    result: Dict[str, Any] = {}

    try:
        # Check kill switch (manual endpoints can explicitly bypass)
        if (not ignore_kill_switch) and (not await get_scheduler_enabled(db)):
            logger.warning(f"{job_name} skipped: kill switch engaged")
            await db.ops_job_runs.update_one(
                {"_id": _running_doc_id}, {"$set": {"status": "skipped"}}
            )
            return {
                "job_name": job_name,
                "status": "skipped",
                "reason": "kill_switch_engaged",
                "started_at": started_at.isoformat(),
            }
        
        # Purge fundamentals_events for tickers outside the canonical Step 3
        # universe before reading pending events — keeps the queue clean.
        await purge_orphaned_fundamentals_events(db)

        now_queue = datetime.now(timezone.utc)

        # Step 3 queue hygiene pass 1: collapse duplicates per (ticker, event_type).
        # Runs immediately after orphan purge, before classification enqueue and
        # before any pending-event reads, so subsequent counts are clean.
        # Snapshots pending count internally before mutating — coherent with update.
        # Marks excess pending events as 'deduped' (no deletes — full audit trail).
        _phase_update("A", status="running", message="Preparing fundamentals queue", activate=True)
        await _write_step3_telemetry(force=True)
        await _progress("Deduplicating pending fundamentals_events queue…")
        dedup_stats = await _deduplicate_pending_events(db, now_queue)

        # ── Logo completeness reconciliation ───────────────────────────────
        # Tickers marked fundamentals_status="complete" before the logo feature
        # may lack logo_status in company_fundamentals_cache.  Detect these
        # stale-complete tickers and reset needs_fundamentals_refresh=True so
        # Step 3 reprocesses them and downloads logos.
        await _progress("Reconciling logo completeness for stale tickers…")
        _logo_reconcile_stats = await _reconcile_logo_completeness(db)
        if _logo_reconcile_stats["reset_count"] > 0:
            logger.info(
                f"{job_name}: logo reconciliation reset {_logo_reconcile_stats['reset_count']} "
                "tickers to needs_fundamentals_refresh=True (missing logo_status)"
            )

        # Ensure Step 3 includes priced tickers missing classification.
        # Exclude tickers already fundamentals_status='complete': if a prior run
        # succeeded but the provider returned no sector/industry, re-processing
        # will not fix the gap.  Without this guard every subsequent run
        # re-enqueues the same ~400+ tickers, sets needs_fundamentals_refresh=True,
        # defeats the skip-gate, and prevents Step 3 from converging.
        class_candidates = await db.tracked_tickers.find(
            {
                **SEED_QUERY,
                "has_price_data": True,
                "fundamentals_status": {"$ne": "complete"},
                "$or": [
                    {"sector": {"$in": [None, ""]}},
                    {"industry": {"$in": [None, ""]}},
                    {"has_classification": {"$ne": True}},
                ],
            },
            {"_id": 0, "ticker": 1},
        ).to_list(None)  # No limit — enqueue ALL tickers missing classification
        class_candidate_tickers = [d.get("ticker") for d in class_candidates if d.get("ticker")]
        class_enqueue = await _enqueue_fundamentals_events(
            db,
            event_type="classification_missing",
            tickers=class_candidate_tickers,
            now=datetime.now(timezone.utc),
            source_job=job_name,
            detector_step="3.0",
        )

        # Canonical Step 3 universe total — informational only (stored in details).
        # Also used as the eligible set for requested_event_types aggregation below.
        universe_total = await db.tracked_tickers.count_documents(STEP3_QUERY)

        # Snapshot pending count AFTER dedup pass (and after classification enqueue).
        pending_after_dedup = await db.fundamentals_events.count_documents({"status": "pending"})

        pending_events = await db.fundamentals_events.find(
            {"status": "pending"},
            {"ticker": 1, "event_type": 1, "created_at": 1}
        ).sort("created_at", 1).to_list(None)

        # Collect unique ticker list for skip-gate lookup.
        all_ticker_fulls: List[str] = list({
            e["ticker"] for e in pending_events if e.get("ticker")
        })

        # Step 3 queue hygiene pass 2: skip tickers already complete with no refresh flag.
        # Marks their events as 'skipped' (no deletes — full audit trail).
        await _progress("Gating already-complete tickers (no refresh needed)…")
        skip_stats = await _skip_already_complete_tickers(db, all_ticker_fulls, now_queue)
        skipped_ticker_set: set = set(skip_stats["skipped_tickers"])

        # Step 3 execution selection is deterministic from tracked_tickers flags.
        # fundamentals_events does not drive processing selection.
        flagged_tickers = await db.tracked_tickers.find(
            {"needs_fundamentals_refresh": True},
            {"_id": 0, "ticker": 1},
        ).to_list(None)
        flagged_full_tickers = {
            str(doc.get("ticker"))
            for doc in flagged_tickers
            if doc.get("ticker")
        }
        tickers_to_sync = sorted({
            t[:-3] if t.endswith(".US") else t
            for t in flagged_full_tickers
        })

        # Build events_by_ticker from remaining pending events (audit-only map for
        # event completion status updates after each successful ticker sync).
        events_by_ticker: Dict[str, List[dict]] = {}
        for event in pending_events:
            ticker_full = event.get("ticker")
            if not ticker_full:
                continue
            # Exclude tickers that were just marked skipped.
            if ticker_full in skipped_ticker_set:
                continue
            # Keep only event rows for tickers selected via flags.
            if ticker_full not in flagged_full_tickers:
                continue
            events_by_ticker.setdefault(ticker_full, []).append(event)

        # Full STEP3_QUERY eligible list — used for both tickers_to_sync filter and
        # the requested_event_types aggregation so both are scoped to the same universe.
        _MAX_ELIGIBLE = 10_000
        eligible_full: List[str] = await db.tracked_tickers.distinct("ticker", STEP3_QUERY)

        # Filter tickers_to_sync to the canonical Step 3 universe.
        if tickers_to_sync and eligible_full:
            eligible_full_set = set(eligible_full)
            tickers_to_sync = [
                t for t in tickers_to_sync
                if (t if t.endswith(".US") else f"{t}.US") in eligible_full_set
            ]

        # requested_event_types: DB aggregation scoped to the Step 3 universe and
        # post-skip-gate pending events. Reuses eligible_full to avoid a second
        # distinct call. Falls back to {} with a warning if the list is unsafe.
        requested_event_types: Dict[str, int] = {}
        if not eligible_full:
            logger.warning(f"{job_name}: eligible_full is empty — skipping requested_event_types aggregation")
        elif len(eligible_full) > _MAX_ELIGIBLE:
            logger.warning(
                f"{job_name}: eligible_full too large ({len(eligible_full)}) "
                f"— skipping requested_event_types aggregation (threshold={_MAX_ELIGIBLE})"
            )
        else:
            async for doc in db.fundamentals_events.aggregate([
                {"$match": {"status": "pending", "ticker": {"$in": eligible_full}}},
                {"$group": {"_id": "$event_type", "count": {"$sum": 1}}},
            ]):
                event_type = doc["_id"] or "unknown"
                requested_event_types[event_type] = doc["count"]

        queue_stats = {
            "pending_before_dedup": dedup_stats["pending_before_dedup"],
            "deduped_event_count": dedup_stats["deduped_event_count"],
            "pending_after_dedup": pending_after_dedup,
            "skipped_complete_count": skip_stats["skipped_ticker_count"],
            "skipped_event_count": skip_stats["skipped_event_count"],
            "actionable_count": len(tickers_to_sync),
        }
        logger.info(
            f"{job_name} queue_stats: {queue_stats}"
        )

        total_to_sync = len(tickers_to_sync) if tickers_to_sync else 0

        if await _is_cancelled():
            return await _finalize_cancelled(
                stop_reason="cancel_requested_before_fundamentals",
                progress_msg="Cancelled before fundamentals processing",
                processed=0,
                total=total_to_sync,
            )

        # Process tickers
        result = {
            "job_name": job_name,
            "status": "completed",
            "processed": 0,
            "success": 0,
            "failed": 0,
            "ticker_timeouts": 0,
            "db_timeouts": 0,
            "classification_events_enqueued": class_enqueue.get("new_inserts", 0),
            "classification_events_already_pending": class_enqueue.get("skipped_existing", 0),
            # requested_event_types moved to _debug — not used by the Step 3 card.
            "_debug": {
                "requested_event_types": requested_event_types,
                "queue_stats": queue_stats,
            },
            "started_at": started_at.isoformat(),
        }

        if not tickers_to_sync:
            logger.info(f"{job_name}: No tickers marked for fundamentals refresh")
            result["message"] = "No tickers marked for fundamentals refresh"
        else:
            total = len(tickers_to_sync)
            _phase_update("A", status="running", processed=0, total=total, message="Syncing fundamentals", activate=True)
            await _write_step3_telemetry(force=True)
            await _progress(f"Processing {total} tickers (parallel, 20 concurrent)…")
            logger.info(f"{job_name}: processing {total} tickers in parallel (universe={universe_total})")

            # Parallel processing — 20 concurrent requests
            CONCURRENCY = 20
            semaphore = asyncio.Semaphore(CONCURRENCY)

            # Shared cancel event — set once by the monitor when the DB flag is found.
            cancel_event = asyncio.Event()
            tasks: List[asyncio.Task] = []
            cancel_monitor_task: Optional[asyncio.Task] = None

            async def _cancel_phase_a(stop_reason: str, progress_msg: str) -> Dict[str, Any]:
                cancel_event.set()
                for task in tasks:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                if cancel_monitor_task:
                    cancel_monitor_task.cancel()
                    await asyncio.gather(cancel_monitor_task, return_exceptions=True)
                return await _finalize_cancelled(
                    stop_reason=stop_reason,
                    progress_msg=progress_msg,
                    processed=done_count,
                    total=total,
                )

            async def _cancel_monitor_sched() -> None:
                """Poll run status every 2 s; set cancel_event when status becomes cancel_requested."""
                while not cancel_event.is_set():
                    await asyncio.sleep(2)
                    if await _is_cancelled():
                        logger.info(f"{job_name}: cancel_requested status detected by monitor")
                        cancel_event.set()
                        return

            async def _process_one(ticker: str) -> dict:
                nonlocal _last_ticker_started, _last_ticker_finished
                t = ticker.upper().strip()
                ticker_full = t if t.endswith(".US") else f"{t}.US"
                # Fast in-memory check — no DB round-trip
                if cancel_event.is_set() or await _is_cancelled():
                    cancel_event.set()
                    return {"ticker": ticker_full, "success": False, "cancelled": True}
                async with semaphore:
                    _last_ticker_started = ticker_full
                    try:
                        res = await asyncio.wait_for(
                            sync_single_ticker_fundamentals(db, ticker, source_job=job_name),
                            timeout=FUNDAMENTALS_SYNC_TICKER_TIMEOUT_SECONDS,
                        )
                    except asyncio.TimeoutError:
                        logger.error(
                            f"{job_name}: ticker {ticker_full} timed out after "
                            f"{FUNDAMENTALS_SYNC_TICKER_TIMEOUT_SECONDS}s"
                        )
                        res = {
                            "ticker": ticker_full,
                            "success": False,
                            "error": (
                                f"Per-ticker hard timeout after "
                                f"{FUNDAMENTALS_SYNC_TICKER_TIMEOUT_SECONDS}s"
                            ),
                            "error_type": "ticker_timeout",
                        }
                    _last_ticker_finished = ticker_full
                    return res

            cancel_monitor_task = asyncio.create_task(_cancel_monitor_sched())
            tasks = [asyncio.create_task(_process_one(t)) for t in tickers_to_sync]
            done_count = 0

            try:
                for coro in asyncio.as_completed(tasks):
                    if cancel_event.is_set() or await _is_cancelled():
                        return await _cancel_phase_a(
                            "cancel_requested_phase_a",
                            "Cancelled during fundamentals processing",
                        )
                    ticker_result = await coro

                    if ticker_result.get("cancelled"):
                        return await _cancel_phase_a(
                            "cancel_requested_phase_a",
                            "Cancelled during fundamentals processing",
                        )

                    result["processed"] += 1
                    done_count += 1

                    # Resolve the canonical .US ticker key used in events_by_ticker.
                    # sync_single_ticker_fundamentals already returns ticker with .US suffix;
                    # do NOT append .US again.
                    raw_ticker = ticker_result.get("ticker", "")
                    ticker_us = raw_ticker if raw_ticker.endswith(".US") else f"{raw_ticker}.US"
                    event_ids = [
                        e.get("_id")
                        for e in events_by_ticker.get(ticker_us, [])
                        if e.get("_id")
                    ]

                    if ticker_result.get("success"):
                        result["success"] += 1
                        if event_ids:
                            await db.fundamentals_events.update_many(
                                {"_id": {"$in": event_ids}},
                                {"$set": {"status": "completed", "completed_at": datetime.now(timezone.utc)}}
                            )
                    else:
                        result["failed"] += 1
                        err_type = ticker_result.get("error_type")
                        if err_type == "ticker_timeout":
                            result["ticker_timeouts"] += 1
                        elif err_type == "db_timeout":
                            result["db_timeouts"] += 1
                        if event_ids:
                            await db.fundamentals_events.update_many(
                                {"_id": {"$in": event_ids}},
                                {"$set": {
                                    "status": "skipped",
                                    "skipped_reason": "sync_failed",
                                    "skipped_at": datetime.now(timezone.utc),
                                }}
                            )

                    # Progress update every 100 tickers
                    _phase_update(
                        "A",
                        status="running",
                        processed=done_count,
                        total=total,
                        message="Syncing fundamentals",
                        activate=True,
                    )
                    await _write_step3_telemetry(throttle_processed=done_count)
                    if done_count % 100 == 0:
                        await _progress(
                            f"Fundamentals sync: {done_count}/{total} done "
                            f"(✓{result['success']} ✗{result['failed']})"
                        )
            finally:
                if cancel_monitor_task:
                    cancel_monitor_task.cancel()
                    await asyncio.gather(cancel_monitor_task, return_exceptions=True)
        # Post-loop safety sweep: mark any remaining pending events from the batch
        # as 'skipped' so they do not stay pending across runs.  This catches events
        # for flagged tickers that were filtered out by the eligible-set gate or any
        # other edge case that left events in events_by_ticker unprocessed.
        if events_by_ticker and result.get("status") != "cancelled":
            remaining_ids = [
                e.get("_id")
                for evts in events_by_ticker.values()
                for e in evts
                if e.get("_id")
            ]
            if remaining_ids:
                sweep_result = await db.fundamentals_events.update_many(
                    {"_id": {"$in": remaining_ids}, "status": "pending"},
                    {"$set": {
                        "status": "skipped",
                        "skipped_reason": "post_step3_sweep",
                        "skipped_at": datetime.now(timezone.utc),
                    }},
                )
                if sweep_result.modified_count:
                    logger.info(
                        f"{job_name}: post-loop sweep marked {sweep_result.modified_count} "
                        f"remaining pending events as skipped"
                    )

        # ── Logo backfill ──────────────────────────────────────────────────
        # Download logos for cached tickers that need a (re-)download.
        # Runs every Step 3 until the worklist is empty.  Handles:
        #   • tickers synced before the logo feature (logo_status missing)
        #   • transient logo errors  (logo_status="error")
        #   • stale "absent" from old CDN base (logo_fetched_at < cutoff)
        # Excludes tickers already processed in Phase A main.
        processed_in_phase_a: set = {
            (t if t.endswith(".US") else f"{t}.US")
            for t in tickers_to_sync
        }
        logo_backfill_worklist = await _build_logo_backfill_worklist(
            db, exclude_tickers=processed_in_phase_a,
        )
        logo_backfill_stats: Dict[str, int] = {
            "targeted": len(logo_backfill_worklist),
            "present": 0,
            "absent": 0,
            "error": 0,
            "skipped": 0,
        }

        # ── Telemetry: logo backfill worklist diagnostics ──────────────────
        _bf_sample = [d.get("ticker") for d in logo_backfill_worklist[:5]]
        step3_telemetry["logo_backfill_visible_worklist_count"] = len(logo_backfill_worklist)
        step3_telemetry["logo_backfill_visible_sample_tickers"] = _bf_sample
        logger.info(
            f"{job_name}: logo_backfill_visible_worklist_count={len(logo_backfill_worklist)} "
            f"sample={_bf_sample}"
        )
        await _write_step3_telemetry(force=True)

        if logo_backfill_worklist and not await _is_cancelled():
            from fundamentals_service import _download_logo as _dl_logo

            bf_total = len(logo_backfill_worklist)
            logger.info(f"{job_name}: logo backfill: {bf_total} tickers queued")
            _phase_update(
                "A", status="running",
                processed=done_count, total=done_count + bf_total,
                message=f"Logo backfill: 0/{bf_total}",
                activate=True,
            )
            await _write_step3_telemetry(force=True)
            await _progress(f"Logo backfill: downloading logos for {bf_total} tickers…")

            _logo_sem = asyncio.Semaphore(LOGO_BACKFILL_CONCURRENCY)
            _bf_done = 0

            async def _backfill_one_logo(cache_doc: dict) -> None:
                nonlocal _bf_done, done_count
                tk = cache_doc["ticker"]
                async with _logo_sem:
                    if await _is_cancelled():
                        logo_backfill_stats["skipped"] += 1
                        return
                    try:
                        lr = await asyncio.wait_for(
                            _dl_logo(cache_doc.get("logo_url"), tk),
                            timeout=LOGO_BACKFILL_TICKER_TIMEOUT,
                        )
                    except asyncio.TimeoutError:
                        lr = {
                            "logo_status": "error",
                            "logo_fetched_at": datetime.now(timezone.utc),
                        }

                    # Persist to company_fundamentals_cache
                    cache_set: dict = {
                        "logo_status": lr["logo_status"],
                        "logo_fetched_at": lr["logo_fetched_at"],
                    }
                    if lr.get("logo_data"):
                        cache_set["logo_data"] = lr["logo_data"]
                        cache_set["logo_content_type"] = lr["logo_content_type"]
                    await db.company_fundamentals_cache.update_one(
                        {"ticker": tk}, {"$set": cache_set},
                    )

                    logo_backfill_stats[lr["logo_status"]] = (
                        logo_backfill_stats.get(lr["logo_status"], 0) + 1
                    )

                    # Promote partial → complete when logo resolves
                    if lr["logo_status"] in ("present", "absent"):
                        await db.tracked_tickers.update_one(
                            {"ticker": tk, "fundamentals_status": "partial"},
                            {"$set": {
                                "fundamentals_status": "complete",
                                "fundamentals_complete": True,
                                "needs_fundamentals_refresh": False,
                            }},
                        )

                    _bf_done += 1
                    done_count += 1
                    if _bf_done % 50 == 0 or _bf_done == bf_total:
                        _phase_update(
                            "A", status="running",
                            processed=done_count,
                            total=done_count + bf_total - _bf_done,
                            message=f"Logo backfill: {_bf_done}/{bf_total}",
                        )
                        await _write_step3_telemetry(throttle_processed=done_count)

            bf_tasks = [
                asyncio.create_task(_backfill_one_logo(d))
                for d in logo_backfill_worklist
            ]
            await asyncio.gather(*bf_tasks, return_exceptions=True)

            logger.info(
                f"{job_name}: logo backfill done — "
                f"targeted={logo_backfill_stats['targeted']} "
                f"present={logo_backfill_stats['present']} "
                f"absent={logo_backfill_stats['absent']} "
                f"error={logo_backfill_stats['error']}"
            )

        result["logo_backfill"] = logo_backfill_stats

        # ── Proof-of-logo diagnostic: find ONE visible ticker with stored logo ─
        _proof_ticker: Optional[str] = None
        try:
            _visible_with_logo = await db.tracked_tickers.aggregate([
                {"$match": {"status": "active", "is_visible": True}},
                {"$lookup": {
                    "from": "company_fundamentals_cache",
                    "localField": "ticker",
                    "foreignField": "ticker",
                    "as": "_fc",
                }},
                {"$unwind": "$_fc"},
                {"$match": {
                    "_fc.logo_status": "present",
                    "_fc.logo_data": {"$exists": True},
                }},
                {"$limit": 1},
                {"$project": {"_id": 0, "ticker": 1}},
            ]).to_list(1)
            if _visible_with_logo:
                _proof_ticker = _visible_with_logo[0]["ticker"]
        except Exception as _proof_exc:
            logger.warning(f"{job_name}: proof-of-logo query failed: {_proof_exc}")

        step3_telemetry["visible_present_logo_example_ticker"] = _proof_ticker
        step3_telemetry["visible_present_logo_example_found"] = _proof_ticker is not None
        logger.info(
            f"{job_name}: proof-of-logo diagnostic: "
            f"found={_proof_ticker is not None} ticker={_proof_ticker}"
        )

        _phase_a_total = (len(tickers_to_sync) if tickers_to_sync else 0) + logo_backfill_stats["targeted"]
        _phase_update(
            "A",
            status="done" if result.get("status") != "cancelled" else "error",
            processed=done_count,
            total=_phase_a_total,
            message="Fundamentals sync completed" if result.get("status") != "cancelled" else "Fundamentals sync cancelled",
            activate=True,
        )
        await _write_step3_telemetry(force=True)

        # Step 3 exclusion report: tickers still missing sector/industry after sync
        now_step3 = datetime.now(timezone.utc)
        step3_report = await save_step3_exclusion_report(db, now_step3)
        result["step3_exclusion_rows"]    = step3_report.get("step3_exclusion_rows", 0)
        result["exclusion_report_run_id"] = step3_report.get("exclusion_report_run_id")
        result["parent_run_id"]           = parent_run_id

        # Step 3 ticker-level funnel counts (authoritative, computed after sync).
        # "Up-to-date" = fundamentals_status='complete'
        #               AND needs_fundamentals_refresh != True
        #               AND fundamentals_updated_at is not null/missing
        step3_input_total = await db.tracked_tickers.count_documents(STEP3_QUERY)
        step3_output_total = await db.tracked_tickers.count_documents({
            **STEP3_QUERY,
            "fundamentals_status": "complete",
            "needs_fundamentals_refresh": {"$ne": True},
            "fundamentals_updated_at": {"$nin": [None, ""], "$exists": True},
        })
        step3_filtered_out_total = max(step3_input_total - step3_output_total, 0)
        result["step3_funnel"] = {
            "input_total": step3_input_total,
            "output_total": step3_output_total,
            "filtered_out_total": step3_filtered_out_total,
        }

        # Step 3 visibility auto-chain: after successful Step 3 completion, recompute visibility.
        step3_visibility = {
            "triggered": False,
            "status": "skipped",
        }
        if result["status"] == "completed":
            if await _is_cancelled():
                return await _finalize_cancelled(
                    stop_reason="cancel_requested_before_visibility",
                    progress_msg="Cancelled before visibility recompute",
                    processed=done_count,
                    total=total_to_sync,
                )
            _phase_update("B", status="running", processed=0, total=1, message="Recomputing visibility", activate=True)
            await _write_step3_telemetry(force=True)
            try:
                # Pass Step 3 exclusion_report_run_id as parent for visibility recompute.
                _s3_excl_run_id = result.get("exclusion_report_run_id")
                visibility_result = await recompute_visibility_all(db, parent_run_id=_s3_excl_run_id)
                step3_visibility = {
                    "triggered": True,
                    "status": "completed",
                    "job_id": visibility_result.get("job_id"),
                    "duration_seconds": visibility_result.get("duration_seconds"),
                    "changed": visibility_result.get("stats", {}).get("changed"),
                    "now_visible": visibility_result.get("after", {}).get("visible_count"),
                }
                logger.info(
                    f"{job_name}: Visibility recompute completed "
                    f"(job_id={step3_visibility.get('job_id')}, changed={step3_visibility.get('changed')})"
                )
                # Visibility exclusion report: tickers filtered by visibility gates
                now_vis = datetime.now(timezone.utc)
                vis_report = await save_step3_visibility_exclusion_report(db, now_vis)
                step3_visibility["exclusion_rows"] = vis_report.get("visibility_exclusion_rows", 0)
                step3_visibility["_debug"] = vis_report.get("_debug", {})
                step3_visibility["exclusion_report_run_id"] = vis_report.get("exclusion_report_run_id")
                result["step3_visibility_exclusion_report_run_id"] = vis_report.get("exclusion_report_run_id")
                _phase_update("B", status="done", processed=1, total=1, message="Visibility recompute completed", activate=True)
            except Exception as vis_error:
                step3_visibility = {
                    "triggered": True,
                    "status": "failed",
                    "error": str(vis_error),
                }
                logger.error(f"{job_name}: Visibility recompute failed: {vis_error}")
                _phase_update("B", status="error", processed=1, total=1, message="Visibility recompute failed", activate=True)
            await _write_step3_telemetry(force=True)
        result["step3_visibility"] = step3_visibility

        # ── Phase C: Download complete price history for VISIBLE tickers only ──
        # Tickers selected if: is_visible == true AND any of:
        #   - price_history_complete != true (legacy operational flag)
        #   - needs_price_redownload == true (remediation flag)
        #   - history_download_proven_at does not exist (strict proof missing)
        # Uses _process_price_ticker from full_sync_service (handles split redownload).
        from full_sync_service import _process_price_ticker

        phase_c_stats = {
            "tickers_targeted": 0,
            "tickers_succeeded": 0,
            "tickers_failed": 0,
            "total_records": 0,
        }

        if result["status"] == "completed":
            _phase_update("C", status="running", processed=0, total=None, message="Preparing price history queue", activate=True)
            await _write_step3_telemetry(force=True)

            # ── Phase C pre-flight: unseal falsely-complete tickers ───────
            # Tickers sealed with price_history_complete=True but whose stored
            # range_proof fields show a mismatch (or are absent) were stamped
            # before range-proof validation existed.  Clear the flag so
            # Phase C re-downloads their full history on this run.
            from full_sync_service import _RANGE_PROOF_TOLERANCE_DAYS, _check_range_proof
            _preflight_sealed = await db.tracked_tickers.find(
                {"price_history_complete": True},
                {"_id": 0, "ticker": 1, "range_proof": 1},
            ).to_list(None)
            _preflight_unsealed: list[str] = []
            if _preflight_sealed:
                for doc in _preflight_sealed:
                    rp = doc.get("range_proof") or {}
                    # Unseal if range_proof is missing entirely or fails validation
                    if not rp or not rp.get("pass"):
                        _preflight_unsealed.append(doc["ticker"])
                        continue
                    # Re-verify stored proof in case tolerance changed
                    if not _check_range_proof(
                        rp.get("provider_first_date"),
                        rp.get("provider_last_date"),
                        rp.get("db_first_date"),
                        rp.get("db_last_date"),
                    ):
                        _preflight_unsealed.append(doc["ticker"])
                _preflight_unsealed.sort()
                if _preflight_unsealed:
                    _unseal_ts = datetime.now(timezone.utc)
                    await db.tracked_tickers.update_many(
                        {"ticker": {"$in": _preflight_unsealed}},
                        {"$set": {
                            "price_history_complete": False,
                            "price_history_status": "unsealed_by_preflight_audit",
                            "needs_price_redownload": True,
                            "preflight_unsealed_at": _unseal_ts,
                            "updated_at": _unseal_ts,
                        }},
                    )
                    logger.info(
                        "[Phase C preflight] Unsealed %d tickers with "
                        "price_history_complete=True but missing/failed "
                        "range_proof — Phase C will re-download. samples=%s",
                        len(_preflight_unsealed),
                        _preflight_unsealed[:10],
                    )
            step3_telemetry["phases"]["C"]["preflight_unsealed"] = len(_preflight_unsealed)
            step3_telemetry["phases"]["C"]["preflight_unsealed_sample"] = _preflight_unsealed[:20]

            # Use Phase C eligible query (gates 1-7, excludes gate 8 / price_history_complete)
            # to avoid chicken-and-egg: Phase C SETS price_history_complete, so we can't
            # require it as a precondition.
            from visibility_rules import get_phase_c_eligible_query
            _phase_c_cursor = db.tracked_tickers.find(
                {
                    **get_phase_c_eligible_query(),
                    "$or": [
                        {"price_history_complete": {"$ne": True}},
                        {"needs_price_redownload": True},
                        {"history_download_proven_at": {"$exists": False}},
                    ],
                },
                {"_id": 0, "ticker": 1, "needs_price_redownload": 1, "price_history_complete": 1, "history_download_proven_at": 1},
            )
            phase_c_docs = [doc async for doc in _phase_c_cursor]
            phase_c_tickers = [
                (doc["ticker"], bool(doc.get("needs_price_redownload")))
                for doc in phase_c_docs
            ]
            phase_c_post_dedupe_total = len(phase_c_tickers)
            phase_c_count_incomplete = sum(
                1 for doc in phase_c_docs
                if doc.get("price_history_complete") is not True
            )
            phase_c_count_redownload = sum(
                1 for doc in phase_c_docs
                if bool(doc.get("needs_price_redownload"))
            )
            phase_c_count_missing_proof = sum(
                1 for doc in phase_c_docs
                if doc.get("history_download_proven_at") is None
            )
            phase_c_selection_sources = []
            if phase_c_count_incomplete > 0:
                phase_c_selection_sources.append("price_history_incomplete")
            if phase_c_count_redownload > 0:
                phase_c_selection_sources.append("needs_price_redownload")
            if phase_c_count_missing_proof > 0:
                phase_c_selection_sources.append("missing_strict_proof")
            phase_c_reasons_by_ticker = {
                doc["ticker"]: {
                    "price_history_incomplete": doc.get("price_history_complete") is not True,
                    "needs_price_redownload": bool(doc.get("needs_price_redownload")),
                    "missing_strict_proof": doc.get("history_download_proven_at") is None,
                }
                for doc in phase_c_docs
            }
            phase_c_counts_by_reason = {
                "price_history_incomplete": 0,
                "needs_price_redownload": 0,
                "missing_strict_proof": 0,
            }
            phase_c_sample_tickers_by_reason = {
                "price_history_incomplete": [],
                "needs_price_redownload": [],
                "missing_strict_proof": [],
            }
            for ticker, _needs_redownload in phase_c_tickers:
                ticker_reasons = phase_c_reasons_by_ticker.get(ticker, {})
                for reason in ("price_history_incomplete", "needs_price_redownload", "missing_strict_proof"):
                    if ticker_reasons.get(reason) is True:
                        phase_c_counts_by_reason[reason] += 1
                        if len(phase_c_sample_tickers_by_reason[reason]) < 10:
                            phase_c_sample_tickers_by_reason[reason].append(ticker)
            step3_telemetry["phases"]["C"]["selection_audit"] = {
                "selection_sources": phase_c_selection_sources,
                "counts_by_source_pre_dedupe": {
                    "price_history_incomplete": phase_c_count_incomplete,
                    "needs_price_redownload": phase_c_count_redownload,
                    "missing_strict_proof": phase_c_count_missing_proof,
                },
                "pre_dedupe_total": len(phase_c_docs),
                "post_dedupe_total": phase_c_post_dedupe_total,
                "counts_by_reason": phase_c_counts_by_reason,
                "sample_tickers_by_reason": phase_c_sample_tickers_by_reason,
                "selection_criteria": "PhaseC: union(price_history_incomplete, needs_price_redownload, missing_strict_proof) then dedupe by ticker",
                "overlap_possible": True,
            }
            phase_c_stats["tickers_targeted"] = phase_c_post_dedupe_total
            _phase_update("C", status="running", processed=0, total=phase_c_post_dedupe_total, message="Syncing price history", activate=True)
            await _write_step3_telemetry(force=True)

            if not phase_c_tickers:
                await _progress("Phase C price history: 0 visible tickers to backfill")
                logger.info(f"{job_name}: Phase C — no visible tickers need price history")
                _phase_update("C", status="done", processed=0, total=0, message="No visible tickers need price history", activate=True)
                await _write_step3_telemetry(force=True)
            else:
                total_c = phase_c_post_dedupe_total
                logger.info(f"{job_name}: Phase C — downloading price history for {total_c} visible tickers")
                if await _is_cancelled():
                    return await _finalize_cancelled(
                        stop_reason="cancel_requested_before_phase_c",
                        progress_msg="Cancelled before Phase C price history",
                        processed=0,
                        total=total_c,
                    )
                try:
                    _phase_c_concurrency = int(
                        os.getenv(
                            "STEP3_PHASE_C_CONCURRENCY",
                            str(STEP3_PHASE_C_CONCURRENCY_DEFAULT),
                        )
                    )
                except (TypeError, ValueError):
                    _phase_c_concurrency = STEP3_PHASE_C_CONCURRENCY_DEFAULT
                phase_c_concurrency = max(
                    1,
                    min(_phase_c_concurrency, STEP3_PHASE_C_CONCURRENCY_MAX),
                )
                logger.info(
                    f"{job_name}: Phase C concurrency={phase_c_concurrency} "
                    f"(requested={_phase_c_concurrency}, "
                    f"max={STEP3_PHASE_C_CONCURRENCY_MAX})"
                )

                semaphore_c = asyncio.Semaphore(phase_c_concurrency)

                async def _process_price_one(
                    ticker: str,
                    needs_redownload: bool,
                ) -> Dict[str, Any]:
                    if await _is_cancelled():
                        return {"ticker": ticker, "success": False, "cancelled": True, "records": 0}
                    async with semaphore_c:
                        try:
                            ph_result = await _process_price_ticker(
                                db, ticker, job_name=job_name,
                                needs_redownload=needs_redownload,
                                cancel_check=_is_cancelled,
                            )
                            return {
                                "ticker": ticker,
                                "success": bool(ph_result.get("success")),
                                "records": int(ph_result.get("records", 0)),
                                "cancelled": bool(ph_result.get("cancelled")),
                            }
                        except Exception as ph_err:
                            logger.warning(f"{job_name}: Phase C failed for {ticker}: {ph_err}")
                            # Record failure on the ticker document and re-flag
                            # for retry so Phase C picks it up on the next run.
                            try:
                                _ticker_us = ticker if ticker.endswith(".US") else f"{ticker}.US"
                                _fail_ts = datetime.now(timezone.utc)
                                _err_set: Dict[str, Any] = {
                                    "price_history_status": "error",
                                    "history_download_failed_at": _fail_ts,
                                    "history_download_error": f"exception: {ph_err}",
                                }
                                if needs_redownload:
                                    _err_set["needs_price_redownload"] = True
                                    _err_set["price_history_complete"] = False
                                await db.tracked_tickers.update_one(
                                    {"ticker": _ticker_us},
                                    {"$set": _err_set},
                                )
                            except Exception:
                                pass  # Best-effort — don't mask the original error
                            return {
                                "ticker": ticker,
                                "success": False,
                                "records": 0,
                            }

                done_c = 0
                try:
                    _phase_c_batch_size = int(
                        os.getenv(
                            "STEP3_PHASE_C_BATCH_SIZE",
                            str(STEP3_PHASE_C_BATCH_SIZE_DEFAULT),
                        )
                    )
                except (TypeError, ValueError):
                    _phase_c_batch_size = STEP3_PHASE_C_BATCH_SIZE_DEFAULT
                phase_c_batch_size = max(1, _phase_c_batch_size)
                logger.info(
                    f"{job_name}: Phase C batch_size={phase_c_batch_size}"
                )

                _phase_c_cancelled = False
                for batch_start in range(0, total_c, phase_c_batch_size):
                    if await _is_cancelled():
                        _phase_c_cancelled = True
                        break
                    batch = phase_c_tickers[batch_start:batch_start + phase_c_batch_size]
                    tasks_c = [
                        asyncio.create_task(_process_price_one(ticker, needs_redownload))
                        for ticker, needs_redownload in batch
                    ]

                    for coro in asyncio.as_completed(tasks_c):
                        if await _is_cancelled():
                            for task in tasks_c:
                                if not task.done():
                                    task.cancel()
                            await asyncio.gather(*tasks_c, return_exceptions=True)
                            _phase_c_cancelled = True
                            break

                        ticker_result = await coro
                        if ticker_result.get("cancelled"):
                            for task in tasks_c:
                                if not task.done():
                                    task.cancel()
                            await asyncio.gather(*tasks_c, return_exceptions=True)
                            _phase_c_cancelled = True
                            break
                        if ticker_result.get("success"):
                            phase_c_stats["tickers_succeeded"] += 1
                            phase_c_stats["total_records"] += int(ticker_result.get("records") or 0)
                        else:
                            phase_c_stats["tickers_failed"] += 1

                        done_c += 1
                        _phase_update(
                            "C",
                            status="running",
                            processed=done_c,
                            total=total_c,
                            message="Syncing price history",
                            activate=True,
                        )
                        await _write_step3_telemetry(throttle_processed=done_c)
                        if done_c % 10 == 0 or done_c == total_c:
                            await _progress(
                                f"Phase C price history: {done_c}/{total_c} "
                                f"(✓{phase_c_stats['tickers_succeeded']} "
                                f"✗{phase_c_stats['tickers_failed']})"
                            )

                    if _phase_c_cancelled:
                        break

                if _phase_c_cancelled:
                    return await _finalize_cancelled(
                        stop_reason="cancel_requested_phase_c",
                        progress_msg="Cancelled during Phase C price history",
                        processed=done_c,
                        total=total_c,
                    )

                if result.get("status") == "cancelled":
                    _phase_update("C", status="error", processed=phase_c_stats["tickers_succeeded"] + phase_c_stats["tickers_failed"], total=total_c, message="Price history sync cancelled", activate=True)
                else:
                    _phase_update("C", status="done", processed=phase_c_stats["tickers_succeeded"] + phase_c_stats["tickers_failed"], total=total_c, message="Price history sync completed", activate=True)
                await _write_step3_telemetry(force=True)

        # ── Defensive: ensure Phase C telemetry is terminal before finalization ──
        # If Phase C was entered but empty, or never entered at all, its status
        # may still be "idle" or "running".  Normalise to "done" so the parent
        # run's telemetry is always consistent when we write the final document.
        _phase_c_status = step3_telemetry["phases"]["C"].get("status")
        if _phase_c_status not in ("done", "error"):
            _phase_update(
                "C",
                status="done",
                processed=0,
                total=0,
                message="No Phase C workload",
                activate=False,
            )
            await _write_step3_telemetry(force=True)

        result["phase_c_stats"] = phase_c_stats
        result["step3_telemetry"] = step3_telemetry

        finished_at = datetime.now(timezone.utc)
        result["finished_at"] = finished_at.isoformat()
        result["started_at_prague"] = _to_prague_iso(started_at)
        result["finished_at_prague"] = _to_prague_iso(finished_at)
        result["log_timezone"] = "Europe/Prague"
        result["universe_total"] = universe_total  # informational only

        total_to_sync = len(tickers_to_sync) if tickers_to_sync else 0

        _done_msg = (
            f"Fundamentals sync: {result['success']}/{total_to_sync} done "
            f"(✓{result['success']} ✗{result['failed']})"
        )
        if phase_c_stats["tickers_targeted"]:
            _done_msg += (
                f" | Price history: "
                f"✓{phase_c_stats['tickers_succeeded']}/{phase_c_stats['tickers_targeted']}"
            )

        # progress_pct: 100 when the job completed all work (even if total was 0).
        if total_to_sync > 0:
            _final_pct = round(done_count / total_to_sync * 100)
        else:
            _final_pct = 100 if result["status"] == "completed" else 0

        await db.ops_job_runs.update_one(
            {"_id": _running_doc_id},
            {"$set": {
                "status": result["status"],
                "finished_at": finished_at,
                "started_at_prague": _to_prague_iso(started_at),
                "finished_at_prague": _to_prague_iso(finished_at),
                "log_timezone": "Europe/Prague",
                "details": result,
                "progress": _done_msg,
                "progress_processed": done_count if tickers_to_sync else 0,
                "progress_total":     total_to_sync,
                "progress_pct":       _final_pct,
                "heartbeat_at": finished_at,
                **({"cancelled_at": finished_at} if result["status"] == "cancelled" else {}),
            }}
        )

        logger.info(
            f"{job_name} {result['status']}: "
            f"processed={result['processed']}, success={result['success']}"
        )

        await _release_single_flight_resources()
        return result

    except Exception as e:
        error_msg = str(e)
        logger.error(f"{job_name} error: {error_msg}")

        finished_ts = datetime.now(timezone.utc)
        progress_done = done_count
        # result["processed"] mirrors done_count; prefer it when set so failure progress
        # matches the same counter used in the success path update.
        if "processed" in result:
            progress_done = result["processed"]
        progress_total = len(tickers_to_sync) if tickers_to_sync else 0
        # tickers_to_sync remains empty if the queue fails before construction.
        if progress_total > 0:
            progress_pct = int(round(progress_done / progress_total * 100))
        else:
            logger.warning(f"{job_name}: failure before queuing tickers; progress fields default to 0")
            progress_pct = 0

        await db.ops_job_runs.update_one(
            {"_id": _running_doc_id},
            {"$set": {
                "status": "error",
                "finished_at": finished_ts,
                "started_at_prague": _to_prague_iso(started_at),
                "finished_at_prague": _to_prague_iso(finished_ts),
                "log_timezone": "Europe/Prague",
                "error": error_msg,
                "error_message": error_msg,
                "details": {"error": error_msg, "step3_telemetry": step3_telemetry},
                "progress_processed": progress_done,
                "progress_total": progress_total,
                "progress_pct": progress_pct,
                "heartbeat_at": finished_ts,
            }},
        )

        await _release_single_flight_resources()
        return {
            "job_name": job_name,
            "status": "error",
            "error": error_msg,
            "started_at": started_at.isoformat(),
        }


async def get_scheduler_status(db) -> Dict[str, Any]:
    """
    Get comprehensive scheduler status.
    
    Returns:
        Status including kill switch, last runs, next scheduled times
    """
    scheduler_enabled = await get_scheduler_enabled(db)
    
    # Get last job runs
    last_price_sync = await db.ops_job_runs.find_one(
        {"job_name": "price_sync"},
        {"_id": 0},
        sort=[("started_at", -1)]
    )
    
    last_fundamentals_sync = await db.ops_job_runs.find_one(
        {"job_name": {"$in": ["fundamentals_sync", "scheduled_fundamentals_sync"]}},
        {"_id": 0},
        sort=[("started_at", -1)]
    )
    
    last_backfill = await db.ops_job_runs.find_one(
        {"job_name": {"$in": ["price_backfill", "scheduled_price_backfill"]}},
        {"_id": 0},
        sort=[("started_at", -1)]
    )
    
    # Get pending work counts
    pending_fundamentals = await db.fundamentals_events.count_documents({"status": "pending"})
    
    def _iso(dt):
        if not dt:
            return None
        return dt.isoformat() if hasattr(dt, "isoformat") else str(dt)

    def _format_last_run(run: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if not run:
            return None
        details = run.get("details") or {}
        started_at = run.get("started_at")
        finished_at = run.get("finished_at")
        seeded_total = (
            run.get("progress_total")
            or details.get("seeded_total")
            or details.get("tickers_seeded_total")
        )
        tickers_with_price_data = details.get("tickers_with_price_data")
        records_upserted = details.get("records_upserted") or run.get("records_upserted")
        phase = run.get("phase") or details.get("phase")
        duration_seconds = None
        if started_at and finished_at:
            duration_seconds = (finished_at - started_at).total_seconds()
        return {
            "status": run.get("status"),
            "started_at": _iso(started_at),
            "finished_at": _iso(finished_at),
            "progress_processed": run.get("progress_processed"),
            "progress_total": seeded_total or run.get("progress_total"),
            "progress_pct": run.get("progress_pct"),
            "phase": phase,
            "duration_seconds": duration_seconds,
            "details": {
                **details,
                "seeded_total": seeded_total,
                "tickers_with_price_data": tickers_with_price_data,
                "records_upserted": records_upserted,
                "phase": phase,
            },
        }

    return {
        "scheduler_enabled": scheduler_enabled,
        "kill_switch_engaged": not scheduler_enabled,
        "schedule": {
            "timezone": "Europe/Prague",
            "days": "Mon-Sat",
            "universe_seed": "03:00",
            "price_sync": "after universe_seed completion",
            "fundamentals_sync": "after price_sync completion",
            "price_backfill": "04:45",
        },
        "last_runs": {
            "price_sync": _format_last_run(last_price_sync),
            "fundamentals_sync": _format_last_run(last_fundamentals_sync),
            "price_backfill": _format_last_run(last_backfill),
        },
        "pending_work": {
            "pending_fundamentals_events": pending_fundamentals,
        },
    }



# =============================================================================
# DELTA FUNDAMENTALS SYNC (Smart sync - avoid redundant downloads)
# =============================================================================

FUNDAMENTALS_STALE_DAYS = 7  # Re-fetch if older than 7 days

async def get_tickers_needing_fundamentals_update(db, limit: int = 100) -> List[str]:
    """
    Get tickers that need fundamentals update, prioritized by:
    1. Never updated (last_fundamentals_update is null)
    2. Oldest updates first (older than FUNDAMENTALS_STALE_DAYS)
    
    Returns:
        List of ticker symbols to update
    """
    stale_threshold = datetime.now(timezone.utc) - timedelta(days=FUNDAMENTALS_STALE_DAYS)
    
    # Priority 1: Never updated
    never_updated = await db.tracked_tickers.find(
        {
            "is_visible": True,
            "$or": [
                {"last_fundamentals_update": {"$exists": False}},
                {"last_fundamentals_update": None}
            ]
        },
        {"_id": 0, "ticker": 1}
    ).limit(limit).to_list(limit)
    
    if len(never_updated) >= limit:
        return [t["ticker"] for t in never_updated]
    
    remaining = limit - len(never_updated)
    
    # Priority 2: Oldest updates (older than threshold)
    stale_tickers = await db.tracked_tickers.find(
        {
            "is_visible": True,
            "last_fundamentals_update": {"$lt": stale_threshold}
        },
        {"_id": 0, "ticker": 1}
    ).sort("last_fundamentals_update", 1).limit(remaining).to_list(remaining)
    
    return [t["ticker"] for t in never_updated] + [t["ticker"] for t in stale_tickers]


async def sync_fundamentals_delta(db, batch_size: int = 50) -> Dict[str, Any]:
    """
    Smart fundamentals sync: only update stale or missing data.
    
    This is the delta fetching logic:
    1. Find tickers with no last_fundamentals_update
    2. Find tickers with last_fundamentals_update > STALE_DAYS old
    3. Update fundamentals and set last_fundamentals_update timestamp
    
    Returns:
        Summary of sync operation
    """
    from batch_jobs_service import sync_single_ticker_fundamentals
    
    started_at = datetime.now(timezone.utc)
    job_name = "fundamentals_sync"
    
    logger.info("[DELTA FUNDAMENTALS] Starting...")
    
    # Get tickers needing update
    tickers = await get_tickers_needing_fundamentals_update(db, batch_size)
    
    if not tickers:
        logger.info("[DELTA FUNDAMENTALS] All tickers up-to-date")
        return {
            "status": "success",
            "job_name": job_name,
            "message": "All tickers up-to-date",
            "tickers_processed": 0,
            "started_at": started_at.isoformat()
        }
    
    logger.info(f"[DELTA FUNDAMENTALS] Found {len(tickers)} tickers to update")
    
    result = {
        "status": "success",
        "job_name": job_name,
        "tickers_processed": 0,
        "tickers_success": 0,
        "tickers_failed": 0,
        "api_calls": 0,
        "started_at": started_at.isoformat()
    }
    
    for ticker in tickers:
        ticker_full = ticker if ticker.endswith(".US") else f"{ticker}.US"
        ticker_short = ticker.replace(".US", "")
        
        try:
            sync_result = await sync_single_ticker_fundamentals(
                db,
                ticker_short,
                source_job=job_name,
            )
            result["tickers_processed"] += 1
            result["api_calls"] += 1
            
            if sync_result.get("success"):
                result["tickers_success"] += 1
                # Update last_fundamentals_update timestamp
                await db.tracked_tickers.update_one(
                    {"ticker": ticker_full},
                    {"$set": {"last_fundamentals_update": datetime.now(timezone.utc)}}
                )
            else:
                result["tickers_failed"] += 1
            
            # Rate limiting
            await asyncio.sleep(0.3)
            
        except Exception as e:
            result["tickers_failed"] += 1
            logger.error(f"[DELTA FUNDAMENTALS] Error for {ticker}: {e}")
    
    result["finished_at"] = datetime.now(timezone.utc).isoformat()
    
    logger.info(f"[DELTA FUNDAMENTALS] Complete: {result['tickers_success']} success, "
               f"{result['tickers_failed']} failed, {result['api_calls']} API calls")
    
    return result


# ═══════════════════════════════════════════════════════════════════════════════
# BULK GAPFILL REMEDIATION
# ═══════════════════════════════════════════════════════════════════════════════

MAX_REMEDIATION_DAYS_PER_RUN = 3


async def _get_remediation_processed_dates_set(db) -> set:
    """
    Collect all dates successfully processed by price_sync OR
    bulk_gapfill_remediation from ops_job_runs.details.price_bulk_gapfill.days[].

    Mirrors the dashboard reader logic (_get_bulk_processed_dates_set in
    market_calendar_service.py) so detection uses the same truth source.
    """
    pipeline = [
        {"$match": {
            "job_name": {"$in": ["price_sync", "bulk_gapfill_remediation"]},
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


async def run_bulk_gapfill_remediation(db) -> Dict[str, Any]:
    """
    Detect missing dates among the last 10 completed trading days and
    remediate up to MAX_REMEDIATION_DAYS_PER_RUN by delegating each date
    to ``remediate_gap_date``.

    Each delegation:
    - Pre-fetches EODHD bulk data once per date (1 API call, not N)
    - Processes only gap tickers (proven + visible, missing DB row)
    - Writes ``gap_free_exclusions`` for NOT-APPLICABLE cases so the
      gap-free metric and chart data_notices stay accurate

    Writes audit proof to ops_job_runs under job_name="bulk_gapfill_remediation"
    using the same details.price_bulk_gapfill.days[] structure the dashboard
    readers already understand.

    Does NOT advance the global watermark (pipeline_state._id="price_bulk").
    """
    from services.market_calendar_service import last_n_completed_trading_days

    started_at = datetime.now(timezone.utc)
    job_name = "bulk_gapfill_remediation"

    # Insert running sentinel
    run_doc_id = (await db.ops_job_runs.insert_one({
        "job_name": job_name,
        "status": "running",
        "started_at": started_at,
        "started_at_prague": _to_prague_iso(started_at),
        "source": "scheduler",
        "details": {},
    })).inserted_id

    days: list = []
    overall_status = "success"

    try:
        # 1) Detect completed trading days
        completed_days = await last_n_completed_trading_days(db, 10, "US")
        logger.info(f"[GAPFILL REMEDIATION] completed_days={completed_days}")

        # 2) Get already-processed dates
        processed_set = await _get_remediation_processed_dates_set(db)
        logger.info(f"[GAPFILL REMEDIATION] processed_set has {len(processed_set)} dates")

        # 3) Compute missing, oldest-first
        missing_dates = sorted([d for d in completed_days if d not in processed_set])
        logger.info(f"[GAPFILL REMEDIATION] missing_dates={missing_dates}")

        # Store detection inputs
        await db.ops_job_runs.update_one(
            {"_id": run_doc_id},
            {"$set": {
                "details.completed_days": completed_days,
                "details.missing_dates": missing_dates,
                "details.processed_set_size": len(processed_set),
            }},
        )

        # 4) Remediate up to MAX_REMEDIATION_DAYS_PER_RUN
        dates_to_fix = missing_dates[:MAX_REMEDIATION_DAYS_PER_RUN]

        if not dates_to_fix:
            logger.info("[GAPFILL REMEDIATION] No missing dates to remediate")
        else:
            for target_date in dates_to_fix:
                day_entry: Dict[str, Any] = {
                    "processed_date": target_date,
                    "status": "error",
                    "rows_written": 0,
                    "advanced_watermark": False,
                    "error": None,
                    "source": "remediate_gap_date",
                }

                try:
                    # Delegate to remediate_gap_date which:
                    # - pre-fetches bulk data once (1 API call vs N)
                    # - only processes gap tickers (not all seeded)
                    # - writes gap_free_exclusions for NOT-APPLICABLE cases
                    gap_result = await remediate_gap_date(db, target_date)
                    gap_status = gap_result.get("status", "error")
                    rows_written = gap_result.get("total_inserted", 0)

                    day_entry["rows_written"] = rows_written
                    day_entry["status"] = gap_status
                    day_entry["gap_tickers_count"] = gap_result.get(
                        "gap_tickers_count", 0
                    )
                    day_entry["exclusions_written"] = len([
                        r for r in gap_result.get("proof_table", [])
                        if not r.get("inserted")
                        and r.get("primary_reason") in _NOT_APPLICABLE_REASONS
                    ])

                    if gap_status == "error":
                        overall_status = "error"

                    logger.info(
                        "[GAPFILL REMEDIATION] %s: %d rows written, "
                        "%d gap tickers, %d exclusions via remediate_gap_date",
                        target_date, rows_written,
                        day_entry["gap_tickers_count"],
                        day_entry["exclusions_written"],
                    )

                except Exception as day_err:
                    day_entry["status"] = "error"
                    day_entry["error"] = str(day_err)[:500]
                    overall_status = "error"
                    logger.error(
                        f"[GAPFILL REMEDIATION] {target_date} failed: {day_err}"
                    )

                days.append(day_entry)

                # Persist after each day so partial progress is visible
                await db.ops_job_runs.update_one(
                    {"_id": run_doc_id},
                    {"$set": {"details.price_bulk_gapfill.days": days}},
                )

    except Exception as exc:
        overall_status = "error"
        logger.error(f"[GAPFILL REMEDIATION] Unhandled error: {exc}")
        # Record the top-level error in the run document
        await db.ops_job_runs.update_one(
            {"_id": run_doc_id},
            {"$set": {"details.error": str(exc)[:500]}},
        )

    # Finalize the ops_job_runs document
    finished_at = datetime.now(timezone.utc)
    await db.ops_job_runs.update_one(
        {"_id": run_doc_id},
        {"$set": {
            "status": overall_status,
            "finished_at": finished_at,
            "finished_at_prague": _to_prague_iso(finished_at),
            "log_timezone": "Europe/Prague",
            "details.price_bulk_gapfill.days": days,
        }},
    )

    logger.info(
        f"[GAPFILL REMEDIATION] Finished: status={overall_status}, "
        f"days_attempted={len(days)}"
    )

    return {
        "job_name": job_name,
        "status": overall_status,
        "days_attempted": len(days),
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
    }


# ── Ticker-level gap remediation ──────────────────────────────────────────
# Fills price gaps for individual (ticker, date) pairs that were missed
# by the bulk ingestion — typically because the ticker was temporarily
# un-seeded when the bulk ran (see whitelist_service.py surgical-unseed fix).
MAX_TICKER_GAP_REMEDIATION_PAIRS = 200  # safety cap per run


async def run_ticker_gap_remediation(db) -> Dict[str, Any]:
    """
    Detect per-ticker price gaps (proven tickers missing stock_prices rows
    for dates that passed bulk sanity) and fill them via per-ticker EODHD
    API.

    This complements run_bulk_gapfill_remediation() which operates at the
    DATE level.  This function operates at the (ticker, date) level —
    fixing tickers that were skipped during an otherwise-successful bulk
    ingestion (e.g. because they were temporarily un-seeded).
    """
    from services.admin_overview_service import _get_bulk_processed_dates
    from price_ingestion_service import fetch_eod_history, parse_eod_record, validate_price_row

    started_at = datetime.now(timezone.utc)
    job_name = "ticker_gap_remediation"

    run_doc_id = (await db.ops_job_runs.insert_one({
        "job_name": job_name,
        "status": "running",
        "started_at": started_at,
        "started_at_prague": _to_prague_iso(started_at),
        "source": "scheduler",
        "details": {},
    })).inserted_id

    overall_status = "success"
    pairs_attempted = 0
    pairs_filled = 0
    rows_written_total = 0
    pair_errors = 0
    skipped_pairs: List[Dict[str, str]] = []

    try:
        # 1) Get expected dates
        expected_dates = await _get_bulk_processed_dates(db)
        if not expected_dates:
            logger.info("[TICKER GAP REMEDIATION] No expected dates — nothing to do")
            overall_status = "success"
            await db.ops_job_runs.update_one(
                {"_id": run_doc_id},
                {"$set": {"details.expected_dates": [], "details.reason": "no_expected_dates"}},
            )
        else:
            # 2) Get proven tickers with anchors
            proven_docs = await db.tracked_tickers.find(
                {
                    "is_visible": True,
                    "history_download_proven_at": {"$exists": True, "$type": "date"},
                    "history_download_proven_anchor": {"$exists": True, "$ne": None},
                },
                {"ticker": 1, "history_download_proven_anchor": 1, "_id": 0},
            ).to_list(None)
            proven_anchors = {
                d["ticker"]: d["history_download_proven_anchor"]
                for d in proven_docs
            }

            if not proven_anchors:
                logger.info("[TICKER GAP REMEDIATION] No proven tickers — nothing to do")
                overall_status = "success"
            else:
                # 3) Find coverage — which (ticker, date) pairs exist
                coverage_pipeline = [
                    {"$match": {
                        "ticker": {"$in": list(proven_anchors.keys())},
                        "date": {"$in": expected_dates},
                    }},
                    {"$group": {"_id": "$ticker", "dates": {"$addToSet": "$date"}}},
                ]
                dates_by_proven: Dict[str, set] = {}
                async for doc in db.stock_prices.aggregate(coverage_pipeline):
                    dates_by_proven[doc["_id"]] = set(doc["dates"])

                # 4) Detect gaps — (ticker, date) pairs that are missing
                gaps: List[Dict[str, str]] = []
                for ticker, anchor in proven_anchors.items():
                    ticker_dates = dates_by_proven.get(ticker, set())
                    for d in expected_dates:
                        if d > anchor and d not in ticker_dates:
                            gaps.append({"ticker": ticker, "date": d})
                            if len(gaps) >= MAX_TICKER_GAP_REMEDIATION_PAIRS:
                                break
                    if len(gaps) >= MAX_TICKER_GAP_REMEDIATION_PAIRS:
                        break

                logger.info(
                    f"[TICKER GAP REMEDIATION] Found {len(gaps)} (ticker,date) gaps "
                    f"(capped at {MAX_TICKER_GAP_REMEDIATION_PAIRS})"
                )

                await db.ops_job_runs.update_one(
                    {"_id": run_doc_id},
                    {"$set": {
                        "details.expected_dates_count": len(expected_dates),
                        "details.proven_tickers_count": len(proven_anchors),
                        "details.gaps_detected": len(gaps),
                    }},
                )

                # 5) Fill gaps using per-ticker EODHD API
                batch_ops: List = []
                for gap in gaps:
                    pairs_attempted += 1
                    ticker = gap["ticker"]
                    target_date = gap["date"]
                    try:
                        records = await fetch_eod_history(
                            ticker, from_date=target_date, to_date=target_date,
                        )
                        if not records:
                            skipped_pairs.append({
                                "ticker": ticker,
                                "date": target_date,
                                "skip_reason": "api_returned_no_records",
                            })
                            logger.info(
                                "[TICKER GAP REMEDIATION] %s %s: "
                                "api_returned_no_records (silent skip prevented)",
                                ticker, target_date,
                            )
                            continue
                        pair_had_data = False
                        for record in records:
                            parsed = parse_eod_record(ticker, record)
                            if not validate_price_row(parsed):
                                logger.warning(
                                    "[TICKER GAP REMEDIATION] %s %s: "
                                    "invalid row (missing ticker/date/close)",
                                    ticker, target_date,
                                )
                                continue
                            batch_ops.append(
                                UpdateOne(
                                    {"ticker": parsed["ticker"], "date": parsed["date"]},
                                    {"$set": parsed},
                                    upsert=True,
                                )
                            )
                            pair_had_data = True
                            pairs_filled += 1
                        if not pair_had_data:
                            skipped_pairs.append({
                                "ticker": ticker,
                                "date": target_date,
                                "skip_reason": "parse_failed_no_date",
                            })
                    except Exception as ticker_err:
                        pair_errors += 1
                        skipped_pairs.append({
                            "ticker": ticker,
                            "date": target_date,
                            "skip_reason": f"fetch_error: {type(ticker_err).__name__}",
                        })
                        logger.warning(
                            f"[TICKER GAP REMEDIATION] {ticker} {target_date} "
                            f"error: {ticker_err}"
                        )

                # 6) Bulk-write all collected operations
                if batch_ops:
                    write_result = await db.stock_prices.bulk_write(
                        batch_ops, ordered=False,
                    )
                    rows_written_total = (
                        write_result.upserted_count + write_result.modified_count
                    )
                    logger.info(
                        f"[TICKER GAP REMEDIATION] Wrote {rows_written_total} rows "
                        f"({write_result.upserted_count} new, "
                        f"{write_result.modified_count} updated)"
                    )

    except Exception as exc:
        overall_status = "error"
        logger.error(f"[TICKER GAP REMEDIATION] Unhandled error: {exc}")
        await db.ops_job_runs.update_one(
            {"_id": run_doc_id},
            {"$set": {"details.error": str(exc)[:500]}},
        )

    finished_at = datetime.now(timezone.utc)
    await db.ops_job_runs.update_one(
        {"_id": run_doc_id},
        {"$set": {
            "status": overall_status,
            "finished_at": finished_at,
            "finished_at_prague": _to_prague_iso(finished_at),
            "log_timezone": "Europe/Prague",
            "details.pairs_attempted": pairs_attempted,
            "details.pairs_filled": pairs_filled,
            "details.pair_errors": pair_errors,
            "details.rows_written": rows_written_total,
            "details.skipped_pairs": skipped_pairs,
        }},
    )

    logger.info(
        f"[TICKER GAP REMEDIATION] Done: status={overall_status}, "
        f"pairs_attempted={pairs_attempted}, pairs_filled={pairs_filled}, "
        f"rows_written={rows_written_total}, "
        f"skipped={len(skipped_pairs)}"
    )

    return {
        "job_name": job_name,
        "status": overall_status,
        "pairs_attempted": pairs_attempted,
        "pairs_filled": pairs_filled,
        "pair_errors": pair_errors,
        "rows_written": rows_written_total,
        "skipped_pairs": skipped_pairs,
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
    }


# ── Single-ticker gap remediation ────────────────────────────────────────
# Fills ALL missing expected_dates for a specific ticker with per-date
# proof reporting.  No cap — all gaps are remediated.

def _normalize_ticker_for_remediation(value) -> Optional[str]:
    """Canonical ticker normalization: uppercase, trim, always .US suffix."""
    if value is None:
        return None
    text = str(value).strip().upper()
    if not text:
        return None
    if text.endswith(".US"):
        text = text[:-3]
    return f"{text}.US"


async def run_single_ticker_gap_remediation(
    db,
    ticker: str,
    *,
    bulk_check: bool = True,
) -> Dict[str, Any]:
    """
    Remediate ALL missing expected_dates for a single ticker.

    Uses the same ``_get_bulk_processed_dates`` as the gap-free metric so
    expected_dates are identical.  Returns a per-date report with proof
    data (bulk_found, db_before, inserted_count, db_after, skip_reason).

    Parameters
    ----------
    db : motor database
    ticker : str  – e.g. "AHH.US"
    bulk_check : bool – when True, fetch EODHD bulk for each missing date
        to populate ``bulk_found``.  Set to False to skip (saves API calls).

    Returns
    -------
    dict with: ticker, expected_dates_count, relevant_dates_count,
    missing_dates_count, per_date_report[], summary.
    """
    from services.admin_overview_service import _get_bulk_processed_dates
    from price_ingestion_service import (
        fetch_eod_history,
        parse_eod_record,
        fetch_bulk_eod_latest,
        _normalize_step2_ticker,
        _is_zero_or_missing_close,
        validate_price_row,
    )

    started_at = datetime.now(timezone.utc)
    job_name = "single_ticker_gap_remediation"
    normalized = _normalize_ticker_for_remediation(ticker)
    if not normalized:
        return {"error": "invalid_ticker", "ticker": ticker}

    # Insert running sentinel
    run_doc_id = (await db.ops_job_runs.insert_one({
        "job_name": job_name,
        "status": "running",
        "started_at": started_at,
        "started_at_prague": _to_prague_iso(started_at),
        "source": "scheduler",
        "details": {"ticker": normalized},
    })).inserted_id

    overall_status = "success"
    per_date_report: List[Dict[str, Any]] = []
    total_inserted = 0
    expected_dates: List[str] = []
    relevant_dates: List[str] = []
    missing_dates: List[str] = []

    try:
        # 1) Get expected dates (same source as gap-free metric)
        expected_dates = await _get_bulk_processed_dates(db)
        if not expected_dates:
            logger.info(
                "[SINGLE TICKER GAP REMEDIATION] No expected dates — nothing to do"
            )
            return {
                "job_name": job_name,
                "ticker": normalized,
                "status": "success",
                "expected_dates_count": 0,
                "relevant_dates_count": 0,
                "missing_dates_count": 0,
                "per_date_report": [],
                "summary": "no_expected_dates",
            }

        # 2) Get ticker's proven anchor
        ticker_doc = await db.tracked_tickers.find_one(
            {"ticker": normalized},
            {
                "ticker": 1,
                "history_download_proven_at": 1,
                "history_download_proven_anchor": 1,
                "is_visible": 1,
                "is_seeded": 1,
                "_id": 0,
            },
        )
        if not ticker_doc:
            return {
                "job_name": job_name,
                "ticker": normalized,
                "status": "success",
                "error": "ticker_not_found_in_tracked_tickers",
                "expected_dates_count": len(expected_dates),
                "relevant_dates_count": 0,
                "missing_dates_count": 0,
                "per_date_report": [],
            }

        anchor = ticker_doc.get("history_download_proven_anchor")
        if not anchor:
            return {
                "job_name": job_name,
                "ticker": normalized,
                "status": "success",
                "error": "ticker_not_proven",
                "expected_dates_count": len(expected_dates),
                "relevant_dates_count": 0,
                "missing_dates_count": 0,
                "per_date_report": [],
            }

        # 3) Determine relevant dates (after anchor) and find missing ones
        relevant_dates = sorted(d for d in expected_dates if d > anchor)

        existing_dates: set = set()
        if relevant_dates:
            cursor = db.stock_prices.find(
                {"ticker": normalized, "date": {"$in": relevant_dates}},
                {"date": 1, "_id": 0},
            )
            async for doc in cursor:
                existing_dates.add(doc["date"])

        missing_dates = [d for d in relevant_dates if d not in existing_dates]

        logger.info(
            "[SINGLE TICKER GAP REMEDIATION] ticker=%s anchor=%s "
            "expected=%d relevant=%d missing=%d",
            normalized, anchor, len(expected_dates),
            len(relevant_dates), len(missing_dates),
        )

        # 4) Remediate each missing date with per-date proof
        for target_date in missing_dates:
            report: Dict[str, Any] = {
                "date": target_date,
                "bulk_found": None,
                "bulk_matched_symbol": None,
                "db_before": False,
                "inserted_count": 0,
                "db_after": False,
                "skip_reason": None,
            }

            # ── 4a. Bulk check (optional) ────────────────────────────
            if bulk_check:
                try:
                    bulk_data, _ = await fetch_bulk_eod_latest(
                        "US", include_meta=True, for_date=target_date,
                    )
                    if not bulk_data:
                        # fetch_bulk_eod_latest swallows errors and
                        # returns [].  EODHD bulk for US has 50,000+
                        # rows on any trading day; an empty result
                        # means the fetch failed, NOT that the ticker
                        # is absent.  Mark as unknown.
                        report["bulk_found"] = None
                        logger.warning(
                            "[SINGLE TICKER GAP REMEDIATION] bulk fetch "
                            "returned empty for %s %s — treating as "
                            "unknown (possible API failure)",
                            normalized, target_date,
                        )
                    else:
                        found = False
                        for record in bulk_data:
                            raw_sym = record.get("code") or record.get("symbol")
                            if raw_sym is None:
                                continue
                            norm = _normalize_step2_ticker(str(raw_sym))
                            if norm == normalized:
                                found = True
                                report["bulk_matched_symbol"] = str(raw_sym)
                                break
                        report["bulk_found"] = found
                except Exception as exc:
                    logger.warning(
                        "[SINGLE TICKER GAP REMEDIATION] bulk check error "
                        "for %s %s: %s", normalized, target_date, exc,
                    )
                    report["bulk_found"] = None  # unknown

            # ── 4b. Try per-ticker EODHD API ─────────────────────────
            inserted_count = 0
            try:
                records = await fetch_eod_history(
                    normalized, from_date=target_date, to_date=target_date,
                )
                if records:
                    ops = []
                    for record in records:
                        # Reject zero-price records
                        if _is_zero_or_missing_close(record.get("close")):
                            report["skip_reason"] = "api_returned_zero_price"
                            continue
                        parsed = parse_eod_record(normalized, record)
                        if not validate_price_row(parsed):
                            report["skip_reason"] = "parse_failed_invalid_row"
                            logger.warning(
                                "[SINGLE TICKER GAP REMEDIATION] %s %s: "
                                "invalid row (missing ticker/date/close)",
                                normalized, target_date,
                            )
                            continue
                        ops.append(
                            UpdateOne(
                                {"ticker": parsed["ticker"], "date": parsed["date"]},
                                {"$set": parsed},
                                upsert=True,
                            )
                        )
                    if ops:
                        write_result = await db.stock_prices.bulk_write(
                            ops, ordered=False,
                        )
                        inserted_count = (
                            write_result.upserted_count + write_result.modified_count
                        )
                else:
                    # Per-ticker API returned no records
                    if report["bulk_found"] is True:
                        report["skip_reason"] = "not_in_per_ticker_api_but_in_bulk"
                    elif report["bulk_found"] is False:
                        report["skip_reason"] = "not_in_bulk_data"
                    elif bulk_check:
                        # Bulk was checked but returned unknown (empty
                        # or errored) — cannot blame the ticker.
                        report["skip_reason"] = "bulk_fetch_returned_empty"
                    else:
                        report["skip_reason"] = "api_returned_no_records"
                    logger.info(
                        "[SINGLE TICKER GAP REMEDIATION] %s %s: %s",
                        normalized, target_date, report["skip_reason"],
                    )
            except Exception as exc:
                report["skip_reason"] = f"write_failed: {type(exc).__name__}"
                logger.warning(
                    "[SINGLE TICKER GAP REMEDIATION] %s %s write error: %s",
                    normalized, target_date, exc,
                )

            report["inserted_count"] = inserted_count
            total_inserted += inserted_count

            # ── 4c. Bulk-data fallback ───────────────────────────────
            # If per-ticker API returned nothing but bulk has the data,
            # extract the row from bulk and insert it directly.
            # Skip if bulk close is zero (halted/delisted).
            if (
                inserted_count == 0
                and bulk_check
                and report["bulk_found"] is True
                and report.get("skip_reason") == "not_in_per_ticker_api_but_in_bulk"
            ):
                try:
                    # Re-fetch bulk for this date (already done above but
                    # we need the actual row).  We search the cached bulk_data.
                    for record in bulk_data:
                        raw_sym = record.get("code") or record.get("symbol")
                        if raw_sym is None:
                            continue
                        norm = _normalize_step2_ticker(str(raw_sym))
                        if norm == normalized:
                            # Reject zero-price bulk row
                            if _is_zero_or_missing_close(record.get("close")):
                                report["skip_reason"] = (
                                    "bulk_found_but_close_is_zero"
                                )
                                logger.info(
                                    "[SINGLE TICKER GAP REMEDIATION] %s %s: "
                                    "bulk fallback skipped — close=0",
                                    normalized, target_date,
                                )
                                break
                            parsed = parse_eod_record(normalized, record)
                            if validate_price_row(parsed):
                                wr = await db.stock_prices.bulk_write(
                                    [UpdateOne(
                                        {"ticker": parsed["ticker"], "date": parsed["date"]},
                                        {"$set": parsed},
                                        upsert=True,
                                    )],
                                    ordered=False,
                                )
                                fallback_count = wr.upserted_count + wr.modified_count
                                report["inserted_count"] = fallback_count
                                total_inserted += fallback_count
                                report["skip_reason"] = (
                                    "resolved_via_bulk_fallback"
                                    if fallback_count > 0
                                    else report["skip_reason"]
                                )
                                logger.info(
                                    "[SINGLE TICKER GAP REMEDIATION] %s %s: "
                                    "bulk fallback inserted %d",
                                    normalized, target_date, fallback_count,
                                )
                            break
                except Exception as exc:
                    logger.warning(
                        "[SINGLE TICKER GAP REMEDIATION] %s %s bulk fallback "
                        "error: %s", normalized, target_date, exc,
                    )
                    report["skip_reason"] = (
                        f"bulk_fallback_write_failed: {type(exc).__name__}"
                    )

            # ── 4d. DB-after check ───────────────────────────────────
            after_doc = await db.stock_prices.find_one(
                {"ticker": normalized, "date": target_date},
                {"_id": 1},
            )
            report["db_after"] = after_doc is not None

            # Final skip_reason classification
            if report["inserted_count"] == 0 and not report["skip_reason"]:
                if report["bulk_found"] is True:
                    report["skip_reason"] = "bulk_found_but_insert_yielded_zero"
                elif report["bulk_found"] is False:
                    report["skip_reason"] = "not_in_bulk_data"
                elif report["bulk_found"] is None and bulk_check:
                    report["skip_reason"] = "bulk_fetch_returned_empty"
                else:
                    report["skip_reason"] = "unknown"

            # ── Persist gap-free exclusion for NOT-APPLICABLE cases ──
            _NA_SKIP = {
                "not_in_bulk_data",
                "bulk_found_but_close_is_zero",
                "bulk_close_zero_api_returned_empty",
                "api_returned_only_zero_price",
            }
            if (
                report["inserted_count"] == 0
                and report.get("skip_reason") in _NA_SKIP
            ):
                try:
                    await db.gap_free_exclusions.update_one(
                        {"ticker": normalized, "date": target_date},
                        {"$set": {
                            "ticker": normalized,
                            "date": target_date,
                            "reason": report["skip_reason"],
                            "bulk_found": report["bulk_found"],
                            "updated_at": datetime.now(timezone.utc),
                        }},
                        upsert=True,
                    )
                except Exception as exc_excl:
                    logger.warning(
                        "[SINGLE TICKER GAP REMEDIATION] "
                        "gap_free_exclusions upsert error %s/%s: %s",
                        normalized, target_date, exc_excl,
                    )

            per_date_report.append(report)

    except Exception as exc:
        overall_status = "error"
        logger.error(
            "[SINGLE TICKER GAP REMEDIATION] Unhandled error for %s: %s",
            normalized, exc,
        )

    finished_at = datetime.now(timezone.utc)
    await db.ops_job_runs.update_one(
        {"_id": run_doc_id},
        {"$set": {
            "status": overall_status,
            "finished_at": finished_at,
            "finished_at_prague": _to_prague_iso(finished_at),
            "log_timezone": "Europe/Prague",
            "details.expected_dates_count": len(expected_dates),
            "details.relevant_dates_count": len(relevant_dates),
            "details.missing_dates_count": len(missing_dates),
            "details.total_inserted": total_inserted,
            "details.per_date_report": per_date_report,
        }},
    )

    result = {
        "job_name": job_name,
        "ticker": normalized,
        "status": overall_status,
        "expected_dates_count": len(expected_dates),
        "relevant_dates_count": len(relevant_dates),
        "missing_dates_count": len(missing_dates),
        "total_inserted": total_inserted,
        "per_date_report": per_date_report,
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
    }

    logger.info(
        "[SINGLE TICKER GAP REMEDIATION] Done: ticker=%s status=%s "
        "missing=%d inserted=%d",
        normalized, overall_status,
        len(missing_dates),
        total_inserted,
    )

    return result


async def remediate_gap_date(
    db,
    target_date: str,
) -> Dict[str, Any]:
    """
    Find ALL tickers missing a stock_prices row for ``target_date`` (a
    completed bulk trading day) and remediate them in batch.

    Steps:
    1. Verify target_date is in expected_dates (completed bulk day).
    2. Identify gap tickers: proven + visible, anchor < target_date, no
       stock_prices row for target_date.
    3. Pre-fetch EODHD bulk data for target_date ONCE.
    4. For each gap ticker, attempt insert from bulk data, then per-ticker
       API fallback.
    5. Return a proof table with per-ticker results.

    Returns
    -------
    dict with: target_date, gap_tickers_count, proof_table[], summary.
    """
    from services.admin_overview_service import _get_bulk_processed_dates
    from price_ingestion_service import (
        fetch_eod_history,
        parse_eod_record,
        fetch_bulk_eod_latest,
        _normalize_step2_ticker,
        _is_zero_or_missing_close,
        validate_price_row,
    )
    from pymongo import UpdateOne

    started_at = datetime.now(timezone.utc)
    job_name = "remediate_gap_date"

    # 1) Verify target_date is a completed bulk day
    expected_dates = await _get_bulk_processed_dates(db)
    if target_date not in expected_dates:
        return {
            "job_name": job_name,
            "target_date": target_date,
            "status": "skipped",
            "reason": "target_date_not_in_expected_dates",
            "expected_dates_count": len(expected_dates),
            "expected_dates_sample": expected_dates[:10],
        }

    # 2) Find gap tickers for this date
    proven_docs = await db.tracked_tickers.find(
        {
            "is_visible": True,
            "history_download_proven_at": {"$exists": True, "$type": "date"},
            "history_download_proven_anchor": {"$exists": True, "$ne": None},
        },
        {"ticker": 1, "history_download_proven_anchor": 1, "_id": 0},
    ).to_list(None)

    # Filter to tickers whose anchor is before target_date
    candidates = {
        doc["ticker"]: doc["history_download_proven_anchor"]
        for doc in proven_docs
        if doc.get("history_download_proven_anchor")
        and doc["history_download_proven_anchor"] < target_date
    }
    if not candidates:
        return {
            "job_name": job_name,
            "target_date": target_date,
            "status": "success",
            "gap_tickers_count": 0,
            "proof_table": [],
            "summary": "no_candidates",
        }

    # Find which candidate tickers already have data for target_date
    existing_cursor = db.stock_prices.find(
        {"ticker": {"$in": list(candidates.keys())}, "date": target_date},
        {"ticker": 1, "_id": 0},
    )
    existing_tickers: set = set()
    async for doc in existing_cursor:
        existing_tickers.add(doc["ticker"])

    gap_tickers = sorted(
        t for t in candidates if t not in existing_tickers
    )
    if not gap_tickers:
        return {
            "job_name": job_name,
            "target_date": target_date,
            "status": "success",
            "gap_tickers_count": 0,
            "proof_table": [],
            "summary": "all_tickers_already_have_data",
        }

    # 3) Pre-fetch EODHD bulk data ONCE
    bulk_data: list = []
    bulk_error: Optional[str] = None
    try:
        bulk_data, _ = await fetch_bulk_eod_latest(
            "US", include_meta=True, for_date=target_date,
        )
        if not bulk_data:
            bulk_error = "bulk_fetch_returned_empty_payload"
    except Exception as exc:
        bulk_error = f"{type(exc).__name__}: bulk fetch failed"
        logger.warning("[REMEDIATE GAP DATE] Bulk fetch error: %s", exc)

    # Build normalized bulk lookup
    bulk_ticker_field: Optional[str] = None
    if bulk_data:  # truthy = non-empty list, safe to index
        sample = bulk_data[0]
        if isinstance(sample, dict):
            if "code" in sample:
                bulk_ticker_field = "code"
            elif "symbol" in sample:
                bulk_ticker_field = "symbol"

    normalized_bulk_map: Dict[str, Dict[str, Any]] = {}
    if bulk_ticker_field:
        for record in bulk_data:
            raw_sym = record.get(bulk_ticker_field)
            if raw_sym is None:
                continue
            norm = _normalize_step2_ticker(str(raw_sym))
            if norm:
                normalized_bulk_map[norm] = record

    # 4) Remediate each gap ticker
    proof_table: List[Dict[str, Any]] = []
    total_inserted = 0

    for ticker in gap_tickers:
        row: Dict[str, Any] = {
            "ticker": ticker,
            "bulk_found": None,
            "matched_symbol": None,
            "bulk_row_snippet": None,
            "db_found_before": False,
            "db_found_after": False,
            "inserted": False,
            "primary_reason": None,
        }

        bulk_record: Optional[Dict[str, Any]] = None
        if bulk_error and not bulk_data:
            row["bulk_found"] = None
            row["primary_reason"] = "bulk_fetch_failed"
        else:
            # Check bulk
            bulk_record = normalized_bulk_map.get(ticker)
            if bulk_record:
                row["bulk_found"] = True
                row["matched_symbol"] = str(
                    bulk_record.get(bulk_ticker_field)
                )
                # Always include the raw snippet so the user can verify
                row["bulk_row_snippet"] = {
                    "code": bulk_record.get("code"),
                    "date": bulk_record.get("date"),
                    "open": bulk_record.get("open"),
                    "high": bulk_record.get("high"),
                    "low": bulk_record.get("low"),
                    "close": bulk_record.get("close"),
                    "adjusted_close": bulk_record.get("adjusted_close"),
                    "volume": bulk_record.get("volume"),
                }
            else:
                row["bulk_found"] = False

        # ── Close-price sanity: reject zero/None close ───────────────
        # EODHD sometimes includes halted/delisted tickers with all-zero
        # prices.  Writing them creates garbage stock_prices rows.
        bulk_close_is_zero = False
        if bulk_record:
            if _is_zero_or_missing_close(bulk_record.get("close")):
                bulk_close_is_zero = True

        # Try to insert from bulk data first (fastest)
        inserted = False
        if row["bulk_found"] is True and bulk_record and not bulk_close_is_zero:
            try:
                parsed = parse_eod_record(ticker, bulk_record)
                if validate_price_row(parsed):
                    wr = await db.stock_prices.bulk_write(
                        [UpdateOne(
                            {"ticker": parsed["ticker"],
                             "date": parsed["date"]},
                            {"$set": parsed},
                            upsert=True,
                        )],
                        ordered=False,
                    )
                    if wr.upserted_count + wr.modified_count > 0:
                        inserted = True
                        total_inserted += 1
                        row["primary_reason"] = "resolved_from_bulk"
            except Exception as exc:
                logger.warning(
                    "[REMEDIATE GAP DATE] Bulk insert error for %s: %s",
                    ticker, exc,
                )
                row["primary_reason"] = (
                    f"bulk_insert_error: {type(exc).__name__}"
                )
        elif row["bulk_found"] is True and bulk_close_is_zero:
            row["primary_reason"] = "bulk_found_but_close_is_zero"

        # Fallback: per-ticker API
        if not inserted:
            try:
                records = await fetch_eod_history(
                    ticker, from_date=target_date, to_date=target_date,
                )
                if records:
                    ops = []
                    for record in records:
                        # Reject zero-price records from per-ticker API too
                        if _is_zero_or_missing_close(record.get("close")):
                            continue
                        parsed = parse_eod_record(ticker, record)
                        if validate_price_row(parsed):
                            ops.append(
                                UpdateOne(
                                    {"ticker": parsed["ticker"],
                                     "date": parsed["date"]},
                                    {"$set": parsed},
                                    upsert=True,
                                )
                            )
                    if ops:
                        wr = await db.stock_prices.bulk_write(
                            ops, ordered=False,
                        )
                        count = wr.upserted_count + wr.modified_count
                        if count > 0:
                            inserted = True
                            total_inserted += count
                            row["primary_reason"] = (
                                "resolved_from_per_ticker_api"
                            )
                        elif not row["primary_reason"]:
                            row["primary_reason"] = (
                                "api_returned_only_zero_price"
                            )
                    elif not row["primary_reason"]:
                        row["primary_reason"] = (
                            "api_returned_only_zero_price"
                        )
                elif not row["primary_reason"]:
                    if row["bulk_found"] is True:
                        row["primary_reason"] = (
                            "bulk_close_zero_api_returned_empty"
                            if bulk_close_is_zero
                            else "in_bulk_but_api_returned_empty"
                        )
                    elif row["bulk_found"] is False:
                        row["primary_reason"] = "not_in_bulk_not_in_api"
                    else:
                        row["primary_reason"] = "bulk_unknown_api_empty"
            except Exception as exc:
                if not row["primary_reason"]:
                    row["primary_reason"] = (
                        f"per_ticker_api_error: {type(exc).__name__}"
                    )
                logger.warning(
                    "[REMEDIATE GAP DATE] Per-ticker API error for %s: %s",
                    ticker, exc,
                )

        # DB-after check
        after_doc = await db.stock_prices.find_one(
            {"ticker": ticker, "date": target_date}, {"_id": 1},
        )
        row["db_found_after"] = after_doc is not None
        row["inserted"] = inserted

        if inserted and not row["primary_reason"]:
            row["primary_reason"] = "resolved"
        elif not inserted and not row["primary_reason"]:
            row["primary_reason"] = "unknown"

        # ── Persist gap-free exclusion for NOT-APPLICABLE cases ──────
        # If the ticker is legitimately absent (not in bulk or close=0),
        # record this so the gap-free metric excludes it.
        if not inserted and row["primary_reason"] in _NOT_APPLICABLE_REASONS:
            try:
                await db.gap_free_exclusions.update_one(
                    {"ticker": ticker, "date": target_date},
                    {"$set": {
                        "ticker": ticker,
                        "date": target_date,
                        "reason": row["primary_reason"],
                        "bulk_found": row["bulk_found"],
                        "updated_at": datetime.now(timezone.utc),
                    }},
                    upsert=True,
                )
            except Exception as exc:
                logger.warning(
                    "[REMEDIATE GAP DATE] gap_free_exclusions upsert error "
                    "for %s/%s: %s", ticker, target_date, exc,
                )

        proof_table.append(row)

    finished_at = datetime.now(timezone.utc)

    # Write ops_job_runs record
    await db.ops_job_runs.insert_one({
        "job_name": job_name,
        "status": "success",
        "started_at": started_at,
        "started_at_prague": _to_prague_iso(started_at),
        "finished_at": finished_at,
        "finished_at_prague": _to_prague_iso(finished_at),
        "log_timezone": "Europe/Prague",
        "source": "scheduler",
        "details": {
            "target_date": target_date,
            "gap_tickers_count": len(gap_tickers),
            "total_inserted": total_inserted,
            "proof_table": proof_table,
        },
    })

    return {
        "job_name": job_name,
        "target_date": target_date,
        "status": "success",
        "gap_tickers_count": len(gap_tickers),
        "total_inserted": total_inserted,
        "bulk_raw_row_count": len(bulk_data),
        "bulk_error": bulk_error,
        "proof_table": proof_table,
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
    }
