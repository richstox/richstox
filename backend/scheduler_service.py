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
CANCEL_REQUESTED_STUCK_SECONDS = 600
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
    api_url = f"{EODHD_BASE_URL}/{endpoint}?type=splits&date={today_str}"

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
    api_url = f"{EODHD_BASE_URL}/{endpoint}?type=dividends&date={today_str}"

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
    api_url = f"{EODHD_BASE_URL}/{endpoint}?from={start}&to={today_str}"

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
        "api_endpoint": split_endpoints[0] if split_endpoints else f"{EODHD_BASE_URL}/eod-bulk-last-day/US?type=splits&date={today_str}",
        "api_endpoints_all": split_endpoints,
        "dates_checked": processed_dates,
        "raw_count": split_raw_total,
        "universe_count": len(split_all_in_universe),
        "flagged_count": split_flagged_total,
        "tickers": split_all_in_universe[:50],
    }
    dividend = {
        "mock_mode": not bool(EODHD_API_KEY),
        "api_endpoint": div_endpoints[0] if div_endpoints else f"{EODHD_BASE_URL}/eod-bulk-last-day/US?type=dividends&date={today_str}",
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
                "min_bulk_rows_ok": min_bulk_rows_ok,
                "min_matched_seeded_tickers_ok": min_matched_seeded_tickers_ok,
                "sanity_threshold_used": sanity_threshold_used,
                "bulk_url_used": "https://eodhd.com/api/eod-bulk-last-day/US",
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
            "min_bulk_rows_ok": min_bulk_rows_ok,
            "min_matched_seeded_tickers_ok": min_matched_seeded_tickers_ok,
            "sanity_threshold_used": sanity_threshold_used,
            "bulk_url_used": "https://eodhd.com/api/eod-bulk-last-day/US",
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
            "bulk_url_used": "https://eodhd.com/api/eod-bulk-last-day/US",
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
                "2.1 Fetching latest prices (bulk)…",
                processed=0,
                total=progress_total_step2,
                phase="2.1_bulk_catchup",
            )
        for idx, _ in enumerate(bulk_attempts):
            await _progress(
                f"2.1 Bulk gapfill {idx + 1}/{len(bulk_attempts)} (provider latest available day)…",
                phase="2.1_bulk_catchup",
            )
            should_append_day = True
            day: Dict[str, Any] = {
                "bulk_date": None,
                "processed_date": None,
                "unique_dates": [],
                "bulk_url_used": "https://eodhd.com/api/eod-bulk-last-day/US",
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
                )
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
                day["processed_date"] = (
                    day_result.get("processed_date")
                    or day_result.get("date")
                    or (
                        target_end_date.strftime("%Y-%m-%d")
                        if day_bulk_fetch_executed
                        else None
                    )
                )
                day["bulk_date"] = day["processed_date"]
                day["unique_dates"] = day_result.get("unique_dates", [])
                result["bulk_writes"] += day_result.get("bulk_writes", 0)

                if len(day["unique_dates"]) != 1:
                    day["status"] = "error"
                    day["error"] = f"bulk payload must contain exactly one date; got {day['unique_dates']}"
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
        logger.error(f"{job_name} failed: {error_msg}")

        _fail_finished_at = datetime.now(timezone.utc)
        await db.ops_job_runs.update_one(
            {"_id": _running_doc_id},
            {"$set": {
                "status": "failed",
                "finished_at": _fail_finished_at,
                "finished_at_prague": _to_prague_iso(_fail_finished_at),
                "error": error_msg,
                "details.parent_run_id": parent_run_id,
                "details.chain_run_id": chain_run_id,
            }},
        )

        return {
            "job_name": job_name,
            "status": "failed",
            "error": error_msg,
            "exclusion_report_run_id": None,
            "records_upserted": 0,
            "dates_processed": 0,
            "started_at": started_at.isoformat(),
            "finished_at": _fail_finished_at.isoformat(),
        }


async def sync_has_price_data_flags(db, include_exclusions: bool = False, tickers_with_price: Optional[List[str]] = None) -> Dict[str, Any]:
    """
    Recompute has_price_data for seeded US Common Stock universe.

    When *tickers_with_price* is provided (from the bulk feed result), only
    those tickers are marked has_price_data=True — no stock_prices query needed.
    Fallback: query stock_prices for valid prices (close > 0 OR adjusted_close > 0).
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
        base = {
            "seeded_total": 0,
            "with_price_data": 0,
            "without_price_data": 0,
            "matched_price_tickers_raw": 0,
        }
        if include_exclusions:
            base["exclusions"] = []
        return base

    seeded_set = set(seeded_tickers)
    from price_ingestion_service import _normalize_step2_ticker

    if tickers_with_price is not None:
        # ── Fast path: use the set returned by the bulk feed ─────────────
        with_price_set = {
            _normalize_step2_ticker(t)
            for t in tickers_with_price
            if _normalize_step2_ticker(t)
        } & seeded_set
        any_price_set = with_price_set  # same set when sourced from bulk feed
        matched_raw = len(with_price_set)
    else:
        # ── Legacy fallback: query stock_prices collection ───────────────
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
        with_price_set = normalized_price_tickers & seeded_set
        any_price_set = normalized_any_price_tickers & seeded_set
        matched_raw = len(with_price_set)

    # Reset all seeded tickers to false, then enable only those with prices.
    await db.tracked_tickers.update_many(
        {"ticker": {"$in": seeded_tickers}},
        {"$set": {"has_price_data": False, "updated_at": datetime.now(timezone.utc)}},
    )
    if with_price_set:
        await db.tracked_tickers.update_many(
            {"ticker": {"$in": list(with_price_set)}},
            {"$set": {"has_price_data": True, "updated_at": datetime.now(timezone.utc)}},
        )

    with_price_data = len(with_price_set)
    summary = {
        "seeded_total": seeded_total,
        "with_price_data": with_price_data,
        "without_price_data": max(seeded_total - with_price_data, 0),
        "matched_price_tickers_raw": matched_raw,
    }
    if include_exclusions:
        exclusions: List[Dict[str, Any]] = []
        for ticker in sorted(seeded_set - with_price_set):
            reason = (
                "Close/adjusted_close missing or zero"
                if ticker in any_price_set
                else "Ticker not present in price data"
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
    return running_zombie_result.modified_count + cancel_zombie_result.modified_count


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
        while not _heartbeat_stop.is_set():
            await asyncio.sleep(FUNDAMENTALS_SYNC_HEARTBEAT_SECONDS)
            heartbeat_now = datetime.now(timezone.utc)
            heartbeat_result = await db.ops_job_runs.update_one(
                {"_id": _running_doc_id, "status": "running"},
                {"$set": {
                    "heartbeat_at": heartbeat_now,
                    **_details_updated_fields(heartbeat_now),
                }},
            )
            if heartbeat_result.modified_count == 0:
                # Exit if the run is no longer active (externally finalized/status changed).
                logger.info(f"{job_name}: heartbeat worker stopped (run no longer active)")
                break
            await _heartbeat_fundamentals_sync_lock(db, _lock_owner_run_id, heartbeat_now)

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
        await db.ops_job_runs.update_one(
            {"_id": _running_doc_id},
            {"$set": {
                "details.step3_telemetry": step3_telemetry,
                "progress": progress_msg,
                "progress_processed": progress_processed,
                "progress_total": progress_total,
                "progress_pct": progress_pct,
                "heartbeat_at": now_utc,
                **_details_updated_fields(now_utc),
            }},
        )
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
                # Fast in-memory check — no DB round-trip
                if cancel_event.is_set() or await _is_cancelled():
                    cancel_event.set()
                    return {"ticker": ticker, "success": False, "cancelled": True}
                async with semaphore:
                    return await sync_single_ticker_fundamentals(db, ticker, source_job=job_name)

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

        _phase_update(
            "A",
            status="done" if result.get("status") != "cancelled" else "error",
            processed=result.get("processed", 0),
            total=len(tickers_to_sync) if tickers_to_sync else 0,
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
            _phase_c_cursor = db.tracked_tickers.find(
                {
                    **STEP3_QUERY,
                    "is_visible": True,
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
        logger.error(f"{job_name} failed: {error_msg}")

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
                "status": "failed",
                "finished_at": finished_ts,
                "started_at_prague": _to_prague_iso(started_at),
                "finished_at_prague": _to_prague_iso(finished_ts),
                "log_timezone": "Europe/Prague",
                "error": error_msg,
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
            "status": "failed",
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
