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
- 23:00 (Mon-Sat): Step 1 Universe Seed
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

logger = logging.getLogger("richstox.scheduler")

# Constants
SCHEDULER_CONFIG_KEY = "scheduler_enabled"
SEED_QUERY = {"exchange": {"$in": ["NYSE", "NASDAQ"]}, "asset_type": "Common Stock"}
# Canonical Step 3 universe — tickers that are seeded and have price data.
# This is the exact filter used by universe_counts_service step3_query and
# is the source of truth for "Tickers with prices" on the Step 3 pipeline card.
STEP3_QUERY = {**SEED_QUERY, "has_price_data": True}
PRAGUE_TZ = ZoneInfo("Europe/Prague")
STEP2_REPORT_STEP = "Step 2 - Price Sync"
EVENTS_WATERMARK_KEY = "last_events_checked_date"
REMEDIATION_WATCHDOG_TIMEOUT_SECONDS = 300
REMEDIATION_HEARTBEAT_SECONDS = 10

EODHD_BASE_URL = "https://eodhd.com/api"
EODHD_API_KEY = os.getenv("EODHD_API_KEY", "")
_FUNDAMENTALS_EVENTS_INDEX_DONE = False
_FUNDAMENTALS_EVENTS_INDEX_LOCK = asyncio.Lock()


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
    if _FUNDAMENTALS_EVENTS_INDEX_DONE:
        return
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
    """
    await _ensure_fundamentals_events_index(db)

    normalized = sorted({t for t in tickers if t})
    if not normalized:
        return {"inserted": 0, "already_pending": 0, "new_inserts": 0, "skipped_existing": 0}

    ops: List[UpdateOne] = []
    for ticker in normalized:
        ops.append(
            UpdateOne(
                {
                    "ticker": ticker,
                    "event_type": event_type,
                    "status": "pending",
                },
                {
                    "$setOnInsert": {
                        "ticker": ticker,
                        "event_type": event_type,
                        "status": "pending",
                        "source": source_job,
                        # Legacy field kept for compatibility; remove after downstream readers migrate.
                        "source_job": source_job,
                        "detector_step": detector_step,
                        "detected_date": detected_date,
                        "created_at": now,
                    },
                    "$set": {
                        "detected_date": detected_date,
                        "source": source_job,
                        # Preserve legacy source_job field for readers expecting it (pending deprecation).
                        "source_job": source_job,
                        "detector_step": detector_step,
                        "updated_at": now,
                    },
                },
                upsert=True,
            )
        )

    result = await db.fundamentals_events.bulk_write(ops, ordered=False)
    new_inserts = len(getattr(result, "upserted_ids", {}))
    skipped_existing = len(normalized) - new_inserts

    return {
        "inserted": new_inserts,
        "already_pending": skipped_existing,
        "new_inserts": new_inserts,
        "skipped_existing": skipped_existing,
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
            {"ticker": {"$in": tickers_us}},
            {"$set": {
                "needs_price_redownload": True,
                "needs_fundamentals_refresh": True,
                "price_history_complete": False,
                "price_history_status": "pending",
                "last_split_detected": today_str,
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
            {"ticker": {"$in": tickers_us}},
            {"$set": {
                "needs_fundamentals_refresh": True,
                "needs_price_redownload": True,
                "price_history_complete": False,
                "price_history_status": "pending",
                "last_dividend_detected": today_str,
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
                    await db.tracked_tickers.update_one(
                        {"ticker": ticker_us},
                        {"$set": {
                            "needs_price_redownload": False,
                            "price_history_complete": True,
                            "price_history_status": "complete",
                            "updated_at": now,
                        }},
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

        await _p(f"2.4 Dividend detector: calling EODHD for {date_str} ({i+1}/{len(missed_dates)})…")
        d = await _detect_dividend_candidates_eodhd(db, date_str)
        div_raw_total += d.get("raw_count", 0)
        div_all_in_universe.extend(d.get("tickers", []))
        div_flagged_total += d.get("flagged_count", 0)
        if d.get("api_endpoint"):
            div_endpoints.append(d["api_endpoint"])

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
                if step in ("2.2", "2.4"):
                    price_redownload_tickers.extend(tickers)

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
    run_doc_id: Optional[Any] = None,
    cancel_check: Optional[Callable[[], Awaitable[bool]]] = None,
) -> Dict[str, Any]:
    """
    Run daily price sync job with GAP DETECTION AND BULK CATCHUP.

    Phases:
    - Phase A: bulk last-day catchup → set has_price_data flags; track progress.
    - Phase B: detect splits/dividends/earnings since last check → set needs_* flags.
    - Phase C: download adjusted price history for split/dividend tickers.

    Config read from ops_config:
    - lookback_days (default: 30)
    - coverage_threshold (default: 0.80)

    run_doc_id: if provided, reuse an externally-inserted ops_job_runs sentinel
    instead of creating a new one (used by the admin full-pipeline chain).
    cancel_check: optional async callable that returns True if cancellation was
    requested (used by the full pipeline chain orchestrator).
    """
    from price_ingestion_service import run_daily_bulk_catchup

    started_at = datetime.now(timezone.utc)
    job_name = "price_sync"

    logger.info(f"Starting {job_name} with gap detection and bulk catchup")

    # Use an externally-inserted sentinel (chain orchestrator) or create our own.
    if run_doc_id is not None:
        _running_doc_id = run_doc_id
    else:
        # Insert "running" sentinel so the frontend poll detects the job started
        _running_doc_id = (await db.ops_job_runs.insert_one({
            "job_name": job_name,
            "status": "running",
            "started_at": started_at,
            "source": "scheduler",
            "details": {"parent_run_id": parent_run_id},
            "phase": "bulk_catchup",
            "progress_processed": 0,
            "progress_total": 0,
            "progress_pct": 0,
        })).inserted_id

    async def _is_cancelled() -> bool:
        """Check both chain cancel_check and per-job cancel flag."""
        if cancel_check and await cancel_check():
            return True
        flag = await db.ops_config.find_one({"key": f"cancel_job_{job_name}"})
        if flag:
            await db.ops_config.delete_one({"key": f"cancel_job_{job_name}"})
            return True
        return False

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
            await db.ops_job_runs.update_one(
                {"_id": _running_doc_id}, {"$set": {"status": "cancelled"}}
            )
            return {
                "job_name": job_name,
                "status": "cancelled",
                "exclusion_report_run_id": None,
                "started_at": started_at.isoformat(),
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
        # Determine progress_total for Step 2: prefer seeded_total from the
        # parent universe_seed run (chain-triggered), fall back to live DB count.
        seeded_total_from_parent: Optional[int] = None
        if parent_run_id:
            try:
                _parent_doc = await db.ops_job_runs.find_one(
                    {
                        "job_name": "universe_seed",
                        "details.exclusion_report_run_id": parent_run_id,
                    },
                    {"details.seeded_total": 1, "result.seeded_total": 1},
                )
                if _parent_doc:
                    _d = (_parent_doc.get("details") or {})
                    _r = (_parent_doc.get("result") or {})
                    _val = _d.get("seeded_total") if "seeded_total" in _d else _r.get("seeded_total")
                    if _val is not None:
                        seeded_total_from_parent = int(_val)
            except Exception as _lookup_exc:
                logger.warning(
                    f"{job_name}: parent universe_seed lookup failed: {_lookup_exc}"
                )

        db_count_fallback = await db.tracked_tickers.count_documents(SEED_QUERY)

        if seeded_total_from_parent is not None:
            progress_total_step2 = seeded_total_from_parent
            if seeded_total_from_parent != db_count_fallback:
                logger.warning(
                    f"{job_name}: seeded_total from parent run "
                    f"({seeded_total_from_parent}) differs from current DB count "
                    f"({db_count_fallback}); using parent value for progress_total"
                )
        else:
            progress_total_step2 = db_count_fallback

        await _progress(
            "2.1 Detecting price gaps (last 30 days)…",
            processed=0,
            total=progress_total_step2,
            phase="bulk_catchup",
        )

        # Run the bulk catchup with gap detection
        result = await run_daily_bulk_catchup(db)

        await _progress(
            f"2.1 Prices synced: {result.get('records_upserted', 0)} records "
            f"across {result.get('dates_processed', 0)} date(s). "
            "Updating has_price_data flags…",
            phase="bulk_catchup",
        )

        # Canonical Step 2 behavior: update has_price_data flags after bulk ingest
        price_flag_summary = await sync_has_price_data_flags(db, include_exclusions=True)
        seeded_total = price_flag_summary["seeded_total"]
        with_price = price_flag_summary["with_price_data"]
        result["tickers_seeded_total"] = seeded_total
        result["tickers_with_price_data"] = with_price
        result["tickers_without_price_data"] = price_flag_summary["without_price_data"]
        result["matched_price_tickers_raw"] = price_flag_summary.get("matched_price_tickers_raw", 0)
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
            f"2.1 Done: {with_price} / {seeded_total} tickers with price data. "
            "Running 2.2 Split detector (EODHD API)…",
            processed=with_price,
            total=seeded_total,
            phase="bulk_catchup",
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
                        "tickers_seeded_total": seeded_total,
                        "tickers_with_price_data": with_price,
                        "exclusion_report_run_id": result.get("exclusion_report_run_id"),
                        "parent_run_id": parent_run_id,
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
            f"2.2 Running split/dividend/earnings detectors…",
            processed=with_price,
            total=seeded_total,
            phase="event_detection",
        )

        # Step 2.2 / 2.4 / 2.6 detectors -> enqueue fundamentals refresh events.
        event_detector_summary = await run_step2_event_detectors(
            db,
            progress_cb=lambda msg: _progress(
                msg, processed=with_price, total=seeded_total, phase="event_detection"
            ),
            exclusion_meta={
                "run_id": result.get("exclusion_report_run_id"),
                "report_date": result.get("exclusion_report_date"),
            },
            cancel_check=_is_cancelled,
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
                        "tickers_seeded_total": seeded_total,
                        "tickers_with_price_data": with_price,
                        "event_detectors": event_detector_summary,
                        "fundamentals_events_enqueued": result.get("fundamentals_events_enqueued", 0),
                        "fundamentals_events_enqueued_skipped_existing": result.get("fundamentals_events_enqueued_skipped_existing", 0),
                        "exclusion_report_run_id": result.get("exclusion_report_run_id"),
                        "parent_run_id": parent_run_id,
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
                    "config": result.get("config"),
                    "gap_analysis": result.get("gap_analysis"),
                    "dates_processed": result.get("dates_processed", 0),
                    "records_upserted": result.get("records_upserted", 0),
                    "tickers_seeded_total": result.get("tickers_seeded_total", 0),
                    "tickers_with_price_data": result.get("tickers_with_price_data", 0),
                    "tickers_without_price_data": result.get("tickers_without_price_data", 0),
                    "matched_price_tickers_raw": result.get("matched_price_tickers_raw", 0),
                    "exclusion_report_rows": result.get("exclusion_report_rows", 0),
                    "api_calls": result.get("api_calls", 0),
                    "bulk_writes": result.get("bulk_writes", 0),
                    "fundamentals_events_enqueued": result.get("fundamentals_events_enqueued", 0),
                    "fundamentals_events_enqueued_skipped_existing": result.get("fundamentals_events_enqueued_skipped_existing", 0),
                    "event_detectors": result.get("event_detectors", {}),
                    "exclusion_report_run_id": result.get("exclusion_report_run_id"),
                    "parent_run_id": parent_run_id,
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
            "tickers_seeded_total": result.get("tickers_seeded_total", 0),
            "tickers_with_price_data": result.get("tickers_with_price_data", 0),
            "tickers_without_price_data": result.get("tickers_without_price_data", 0),
            "matched_price_tickers_raw": result.get("matched_price_tickers_raw", 0),
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


async def sync_has_price_data_flags(db, include_exclusions: bool = False) -> Dict[str, Any]:
    """
    Recompute has_price_data for seeded US Common Stock universe from stock_prices.
    Step 2 canonical: has_price_data=true only for tickers with valid price
    (close > 0 OR adjusted_close > 0).
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

    # Backward compatibility: older stock_prices may contain ticker without .US suffix.
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

    def normalize_ticker(value: str) -> str:
        if not value:
            return value
        return value if value.endswith(".US") else f"{value}.US"

    normalized_price_tickers = {normalize_ticker(t) for t in raw_price_tickers if t}
    normalized_any_price_tickers = {normalize_ticker(t) for t in raw_any_price_tickers if t}
    seeded_set = set(seeded_tickers)
    with_price_set = normalized_price_tickers & seeded_set
    any_price_set = normalized_any_price_tickers & seeded_set

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
        "matched_price_tickers_raw": len(raw_price_tickers),
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
STEP4_REPORT_STEP = "Step 4 - Visible Universe"


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


async def save_step4_exclusion_report(db, now: datetime) -> Dict[str, Any]:
    """
    Write Step 4 exclusion rows using the canonical Step 4 funnel definition.

    Canonical Step 4 funnel:
      classified_total = count(classified_query)
      visible_total    = count(classified_query AND is_visible=True)
      filtered_out     = classified_query AND is_visible=False

    One row per ticker in filtered_out. Reason = visibility_failed_reason set by
    recompute_visibility_all (all enum values mapped to human-readable labels).

    Returns _debug counts so the caller can store them in ops_job_runs.details.
    """
    report_date = now.astimezone(PRAGUE_TZ).strftime("%Y-%m-%d")
    run_id = f"visible_universe_{now.strftime('%Y%m%d_%H%M%S')}"

    # Only tickers that have completed Step 3 (fundamentals).
    _classified_query = {"fundamentals_status": "complete"}

    # Filtered-out = classified AND NOT visible.
    # Use $ne:True (not False) to include tickers where is_visible is null/missing —
    # tickers that are classified but were never processed by recompute_visibility_all
    # (e.g. they fail shares/currency gates so the recompute cursor skips them)
    # are legitimately not visible and must appear in the exclusion report.
    _filtered_query = {**_classified_query, "is_visible": {"$ne": True}}

    # Snapshot counts matching the canonical Step 4 funnel.
    step4_card_classified_count = await db.tracked_tickers.count_documents(_classified_query)
    step4_card_visible_count    = await db.tracked_tickers.count_documents(
        {**_classified_query, "is_visible": True}
    )
    step4_report_query_count    = await db.tracked_tickers.count_documents(_filtered_query)

    all_reason_labels = {
        "INVALID_EXCHANGE":           "Invalid exchange",
        "NOT_COMMON_STOCK":           "Not common stock",
        "NO_PRICE_DATA":              "No price data",
        "MISSING_SECTOR":             "Sector missing",
        "MISSING_INDUSTRY":           "Industry missing",
        "DELISTED":                   "Ticker is delisted",
        "MISSING_SHARES":             "Shares outstanding missing or zero",
        "MISSING_FINANCIAL_CURRENCY": "Financial currency missing",
    }

    docs_cursor = db.tracked_tickers.find(
        _filtered_query,
        {"_id": 0, "ticker": 1, "name": 1, "visibility_failed_reason": 1,
         "shares_outstanding": 1},
    )

    await db.pipeline_exclusion_report.delete_many(
        {"report_date": report_date, "step": STEP4_REPORT_STEP}
    )

    docs = []
    async for doc in docs_cursor:
        raw_reason = doc.get("visibility_failed_reason") or "UNKNOWN"
        row = {
            "ticker":      doc.get("ticker", "(unknown)"),
            "name":        doc.get("name", ""),
            "step":        STEP4_REPORT_STEP,
            "reason":      all_reason_labels.get(raw_reason, raw_reason),
            "report_date": report_date,
            "source_job":  "compute_visible_universe",
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

    step4_report_rows_written = len(docs)
    return {
        "step4_exclusion_rows": step4_report_rows_written,
        "exclusion_report_run_id": run_id,
        "report_date": report_date,
        "_debug": {
            "step4_card_classified_count": step4_card_classified_count,
            "step4_card_visible_count":    step4_card_visible_count,
            "step4_report_rows_written":   step4_report_rows_written,
            "step4_report_query_count":    step4_report_query_count,
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


async def run_fundamentals_changes_sync(db, batch_size: int = 50, ignore_kill_switch: bool = False, parent_run_id: Optional[str] = None, cancel_check: Optional[Callable[[], Awaitable[bool]]] = None) -> Dict[str, Any]:
    """
    Run fundamentals sync for tickers with changes/events.
    Processes tickers in parallel (up to 20 concurrent) for speed.
    With batch_size=2000: processes all 6,435 tickers in ~15-20 minutes.
    """
    from batch_jobs_service import sync_single_ticker_fundamentals
    from visibility_rules import recompute_visibility_all

    started_at = datetime.now(timezone.utc)
    job_name = "fundamentals_sync"

    logger.info(f"Starting {job_name}")

    # Running sentinel so frontend poll detects job start immediately
    _running_doc_id = (await db.ops_job_runs.insert_one({
        "job_name": job_name,
        "status": "running",
        "started_at": started_at,
        "source": "scheduler",
        "progress": "Queuing tickers for fundamentals sync…",
        "details": {"parent_run_id": parent_run_id},
    })).inserted_id

    async def _progress(msg: str) -> None:
        await db.ops_job_runs.update_one(
            {"_id": _running_doc_id}, {"$set": {"progress": msg}}
        )

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
        await _progress("Deduplicating pending fundamentals_events queue…")
        dedup_stats = await _deduplicate_pending_events(db, now_queue)

        # Ensure Step 3 includes priced tickers missing classification.
        class_candidates = await db.tracked_tickers.find(
            {
                **SEED_QUERY,
                "has_price_data": True,
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

        # Build events_by_ticker from remaining pending events (re-filter in memory —
        # avoids a second DB round-trip; skip-gate updated status in DB already).
        events_by_ticker: Dict[str, List[dict]] = {}
        for event in pending_events:
            ticker_full = event.get("ticker")
            if not ticker_full:
                continue
            # Exclude tickers that were just marked skipped.
            if ticker_full in skipped_ticker_set:
                continue
            if ticker_full not in events_by_ticker and len(events_by_ticker) >= batch_size:
                continue
            events_by_ticker.setdefault(ticker_full, []).append(event)

        tickers_to_sync = [t.replace(".US", "") for t in events_by_ticker.keys()]

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

        # Process tickers
        result = {
            "job_name": job_name,
            "status": "completed",
            "processed": 0,
            "success": 0,
            "failed": 0,
            "classification_events_enqueued": class_enqueue.get("inserted", 0),
            "classification_events_already_pending": class_enqueue.get("already_pending", 0),
            # requested_event_types moved to _debug — not used by the Step 3 card.
            "_debug": {
                "requested_event_types": requested_event_types,
                "queue_stats": queue_stats,
            },
            "started_at": started_at.isoformat(),
        }

        if not tickers_to_sync:
            logger.info(f"{job_name}: No pending events to process")
            result["message"] = "No pending events"
        else:
            total = len(tickers_to_sync)
            await _progress(f"Processing {total} tickers (parallel, 20 concurrent)…")
            logger.info(f"{job_name}: processing {total} tickers in parallel (universe={universe_total})")

            # Parallel processing — 20 concurrent requests
            CONCURRENCY = 20
            semaphore = asyncio.Semaphore(CONCURRENCY)

            # Shared cancel event — set once by the monitor when the DB flag is found.
            cancel_event = asyncio.Event()

            async def _cancel_monitor_sched() -> None:
                """Poll DB every 2 s; consume flag ONCE and set cancel_event."""
                while not cancel_event.is_set():
                    await asyncio.sleep(2)
                    doc = await db.ops_config.find_one({"key": f"cancel_job_{job_name}"})
                    if doc:
                        await db.ops_config.delete_one({"key": f"cancel_job_{job_name}"})
                        logger.info(f"{job_name}: cancel flag consumed by monitor")
                        cancel_event.set()
                        return
                    if cancel_check and await cancel_check():
                        logger.info(f"{job_name}: chain cancel detected by monitor")
                        cancel_event.set()
                        return

            async def _process_one(ticker: str) -> dict:
                # Fast in-memory check — no DB round-trip
                if cancel_event.is_set():
                    return {"ticker": ticker, "success": False, "cancelled": True}
                async with semaphore:
                    return await sync_single_ticker_fundamentals(db, ticker, source_job=job_name)

            monitor_task_sched = asyncio.create_task(_cancel_monitor_sched())
            tasks = [asyncio.create_task(_process_one(t)) for t in tickers_to_sync]
            done_count = 0

            try:
                for coro in asyncio.as_completed(tasks):
                    ticker_result = await coro

                    if ticker_result.get("cancelled"):
                        result["status"] = "cancelled"
                        result["cancelled_at"] = datetime.now(timezone.utc).isoformat()
                        logger.info(f"{job_name}: cancelled after {done_count} tickers")
                        # Do NOT call t.cancel() — in-flight tasks finish naturally;
                        # cancel_event prevents any new ones from starting.
                        break

                    result["processed"] += 1
                    done_count += 1

                    if ticker_result.get("success"):
                        result["success"] += 1
                        ticker_us = f"{ticker_result.get('ticker', '')}.US"
                        event_ids = [
                            e.get("_id")
                            for e in events_by_ticker.get(ticker_us, [])
                            if e.get("_id")
                        ]
                        if event_ids:
                            await db.fundamentals_events.update_many(
                                {"_id": {"$in": event_ids}},
                                {"$set": {"status": "completed", "completed_at": datetime.now(timezone.utc)}}
                            )
                    else:
                        result["failed"] += 1

                    # Progress update every 100 tickers
                    if done_count % 100 == 0:
                        await _progress(
                            f"Fundamentals sync: {done_count}/{total} done "
                            f"(✓{result['success']} ✗{result['failed']})"
                        )
            finally:
                monitor_task_sched.cancel()

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

        # Step 4 auto-chain: after successful Step 3 completion, recompute visibility.
        step4_chain = {
            "triggered": False,
            "status": "skipped",
        }
        if result["status"] == "completed":
            try:
                # Pass Step 3 exclusion_report_run_id as parent for Step 4.
                _s3_excl_run_id = result.get("exclusion_report_run_id")
                visibility_result = await recompute_visibility_all(db, parent_run_id=_s3_excl_run_id)
                step4_chain = {
                    "triggered": True,
                    "status": "completed",
                    "job_id": visibility_result.get("job_id"),
                    "duration_seconds": visibility_result.get("duration_seconds"),
                    "changed": visibility_result.get("stats", {}).get("changed"),
                    "now_visible": visibility_result.get("after", {}).get("visible_count"),
                }
                logger.info(
                    f"{job_name}: Step 4 auto-chain completed "
                    f"(job_id={step4_chain.get('job_id')}, changed={step4_chain.get('changed')})"
                )
                # Step 4 exclusion report: tickers filtered by visibility sieve
                now_step4 = datetime.now(timezone.utc)
                step4_report = await save_step4_exclusion_report(db, now_step4)
                step4_chain["exclusion_rows"] = step4_report.get("step4_exclusion_rows", 0)
                step4_chain["_debug"] = step4_report.get("_debug", {})
                step4_chain["exclusion_report_run_id"] = step4_report.get("exclusion_report_run_id")
                result["step4_exclusion_report_run_id"] = step4_report.get("exclusion_report_run_id")
            except Exception as step4_error:
                step4_chain = {
                    "triggered": True,
                    "status": "failed",
                    "error": str(step4_error),
                }
                logger.error(f"{job_name}: Step 4 auto-chain failed: {step4_error}")
        result["step4_visibility"] = step4_chain
        
        finished_at = datetime.now(timezone.utc)
        result["finished_at"] = finished_at.isoformat()
        result["started_at_prague"] = _to_prague_iso(started_at)
        result["finished_at_prague"] = _to_prague_iso(finished_at)
        result["log_timezone"] = "Europe/Prague"
        result["universe_total"] = universe_total  # informational only

        total_to_sync = len(tickers_to_sync) if tickers_to_sync else 0

        await db.ops_job_runs.update_one(
            {"_id": _running_doc_id},
            {"$set": {
                "status": result["status"],
                "finished_at": finished_at,
                "started_at_prague": _to_prague_iso(started_at),
                "finished_at_prague": _to_prague_iso(finished_at),
                "log_timezone": "Europe/Prague",
                "details": result,
                "progress": (
                    f"Fundamentals sync: {result['success']}/{total_to_sync} done "
                    f"(✓{result['success']} ✗{result['failed']})"
                ),
                "progress_processed": done_count if tickers_to_sync else 0,
                "progress_total":     total_to_sync,
                "progress_pct":       round(done_count / total_to_sync * 100) if total_to_sync else 0,
                **({"cancelled_at": finished_at} if result["status"] == "cancelled" else {}),
            }}
        )

        await log_scheduled_job(
            db,
            job_name=job_name,
            status=result["status"],
            details=result,
            started_at=started_at,
            finished_at=finished_at,
        )

        logger.info(
            f"{job_name} {result['status']}: "
            f"processed={result['processed']}, success={result['success']}"
        )

        return result

    except Exception as e:
        error_msg = str(e)
        logger.error(f"{job_name} failed: {error_msg}")

        await db.ops_job_runs.update_one(
            {"_id": _running_doc_id},
            {"$set": {"status": "failed", "finished_at": datetime.now(timezone.utc), "error": error_msg}},
        )
        await log_scheduled_job(
            db,
            job_name=job_name,
            status="failed",
            details={"error": error_msg},
            started_at=started_at,
            error=error_msg
        )

        return {
            "job_name": job_name,
            "status": "failed",
            "error": error_msg,
            "started_at": started_at.isoformat(),
        }


async def get_tickers_needing_backfill(db, limit: int = 100) -> List[str]:
    """
    Get tickers that need price backfill.
    
    Returns tickers that:
    1. Are active (have fundamentals) but no price data
    2. Have detected price gaps
    3. Have corporate actions requiring price refresh (splits, etc.)
    """
    tickers_to_backfill = []
    
    # 1. Active tickers without price data
    active_tickers = await db.company_fundamentals_cache.distinct("ticker")
    tickers_with_prices = await db.stock_prices.distinct("ticker")
    tickers_with_prices_set = set(tickers_with_prices)
    
    missing_prices = [t for t in active_tickers if t not in tickers_with_prices_set]
    tickers_to_backfill.extend(missing_prices[:limit])
    
    if len(tickers_to_backfill) >= limit:
        return tickers_to_backfill[:limit]
    
    # 2. Tickers with detected data gaps (price field)
    remaining = limit - len(tickers_to_backfill)
    gaps = await db.data_gaps.find(
        {"missing_price": True, "resolved": {"$ne": True}},
        {"ticker": 1}
    ).limit(remaining).to_list(length=remaining)
    
    gap_tickers = [g.get("ticker") for g in gaps if g.get("ticker") and g.get("ticker") not in tickers_to_backfill]
    tickers_to_backfill.extend(gap_tickers)
    
    # 3. Future: Corporate actions (splits, etc.) - placeholder
    # This would check a corporate_actions collection
    
    return tickers_to_backfill[:limit]


async def run_price_backfill_gaps(db, batch_size: int = 50, ignore_kill_switch: bool = False) -> Dict[str, Any]:
    """
    Run price backfill for tickers that need it.
    
    Only processes:
    - Newly activated tickers (have fundamentals, no prices)
    - Tickers with detected price gaps
    - Tickers with corporate actions (splits)
    
    Does NOT backfill all tickers - only those flagged.
    """
    from price_ingestion_service import backfill_ticker_prices
    
    started_at = datetime.now(timezone.utc)
    job_name = "price_backfill"
    
    logger.info(f"Starting {job_name}")
    
    try:
        # Check kill switch (manual endpoints can explicitly bypass)
        if (not ignore_kill_switch) and (not await get_scheduler_enabled(db)):
            logger.warning(f"{job_name} skipped: kill switch engaged")
            return {
                "job_name": job_name,
                "status": "skipped",
                "reason": "kill_switch_engaged",
                "started_at": started_at.isoformat(),
            }
        
        # Get tickers that need backfill
        tickers_to_backfill = await get_tickers_needing_backfill(db, limit=batch_size)
        
        if not tickers_to_backfill:
            logger.info(f"{job_name}: No tickers need backfill")
            return {
                "job_name": job_name,
                "status": "completed",
                "message": "No tickers need backfill",
                "processed": 0,
                "started_at": started_at.isoformat(),
            }
        
        # Process tickers
        result = {
            "job_name": job_name,
            "status": "completed",
            "processed": 0,
            "success": 0,
            "failed": 0,
            "total_records": 0,
            "tickers_processed": [],
            "started_at": started_at.isoformat(),
        }
        
        for ticker in tickers_to_backfill:
            # Check kill switch between tickers (manual endpoints can bypass)
            if (not ignore_kill_switch) and (not await get_scheduler_enabled(db)):
                result["status"] = "interrupted"
                result["reason"] = "kill_switch_engaged"
                break
            
            ticker_result = await backfill_ticker_prices(db, ticker)
            result["processed"] += 1
            
            if ticker_result["success"]:
                result["success"] += 1
                result["total_records"] += ticker_result.get("records_upserted", 0)
                result["tickers_processed"].append({
                    "ticker": ticker,
                    "records": ticker_result.get("records_upserted", 0)
                })
                
                # Mark data gap as resolved if it existed
                await db.data_gaps.update_one(
                    {"ticker": ticker, "missing_price": True},
                    {"$set": {"resolved": True, "resolved_at": datetime.now(timezone.utc)}}
                )
            else:
                result["failed"] += 1
            
            # Small delay to avoid overwhelming API
            await asyncio.sleep(0.3)
        
        result["finished_at"] = datetime.now(timezone.utc).isoformat()
        
        # Log job run
        await log_scheduled_job(
            db,
            job_name=job_name,
            status=result["status"],
            details={k: v for k, v in result.items() if k != "tickers_processed"},  # Don't store full list
            started_at=started_at,
            finished_at=datetime.now(timezone.utc)
        )
        
        logger.info(f"{job_name} completed: processed={result['processed']}, records={result['total_records']}")
        
        return result
        
    except Exception as e:
        error_msg = str(e)
        logger.error(f"{job_name} failed: {error_msg}")
        
        await log_scheduled_job(
            db,
            job_name=job_name,
            status="failed",
            details={"error": error_msg},
            started_at=started_at,
            error=error_msg
        )
        
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
        {"job_name": {"$in": ["price_sync", "scheduled_price_sync", "daily_price_sync"]}},
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
    active_without_prices = len(await get_tickers_needing_backfill(db, limit=1000))
    
    return {
        "scheduler_enabled": scheduler_enabled,
        "kill_switch_engaged": not scheduler_enabled,
        "schedule": {
            "timezone": "Europe/Prague",
            "days": "Mon-Sat",
            "universe_seed": "23:00",
            "price_sync": "after universe_seed completion",
            "fundamentals_sync": "after price_sync completion",
            "price_backfill": "04:45",
        },
        "last_runs": {
            "price_sync": {
                "status": last_price_sync.get("status") if last_price_sync else None,
                "started_at": last_price_sync.get("started_at").isoformat() if last_price_sync and last_price_sync.get("started_at") else None,
                "records": last_price_sync.get("records_upserted") if last_price_sync else None,
            } if last_price_sync else None,
            "fundamentals_sync": {
                "status": last_fundamentals_sync.get("status") if last_fundamentals_sync else None,
                "started_at": last_fundamentals_sync.get("started_at").isoformat() if last_fundamentals_sync and last_fundamentals_sync.get("started_at") else None,
                "processed": last_fundamentals_sync.get("details", {}).get("processed") if last_fundamentals_sync else None,
            } if last_fundamentals_sync else None,
            "price_backfill": {
                "status": last_backfill.get("status") if last_backfill else None,
                "started_at": last_backfill.get("started_at").isoformat() if last_backfill and last_backfill.get("started_at") else None,
                "records": last_backfill.get("details", {}).get("total_records") if last_backfill else None,
            } if last_backfill else None,
        },
        "pending_work": {
            "pending_fundamentals_events": pending_fundamentals,
            "tickers_needing_price_backfill": active_without_prices,
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
