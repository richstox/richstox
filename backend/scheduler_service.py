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
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional, List
import asyncio
from zoneinfo import ZoneInfo
from collections import defaultdict

logger = logging.getLogger("richstox.scheduler")

# Constants
SCHEDULER_CONFIG_KEY = "scheduler_enabled"
SEED_QUERY = {"exchange": {"$in": ["NYSE", "NASDAQ"]}, "asset_type": "Common Stock"}
PRAGUE_TZ = ZoneInfo("Europe/Prague")
STEP2_REPORT_STEP = "Step 2 - Price Sync"


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
    
    result = await db.ops_config.update_one(
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


async def _enqueue_fundamentals_events(
    db,
    event_type: str,
    tickers: List[str],
    now: datetime,
    source_job: str,
    detector_step: str,
) -> Dict[str, int]:
    """
    Enqueue fundamentals events idempotently.
    Creates one pending event per (ticker, event_type) while pending/processing exists.
    """
    inserted = 0
    existing = 0
    normalized = sorted({t for t in tickers if t})
    for ticker in normalized:
        result = await db.fundamentals_events.update_one(
            {
                "ticker": ticker,
                "event_type": event_type,
                "status": {"$in": ["pending", "processing"]},
            },
            {
                "$setOnInsert": {
                    "ticker": ticker,
                    "event_type": event_type,
                    "status": "pending",
                    "source_job": source_job,
                    "detector_step": detector_step,
                    "created_at": now,
                },
                "$set": {
                    "updated_at": now,
                },
            },
            upsert=True,
        )
        if result.upserted_id is not None:
            inserted += 1
        else:
            existing += 1
    return {"inserted": inserted, "already_pending": existing}


async def _detect_split_candidates(db, max_candidates: int = 500) -> Dict[str, Any]:
    """
    Step 2.2: detect probable split/reverse-split from two latest trading closes.
    """
    date_rows = await db.stock_prices.aggregate([
        {"$group": {"_id": "$date"}},
        {"$sort": {"_id": -1}},
        {"$limit": 2},
    ]).to_list(2)
    dates = [d.get("_id") for d in date_rows if d.get("_id")]
    if len(dates) < 2:
        return {"checked_tickers": 0, "candidates": [], "latest_date": None, "previous_date": None}

    latest_date, previous_date = dates[0], dates[1]
    seed_docs = await db.tracked_tickers.find(
        {**SEED_QUERY, "has_price_data": True},
        {"_id": 0, "ticker": 1},
    ).to_list(None)
    seeded = {d.get("ticker") for d in seed_docs if d.get("ticker")}
    if not seeded:
        return {"checked_tickers": 0, "candidates": [], "latest_date": latest_date, "previous_date": previous_date}

    rows = await db.stock_prices.find(
        {"ticker": {"$in": list(seeded)}, "date": {"$in": [latest_date, previous_date]}},
        {"_id": 0, "ticker": 1, "date": 1, "close": 1, "adjusted_close": 1},
    ).to_list(None)

    by_ticker: Dict[str, Dict[str, float]] = defaultdict(dict)
    for row in rows:
        ticker = row.get("ticker")
        date = row.get("date")
        price = _pick_price(row)
        if ticker and date and price is not None:
            by_ticker[ticker][date] = price

    candidates: List[str] = []
    checked = 0
    for ticker, values in by_ticker.items():
        if latest_date not in values or previous_date not in values:
            continue
        prev_price = values[previous_date]
        latest_price = values[latest_date]
        if prev_price <= 0:
            continue
        checked += 1
        ratio = latest_price / prev_price
        # 2:1, 3:1, 4:1 and reverse-split-like jumps.
        if ratio >= 1.8 or ratio <= 0.56:
            candidates.append(ticker)
            if len(candidates) >= max_candidates:
                break

    return {
        "checked_tickers": checked,
        "candidates": candidates,
        "latest_date": latest_date,
        "previous_date": previous_date,
    }


async def _detect_dividend_candidates(db, lookback_days: int = 2, lookahead_days: int = 7, max_candidates: int = 500) -> Dict[str, Any]:
    """
    Step 2.4: detect ex-dividend window from cached fundamentals.
    """
    today = datetime.now(PRAGUE_TZ).date()
    start = (today - timedelta(days=lookback_days)).isoformat()
    end = (today + timedelta(days=lookahead_days)).isoformat()

    docs = await db.company_fundamentals_cache.find(
        {"ex_dividend_date": {"$gte": start, "$lte": end}},
        {"_id": 0, "ticker": 1, "ex_dividend_date": 1},
    ).limit(max_candidates).to_list(max_candidates)
    candidates = [d.get("ticker") for d in docs if d.get("ticker")]
    return {
        "window_start": start,
        "window_end": end,
        "candidates": candidates,
        "checked_tickers": len(docs),
    }


async def _detect_earnings_candidates(db, stale_days: int = 95, max_candidates: int = 500) -> Dict[str, Any]:
    """
    Step 2.6: detect stale fundamentals likely requiring earnings refresh.
    """
    stale_cutoff = datetime.now(timezone.utc) - timedelta(days=stale_days)
    docs = await db.tracked_tickers.find(
        {
            **SEED_QUERY,
            "has_price_data": True,
            "$or": [
                {"fundamentals_updated_at": {"$exists": False}},
                {"fundamentals_updated_at": None},
                {"fundamentals_updated_at": {"$lt": stale_cutoff}},
            ],
        },
        {"_id": 0, "ticker": 1},
    ).sort("fundamentals_updated_at", 1).limit(max_candidates).to_list(max_candidates)

    candidates = [d.get("ticker") for d in docs if d.get("ticker")]
    return {
        "stale_cutoff": stale_cutoff.isoformat(),
        "checked_tickers": len(docs),
        "candidates": candidates,
    }


async def run_step2_event_detectors(db) -> Dict[str, Any]:
    """
    Execute Step 2 sub-steps and enqueue fundamentals refresh events:
    2.2 split detector, 2.4 dividend detector, 2.6 earnings detector.
    """
    now = datetime.now(timezone.utc)

    split = await _detect_split_candidates(db)
    dividend = await _detect_dividend_candidates(db)
    earnings = await _detect_earnings_candidates(db)

    split_enqueue = await _enqueue_fundamentals_events(
        db,
        event_type="split_detected",
        tickers=split.get("candidates", []),
        now=now,
        source_job="price_sync",
        detector_step="2.2",
    )
    dividend_enqueue = await _enqueue_fundamentals_events(
        db,
        event_type="dividend_detected",
        tickers=dividend.get("candidates", []),
        now=now,
        source_job="price_sync",
        detector_step="2.4",
    )
    earnings_enqueue = await _enqueue_fundamentals_events(
        db,
        event_type="earnings_refresh_due",
        tickers=earnings.get("candidates", []),
        now=now,
        source_job="price_sync",
        detector_step="2.6",
    )

    return {
        "step_2_2_split": {
            "checked_tickers": split.get("checked_tickers", 0),
            "candidate_tickers": len(split.get("candidates", [])),
            "enqueued": split_enqueue.get("inserted", 0),
            "already_pending": split_enqueue.get("already_pending", 0),
            "latest_date": split.get("latest_date"),
            "previous_date": split.get("previous_date"),
        },
        "step_2_4_dividend": {
            "checked_tickers": dividend.get("checked_tickers", 0),
            "candidate_tickers": len(dividend.get("candidates", [])),
            "enqueued": dividend_enqueue.get("inserted", 0),
            "already_pending": dividend_enqueue.get("already_pending", 0),
            "window_start": dividend.get("window_start"),
            "window_end": dividend.get("window_end"),
        },
        "step_2_6_earnings": {
            "checked_tickers": earnings.get("checked_tickers", 0),
            "candidate_tickers": len(earnings.get("candidates", [])),
            "enqueued": earnings_enqueue.get("inserted", 0),
            "already_pending": earnings_enqueue.get("already_pending", 0),
            "stale_cutoff": earnings.get("stale_cutoff"),
        },
        "enqueued_total": (
            split_enqueue.get("inserted", 0)
            + dividend_enqueue.get("inserted", 0)
            + earnings_enqueue.get("inserted", 0)
        ),
    }


async def run_daily_price_sync(db, ignore_kill_switch: bool = False) -> Dict[str, Any]:
    """
    Run daily price sync job with GAP DETECTION AND BULK CATCHUP.
    
    This job now:
    1. Detects gaps in price coverage (dates with <80% tickers)
    2. Fetches bulk data for gap dates using EODHD bulk API
    3. Upserts using bulk_write for performance
    
    Config read from ops_config:
    - lookback_days (default: 30)
    - coverage_threshold (default: 0.80)
    """
    from price_ingestion_service import run_daily_bulk_catchup
    
    started_at = datetime.now(timezone.utc)
    job_name = "price_sync"
    
    logger.info(f"Starting {job_name} with gap detection and bulk catchup")
    
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
        
        # Run the NEW bulk catchup with gap detection
        result = await run_daily_bulk_catchup(db)
        
        # Canonical Step 2 behavior: update has_price_data flags after bulk ingest
        price_flag_summary = await sync_has_price_data_flags(db, include_exclusions=True)
        result["tickers_seeded_total"] = price_flag_summary["seeded_total"]
        result["tickers_with_price_data"] = price_flag_summary["with_price_data"]
        result["tickers_without_price_data"] = price_flag_summary["without_price_data"]
        result["matched_price_tickers_raw"] = price_flag_summary.get("matched_price_tickers_raw", 0)
        result.update(
            await save_price_sync_exclusion_report(
                db,
                rows=price_flag_summary.get("exclusions", []),
                now=datetime.now(timezone.utc),
            )
        )
        # Step 2.2 / 2.4 / 2.6 detectors -> enqueue fundamentals refresh events.
        event_detector_summary = await run_step2_event_detectors(db)
        result["event_detectors"] = event_detector_summary
        result["fundamentals_events_enqueued"] = event_detector_summary.get("enqueued_total", 0)

        # Log to ops_job_runs
        finished_at = datetime.now(timezone.utc)
        await db.ops_job_runs.insert_one({
            "job_name": job_name,
            "source": "scheduler",
            "started_at": started_at,
            "finished_at": finished_at,
            "started_at_prague": _to_prague_iso(started_at),
            "finished_at_prague": _to_prague_iso(finished_at),
            "log_timezone": "Europe/Prague",
            "status": result.get("status", "completed"),
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
                "event_detectors": result.get("event_detectors", {}),
            }
        })
        
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
            "dates_processed": result.get("dates_processed", 0),
            "api_calls": result.get("api_calls", 0),
            "fundamentals_events_enqueued": result.get("fundamentals_events_enqueued", 0),
            "started_at": started_at.isoformat(),
            "finished_at": datetime.now(timezone.utc).isoformat(),
        }
        
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
            "name": row.get("name", "(unknown)"),
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


async def run_fundamentals_changes_sync(db, batch_size: int = 50, ignore_kill_switch: bool = False) -> Dict[str, Any]:
    """
    Run fundamentals sync for tickers with changes/events.
    
    Only processes:
    - Tickers with pending fundamentals_events
    - Tickers flagged for refresh (future: corporate actions, splits)
    
    Does NOT do a full refresh of all tickers.
    """
    from batch_jobs_service import sync_single_ticker_fundamentals
    
    started_at = datetime.now(timezone.utc)
    job_name = "fundamentals_sync"
    
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
        ).limit(batch_size * 2).to_list(batch_size * 2)
        class_candidate_tickers = [d.get("ticker") for d in class_candidates if d.get("ticker")]
        class_enqueue = await _enqueue_fundamentals_events(
            db,
            event_type="classification_missing",
            tickers=class_candidate_tickers,
            now=datetime.now(timezone.utc),
            source_job=job_name,
            detector_step="3.0",
        )

        # Get tickers with pending events (not full refresh)
        pending_events = await db.fundamentals_events.find(
            {"status": "pending"},
            {"ticker": 1, "event_type": 1, "created_at": 1}
        ).sort("created_at", 1).limit(batch_size * 5).to_list(length=batch_size * 5)

        events_by_ticker: Dict[str, List[dict]] = {}
        for event in pending_events:
            ticker_full = event.get("ticker")
            if not ticker_full:
                continue
            if ticker_full not in events_by_ticker and len(events_by_ticker) >= batch_size:
                continue
            events_by_ticker.setdefault(ticker_full, []).append(event)

        tickers_to_sync = [t.replace(".US", "") for t in events_by_ticker.keys()]
        requested_event_types: Dict[str, int] = {}
        for event_group in events_by_ticker.values():
            for event in event_group:
                event_type = event.get("event_type") or "unknown"
                requested_event_types[event_type] = requested_event_types.get(event_type, 0) + 1
        
        if not tickers_to_sync:
            logger.info(f"{job_name}: No pending events to process")
            return {
                "job_name": job_name,
                "status": "completed",
                "message": "No pending events",
                "processed": 0,
                "classification_events_enqueued": class_enqueue.get("inserted", 0),
                "started_at": started_at.isoformat(),
            }
        
        # Process tickers
        result = {
            "job_name": job_name,
            "status": "completed",
            "processed": 0,
            "success": 0,
            "failed": 0,
            "classification_events_enqueued": class_enqueue.get("inserted", 0),
            "classification_events_already_pending": class_enqueue.get("already_pending", 0),
            "requested_event_types": requested_event_types,
            "started_at": started_at.isoformat(),
        }
        
        for ticker in tickers_to_sync:
            # Check kill switch between tickers (manual endpoints can bypass)
            if (not ignore_kill_switch) and (not await get_scheduler_enabled(db)):
                result["status"] = "interrupted"
                result["reason"] = "kill_switch_engaged"
                break
            
            ticker_result = await sync_single_ticker_fundamentals(
                db,
                ticker,
                source_job=job_name,
            )
            result["processed"] += 1
            
            if ticker_result["success"]:
                result["success"] += 1
                # Mark selected pending events for this ticker as completed.
                event_ids = [
                    e.get("_id")
                    for e in events_by_ticker.get(f"{ticker}.US", [])
                    if e.get("_id")
                ]
                if event_ids:
                    await db.fundamentals_events.update_many(
                        {"_id": {"$in": event_ids}},
                        {"$set": {"status": "completed", "completed_at": datetime.now(timezone.utc)}}
                    )
            else:
                result["failed"] += 1
        
        result["finished_at"] = datetime.now(timezone.utc).isoformat()
        
        # Log job run
        await log_scheduled_job(
            db,
            job_name=job_name,
            status=result["status"],
            details=result,
            started_at=started_at,
            finished_at=datetime.now(timezone.utc)
        )
        
        logger.info(f"{job_name} completed: processed={result['processed']}, success={result['success']}")
        
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
    
    logger.info(f"[DELTA FUNDAMENTALS] Starting...")
    
    # Get tickers needing update
    tickers = await get_tickers_needing_fundamentals_update(db, batch_size)
    
    if not tickers:
        logger.info(f"[DELTA FUNDAMENTALS] All tickers up-to-date")
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
