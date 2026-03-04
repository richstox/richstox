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
- 04:00: Daily price sync (bulk API call) + corporate actions detection
- 04:15: SP500TR benchmark update
- 04:30: Fundamentals sync (only tickers with changes/events/splits)
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

logger = logging.getLogger("richstox.scheduler")

# Constants
SCHEDULER_CONFIG_KEY = "scheduler_enabled"


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
        "duration_seconds": duration,
        "error": error,
        "created_at": now,
    }
    
    result = await db.ops_job_runs.insert_one(doc)
    return str(result.inserted_id)


async def run_daily_price_sync(db) -> Dict[str, Any]:
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
        # Check kill switch
        if not await get_scheduler_enabled(db):
            logger.warning(f"{job_name} skipped: kill switch engaged")
            return {
                "job_name": job_name,
                "status": "skipped",
                "reason": "kill_switch_engaged",
                "started_at": started_at.isoformat(),
            }
        
        # Run the NEW bulk catchup with gap detection
        result = await run_daily_bulk_catchup(db)
        
        # Log to ops_job_runs
        await db.ops_job_runs.insert_one({
            "job_name": job_name,
            "source": "scheduler",
            "started_at": started_at,
            "finished_at": datetime.now(timezone.utc),
            "status": result.get("status", "completed"),
            "details": {
                "config": result.get("config"),
                "gap_analysis": result.get("gap_analysis"),
                "dates_processed": result.get("dates_processed", 0),
                "records_upserted": result.get("records_upserted", 0),
                "api_calls": result.get("api_calls", 0),
                "bulk_writes": result.get("bulk_writes", 0),
            }
        })
        
        logger.info(f"{job_name} completed: {result.get('records_upserted', 0)} records, "
                   f"{result.get('dates_processed', 0)} gap dates processed, "
                   f"{result.get('api_calls', 0)} API calls")
        
        return {
            "job_name": job_name,
            "status": result.get("status", "completed"),
            "records_upserted": result.get("records_upserted", 0),
            "dates_processed": result.get("dates_processed", 0),
            "api_calls": result.get("api_calls", 0),
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


async def run_fundamentals_changes_sync(db, batch_size: int = 50) -> Dict[str, Any]:
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
        # Check kill switch
        if not await get_scheduler_enabled(db):
            logger.warning(f"{job_name} skipped: kill switch engaged")
            return {
                "job_name": job_name,
                "status": "skipped",
                "reason": "kill_switch_engaged",
                "started_at": started_at.isoformat(),
            }
        
        # Get tickers with pending events (not full refresh)
        pending_events = await db.fundamentals_events.find(
            {"status": "pending"},
            {"ticker": 1}
        ).limit(batch_size).to_list(length=batch_size)
        
        tickers_to_sync = [e.get("ticker", "").replace(".US", "") for e in pending_events if e.get("ticker")]
        
        if not tickers_to_sync:
            logger.info(f"{job_name}: No pending events to process")
            return {
                "job_name": job_name,
                "status": "completed",
                "message": "No pending events",
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
            "started_at": started_at.isoformat(),
        }
        
        for ticker in tickers_to_sync:
            # Check kill switch between tickers
            if not await get_scheduler_enabled(db):
                result["status"] = "interrupted"
                result["reason"] = "kill_switch_engaged"
                break
            
            ticker_result = await sync_single_ticker_fundamentals(db, ticker)
            result["processed"] += 1
            
            if ticker_result["success"]:
                result["success"] += 1
                # Mark event as completed
                await db.fundamentals_events.update_one(
                    {"ticker": f"{ticker}.US", "status": "pending"},
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


async def run_price_backfill_gaps(db, batch_size: int = 50) -> Dict[str, Any]:
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
        # Check kill switch
        if not await get_scheduler_enabled(db):
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
            # Check kill switch between tickers
            if not await get_scheduler_enabled(db):
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
            "price_sync": "04:00",
            "fundamentals_sync": "04:30",
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
            sync_result = await sync_single_ticker_fundamentals(db, ticker_short)
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
