"""
RICHSTOX Batch Job Service
===========================
Manages large-scale data sync jobs with:
- Batch processing (configurable batch size)
- Kill switch (pause/resume)
- Progress tracking
- Robust error handling
- Job logging to ops_job_runs collection
"""

import os
import logging
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
import asyncio
from zoneinfo import ZoneInfo

from fundamentals_service import (
    fetch_fundamentals_from_eodhd,
    parse_company_fundamentals,
    parse_financials,
    parse_earnings_history,
    parse_insider_activity,
)
from provider_debug_service import upsert_provider_debug_snapshot

logger = logging.getLogger("richstox.batch_jobs")
PRAGUE_TZ = ZoneInfo("Europe/Prague")

# Kill switch - set to True to pause all batch jobs
BATCH_JOB_KILL_SWITCH = False


def get_kill_switch() -> bool:
    """Check if kill switch is enabled."""
    global BATCH_JOB_KILL_SWITCH
    return BATCH_JOB_KILL_SWITCH


def set_kill_switch(enabled: bool):
    """Set kill switch state."""
    global BATCH_JOB_KILL_SWITCH
    BATCH_JOB_KILL_SWITCH = enabled
    logger.info(f"Kill switch {'ENABLED' if enabled else 'DISABLED'}")


def _to_prague_iso(dt: Optional[datetime]) -> Optional[str]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(PRAGUE_TZ).isoformat()


async def log_job_run(
    db,
    job_name: str,
    status: str,
    details: Dict[str, Any],
    started_at: datetime,
    finished_at: Optional[datetime] = None
):
    """Log job run to ops_job_runs collection."""
    end_time = finished_at or datetime.now(timezone.utc)
    doc = {
        "job_name": job_name,
        "status": status,
        "details": details,
        "started_at": started_at,
        "finished_at": end_time,
        "started_at_prague": _to_prague_iso(started_at),
        "finished_at_prague": _to_prague_iso(end_time),
        "log_timezone": "Europe/Prague",
        "created_at": datetime.now(timezone.utc),
    }
    await db.ops_job_runs.insert_one(doc)
    return doc


async def sync_single_ticker_fundamentals(
    db,
    ticker: str,
    source_job: str = "fundamentals_sync",
) -> Dict[str, Any]:
    """
    Sync fundamentals for a single ticker with robust error handling.
    Returns detailed result including which data was available/missing.
    """
    ticker_upper = ticker.upper()
    ticker_full = ticker_upper if ticker_upper.endswith(".US") else f"{ticker_upper}.US"
    now = datetime.now(timezone.utc)
    
    result = {
        "ticker": ticker_full,
        "success": False,
        "has_fundamentals": False,
        "has_financials": False,
        "has_earnings": False,
        "has_insider": False,
        "financials_count": 0,
        "earnings_count": 0,
        "error": None,
        "error_type": None,
        "provider_debug_snapshot_stored": False,
    }
    
    try:
        # Fetch from EODHD
        data = await fetch_fundamentals_from_eodhd(ticker_upper)
        
        if not data:
            result["error"] = "No fundamentals data from EODHD"
            result["error_type"] = "no_data"
            # Mark ticker as no_fundamentals
            await db.tracked_tickers.update_one(
                {"ticker": ticker_full},
                {"$set": {
                    "status": "no_fundamentals",
                    "is_active": False,
                    "fundamentals_error": "No data from EODHD",
                    "updated_at": now,
                }},
                upsert=True
            )
            return result

        debug_result = await upsert_provider_debug_snapshot(
            db=db,
            ticker=ticker_full,
            raw_payload=data,
            source_job=source_job,
        )
        result["provider_debug_snapshot_stored"] = bool(debug_result.get("stored"))
        
        # 1. Parse and store company fundamentals
        company_doc = parse_company_fundamentals(ticker_upper, data)
        await db.company_fundamentals_cache.update_one(
            {"ticker": ticker_full},
            {"$set": company_doc},
            upsert=True
        )
        result["has_fundamentals"] = True
        
        # 2. Parse and store financials (may be empty for some stocks)
        financials_rows = parse_financials(ticker_upper, data)
        if financials_rows:
            await db.financials_cache.delete_many({"ticker": ticker_full})
            await db.financials_cache.insert_many(financials_rows)
            result["has_financials"] = True
            result["financials_count"] = len(financials_rows)
        
        # 3. Parse and store earnings history (may be empty)
        earnings_rows = parse_earnings_history(ticker_upper, data)
        if earnings_rows:
            await db.earnings_history_cache.delete_many({"ticker": ticker_full})
            await db.earnings_history_cache.insert_many(earnings_rows)
            result["has_earnings"] = True
            result["earnings_count"] = len(earnings_rows)
        
        # 4. Parse and store insider activity (often empty/null)
        insider_doc = parse_insider_activity(ticker_upper, data)
        if insider_doc and (insider_doc.get("buyers_count", 0) > 0 or insider_doc.get("sellers_count", 0) > 0):
            await db.insider_activity_cache.update_one(
                {"ticker": ticker_full},
                {"$set": insider_doc},
                upsert=True
            )
            result["has_insider"] = True
        # Note: Missing insider data is normal - many stocks have no insider transactions
        
        # 5. Activate ticker in tracked_tickers
        # Extract financial_currency using the new extraction utility (Option B - Persist)
        from utils.currency_utils import extract_statement_currency
        financial_currency = extract_statement_currency(data)
        sector = (company_doc.get("sector") or "").strip()
        industry = (company_doc.get("industry") or "").strip()
        has_classification = bool(sector and industry)
        
        await db.tracked_tickers.update_one(
            {"ticker": ticker_full},
            {
                "$set": {
                    "status": "active",
                    "is_active": True,
                    "name": company_doc.get("name"),
                    "sector": sector or None,
                    "industry": industry or None,
                    "has_classification": has_classification,
                    "financial_currency": financial_currency,  # P1 Policy: Persist currency
                    "fundamentals_updated_at": now,
                    "updated_at": now,
                }
            },
            upsert=True
        )
        
        result["success"] = True
        
    except Exception as e:
        result["error"] = str(e)
        result["error_type"] = "exception"
        logger.error(f"Error syncing {ticker_full}: {e}")
    
    return result


async def run_fundamentals_batch_job(
    db,
    tickers: List[str],
    batch_size: int = 50,
    delay_between_batches: float = 1.0,
    job_name: str = "fundamentals_batch_sync"
) -> Dict[str, Any]:
    """
    Run batch fundamentals sync with kill switch support.
    
    Args:
        db: MongoDB database
        tickers: List of tickers to sync
        batch_size: Tickers per batch
        delay_between_batches: Seconds to wait between batches
        job_name: Name for logging
    
    Returns:
        Job result with statistics
    """
    started_at = datetime.now(timezone.utc)
    
    result = {
        "job_name": job_name,
        "started_at": started_at.isoformat(),
        "total_tickers": len(tickers),
        "batch_size": batch_size,
        "processed": 0,
        "success": 0,
        "failed": 0,
        "no_data": 0,
        "has_fundamentals": 0,
        "has_financials": 0,
        "has_earnings": 0,
        "has_insider": 0,
        "batches_completed": 0,
        "killed": False,
        "errors": [],
        "api_calls_used": 0,
    }
    
    # Process in batches
    total_batches = (len(tickers) + batch_size - 1) // batch_size
    
    for batch_idx in range(total_batches):
        # Check kill switch before each batch
        if get_kill_switch():
            result["killed"] = True
            result["kill_reason"] = "Kill switch enabled"
            logger.warning(f"Job {job_name} killed at batch {batch_idx + 1}/{total_batches}")
            break
        
        batch_start = batch_idx * batch_size
        batch_end = min(batch_start + batch_size, len(tickers))
        batch_tickers = tickers[batch_start:batch_end]
        
        logger.info(f"Processing batch {batch_idx + 1}/{total_batches}: {len(batch_tickers)} tickers")
        
        for ticker in batch_tickers:
            # Check kill switch for each ticker too
            if get_kill_switch():
                result["killed"] = True
                result["kill_reason"] = "Kill switch enabled"
                break
            
            ticker_result = await sync_single_ticker_fundamentals(db, ticker)
            result["processed"] += 1
            result["api_calls_used"] += 10
            
            if ticker_result["success"]:
                result["success"] += 1
                if ticker_result["has_fundamentals"]:
                    result["has_fundamentals"] += 1
                if ticker_result["has_financials"]:
                    result["has_financials"] += 1
                if ticker_result["has_earnings"]:
                    result["has_earnings"] += 1
                if ticker_result["has_insider"]:
                    result["has_insider"] += 1
            else:
                result["failed"] += 1
                if ticker_result["error_type"] == "no_data":
                    result["no_data"] += 1
                if ticker_result["error"]:
                    result["errors"].append({
                        "ticker": ticker,
                        "error": ticker_result["error"][:200]  # Truncate
                    })
        
        if result["killed"]:
            break
        
        result["batches_completed"] += 1
        
        # Log progress every batch
        if (batch_idx + 1) % 5 == 0:
            logger.info(
                f"Progress: {result['processed']}/{len(tickers)} "
                f"(success={result['success']}, failed={result['failed']})"
            )
        
        # Delay between batches to avoid rate limiting
        if batch_idx < total_batches - 1 and delay_between_batches > 0:
            await asyncio.sleep(delay_between_batches)
    
    result["finished_at"] = datetime.now(timezone.utc).isoformat()
    result["duration_seconds"] = (datetime.now(timezone.utc) - started_at).total_seconds()
    
    # Truncate errors list for storage
    if len(result["errors"]) > 50:
        result["errors"] = result["errors"][:50]
        result["errors_truncated"] = True
    
    # Log to ops_job_runs
    await log_job_run(
        db,
        job_name=job_name,
        status="killed" if result["killed"] else "completed",
        details=result,
        started_at=started_at,
        finished_at=datetime.now(timezone.utc)
    )
    
    logger.info(
        f"Job {job_name} {'killed' if result['killed'] else 'completed'}: "
        f"processed={result['processed']}, success={result['success']}, "
        f"failed={result['failed']}, duration={result['duration_seconds']:.1f}s"
    )
    
    return result


async def get_tickers_for_sync(db, limit: Optional[int] = None, offset: int = 0) -> List[str]:
    """
    Get list of tickers that need fundamentals sync.
    Excludes already synced tickers.
    """
    # Get tickers from exchange-symbol-list that are Common Stock
    # For now, we'll use the EODHD API to get the full list
    from whitelist_service import fetch_exchange_symbols, filter_whitelist_candidates
    
    all_candidates = []
    for exchange in ["NYSE", "NASDAQ"]:
        symbols = await fetch_exchange_symbols(exchange)
        candidates = filter_whitelist_candidates(symbols, exchange)
        all_candidates.extend(candidates)
    
    # Get already synced tickers
    synced = await db.company_fundamentals_cache.distinct("ticker")
    synced_set = set(synced)
    
    # Filter out already synced
    tickers = [c["code"] for c in all_candidates if f"{c['code']}.US" not in synced_set]
    
    # Apply offset and limit
    if offset > 0:
        tickers = tickers[offset:]
    if limit:
        tickers = tickers[:limit]
    
    return tickers


async def get_job_status(db) -> Dict[str, Any]:
    """Get current batch job status."""
    # Get latest job run
    latest_job = await db.ops_job_runs.find_one(
        {"job_name": {"$regex": "^fundamentals"}},
        sort=[("started_at", -1)]
    )
    
    # Get counts
    from fundamentals_service import get_fundamentals_stats
    stats = await get_fundamentals_stats(db)
    
    return {
        "kill_switch_enabled": get_kill_switch(),
        "latest_job": {
            "job_name": latest_job.get("job_name") if latest_job else None,
            "status": latest_job.get("status") if latest_job else None,
            "started_at": latest_job.get("started_at").isoformat() if latest_job and latest_job.get("started_at") else None,
            "finished_at": latest_job.get("finished_at").isoformat() if latest_job and latest_job.get("finished_at") else None,
        } if latest_job else None,
        "fundamentals_stats": stats,
    }
