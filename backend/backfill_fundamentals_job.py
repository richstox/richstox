# ==============================================================================
# RICHSTOX FUNDAMENTALS BACKFILL (Complete)
# ==============================================================================
# BINDING: Uses whitelist_mapper.py - ONLY approved fields are stored.
# Status: LOCKED. No changes without explicit Richard approval.
# ==============================================================================

import asyncio
import httpx
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
import os

from whitelist_mapper import (
    apply_whitelist, 
    get_whitelist_document, 
    verify_whitelist_integrity,
    WHITELIST_VERSION
)
from provider_debug_service import upsert_provider_debug_snapshot

logger = logging.getLogger("richstox.backfill_fundamentals")

EODHD_API_KEY = os.environ.get("EODHD_API_KEY", "")
EODHD_BASE_URL = "https://eodhd.com/api"

# Rate limiting
REQUEST_DELAY_SECONDS = 0.2  # 5 requests per second max
BATCH_SIZE = 50  # Process in batches for progress reporting


async def fetch_fundamentals_from_eodhd(ticker: str) -> Optional[Dict[str, Any]]:
    """
    Fetch fundamentals from EODHD API.
    
    Args:
        ticker: Ticker symbol (e.g., "AAPL.US")
        
    Returns:
        Raw fundamentals payload or None if failed
    """
    url = f"{EODHD_BASE_URL}/fundamentals/{ticker}"
    params = {
        "api_token": EODHD_API_KEY,
        "fmt": "json"
    }
    
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(url, params=params)
            response.raise_for_status()
            return response.json()
    except Exception as e:
        logger.error(f"Failed to fetch fundamentals for {ticker}: {e}")
        return None


async def backfill_fundamentals_complete(
    db,
    triggered_by: str = "manual",
    target_tickers: Optional[List[str]] = None
) -> Dict[str, Any]:
    """
    Complete fundamentals backfill for all visible tickers.
    
    BINDING: Uses whitelist_mapper.py - ONLY approved fields are stored.
    
    Args:
        db: MongoDB database
        triggered_by: "admin_ui", "scheduler", "manual"
        target_tickers: Optional list of specific tickers (default: all visible)
        
    Returns:
        Job result dict for ops_job_runs
    """
    job_id = f"backfill_fundamentals_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    started_at = datetime.now(timezone.utc)
    
    # Verify whitelist integrity first
    whitelist_ok, whitelist_msg = verify_whitelist_integrity()
    if not whitelist_ok:
        return {
            "job_id": job_id,
            "status": "failed",
            "error": f"Whitelist integrity check failed: {whitelist_msg}",
            "started_at": started_at,
            "finished_at": datetime.now(timezone.utc)
        }
    
    # Get inventory snapshot BEFORE
    inventory_before = {
        "tracked_tickers": await db.tracked_tickers.count_documents({}),
        "visible_tickers": await db.tracked_tickers.count_documents({"is_visible": True}),
        "with_fundamentals": await db.tracked_tickers.count_documents({
            "is_visible": True,
            "fundamentals": {"$exists": True, "$ne": None}
        })
    }
    
    # Get target tickers - SKIP already processed ones (resume support)
    if target_tickers:
        tickers = target_tickers
    else:
        # Only get tickers that DON'T have the current whitelist version
        cursor = db.tracked_tickers.find(
            {
                "is_visible": True,
                "$or": [
                    {"fundamentals_whitelist_version": {"$exists": False}},
                    {"fundamentals_whitelist_version": {"$ne": WHITELIST_VERSION}}
                ]
            },
            {"ticker": 1}
        )
        tickers = [doc["ticker"] async for doc in cursor]
    
    total_tickers = len(tickers)
    logger.info(f"[{job_id}] Starting fundamentals backfill for {total_tickers} tickers")
    
    # Stats
    stats = {
        "tickers_targeted": total_tickers,
        "tickers_updated": 0,
        "tickers_failed": 0,
        "api_calls": 0,
        "total_fields_kept": 0,
        "total_fields_stripped": 0,
        "errors": [],
        "stripped_sections_summary": {}
    }
    
    # Process tickers in batches
    for i in range(0, total_tickers, BATCH_SIZE):
        batch = tickers[i:i + BATCH_SIZE]
        batch_num = (i // BATCH_SIZE) + 1
        total_batches = (total_tickers + BATCH_SIZE - 1) // BATCH_SIZE
        
        logger.info(f"[{job_id}] Processing batch {batch_num}/{total_batches}")
        
        for ticker in batch:
            try:
                # Fetch from EODHD
                raw_payload = await fetch_fundamentals_from_eodhd(ticker)
                stats["api_calls"] += 1
                
                if not raw_payload:
                    stats["tickers_failed"] += 1
                    stats["errors"].append({"ticker": ticker, "error": "API returned null"})
                    continue
                
                # Apply whitelist mapper
                filtered_payload, audit_info = apply_whitelist(raw_payload, ticker)

                await upsert_provider_debug_snapshot(
                    db=db,
                    ticker=ticker,
                    raw_payload=raw_payload,
                    source_job="backfill_fundamentals_complete",
                    audit_info=audit_info,
                )
                
                # Update stats
                stats["total_fields_kept"] += audit_info["fields_kept_count"]
                stats["total_fields_stripped"] += audit_info["fields_stripped_count"]
                
                for section in audit_info.get("sections_stripped", []):
                    stats["stripped_sections_summary"][section] = \
                        stats["stripped_sections_summary"].get(section, 0) + 1
                
                # Store in DB (replace fundamentals field)
                await db.tracked_tickers.update_one(
                    {"ticker": ticker},
                    {
                        "$set": {
                            "fundamentals": filtered_payload,
                            "fundamentals_updated_at": datetime.now(timezone.utc),
                            "fundamentals_whitelist_version": WHITELIST_VERSION
                        }
                    }
                )
                
                stats["tickers_updated"] += 1
                
                # Rate limiting
                await asyncio.sleep(REQUEST_DELAY_SECONDS)
                
            except Exception as e:
                logger.error(f"[{job_id}] Error processing {ticker}: {e}")
                stats["tickers_failed"] += 1
                stats["errors"].append({"ticker": ticker, "error": str(e)})
        
        # Progress log
        progress = (i + len(batch)) / total_tickers * 100
        logger.info(
            f"[{job_id}] Progress: {progress:.1f}% - "
            f"Updated: {stats['tickers_updated']}, Failed: {stats['tickers_failed']}"
        )
    
    finished_at = datetime.now(timezone.utc)
    
    # Get inventory snapshot AFTER
    inventory_after = {
        "tracked_tickers": await db.tracked_tickers.count_documents({}),
        "visible_tickers": await db.tracked_tickers.count_documents({"is_visible": True}),
        "with_fundamentals": await db.tracked_tickers.count_documents({
            "is_visible": True,
            "fundamentals": {"$exists": True, "$ne": None}
        })
    }
    
    # Build result
    result = {
        "job_id": job_id,
        "job_name": "backfill_fundamentals_complete",
        "status": "completed" if stats["tickers_failed"] == 0 else "completed_with_errors",
        "triggered_by": triggered_by,
        "started_at": started_at,
        "finished_at": finished_at,
        "duration_seconds": (finished_at - started_at).total_seconds(),
        "whitelist_version": WHITELIST_VERSION,
        "tickers_targeted": stats["tickers_targeted"],
        "tickers_updated": stats["tickers_updated"],
        "tickers_failed": stats["tickers_failed"],
        "api_calls": stats["api_calls"],
        "fields_kept_total": stats["total_fields_kept"],
        "fields_stripped_total": stats["total_fields_stripped"],
        "stripped_sections_summary": stats["stripped_sections_summary"],
        "errors": stats["errors"][:50],  # Limit errors stored
        "inventory_snapshot_before": inventory_before,
        "inventory_snapshot_after": inventory_after
    }
    
    # Log to ops_job_runs
    await db.ops_job_runs.insert_one(result)
    
    logger.info(
        f"[{job_id}] Completed: {stats['tickers_updated']} updated, "
        f"{stats['tickers_failed']} failed, {stats['api_calls']} API calls"
    )
    
    return result


async def verify_no_forbidden_fields(db) -> Dict[str, Any]:
    """
    Verify no forbidden fields exist in stored fundamentals.
    
    Returns:
        Verification result
    """
    from whitelist_mapper import FORBIDDEN_SECTIONS, GENERAL_FORBIDDEN_FIELDS
    
    violations = []
    
    # Sample check - look at 100 tickers
    cursor = db.tracked_tickers.find(
        {"fundamentals": {"$exists": True}},
        {"ticker": 1, "fundamentals": 1}
    ).limit(100)
    
    async for doc in cursor:
        ticker = doc["ticker"]
        fund = doc.get("fundamentals", {})
        
        # Check for forbidden sections
        for section in FORBIDDEN_SECTIONS:
            if section in fund:
                violations.append({
                    "ticker": ticker,
                    "violation": f"Contains forbidden section: {section}"
                })
        
        # Check for forbidden General fields
        general = fund.get("General", {})
        for field in GENERAL_FORBIDDEN_FIELDS:
            if field in general:
                violations.append({
                    "ticker": ticker,
                    "violation": f"Contains forbidden field: General.{field}"
                })
    
    return {
        "checked_tickers": 100,
        "violations_found": len(violations),
        "violations": violations[:20],  # Limit output
        "passed": len(violations) == 0
    }
