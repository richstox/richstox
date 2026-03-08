# ==============================================================================
# RICHSTOX DATA SUPREMACY MANIFESTO v1.0
# ==============================================================================
# BINDING: This is the ONLY source of truth for visibility.
# No improvements, no reinterpretation.
# ==============================================================================

from enum import Enum
from typing import Tuple, Optional, Dict, Any, List, Set
from datetime import datetime, timezone
import logging
import httpx
import os

logger = logging.getLogger("richstox.visibility")

# EODHD API config
EODHD_API_KEY = os.environ.get("EODHD_API_KEY", "")
EODHD_BASE_URL = "https://eodhd.com/api"

# Safety guard threshold
MINIMUM_MASTER_LIST_SIZE = 5000

# ==============================================================================
# PROTECTED_TICKERS - BINDING (IMMUTABLE)
# ==============================================================================
# These tickers are NEVER eligible for zombie deletion, even if not in
# the EODHD common-stock master list.
# 
# CANNOT be changed without explicit Richard approval.
# ==============================================================================
PROTECTED_TICKERS = {
    "SP500TR.INDX",      # S&P 500 Total Return benchmark
    # Add more as needed with Richard approval
}

# ==============================================================================
# ALIAS_MAP - BINDING (IMMUTABLE)
# ==============================================================================
# Symbol format normalization: DB format → EODHD format
# 
# EODHD uses hyphen for Class A/B shares: BRK-B
# We may store with dot: BRK.B
# 
# CANNOT be changed without explicit Richard approval.
# ==============================================================================
ALIAS_MAP = {
    # Berkshire Hathaway
    "BRK.B.US": "BRK-B.US",
    "BRK.A.US": "BRK-A.US",
    # Brown-Forman
    "BF.B.US": "BF-B.US",
    "BF.A.US": "BF-A.US",
    # Crawford & Company
    "CRD.A.US": "CRD-A.US",
    "CRD.B.US": "CRD-B.US",
    # Embotelladora Andina
    "AKO.A.US": "AKO-A.US",
    "AKO.B.US": "AKO-B.US",
    # Biglari Holdings
    "BH.A.US": "BH-A.US",
    # Federal Agricultural Mortgage
    "AGM.A.US": "AGM-A.US",
    # Add more as discovered
}

# Reverse map for lookups
ALIAS_MAP_REVERSE = {v: k for k, v in ALIAS_MAP.items()}


class VisibilityFailedReason(str, Enum):
    """Enum for visibility failure reasons."""
    INVALID_EXCHANGE = "INVALID_EXCHANGE"
    NOT_COMMON_STOCK = "NOT_COMMON_STOCK"
    NO_PRICE_DATA = "NO_PRICE_DATA"
    MISSING_SECTOR = "MISSING_SECTOR"
    MISSING_INDUSTRY = "MISSING_INDUSTRY"
    DELISTED = "DELISTED"
    # P1 DATA QUALITY FILTERS (2026-02-27):
    MISSING_SHARES = "MISSING_SHARES"
    MISSING_FINANCIAL_CURRENCY = "MISSING_FINANCIAL_CURRENCY"


# Canonical MongoDB query for visible tickers (runtime filter)
VISIBLE_TICKERS_QUERY = {"is_visible": True}


def compute_visibility(ticker_doc: dict) -> Tuple[bool, Optional[str]]:
    """
    Canonical visibility sieve.
    
    SEEDING: exchange ∈ {NYSE, NASDAQ} AND asset_type == "Common Stock"
    ACTIVITY: has_price_data == true
    QUALITY: sector AND industry present
    STATUS: is_delisted != true
    DATA QUALITY: shares_outstanding > 0 AND financial_currency present
    
    Returns:
        (is_visible: bool, visibility_failed_reason: str | None)
    """
    # SEEDING: exchange ∈ {NYSE, NASDAQ}
    exchange = ticker_doc.get("exchange", "")
    if exchange not in ["NYSE", "NASDAQ"]:
        return False, VisibilityFailedReason.INVALID_EXCHANGE.value
    
    # SEEDING: asset_type == "Common Stock"
    asset_type = ticker_doc.get("asset_type", "")
    if asset_type != "Common Stock":
        return False, VisibilityFailedReason.NOT_COMMON_STOCK.value
    
    # ACTIVITY: has_price_data == true
    has_price_data = ticker_doc.get("has_price_data", False)
    if not has_price_data:
        return False, VisibilityFailedReason.NO_PRICE_DATA.value
    
    # QUALITY: sector present
    sector = (ticker_doc.get("sector") or "").strip()
    if not sector:
        return False, VisibilityFailedReason.MISSING_SECTOR.value
    
    # QUALITY: industry present
    industry = (ticker_doc.get("industry") or "").strip()
    if not industry:
        return False, VisibilityFailedReason.MISSING_INDUSTRY.value
    
    # STATUS: is_delisted != true
    is_delisted = ticker_doc.get("is_delisted", False)
    if is_delisted:
        return False, VisibilityFailedReason.DELISTED.value
    
    # =========================================
    # DATA QUALITY: shares_outstanding > 0
    # =========================================
    shares = ticker_doc.get("shares_outstanding")
    if not shares:
        return False, VisibilityFailedReason.MISSING_SHARES.value
    try:
        if float(shares) <= 0:
            return False, VisibilityFailedReason.MISSING_SHARES.value
    except (ValueError, TypeError):
        return False, VisibilityFailedReason.MISSING_SHARES.value
    
    # =========================================
    # DATA QUALITY: financial_currency present
    # =========================================
    financial_currency = (ticker_doc.get("financial_currency") or "").strip()
    if not financial_currency:
        return False, VisibilityFailedReason.MISSING_FINANCIAL_CURRENCY.value
    
    # VISIBLE
    return True, None


def get_canonical_sieve_query() -> dict:
    """
    MongoDB query equivalent of compute_visibility.
    Used for count verification.
    """
    return {
        "exchange": {"$in": ["NYSE", "NASDAQ"]},
        "asset_type": "Common Stock",
        "has_price_data": True,
        "sector": {"$nin": [None, ""]},
        "industry": {"$nin": [None, ""]},
        "is_delisted": {"$ne": True},
        # P1 DATA QUALITY FILTERS:
        "$or": [
            {"fundamentals.SharesStats.SharesOutstanding": {"$gt": 0}},
            {"fundamentals.Highlights.SharesOutstanding": {"$gt": 0}},
        ],
        "financial_currency": {"$nin": [None, ""]},
    }


async def recompute_visibility_all(db) -> Dict[str, Any]:
    """
    Recompute is_visible for ALL tickers.

    Uses batched bulk_write (BATCH_SIZE ops per call) instead of per-ticker
    update_one — orders-of-magnitude faster for large universes.
    Progress is written to ops_job_runs after each batch so the Admin UI
    can display processed / total / pct in real-time.

    SAFE CLEANUP RULE:
    Pre-snapshot tickers that are already is_visible=false before this run.
    After recompute, only delete data for tickers in that pre-snapshot that
    are ALSO still is_visible=false after the run. Never delete data for a
    ticker that was visible at job start.
    """
    from pymongo import UpdateOne as _UpdateOne
    from zoneinfo import ZoneInfo

    BATCH_SIZE = 500
    PRAGUE = ZoneInfo("Europe/Prague")
    job_name = "compute_visible_universe"
    started_at = datetime.now(timezone.utc)
    job_id = f"recompute_visibility_{started_at.strftime('%Y%m%d_%H%M%S')}"

    logger.info(f"Starting visibility recompute job: {job_id}")

    # Canonical filter for "Classified tickers" — reuses the existing
    # get_canonical_sieve_query() defined in this file, which is the same
    # query used by the admin counts endpoint for Step 4.
    _CLASSIFIED_FILTER = get_canonical_sieve_query()

    total = await db.tracked_tickers.count_documents(_CLASSIFIED_FILTER)

    # Pre-snapshot: tickers that were already invisible BEFORE this run.
    # Cleanup is only allowed for members of this set.
    pre_invisible: Set[str] = set(
        await db.tracked_tickers.distinct("ticker", {"is_visible": False})
    )

    # Insert running sentinel — polled by frontend via /admin/jobs/{job_name}/status
    run_doc = await db.ops_job_runs.insert_one({
        "job_name":           job_name,
        "status":             "running",
        "started_at":         started_at,
        "started_at_prague":  started_at.astimezone(PRAGUE).isoformat(),
        "progress":           f"Starting… 0 / {total:,}",
        "progress_processed": 0,
        "progress_total":     total,
        "progress_pct":       0,
    })
    run_doc_id = run_doc.inserted_id

    # Snapshot BEFORE
    before = {
        "visible_count":          await db.tracked_tickers.count_documents({"is_visible": True}),
        "invisible_count":        len(pre_invisible),
        "stock_prices_count":     await db.stock_prices.count_documents({}),
        "financials_cache_count": await db.financials_cache.count_documents({}),
    }
    logger.info(f"Before: {before}")

    stats: Dict[str, Any] = {
        "processed": 0, "now_visible": 0, "now_invisible": 0,
        "changed": 0, "reasons": {}
    }
    now_invisible_set: Set[str] = set()  # tickers that are invisible after this run
    batch_ops: list = []
    processed = 0

    async for ticker_doc in db.tracked_tickers.find(_CLASSIFIED_FILTER):
        ticker = ticker_doc.get("ticker", "")
        is_visible, failed_reason = compute_visibility(ticker_doc)

        reason_key = failed_reason or "VISIBLE"
        stats["reasons"][reason_key] = stats["reasons"].get(reason_key, 0) + 1

        old_visible = ticker_doc.get("is_visible")
        if old_visible != is_visible:
            stats["changed"] += 1

        update_fields: Dict[str, Any] = {
            "is_visible":            is_visible,
            "visibility_updated_at": datetime.now(timezone.utc),
        }
        if is_visible:
            stats["now_visible"] += 1
            update_fields["visibility_failed_reason"] = None
        else:
            stats["now_invisible"] += 1
            update_fields["visibility_failed_reason"] = failed_reason
            now_invisible_set.add(ticker)

        batch_ops.append(_UpdateOne({"ticker": ticker}, {"$set": update_fields}))
        processed += 1
        stats["processed"] += 1

        if len(batch_ops) >= BATCH_SIZE:
            await db.tracked_tickers.bulk_write(batch_ops, ordered=False)
            batch_ops = []
            pct = round(processed / total * 100) if total > 0 else 0
            now_ts = datetime.now(timezone.utc)
            await db.ops_job_runs.update_one(
                {"_id": run_doc_id},
                {"$set": {
                    "progress":           f"Processed {processed:,} / {total:,} ({pct}%)",
                    "progress_processed": processed,
                    "progress_pct":       pct,
                    "updated_at":         now_ts,
                    "updated_at_prague":  now_ts.astimezone(PRAGUE).isoformat(),
                }},
            )
            if processed % 2000 == 0:
                logger.info(f"Processed {processed}/{total} tickers…")

    # Flush remaining batch
    if batch_ops:
        await db.tracked_tickers.bulk_write(batch_ops, ordered=False)

    # SAFE CLEANUP: only tickers that were already invisible at job start
    # AND are still invisible after the recompute.
    tickers_to_cleanup = list(pre_invisible & now_invisible_set)
    logger.info(f"Cleanup candidates: {len(tickers_to_cleanup)} "
                f"(pre_invisible={len(pre_invisible)}, still_invisible={len(now_invisible_set)})")

    cleanup_stats: Dict[str, int] = {"stock_prices_deleted": 0, "financials_cache_deleted": 0}
    if tickers_to_cleanup:
        r = await db.stock_prices.delete_many({"ticker": {"$in": tickers_to_cleanup}})
        cleanup_stats["stock_prices_deleted"] = r.deleted_count
        r = await db.financials_cache.delete_many({"ticker": {"$in": tickers_to_cleanup}})
        cleanup_stats["financials_cache_deleted"] = r.deleted_count
        logger.info(f"Deleted {cleanup_stats['stock_prices_deleted']} prices, "
                    f"{cleanup_stats['financials_cache_deleted']} financials")

    # Snapshot AFTER
    after = {
        "visible_count":          await db.tracked_tickers.count_documents({"is_visible": True}),
        "invisible_count":        await db.tracked_tickers.count_documents({"is_visible": False}),
        "stock_prices_count":     await db.stock_prices.count_documents({}),
        "financials_cache_count": await db.financials_cache.count_documents({}),
    }
    logger.info(f"After: {after}")

    completed_at = datetime.now(timezone.utc)
    duration_seconds = (completed_at - started_at).total_seconds()

    space_freed_mb = round(
        (cleanup_stats["stock_prices_deleted"] * 150 +
         cleanup_stats["financials_cache_deleted"] * 1500) / 1024 / 1024, 2
    )

    result: Dict[str, Any] = {
        "job_id":           job_id,
        "duration_seconds": duration_seconds,
        "before":           before,
        "after":            after,
        "stats":            stats,
        "cleanup":          cleanup_stats,
        "space_freed_mb":   space_freed_mb,
    }

    # Finalize sentinel
    await db.ops_job_runs.update_one(
        {"_id": run_doc_id},
        {"$set": {
            "status":             "completed",
            "finished_at":        completed_at,
            "finished_at_prague": completed_at.astimezone(PRAGUE).isoformat(),
            "progress":           f"Done: {stats['now_visible']:,} visible / {total:,} total",
            "progress_processed": total,
            "progress_total":     total,
            "progress_pct":       100,
            "details":            result,
        }},
    )

    logger.info(f"Job {job_id} completed in {duration_seconds:.1f}s. "
                f"Space freed: {space_freed_mb} MB")
    return result


# ==============================================================================
# NO ZOMBIE TICKERS RULE - BINDING
# ==============================================================================
# If a ticker is NOT in EODHD master list (NYSE + NASDAQ Common Stock):
# - Delete from tracked_tickers
# - Delete from stock_prices (all historical records)
# - Delete from fundamentals caches
# 
# SAFETY GUARD: Only run if master list has >= 5,000 tickers
# ==============================================================================

async def fetch_eodhd_master_list() -> Tuple[Set[str], Optional[str]]:
    """
    Fetch current master list of tickers from EODHD API.
    Returns: (set of tickers with .US suffix, error message or None)
    """
    tickers = set()
    
    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            for exchange in ["NYSE", "NASDAQ"]:
                url = f"{EODHD_BASE_URL}/exchange-symbol-list/{exchange}"
                params = {"api_token": EODHD_API_KEY, "fmt": "json"}
                
                response = await client.get(url, params=params)
                
                if response.status_code != 200:
                    return set(), f"EODHD API error for {exchange}: HTTP {response.status_code}"
                
                data = response.json()
                
                if not isinstance(data, list):
                    return set(), f"Unexpected response format for {exchange}"
                
                # Filter: Common Stock only, add .US suffix
                for item in data:
                    if item.get("Type") == "Common Stock":
                        code = item.get("Code", "")
                        if code:
                            tickers.add(f"{code}.US")
                
                logger.info(f"Fetched {len(data)} symbols from {exchange}, {len([i for i in data if i.get('Type') == 'Common Stock'])} Common Stock")
        
        return tickers, None
        
    except Exception as e:
        return set(), f"Exception fetching master list: {str(e)}"


async def clean_zombie_tickers(db) -> Dict[str, Any]:
    """
    BINDING: No Zombie Tickers Rule
    
    Delete ALL data for tickers that are not in EODHD master list.
    
    SAFETY GUARDS (in order):
    1. Protected check: If ticker in PROTECTED_TICKERS → SKIP
    2. Alias normalization: Apply ALIAS_MAP before master-list lookup
    3. Master list check: Use normalized symbol
    4. Minimum master list size: >= 5,000 tickers
    
    Returns audit output with detailed stats.
    """
    job_id = f"clean_zombies_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    started_at = datetime.now(timezone.utc)
    
    logger.info(f"[{job_id}] Starting No Zombie Tickers cleanup...")
    logger.info(f"[{job_id}] Protected tickers: {PROTECTED_TICKERS}")
    logger.info(f"[{job_id}] Alias map entries: {len(ALIAS_MAP)}")
    
    # Step 1: Fetch EODHD master list
    logger.info(f"[{job_id}] Fetching EODHD master list (NYSE + NASDAQ Common Stock)...")
    master_list, error = await fetch_eodhd_master_list()
    
    if error:
        logger.error(f"[{job_id}] ABORT: {error}")
        return {
            "job_id": job_id,
            "status": "aborted",
            "reason": error,
            "message": "Safety guard triggered - no deletions performed"
        }
    
    # SAFETY GUARD: Must have >= 5,000 tickers
    if len(master_list) < MINIMUM_MASTER_LIST_SIZE:
        logger.error(f"[{job_id}] ABORT: Master list has only {len(master_list)} tickers (minimum: {MINIMUM_MASTER_LIST_SIZE})")
        return {
            "job_id": job_id,
            "status": "aborted",
            "reason": f"Master list too small: {len(master_list)} < {MINIMUM_MASTER_LIST_SIZE}",
            "message": "Safety guard triggered - API may be down or returning partial data"
        }
    
    logger.info(f"[{job_id}] Master list OK: {len(master_list)} tickers")
    
    # Step 2: Get all tickers from our DB
    db_tickers = set()
    async for doc in db.tracked_tickers.find({}, {"ticker": 1}):
        db_tickers.add(doc["ticker"])
    
    logger.info(f"[{job_id}] DB has {len(db_tickers)} tickers")
    
    # Step 3: Process each ticker with safety checks
    zombies = []
    protected_skips = []
    alias_normalizations = []
    kept_tickers = []
    
    for ticker in db_tickers:
        # SAFETY CHECK 1: Protected tickers
        if ticker in PROTECTED_TICKERS:
            protected_skips.append(ticker)
            logger.info(f"[{job_id}] PROTECTED_SKIP: {ticker}")
            continue
        
        # SAFETY CHECK 2: Alias normalization
        normalized_ticker = ALIAS_MAP.get(ticker, ticker)
        if normalized_ticker != ticker:
            alias_normalizations.append({"db": ticker, "normalized": normalized_ticker})
            logger.info(f"[{job_id}] ALIAS_NORMALIZED: {ticker} → {normalized_ticker}")
        
        # SAFETY CHECK 3: Master list check (using normalized ticker)
        if normalized_ticker in master_list:
            kept_tickers.append(ticker)
            continue
        
        # Not protected, not in master list (even after normalization) → ZOMBIE
        zombies.append(ticker)
    
    logger.info(f"[{job_id}] Protected skips: {len(protected_skips)}")
    logger.info(f"[{job_id}] Alias normalizations: {len(alias_normalizations)}")
    logger.info(f"[{job_id}] Found {len(zombies)} zombie tickers")
    
    if not zombies:
        logger.info(f"[{job_id}] No zombies found - database is clean!")
        return {
            "job_id": job_id,
            "status": "completed",
            "master_list_size": len(master_list),
            "db_tickers": len(db_tickers),
            "protected_skips": protected_skips,
            "alias_normalizations": alias_normalizations,
            "zombies_found": 0,
            "message": "No zombie tickers - database is clean"
        }
    
    # Step 4: Delete all zombie data
    deleted_tickers = []
    total_prices_deleted = 0
    total_fundamentals_deleted = 0
    
    for ticker in zombies:
        ticker_stats = {"ticker": ticker, "prices": 0, "fundamentals": 0, "action": "deleted", "reason": "not_in_master_list"}
        
        # Delete from stock_prices
        result = await db.stock_prices.delete_many({"ticker": ticker})
        ticker_stats["prices"] = result.deleted_count
        total_prices_deleted += result.deleted_count
        
        # Delete from company_fundamentals_cache (if exists)
        try:
            result = await db.company_fundamentals_cache.delete_many({"ticker": ticker})
            ticker_stats["fundamentals"] = result.deleted_count
            total_fundamentals_deleted += result.deleted_count
        except:
            pass
        
        # Delete from tracked_tickers
        await db.tracked_tickers.delete_one({"ticker": ticker})
        
        deleted_tickers.append(ticker_stats)
        logger.info(f"[{job_id}] DELETED: {ticker} (prices: {ticker_stats['prices']}, fundamentals: {ticker_stats['fundamentals']})")
    
    # Step 5: Estimate space freed
    space_freed_mb = round(
        (total_prices_deleted * 150 + total_fundamentals_deleted * 1500) / 1024 / 1024, 2
    )
    
    completed_at = datetime.now(timezone.utc)
    duration_seconds = (completed_at - started_at).total_seconds()
    
    # Build result
    result = {
        "job_id": job_id,
        "status": "completed",
        "started_at": started_at,
        "completed_at": completed_at,
        "duration_seconds": duration_seconds,
        "master_list_size": len(master_list),
        "db_tickers_before": len(db_tickers),
        "db_tickers_after": len(db_tickers) - len(zombies),
        
        # Safety checks results
        "protected_skips": protected_skips,
        "alias_normalizations": alias_normalizations,
        
        # Cleanup stats
        "zombies_cleaned": {
            "tickers_deleted": len(zombies),
            "price_records_deleted": total_prices_deleted,
            "fundamentals_deleted": total_fundamentals_deleted,
            "space_freed_mb": space_freed_mb,
            "deleted_tickers": [t["ticker"] for t in deleted_tickers]
        }
    }
    
    # Log to ops_job_runs
    await db.ops_job_runs.insert_one({
        "job_type": "clean_zombie_tickers",
        "job_id": job_id,
        **result
    })
    
    # Final report
    logger.info("=" * 80)
    logger.info(f"[{job_id}] ZOMBIES CLEANED - FINAL REPORT")
    logger.info("=" * 80)
    logger.info(f"Master list size: {len(master_list)}")
    logger.info(f"Protected skips: {len(protected_skips)} - {protected_skips}")
    logger.info(f"Alias normalizations: {len(alias_normalizations)}")
    logger.info(f"Tickers deleted: {len(zombies)}")
    logger.info(f"Price records deleted: {total_prices_deleted:,}")
    logger.info(f"Fundamentals deleted: {total_fundamentals_deleted:,}")
    logger.info(f"Disk space freed: ~{space_freed_mb} MB")
    logger.info(f"Deleted tickers: {[t['ticker'] for t in deleted_tickers]}")
    logger.info("=" * 80)
    
    return result


async def recompute_visibility_with_zombie_cleanup(db) -> Dict[str, Any]:
    """
    Combined job: Recompute visibility + Clean zombie tickers.
    
    Order:
    1. Clean zombie tickers first (removes tickers not in master list)
    2. Then recompute visibility for remaining tickers
    
    This ensures we don't process zombies during visibility recompute.
    """
    logger.info("Starting combined visibility recompute + zombie cleanup...")
    
    # Step 1: Clean zombies
    zombie_result = await clean_zombie_tickers(db)
    
    # Step 2: Recompute visibility (only if zombie cleanup succeeded or found no zombies)
    if zombie_result.get("status") in ["completed"]:
        visibility_result = await recompute_visibility_all(db)
        
        return {
            "zombie_cleanup": zombie_result,
            "visibility_recompute": visibility_result
        }
    else:
        return {
            "zombie_cleanup": zombie_result,
            "visibility_recompute": "skipped - zombie cleanup aborted"
        }
