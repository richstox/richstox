# ==============================================================================
# RICHSTOX DATA SUPREMACY MANIFESTO v1.0
# ==============================================================================
# BINDING: This is the ONLY source of truth for visibility.
# No improvements, no reinterpretation.
# ==============================================================================

from enum import Enum
from typing import Tuple, Optional, Dict, Any, List, Set
from datetime import datetime, timezone, timedelta
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
    """
    Deterministic visibility failure reason codes.

    Precedence order (checked top-to-bottom in compute_visibility):
      NOT_SEEDED → NO_PRICE_DATA → MISSING_SECTOR → MISSING_INDUSTRY
      → MISSING_SHARES → MISSING_CURRENCY → DELISTED
    """
    NOT_SEEDED = "NOT_SEEDED"
    NO_PRICE_DATA = "NO_PRICE_DATA"
    MISSING_SECTOR = "MISSING_SECTOR"
    MISSING_INDUSTRY = "MISSING_INDUSTRY"
    MISSING_SHARES = "MISSING_SHARES"
    MISSING_CURRENCY = "MISSING_CURRENCY"
    DELISTED = "DELISTED"

    # Legacy aliases kept for backward compatibility with stored DB values.
    INVALID_EXCHANGE = "INVALID_EXCHANGE"
    NOT_COMMON_STOCK = "NOT_COMMON_STOCK"
    MISSING_FINANCIAL_CURRENCY = "MISSING_FINANCIAL_CURRENCY"


# Canonical MongoDB query for visible tickers (runtime filter)
VISIBLE_TICKERS_QUERY = {"is_visible": True}


def compute_visibility(ticker_doc: dict) -> Tuple[bool, Optional[str]]:
    """
    Canonical visibility sieve — hard gates, fixed precedence.

    A ticker is visible (is_visible == True) ONLY when ALL 7 conditions hold:
      1. is_seeded == True       (NYSE/NASDAQ Common Stock seed)
      2. has_price_data == True  (current price data present)
      3. sector present          (not null/empty)
      4. industry present        (not null/empty)
      5. shares_outstanding > 0
      6. financial_currency present (not null/empty; any currency accepted)
      7. not delisted            (status != 'delisted' AND is_delisted != True)

    Returns (is_visible: bool, failed_reason: str | None).
    """
    # Gate 1: seeded universe (NYSE/NASDAQ Common Stock)
    if not ticker_doc.get("is_seeded", False):
        return False, VisibilityFailedReason.NOT_SEEDED.value

    # Gate 2: has current price data
    if not ticker_doc.get("has_price_data", False):
        return False, VisibilityFailedReason.NO_PRICE_DATA.value

    # Gate 3: sector present
    if not (ticker_doc.get("sector") or "").strip():
        return False, VisibilityFailedReason.MISSING_SECTOR.value

    # Gate 4: industry present
    if not (ticker_doc.get("industry") or "").strip():
        return False, VisibilityFailedReason.MISSING_INDUSTRY.value

    # Gate 5: shares_outstanding > 0
    shares = ticker_doc.get("shares_outstanding")
    try:
        if not shares or float(shares) <= 0:
            return False, VisibilityFailedReason.MISSING_SHARES.value
    except (ValueError, TypeError):
        return False, VisibilityFailedReason.MISSING_SHARES.value

    # Gate 6: financial_currency present (any currency accepted)
    if not (ticker_doc.get("financial_currency") or "").strip():
        return False, VisibilityFailedReason.MISSING_CURRENCY.value

    # Gate 7: not delisted
    status = (ticker_doc.get("status") or "").strip().lower()
    if ticker_doc.get("is_delisted", False) or status == "delisted":
        return False, VisibilityFailedReason.DELISTED.value

    return True, None

def compute_visibility_failed_reason(ticker_doc: dict) -> Optional[str]:
    """
    Return the deterministic visibility failure reason for a ticker doc,
    or None if the ticker passes all gates (is visible).

    Convenience wrapper around compute_visibility().
    """
    _, reason = compute_visibility(ticker_doc)
    return reason


# Backward-compatibility alias used by existing callers.
def compute_visibility_step4_only(ticker_doc: dict) -> Tuple[bool, Optional[str]]:
    """
    Deprecated alias for compute_visibility().

    Previously applied only P1 (data-quality) gates; now delegates to the
    full canonical sieve so that all callers enforce identical rules.
    """
    return compute_visibility(ticker_doc)


def get_canonical_sieve_query() -> dict:
    """
    MongoDB query equivalent of compute_visibility — all 7 hard gates.

    Used for:
    - count verification (universe_counts_service)
    - cursor filter in recompute_visibility_all

    Must stay in sync with compute_visibility().
    """
    return {
        "is_seeded": True,
        "has_price_data": True,
        "sector": {"$nin": [None, ""]},
        "industry": {"$nin": [None, ""]},
        "shares_outstanding": {"$gt": 0},
        "financial_currency": {"$nin": [None, ""]},
        "is_delisted": {"$ne": True},
        # Case-insensitive match: compute_visibility normalises status to lower-case
        # before comparing, so "Delisted", "DELISTED", etc. are all excluded.
        "status": {"$not": {"$regex": "^delisted$", "$options": "i"}},
    }


async def recompute_visibility_all(db, parent_run_id: Optional[str] = None) -> Dict[str, Any]:
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
    now = datetime.now(timezone.utc)

    # ------------------------------------------------------------------
    # 1) Stuck-finalize: mark any running run older than 15 min as failed
    # ------------------------------------------------------------------
    stale_cutoff = now - timedelta(minutes=15)
    stale_query = {
        "job_name": job_name,
        "status":   "running",
        "$or": [
            {"updated_at": {"$lt": stale_cutoff}},
            {"updated_at": {"$exists": False}, "started_at": {"$lt": stale_cutoff}},
        ],
    }
    async for stale_run in db.ops_job_runs.find(stale_query):
        await db.ops_job_runs.update_one(
            {"_id": stale_run["_id"], "status": "running"},
            {"$set": {
                "status":                        "failed",
                "error":                         "stuck_run_timeout",
                "finished_at":                   now,
                "end_time":                      now,
                "finished_at_prague":            now.astimezone(PRAGUE).isoformat(),
                "updated_at":                    now,
                "updated_at_prague":             now.astimezone(PRAGUE).isoformat(),
                "log_timezone":                  "Europe/Prague",
                "details.error":                 "stuck_run_timeout",
                "details.stuck_timeout_minutes": 15,
                "details.last_progress": {
                    "progress":           stale_run.get("progress"),
                    "progress_processed": stale_run.get("progress_processed"),
                    "progress_total":     stale_run.get("progress_total"),
                    "progress_pct":       stale_run.get("progress_pct"),
                },
            }},
        )
        logger.warning(
            f"Finalized stuck run {stale_run['_id']} as failed (stuck_run_timeout)"
        )

    # ------------------------------------------------------------------
    # 2) Concurrency guard (best-effort): skip if a fresh run is already active
    # ------------------------------------------------------------------
    running_run = await db.ops_job_runs.find_one(
        {"job_name": job_name, "status": "running"},
        sort=[("started_at", -1)],
    )
    if running_run is not None:
        freshness_time = running_run.get("updated_at") or running_run.get("started_at")
        if freshness_time is not None and freshness_time >= stale_cutoff:
            blocked_by_run_id = str(running_run["_id"])
            skipped_at = datetime.now(timezone.utc)
            await db.ops_job_runs.insert_one({
                "job_name":           job_name,
                "job_id":             job_id,
                "status":             "skipped",
                "started_at":         skipped_at,
                "started_at_prague":  skipped_at.astimezone(PRAGUE).isoformat(),
                "finished_at":        skipped_at,
                "finished_at_prague": skipped_at.astimezone(PRAGUE).isoformat(),
                "progress":           f"Skipped: {job_name} already running",
                "details": {
                    "skip_reason":       "already_running",
                    "blocked_by_run_id": blocked_by_run_id,
                },
            })
            logger.info(
                f"Skipping {job_name}: already running (blocked by {blocked_by_run_id})"
            )
            return {
                "job_id":            job_id,
                "duration_seconds":  0,
                "before":            {},
                "after":             {},
                "stats":             {"processed": 0, "now_visible": 0, "now_invisible": 0, "changed": 0, "reasons": {}},
                "cleanup":           {"stock_prices_deleted": 0, "financials_cache_deleted": 0},
                "space_freed_mb":    0,
                "parent_run_id":     parent_run_id,
                "status":            "skipped",
                "skip_reason":       "already_running",
                "blocked_by_run_id": blocked_by_run_id,
            }

    logger.info(f"Starting visibility recompute job: {job_id}")

    # Process the full seeded universe so every ticker gets an up-to-date
    # is_visible flag and visibility_failed_reason regardless of Step 3 state.
    _SEEDED_FILTER = {"is_seeded": True}

    total = await db.tracked_tickers.count_documents(_SEEDED_FILTER)

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
        "details":            {"parent_run_id": parent_run_id},
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

    async for ticker_doc in db.tracked_tickers.find(_SEEDED_FILTER):
        ticker = ticker_doc.get("ticker", "")
        is_visible, failed_reason = compute_visibility(ticker_doc)

        reason_key = failed_reason or "VISIBLE"
        stats["reasons"][reason_key] = stats["reasons"].get(reason_key, 0) + 1

        old_visible = ticker_doc.get("is_visible")
        if old_visible != is_visible:
            stats["changed"] += 1

        update_fields: Dict[str, Any] = {
            "is_visible":                is_visible,
            "visibility_updated_at":     datetime.now(timezone.utc),
            "visibility_reason_updated_at": datetime.now(timezone.utc),
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

        if processed % 50 == 0:
            hb_now = datetime.now(timezone.utc)
            progress_pct = int(processed * 100 / total) if total else 0
            await db.ops_job_runs.update_one(
                {"_id": run_doc_id},
                {"$set": {
                    "updated_at":         hb_now,
                    "updated_at_prague":  hb_now.astimezone(PRAGUE).isoformat(),
                    "progress_processed": processed,
                    "progress_total":     total,
                    "progress_pct":       progress_pct,
                    "progress":           f"Processed {processed:,} / {total:,} ({progress_pct}%)",
                }},
            )

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
        "parent_run_id":    parent_run_id,
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
