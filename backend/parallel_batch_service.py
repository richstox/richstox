# ==============================================================================
# 🛡️ SCHEDULER-ONLY FILE - EXTERNAL API CALLS ALLOWED
# ==============================================================================
# This file is in ALLOWLIST: /app/scripts/audit_external_calls.py
# May call: eodhd.com/api/* (via httpx)
# Context: ONLY from scheduler.py jobs or admin backfill endpoints
# Runtime API endpoints MUST NOT import from this file
# ==============================================================================
"""
RICHSTOX Parallel Batch Service
================================
Optimized parallel data fetching with:
- Concurrent requests (10 tickers in parallel)
- Retry logic (2 retries before marking as failed)
- Rate limit handling with exponential backoff
- Progress tracking (log every 500 tickers)
- Safety stops: rate-limit >30s, error rate >5%, max runtime 4h

EODHD Rate Limits:
- API calls are limited based on plan (typically 100K/day)
- Recommended: 10 concurrent requests with 0.1s delay
"""

import os
import logging
import asyncio
import time
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional, Callable
from dataclasses import dataclass, field
import httpx
from zoneinfo import ZoneInfo

PRAGUE_TZ = ZoneInfo("Europe/Prague")

logger = logging.getLogger("richstox.parallel_batch")

EODHD_BASE_URL = "https://eodhd.com/api"
EODHD_API_KEY = os.getenv("EODHD_API_KEY", "")

# Configuration
DEFAULT_CONCURRENCY = 10  # Concurrent requests
DEFAULT_RETRY_COUNT = 2   # Retries before marking as failed
DEFAULT_BACKOFF_BASE = 1.0  # Base backoff in seconds
DEFAULT_BACKOFF_MAX = 30.0  # Max backoff in seconds
PROGRESS_LOG_INTERVAL = 500  # Log every N tickers
BULK_CHUNK_SIZE = 5000  # Rows per bulk write

# Safety stops
MAX_RUNTIME_HOURS = 4  # Max job runtime
MAX_ERROR_RATE_PCT = 5  # Stop if error rate exceeds this
MAX_BACKOFF_THRESHOLD = 30  # Stop if backoff exceeds this (seconds)


@dataclass
class BatchResult:
    """Result of a parallel batch operation."""
    job_name: str
    started_at: datetime
    finished_at: Optional[datetime] = None
    total_tickers: int = 0
    processed: int = 0
    success: int = 0
    failed: int = 0
    retried: int = 0
    rate_limited: int = 0
    total_records: int = 0
    api_calls: int = 0
    batches_completed: int = 0
    killed: bool = False
    kill_reason: Optional[str] = None
    errors: List[Dict[str, Any]] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "job_name": self.job_name,
            "started_at": self.started_at.isoformat(),
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
            "duration_seconds": (self.finished_at - self.started_at).total_seconds() if self.finished_at else None,
            "total_tickers": self.total_tickers,
            "processed": self.processed,
            "success": self.success,
            "failed": self.failed,
            "retried": self.retried,
            "rate_limited": self.rate_limited,
            "total_records": self.total_records,
            "api_calls": self.api_calls,
            "batches_completed": self.batches_completed,
            "killed": self.killed,
            "kill_reason": self.kill_reason,
            "errors": self.errors[:50],  # Truncate
            "errors_truncated": len(self.errors) > 50,
        }


@dataclass
class TickerResult:
    """Result of processing a single ticker."""
    ticker: str
    success: bool
    records: int = 0
    retries: int = 0
    error: Optional[str] = None
    rate_limited: bool = False


async def fetch_with_retry(
    url: str,
    params: Dict[str, Any],
    max_retries: int = DEFAULT_RETRY_COUNT,
    backoff_base: float = DEFAULT_BACKOFF_BASE,
    backoff_max: float = DEFAULT_BACKOFF_MAX,
) -> tuple[Optional[Any], int, bool]:
    """
    Fetch URL with retry logic and rate limit handling.
    
    Returns:
        (data, retries_used, was_rate_limited)
    """
    retries = 0
    was_rate_limited = False
    
    async with httpx.AsyncClient(timeout=60) as client:
        while retries <= max_retries:
            try:
                response = await client.get(url, params=params)
                
                # Rate limit handling
                if response.status_code == 429:
                    was_rate_limited = True
                    backoff = min(backoff_base * (2 ** retries), backoff_max)
                    logger.warning(f"Rate limited. Backing off for {backoff:.1f}s (retry {retries + 1}/{max_retries})")
                    await asyncio.sleep(backoff)
                    retries += 1
                    continue
                
                # 404 = no data (not an error)
                if response.status_code == 404:
                    return None, retries, was_rate_limited
                
                response.raise_for_status()
                data = response.json()
                return data, retries, was_rate_limited
                
            except httpx.TimeoutException:
                retries += 1
                if retries <= max_retries:
                    backoff = min(backoff_base * (2 ** retries), backoff_max)
                    logger.warning(f"Timeout. Retrying in {backoff:.1f}s (retry {retries}/{max_retries})")
                    await asyncio.sleep(backoff)
            except Exception as e:
                retries += 1
                if retries <= max_retries:
                    backoff = min(backoff_base * (2 ** retries), backoff_max)
                    logger.warning(f"Error: {e}. Retrying in {backoff:.1f}s (retry {retries}/{max_retries})")
                    await asyncio.sleep(backoff)
                else:
                    raise
    
    return None, retries, was_rate_limited


def normalize_ticker_for_eodhd(ticker: str) -> str:
    """
    Normalize ticker symbol for EODHD API.
    
    Handles special cases like:
    - BRK.B → BRK-B (Berkshire Hathaway Class B)
    - BRK.A → BRK-A (Berkshire Hathaway Class A)
    - Other class shares with dots → dashes
    """
    ticker_full = ticker if ticker.endswith(".US") else f"{ticker}.US"
    
    # Replace dots in ticker symbol (except the .US suffix)
    # BRK.B.US → BRK-B.US
    base = ticker_full[:-3]  # Remove .US
    base_normalized = base.replace(".", "-")
    return f"{base_normalized}.US"


async def fetch_ticker_prices(ticker: str) -> tuple[List[Dict], int, bool]:
    """
    Fetch full price history for a single ticker with retry.
    
    Returns:
        (price_records, retries_used, was_rate_limited)
    """
    ticker_full = normalize_ticker_for_eodhd(ticker)
    
    url = f"{EODHD_BASE_URL}/eod/{ticker_full}"
    params = {
        "api_token": EODHD_API_KEY,
        "fmt": "json",
    }
    
    data, retries, rate_limited = await fetch_with_retry(url, params)
    
    if data and isinstance(data, list):
        return data, retries, rate_limited
    return [], retries, rate_limited


async def ensure_price_indexes(db):
    """
    Ensure optimal indexes exist on stock_prices collection.
    Creates compound unique index on (ticker, date) for fast upserts.
    """
    try:
        # Check if index already exists
        indexes = await db.stock_prices.index_information()
        if 'ticker_date_unique' not in indexes:
            logger.info("Creating index ticker_date_unique on stock_prices...")
            await db.stock_prices.create_index(
                [("ticker", 1), ("date", 1)],
                unique=True,
                name="ticker_date_unique",
                background=True
            )
            logger.info("Index ticker_date_unique created successfully")
        else:
            logger.debug("Index ticker_date_unique already exists")
    except Exception as e:
        logger.warning(f"Failed to create index (may already exist): {e}")


async def process_ticker_prices(db, ticker: str) -> TickerResult:
    """
    Process a single ticker: fetch prices and upsert to DB.
    OPTIMIZED: Uses bulk upsert with larger batch sizes.
    
    Note: Uses normalized ticker for API (BRK.B → BRK-B) but stores
    with original format for consistency with whitelist.
    """
    # Normalize for storage (add .US if missing)
    ticker_storage = ticker if ticker.endswith(".US") else f"{ticker}.US"
    # Normalize for API (BRK.B → BRK-B)
    ticker_api = normalize_ticker_for_eodhd(ticker)
    
    result = TickerResult(ticker=ticker_storage, success=False)
    
    try:
        prices, retries, rate_limited = await fetch_ticker_prices(ticker)
        result.retries = retries
        result.rate_limited = rate_limited
        
        if not prices:
            result.success = True  # No data is not an error
            return result
        
        # Prepare documents for bulk upsert
        from pymongo import UpdateOne
        operations = []
        
        for record in prices:
            date = record.get("date")
            if not date:
                continue
            
            # Store with normalized ticker (ticker_api for consistency)
            parsed = {
                "ticker": ticker_api,
                "date": date,
                "open": float(record.get("open", 0)) if record.get("open") else None,
                "high": float(record.get("high", 0)) if record.get("high") else None,
                "low": float(record.get("low", 0)) if record.get("low") else None,
                "close": float(record.get("close", 0)) if record.get("close") else None,
                "adjusted_close": float(record.get("adjusted_close", 0)) if record.get("adjusted_close") else None,
                "volume": int(record.get("volume", 0)) if record.get("volume") else None,
            }
            
            operations.append(UpdateOne(
                {"ticker": ticker_api, "date": date},
                {"$set": parsed},
                upsert=True
            ))
        
        # Execute bulk write in chunks for efficiency
        if operations:
            for i in range(0, len(operations), BULK_CHUNK_SIZE):
                chunk = operations[i:i + BULK_CHUNK_SIZE]
                await db.stock_prices.bulk_write(chunk, ordered=False)
        
        result.records = len(operations)
        result.success = True
        
    except Exception as e:
        result.error = str(e)[:200]
        logger.error(f"Error processing {ticker_api}: {e}")
    
    return result


async def run_parallel_price_backfill(
    db,
    tickers: List[str],
    concurrency: int = DEFAULT_CONCURRENCY,
    job_name: str = "parallel_price_backfill",
    check_kill_switch: Optional[Callable] = None,
    max_runtime_hours: float = MAX_RUNTIME_HOURS,
) -> BatchResult:
    """
    Run parallel price backfill with concurrent requests and safety stops.
    
    Args:
        db: MongoDB database
        tickers: List of tickers to backfill
        concurrency: Number of concurrent requests (default 10)
        job_name: Name for logging
        check_kill_switch: Optional async function to check if job should stop
        max_runtime_hours: Maximum runtime before auto-stop (default 4h)
    
    Safety stops:
        - Rate-limit backoff >30s
        - Error rate >5%
        - Max runtime (default 4 hours)
    
    Returns:
        BatchResult with statistics
    """
    started_at = datetime.now(timezone.utc)
    max_end_time = started_at + timedelta(hours=max_runtime_hours)
    
    result = BatchResult(
        job_name=job_name,
        started_at=started_at,
        total_tickers=len(tickers),
    )
    
    # Track max backoff seen
    max_backoff_seen = 0.0
    
    logger.info(f"Starting {job_name}: {len(tickers)} tickers, concurrency={concurrency}, max_runtime={max_runtime_hours}h")
    
    # Ensure indexes exist for optimal upsert performance
    await ensure_price_indexes(db)
    
    # Track timing for rows/sec calculation
    write_start_time = time.time()
    
    # Process in parallel batches
    semaphore = asyncio.Semaphore(concurrency)
    
    async def process_with_semaphore(ticker: str) -> TickerResult:
        async with semaphore:
            return await process_ticker_prices(db, ticker)
    
    # Process all tickers with controlled concurrency
    pending = []
    for i, ticker in enumerate(tickers):
        # === SAFETY CHECK 1: Kill switch ===
        if check_kill_switch and await check_kill_switch(db):
            result.killed = True
            result.kill_reason = "Kill switch engaged"
            logger.warning(f"{job_name} STOPPED: Kill switch engaged at {i}/{len(tickers)}")
            break
        
        # === SAFETY CHECK 2: Max runtime ===
        if datetime.now(timezone.utc) >= max_end_time:
            result.killed = True
            result.kill_reason = f"Max runtime exceeded ({max_runtime_hours}h)"
            logger.warning(f"{job_name} STOPPED: Max runtime {max_runtime_hours}h exceeded at {i}/{len(tickers)}")
            break
        
        # === SAFETY CHECK 3: Error rate ===
        if result.processed > 100:  # Only check after 100 tickers
            error_rate = (result.failed / result.processed) * 100
            if error_rate > MAX_ERROR_RATE_PCT:
                result.killed = True
                result.kill_reason = f"Error rate {error_rate:.1f}% > {MAX_ERROR_RATE_PCT}%"
                logger.warning(f"{job_name} STOPPED: Error rate {error_rate:.1f}% exceeded threshold at {i}/{len(tickers)}")
                break
        
        pending.append(process_with_semaphore(ticker))
        
        # Process in micro-batches for progress tracking
        if len(pending) >= concurrency * 2:
            batch_results = await asyncio.gather(*pending, return_exceptions=True)
            
            for br in batch_results:
                if isinstance(br, Exception):
                    result.failed += 1
                    result.errors.append({"error": str(br)[:200]})
                else:
                    result.processed += 1
                    result.api_calls += 1
                    
                    if br.success:
                        result.success += 1
                        result.total_records += br.records
                    else:
                        result.failed += 1
                        if br.error:
                            result.errors.append({"ticker": br.ticker, "error": br.error})
                    
                    if br.retries > 0:
                        result.retried += 1
                        # Track max backoff (approximate from retries)
                        approx_backoff = DEFAULT_BACKOFF_BASE * (2 ** br.retries)
                        max_backoff_seen = max(max_backoff_seen, approx_backoff)
                    
                    if br.rate_limited:
                        result.rate_limited += 1
                        
                        # === SAFETY CHECK 4: Rate limit backoff ===
                        if max_backoff_seen > MAX_BACKOFF_THRESHOLD:
                            result.killed = True
                            result.kill_reason = f"Rate limit backoff {max_backoff_seen:.1f}s > {MAX_BACKOFF_THRESHOLD}s"
                            logger.warning(f"{job_name} STOPPED: Rate limit backoff exceeded at {result.processed}/{len(tickers)}")
                            break
            
            if result.killed:
                break
                
            result.batches_completed += 1
            pending = []
            
            # Progress logging every PROGRESS_LOG_INTERVAL tickers
            if result.processed % PROGRESS_LOG_INTERVAL == 0 and result.processed > 0:
                elapsed = (datetime.now(timezone.utc) - started_at).total_seconds()
                rate = result.processed / elapsed if elapsed > 0 else 0
                error_rate = (result.failed / result.processed) * 100 if result.processed > 0 else 0
                eta_remaining = (len(tickers) - result.processed) / rate if rate > 0 else 0
                logger.info(
                    f"Progress: {result.processed}/{len(tickers)} "
                    f"({result.processed/len(tickers)*100:.1f}%) "
                    f"| success={result.success}, failed={result.failed} ({error_rate:.1f}%) "
                    f"| rate_limited={result.rate_limited} "
                    f"| {rate:.1f} tickers/s, ETA: {eta_remaining/60:.0f}min"
                )
            
            # Small delay between batches
            await asyncio.sleep(0.05)
    
    # Process remaining
    if pending and not result.killed:
        batch_results = await asyncio.gather(*pending, return_exceptions=True)
        
        for br in batch_results:
            if isinstance(br, Exception):
                result.failed += 1
                result.errors.append({"error": str(br)[:200]})
            else:
                result.processed += 1
                result.api_calls += 1
                
                if br.success:
                    result.success += 1
                    result.total_records += br.records
                else:
                    result.failed += 1
                    if br.error:
                        result.errors.append({"ticker": br.ticker, "error": br.error})
                
                if br.retries > 0:
                    result.retried += 1
                if br.rate_limited:
                    result.rate_limited += 1
        
        result.batches_completed += 1
    
    result.finished_at = datetime.now(timezone.utc)
    
    # Calculate performance metrics
    duration = (result.finished_at - started_at).total_seconds()
    write_duration = time.time() - write_start_time
    rows_per_sec = result.total_records / write_duration if write_duration > 0 else 0
    tickers_per_sec = result.processed / duration if duration > 0 else 0
    error_rate = (result.failed / result.processed) * 100 if result.processed > 0 else 0
    
    # Log final summary with rows/sec
    status_msg = "KILLED" if result.killed else "COMPLETED"
    logger.info(
        f"{job_name} {status_msg}: "
        f"processed={result.processed}/{len(tickers)}, success={result.success}, "
        f"failed={result.failed} ({error_rate:.1f}%), records={result.total_records:,}, "
        f"duration={duration:.1f}s ({duration/60:.1f}min), "
        f"tickers/s={tickers_per_sec:.2f}, rows/s={rows_per_sec:,.0f}"
    )
    if result.killed:
        logger.warning(f"  Kill reason: {result.kill_reason}")
    
    # Log to ops_job_runs with performance metrics
    await db.ops_job_runs.insert_one({
        "job_type": job_name,
        "source": "scheduler",
        "status": "killed" if result.killed else "completed",
        "started_at": started_at,
        "finished_at": result.finished_at,
        "duration_seconds": duration,
        "performance": {
            "tickers_per_sec": round(tickers_per_sec, 2),
            "rows_per_sec": round(rows_per_sec, 0),
            "write_duration_sec": round(write_duration, 2),
            "error_rate_pct": round(error_rate, 2),
            "max_backoff_seen": round(max_backoff_seen, 2),
        },
        "safety_stops": {
            "max_runtime_hours": max_runtime_hours,
            "max_error_rate_pct": MAX_ERROR_RATE_PCT,
            "max_backoff_threshold": MAX_BACKOFF_THRESHOLD,
        },
        "details": result.to_dict(),
    })
    
    return result


async def get_tickers_without_full_prices(db, limit: int = None) -> List[str]:
    """
    Get ALL whitelisted tickers that need price backfill.
    
    Returns tickers from tracked_tickers (whitelist) that:
    1. Have no price data at all
    2. Have less than 252 price records (less than 1 year)
    
    NOTE: Uses WHITELIST (6,542 tickers), not just fundamentals (2,349)!
    """
    # Get ALL whitelisted tickers (not just those with fundamentals)
    all_whitelisted = await db.tracked_tickers.distinct("ticker")
    logger.info(f"Total whitelisted tickers: {len(all_whitelisted)}")
    
    # Get tickers with price counts
    pipeline = [
        {"$group": {"_id": "$ticker", "count": {"$sum": 1}}},
    ]
    price_counts = await db.stock_prices.aggregate(pipeline).to_list(length=10000)
    price_count_map = {r["_id"]: r["count"] for r in price_counts}
    
    # Find tickers needing backfill
    missing_prices = []  # No prices at all
    sparse_prices = []   # Less than 1 year of data
    
    for ticker in all_whitelisted:
        ticker_us = ticker if ticker.endswith(".US") else f"{ticker}.US"
        count = price_count_map.get(ticker_us, 0)
        
        if count == 0:
            missing_prices.append(ticker_us)
        elif count < 252:
            sparse_prices.append(ticker_us)
    
    # Prioritize: missing first, then sparse
    result = missing_prices + sparse_prices
    
    logger.info(f"Tickers needing backfill: {len(missing_prices)} missing, {len(sparse_prices)} sparse, total={len(result)}")
    
    if limit:
        return result[:limit]
    return result  # Return all if no limit


# ─── Full Backfill Baseline ──────────────────────────────────────────────────
# The "full_backfill_baseline" document in pipeline_state is the canonical
# marker for the last successful full price-history backfill.  It is written
# ONLY by run_scheduled_backfill_all_prices() on a fully successful (not
# killed, not skipped) completion.  Failed / partial / interrupted runs MUST
# NOT update this baseline.
#
# Fields:
#   _id:           "full_backfill_baseline"
#   completed_at:  datetime (UTC) when the backfill finished
#   completed_at_prague: ISO string in Europe/Prague
#   through_date:  latest trading day covered (from pipeline_state.price_bulk)
#   job_run_id:    reference to the ops_job_runs document for this run
# ─────────────────────────────────────────────────────────────────────────────

FULL_BACKFILL_BASELINE_ID = "full_backfill_baseline"


async def _write_full_backfill_baseline(db, finished_at: datetime, job_run_id: str) -> None:
    """
    Persist the canonical baseline marker after a fully successful backfill.
    Reads through_date from pipeline_state.price_bulk.
    """
    bulk_state = await db.pipeline_state.find_one({"_id": "price_bulk"})
    through_date = (bulk_state or {}).get("global_last_bulk_date_processed")

    if finished_at.tzinfo is None:
        finished_at = finished_at.replace(tzinfo=timezone.utc)
    prague_iso = finished_at.astimezone(PRAGUE_TZ).isoformat()

    await db.pipeline_state.update_one(
        {"_id": FULL_BACKFILL_BASELINE_ID},
        {"$set": {
            "_id": FULL_BACKFILL_BASELINE_ID,
            "completed_at": finished_at,
            "completed_at_prague": prague_iso,
            "through_date": through_date,
            "job_run_id": job_run_id,
        }},
        upsert=True,
    )
    logger.info(
        "Full backfill baseline written: through_date=%s, job_run_id=%s",
        through_date, job_run_id,
    )


async def run_scheduled_backfill_all_prices(db) -> Dict[str, Any]:
    """
    Scheduled job: Backfill ALL prices until completion.
    
    Runs at 05:00 Prague time. Continues until all tickers complete.
    Uses parallel fetching with safety stops.
    
    Safety stops:
    - Rate-limit backoff >30s
    - Error rate >5%
    - Max runtime: 4 hours
    
    Config:
    - NO LIMIT (runs until completion)
    - Concurrency: 10 parallel requests
    - Bulk chunks: 5,000 rows
    - Progress log: every 500 tickers
    """
    from scheduler_service import get_scheduler_enabled
    
    started_at = datetime.now(timezone.utc)
    job_type = "scheduled_backfill_all_prices"
    
    # Job configuration
    CONCURRENCY = 10  # parallel API requests
    
    logger.info(f"Starting {job_type} (NO LIMIT - run until completion, concurrency={CONCURRENCY})")
    
    # Check kill switch
    if not await get_scheduler_enabled(db):
        logger.warning(f"{job_type} skipped: kill switch engaged")
        return {
            "job_type": job_type,
            "status": "skipped",
            "reason": "kill_switch_engaged",
            "started_at": started_at.isoformat(),
        }
    
    # Get ALL tickers that need backfill (no limit)
    tickers = await get_tickers_without_full_prices(db, limit=None)
    
    if not tickers:
        logger.info(f"{job_type}: All tickers have full price history ✅")
        finished_at = datetime.now(timezone.utc)
        job_run_doc = {
            "job_type": job_type,
            "source": "scheduler",
            "status": "completed",
            "started_at": started_at,
            "finished_at": finished_at,
            "details": {"message": "All tickers have full price history", "processed": 0},
        }
        insert_result = await db.ops_job_runs.insert_one(job_run_doc)
        # A) Baseline: all tickers already complete → successful full backfill
        await _write_full_backfill_baseline(db, finished_at, str(insert_result.inserted_id))
        return {
            "job_type": job_type,
            "status": "completed",
            "message": "All tickers have full price history",
            "processed": 0,
        }
    
    # Run parallel backfill
    async def check_kill(d):
        return not await get_scheduler_enabled(d)
    
    result = await run_parallel_price_backfill(
        db,
        tickers,
        concurrency=CONCURRENCY,
        job_name=job_type,
        check_kill_switch=check_kill,
    )
    
    # A) Baseline: write only on fully successful completion (not killed)
    if not result.killed and result.finished_at:
        # Retrieve the job_run_id just inserted by run_parallel_price_backfill
        job_run_doc = await db.ops_job_runs.find_one(
            {"job_type": job_type, "finished_at": result.finished_at},
            {"_id": 1},
            sort=[("finished_at", -1)],
        )
        job_run_id = str(job_run_doc["_id"]) if job_run_doc else "unknown"
        await _write_full_backfill_baseline(db, result.finished_at, job_run_id)
    
    return result.to_dict()
