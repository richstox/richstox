"""
History Completeness Service — Canonical price-history truth model
==================================================================
Single source of truth for price-history completeness.  All admin UI
and ticker-page banners read the persisted fields written by this
service; no ad-hoc heuristics elsewhere.

Canonical fields persisted on ``tracked_tickers``:
  - price_history_complete        (bool)
  - price_history_first_date      (str YYYY-MM-DD | None)
  - price_history_last_date       (str YYYY-MM-DD | None)
  - price_history_missing_days_count (int)
  - price_history_last_verified_at   (datetime UTC)
  - price_history_status          (str — one of ALLOWED_STATUSES)

Canonical terms:
  - bulk_found_for_day: ticker present in the completed bulk file for a
    specific trading day (from gap_free_exclusions / bulk ingestion).
  - latest_bulk_day: the most recent completed closing day processed by
    the system (from pipeline_state.global_last_bulk_date_processed).
  - history_completeness: result of range-proof across expected trading
    days (from _get_bulk_processed_dates via market calendar).

Verification range:
  The expected verification range is from the ticker's
  ``history_download_proven_anchor`` (the last date covered by the
  full historical download) through the latest successfully processed
  bulk day.  Expected trading dates within that range come from the
  market calendar (``market_calendar`` collection, is_trading_day=True).
  Only dates that also appear in successful bulk runs with sufficient
  matched tickers (≥4000) are considered required.

Required-day rule:
  A day is *required* only when the ticker is present in bulk for that
  day AND the canonical bulk price used for eligibility (``close``) is
  > 0.  If the ticker is absent from bulk or close==0, the day is
  NOT APPLICABLE and excluded from the gap count.

Missing-day definition:
  A day is *missing* when it is a required day but no ``stock_prices``
  DB row exists for that (ticker, date) pair.

Price-field rule (consistency):
  The canonical price field is ``close`` everywhere.  ``adjusted_close``
  is used only for chart display normalization — never for eligibility
  or completeness decisions.
"""

import logging
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, List, Optional, Set

logger = logging.getLogger("richstox.history_completeness")


# ── Allowed status values (canonical enum) ──────────────────────────────────
# Every value that may be written to tracked_tickers.price_history_status
# MUST be listed here.  Documented for auditing.
ALLOWED_STATUSES = frozenset({
    "complete",                         # Range-proof passed, no missing days
    "incomplete",                       # Range-proof ran, missing days > 0
    "no_history_download",              # No proven historical download yet
    "no_bulk_dates",                    # No canonical bulk dates available
    "pending",                          # Awaiting first Phase C download
    "range_proof_failed",               # Phase C range-proof did not pass
    "auto_reflagged_missing_bulk_row",  # Auto-remediation reflagged
    "admin_forced_redownload",          # Admin forced redownload
    "malformed_purged",                 # Malformed data purged
    "error",                            # Error during Phase C
})


async def _get_bulk_processed_dates(db) -> List[str]:
    """Return canonical bulk-processed trading dates.

    Delegates to the shared implementation in admin_overview_service so
    there is exactly ONE definition of "which bulk dates count".
    """
    from services.admin_overview_service import _get_bulk_processed_dates as _impl
    return await _impl(db)


async def _get_gap_free_exclusions(
    db,
    tickers: List[str],
    expected_dates: List[str],
) -> Set[tuple]:
    """Return set of (ticker, date) pairs that are NOT APPLICABLE.

    A pair is excluded when the ticker is absent from bulk or has
    close==0 for that date — recorded in ``gap_free_exclusions``.
    """
    if not tickers or not expected_dates:
        return set()
    exclusion_set: Set[tuple] = set()
    try:
        cursor = db.gap_free_exclusions.find(
            {
                "ticker": {"$in": tickers},
                "date": {"$in": expected_dates},
            },
            {"_id": 0, "ticker": 1, "date": 1},
        )
        async for doc in cursor:
            exclusion_set.add((doc["ticker"], doc["date"]))
    except Exception:
        pass  # Collection may not exist yet
    return exclusion_set


async def verify_ticker_history_completeness(
    db,
    ticker: str,
    *,
    expected_dates: Optional[List[str]] = None,
    exclusion_set: Optional[Set[tuple]] = None,
) -> Dict[str, Any]:
    """Compute canonical history-completeness for a single ticker.

    Returns a dict with the canonical truth fields (not yet persisted).
    The caller is responsible for writing the result to tracked_tickers.

    Parameters
    ----------
    db : Motor database
    ticker : str — canonical ticker (e.g. "AAPL.US")
    expected_dates : pre-computed list from _get_bulk_processed_dates;
        if None, fetched internally (slower for batch usage).
    exclusion_set : pre-computed set of (ticker, date) NOT-APPLICABLE
        pairs; if None, fetched internally.
    """
    now = datetime.now(timezone.utc)

    # 1. Read the ticker's proof markers
    doc = await db.tracked_tickers.find_one(
        {"ticker": ticker},
        {
            "history_download_proven_at": 1,
            "history_download_proven_anchor": 1,
            "_id": 0,
        },
    )
    if not doc:
        return _result(
            status="no_history_download",
            complete=False,
            verified_at=now,
        )

    has_proof = doc.get("history_download_proven_at") is not None
    anchor = doc.get("history_download_proven_anchor") if has_proof else None
    if not has_proof or anchor is None:
        return _result(
            status="no_history_download",
            complete=False,
            verified_at=now,
        )

    # 2. Get expected bulk dates
    if expected_dates is None:
        expected_dates = await _get_bulk_processed_dates(db)
    if not expected_dates:
        return _result(
            status="no_bulk_dates",
            complete=False,
            verified_at=now,
        )

    # 3. Filter to dates after the anchor
    relevant_dates = sorted(d for d in expected_dates if d > anchor)

    # 4. Get the ticker's actual price dates within the relevant range
    actual_dates_set: set = set()
    if relevant_dates:
        cursor = db.stock_prices.aggregate([
            {"$match": {"ticker": ticker, "date": {"$in": relevant_dates}}},
            {"$group": {"_id": None, "dates": {"$addToSet": "$date"}}},
        ])
        async for agg_doc in cursor:
            actual_dates_set = set(agg_doc.get("dates", []))

    # 5. Load exclusions if not pre-loaded
    if exclusion_set is None:
        exclusion_set = await _get_gap_free_exclusions(
            db, [ticker], relevant_dates,
        )

    # 6. Compute missing required days
    missing_days = []
    for d in relevant_dates:
        if d in actual_dates_set:
            continue
        if (ticker, d) in exclusion_set:
            continue  # NOT APPLICABLE — absent from bulk or close==0
        missing_days.append(d)

    # 7. Get first/last dates from stock_prices
    agg_result = await db.stock_prices.aggregate([
        {"$match": {"ticker": ticker}},
        {"$group": {
            "_id": None,
            "first_date": {"$min": "$date"},
            "last_date": {"$max": "$date"},
        }},
    ]).to_list(1)
    first_date = agg_result[0]["first_date"] if agg_result else None
    last_date = agg_result[0]["last_date"] if agg_result else None

    complete = len(missing_days) == 0
    status = "complete" if complete else "incomplete"

    return _result(
        status=status,
        complete=complete,
        first_date=first_date,
        last_date=last_date,
        missing_days_count=len(missing_days),
        missing_days=missing_days,
        verified_at=now,
    )


def _result(
    *,
    status: str,
    complete: bool,
    first_date: Optional[str] = None,
    last_date: Optional[str] = None,
    missing_days_count: int = 0,
    missing_days: Optional[List[str]] = None,
    verified_at: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Build a canonical result dict."""
    return {
        "price_history_complete": complete,
        "price_history_first_date": first_date,
        "price_history_last_date": last_date,
        "price_history_missing_days_count": missing_days_count,
        "price_history_missing_days": missing_days or [],
        "price_history_last_verified_at": verified_at,
        "price_history_status": status,
    }


async def persist_ticker_completeness(
    db,
    ticker: str,
    result: Dict[str, Any],
) -> None:
    """Write the canonical truth fields to tracked_tickers."""
    await db.tracked_tickers.update_one(
        {"ticker": ticker},
        {"$set": {
            "price_history_complete": result["price_history_complete"],
            "price_history_first_date": result["price_history_first_date"],
            "price_history_last_date": result["price_history_last_date"],
            "price_history_missing_days_count": result["price_history_missing_days_count"],
            "price_history_last_verified_at": result["price_history_last_verified_at"],
            "price_history_status": result["price_history_status"],
        }},
    )


async def run_history_completeness_sweep(
    db,
    *,
    progress_cb: Optional[Callable[[int, int, str], Awaitable[None]]] = None,
) -> Dict[str, Any]:
    """Batch ops job: verify history completeness for all visible tickers.

    Intended to run at Step 2 end or Step 3.  Persists canonical truth
    fields on each tracked_ticker so the admin UI and ticker page can
    read them directly without re-computing.

    Performance: pre-loads expected_dates and exclusions once, then
    iterates tickers.  No global verifier sweep on every request.
    """
    started_at = datetime.now(timezone.utc)

    # 1. Get all visible tickers with proof markers
    visible_docs = await db.tracked_tickers.find(
        {"is_visible": True},
        {
            "ticker": 1,
            "history_download_proven_at": 1,
            "history_download_proven_anchor": 1,
            "_id": 0,
        },
    ).to_list(None)
    if not visible_docs:
        return {"status": "no_work", "total": 0, "complete": 0, "incomplete": 0}

    tickers = [d["ticker"] for d in visible_docs]
    ticker_info = {d["ticker"]: d for d in visible_docs}
    total = len(tickers)

    # 2. Pre-load expected dates and exclusions (single query each)
    expected_dates = await _get_bulk_processed_dates(db)

    exclusion_set: Set[tuple] = set()
    if expected_dates:
        exclusion_set = await _get_gap_free_exclusions(
            db, tickers, expected_dates,
        )

    # 3. Pre-load actual price dates for all tickers (single aggregation)
    dates_by_ticker: Dict[str, set] = {}
    if expected_dates:
        cursor = db.stock_prices.aggregate([
            {"$match": {"ticker": {"$in": tickers}, "date": {"$in": expected_dates}}},
            {"$group": {"_id": "$ticker", "dates": {"$addToSet": "$date"}}},
        ])
        async for agg_doc in cursor:
            dates_by_ticker[agg_doc["_id"]] = set(agg_doc.get("dates", []))

    # Pre-load first/last dates for all tickers
    first_last_by_ticker: Dict[str, Dict[str, Optional[str]]] = {}
    fl_cursor = db.stock_prices.aggregate([
        {"$match": {"ticker": {"$in": tickers}}},
        {"$group": {
            "_id": "$ticker",
            "first_date": {"$min": "$date"},
            "last_date": {"$max": "$date"},
        }},
    ])
    async for fl_doc in fl_cursor:
        first_last_by_ticker[fl_doc["_id"]] = {
            "first_date": fl_doc["first_date"],
            "last_date": fl_doc["last_date"],
        }

    # 4. Compute per-ticker and persist
    now = datetime.now(timezone.utc)
    complete_count = 0
    incomplete_count = 0
    no_proof_count = 0
    from pymongo import UpdateOne
    ops: List[Any] = []

    for i, ticker in enumerate(tickers):
        info = ticker_info[ticker]
        has_proof = info.get("history_download_proven_at") is not None
        anchor = info.get("history_download_proven_anchor") if has_proof else None

        if not has_proof or anchor is None:
            result = _result(
                status="no_history_download",
                complete=False,
                first_date=first_last_by_ticker.get(ticker, {}).get("first_date"),
                last_date=first_last_by_ticker.get(ticker, {}).get("last_date"),
                verified_at=now,
            )
            no_proof_count += 1
        elif not expected_dates:
            result = _result(
                status="no_bulk_dates",
                complete=False,
                first_date=first_last_by_ticker.get(ticker, {}).get("first_date"),
                last_date=first_last_by_ticker.get(ticker, {}).get("last_date"),
                verified_at=now,
            )
        else:
            relevant = sorted(d for d in expected_dates if d > anchor)
            actual = dates_by_ticker.get(ticker, set())
            missing = [
                d for d in relevant
                if d not in actual and (ticker, d) not in exclusion_set
            ]
            fl = first_last_by_ticker.get(ticker, {})
            complete = len(missing) == 0
            result = _result(
                status="complete" if complete else "incomplete",
                complete=complete,
                first_date=fl.get("first_date"),
                last_date=fl.get("last_date"),
                missing_days_count=len(missing),
                missing_days=missing,
                verified_at=now,
            )
            if complete:
                complete_count += 1
            else:
                incomplete_count += 1

        ops.append(UpdateOne(
            {"ticker": ticker},
            {"$set": {
                "price_history_complete": result["price_history_complete"],
                "price_history_first_date": result["price_history_first_date"],
                "price_history_last_date": result["price_history_last_date"],
                "price_history_missing_days_count": result["price_history_missing_days_count"],
                "price_history_last_verified_at": result["price_history_last_verified_at"],
                "price_history_status": result["price_history_status"],
            }},
        ))

        if progress_cb and (i + 1) % 500 == 0:
            await progress_cb(i + 1, total, "history_completeness_sweep")

    # Bulk write all updates
    if ops:
        await db.tracked_tickers.bulk_write(ops, ordered=False)

    finished_at = datetime.now(timezone.utc)
    duration_s = (finished_at - started_at).total_seconds()

    summary = {
        "status": "success",
        "total": total,
        "complete": complete_count,
        "incomplete": incomplete_count,
        "no_proof": no_proof_count,
        "expected_bulk_dates_count": len(expected_dates),
        "duration_seconds": round(duration_s, 2),
        "verified_at": now.isoformat() + "Z",
    }

    # Write audit record
    try:
        await db.ops_job_runs.insert_one({
            "job_name": "history_completeness_sweep",
            "status": "success",
            "started_at": started_at,
            "finished_at": finished_at,
            "duration_seconds": round(duration_s, 2),
            "details": summary,
        })
    except Exception as exc:
        logger.warning("Failed to write audit record: %s", exc)

    logger.info(
        "[history_completeness_sweep] Done: %d total, %d complete, "
        "%d incomplete, %d no_proof, %.1fs",
        total, complete_count, incomplete_count, no_proof_count, duration_s,
    )

    return summary
