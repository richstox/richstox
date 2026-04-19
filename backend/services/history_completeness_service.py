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
from typing import Any, Awaitable, Callable, Dict, List, Optional, Set, Tuple

logger = logging.getLogger("richstox.history_completeness")


# ── Allowed status values (canonical enum) ──────────────────────────────────
# Every value that may be written to tracked_tickers.price_history_status
# MUST be listed here.  Documented for auditing.
ALLOWED_STATUSES = frozenset({
    "complete",                         # Range-proof passed, no missing days
    "incomplete",                       # Range-proof ran, missing days > 0
    "missing_trading_days",             # Internal calendar gap detected
    "no_history_download",              # No proven historical download yet
    "no_bulk_dates",                    # No canonical bulk dates available
    "pending",                          # Awaiting first Phase C download
    "range_proof_failed",               # Phase C range-proof did not pass
    "auto_reflagged_missing_bulk_row",  # Auto-remediation reflagged
    "admin_forced_redownload",          # Admin forced redownload
    "malformed_purged",                 # Malformed data purged
    "cleanup_reset",                    # Data deleted by visibility cleanup
    "error",                            # Error during Phase C
    "history_data_lost",                # Stored proof stale — historical rows missing
})

# ── Threshold for data-loss detection (days) ──────────────────────────────
# When the live stock_prices first_date is more than this many days AFTER
# the stored range_proof.db_first_date, the sweep treats the stored proof
# as stale (historical data was deleted without clearing the proof fields).
# 30 days is generous enough to avoid false positives from close=0 gaps.
_DATA_LOSS_THRESHOLD_DAYS = 30

# ── Fields cleared when data-loss is detected ────────────────────────────
# These Phase-C-owned proof fields become stale when the underlying
# stock_prices rows are deleted.  The sweep resets them so Phase C will
# re-download the full history on its next run.
STALE_PROOF_RESET_FIELDS = {
    "range_proof": None,
    "history_download_records": None,
    "history_download_proven_at": None,
    "history_download_proven_anchor": None,
    "full_history_downloaded_at": None,
    "full_history_source": None,
    "full_history_version": None,
    "history_download_completed": False,
    "gap_free_since_history_download": False,
    "price_history_complete_as_of": None,
    "needs_price_redownload": True,
}


# ── Internal gap detection constants ──────────────────────────────────────
# Number of recent trading days (from the last price date backward) in
# which an internal calendar gap will immediately flag the ticker as
# "missing_trading_days" and trigger a redownload.
_INTERNAL_GAP_RECENT_WINDOW = 30

# Maximum number of missing calendar dates persisted in the sample
# (for admin/proof diagnostics).
_MAX_MISSING_SAMPLE = 10


async def _get_calendar_trading_dates(
    db,
    start_date: str,
    end_date: str,
    market: str = "US",
) -> List[str]:
    """Return sorted trading dates from market_calendar in [start_date, end_date].

    Only dates with ``is_trading_day=True`` are returned.  If the calendar
    has no rows in the range the result is empty (fail-closed).
    """
    docs = await db.market_calendar.find(
        {
            "market": market,
            "is_trading_day": True,
            "date": {"$gte": start_date, "$lte": end_date},
        },
        {"date": 1, "_id": 0},
    ).sort("date", 1).to_list(None)
    return [d["date"] for d in docs]


async def _get_bulk_processed_dates(db) -> List[str]:
    """Return canonical bulk-processed trading dates.

    Delegates to the shared implementation in admin_overview_service so
    there is exactly ONE definition of "which bulk dates count".

    NOTE: uses a deferred import to avoid a circular import at module
    load time.  This is the standard pattern used across the codebase
    (see full_sync_service.py, credit_log_service.py, etc.).
    """
    from services.admin_overview_service import _get_bulk_processed_dates as _impl
    return await _impl(db)


async def _get_gap_free_exclusions(
    db,
    tickers: List[str],
    expected_dates: List[str],
) -> Tuple[Set[tuple], Set[tuple]]:
    """Return ``(exclusion_set, not_in_bulk_set)`` for the given tickers/dates.

    *exclusion_set* — set of ``(ticker, date)`` pairs that are NOT APPLICABLE
    (absent from bulk or close==0).

    *not_in_bulk_set* — subset of *exclusion_set* where ``bulk_found`` is
    ``False`` (ticker was not in the EODHD bulk file at all for that date).
    These dates may still need a full-history redownload to confirm whether
    the ticker genuinely did not trade.
    """
    exclusion_set: Set[tuple] = set()
    not_in_bulk_set: Set[tuple] = set()
    if not tickers or not expected_dates:
        return exclusion_set, not_in_bulk_set
    try:
        cursor = db.gap_free_exclusions.find(
            {
                "ticker": {"$in": tickers},
                "date": {"$in": expected_dates},
            },
            {"_id": 0, "ticker": 1, "date": 1, "bulk_found": 1},
        )
        async for doc in cursor:
            pair = (doc["ticker"], doc["date"])
            exclusion_set.add(pair)
            if doc.get("bulk_found") is False:
                not_in_bulk_set.add(pair)
    except Exception:
        pass  # Collection may not exist yet
    return exclusion_set, not_in_bulk_set


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

    # 1. Read the ticker's proof markers + range_proof for data-loss check
    doc = await db.tracked_tickers.find_one(
        {"ticker": ticker},
        {
            "history_download_proven_at": 1,
            "history_download_proven_anchor": 1,
            "range_proof": 1,
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
    not_in_bulk_set: Set[tuple] = set()
    if exclusion_set is None:
        exclusion_set, not_in_bulk_set = await _get_gap_free_exclusions(
            db, [ticker], relevant_dates,
        )

    # 6. Compute missing required days
    missing_days = []
    # Count dates excluded as "not in bulk" where stock_prices is also
    # missing — these need a full-history redownload to verify whether
    # the ticker genuinely did not trade on those days.
    not_in_bulk_excluded_count = 0
    for d in relevant_dates:
        if d in actual_dates_set:
            continue
        if (ticker, d) in exclusion_set:
            if (ticker, d) in not_in_bulk_set:
                not_in_bulk_excluded_count += 1
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

    # 8. Data-loss cross-check: stored range_proof vs live first_date
    #    If the live first_date is much later than the stored
    #    range_proof.db_first_date, historical rows were deleted without
    #    clearing the proof fields.  Declare data loss so Phase C re-downloads.
    data_lost = _detect_data_loss(doc.get("range_proof"), first_date)
    if data_lost:
        return _result(
            status="history_data_lost",
            complete=False,
            first_date=first_date,
            last_date=last_date,
            verified_at=now,
        )

    complete = len(missing_days) == 0
    status = "complete" if complete else "incomplete"

    # ── 9. Internal calendar-gap check ───────────────────────────────
    # Even when the bulk-based check passes, an internal gap may exist
    # for trading days that were never bulk-processed (e.g. the bulk
    # run was missing or had no data for that date).  Use the market
    # calendar as the authoritative source of expected trading days
    # in a recent window [anchor+1 .. last_price_date].
    calendar_missing: List[str] = []
    if complete and last_date and anchor:
        calendar_missing = await _check_internal_calendar_gaps(
            db, ticker, anchor, last_date,
        )
        if calendar_missing:
            complete = False
            status = "missing_trading_days"
            missing_days = calendar_missing
    # ────────────────────────────────────────────────────────────────

    result = _result(
        status=status,
        complete=complete,
        first_date=first_date,
        last_date=last_date,
        missing_days_count=len(missing_days),
        missing_days=missing_days,
        verified_at=now,
    )
    # Attach not-in-bulk metadata so callers can trigger redownload.
    result["not_in_bulk_excluded_count"] = not_in_bulk_excluded_count
    # Attach calendar-gap sample for diagnostics.
    result["calendar_gap_missing_sample"] = calendar_missing[:_MAX_MISSING_SAMPLE]
    return result


async def _check_internal_calendar_gaps(
    db,
    ticker: str,
    anchor: str,
    last_date: str,
) -> List[str]:
    """Check for internal gaps using the market calendar as source of truth.

    For the window ``(anchor, last_date]`` (bounded to the most recent
    ``_INTERNAL_GAP_RECENT_WINDOW`` trading days), build the expected
    trading dates from ``market_calendar`` and verify that a
    ``stock_prices`` row exists for each.

    Returns a sorted list of missing trading-day date strings.  An empty
    list means no internal gaps were found.
    """
    if not anchor or not last_date or last_date <= anchor:
        return []

    # Day after anchor (anchor itself was validated by the full-history download)
    from datetime import timedelta
    anchor_dt = datetime.strptime(anchor, "%Y-%m-%d")
    start_date = (anchor_dt + timedelta(days=1)).strftime("%Y-%m-%d")

    # Bound the calendar query to a reasonable window: use at most
    # 50 calendar days before last_date to ensure we capture at least
    # _INTERNAL_GAP_RECENT_WINDOW trading days while keeping the query small.
    last_dt = datetime.strptime(last_date, "%Y-%m-%d")
    bounded_start = (last_dt - timedelta(days=50)).strftime("%Y-%m-%d")
    effective_start = max(start_date, bounded_start)

    # Get expected trading days from market calendar
    calendar_dates = await _get_calendar_trading_dates(db, effective_start, last_date)
    if not calendar_dates:
        return []

    # Bound to recent window to limit DB load
    if len(calendar_dates) > _INTERNAL_GAP_RECENT_WINDOW:
        calendar_dates = calendar_dates[-_INTERNAL_GAP_RECENT_WINDOW:]

    # Fetch actual price dates for these calendar dates
    actual_dates_set: set = set()
    cursor = db.stock_prices.aggregate([
        {"$match": {"ticker": ticker, "date": {"$in": calendar_dates}}},
        {"$group": {"_id": None, "dates": {"$addToSet": "$date"}}},
    ])
    async for agg_doc in cursor:
        actual_dates_set = set(agg_doc.get("dates", []))

    # Compute missing
    missing = sorted(d for d in calendar_dates if d not in actual_dates_set)
    return missing


def _detect_data_loss(
    range_proof: Optional[Dict[str, Any]],
    live_first_date: Optional[str],
) -> bool:
    """Return True when stored range_proof is stale vs live stock_prices.

    Data loss is detected when the live stock_prices first_date is more
    than ``_DATA_LOSS_THRESHOLD_DAYS`` after the range_proof's
    db_first_date.  This means the historical rows that the proof
    originally validated have been deleted (e.g. by visibility cleanup)
    without clearing the proof fields.
    """
    if not range_proof or not live_first_date:
        return False
    proof_first = range_proof.get("db_first_date")
    if not proof_first:
        return False
    try:
        _pf = datetime.strptime(proof_first, "%Y-%m-%d")
        _lf = datetime.strptime(live_first_date, "%Y-%m-%d")
        gap_days = (_lf - _pf).days
        return gap_days > _DATA_LOSS_THRESHOLD_DAYS
    except (ValueError, TypeError):
        return False


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
    set_fields = {
        "price_history_complete": result["price_history_complete"],
        "price_history_first_date": result["price_history_first_date"],
        "price_history_last_date": result["price_history_last_date"],
        "price_history_missing_days_count": result["price_history_missing_days_count"],
        "price_history_last_verified_at": result["price_history_last_verified_at"],
        "price_history_status": result["price_history_status"],
    }
    # Persist missing-day sample for admin/proof diagnostics
    cal_sample = result.get("calendar_gap_missing_sample")
    if cal_sample:
        set_fields["price_history_missing_days_sample"] = cal_sample
    else:
        # Clear stale sample when the ticker is now complete
        set_fields["price_history_missing_days_sample"] = []
    # If missing_trading_days status, flag for immediate redownload
    if result.get("price_history_status") == "missing_trading_days":
        set_fields["needs_price_redownload"] = True
    await db.tracked_tickers.update_one(
        {"ticker": ticker},
        {"$set": set_fields},
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

    # 1. Get all visible tickers with proof markers + range_proof
    visible_docs = await db.tracked_tickers.find(
        {"is_visible": True},
        {
            "ticker": 1,
            "history_download_proven_at": 1,
            "history_download_proven_anchor": 1,
            "range_proof": 1,
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
    not_in_bulk_set: Set[tuple] = set()
    if expected_dates:
        exclusion_set, not_in_bulk_set = await _get_gap_free_exclusions(
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
    data_lost_count = 0
    not_in_bulk_redownload_count = 0
    calendar_gap_redownload_count = 0
    from pymongo import UpdateOne
    ops: List[Any] = []

    for i, ticker in enumerate(tickers):
        info = ticker_info[ticker]
        has_proof = info.get("history_download_proven_at") is not None
        anchor = info.get("history_download_proven_anchor") if has_proof else None

        # ── Data-loss cross-check (runs before all other branches) ────
        # If stored range_proof says first_date=2021-01-15 but live DB
        # first_date=2026-04-13, the historical rows were deleted without
        # clearing the proof fields.  Treat as data-loss.
        fl = first_last_by_ticker.get(ticker, {})
        live_first = fl.get("first_date")
        data_lost = _detect_data_loss(info.get("range_proof"), live_first)

        # Per-ticker not-in-bulk excluded count (used to decide redownload)
        _ticker_not_in_bulk_excluded = 0

        if data_lost:
            result = _result(
                status="history_data_lost",
                complete=False,
                first_date=live_first,
                last_date=fl.get("last_date"),
                verified_at=now,
            )
            data_lost_count += 1
            # For data-loss tickers, clear stale proof fields and flag
            # for Phase C redownload in addition to the normal fields.
            set_fields = {
                "price_history_complete": result["price_history_complete"],
                "price_history_first_date": result["price_history_first_date"],
                "price_history_last_date": result["price_history_last_date"],
                "price_history_missing_days_count": result["price_history_missing_days_count"],
                "price_history_last_verified_at": result["price_history_last_verified_at"],
                "price_history_status": result["price_history_status"],
                **STALE_PROOF_RESET_FIELDS,
            }
            ops.append(UpdateOne({"ticker": ticker}, {"$set": set_fields}))
            if data_lost_count <= 20:
                logger.warning(
                    "[history_completeness_sweep] %s: DATA LOSS detected — "
                    "range_proof.db_first_date=%s but live first_date=%s. "
                    "Clearing stale proof fields, flagging for redownload.",
                    ticker,
                    (info.get("range_proof") or {}).get("db_first_date"),
                    live_first,
                )
        elif not has_proof or anchor is None:
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
            # Count not-in-bulk excluded dates where stock_prices is also
            # missing.  These are dates that the full-history download has
            # not yet covered (d > anchor).  A redownload is needed so
            # Phase C can verify whether the ticker genuinely did not trade.
            for d in relevant:
                if d in actual:
                    continue
                if (ticker, d) in not_in_bulk_set:
                    _ticker_not_in_bulk_excluded += 1
            fl = first_last_by_ticker.get(ticker, {})
            complete = len(missing) == 0

            # ── Internal calendar-gap check (sweep path) ─────────────
            # Even when the bulk-based check passes, verify that the
            # market calendar has no unaccounted-for trading days without
            # a stock_prices row.  This catches the CEV.US scenario where
            # a trading day (e.g. 2026-04-15) was never bulk-processed
            # but IS a real trading day.
            cal_gap_missing: List[str] = []
            if complete and fl.get("last_date") and anchor:
                cal_gap_missing = await _check_internal_calendar_gaps(
                    db, ticker, anchor, fl["last_date"],
                )
                if cal_gap_missing:
                    complete = False
                    missing = cal_gap_missing
            # ─────────────────────────────────────────────────────────

            _status = "complete" if complete else (
                "missing_trading_days" if cal_gap_missing else "incomplete"
            )
            result = _result(
                status=_status,
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

        if not data_lost:
            set_fields = {
                "price_history_complete": result["price_history_complete"],
                "price_history_first_date": result["price_history_first_date"],
                "price_history_last_date": result["price_history_last_date"],
                "price_history_missing_days_count": result["price_history_missing_days_count"],
                "price_history_last_verified_at": result["price_history_last_verified_at"],
                "price_history_status": result["price_history_status"],
            }
            # ── Not-in-bulk redownload trigger ────────────────────────
            # If there are dates excluded as "not in bulk" (bulk_found=false)
            # and the stock_prices row is also missing, the full history
            # hasn't been verified for those dates yet (d > anchor).  Flag
            # the ticker for Phase C redownload so it can confirm whether
            # the ticker genuinely did not trade on those days.
            #
            # After Phase C runs, the anchor moves past these dates.  If
            # the date is still not in stock_prices (EODHD history doesn't
            # have it), it falls behind the anchor and is no longer checked
            # — confirmed as a non-trading day.  No infinite loop.
            if _ticker_not_in_bulk_excluded > 0:
                set_fields["needs_price_redownload"] = True
                not_in_bulk_redownload_count += 1
                if not_in_bulk_redownload_count <= 20:
                    logger.info(
                        "[history_completeness_sweep] %s: %d not-in-bulk "
                        "excluded date(s) with missing stock_prices row — "
                        "flagging needs_price_redownload=True for Phase C "
                        "verification",
                        ticker,
                        _ticker_not_in_bulk_excluded,
                    )
            # ── Calendar-gap redownload trigger ───────────────────────
            # If the internal calendar-gap check found missing trading
            # days, immediately flag for redownload so Phase C can
            # backfill the gap.  Also persist a sample of the missing
            # dates for admin/proof diagnostics.
            if result.get("price_history_status") == "missing_trading_days":
                _cal_missing = result.get("price_history_missing_days", [])
                set_fields["needs_price_redownload"] = True
                set_fields["price_history_missing_days_sample"] = (
                    _cal_missing[:_MAX_MISSING_SAMPLE]
                )
                calendar_gap_redownload_count += 1
                if calendar_gap_redownload_count <= 20:
                    logger.warning(
                        "[history_completeness_sweep] %s: %d internal "
                        "calendar-gap missing trading day(s) — "
                        "flagging needs_price_redownload=True. "
                        "Sample: %s",
                        ticker,
                        len(_cal_missing),
                        _cal_missing[:5],
                    )
            elif result.get("price_history_complete"):
                # Clear stale missing-days sample when the ticker is now
                # complete (e.g. after Phase C backfilled the gap).
                set_fields["price_history_missing_days_sample"] = []
            ops.append(UpdateOne({"ticker": ticker}, {"$set": set_fields}))

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
        "data_lost": data_lost_count,
        "not_in_bulk_redownload": not_in_bulk_redownload_count,
        "calendar_gap_redownload": calendar_gap_redownload_count,
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
        "%d incomplete, %d no_proof, %d data_lost, "
        "%d not_in_bulk_redownload, %d calendar_gap_redownload, %.1fs",
        total, complete_count, incomplete_count, no_proof_count,
        data_lost_count, not_in_bulk_redownload_count,
        calendar_gap_redownload_count, duration_s,
    )

    return summary
