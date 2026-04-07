# ==============================================================================
# Proof Mode Service — Single-ticker/date reconciliation
# ==============================================================================
# Diagnoses why a ticker might appear in EODHD bulk data but be missing
# from stock_prices.  Returns a structured report covering:
#   1. EODHD bulk presence (boolean + matched symbol string)
#   2. stock_prices presence (boolean + count + sample _id)
#   3. Canonical normalization applied at each stage
#   4. Skip reason counters when bulk contains it but DB doesn't
# ==============================================================================

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set

logger = logging.getLogger("richstox.proof_mode")


# ---------------------------------------------------------------------------
# Normalization (mirrors price_ingestion_service._normalize_step2_ticker)
# ---------------------------------------------------------------------------
def _normalize_ticker(value: Any) -> Optional[str]:
    """
    Canonical Step 2 ticker normalization:
    uppercase, trim, always end with .US.
    """
    if value is None:
        return None
    text = str(value).strip().upper()
    if not text:
        return None
    if text.endswith(".US"):
        text = text[:-3]
    return f"{text}.US"


# ---------------------------------------------------------------------------
# Main reconciliation
# ---------------------------------------------------------------------------
async def run_proof_mode(
    db,
    ticker: str,
    date: str,
    *,
    bulk_data_override: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """
    For a single (ticker, date) pair, reconcile EODHD bulk data against
    the DB and return a structured diagnostic report.

    Parameters
    ----------
    db : motor database
    ticker : str   – e.g. "AHH.US"
    date : str     – YYYY-MM-DD, e.g. "2026-03-31"
    bulk_data_override : optional pre-fetched bulk payload (for testing)

    Returns
    -------
    dict with sections: bulk_check, db_check, normalization, skip_reasons, summary
    """
    started_at = datetime.now(timezone.utc).isoformat()
    normalized_input = _normalize_ticker(ticker)

    # ------------------------------------------------------------------
    # 1. EODHD bulk check
    # ------------------------------------------------------------------
    bulk_found = False
    bulk_matched_symbol: Optional[str] = None
    bulk_row: Optional[Dict[str, Any]] = None
    bulk_raw_row_count = 0
    bulk_error: Optional[str] = None
    _bulk_is_override = bulk_data_override is not None

    if _bulk_is_override:
        bulk_data = bulk_data_override
    else:
        try:
            from price_ingestion_service import fetch_bulk_eod_latest

            bulk_data, _ = await fetch_bulk_eod_latest(
                "US", include_meta=True, for_date=date,
            )
            if not bulk_data:
                # fetch_bulk_eod_latest swallows errors and returns [].
                # EODHD bulk for US has 50,000+ rows on any trading
                # day; an empty result means the fetch failed, NOT that
                # the ticker is absent.
                bulk_error = "bulk_fetch_returned_empty_payload"
        except Exception as exc:
            bulk_data = []
            bulk_error = f"{type(exc).__name__}: bulk fetch failed"
            logger.warning("Proof mode bulk fetch error: %s", exc)

    bulk_raw_row_count = len(bulk_data)

    # Determine ticker field (code or symbol)
    bulk_ticker_field: Optional[str] = None
    if bulk_data:
        sample = bulk_data[0]
        if isinstance(sample, dict):
            if "code" in sample:
                bulk_ticker_field = "code"
            elif "symbol" in sample:
                bulk_ticker_field = "symbol"

    # Search for the ticker in the bulk payload
    bulk_symbol_normalized_map: Dict[str, str] = {}  # normalized -> raw
    if bulk_ticker_field:
        for record in bulk_data:
            raw_sym = record.get(bulk_ticker_field)
            if raw_sym is None:
                continue
            raw_text = str(raw_sym)
            norm = _normalize_ticker(raw_text)
            if norm:
                bulk_symbol_normalized_map[norm] = raw_text
            if norm == normalized_input:
                bulk_found = True
                bulk_matched_symbol = raw_text
                bulk_row = {
                    "code": record.get("code"),
                    "date": record.get("date"),
                    "open": record.get("open"),
                    "high": record.get("high"),
                    "low": record.get("low"),
                    "close": record.get("close"),
                    "adjusted_close": record.get("adjusted_close"),
                    "volume": record.get("volume"),
                }
                break  # found — stop scanning

    # When a live EODHD fetch returned empty (API failure / swallowed
    # error), we cannot conclude the ticker is absent — mark unknown.
    # Explicit bulk_data_override=[] (testing) keeps bulk_found=False.
    if not _bulk_is_override and not bulk_data and bulk_error:
        bulk_found = None

    # ------------------------------------------------------------------
    # 2. stock_prices DB check
    # ------------------------------------------------------------------
    db_found = False
    db_count = 0
    db_sample_id: Optional[str] = None
    db_sample_row: Optional[Dict[str, Any]] = None

    # Try with the normalized input ticker
    db_cursor = db.stock_prices.find(
        {"ticker": normalized_input, "date": date},
        {"_id": 1, "ticker": 1, "date": 1, "close": 1},
    ).limit(2)
    db_rows = await db_cursor.to_list(length=2)
    db_count = len(db_rows)
    if db_rows:
        db_found = True
        db_sample_id = str(db_rows[0].get("_id"))
        db_sample_row = {
            k: (str(v) if k == "_id" else v) for k, v in db_rows[0].items()
        }

    # ------------------------------------------------------------------
    # 3. Normalization audit
    # ------------------------------------------------------------------
    # Seeded ticker form
    seeded_doc = await db.tracked_tickers.find_one(
        {"ticker": normalized_input},
        {"_id": 0, "ticker": 1, "exchange": 1, "asset_type": 1,
         "is_seeded": 1, "is_visible": 1},
    )
    seeded_ticker_raw = seeded_doc.get("ticker") if seeded_doc else None
    seeded_ticker_normalized = _normalize_ticker(seeded_ticker_raw) if seeded_ticker_raw else None

    normalization_audit = {
        "input_ticker": ticker,
        "input_normalized": normalized_input,
        "bulk_symbol_raw": bulk_matched_symbol,
        "bulk_symbol_normalized": _normalize_ticker(bulk_matched_symbol) if bulk_matched_symbol else None,
        "seeded_ticker_raw": seeded_ticker_raw,
        "seeded_ticker_normalized": seeded_ticker_normalized,
        "db_stock_prices_ticker": db_sample_row.get("ticker") if db_sample_row else None,
        "all_match": (
            normalized_input
            == (_normalize_ticker(bulk_matched_symbol) if bulk_matched_symbol else None)
            == seeded_ticker_normalized
        ) if (bulk_matched_symbol and seeded_ticker_raw) else None,
    }

    # ------------------------------------------------------------------
    # 4. Skip reason analysis (when bulk has data but DB doesn't)
    # ------------------------------------------------------------------
    skip_reasons: Dict[str, Any] = {}
    if bulk_found and not db_found:
        # 4a. not_in_seeded: ticker not in tracked_tickers with Step 2 criteria
        _STEP2_QUERY = {
            "exchange": {"$in": ["NYSE", "NASDAQ"]},
            "asset_type": "Common Stock",
        }
        seeded_step2 = await db.tracked_tickers.find_one(
            {**_STEP2_QUERY, "ticker": normalized_input},
            {"_id": 0, "ticker": 1, "is_seeded": 1},
        )
        is_in_seeded = seeded_step2 is not None
        is_currently_seeded = (
            seeded_step2 is not None and seeded_step2.get("is_seeded") is True
        )
        skip_reasons["not_in_seeded"] = not is_in_seeded
        skip_reasons["is_currently_seeded"] = is_currently_seeded

        # 4b. not_visible: compute visibility if seeded
        visibility_info: Optional[Dict[str, Any]] = None
        if seeded_doc:
            from visibility_rules import compute_visibility
            is_visible, failed_reason = compute_visibility(seeded_doc)
            visibility_info = {
                "is_visible": is_visible,
                "failed_reason": failed_reason,
            }
        skip_reasons["not_visible"] = (
            visibility_info is not None and not visibility_info.get("is_visible", False)
        )
        skip_reasons["visibility_detail"] = visibility_info

        # 4c. normalization_mismatch: do normalized forms disagree?
        bulk_norm = _normalize_ticker(bulk_matched_symbol) if bulk_matched_symbol else None
        skip_reasons["normalization_mismatch"] = (
            bulk_norm is not None
            and seeded_ticker_normalized is not None
            and bulk_norm != seeded_ticker_normalized
        )

        # 4d. write_failed: check ops_job_runs for bulk_catchup errors on this date
        ops_doc = await db.ops_job_runs.find_one(
            {
                "job_name": {"$in": ["price_sync", "bulk_gapfill_remediation"]},
                "details.price_bulk_gapfill.days.processed_date": date,
            },
            {"_id": 0, "status": 1, "details.price_bulk_gapfill.days": 1},
            sort=[("finished_at", -1)],
        )
        write_failed = False
        ops_day_status: Optional[str] = None
        ops_rows_written: Optional[int] = None
        ops_matched_seeded: Optional[int] = None
        if ops_doc:
            days = (
                (ops_doc.get("details") or {})
                .get("price_bulk_gapfill", {})
                .get("days", [])
            )
            for day in days:
                if day.get("processed_date") == date:
                    ops_day_status = day.get("status")
                    ops_rows_written = day.get("rows_written")
                    ops_matched_seeded = day.get("matched_seeded_tickers_count")
                    if ops_day_status != "success" or (ops_rows_written or 0) == 0:
                        write_failed = True
                    break
        skip_reasons["write_failed"] = write_failed
        skip_reasons["ops_day_detail"] = {
            "status": ops_day_status,
            "rows_written": ops_rows_written,
            "matched_seeded_tickers_count": ops_matched_seeded,
        }

        # 4e. filtered_by_non_trading_day: check market_calendar
        non_trading_day = False
        holiday_name: Optional[str] = None
        try:
            from services.market_calendar_service import COLLECTION as MC_COLLECTION
            cal_doc = await db[MC_COLLECTION].find_one(
                {"market": "US", "date": date},
                {"is_trading_day": 1, "holiday_name": 1, "_id": 0},
            )
            if cal_doc is not None and not cal_doc.get("is_trading_day", True):
                non_trading_day = True
                holiday_name = cal_doc.get("holiday_name")
        except Exception:
            pass
        skip_reasons["filtered_by_non_trading_day"] = non_trading_day
        skip_reasons["holiday_name"] = holiday_name

        # 4f. Check for close=0 in the bulk row (halted/delisted ticker)
        bulk_close_is_zero = False
        if bulk_row:
            raw_close = bulk_row.get("close")
            if raw_close is None or float(raw_close) == 0:
                bulk_close_is_zero = True
        skip_reasons["bulk_close_is_zero"] = bulk_close_is_zero

        # 4g. Determine the primary skip reason
        if bulk_close_is_zero:
            skip_reasons["primary_reason"] = "bulk_found_but_close_is_zero"
        elif not is_in_seeded:
            skip_reasons["primary_reason"] = "not_in_seeded"
        elif not is_currently_seeded:
            skip_reasons["primary_reason"] = "temporarily_unseeded"
        elif skip_reasons.get("normalization_mismatch"):
            skip_reasons["primary_reason"] = "normalization_mismatch"
        elif non_trading_day:
            skip_reasons["primary_reason"] = "filtered_by_non_trading_day"
        elif write_failed:
            skip_reasons["primary_reason"] = "write_failed"
        elif ops_day_status == "success" and (ops_rows_written or 0) > 0:
            # Bulk ran successfully but this specific ticker wasn't written.
            # Most likely cause: ticker was temporarily un-seeded when the
            # Step 1 → Step 2 chain ran for this date.
            skip_reasons["primary_reason"] = "ticker_skipped_during_successful_bulk"
        else:
            skip_reasons["primary_reason"] = "unknown"

    # ------------------------------------------------------------------
    # 5. Gap-check context: is this date in expected_dates?
    # ------------------------------------------------------------------
    from services.admin_overview_service import _get_bulk_processed_dates
    expected_dates = await _get_bulk_processed_dates(db)
    date_in_expected = date in expected_dates

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    if bulk_found is None:
        summary = (
            "INCONCLUSIVE: EODHD bulk fetch returned empty "
            "(possible API failure). Cannot determine ticker presence."
        )
    elif bulk_found and db_found:
        summary = "CONSISTENT: Ticker present in both EODHD bulk and stock_prices."
    elif not bulk_found and not db_found:
        summary = "CONSISTENT: Ticker absent from both EODHD bulk and stock_prices."
    elif bulk_found and not db_found:
        primary = skip_reasons.get("primary_reason", "unknown")
        if primary == "bulk_found_but_close_is_zero":
            summary = (
                "EXPECTED GAP: Ticker is in EODHD bulk but close=0 "
                "(halted/delisted/no trade). Zero-price rows are not written to stock_prices."
            )
        else:
            summary = (
                f"GAP DETECTED: Ticker present in EODHD bulk but MISSING from stock_prices. "
                f"Primary skip reason: {primary}"
            )
    else:
        summary = (
            "UNEXPECTED: Ticker in stock_prices but NOT in EODHD bulk for this date. "
            "Possibly from historical backfill or prior ingestion."
        )

    return {
        "ticker": ticker,
        "date": date,
        "normalized_ticker": normalized_input,
        "started_at": started_at,
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "bulk_check": {
            "found": bulk_found,
            "matched_symbol": bulk_matched_symbol,
            "matched_row": bulk_row,
            "raw_row_count": bulk_raw_row_count,
            "ticker_field": bulk_ticker_field,
            "error": bulk_error,
        },
        "db_check": {
            "found": db_found,
            "count": db_count,
            "sample_id": db_sample_id,
            "sample_row": db_sample_row,
        },
        "normalization": normalization_audit,
        "skip_reasons": skip_reasons if skip_reasons else None,
        "gap_check_context": {
            "date_in_expected_dates": date_in_expected,
            "expected_dates_count": len(expected_dates),
            "expected_dates_sample": expected_dates[:10] if expected_dates else [],
        },
        "summary": summary,
    }
