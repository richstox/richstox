"""
Tests for ticker-gap remediation:
- run_ticker_gap_remediation (batch mode) with skip reason logging
- run_single_ticker_gap_remediation (single-ticker mode) with per-date reports

All tests use mocked DB and EODHD API — no external calls.
"""

import asyncio
import os
import sys
from copy import deepcopy
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


# ── Helpers ──────────────────────────────────────────────────────────────────


class _FakeAggregateCursor:
    """Async iterable cursor returned by aggregate()."""

    def __init__(self, docs):
        self._docs = list(docs)

    def __aiter__(self):
        return _FakeAggregateCursorIter(self._docs)


class _FakeAggregateCursorIter:
    def __init__(self, docs):
        self._iter = iter(docs)

    async def __anext__(self):
        try:
            return next(self._iter)
        except StopIteration:
            raise StopAsyncIteration


class _FakeFindCursor:
    """Async iterable cursor for find()."""

    def __init__(self, docs):
        self._docs = list(docs)

    def __aiter__(self):
        return _FakeAggregateCursorIter(self._docs)

    async def to_list(self, _length=None):
        return deepcopy(self._docs)


class _FakeWriteResult:
    def __init__(self, upserted=0, modified=0):
        self.upserted_count = upserted
        self.modified_count = modified


def _make_ops_agg_docs(dates_with_status):
    """
    Build ops_job_runs aggregate result docs.
    dates_with_status: list of (date, rows_written, matched_seeded)
    """
    return [{
        "details": {
            "price_bulk_gapfill": {
                "days": [
                    {
                        "processed_date": d,
                        "status": "success",
                        "rows_written": rw,
                        "matched_seeded_tickers_count": ms,
                    }
                    for d, rw, ms in dates_with_status
                ],
            },
        },
    }]


def _make_db(
    *,
    ops_agg_docs=None,
    tracked_tickers_docs=None,
    tracked_tickers_find_one_doc=None,
    stock_prices_agg_docs=None,
    stock_prices_find_docs=None,
    market_calendar_docs=None,
    bulk_write_result=None,
    find_one_result=None,
):
    """Build a mock motor database for ticker-gap remediation tests."""
    db = MagicMock()

    # ops_job_runs
    db.ops_job_runs.insert_one = AsyncMock(
        return_value=SimpleNamespace(inserted_id=1)
    )
    db.ops_job_runs.update_one = AsyncMock()
    db.ops_job_runs.aggregate = MagicMock(
        return_value=_FakeAggregateCursor(ops_agg_docs or [])
    )

    # tracked_tickers
    db.tracked_tickers.find = MagicMock(
        return_value=MagicMock(
            to_list=AsyncMock(return_value=tracked_tickers_docs or [])
        )
    )
    db.tracked_tickers.find_one = AsyncMock(
        return_value=tracked_tickers_find_one_doc
    )

    # stock_prices
    db.stock_prices.aggregate = MagicMock(
        return_value=_FakeAggregateCursor(stock_prices_agg_docs or [])
    )
    db.stock_prices.find = MagicMock(
        return_value=_FakeFindCursor(stock_prices_find_docs or [])
    )
    db.stock_prices.find_one = AsyncMock(return_value=find_one_result)
    db.stock_prices.bulk_write = AsyncMock(
        return_value=bulk_write_result or _FakeWriteResult()
    )

    # market_calendar collection
    mc_collection = MagicMock()
    mc_find = MagicMock()
    mc_find.to_list = AsyncMock(
        return_value=market_calendar_docs or []
    )
    mc_collection.find = MagicMock(return_value=mc_find)
    db.__getitem__ = MagicMock(return_value=mc_collection)

    return db


EXPECTED_DATES_BULK = [
    ("2026-03-31", 5000, 5000),
    ("2026-04-01", 5000, 5000),
]

MARKET_CAL_DOCS = [
    {"date": "2026-03-31", "is_trading_day": True},
    {"date": "2026-04-01", "is_trading_day": True},
]


# ── Tests: Batch mode skip reasons ──────────────────────────────────────────


class TestBatchSkipReasons:
    """Batch run_ticker_gap_remediation logs skip reasons for empty API returns."""

    @pytest.mark.asyncio
    async def test_skip_reason_api_returned_no_records(self):
        """When fetch_eod_history returns [] for a gap, skip_reason is logged."""
        db = _make_db(
            ops_agg_docs=_make_ops_agg_docs(EXPECTED_DATES_BULK),
            tracked_tickers_docs=None,  # overridden below
            stock_prices_agg_docs=[],
            market_calendar_docs=MARKET_CAL_DOCS,
        )

        # Override tracked_tickers.find() to return one proven ticker
        proven_docs = [
            {
                "ticker": "AHH.US",
                "history_download_proven_anchor": "2026-03-30",
            }
        ]
        db.tracked_tickers.find = MagicMock(
            return_value=MagicMock(to_list=AsyncMock(return_value=proven_docs))
        )

        with patch(
            "price_ingestion_service.fetch_eod_history",
            new=AsyncMock(return_value=[]),
        ), patch(
            "price_ingestion_service.parse_eod_record",
            side_effect=lambda t, r: {"ticker": t, "date": r.get("date")},
        ):
            from scheduler_service import run_ticker_gap_remediation
            result = await run_ticker_gap_remediation(db)

        assert result["status"] == "success"
        assert len(result["skipped_pairs"]) > 0
        for sp in result["skipped_pairs"]:
            assert sp["skip_reason"] == "api_returned_no_records"
            assert sp["ticker"] == "AHH.US"

    @pytest.mark.asyncio
    async def test_no_skipped_pairs_when_all_filled(self):
        """When fetch_eod_history returns data, no skipped_pairs."""
        db = _make_db(
            ops_agg_docs=_make_ops_agg_docs(EXPECTED_DATES_BULK),
            stock_prices_agg_docs=[],
            market_calendar_docs=MARKET_CAL_DOCS,
            bulk_write_result=_FakeWriteResult(upserted=2),
        )

        proven_docs = [
            {
                "ticker": "AHH.US",
                "history_download_proven_anchor": "2026-03-30",
            }
        ]
        db.tracked_tickers.find = MagicMock(
            return_value=MagicMock(to_list=AsyncMock(return_value=proven_docs))
        )

        eod_record = {"date": "2026-04-01", "close": 12.5}
        with patch(
            "price_ingestion_service.fetch_eod_history",
            new=AsyncMock(return_value=[eod_record]),
        ), patch(
            "price_ingestion_service.parse_eod_record",
            side_effect=lambda t, r: {"ticker": t, "date": r.get("date"), "close": r.get("close")},
        ):
            from scheduler_service import run_ticker_gap_remediation
            result = await run_ticker_gap_remediation(db)

        assert result["status"] == "success"
        assert result["skipped_pairs"] == []
        assert result["pairs_filled"] > 0


# ── Tests: Single-ticker mode ──────────────────────────────────────────────


class TestSingleTickerNoExpectedDates:
    """When no expected dates exist, returns early."""

    @pytest.mark.asyncio
    async def test_no_expected_dates(self):
        db = _make_db(
            ops_agg_docs=[],
            market_calendar_docs=[],
        )

        from scheduler_service import run_single_ticker_gap_remediation
        result = await run_single_ticker_gap_remediation(db, "AHH.US")

        assert result["status"] == "success"
        assert result["expected_dates_count"] == 0
        assert result["per_date_report"] == []


class TestSingleTickerNotFound:
    """When ticker not in tracked_tickers, returns error."""

    @pytest.mark.asyncio
    async def test_ticker_not_found(self):
        db = _make_db(
            ops_agg_docs=_make_ops_agg_docs(EXPECTED_DATES_BULK),
            tracked_tickers_find_one_doc=None,
            market_calendar_docs=MARKET_CAL_DOCS,
        )

        from scheduler_service import run_single_ticker_gap_remediation
        result = await run_single_ticker_gap_remediation(db, "ZZZZ.US")

        assert result.get("error") == "ticker_not_found_in_tracked_tickers"


class TestSingleTickerNotProven:
    """When ticker has no proven anchor, returns error."""

    @pytest.mark.asyncio
    async def test_ticker_not_proven(self):
        db = _make_db(
            ops_agg_docs=_make_ops_agg_docs(EXPECTED_DATES_BULK),
            tracked_tickers_find_one_doc={
                "ticker": "AHH.US",
                "is_visible": True,
                "is_seeded": True,
                # No history_download_proven_anchor
            },
            market_calendar_docs=MARKET_CAL_DOCS,
        )

        from scheduler_service import run_single_ticker_gap_remediation
        result = await run_single_ticker_gap_remediation(db, "AHH.US")

        assert result.get("error") == "ticker_not_proven"


class TestSingleTickerFullRemediation:
    """Full single-ticker remediation with per-date reports."""

    @pytest.mark.asyncio
    async def test_fills_all_missing_dates(self):
        """
        AHH.US is missing both 2026-03-31 and 2026-04-01.
        Both dates should be remediated (no cap).
        """
        db = _make_db(
            ops_agg_docs=_make_ops_agg_docs(EXPECTED_DATES_BULK),
            tracked_tickers_find_one_doc={
                "ticker": "AHH.US",
                "history_download_proven_anchor": "2026-03-30",
                "is_visible": True,
                "is_seeded": True,
            },
            stock_prices_find_docs=[],  # no existing prices
            market_calendar_docs=MARKET_CAL_DOCS,
            bulk_write_result=_FakeWriteResult(upserted=1),
            find_one_result={"_id": "fake", "ticker": "AHH.US"},  # db_after
        )

        eod_record = lambda date: [{"date": date, "close": 12.5}]

        with patch(
            "price_ingestion_service.fetch_eod_history",
            new=AsyncMock(side_effect=lambda t, from_date=None, to_date=None: eod_record(from_date)),
        ), patch(
            "price_ingestion_service.parse_eod_record",
            side_effect=lambda t, r: {"ticker": t, "date": r.get("date"), "close": r.get("close")},
        ), patch(
            "price_ingestion_service.fetch_bulk_eod_latest",
            new=AsyncMock(return_value=([], True)),
        ):
            from scheduler_service import run_single_ticker_gap_remediation
            result = await run_single_ticker_gap_remediation(
                db, "AHH.US", bulk_check=False,
            )

        assert result["status"] == "success"
        assert result["ticker"] == "AHH.US"
        assert result["missing_dates_count"] == 2
        assert result["total_inserted"] == 2
        assert len(result["per_date_report"]) == 2

        dates = [r["date"] for r in result["per_date_report"]]
        assert "2026-03-31" in dates
        assert "2026-04-01" in dates

        for r in result["per_date_report"]:
            assert r["inserted_count"] == 1
            assert r["db_after"] is True

    @pytest.mark.asyncio
    async def test_per_date_report_structure(self):
        """Each per-date report has the required keys."""
        db = _make_db(
            ops_agg_docs=_make_ops_agg_docs([("2026-04-01", 5000, 5000)]),
            tracked_tickers_find_one_doc={
                "ticker": "AHH.US",
                "history_download_proven_anchor": "2026-03-30",
                "is_visible": True,
            },
            stock_prices_find_docs=[],
            market_calendar_docs=[{"date": "2026-04-01", "is_trading_day": True}],
            bulk_write_result=_FakeWriteResult(upserted=1),
            find_one_result={"_id": "fake"},
        )

        with patch(
            "price_ingestion_service.fetch_eod_history",
            new=AsyncMock(return_value=[{"date": "2026-04-01", "close": 12.5}]),
        ), patch(
            "price_ingestion_service.parse_eod_record",
            side_effect=lambda t, r: {"ticker": t, "date": r.get("date")},
        ), patch(
            "price_ingestion_service.fetch_bulk_eod_latest",
            new=AsyncMock(return_value=([], True)),
        ):
            from scheduler_service import run_single_ticker_gap_remediation
            result = await run_single_ticker_gap_remediation(
                db, "AHH.US", bulk_check=False,
            )

        report = result["per_date_report"][0]
        required_keys = {
            "date", "bulk_found", "bulk_matched_symbol",
            "db_before", "inserted_count", "db_after", "skip_reason",
        }
        assert required_keys.issubset(report.keys())


class TestSingleTickerSkipReasons:
    """Single-ticker mode logs deterministic skip reasons."""

    @pytest.mark.asyncio
    async def test_skip_reason_api_returned_no_records(self):
        """When per-ticker API returns empty and bulk_check=False."""
        db = _make_db(
            ops_agg_docs=_make_ops_agg_docs([("2026-04-01", 5000, 5000)]),
            tracked_tickers_find_one_doc={
                "ticker": "AHH.US",
                "history_download_proven_anchor": "2026-03-30",
            },
            stock_prices_find_docs=[],
            market_calendar_docs=[{"date": "2026-04-01", "is_trading_day": True}],
            find_one_result=None,  # db_after still missing
        )

        with patch(
            "price_ingestion_service.fetch_eod_history",
            new=AsyncMock(return_value=[]),
        ), patch(
            "price_ingestion_service.fetch_bulk_eod_latest",
            new=AsyncMock(return_value=([], True)),
        ):
            from scheduler_service import run_single_ticker_gap_remediation
            result = await run_single_ticker_gap_remediation(
                db, "AHH.US", bulk_check=False,
            )

        report = result["per_date_report"][0]
        assert report["inserted_count"] == 0
        assert report["skip_reason"] == "api_returned_no_records"
        assert report["db_after"] is False

    @pytest.mark.asyncio
    async def test_skip_reason_not_in_bulk_data(self):
        """When per-ticker API returns empty and bulk also doesn't contain the ticker."""
        db = _make_db(
            ops_agg_docs=_make_ops_agg_docs([("2026-04-01", 5000, 5000)]),
            tracked_tickers_find_one_doc={
                "ticker": "AHH.US",
                "history_download_proven_anchor": "2026-03-30",
            },
            stock_prices_find_docs=[],
            market_calendar_docs=[{"date": "2026-04-01", "is_trading_day": True}],
            find_one_result=None,
        )

        # Bulk data does NOT contain AHH
        bulk_data = [
            {"code": "AAPL", "date": "2026-04-01", "close": 200},
            {"code": "MSFT", "date": "2026-04-01", "close": 400},
        ]

        with patch(
            "price_ingestion_service.fetch_eod_history",
            new=AsyncMock(return_value=[]),
        ), patch(
            "price_ingestion_service.fetch_bulk_eod_latest",
            new=AsyncMock(return_value=(bulk_data, True)),
        ):
            from scheduler_service import run_single_ticker_gap_remediation
            result = await run_single_ticker_gap_remediation(
                db, "AHH.US", bulk_check=True,
            )

        report = result["per_date_report"][0]
        assert report["bulk_found"] is False
        assert report["skip_reason"] == "not_in_bulk_data"

    @pytest.mark.asyncio
    async def test_skip_reason_not_in_per_ticker_api_but_in_bulk(self):
        """
        When per-ticker API returns empty but bulk DOES contain the ticker,
        the bulk fallback path should kick in.
        """
        db = _make_db(
            ops_agg_docs=_make_ops_agg_docs([("2026-04-01", 5000, 5000)]),
            tracked_tickers_find_one_doc={
                "ticker": "AHH.US",
                "history_download_proven_anchor": "2026-03-30",
            },
            stock_prices_find_docs=[],
            market_calendar_docs=[{"date": "2026-04-01", "is_trading_day": True}],
            bulk_write_result=_FakeWriteResult(upserted=1),
            find_one_result={"_id": "fake"},  # db_after succeeds
        )

        # Bulk data DOES contain AHH
        bulk_data = [
            {"code": "AHH", "date": "2026-04-01", "close": 12.5,
             "open": 12.0, "high": 13.0, "low": 11.5,
             "adjusted_close": 12.5, "volume": 50000},
            {"code": "AAPL", "date": "2026-04-01", "close": 200},
        ]

        with patch(
            "price_ingestion_service.fetch_eod_history",
            new=AsyncMock(return_value=[]),
        ), patch(
            "price_ingestion_service.fetch_bulk_eod_latest",
            new=AsyncMock(return_value=(bulk_data, True)),
        ), patch(
            "price_ingestion_service.parse_eod_record",
            side_effect=lambda t, r: {
                "ticker": t, "date": r.get("date"), "close": r.get("close"),
            },
        ):
            from scheduler_service import run_single_ticker_gap_remediation
            result = await run_single_ticker_gap_remediation(
                db, "AHH.US", bulk_check=True,
            )

        report = result["per_date_report"][0]
        assert report["bulk_found"] is True
        assert report["inserted_count"] == 1
        assert report["skip_reason"] == "resolved_via_bulk_fallback"
        assert report["db_after"] is True


class TestSingleTickerEmptyBulkFetch:
    """When bulk fetch returns [] (API failure), bulk_found must be None, not False."""

    @pytest.mark.asyncio
    async def test_empty_bulk_sets_bulk_found_none(self):
        """Empty bulk → bulk_found=None (unknown), skip_reason=bulk_fetch_returned_empty."""
        db = _make_db(
            ops_agg_docs=_make_ops_agg_docs([("2026-04-01", 5000, 5000)]),
            tracked_tickers_find_one_doc={
                "ticker": "AHH.US",
                "history_download_proven_anchor": "2026-03-30",
            },
            stock_prices_find_docs=[],
            market_calendar_docs=[{"date": "2026-04-01", "is_trading_day": True}],
            find_one_result=None,
        )

        # fetch_bulk_eod_latest returns empty (simulates API failure)
        with patch(
            "price_ingestion_service.fetch_eod_history",
            new=AsyncMock(return_value=[]),
        ), patch(
            "price_ingestion_service.fetch_bulk_eod_latest",
            new=AsyncMock(return_value=([], True)),
        ):
            from scheduler_service import run_single_ticker_gap_remediation
            result = await run_single_ticker_gap_remediation(
                db, "AHH.US", bulk_check=True,
            )

        report = result["per_date_report"][0]
        # Must be None (unknown), NOT False (which would mean "ticker not in bulk")
        assert report["bulk_found"] is None
        assert report["skip_reason"] == "bulk_fetch_returned_empty"

    @pytest.mark.asyncio
    async def test_nonempty_bulk_without_ticker_stays_false(self):
        """Non-empty bulk that genuinely lacks the ticker → bulk_found=False."""
        db = _make_db(
            ops_agg_docs=_make_ops_agg_docs([("2026-04-01", 5000, 5000)]),
            tracked_tickers_find_one_doc={
                "ticker": "AHH.US",
                "history_download_proven_anchor": "2026-03-30",
            },
            stock_prices_find_docs=[],
            market_calendar_docs=[{"date": "2026-04-01", "is_trading_day": True}],
            find_one_result=None,
        )

        bulk_data = [
            {"code": "AAPL", "date": "2026-04-01", "close": 200},
            {"code": "MSFT", "date": "2026-04-01", "close": 400},
        ]

        with patch(
            "price_ingestion_service.fetch_eod_history",
            new=AsyncMock(return_value=[]),
        ), patch(
            "price_ingestion_service.fetch_bulk_eod_latest",
            new=AsyncMock(return_value=(bulk_data, True)),
        ):
            from scheduler_service import run_single_ticker_gap_remediation
            result = await run_single_ticker_gap_remediation(
                db, "AHH.US", bulk_check=True,
            )

        report = result["per_date_report"][0]
        assert report["bulk_found"] is False
        assert report["skip_reason"] == "not_in_bulk_data"


class TestSingleTickerAlreadyComplete:
    """When ticker has no missing dates, reports empty list."""

    @pytest.mark.asyncio
    async def test_no_gaps(self):
        db = _make_db(
            ops_agg_docs=_make_ops_agg_docs(EXPECTED_DATES_BULK),
            tracked_tickers_find_one_doc={
                "ticker": "AHH.US",
                "history_download_proven_anchor": "2026-03-30",
            },
            # Both dates already exist
            stock_prices_find_docs=[
                {"date": "2026-03-31"},
                {"date": "2026-04-01"},
            ],
            market_calendar_docs=MARKET_CAL_DOCS,
        )

        with patch(
            "price_ingestion_service.fetch_eod_history",
            new=AsyncMock(return_value=[]),
        ), patch(
            "price_ingestion_service.fetch_bulk_eod_latest",
            new=AsyncMock(return_value=([], True)),
        ):
            from scheduler_service import run_single_ticker_gap_remediation
            result = await run_single_ticker_gap_remediation(
                db, "AHH.US", bulk_check=False,
            )

        assert result["missing_dates_count"] == 0
        assert result["per_date_report"] == []
        assert result["total_inserted"] == 0


class TestSingleTickerNormalization:
    """Normalization works correctly for different input formats."""

    @pytest.mark.asyncio
    async def test_normalizes_lowercase_input(self):
        db = _make_db(
            ops_agg_docs=[],
            market_calendar_docs=[],
        )

        from scheduler_service import run_single_ticker_gap_remediation
        result = await run_single_ticker_gap_remediation(db, "ahh.us")

        assert result["ticker"] == "AHH.US"

    @pytest.mark.asyncio
    async def test_normalizes_without_suffix(self):
        db = _make_db(
            ops_agg_docs=[],
            market_calendar_docs=[],
        )

        from scheduler_service import run_single_ticker_gap_remediation
        result = await run_single_ticker_gap_remediation(db, "AHH")

        assert result["ticker"] == "AHH.US"


# ── Tests: remediate_gap_date ───────────────────────────────────────────────


def _make_db_for_gap_date(
    *,
    ops_agg_docs=None,
    proven_tracked_docs=None,
    existing_stock_prices_tickers=None,
    market_calendar_docs=None,
    bulk_write_result=None,
    find_one_result=None,
):
    """Build mock DB for remediate_gap_date tests."""
    db = MagicMock()

    # ops_job_runs
    db.ops_job_runs.insert_one = AsyncMock(
        return_value=SimpleNamespace(inserted_id=1)
    )
    db.ops_job_runs.aggregate = MagicMock(
        return_value=_FakeAggregateCursor(ops_agg_docs or [])
    )

    # tracked_tickers.find() returns proven docs
    db.tracked_tickers.find = MagicMock(
        return_value=MagicMock(
            to_list=AsyncMock(return_value=proven_tracked_docs or [])
        )
    )

    # stock_prices.find() returns existing tickers for target_date
    existing_docs = [
        {"ticker": t} for t in (existing_stock_prices_tickers or [])
    ]
    db.stock_prices.find = MagicMock(
        return_value=_FakeFindCursor(existing_docs)
    )
    db.stock_prices.find_one = AsyncMock(return_value=find_one_result)
    db.stock_prices.bulk_write = AsyncMock(
        return_value=bulk_write_result or _FakeWriteResult(upserted=1)
    )

    # market_calendar collection
    mc_collection = MagicMock()
    mc_find = MagicMock()
    mc_find.to_list = AsyncMock(
        return_value=market_calendar_docs or []
    )
    mc_collection.find = MagicMock(return_value=mc_find)
    db.__getitem__ = MagicMock(return_value=mc_collection)

    return db


GAP_DATE = "2026-04-02"
GAP_DATE_OPS = [("2026-04-02", 5000, 5000)]
GAP_DATE_MC = [{"date": "2026-04-02", "is_trading_day": True}]


class TestRemediateGapDateNotExpected:
    """Target date not in expected_dates → skipped."""

    @pytest.mark.asyncio
    async def test_date_not_expected(self):
        db = _make_db_for_gap_date(
            ops_agg_docs=[],  # no ops = no expected dates
            market_calendar_docs=[],
        )

        from scheduler_service import remediate_gap_date
        result = await remediate_gap_date(db, target_date=GAP_DATE)

        assert result["status"] == "skipped"
        assert result["reason"] == "target_date_not_in_expected_dates"


class TestRemediateGapDateNoGaps:
    """All tickers already have data → 0 gaps."""

    @pytest.mark.asyncio
    async def test_all_covered(self):
        proven = [
            {
                "ticker": "NYC.US",
                "history_download_proven_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
                "history_download_proven_anchor": "2026-03-01",
            },
        ]
        db = _make_db_for_gap_date(
            ops_agg_docs=_make_ops_agg_docs(GAP_DATE_OPS),
            proven_tracked_docs=proven,
            existing_stock_prices_tickers=["NYC.US"],
            market_calendar_docs=GAP_DATE_MC,
        )

        from scheduler_service import remediate_gap_date
        result = await remediate_gap_date(db, target_date=GAP_DATE)

        assert result["status"] == "success"
        assert result["gap_tickers_count"] == 0


class TestRemediateGapDateFromBulk:
    """Gap ticker resolved from bulk data."""

    @pytest.mark.asyncio
    async def test_resolves_from_bulk(self):
        proven = [
            {
                "ticker": "NYC.US",
                "history_download_proven_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
                "history_download_proven_anchor": "2026-03-01",
            },
        ]
        # stock_prices.find_one returns a doc AFTER insertion
        find_one_after = {"_id": "fake", "ticker": "NYC.US", "date": GAP_DATE}

        db = _make_db_for_gap_date(
            ops_agg_docs=_make_ops_agg_docs(GAP_DATE_OPS),
            proven_tracked_docs=proven,
            existing_stock_prices_tickers=[],  # NYC.US NOT in stock_prices
            market_calendar_docs=GAP_DATE_MC,
            bulk_write_result=_FakeWriteResult(upserted=1),
            find_one_result=find_one_after,
        )

        bulk_data = [
            {"code": "NYC", "date": "2026-04-02", "close": 8.44,
             "open": 8.40, "high": 8.50, "low": 8.30,
             "adjusted_close": 8.44, "volume": 10000},
            {"code": "AAPL", "date": "2026-04-02", "close": 200,
             "open": 199, "high": 201, "low": 198,
             "adjusted_close": 200, "volume": 50000},
        ]

        with patch(
            "price_ingestion_service.fetch_bulk_eod_latest",
            new=AsyncMock(return_value=(bulk_data, True)),
        ), patch(
            "price_ingestion_service.parse_eod_record",
            side_effect=lambda t, r: {
                "ticker": t, "date": r.get("date"),
                "close": r.get("close"), "open": r.get("open"),
                "high": r.get("high"), "low": r.get("low"),
                "adjusted_close": r.get("adjusted_close"),
                "volume": r.get("volume"),
            },
        ), patch(
            "price_ingestion_service.fetch_eod_history",
            new=AsyncMock(return_value=[]),
        ):
            from scheduler_service import remediate_gap_date
            result = await remediate_gap_date(db, target_date=GAP_DATE)

        assert result["status"] == "success"
        assert result["gap_tickers_count"] == 1
        assert result["total_inserted"] == 1

        proof = result["proof_table"][0]
        assert proof["ticker"] == "NYC.US"
        assert proof["bulk_found"] is True
        assert proof["matched_symbol"] == "NYC"
        assert proof["inserted"] is True
        assert proof["primary_reason"] == "resolved_from_bulk"
        assert proof["db_found_after"] is True


class TestRemediateGapDateNotInBulk:
    """Gap ticker not found in bulk → falls back to per-ticker API."""

    @pytest.mark.asyncio
    async def test_not_in_bulk_not_in_api(self):
        proven = [
            {
                "ticker": "NYC.US",
                "history_download_proven_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
                "history_download_proven_anchor": "2026-03-01",
            },
        ]
        db = _make_db_for_gap_date(
            ops_agg_docs=_make_ops_agg_docs(GAP_DATE_OPS),
            proven_tracked_docs=proven,
            existing_stock_prices_tickers=[],
            market_calendar_docs=GAP_DATE_MC,
            find_one_result=None,
        )

        bulk_data = [
            {"code": "AAPL", "date": "2026-04-02", "close": 200},
        ]

        with patch(
            "price_ingestion_service.fetch_bulk_eod_latest",
            new=AsyncMock(return_value=(bulk_data, True)),
        ), patch(
            "price_ingestion_service.parse_eod_record",
            side_effect=lambda t, r: {"ticker": t, "date": r.get("date")},
        ), patch(
            "price_ingestion_service.fetch_eod_history",
            new=AsyncMock(return_value=[]),
        ):
            from scheduler_service import remediate_gap_date
            result = await remediate_gap_date(db, target_date=GAP_DATE)

        proof = result["proof_table"][0]
        assert proof["bulk_found"] is False
        assert proof["inserted"] is False
        assert proof["primary_reason"] == "not_in_bulk_not_in_api"


class TestRemediateGapDateMultipleTickers:
    """Multiple gap tickers remediated in one call."""

    @pytest.mark.asyncio
    async def test_multiple_tickers(self):
        proven = [
            {
                "ticker": "NYC.US",
                "history_download_proven_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
                "history_download_proven_anchor": "2026-03-01",
            },
            {
                "ticker": "EXAS.US",
                "history_download_proven_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
                "history_download_proven_anchor": "2026-03-01",
            },
        ]
        find_one_after = {"_id": "fake"}

        db = _make_db_for_gap_date(
            ops_agg_docs=_make_ops_agg_docs(GAP_DATE_OPS),
            proven_tracked_docs=proven,
            existing_stock_prices_tickers=[],
            market_calendar_docs=GAP_DATE_MC,
            bulk_write_result=_FakeWriteResult(upserted=1),
            find_one_result=find_one_after,
        )

        bulk_data = [
            {"code": "NYC", "date": "2026-04-02", "close": 8.44,
             "open": 8.40, "high": 8.50, "low": 8.30,
             "adjusted_close": 8.44, "volume": 10000},
            {"code": "EXAS", "date": "2026-04-02", "close": 55.0,
             "open": 54.0, "high": 56.0, "low": 53.0,
             "adjusted_close": 55.0, "volume": 20000},
        ]

        with patch(
            "price_ingestion_service.fetch_bulk_eod_latest",
            new=AsyncMock(return_value=(bulk_data, True)),
        ), patch(
            "price_ingestion_service.parse_eod_record",
            side_effect=lambda t, r: {
                "ticker": t, "date": r.get("date"),
                "close": r.get("close"),
            },
        ), patch(
            "price_ingestion_service.fetch_eod_history",
            new=AsyncMock(return_value=[]),
        ):
            from scheduler_service import remediate_gap_date
            result = await remediate_gap_date(db, target_date=GAP_DATE)

        assert result["gap_tickers_count"] == 2
        assert result["total_inserted"] == 2
        tickers_in_table = [r["ticker"] for r in result["proof_table"]]
        assert "NYC.US" in tickers_in_table
        assert "EXAS.US" in tickers_in_table


class TestRemediateGapDateBulkFetchFailed:
    """Bulk fetch fails → falls back to per-ticker API."""

    @pytest.mark.asyncio
    async def test_bulk_failed_api_fallback(self):
        proven = [
            {
                "ticker": "NYC.US",
                "history_download_proven_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
                "history_download_proven_anchor": "2026-03-01",
            },
        ]
        find_one_after = {"_id": "fake"}

        db = _make_db_for_gap_date(
            ops_agg_docs=_make_ops_agg_docs(GAP_DATE_OPS),
            proven_tracked_docs=proven,
            existing_stock_prices_tickers=[],
            market_calendar_docs=GAP_DATE_MC,
            bulk_write_result=_FakeWriteResult(upserted=1),
            find_one_result=find_one_after,
        )

        with patch(
            "price_ingestion_service.fetch_bulk_eod_latest",
            new=AsyncMock(return_value=([], True)),  # empty = failed
        ), patch(
            "price_ingestion_service.parse_eod_record",
            side_effect=lambda t, r: {
                "ticker": t, "date": r.get("date"),
                "close": r.get("close"),
            },
        ), patch(
            "price_ingestion_service.fetch_eod_history",
            new=AsyncMock(return_value=[
                {"date": "2026-04-02", "close": 8.44, "open": 8.40,
                 "high": 8.50, "low": 8.30, "adjusted_close": 8.44,
                 "volume": 10000},
            ]),
        ):
            from scheduler_service import remediate_gap_date
            result = await remediate_gap_date(db, target_date=GAP_DATE)

        proof = result["proof_table"][0]
        assert proof["bulk_found"] is None  # bulk failed
        assert proof["inserted"] is True
        assert proof["primary_reason"] == "resolved_from_per_ticker_api"
