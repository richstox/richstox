import asyncio
import os
import sys
from unittest.mock import AsyncMock, MagicMock, patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from whitelist_service import (
    DUPLICATE_REASON_PREFIX,
    aggregate_step1_reason_counts,
    build_step1_progress_snapshot,
    sync_ticker_whitelist,
)


class MockUpdateResult:
    def __init__(self, modified_count=0):
        self.modified_count = modified_count


def _make_symbol(code, *, type_="Common Stock", exchange="NYSE", name=None):
    return {
        "Code": code,
        "Name": name or (code or "Unknown"),
        "Type": type_,
        "Exchange": exchange,
        "Currency": "USD",
        "Country": "USA",
        "Isin": "",
        "_exchange": exchange,
    }


def _make_db_mock(seed_count: int):
    db = MagicMock()

    bulk_result = MagicMock()
    bulk_result.upserted_count = 0
    bulk_result.matched_count = seed_count
    bulk_result.upserted_ids = {}
    db.tracked_tickers.bulk_write = AsyncMock(return_value=bulk_result)
    db.tracked_tickers.update_many = AsyncMock(
        side_effect=[MockUpdateResult(0), MockUpdateResult(0)]
    )

    db.universe_seed_seeded_tickers.delete_many = AsyncMock()
    db.universe_seed_seeded_tickers.insert_many = AsyncMock()
    db.universe_seed_raw_rows.insert_many = AsyncMock()
    db.universe_seed_raw_rows.aggregate = MagicMock()
    db.universe_seed_raw_rows.aggregate.return_value.to_list = AsyncMock(return_value=[])
    db.pipeline_exclusion_report.delete_many = AsyncMock()
    db.pipeline_exclusion_report.insert_many = AsyncMock()
    db.fundamentals_events.insert_many = AsyncMock()
    return db


def test_step1_reason_count_aggregation_and_snapshot_fields():
    rows = [
        {"reason": "Type != Common Stock"},
        {"reason": "Empty ticker code"},
        {"reason": "Ticker contains dot"},
        {"reason": "Excluded pattern (-WT)"},
        {"reason": f"{DUPLICATE_REASON_PREFIX} global_raw_row_id=1)"},
        {"reason": "Some new reason"},
    ]

    counts = aggregate_step1_reason_counts(rows)

    assert counts == {
        "filtered_not_common_stock": 1,
        "filtered_duplicates": 1,
        "filtered_pattern": 1,
        "filtered_empty_code": 1,
        "filtered_dot_ticker": 1,
        "filtered_other": 1,
    }

    snapshot = build_step1_progress_snapshot(
        phase="upserting",
        raw_rows_total=10,
        raw_distinct_total=8,
        raw_per_exchange={"NYSE": 6, "NASDAQ": 4},
        reason_counts=counts,
        filtered_out_total_step1=4,
        seeded_total=4,
        db_written_total=3,
        progress_processed=3,
        progress_total=4,
        progress_message="Writing Step 1 seed universe to DB (3/4)",
    )

    assert snapshot["phase"] == "upserting"
    assert snapshot["raw_rows_total"] == 10
    assert snapshot["raw_distinct_total"] == 8
    assert snapshot["raw_per_exchange"] == {"NYSE": 6, "NASDAQ": 4}
    assert snapshot["filtered_out_total_step1"] == 4
    assert snapshot["seeded_total"] == 4
    assert snapshot["db_written_total"] == 3
    assert snapshot["progress_processed"] == 3
    assert snapshot["progress_total"] == 4
    assert snapshot["progress_pct"] == 75


def test_sync_ticker_whitelist_emits_structured_step1_progress_snapshots():
    nyse_symbols = [
        _make_symbol("AAPL", exchange="NYSE"),
        _make_symbol("ETF1", type_="ETF", exchange="NYSE"),
        _make_symbol("BRK.B", exchange="NYSE"),
        _make_symbol("DUPL", exchange="NYSE"),
    ]
    nasdaq_symbols = [
        _make_symbol("DUPL", exchange="NASDAQ", name="Duplicate Ticker"),
    ]
    db = _make_db_mock(seed_count=1)
    snapshots = []

    async def _collect(snapshot):
        snapshots.append(snapshot)

    async def _run():
        with patch(
            "whitelist_service.fetch_exchange_symbols",
            new=AsyncMock(side_effect=[nyse_symbols, nasdaq_symbols]),
        ):
            return await sync_ticker_whitelist(
                db,
                dry_run=False,
                exchanges=["NYSE", "NASDAQ"],
                job_run_id="universe_seed_test_progress",
                progress_callback=_collect,
            )

    result = asyncio.run(_run())

    phases = [snapshot["phase"] for snapshot in snapshots]
    assert phases[:3] == ["raw_fetched", "raw_distinct_built", "filtered"]
    assert "upserting" in phases
    assert phases[-1] == "completed"

    final_snapshot = snapshots[-1]
    assert final_snapshot["raw_rows_total"] == 5
    assert final_snapshot["raw_distinct_total"] == 4
    assert final_snapshot["raw_per_exchange"] == {"NYSE": 4, "NASDAQ": 1}
    assert final_snapshot["filtered_not_common_stock"] == 1
    assert final_snapshot["filtered_dot_ticker"] == 1
    assert final_snapshot["filtered_duplicates"] == 1
    assert final_snapshot["seeded_total"] == 1
    assert final_snapshot["db_written_total"] == 1
    assert final_snapshot["filtered_out_total_step1"] == 3

    assert result["raw_rows_total"] == 5
    assert result["raw_distinct_total"] == 4
    assert result["seeded_total"] == 1
    assert result["db_written_total"] == 1
    assert result["filtered_out_total_step1"] == 3
    assert result["step1_reason_counts"]["filtered_duplicates"] == 1
