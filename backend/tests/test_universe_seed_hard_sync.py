# ==============================================================================
# UNIVERSE SEED HARD-SYNC RECONCILIATION — UNIT TESTS
# ==============================================================================
# Verifies that after sync_ticker_whitelist() succeeds, tickers belonging to
# the Step 1 seed universe (NYSE/NASDAQ Common Stock) that are absent from the
# current seed set are archived via the hard-sync reconciliation step.
# ==============================================================================

import pytest
import sys
import os
from unittest.mock import AsyncMock, MagicMock, patch, call

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class MockUpdateResult:
    def __init__(self, modified_count=0):
        self.modified_count = modified_count


def _make_db_mock(
    candidate_tickers,
    hard_sync_modified=2,
    deactivate_modified=0,
):
    """Build a minimal async DB mock for sync_ticker_whitelist."""
    db = MagicMock()

    # tracked_tickers collection
    tt = MagicMock()
    db.tracked_tickers = tt

    # bulk_write (upsert candidates) — return no upserts so no fundamentals events
    bulk_result = MagicMock()
    bulk_result.upserted_count = 0
    bulk_result.matched_count = len(candidate_tickers)
    bulk_result.upserted_ids = {}
    tt.bulk_write = AsyncMock(return_value=bulk_result)

    # update_many: first call = deactivate, second call = hard-sync archive
    tt.update_many = AsyncMock(
        side_effect=[
            MockUpdateResult(deactivate_modified),
            MockUpdateResult(hard_sync_modified),
        ]
    )

    # Other collections
    db.universe_seed_seeded_tickers.delete_many = AsyncMock()
    db.universe_seed_seeded_tickers.insert_many = AsyncMock()
    db.universe_seed_raw_rows.insert_many = AsyncMock()
    db.universe_seed_raw_rows.aggregate = MagicMock(
        return_value=MagicMock(__aiter__=MagicMock(return_value=iter([])))
    )
    db.universe_seed_raw_rows.aggregate.return_value.to_list = AsyncMock(return_value=[])
    db.pipeline_exclusion_report.delete_many = AsyncMock()
    db.pipeline_exclusion_report.insert_many = AsyncMock()
    db.fundamentals_events.insert_many = AsyncMock()

    return db


# ---------------------------------------------------------------------------
# Helpers: minimal symbol stubs for filter_whitelist_candidates
# ---------------------------------------------------------------------------

def _make_symbol(code, name="Test Inc", type_="Common Stock", exchange="NYSE"):
    return {
        "Code": code,
        "Name": name,
        "Type": type_,
        "Exchange": exchange,
        "Currency": "USD",
        "Country": "USA",
        "Isin": "",
        "_exchange": exchange,
    }


class TestHardSyncReconciliation:

    @pytest.mark.asyncio
    async def test_hard_sync_update_many_called_with_correct_filter(self):
        """
        After a successful seed, update_many must be called with the hard-sync
        filter: exchange in seeded exchanges, asset_type == Common Stock,
        ticker NOT IN latest_seed_set.
        """
        from whitelist_service import sync_ticker_whitelist

        candidate_code = "AAPL"
        symbols = [_make_symbol(candidate_code)]

        db = _make_db_mock(
            candidate_tickers={f"{candidate_code}.US"},
            hard_sync_modified=3,
        )

        with (
            patch("whitelist_service.fetch_exchange_symbols", new=AsyncMock(return_value=symbols)),
            patch("whitelist_service.save_universe_seed_exclusion_report", new=AsyncMock(
                return_value={"exclusion_report_run_id": "run_test", "_debug": {}}
            )),
            patch("whitelist_service.upsert_provider_debug_snapshot", new=AsyncMock()),
        ):
            result = await sync_ticker_whitelist(
                db,
                dry_run=False,
                exchanges=["NYSE"],
                job_run_id="test_run_001",
            )

        # hard_sync_archived must reflect the update_many result
        assert result["hard_sync_archived"] == 3

        # Second update_many call is the hard-sync
        calls = db.tracked_tickers.update_many.call_args_list
        assert len(calls) == 2, "Expected exactly 2 update_many calls (deactivate + hard-sync)"

        hard_sync_call = calls[1]
        filter_doc, update_doc = hard_sync_call.args

        assert filter_doc["exchange"] == {"$in": ["NYSE"]}
        assert filter_doc["asset_type"] == "Common Stock"
        assert "ticker" in filter_doc
        assert "$nin" in filter_doc["ticker"]
        assert "AAPL.US" in filter_doc["ticker"]["$nin"]

        assert update_doc["$set"]["archived"] is True
        assert update_doc["$set"]["is_visible"] is False
        assert update_doc["$set"]["archived_reason"] == "not in latest seed"

    @pytest.mark.asyncio
    async def test_hard_sync_archived_zero_when_all_tickers_in_seed(self):
        """
        When no tickers have drifted, hard_sync_archived should be 0.
        """
        from whitelist_service import sync_ticker_whitelist

        symbols = [_make_symbol("MSFT"), _make_symbol("GOOG")]

        db = _make_db_mock(
            candidate_tickers={"MSFT.US", "GOOG.US"},
            hard_sync_modified=0,
        )

        with (
            patch("whitelist_service.fetch_exchange_symbols", new=AsyncMock(return_value=symbols)),
            patch("whitelist_service.save_universe_seed_exclusion_report", new=AsyncMock(
                return_value={"exclusion_report_run_id": "run_test2", "_debug": {}}
            )),
            patch("whitelist_service.upsert_provider_debug_snapshot", new=AsyncMock()),
        ):
            result = await sync_ticker_whitelist(
                db,
                dry_run=False,
                exchanges=["NYSE"],
                job_run_id="test_run_002",
            )

        assert result["hard_sync_archived"] == 0

    @pytest.mark.asyncio
    async def test_hard_sync_not_called_in_dry_run(self):
        """
        In dry_run mode, no DB writes should occur (hard-sync must not run).
        """
        from whitelist_service import sync_ticker_whitelist

        symbols = [_make_symbol("TSLA")]

        db = MagicMock()
        db.tracked_tickers.find = MagicMock(
            return_value=MagicMock(to_list=AsyncMock(return_value=[]))
        )
        db.tracked_tickers.update_many = AsyncMock()

        with (
            patch("whitelist_service.fetch_exchange_symbols", new=AsyncMock(return_value=symbols)),
        ):
            result = await sync_ticker_whitelist(
                db,
                dry_run=True,
                exchanges=["NYSE"],
                job_run_id="test_run_dry",
            )

        # update_many must not have been called at all in dry_run
        db.tracked_tickers.update_many.assert_not_called()
        # hard_sync_archived still present in result (initialised to 0)
        assert result.get("hard_sync_archived", 0) == 0

    @pytest.mark.asyncio
    async def test_hard_sync_uses_exchanges_param(self):
        """
        The hard-sync filter must use the `exchanges` parameter, not a
        hard-coded constant, so partial-exchange runs are scoped correctly.
        """
        from whitelist_service import sync_ticker_whitelist

        symbols = [_make_symbol("IBM", exchange="NYSE")]

        db = _make_db_mock(
            candidate_tickers={"IBM.US"},
            hard_sync_modified=1,
        )

        with (
            patch("whitelist_service.fetch_exchange_symbols", new=AsyncMock(return_value=symbols)),
            patch("whitelist_service.save_universe_seed_exclusion_report", new=AsyncMock(
                return_value={"exclusion_report_run_id": "run_test3", "_debug": {}}
            )),
            patch("whitelist_service.upsert_provider_debug_snapshot", new=AsyncMock()),
        ):
            result = await sync_ticker_whitelist(
                db,
                dry_run=False,
                exchanges=["NYSE"],  # only NYSE — NASDAQ must NOT appear in filter
                job_run_id="test_run_003",
            )

        calls = db.tracked_tickers.update_many.call_args_list
        hard_sync_filter = calls[1].args[0]
        assert hard_sync_filter["exchange"] == {"$in": ["NYSE"]}
        assert "NASDAQ" not in hard_sync_filter["exchange"]["$in"]
