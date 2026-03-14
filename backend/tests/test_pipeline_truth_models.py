import asyncio
import os
import sys
from unittest.mock import AsyncMock, MagicMock, patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from credit_log_service import get_pipeline_sync_status
from visibility_rules import compute_visibility_failed_reason, get_canonical_sieve_query


def test_canonical_visibility_query_matches_runtime_rule():
    query = get_canonical_sieve_query()

    assert query["exchange"] == {"$in": ["NYSE", "NASDAQ"]}
    assert query["asset_type"] == "Common Stock"
    assert query["has_price_data"] is True
    assert query["sector"] == {"$nin": [None, ""]}
    assert query["industry"] == {"$nin": [None, ""]}
    assert query["is_delisted"] == {"$ne": True}
    assert query["shares_outstanding"] == {"$gt": 0}
    assert query["financial_currency"] == {"$nin": [None, ""]}


def test_compute_visibility_failed_reason_tracks_first_real_blocker():
    assert compute_visibility_failed_reason({
        "exchange": "NYSE",
        "asset_type": "Common Stock",
        "has_price_data": False,
        "sector": "Tech",
        "industry": "Software",
        "shares_outstanding": 1,
        "financial_currency": "USD",
    }) == "NO_PRICE_DATA"

    assert compute_visibility_failed_reason({
        "exchange": "NYSE",
        "asset_type": "Common Stock",
        "has_price_data": True,
        "sector": "",
        "industry": "Software",
        "shares_outstanding": 1,
        "financial_currency": "USD",
    }) == "MISSING_SECTOR"


def test_pipeline_sync_status_reports_seeded_visible_and_refresh_truth():
    db = MagicMock()
    db.tracked_tickers.aggregate.return_value.to_list = AsyncMock(return_value=[{
        "seeded_total": [{"n": 10}],
        "visible_total": [{"n": 4}],
        "missing_price_data": [{"n": 3}],
        "missing_classification": [{"n": 2}],
        "pending_fundamentals": [{"n": 1}],
        "price_complete": [{"n": 3}],
        "fundamentals_complete": [{"n": 2}],
        "needs_price_redownload": [{"n": 1}],
        "needs_fundamentals_refresh": [{"n": 2}],
        "non_visible_reasons": [
            {"_id": "NO_PRICE_DATA", "count": 3},
            {"_id": "MISSING_SECTOR", "count": 2},
            {"_id": "MISSING_SHARES", "count": 1},
        ],
    }])

    db.tracked_tickers.find_one = AsyncMock(side_effect=[
        {"price_data_current_through": "2026-03-13"},
        {"fundamentals_updated_at": "2026-03-14T04:30:00+00:00"},
        {"price_refresh_requested_at": "2026-03-14T04:02:00+00:00"},
        {"fundamentals_refresh_requested_at": "2026-03-14T04:31:00+00:00"},
    ])

    async def _run():
        with patch("credit_log_service.get_daily_credit_usage", new=AsyncMock(return_value={"total_credits": 42})):
            return await get_pipeline_sync_status(db)

    result = asyncio.run(_run())

    assert result["seeded_tickers"] == 10
    assert result["visible_tickers"] == 4
    assert result["non_visible_tickers"] == 6
    assert result["missing_price_data"] == 3
    assert result["missing_classification"] == 2
    assert result["pending_fundamentals"] == 1
    assert result["needs_price_redownload"] == 1
    assert result["needs_fundamentals_refresh"] == 2
    assert result["price_data_updated_through"] == "2026-03-13"
    assert result["fundamentals_updated_through"] == "2026-03-14T04:30:00+00:00"
    assert result["visibility_failed_reasons"]["NO_PRICE_DATA"] == 3
    assert result["visibility_failed_reasons"]["MISSING_SHARES"] == 1
    assert result["price_history_pct"] == 75.0
    assert result["fundamentals_pct"] == 50.0
