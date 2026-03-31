import os
import sys
import asyncio
from unittest.mock import AsyncMock, MagicMock

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from fundamentals_service import parse_company_fundamentals
from provider_debug_service import build_provider_debug_snapshot, upsert_provider_debug_snapshot


def test_build_provider_debug_snapshot_keeps_debug_only_sections():
    raw_payload = {
        "General": {
            "Name": "Acme Corp",
            "Code": "ACME",
            "Officers": [{"name": "CEO"}],
            "CurrencyName": "US Dollar",
        },
        "Highlights": {"PERatio": 22.1, "MarketCapitalization": 123456},
        "Valuation": {"PriceBookMRQ": 3.1},
        "Technicals": {"Beta": 1.2},
        "Financials": {"Income_Statement": {"quarterly": {"2025-12-31": {"netIncome": 1}}}},
    }

    snapshot = build_provider_debug_snapshot(
        ticker="acme",
        raw_payload=raw_payload,
        source_job="unit_test",
    )

    assert snapshot["debug_only"] is True
    assert snapshot["runtime_allowed"] is False
    assert snapshot["ticker"] == "ACME.US"
    assert "Highlights" in snapshot["forbidden_sections"]
    assert "Valuation" in snapshot["forbidden_sections"]
    assert "Technicals" in snapshot["forbidden_sections"]
    assert "GeneralForbiddenFields" in snapshot["forbidden_sections"]
    assert "Financials" not in snapshot["forbidden_sections"]


def test_parse_company_fundamentals_never_stores_provider_metrics():
    raw_payload = {
        "General": {
            "Code": "ACME",
            "Name": "Acme Corp",
            "Exchange": "NASDAQ",
            "Sector": "Technology",
            "Industry": "Software",
        },
        "Highlights": {
            "MarketCapitalization": 999999999,
            "PERatio": 50,
            "ProfitMargin": 0.31,
            "EarningsShare": 99,  # should not be used as fallback
        },
        "Valuation": {"PriceBookMRQ": 8.8, "EnterpriseValue": 123456789},
        "Technicals": {"Beta": 1.9, "52WeekHigh": 200},
        "Earnings": {
            "History": {
                "q1": {"reportDate": "2025-12-31", "epsActual": 1.0},
                "q2": {"reportDate": "2025-09-30", "epsActual": 2.0},
                "q3": {"reportDate": "2025-06-30", "epsActual": 3.0},
                "q4": {"reportDate": "2025-03-31", "epsActual": 4.0},
            }
        },
    }

    doc = parse_company_fundamentals("ACME", raw_payload)

    assert doc["market_cap"] is None
    assert doc["enterprise_value"] is None
    assert doc["pe_ratio"] is None
    assert doc["ps_ratio"] is None
    assert doc["pb_ratio"] is None
    assert doc["ev_ebitda"] is None
    assert doc["ev_revenue"] is None
    assert doc["dividend_yield"] is None
    assert doc["beta"] is None
    assert doc["book_value"] is None
    assert doc["ebitda"] is None
    assert doc["eps_ttm"] == 10.0


class TestProviderDebugSnapshotUpsertNoConflict:
    """Verify the upsert does not have $setOnInsert/$inc conflict on capture_count."""

    def test_no_capture_count_in_set_on_insert(self):
        """capture_count must NOT appear in $setOnInsert (would conflict with $inc)."""
        source = open(
            os.path.join(os.path.dirname(__file__), "..", "provider_debug_service.py")
        ).read()
        # Find the upsert block
        start = source.index("await db.provider_debug_snapshot.update_one")
        block = source[start:start + 400]
        # Extract just the $setOnInsert value (up to the next $)
        set_on_insert_start = block.index("$setOnInsert")
        rest = block[set_on_insert_start:]
        # Find the closing brace of $setOnInsert value dict
        brace_count = 0
        end = 0
        for i, ch in enumerate(rest):
            if ch == "{":
                brace_count += 1
            elif ch == "}":
                brace_count -= 1
                if brace_count == 0:
                    end = i + 1
                    break
        set_on_insert_block = rest[:end]
        assert "capture_count" not in set_on_insert_block, (
            "$setOnInsert must not include capture_count "
            "(conflicts with $inc on the same path)"
        )

    def test_inc_capture_count_present(self):
        """$inc must include capture_count: 1 for idempotent counting."""
        source = open(
            os.path.join(os.path.dirname(__file__), "..", "provider_debug_service.py")
        ).read()
        start = source.index("await db.provider_debug_snapshot.update_one")
        block = source[start:start + 400]
        assert '"$inc"' in block or "'$inc'" in block, "$inc operator must be present"
        inc_start = block.index("$inc")
        inc_block = block[inc_start:inc_start + 80]
        assert "capture_count" in inc_block, "$inc must increment capture_count"

    def test_consecutive_upserts_succeed(self):
        """Two upserts for the same ticker must not raise an error."""
        mock_collection = AsyncMock()
        mock_collection.update_one = AsyncMock(
            return_value=MagicMock(upserted_id=None, modified_count=1)
        )
        mock_db = MagicMock()
        mock_db.provider_debug_snapshot = mock_collection

        payload = {
            "Highlights": {"PERatio": 15.0},
            "General": {"Code": "TEST", "Name": "Test Corp"},
        }

        # First upsert (insert)
        result1 = asyncio.run(
            upsert_provider_debug_snapshot(
                db=mock_db,
                ticker="TEST",
                raw_payload=payload,
                source_job="unit_test",
            )
        )
        assert result1["stored"] is True
        assert result1["ticker"] == "TEST.US"

        # Second upsert (update) — must not raise
        result2 = asyncio.run(
            upsert_provider_debug_snapshot(
                db=mock_db,
                ticker="TEST",
                raw_payload=payload,
                source_job="unit_test",
            )
        )
        assert result2["stored"] is True

        # Verify update_one was called twice
        assert mock_collection.update_one.call_count == 2

        # Verify the update dict does NOT have capture_count in $setOnInsert
        for call in mock_collection.update_one.call_args_list:
            update_dict = call[0][1]  # second positional arg is the update
            set_on_insert = update_dict.get("$setOnInsert", {})
            assert "capture_count" not in set_on_insert, (
                "$setOnInsert must not include capture_count"
            )
            inc = update_dict.get("$inc", {})
            assert inc.get("capture_count") == 1, (
                "$inc must increment capture_count by 1"
            )
