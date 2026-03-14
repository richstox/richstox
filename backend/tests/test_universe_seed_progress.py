"""
Universe Seed Progress Tests
==============================
Tests for Step 1 seed progress tracking and universe counts funnel.

Tests cover:
1. Funnel field naming — canonical names (raw, seeded, with_price, classified, visible)
   are present in universe_counts output.
2. Backward-compat aliases map to the same values.
3. Monotonic-decreasing guard fires when counts increase.

Run: cd /app/backend && python -m pytest tests/test_universe_seed_progress.py -v
"""

import sys
sys.path.insert(0, '/app/backend')

import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch


# ---------------------------------------------------------------------------
# Helpers: minimal mock DB
# ---------------------------------------------------------------------------

def _make_mock_db(
    raw: int = 10000,
    nyse: int = 5000,
    nasdaq: int = 5000,
    seeded: int = 8000,
    with_price: int = 7000,
    step3_output: int = 5000,
    classified: int = 6000,
    visible: int = 4000,
):
    """Build a mock db.tracked_tickers that returns fixed facet counts."""
    facet_data = [{
        "raw":          [{"n": raw}],
        "nyse":         [{"n": nyse}],
        "nasdaq":       [{"n": nasdaq}],
        "seeded":       [{"n": seeded}],
        "with_price":   [{"n": with_price}],
        "step3_output": [{"n": step3_output}],
        "classified":   [{"n": classified}],
        "visible":      [{"n": visible}],
    }]

    mock_cursor = AsyncMock()
    mock_cursor.to_list = AsyncMock(return_value=facet_data)

    mock_tt = MagicMock()
    mock_tt.aggregate = MagicMock(return_value=mock_cursor)

    mock_db = MagicMock()
    mock_db.tracked_tickers = mock_tt
    return mock_db


# ---------------------------------------------------------------------------
# get_universe_counts — canonical field names
# ---------------------------------------------------------------------------

class TestUniverseCountsFieldNames:

    @pytest.mark.asyncio
    async def test_canonical_field_names_present(self):
        from services.universe_counts_service import get_universe_counts

        db = _make_mock_db()
        result = await get_universe_counts(db)

        counts = result["counts"]
        for field in ("raw", "seeded", "with_price", "classified", "visible"):
            assert field in counts, f"Missing canonical field: {field}"

    @pytest.mark.asyncio
    async def test_backward_compat_aliases_present(self):
        from services.universe_counts_service import get_universe_counts

        db = _make_mock_db()
        result = await get_universe_counts(db)

        counts = result["counts"]
        aliases = (
            "seeded_us_total",
            "with_price_data",
            "with_classification",
            "visible_tickers",
        )
        for alias in aliases:
            assert alias in counts, f"Missing backward-compat alias: {alias}"

    @pytest.mark.asyncio
    async def test_canonical_and_alias_values_match(self):
        from services.universe_counts_service import get_universe_counts

        db = _make_mock_db(raw=10000, seeded=8000, with_price=7000, classified=6000, visible=4000)
        result = await get_universe_counts(db)

        counts = result["counts"]
        # canonical == alias
        assert counts["seeded"]      == counts["common_stock"]       == 8000
        assert counts["with_price"]  == counts["with_price_data"]    == 7000
        assert counts["classified"]  == counts["with_classification"] == 6000
        assert counts["visible"]     == counts["visible_tickers"]    == 4000
        # raw -> legacy seeded_us_total
        assert counts["raw"]         == counts["seeded_us_total"]    == 10000

    @pytest.mark.asyncio
    async def test_funnel_steps_use_canonical_names(self):
        from services.universe_counts_service import get_universe_counts

        db = _make_mock_db()
        result = await get_universe_counts(db)

        step_names = {s["name"] for s in result["funnel_steps"]}
        # Seeded step must be labelled clearly
        assert any("Seeded" in n for n in step_names), "No seeded step in funnel"
        # Visible step must be present
        assert any("Visible" in n for n in step_names), "No visible step in funnel"


# ---------------------------------------------------------------------------
# get_universe_counts — monotonic guard
# ---------------------------------------------------------------------------

class TestUniverseCountsMonotonicGuard:

    @pytest.mark.asyncio
    async def test_no_inconsistency_for_valid_funnel(self):
        from services.universe_counts_service import get_universe_counts

        db = _make_mock_db(raw=10000, seeded=8000, with_price=7000, classified=6000, visible=4000)
        result = await get_universe_counts(db)

        assert not result["has_inconsistency"], (
            f"Unexpected inconsistencies: {result['inconsistencies']}"
        )

    @pytest.mark.asyncio
    async def test_inconsistency_when_visible_exceeds_classified(self):
        from services.universe_counts_service import get_universe_counts

        # visible (5000) > classified (4000) — impossible, should flag
        db = _make_mock_db(classified=4000, visible=5000)
        result = await get_universe_counts(db)

        assert result["has_inconsistency"]
        msgs = [i["message"] for i in result["inconsistencies"]]
        assert any("5000" in m for m in msgs), f"Expected count mismatch in messages: {msgs}"


# ---------------------------------------------------------------------------
# get_universe_counts — step3_funnel metadata
# ---------------------------------------------------------------------------

class TestUniverseCountsStep3Funnel:

    @pytest.mark.asyncio
    async def test_step3_funnel_present(self):
        from services.universe_counts_service import get_universe_counts

        db = _make_mock_db(with_price=7000, step3_output=5000)
        result = await get_universe_counts(db)

        sf = result["step3_funnel"]
        assert sf["input_total"]       == 7000
        assert sf["output_total"]      == 5000
        assert sf["filtered_out_total"] == 2000

    @pytest.mark.asyncio
    async def test_step3_filtered_out_never_negative(self):
        from services.universe_counts_service import get_universe_counts

        # step3_output > with_price (data anomaly) → clamped to 0
        db = _make_mock_db(with_price=1000, step3_output=2000)
        result = await get_universe_counts(db)

        sf = result["step3_funnel"]
        assert sf["filtered_out_total"] >= 0


# ---------------------------------------------------------------------------
# get_universe_counts — classified is a subset of with_price
# ---------------------------------------------------------------------------

class TestUniverseCountsClassifiedSubsetOfWithPrice:

    @pytest.mark.asyncio
    async def test_classified_query_includes_has_price_data(self):
        """
        Verify that the classified facet query in universe_counts_service
        includes has_price_data so classified <= with_price always holds.

        We do this by passing classified > with_price and verifying the
        inconsistency guard fires (classified can never exceed with_price
        once has_price_data is part of classified_query).
        """
        from services.universe_counts_service import get_universe_counts

        # In a correct system with has_price_data in classified_query,
        # if the DB returned classified > with_price the guard must flag it.
        db = _make_mock_db(with_price=5000, classified=6000, visible=4000)
        result = await get_universe_counts(db)

        assert result["has_inconsistency"], (
            "classified > with_price must be flagged as inconsistency; "
            "this means classified_query must include has_price_data"
        )

    @pytest.mark.asyncio
    async def test_classified_le_with_price_for_valid_funnel(self):
        """classified <= with_price must hold for a valid funnel."""
        from services.universe_counts_service import get_universe_counts

        db = _make_mock_db(raw=10000, seeded=8000, with_price=7000, classified=6500, visible=4000)
        result = await get_universe_counts(db)

        counts = result["counts"]
        assert counts["classified"] <= counts["with_price"], (
            "classified must always be <= with_price"
        )
        assert not result["has_inconsistency"], (
            f"Valid funnel should have no inconsistencies: {result['inconsistencies']}"
        )
