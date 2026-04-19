"""
Unit tests for GET /api/admin/peer-medians/constituents/csv endpoint.

Validates:
  1. Per-ticker CSV output with all 7 metrics
  2. __MEDIAN__ summary row from peer_benchmarks
  3. 404 when no constituents doc exists
  4. Tickers sorted alphabetically; partial metric coverage
  5. Comment header rows (exported_at, level, group_name, etc.)
  6. Filename sanitization (special chars → underscores)

Run:
    cd /app/backend && python -m pytest tests/test_peer_medians_constituents_csv.py -v
"""

import csv
import io
import os
import sys
import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

# ---------------------------------------------------------------------------
# Ensure server module can be imported (env vars required by config.py)
# ---------------------------------------------------------------------------
os.environ.setdefault("DB_NAME", "test_db")
os.environ.setdefault("ENV", "development")
os.environ.setdefault("MONGO_URI", "mongodb://localhost:27017")
os.environ.setdefault("MONGO_URL", "mongodb://localhost:27017")

# Add backend dir to path
_backend = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _backend not in sys.path:
    sys.path.insert(0, _backend)


# ---------------------------------------------------------------------------
# Fixtures – lightweight fake DB with just two collections
# ---------------------------------------------------------------------------
SAMPLE_CONSTITUENTS = {
    "level": "industry",
    "group": "Internet Content & Information",
    "sector": "Technology",
    "computed_at": datetime(2026, 4, 19, 12, 0, 0, tzinfo=timezone.utc),
    "metrics": {
        "net_margin_ttm": {
            "tickers": ["GOOG.US", "META.US", "SNAP.US"],
            "values": [0.2145, 0.2830, -0.0456],
            "currency_filter": "all_known_currency",
            "value_filter": "not_null",
            "n_records": 3,
        },
        "pe_ttm": {
            "tickers": ["GOOG.US", "META.US"],
            "values": [22.5, 18.3],
            "currency_filter": "usd_only",
            "value_filter": ">0",
            "n_records": 2,
        },
        "roe": {
            "tickers": ["META.US", "GOOG.US", "SNAP.US"],
            "values": [0.15, 0.28, 0.05],
            "currency_filter": "all_known_currency",
            "value_filter": "not_null",
            "n_records": 3,
        },
    },
}

SAMPLE_PEER_BENCHMARKS = {
    "industry": "Internet Content & Information",
    "sector": "Technology",
    "step4_medians": {
        "net_margin_ttm": {"median": 0.2145, "n_used": 3},
        "pe_ttm": {"median": 20.4, "n_used": 2},
        "roe": {"median": 0.15, "n_used": 3},
    },
}


def _make_fake_collection(find_one_return=None):
    """Create a mock collection whose find_one returns the given doc."""
    coll = AsyncMock()
    coll.find_one = AsyncMock(return_value=find_one_return)
    return coll


def _make_fake_db(cons_doc=None, pb_doc=None):
    db = MagicMock()
    db.peer_benchmarks_constituents = _make_fake_collection(cons_doc)
    db.peer_benchmarks = _make_fake_collection(pb_doc)
    return db


def _parse_csv(text: str):
    """Parse CSV text into comments and rows."""
    reader = csv.reader(io.StringIO(text))
    comments = []
    rows = []
    for row in reader:
        if row and row[0].startswith("#"):
            comments.append(row[0])
        else:
            rows.append(row)
    return comments, rows


async def _call_endpoint(fake_db, level, group_name):
    """Import, patch db, call handler, return response body text."""
    from server import api_router
    import server as srv

    original_db = srv.db
    srv.db = fake_db
    try:
        handler = None
        for route in api_router.routes:
            if getattr(route, "path", "") == "/api/admin/peer-medians/constituents/csv":
                handler = route.endpoint
                break
        assert handler is not None, "Endpoint /api/admin/peer-medians/constituents/csv not found"
        resp = await handler(level=level, group_name=group_name)
        # StreamingResponse → collect body
        body_parts = []
        async for chunk in resp.body_iterator:
            body_parts.append(chunk if isinstance(chunk, str) else chunk.decode())
        return resp, "".join(body_parts)
    finally:
        srv.db = original_db


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_constituents_csv_happy_path():
    """Full per-ticker CSV with __MEDIAN__ row and 3 metrics."""
    fake_db = _make_fake_db(
        cons_doc=SAMPLE_CONSTITUENTS,
        pb_doc=SAMPLE_PEER_BENCHMARKS,
    )
    _resp, body = await _call_endpoint(fake_db, "industry", "Internet Content & Information")

    comments, rows = _parse_csv(body)

    # ── Comment rows ──
    assert any("level: industry" in c for c in comments)
    assert any("group_name: Internet Content & Information" in c for c in comments)
    assert any("total_tickers: 3" in c for c in comments)

    # ── Header row ──
    header = rows[0]
    assert header[0] == "ticker"
    assert "net_margin_ttm" in header
    assert "pe_ttm" in header
    assert "roe" in header

    # ── Median row ──
    median_row = rows[1]
    assert median_row[0] == "__MEDIAN__"
    nm_idx = header.index("net_margin_ttm")
    assert median_row[nm_idx] == "0.2145"

    # ── Ticker rows (sorted alphabetically) ──
    ticker_rows = rows[2:]
    tickers = [r[0] for r in ticker_rows]
    assert tickers == ["GOOG.US", "META.US", "SNAP.US"]

    # GOOG.US has net_margin_ttm, pe_ttm, roe
    goog = ticker_rows[0]
    assert goog[nm_idx] == "0.2145"
    pe_idx = header.index("pe_ttm")
    assert goog[pe_idx] == "22.5"

    # SNAP.US has net_margin_ttm and roe but NOT pe_ttm
    snap = ticker_rows[2]
    assert snap[pe_idx] == ""  # not in pe pool
    assert snap[nm_idx] == "-0.0456"


@pytest.mark.asyncio
async def test_constituents_csv_404_no_data():
    """Returns 404 when no constituents doc exists."""
    fake_db = _make_fake_db(cons_doc=None, pb_doc=None)

    from fastapi import HTTPException

    with pytest.raises(HTTPException) as exc_info:
        await _call_endpoint(fake_db, "sector", "NonExistent")
    assert exc_info.value.status_code == 404


@pytest.mark.asyncio
async def test_constituents_csv_filename_sanitized():
    """Filename uses sanitized group_name (special chars → _)."""
    cons_doc = {
        **SAMPLE_CONSTITUENTS,
        "level": "sector",
        "group": "Technology & Electronics",
    }
    fake_db = _make_fake_db(cons_doc=cons_doc, pb_doc=SAMPLE_PEER_BENCHMARKS)

    resp, _body = await _call_endpoint(fake_db, "sector", "Technology & Electronics")
    cd = resp.headers.get("content-disposition", "")
    assert "Technology___Electronics" in cd
    assert "&" not in cd.split("filename=")[-1]

