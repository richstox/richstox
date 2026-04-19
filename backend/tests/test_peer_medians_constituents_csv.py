"""
Unit tests for GET /api/admin/peer-medians/constituents/csv endpoint.

Validates the **evidence-based** CSV export that computes dividend diagnostics
from canonical sources (tracked_tickers, stock_prices, company_financials,
dividend_history) using compute_canonical_dividend_yield.

Covers:
  1. Happy path: per-ticker rows with dividend diagnostics + 7-metric values
  2. Footer totals (total_in_group, visible_in_group, dividend_included_count,
     dividend_excluded_count_by_reason)
  3. 404 when no tickers exist for group
  4. Market-level support
  5. Exclusion reasons (not_visible, na:missing_inputs, etc.)
  6. Filename sanitization

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

_backend = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _backend not in sys.path:
    sys.path.insert(0, _backend)


# ---------------------------------------------------------------------------
# Helpers — mock MongoDB collections
# ---------------------------------------------------------------------------
def _make_find_cursor(docs):
    """Return an AsyncMock that supports .sort().to_list() chains."""
    cursor = AsyncMock()
    cursor.sort = MagicMock(return_value=cursor)
    cursor.to_list = AsyncMock(return_value=list(docs))
    return cursor


def _make_agg_cursor(docs):
    """Return an async iterator over docs (for aggregate)."""
    async def _iter(pipeline):
        for d in docs:
            yield d
    return _iter


class FakeCollection:
    """Minimal mock of a Motor collection."""

    def __init__(self, *, find_one_return=None, find_docs=None, agg_docs=None):
        self._find_one_return = find_one_return
        self._find_docs = find_docs or []
        self._agg_docs = agg_docs or []

    async def find_one(self, *args, **kwargs):
        return self._find_one_return

    def find(self, *args, **kwargs):
        return _make_find_cursor(self._find_docs)

    def aggregate(self, pipeline):
        return _make_agg_cursor(self._agg_docs)(pipeline)


# ---------------------------------------------------------------------------
# Sample data
# ---------------------------------------------------------------------------
TICKER_AAPL = {
    "ticker": "AAPL.US", "is_visible": True, "has_price_data": True,
    "sector": "Technology", "industry": "Consumer Electronics",
    "financial_currency": "USD", "shares_outstanding": 15000000000,
    "fundamentals_status": "complete",
}
TICKER_MSFT = {
    "ticker": "MSFT.US", "is_visible": True, "has_price_data": True,
    "sector": "Technology", "industry": "Consumer Electronics",
    "financial_currency": "USD", "shares_outstanding": 7500000000,
    "fundamentals_status": "complete",
}
TICKER_INVISIBLE = {
    "ticker": "GONE.US", "is_visible": False, "has_price_data": False,
    "sector": "Technology", "industry": "Consumer Electronics",
    "financial_currency": "USD", "shares_outstanding": 1000000,
    "fundamentals_status": "complete",
}
TICKER_NO_FUNDAMENTALS = {
    "ticker": "NOFUN.US", "is_visible": True, "has_price_data": True,
    "sector": "Technology", "industry": "Consumer Electronics",
    "financial_currency": "USD", "shares_outstanding": 1000000,
    "fundamentals_status": "pending",
}

SAMPLE_CONSTITUENTS = {
    "level": "industry",
    "group": "Consumer Electronics",
    "computed_at": datetime(2026, 4, 19, 12, 0, 0, tzinfo=timezone.utc),
    "metrics": {
        "net_margin_ttm": {
            "tickers": ["AAPL.US", "MSFT.US"],
            "values": [25.3, 36.7],
            "currency_filter": "all_known_currency",
            "value_filter": "not_null",
            "n_records": 2,
        },
        "dividend_yield_ttm": {
            "tickers": ["AAPL.US", "MSFT.US"],
            "values": [0.55, 0.82],
            "currency_filter": "all_known_currency",
            "value_filter": ">=0",
            "n_records": 2,
        },
    },
}

SAMPLE_PEER_BENCHMARKS = {
    "industry": "Consumer Electronics",
    "step4_medians": {
        "net_margin_ttm": {"median": 31.0, "n_used": 2},
        "dividend_yield_ttm": {"median": 0.685, "n_used": 2},
    },
}

# stock_prices aggregates
SP_AGG_DOCS = [
    {"_id": "AAPL.US", "price": 175.50, "date": "2026-04-18"},
    {"_id": "MSFT.US", "price": 420.00, "date": "2026-04-18"},
]

# dividend_history aggregates
DH_AGG_DOCS = [
    {"_id": "AAPL.US", "total_amount": 0.96, "count": 4},
    {"_id": "MSFT.US", "total_amount": 3.00, "count": 4},
]

# company_financials cashflow docs (4 quarters each, sorted by period_date desc)
CF_DOCS = [
    {"ticker": "AAPL.US", "period_date": "2026-03-31", "dividends_paid": -3800000000},
    {"ticker": "AAPL.US", "period_date": "2025-12-31", "dividends_paid": -3800000000},
    {"ticker": "AAPL.US", "period_date": "2025-09-30", "dividends_paid": -3700000000},
    {"ticker": "AAPL.US", "period_date": "2025-06-30", "dividends_paid": -3700000000},
    {"ticker": "MSFT.US", "period_date": "2026-03-31", "dividends_paid": -5600000000},
    {"ticker": "MSFT.US", "period_date": "2025-12-31", "dividends_paid": -5600000000},
    {"ticker": "MSFT.US", "period_date": "2025-09-30", "dividends_paid": -5500000000},
    {"ticker": "MSFT.US", "period_date": "2025-06-30", "dividends_paid": -5500000000},
]


def _build_db(tracked_tickers=None, cons_doc=None, pb_doc=None,
              sp_docs=None, dh_docs=None, cf_docs=None):
    """Build a fake DB object."""
    db = MagicMock()
    db.tracked_tickers = FakeCollection(find_docs=tracked_tickers or [])
    db.peer_benchmarks_constituents = FakeCollection(find_one_return=cons_doc)
    db.peer_benchmarks = FakeCollection(find_one_return=pb_doc)
    db.stock_prices = FakeCollection(agg_docs=sp_docs or [])
    db.dividend_history = FakeCollection(agg_docs=dh_docs or [])
    db.company_financials = FakeCollection(find_docs=cf_docs or [])
    return db


def _parse_csv(text: str):
    reader = csv.reader(io.StringIO(text))
    comments, header_rows, footer_rows = [], [], []
    in_footer = False
    for row in reader:
        if not row:
            continue
        if row[0] == "__FOOTER__":
            in_footer = True
            continue
        if row[0].startswith("#"):
            comments.append(row[0])
        elif in_footer:
            footer_rows.append(row)
        else:
            header_rows.append(row)
    return comments, header_rows, footer_rows


async def _call_endpoint(fake_db, level, group_name=""):
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
        assert handler is not None, "Endpoint not found"
        resp = await handler(level=level, group_name=group_name)
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
async def test_evidence_based_csv_happy_path():
    """Industry-level CSV with dividend diagnostics for 2 visible + 1 invisible ticker."""
    fake_db = _build_db(
        tracked_tickers=[TICKER_AAPL, TICKER_MSFT, TICKER_INVISIBLE],
        cons_doc=SAMPLE_CONSTITUENTS,
        pb_doc=SAMPLE_PEER_BENCHMARKS,
        sp_docs=SP_AGG_DOCS,
        dh_docs=DH_AGG_DOCS,
        cf_docs=CF_DOCS,
    )
    _resp, body = await _call_endpoint(fake_db, "industry", "Consumer Electronics")

    comments, rows, footer = _parse_csv(body)

    # ── Comment header rows ──
    assert any("level: industry" in c for c in comments)
    assert any("group_name: Consumer Electronics" in c for c in comments)
    assert any("total_in_group: 3" in c for c in comments)
    assert any("visible_in_group: 2" in c for c in comments)
    assert any("dividend_included_count:" in c for c in comments)

    # ── Column headers ──
    header = rows[0]
    assert header[0] == "ticker"
    assert "net_margin_ttm" in header
    assert "is_visible" in header
    assert "dividend_yield_ttm_value" in header
    assert "included_in_dividend_peer_pool" in header
    assert "excluded_reason" in header

    # ── __MEDIAN__ row ──
    median_row = rows[1]
    assert median_row[0] == "__MEDIAN__"

    # ── Ticker rows (sorted alphabetically) ──
    ticker_rows = rows[2:]
    tickers = [r[0] for r in ticker_rows]
    assert "AAPL.US" in tickers
    assert "MSFT.US" in tickers
    assert "GONE.US" in tickers  # invisible ticker is included

    # Check AAPL has diagnostic columns
    aapl_idx = tickers.index("AAPL.US")
    aapl = ticker_rows[aapl_idx]
    vis_idx = header.index("is_visible")
    assert aapl[vis_idx] == "True"

    dh_count_idx = header.index("dividend_history_count_365d")
    assert aapl[dh_count_idx] == "4"

    pool_idx = header.index("included_in_dividend_peer_pool")
    assert aapl[pool_idx] == "True"

    # GONE.US should be excluded as not_visible
    gone_idx = tickers.index("GONE.US")
    gone = ticker_rows[gone_idx]
    assert gone[vis_idx] == "False"
    excl_idx = header.index("excluded_reason")
    assert gone[excl_idx] == "not_visible"
    assert gone[pool_idx] == "False"

    # ── Footer ──
    footer_map = {r[0]: r[1] for r in footer}
    assert footer_map["total_in_group"] == "3"
    assert footer_map["visible_in_group"] == "2"


@pytest.mark.asyncio
async def test_csv_404_no_tickers():
    """Returns 404 when no tickers found for group."""
    fake_db = _build_db(tracked_tickers=[])
    from fastapi import HTTPException

    with pytest.raises(HTTPException) as exc_info:
        await _call_endpoint(fake_db, "industry", "NonExistent Industry")
    assert exc_info.value.status_code == 404


@pytest.mark.asyncio
async def test_csv_market_level():
    """Market level includes all tickers regardless of group."""
    fake_db = _build_db(
        tracked_tickers=[TICKER_AAPL, TICKER_MSFT],
        cons_doc={**SAMPLE_CONSTITUENTS, "level": "market", "group": "market"},
        pb_doc={"sector": None, "industry": None, "step4_medians": {}},
        sp_docs=SP_AGG_DOCS,
        dh_docs=DH_AGG_DOCS,
        cf_docs=CF_DOCS,
    )
    _resp, body = await _call_endpoint(fake_db, "market", "")

    comments, rows, _footer = _parse_csv(body)
    assert any("level: market" in c for c in comments)
    assert any("group_name: US Market" in c for c in comments)
    # Both visible tickers should appear
    ticker_rows = rows[2:]
    tickers = [r[0] for r in ticker_rows]
    assert "AAPL.US" in tickers
    assert "MSFT.US" in tickers


@pytest.mark.asyncio
async def test_csv_exclusion_fundamentals_incomplete():
    """Ticker with fundamentals_status != complete is excluded as fundamentals_incomplete."""
    fake_db = _build_db(
        tracked_tickers=[TICKER_AAPL, TICKER_NO_FUNDAMENTALS],
        cons_doc=SAMPLE_CONSTITUENTS,
        pb_doc=SAMPLE_PEER_BENCHMARKS,
        sp_docs=SP_AGG_DOCS,
        dh_docs=DH_AGG_DOCS,
        cf_docs=CF_DOCS,
    )
    _resp, body = await _call_endpoint(fake_db, "industry", "Consumer Electronics")

    comments, rows, footer = _parse_csv(body)
    header = rows[0]
    excl_idx = header.index("excluded_reason")
    ticker_rows = rows[2:]

    nofun = next(r for r in ticker_rows if r[0] == "NOFUN.US")
    assert nofun[excl_idx] == "fundamentals_incomplete"

    footer_map = {r[0]: r[1] for r in footer}
    assert "dividend_excluded_fundamentals_incomplete" in footer_map


@pytest.mark.asyncio
async def test_csv_filename_sanitized():
    """Filename sanitizes special characters."""
    fake_db = _build_db(
        tracked_tickers=[TICKER_AAPL],
        cons_doc={**SAMPLE_CONSTITUENTS, "group": "Technology & Electronics"},
        pb_doc=SAMPLE_PEER_BENCHMARKS,
        sp_docs=SP_AGG_DOCS,
        dh_docs=DH_AGG_DOCS,
        cf_docs=CF_DOCS,
    )
    resp, _body = await _call_endpoint(fake_db, "industry", "Technology & Electronics")
    cd = resp.headers.get("content-disposition", "")
    assert "&" not in cd.split("filename=")[-1]
    assert "Technology___Electronics" in cd

