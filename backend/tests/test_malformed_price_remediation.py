"""
Tests for admin malformed price document remediation endpoints.

Endpoints:
  GET  /api/admin/prices/malformed       — detect malformed stock_prices docs
  POST /api/admin/prices/purge-malformed — purge + reflag (dry_run toggle)

Malformed predicate: missing/null date OR missing/null close.

These tests use self-contained in-memory fakes (no server.py imports) because
importing server.py pulls in FastAPI, Motor, dotenv, and dozens of deps.

Run:
    cd /app/backend && python -m pytest tests/test_malformed_price_remediation.py -v
"""

import pytest
from bson import ObjectId
from copy import deepcopy


# ---------------------------------------------------------------------------
# Malformed predicate (replicated from server.py for unit-test isolation)
# ---------------------------------------------------------------------------
MALFORMED_PRICE_PREDICATE = {
    "$or": [
        {"date": {"$exists": False}},
        {"date": None},
        {"close": {"$exists": False}},
        {"close": None},
    ]
}


def _matches_predicate(doc: dict) -> bool:
    """Pure-Python evaluation of the malformed predicate for test assertions."""
    date_val = doc.get("date", _MISSING)
    close_val = doc.get("close", _MISSING)
    return date_val is _MISSING or date_val is None or close_val is _MISSING or close_val is None


_MISSING = object()  # sentinel


# ---------------------------------------------------------------------------
# In-memory fake MongoDB collections
# ---------------------------------------------------------------------------
class FakeCollection:
    """Minimal in-memory MongoDB collection fake supporting the ops used by
    the remediation endpoints."""

    def __init__(self, docs=None):
        self._docs = []
        for d in (docs or []):
            d = dict(d)
            if "_id" not in d:
                d["_id"] = ObjectId()
            self._docs.append(d)

    async def insert_one(self, doc):
        doc = dict(doc)
        if "_id" not in doc:
            doc["_id"] = ObjectId()
        self._docs.append(doc)

        class _R:
            inserted_id = doc["_id"]
        return _R()

    async def insert_many(self, docs):
        ids = []
        for d in docs:
            r = await self.insert_one(d)
            ids.append(r.inserted_id)

        class _R:
            inserted_ids = ids
        return _R()

    def find(self, query=None, projection=None):
        """Returns an async-iterable cursor."""
        matched = self._match(query or {})
        return _FakeCursor(matched, projection)

    async def find_one(self, query=None, projection=None):
        for doc in self._match(query or {}):
            return _project(doc, projection)
        return None

    async def count_documents(self, query=None):
        return len(self._match(query or {}))

    async def delete_many(self, query):
        before = len(self._docs)
        matched_ids = {d["_id"] for d in self._match(query)}
        self._docs = [d for d in self._docs if d["_id"] not in matched_ids]

        class _R:
            deleted_count = before - len(self._docs)
        return _R()

    async def update_many(self, query, update):
        matched = self._match(query)
        modified = 0
        for doc in matched:
            if "$set" in update:
                doc.update(update["$set"])
                modified += 1

        class _R:
            modified_count = modified
        return _R()

    def aggregate(self, pipeline):
        """Minimal aggregation support for $match → $group → $sort."""
        docs = list(self._docs)
        for stage in pipeline:
            if "$match" in stage:
                docs = [d for d in docs if self._eval_match(d, stage["$match"])]
            elif "$group" in stage:
                docs = self._eval_group(docs, stage["$group"])
            elif "$sort" in stage:
                for key, direction in reversed(list(stage["$sort"].items())):
                    docs.sort(key=lambda d, k=key: d.get(k, 0), reverse=(direction == -1))
        return _FakeAggCursor(docs)

    # --- internal helpers ---
    def _match(self, query):
        return [d for d in self._docs if self._eval_match(d, query)]

    @staticmethod
    def _eval_match(doc, query):
        for key, cond in query.items():
            if key == "$or":
                if not any(FakeCollection._eval_match(doc, sub) for sub in cond):
                    return False
                continue
            if isinstance(cond, dict):
                for op, val in cond.items():
                    if op == "$in":
                        if doc.get(key) not in val:
                            return False
                    elif op == "$exists":
                        if val and key not in doc:
                            return False
                        if not val and key in doc:
                            return False
                    else:
                        return False
            else:
                if doc.get(key) != cond:
                    return False
        return True

    @staticmethod
    def _eval_group(docs, spec):
        from collections import defaultdict
        groups = defaultdict(list)
        group_key = spec["_id"]  # e.g. "$ticker"
        field_name = group_key.lstrip("$") if isinstance(group_key, str) else None
        for d in docs:
            k = d.get(field_name) if field_name else None
            groups[k].append(d)
        result = []
        for k, group_docs in groups.items():
            row = {"_id": k}
            for out_field, expr in spec.items():
                if out_field == "_id":
                    continue
                if isinstance(expr, dict):
                    if "$sum" in expr:
                        row[out_field] = len(group_docs) if expr["$sum"] == 1 else sum(d.get(out_field, 0) for d in group_docs)
                    elif "$push" in expr:
                        push_expr = expr["$push"]
                        if isinstance(push_expr, dict) and "$toString" in push_expr:
                            field = push_expr["$toString"].lstrip("$")
                            row[out_field] = [str(d[field]) for d in group_docs]
                        elif isinstance(push_expr, str) and push_expr.startswith("$"):
                            field = push_expr.lstrip("$")
                            row[out_field] = [d.get(field) for d in group_docs]
                        else:
                            row[out_field] = [push_expr for _ in group_docs]
            result.append(row)
        return result

    @property
    def docs(self):
        return list(self._docs)


class _FakeCursor:
    def __init__(self, docs, projection=None):
        self._docs = docs
        self._projection = projection

    def __aiter__(self):
        self._iter = iter(self._docs)
        return self

    async def __anext__(self):
        try:
            doc = next(self._iter)
            return _project(doc, self._projection)
        except StopIteration:
            raise StopAsyncIteration

    async def to_list(self, length=None):
        return [_project(d, self._projection) for d in self._docs]


class _FakeAggCursor:
    def __init__(self, docs):
        self._docs = docs

    async def to_list(self, length=None):
        return list(self._docs)


def _project(doc, projection):
    if not projection:
        return dict(doc)
    out = {}
    for k, v in projection.items():
        if v == 0:
            continue
        if v == 1 and k in doc:
            out[k] = doc[k]
    if "_id" not in projection:
        out["_id"] = doc.get("_id")
    return out


# ---------------------------------------------------------------------------
# Replicated endpoint logic (self-contained, no server.py import)
# ---------------------------------------------------------------------------
async def _detect_malformed(stock_prices, tracked_tickers):
    """Replicated GET /admin/prices/malformed logic."""
    pipeline = [
        {"$match": MALFORMED_PRICE_PREDICATE},
        {"$group": {
            "_id": "$ticker",
            "malformed_count": {"$sum": 1},
            "sample_ids": {"$push": {"$toString": "$_id"}},
        }},
        {"$sort": {"malformed_count": -1}},
    ]
    groups = await stock_prices.aggregate(pipeline).to_list(None)

    if not groups:
        return {
            "items": [],
            "totals": {"affected_tickers": 0, "malformed_docs": 0, "visible_affected": 0},
            "markdown_table": "No malformed documents found.",
        }

    ticker_list = [g["_id"] for g in groups if g["_id"]]
    tt_cursor = tracked_tickers.find(
        {"ticker": {"$in": ticker_list}},
        {"_id": 0, "ticker": 1, "is_visible": 1, "needs_price_redownload": 1},
    )
    tt_map = {doc["ticker"]: doc async for doc in tt_cursor}

    items = []
    total_malformed = 0
    visible_affected = 0
    for g in groups:
        ticker = g["_id"] or "(no ticker)"
        count = g["malformed_count"]
        total_malformed += count
        sample = g["sample_ids"][:5]
        tt = tt_map.get(ticker, {})
        is_vis = tt.get("is_visible", False)
        needs_re = tt.get("needs_price_redownload", False)
        if is_vis:
            visible_affected += 1
        items.append({
            "ticker": ticker,
            "malformed_count": count,
            "sample_ids": sample,
            "is_visible": is_vis,
            "needs_price_redownload": needs_re,
        })

    totals = {
        "affected_tickers": len(items),
        "malformed_docs": total_malformed,
        "visible_affected": visible_affected,
    }

    md_lines = ["| Ticker | Malformed | Visible | Needs Redownload |",
                "|--------|-----------|---------|------------------|"]
    for it in items:
        md_lines.append(
            f"| {it['ticker']} | {it['malformed_count']} "
            f"| {'✅' if it['is_visible'] else '❌'} "
            f"| {'✅' if it['needs_price_redownload'] else '❌'} |"
        )
    md_lines.append(f"| **TOTAL** | **{total_malformed}** | **{visible_affected} visible** | |")

    return {
        "items": items,
        "totals": totals,
        "markdown_table": "\n".join(md_lines),
    }


async def _purge_malformed(stock_prices, tracked_tickers, dry_run: bool):
    """Replicated POST /admin/prices/purge-malformed logic."""
    pipeline = [
        {"$match": MALFORMED_PRICE_PREDICATE},
        {"$group": {
            "_id": "$ticker",
            "malformed_count": {"$sum": 1},
            "doc_ids": {"$push": "$_id"},
        }},
        {"$sort": {"malformed_count": -1}},
    ]
    groups = await stock_prices.aggregate(pipeline).to_list(None)

    if not groups:
        return {
            "dry_run": dry_run,
            "items": [],
            "totals": {"affected_tickers": 0, "malformed_docs": 0, "visible_affected": 0},
            "deleted_count": 0,
            "reflagged_count": 0,
            "markdown_table": "No malformed documents found.",
        }

    ticker_list = [g["_id"] for g in groups if g["_id"]]
    tt_cursor = tracked_tickers.find(
        {"ticker": {"$in": ticker_list}},
        {"_id": 0, "ticker": 1, "is_visible": 1},
    )
    tt_map = {doc["ticker"]: doc async for doc in tt_cursor}

    items = []
    total_malformed = 0
    all_doc_ids = []
    visible_affected = 0
    for g in groups:
        ticker = g["_id"] or "(no ticker)"
        count = g["malformed_count"]
        total_malformed += count
        all_doc_ids.extend(g["doc_ids"])
        tt = tt_map.get(ticker, {})
        is_vis = tt.get("is_visible", False)
        if is_vis:
            visible_affected += 1
        items.append({
            "ticker": ticker,
            "malformed_count": count,
            "sample_ids": [str(oid) for oid in g["doc_ids"][:5]],
            "is_visible": is_vis,
        })

    totals = {
        "affected_tickers": len(items),
        "malformed_docs": total_malformed,
        "visible_affected": visible_affected,
    }

    deleted_count = 0
    reflagged_count = 0

    if not dry_run:
        del_result = await stock_prices.delete_many({"_id": {"$in": all_doc_ids}})
        deleted_count = del_result.deleted_count

        if ticker_list:
            reflag_result = await tracked_tickers.update_many(
                {"ticker": {"$in": ticker_list}},
                {"$set": {
                    "needs_price_redownload": True,
                    "price_history_complete": False,
                    "price_history_status": "malformed_purged",
                    "history_download_error": "malformed_docs_purged",
                }},
            )
            reflagged_count = reflag_result.modified_count

    return {
        "dry_run": dry_run,
        "items": items,
        "totals": totals,
        "deleted_count": deleted_count,
        "reflagged_count": reflagged_count,
    }


# ---------------------------------------------------------------------------
# Test fixtures
# ---------------------------------------------------------------------------
def _make_valid_doc(ticker, date="2024-01-02", close=150.0):
    return {"_id": ObjectId(), "ticker": ticker, "date": date, "close": close, "volume": 100}


def _make_malformed_no_date(ticker):
    return {"_id": ObjectId(), "ticker": ticker, "close": None}


def _make_malformed_no_close(ticker):
    return {"_id": ObjectId(), "ticker": ticker, "date": "2024-01-02"}


def _make_malformed_null_date(ticker):
    return {"_id": ObjectId(), "ticker": ticker, "date": None, "close": 50.0}


def _make_malformed_null_close(ticker):
    return {"_id": ObjectId(), "ticker": ticker, "date": "2024-01-02", "close": None}


def _make_malformed_ticker_only(ticker):
    """Exact production shape: {ticker: "ALOT.US"} — no date, no close."""
    return {"_id": ObjectId(), "ticker": ticker}


@pytest.fixture
def sample_data():
    """Two malformed tickers (GRTUF.US, PFX.US) plus one clean ticker (AAPL.US)."""
    prices = [
        _make_valid_doc("AAPL.US", "2024-01-02", 150.0),
        _make_valid_doc("AAPL.US", "2024-01-03", 151.0),
        _make_malformed_ticker_only("GRTUF.US"),
        _make_malformed_null_close("GRTUF.US"),
        _make_malformed_no_date("PFX.US"),
    ]
    tickers = [
        {"ticker": "AAPL.US", "is_visible": True, "needs_price_redownload": False,
         "price_history_complete": True, "price_history_status": "complete"},
        {"ticker": "GRTUF.US", "is_visible": True, "needs_price_redownload": False,
         "price_history_complete": True, "price_history_status": "complete"},
        {"ticker": "PFX.US", "is_visible": False, "needs_price_redownload": False,
         "price_history_complete": True, "price_history_status": "complete"},
    ]
    return prices, tickers


# ---------------------------------------------------------------------------
# Predicate safety tests
# ---------------------------------------------------------------------------
class TestMalformedPredicate:
    """The predicate must match exactly: missing/null date OR missing/null close."""

    def test_valid_row_not_matched(self):
        assert not _matches_predicate({"ticker": "A", "date": "2024-01-02", "close": 150.0})

    def test_missing_date_matched(self):
        assert _matches_predicate({"ticker": "A", "close": 150.0})

    def test_null_date_matched(self):
        assert _matches_predicate({"ticker": "A", "date": None, "close": 150.0})

    def test_missing_close_matched(self):
        assert _matches_predicate({"ticker": "A", "date": "2024-01-02"})

    def test_null_close_matched(self):
        assert _matches_predicate({"ticker": "A", "date": "2024-01-02", "close": None})

    def test_ticker_only_doc_matched(self):
        assert _matches_predicate({"ticker": "ALOT.US"})

    def test_completely_empty_matched(self):
        assert _matches_predicate({})

    def test_zero_close_not_matched(self):
        """close=0 is NOT malformed by this predicate (validate_price_row catches it)."""
        assert not _matches_predicate({"ticker": "A", "date": "2024-01-02", "close": 0})


# ---------------------------------------------------------------------------
# Detect endpoint tests
# ---------------------------------------------------------------------------
class TestDetectMalformed:

    @pytest.mark.asyncio
    async def test_detect_returns_items_and_totals(self, sample_data):
        prices, tickers = sample_data
        sp = FakeCollection(prices)
        tt = FakeCollection(tickers)

        result = await _detect_malformed(sp, tt)

        assert result["totals"]["affected_tickers"] == 2
        assert result["totals"]["malformed_docs"] == 3
        # GRTUF.US is visible
        assert result["totals"]["visible_affected"] == 1

        ticker_names = [it["ticker"] for it in result["items"]]
        assert "GRTUF.US" in ticker_names
        assert "PFX.US" in ticker_names
        assert "AAPL.US" not in ticker_names

    @pytest.mark.asyncio
    async def test_detect_limits_sample_ids_to_5(self):
        """If a ticker has >5 malformed docs, sample_ids is capped at 5."""
        docs = [_make_malformed_ticker_only("BIG.US") for _ in range(10)]
        sp = FakeCollection(docs)
        tt = FakeCollection([{"ticker": "BIG.US", "is_visible": False, "needs_price_redownload": False}])

        result = await _detect_malformed(sp, tt)
        big = [it for it in result["items"] if it["ticker"] == "BIG.US"][0]
        assert len(big["sample_ids"]) == 5
        assert big["malformed_count"] == 10

    @pytest.mark.asyncio
    async def test_detect_no_malformed_returns_empty(self):
        sp = FakeCollection([_make_valid_doc("AAPL.US")])
        tt = FakeCollection([{"ticker": "AAPL.US", "is_visible": True}])

        result = await _detect_malformed(sp, tt)
        assert result["items"] == []
        assert result["totals"]["affected_tickers"] == 0

    @pytest.mark.asyncio
    async def test_detect_includes_markdown_table(self, sample_data):
        prices, tickers = sample_data
        sp = FakeCollection(prices)
        tt = FakeCollection(tickers)

        result = await _detect_malformed(sp, tt)
        assert "markdown_table" in result
        assert "GRTUF.US" in result["markdown_table"]


# ---------------------------------------------------------------------------
# Purge endpoint: dry_run vs execute
# ---------------------------------------------------------------------------
class TestPurgeDryRun:

    @pytest.mark.asyncio
    async def test_dry_run_does_not_delete(self, sample_data):
        prices, tickers = sample_data
        sp = FakeCollection(prices)
        tt = FakeCollection(tickers)

        result = await _purge_malformed(sp, tt, dry_run=True)

        assert result["dry_run"] is True
        assert result["totals"]["malformed_docs"] == 3
        assert result["deleted_count"] == 0
        assert result["reflagged_count"] == 0

        # Verify no docs were actually removed
        remaining = await sp.count_documents({})
        assert remaining == 5  # all original docs still present

    @pytest.mark.asyncio
    async def test_dry_run_does_not_reflag(self, sample_data):
        prices, tickers = sample_data
        sp = FakeCollection(prices)
        tt = FakeCollection(tickers)

        await _purge_malformed(sp, tt, dry_run=True)

        grtuf = await tt.find_one({"ticker": "GRTUF.US"})
        assert grtuf["needs_price_redownload"] is False
        assert grtuf["price_history_status"] == "complete"


class TestPurgeExecute:

    @pytest.mark.asyncio
    async def test_execute_deletes_malformed_docs(self, sample_data):
        prices, tickers = sample_data
        sp = FakeCollection(prices)
        tt = FakeCollection(tickers)

        result = await _purge_malformed(sp, tt, dry_run=False)

        assert result["dry_run"] is False
        assert result["deleted_count"] == 3
        remaining = await sp.count_documents({})
        assert remaining == 2  # only AAPL.US docs remain

    @pytest.mark.asyncio
    async def test_execute_reflags_only_affected_tickers(self, sample_data):
        prices, tickers = sample_data
        sp = FakeCollection(prices)
        tt = FakeCollection(tickers)

        result = await _purge_malformed(sp, tt, dry_run=False)

        # GRTUF.US and PFX.US reflagged
        assert result["reflagged_count"] == 2

        grtuf = await tt.find_one({"ticker": "GRTUF.US"})
        assert grtuf["needs_price_redownload"] is True
        assert grtuf["price_history_complete"] is False
        assert grtuf["price_history_status"] == "malformed_purged"
        assert grtuf["history_download_error"] == "malformed_docs_purged"

        pfx = await tt.find_one({"ticker": "PFX.US"})
        assert pfx["needs_price_redownload"] is True
        assert pfx["price_history_status"] == "malformed_purged"

        # AAPL.US NOT reflagged (clean ticker)
        aapl = await tt.find_one({"ticker": "AAPL.US"})
        assert aapl["needs_price_redownload"] is False
        assert aapl["price_history_status"] == "complete"

    @pytest.mark.asyncio
    async def test_execute_does_not_touch_clean_docs(self, sample_data):
        prices, tickers = sample_data
        sp = FakeCollection(prices)
        tt = FakeCollection(tickers)

        await _purge_malformed(sp, tt, dry_run=False)

        # AAPL.US valid docs survive
        aapl_docs = await sp.find({"ticker": "AAPL.US"}).to_list(None)
        assert len(aapl_docs) == 2
        for d in aapl_docs:
            assert d["close"] in (150.0, 151.0)


# ---------------------------------------------------------------------------
# Idempotency tests
# ---------------------------------------------------------------------------
class TestIdempotency:

    @pytest.mark.asyncio
    async def test_double_purge_is_idempotent(self, sample_data):
        prices, tickers = sample_data
        sp = FakeCollection(prices)
        tt = FakeCollection(tickers)

        r1 = await _purge_malformed(sp, tt, dry_run=False)
        assert r1["deleted_count"] == 3

        # Second purge: no malformed docs left → no-op
        r2 = await _purge_malformed(sp, tt, dry_run=False)
        assert r2["deleted_count"] == 0
        assert r2["reflagged_count"] == 0
        assert r2["totals"]["malformed_docs"] == 0
        assert r2["items"] == []

    @pytest.mark.asyncio
    async def test_detect_after_purge_returns_empty(self, sample_data):
        prices, tickers = sample_data
        sp = FakeCollection(prices)
        tt = FakeCollection(tickers)

        await _purge_malformed(sp, tt, dry_run=False)

        result = await _detect_malformed(sp, tt)
        assert result["items"] == []
        assert result["totals"]["malformed_docs"] == 0


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------
class TestEdgeCases:

    @pytest.mark.asyncio
    async def test_empty_collection(self):
        sp = FakeCollection([])
        tt = FakeCollection([])

        det = await _detect_malformed(sp, tt)
        assert det["items"] == []

        purge = await _purge_malformed(sp, tt, dry_run=False)
        assert purge["deleted_count"] == 0

    @pytest.mark.asyncio
    async def test_all_four_malformed_shapes(self):
        """All four malformed shapes are caught and purged."""
        docs = [
            _make_malformed_ticker_only("A.US"),    # no date, no close
            _make_malformed_no_date("B.US"),         # close=None, no date key
            _make_malformed_null_date("C.US"),        # date=None
            _make_malformed_null_close("D.US"),       # close=None
            _make_valid_doc("E.US"),                  # clean
        ]
        tts = [
            {"ticker": t, "is_visible": False, "needs_price_redownload": False,
             "price_history_complete": True, "price_history_status": "complete"}
            for t in ["A.US", "B.US", "C.US", "D.US", "E.US"]
        ]
        sp = FakeCollection(docs)
        tt = FakeCollection(tts)

        result = await _purge_malformed(sp, tt, dry_run=False)
        assert result["deleted_count"] == 4
        assert result["totals"]["affected_tickers"] == 4

        # E.US not reflagged
        e = await tt.find_one({"ticker": "E.US"})
        assert e["price_history_status"] == "complete"
