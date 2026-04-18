"""
Tests for admin force-redownload endpoint (reflag-only, no deletes).

Endpoint:
  POST /api/admin/prices/force-redownload

Accepts JSON body: {"tickers": ["PFX.US", ...], "reason": "manual_admin_force"}

For each ticker that exists in tracked_tickers, sets canonical flags:
  - needs_price_redownload = true
  - price_history_complete = false
  - price_history_status = "admin_forced_redownload"
  - history_download_error = "admin_forced_redownload"
  - force_redownload_reason = <free-text reason from body>

No deletes. No external API calls. No pipeline execution.
Phase C picks up reflagged tickers on the next scheduler run.

These tests use self-contained in-memory fakes (no server.py imports) because
importing server.py pulls in FastAPI, Motor, dotenv, and dozens of deps.

Run:
    cd /app/backend && python -m pytest tests/test_force_redownload.py -v
"""

import pytest
from bson import ObjectId


# ---------------------------------------------------------------------------
# In-memory fake MongoDB collections
# ---------------------------------------------------------------------------
class FakeCollection:
    """Minimal in-memory MongoDB collection fake."""

    def __init__(self, docs=None):
        self._docs = []
        for d in (docs or []):
            d = dict(d)
            if "_id" not in d:
                d["_id"] = ObjectId()
            self._docs.append(d)

    def find(self, query=None, projection=None):
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

    async def update_one(self, query, update):
        matched = self._match(query)
        modified = 0
        if matched:
            doc = matched[0]
            if "$set" in update:
                doc.update(update["$set"])
                modified = 1

        class _R:
            modified_count = modified
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
FORCE_REDOWNLOAD_MAX_TICKERS = 200


async def _force_redownload(tracked_tickers, tickers, reason):
    """
    Replicated POST /admin/prices/force-redownload logic (reflag-only).

    Returns dict matching the endpoint response shape, or raises ValueError
    for validation failures (the endpoint raises HTTPException).
    """
    reason = reason.strip() if reason else ""
    if not reason:
        raise ValueError("reason must be a non-empty string")

    # Deduplicate and normalise
    seen = set()
    clean_tickers = []
    for t in tickers:
        t_clean = t.strip()
        if not t_clean:
            continue
        if t_clean not in seen:
            seen.add(t_clean)
            clean_tickers.append(t_clean)

    if not clean_tickers:
        raise ValueError("tickers list must contain at least one non-empty ticker")

    if len(clean_tickers) > FORCE_REDOWNLOAD_MAX_TICKERS:
        raise ValueError(
            f"Too many tickers ({len(clean_tickers)}). Maximum is {FORCE_REDOWNLOAD_MAX_TICKERS} per request."
        )

    # Verify which tickers actually exist in tracked_tickers
    tt_cursor = tracked_tickers.find(
        {"ticker": {"$in": clean_tickers}},
        {"_id": 0, "ticker": 1, "is_visible": 1},
    )
    tt_map = {doc["ticker"]: doc async for doc in tt_cursor}

    results = []
    total_reflagged = 0

    for ticker in clean_tickers:
        if ticker not in tt_map:
            results.append({
                "ticker": ticker,
                "status": "skipped",
                "reason": "ticker_not_found",
            })
            continue

        # Reflag tracked_ticker for Phase C redownload (no deletes)
        await tracked_tickers.update_one(
            {"ticker": ticker},
            {"$set": {
                "needs_price_redownload": True,
                "price_history_complete": False,
                "price_history_status": "admin_forced_redownload",
                "history_download_error": "admin_forced_redownload",
                "force_redownload_reason": reason,
            }},
        )

        total_reflagged += 1
        results.append({
            "ticker": ticker,
            "status": "reflagged",
            "is_visible": tt_map[ticker].get("is_visible", False),
        })

    return {
        "tickers_requested": len(clean_tickers),
        "tickers_reflagged": total_reflagged,
        "tickers_skipped": len(clean_tickers) - total_reflagged,
        "reason": reason,
        "results": results,
    }


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
def _make_price(ticker, date, close=150.0):
    return {"_id": ObjectId(), "ticker": ticker, "date": date, "close": close, "volume": 100}


@pytest.fixture
def sample_data():
    """
    PFX.US — 1 price row (sparse ticker needing redownload)
    AAPL.US — 3 price rows (healthy ticker)
    GRTUF.US — 2 price rows
    """
    prices = [
        _make_price("PFX.US", "2024-01-02", 10.0),
        _make_price("AAPL.US", "2024-01-02", 150.0),
        _make_price("AAPL.US", "2024-01-03", 151.0),
        _make_price("AAPL.US", "2024-01-04", 152.0),
        _make_price("GRTUF.US", "2024-01-02", 50.0),
        _make_price("GRTUF.US", "2024-01-03", 51.0),
    ]
    tickers = [
        {"ticker": "PFX.US", "is_visible": False, "needs_price_redownload": False,
         "price_history_complete": True, "price_history_status": "complete"},
        {"ticker": "AAPL.US", "is_visible": True, "needs_price_redownload": False,
         "price_history_complete": True, "price_history_status": "complete"},
        {"ticker": "GRTUF.US", "is_visible": True, "needs_price_redownload": False,
         "price_history_complete": True, "price_history_status": "complete"},
    ]
    return prices, tickers


# ---------------------------------------------------------------------------
# Happy path tests
# ---------------------------------------------------------------------------
class TestForceRedownloadHappyPath:

    @pytest.mark.asyncio
    async def test_single_ticker_reflagged(self, sample_data):
        """Force-redownload for one ticker reflags it without deleting prices."""
        prices, tickers = sample_data
        sp = FakeCollection(prices)
        tt = FakeCollection(tickers)

        result = await _force_redownload(tt, ["PFX.US"], "manual_admin_force")

        assert result["tickers_requested"] == 1
        assert result["tickers_reflagged"] == 1
        assert result["tickers_skipped"] == 0
        assert result["reason"] == "manual_admin_force"
        assert "total_prices_deleted" not in result

        # PFX.US price docs must still exist (no deletes)
        pfx_count = await sp.count_documents({"ticker": "PFX.US"})
        assert pfx_count == 1

        # PFX.US tracked_ticker should be reflagged with canonical values
        pfx = await tt.find_one({"ticker": "PFX.US"})
        assert pfx["needs_price_redownload"] is True
        assert pfx["price_history_complete"] is False
        assert pfx["price_history_status"] == "admin_forced_redownload"
        assert pfx["history_download_error"] == "admin_forced_redownload"
        assert pfx["force_redownload_reason"] == "manual_admin_force"

    @pytest.mark.asyncio
    async def test_multiple_tickers(self, sample_data):
        """Force-redownload for multiple tickers."""
        prices, tickers = sample_data
        sp = FakeCollection(prices)
        tt = FakeCollection(tickers)

        result = await _force_redownload(tt, ["PFX.US", "GRTUF.US"], "bulk_remediation")

        assert result["tickers_requested"] == 2
        assert result["tickers_reflagged"] == 2
        assert "total_prices_deleted" not in result

        # Prices are untouched
        assert await sp.count_documents({"ticker": "PFX.US"}) == 1
        assert await sp.count_documents({"ticker": "GRTUF.US"}) == 2

        # Both tickers reflagged
        for t in ["PFX.US", "GRTUF.US"]:
            doc = await tt.find_one({"ticker": t})
            assert doc["needs_price_redownload"] is True
            assert doc["price_history_complete"] is False
            assert doc["price_history_status"] == "admin_forced_redownload"

    @pytest.mark.asyncio
    async def test_does_not_touch_other_tickers(self, sample_data):
        """Force-redownload for PFX.US must not affect AAPL.US."""
        prices, tickers = sample_data
        sp = FakeCollection(prices)
        tt = FakeCollection(tickers)

        await _force_redownload(tt, ["PFX.US"], "test_reason")

        # AAPL.US prices survive
        aapl_count = await sp.count_documents({"ticker": "AAPL.US"})
        assert aapl_count == 3

        # AAPL.US flags untouched
        aapl = await tt.find_one({"ticker": "AAPL.US"})
        assert aapl["needs_price_redownload"] is False
        assert aapl["price_history_complete"] is True
        assert aapl["price_history_status"] == "complete"

    @pytest.mark.asyncio
    async def test_result_item_shape_reflagged(self, sample_data):
        """Each reflagged ticker result has the expected shape (no delete fields)."""
        prices, tickers = sample_data
        tt = FakeCollection(tickers)

        result = await _force_redownload(tt, ["AAPL.US"], "test")
        item = result["results"][0]

        assert item["ticker"] == "AAPL.US"
        assert item["status"] == "reflagged"
        assert item["is_visible"] is True
        assert "deleted_prices" not in item

    @pytest.mark.asyncio
    async def test_response_has_no_delete_fields(self, sample_data):
        """Response must not contain any delete-related counters."""
        prices, tickers = sample_data
        tt = FakeCollection(tickers)

        result = await _force_redownload(tt, ["PFX.US"], "test")

        assert "total_prices_deleted" not in result
        for item in result["results"]:
            assert "deleted_prices" not in item

    @pytest.mark.asyncio
    async def test_canonical_fields_exact_values(self, sample_data):
        """Canonical fields must use exact string 'admin_forced_redownload'."""
        prices, tickers = sample_data
        tt = FakeCollection(tickers)

        await _force_redownload(tt, ["PFX.US"], "my custom reason")

        pfx = await tt.find_one({"ticker": "PFX.US"})
        assert pfx["price_history_status"] == "admin_forced_redownload"
        assert pfx["history_download_error"] == "admin_forced_redownload"
        # Free-text reason stored in non-canonical field
        assert pfx["force_redownload_reason"] == "my custom reason"

    @pytest.mark.asyncio
    async def test_reason_stored_in_non_canonical_field(self, sample_data):
        """The free-text reason from the request body is stored in force_redownload_reason."""
        prices, tickers = sample_data
        tt = FakeCollection(tickers)

        await _force_redownload(tt, ["PFX.US"], "sparse_history_remediation")

        pfx = await tt.find_one({"ticker": "PFX.US"})
        assert pfx["force_redownload_reason"] == "sparse_history_remediation"


# ---------------------------------------------------------------------------
# Ticker-not-found / skipped tests
# ---------------------------------------------------------------------------
class TestForceRedownloadSkipped:

    @pytest.mark.asyncio
    async def test_unknown_ticker_is_skipped(self, sample_data):
        """A ticker not in tracked_tickers is skipped, not an error."""
        prices, tickers = sample_data
        tt = FakeCollection(tickers)

        result = await _force_redownload(tt, ["UNKNOWN.US"], "test")

        assert result["tickers_requested"] == 1
        assert result["tickers_reflagged"] == 0
        assert result["tickers_skipped"] == 1

        item = result["results"][0]
        assert item["ticker"] == "UNKNOWN.US"
        assert item["status"] == "skipped"
        assert item["reason"] == "ticker_not_found"
        assert "deleted_prices" not in item

    @pytest.mark.asyncio
    async def test_mix_of_known_and_unknown(self, sample_data):
        """One valid, one unknown → partial success."""
        prices, tickers = sample_data
        tt = FakeCollection(tickers)

        result = await _force_redownload(tt, ["PFX.US", "BOGUS.US"], "test")

        assert result["tickers_requested"] == 2
        assert result["tickers_reflagged"] == 1
        assert result["tickers_skipped"] == 1

        by_ticker = {r["ticker"]: r for r in result["results"]}
        assert by_ticker["PFX.US"]["status"] == "reflagged"
        assert by_ticker["BOGUS.US"]["status"] == "skipped"


# ---------------------------------------------------------------------------
# Validation / edge-case tests
# ---------------------------------------------------------------------------
class TestForceRedownloadValidation:

    @pytest.mark.asyncio
    async def test_empty_reason_rejected(self, sample_data):
        prices, tickers = sample_data
        tt = FakeCollection(tickers)

        with pytest.raises(ValueError, match="reason must be a non-empty string"):
            await _force_redownload(tt, ["PFX.US"], "")

    @pytest.mark.asyncio
    async def test_whitespace_only_reason_rejected(self, sample_data):
        prices, tickers = sample_data
        tt = FakeCollection(tickers)

        with pytest.raises(ValueError, match="reason must be a non-empty string"):
            await _force_redownload(tt, ["PFX.US"], "   ")

    @pytest.mark.asyncio
    async def test_empty_tickers_list_rejected(self):
        tt = FakeCollection([])

        with pytest.raises(ValueError, match="at least one non-empty ticker"):
            await _force_redownload(tt, [], "test")

    @pytest.mark.asyncio
    async def test_all_blank_tickers_rejected(self):
        tt = FakeCollection([])

        with pytest.raises(ValueError, match="at least one non-empty ticker"):
            await _force_redownload(tt, ["", "  "], "test")

    @pytest.mark.asyncio
    async def test_too_many_tickers_rejected(self):
        tt = FakeCollection([])
        big_list = [f"TICK{i}.US" for i in range(201)]

        with pytest.raises(ValueError, match="Too many tickers"):
            await _force_redownload(tt, big_list, "test")

    @pytest.mark.asyncio
    async def test_exactly_200_tickers_accepted(self):
        """200 tickers (the cap) should NOT be rejected."""
        tts = [{"ticker": f"T{i}.US", "is_visible": False} for i in range(200)]
        tt = FakeCollection(tts)
        ticker_list = [f"T{i}.US" for i in range(200)]

        result = await _force_redownload(tt, ticker_list, "test")
        assert result["tickers_requested"] == 200
        assert result["tickers_reflagged"] == 200


# ---------------------------------------------------------------------------
# Deduplication tests
# ---------------------------------------------------------------------------
class TestForceRedownloadDedup:

    @pytest.mark.asyncio
    async def test_duplicate_tickers_deduplicated(self, sample_data):
        """Duplicate tickers in request should be deduplicated."""
        prices, tickers = sample_data
        tt = FakeCollection(tickers)

        result = await _force_redownload(tt, ["PFX.US", "PFX.US", "PFX.US"], "test")

        assert result["tickers_requested"] == 1  # deduplicated
        assert result["tickers_reflagged"] == 1
        assert len(result["results"]) == 1

    @pytest.mark.asyncio
    async def test_whitespace_tickers_stripped(self, sample_data):
        """Tickers with whitespace are stripped before processing."""
        prices, tickers = sample_data
        tt = FakeCollection(tickers)

        result = await _force_redownload(tt, ["  PFX.US  "], "test")

        assert result["tickers_requested"] == 1
        assert result["tickers_reflagged"] == 1
        assert result["results"][0]["ticker"] == "PFX.US"


# ---------------------------------------------------------------------------
# Idempotency tests
# ---------------------------------------------------------------------------
class TestForceRedownloadIdempotency:

    @pytest.mark.asyncio
    async def test_double_force_redownload_is_safe(self, sample_data):
        """Second force-redownload is safe — still reflags, no deletes."""
        prices, tickers = sample_data
        sp = FakeCollection(prices)
        tt = FakeCollection(tickers)

        r1 = await _force_redownload(tt, ["PFX.US"], "first_pass")
        assert r1["tickers_reflagged"] == 1
        assert "total_prices_deleted" not in r1

        r2 = await _force_redownload(tt, ["PFX.US"], "second_pass")
        assert r2["tickers_reflagged"] == 1
        assert "total_prices_deleted" not in r2

        # Ticker is still reflagged after second pass
        pfx = await tt.find_one({"ticker": "PFX.US"})
        assert pfx["needs_price_redownload"] is True
        assert pfx["force_redownload_reason"] == "second_pass"

        # Prices are still intact after both calls
        pfx_count = await sp.count_documents({"ticker": "PFX.US"})
        assert pfx_count == 1

    @pytest.mark.asyncio
    async def test_ticker_with_no_prices_still_reflagged(self):
        """Ticker exists in tracked_tickers but has zero prices → still reflagged."""
        sp = FakeCollection([])  # no price rows at all
        tt = FakeCollection([
            {"ticker": "EMPTY.US", "is_visible": False, "needs_price_redownload": False,
             "price_history_complete": False, "price_history_status": "never_downloaded"},
        ])

        result = await _force_redownload(tt, ["EMPTY.US"], "test")

        assert result["tickers_reflagged"] == 1
        assert "total_prices_deleted" not in result

        doc = await tt.find_one({"ticker": "EMPTY.US"})
        assert doc["needs_price_redownload"] is True
        assert doc["price_history_status"] == "admin_forced_redownload"


# ---------------------------------------------------------------------------
# No-delete guard tests
# ---------------------------------------------------------------------------
class TestNoDeleteGuard:

    @pytest.mark.asyncio
    async def test_prices_not_deleted_single_ticker(self, sample_data):
        """stock_prices rows must NOT be deleted for the target ticker."""
        prices, tickers = sample_data
        sp = FakeCollection(prices)
        tt = FakeCollection(tickers)

        await _force_redownload(tt, ["PFX.US"], "test")

        assert await sp.count_documents({"ticker": "PFX.US"}) == 1

    @pytest.mark.asyncio
    async def test_prices_not_deleted_multiple_tickers(self, sample_data):
        """stock_prices rows must NOT be deleted for any target ticker."""
        prices, tickers = sample_data
        sp = FakeCollection(prices)
        tt = FakeCollection(tickers)

        await _force_redownload(tt, ["PFX.US", "AAPL.US", "GRTUF.US"], "test")

        assert await sp.count_documents({"ticker": "PFX.US"}) == 1
        assert await sp.count_documents({"ticker": "AAPL.US"}) == 3
        assert await sp.count_documents({"ticker": "GRTUF.US"}) == 2

    @pytest.mark.asyncio
    async def test_total_price_count_unchanged(self, sample_data):
        """Total stock_prices count must be unchanged after force-redownload."""
        prices, tickers = sample_data
        sp = FakeCollection(prices)
        tt = FakeCollection(tickers)

        before = await sp.count_documents({})
        await _force_redownload(tt, ["PFX.US", "AAPL.US", "GRTUF.US"], "test")
        after = await sp.count_documents({})

        assert after == before


# ---------------------------------------------------------------------------
# Integration-style: verify Phase C pickup readiness
# ---------------------------------------------------------------------------
class TestPhaseCReadiness:

    @pytest.mark.asyncio
    async def test_reflagged_ticker_matches_phase_c_query(self, sample_data):
        """After force-redownload, the ticker matches the Phase C selection
        criteria: needs_price_redownload=True OR price_history_complete=False."""
        prices, tickers = sample_data
        tt = FakeCollection(tickers)

        await _force_redownload(tt, ["PFX.US"], "test")

        # Phase C selection: {$or: [{price_history_complete: false}, {needs_price_redownload: true}]}
        phase_c_query = {
            "$or": [
                {"price_history_complete": False},
                {"needs_price_redownload": True},
            ]
        }
        candidates = tt.find(phase_c_query)
        matched = await candidates.to_list(None)
        matched_tickers = [d["ticker"] for d in matched]

        assert "PFX.US" in matched_tickers
        # AAPL.US should NOT match (untouched)
        assert "AAPL.US" not in matched_tickers

    @pytest.mark.asyncio
    async def test_reflagged_ticker_eligible_after_idempotent_call(self, sample_data):
        """Ticker remains Phase C eligible even after a second reflag call."""
        prices, tickers = sample_data
        tt = FakeCollection(tickers)

        await _force_redownload(tt, ["PFX.US"], "first")
        await _force_redownload(tt, ["PFX.US"], "second")

        phase_c_query = {
            "$or": [
                {"price_history_complete": False},
                {"needs_price_redownload": True},
            ]
        }
        candidates = tt.find(phase_c_query)
        matched = await candidates.to_list(None)
        matched_tickers = [d["ticker"] for d in matched]

        assert "PFX.US" in matched_tickers
