import asyncio
import os
import sys
from types import SimpleNamespace

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import price_ingestion_service


class _FakeOpsConfig:
    async def find_one(self, filt):
        _ = filt
        return None

    async def delete_one(self, filt):
        _ = filt
        return SimpleNamespace(deleted_count=0)


class _FakeTrackedTickers:
    def __init__(self, tickers):
        self._tickers = list(tickers)

    async def distinct(self, field, query):
        _ = field, query
        return list(self._tickers)


class _FakeStockPrices:
    def __init__(self):
        self.writes = []

    async def bulk_write(self, batch, ordered=False):
        _ = ordered
        self.writes.extend(batch)
        return SimpleNamespace(upserted_count=len(batch), modified_count=0)


class _FakeDB:
    def __init__(self, seeded_tickers):
        self.ops_config = _FakeOpsConfig()
        self.tracked_tickers = _FakeTrackedTickers(seeded_tickers)
        self.stock_prices = _FakeStockPrices()


def test_step2_bulk_match_normalizes_bulk_and_seeded_tickers(monkeypatch):
    db = _FakeDB(seeded_tickers=["AAPL.US", "MSFT.US"])
    bulk_payload = [
        {
            "code": "aapl",
            "date": "2026-03-18",
            "open": 100,
            "high": 101,
            "low": 99,
            "close": 100.5,
            "adjusted_close": 100.4,
            "volume": 1000,
        },
        {
            "code": "MSFT.US",
            "date": "2026-03-18",
            "open": 200,
            "high": 201,
            "low": 199,
            "close": 200.5,
            "adjusted_close": 200.4,
            "volume": 2000,
        },
    ]

    async def _fake_fetch_bulk(_exchange="US", include_meta=False, **kwargs):
        _ = _exchange
        if include_meta:
            return bulk_payload, True
        return bulk_payload

    monkeypatch.setattr(price_ingestion_service, "fetch_bulk_eod_latest", _fake_fetch_bulk)

    result = asyncio.run(price_ingestion_service.run_daily_bulk_catchup(db))

    assert result["matched_price_tickers_raw"] == 2
    assert result["tickers_with_price_data"] == 2
    assert result["rows_written"] > 0
    assert sorted(result["tickers_with_price"]) == ["AAPL.US", "MSFT.US"]
    assert result["ticker_samples"]["bulk_rows_sample"][:2] == ["aapl", "MSFT.US"]
    assert sorted(result["ticker_samples"]["bulk_rows_normalized_sample"]) == ["AAPL.US", "MSFT.US"]
