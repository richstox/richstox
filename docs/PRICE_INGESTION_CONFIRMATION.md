# Price Ingestion - Confirmation Report

## 1) EODHD API Call — Exact URL ✅

**Confirmed URL format:**
```
GET https://eodhd.com/api/eod/{ticker}?api_token={API_KEY}&fmt=json
```

**NO date range parameters** - Returns full history from IPO to present.

**Example log for MCD.US:**
```
INFO:httpx:HTTP Request: GET https://eodhd.com/api/eod/MCD.US?api_token=699228759500b9.79838098&fmt=json "HTTP/1.1 200 OK"
```

**Code reference** (`price_ingestion_service.py:38-67`):
```python
async def fetch_eod_history(ticker: str, from_date: str = None, to_date: str = None):
    url = f"{EODHD_BASE_URL}/eod/{ticker_full}"
    params = {
        "api_token": EODHD_API_KEY,
        "fmt": "json",
    }
    # NO from_date or to_date added by default
```

---

## 2) Data Storage — Upsert Logic ✅

**Unique constraint:** `(ticker, date)` - deduplicates on date

**Index created:**
```python
await db.stock_prices.create_index([("ticker", 1), ("date", 1)], unique=True)
```

**Columns stored:**
| Column | Source |
|--------|--------|
| ticker | `{CODE}.US` |
| date | `record.date` (YYYY-MM-DD) |
| open | `record.open` |
| high | `record.high` |
| low | `record.low` |
| close | `record.close` |
| adjusted_close | `record.adjusted_close` |
| volume | `record.volume` |

**No filtering:** All rows returned by EODHD are stored, including $0 prices.

**Code reference** (`price_ingestion_service.py:169-184`):
```python
for record in eod_data:
    parsed = parse_eod_record(ticker_full, record)
    if not parsed["date"]:
        continue
    await db.stock_prices.update_one(
        {"ticker": ticker_full, "date": parsed["date"]},
        {"$set": parsed},
        upsert=True
    )
```

---

## 3) Active Tickers Scope — Definition ✅

**Query for "active" tickers:**
```javascript
db.tracked_tickers.find({ "status": "active" })
```

**A ticker becomes "active" when:**
1. It exists in `tracked_tickers` with `status: "active"`
2. This happens after fundamentals are synced (data exists in `company_fundamentals_cache`)

**Breakdown (from `/api/whitelist/stats`):**

| Status | Count | Description |
|--------|-------|-------------|
| total | 6,542 | All whitelisted tickers |
| active | ~2,400 | Have fundamentals synced |
| pending_fundamentals | ~4,100 | Awaiting fundamentals sync |
| no_fundamentals | 0 | No longer used |

**For price backfill:**
```python
# Query: Get tickers that have fundamentals but no prices
all_tickers = await db.company_fundamentals_cache.distinct("ticker")
tickers_with_prices = await db.stock_prices.distinct("ticker")
missing_prices = [t for t in all_tickers if t not in tickers_with_prices_set]
```

---

## 4) 52W High/Low — Computed Correctly ✅ (FIXED)

**Computation logic:**
```python
52w_high = MAX(adjusted_close) FROM stock_prices 
           WHERE ticker = X 
           ORDER BY date DESC LIMIT 252

52w_low = MIN(adjusted_close) FROM stock_prices 
          WHERE ticker = X 
          ORDER BY date DESC LIMIT 252
```

**Uses `adjusted_close` (or `close` as fallback), NOT intraday `high`/`low`.**

**Code reference** (`price_ingestion_service.py:333-400`):
```python
async def compute_52w_high_low(db, ticker: str):
    """
    IMPORTANT: Uses close_price (or adjusted_close), NOT intraday high/low.
    """
    cursor = db.stock_prices.find(
        {"ticker": ticker_full},
        {"_id": 0, "date": 1, "close": 1, "adjusted_close": 1}
    ).sort("date", -1).limit(252)
    
    for p in prices:
        close = p.get("adjusted_close") or p.get("close")  # Prefer adjusted
        if max_close is None or close > max_close:
            max_close = close
        if min_close is None or close < min_close:
            min_close = close
```

---

## 5) Backfill Progress & ETA

**Current progress:**
- Records: ~95,000+ rows
- Unique tickers: ~25-30 (batch still running)

**Scope:**
- Total active tickers: ~2,400
- Average rows per ticker: ~8,000 (varies by IPO date)
- Total expected: ~19.2M rows

**Rate (observed):**
- ~2-3 tickers/minute (with 0.3s delay between calls)
- Each ticker = 1 EODHD API call + ~5-10 sec DB upserts

**ETA to completion:**
- 2,400 tickers ÷ 3 tickers/min = ~800 minutes = **~13 hours**
- Can be parallelized with multiple batch jobs

---

## 6) Daily Cron Setup

**Endpoint:** `POST /api/admin/prices/sync-daily`

**Implementation:**
- Uses EODHD bulk endpoint: `GET /api/eod-bulk-last-day/US`
- Single API call covers entire exchange
- Only updates tickers in `company_fundamentals_cache` (active tickers)

**NOT YET SCHEDULED** - Requires cron job setup:
```bash
# Recommended crontab entry:
# Run at 22:00 CET (16:00 ET) - after US market close
0 22 * * 1-5 curl -X POST https://app.domain/api/admin/prices/sync-daily
```

**Logging:** Each run will be logged to `ops_job_runs`:
```python
# To be added:
await db.ops_job_runs.insert_one({
    "job_type": "daily_price_sync",
    "started_at": now,
    "finished_at": finished,
    "status": "completed",
    "result": {"tickers_updated": count, "api_calls": 1}
})
```

---

## Summary

| Item | Status |
|------|--------|
| EODHD URL (no date params) | ✅ Confirmed |
| Upsert logic (ticker+date unique) | ✅ Confirmed |
| Active tickers definition | ✅ Defined |
| 52W High/Low (using close price) | ✅ Fixed |
| Backfill progress | 🔄 Running (~25 tickers done) |
| Daily cron | ⏳ Endpoint ready, cron not scheduled |

---

**Next steps after your approval:**
1. Scale batch to all 2,400 active tickers
2. Set up daily cron for sync-daily
3. Move to P1: Price Chart + Range Selector UI
