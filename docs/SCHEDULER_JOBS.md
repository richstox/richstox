# RICHSTOX SCHEDULER JOBS — BINDING SPECIFICATION

> **IMMUTABLE**: Do not change schedule, endpoints, or conditions without Richard's explicit approval (kurtarichard@gmail.com).

## Job Schedule Overview

| # | Job Name | Day | Time (Prague) | EODHD Endpoint | API Calls | Condition |
|---|----------|-----|---------------|----------------|-----------|-----------|
| 1 | **Universe Seed** | Sunday | 04:00 | `/exchange-symbol-list/US` | 1 | `is_sunday() && not run today` |
| 2 | **News Refresh** | Sun-Sat | 13:00 | `/news?s={TICKER}.US` | N unique tickers | `not run today` |
| 3 | Price Sync | Mon-Sat | 04:00 | `/eod-bulk-last-day/US` | 1 (bulk) | `is_daily_job_day() && not run today` |
| 4 | SP500TR Update | Mon-Sat | 04:15 | `/eod/SP500TR.INDX` | 1 | `is_daily_job_day() && not run today` |
| 5 | Fundamentals Sync | Mon-Sat | 04:30 | `/fundamentals/{TICKER}.US` | 0-50 | `pending events exist` |
| 6 | Backfill Gaps | Mon-Sat | 04:45 | `/eod/{TICKER}.US` | 0-50 | `tickers with gaps exist` |
| 7 | Backfill All | Mon-Sat | 05:00 | `/eod/{TICKER}.US` | 0-N | `tickers without full history` |
| 8 | Key Metrics | Mon-Sat | 05:00 | None (DB only) | 0 | `not run today` |
| 9 | Peer Medians | Mon-Sat | 05:30 | None (DB only) | 0 | `not run today` |
| 10 | PAIN Cache | Mon-Sat | 05:00 | None (DB only) | 0 | `not run today` |
| 11 | **Admin Report** | Mon-Sat | 06:00 | None (DB only) | 0 | `not run today` |

## Configuration Constants

```python
TIMEZONE = "Europe/Prague"
UNIVERSE_SEED_HOUR = 4
UNIVERSE_SEED_MINUTE = 0
UNIVERSE_SEED_DAY = 6  # Sunday
PRICE_SYNC_HOUR = 4
PRICE_SYNC_MINUTE = 0
FUNDAMENTALS_SYNC_HOUR = 4
FUNDAMENTALS_SYNC_MINUTE = 30
BACKFILL_HOUR = 4
BACKFILL_MINUTE = 45
BACKFILL_ALL_HOUR = 5
BACKFILL_ALL_MINUTE = 0
NEWS_REFRESH_HOUR = 13
NEWS_REFRESH_MINUTE = 0
KEY_METRICS_HOUR = 5
KEY_METRICS_MINUTE = 0
PEER_MEDIANS_HOUR = 5
PEER_MEDIANS_MINUTE = 30
PAIN_CACHE_HOUR = 5
ADMIN_REPORT_HOUR = 6
ADMIN_REPORT_MINUTE = 0
```

## Job Details

### 1. Universe Seed (Sunday 04:00)
- **File**: `/app/backend/whitelist_service.py` → `sync_ticker_whitelist()`
- **Purpose**: Refresh tracked_tickers from EODHD exchange-symbol-list (NYSE/NASDAQ, Common Stock only)
- **API**: `GET https://eodhd.com/api/exchange-symbol-list/US`
- **Cost**: 1 API call/week

### 2. News Refresh (Daily 13:00, including Sunday)
- **File**: `/app/backend/services/news_service.py` → `news_daily_refresh()` → `refresh_hot_tickers_news()`
- **Purpose**: Fetch 3 newest articles per ticker for ALL users' followed tickers (global union)
- **API**: `GET https://eodhd.com/api/news?s={TICKER}.US&limit=3`
- **Cost**: N API calls/day (N = unique tickers across all users)
- **Dedup**: `$setOnInsert` by article_id prevents duplicate storage

### 3. Price Sync (Mon-Sat 04:00)
- **File**: `/app/backend/scheduler_service.py` → `run_daily_price_sync()`
- **Purpose**: Sync latest daily prices for all tracked tickers (bulk endpoint)
- **API**: `GET https://eodhd.com/api/eod-bulk-last-day/US`
- **Cost**: 1 API call/day

### 4. SP500TR Update (Mon-Sat 04:15)
- **File**: `/app/backend/scheduler_service.py`
- **Purpose**: Update S&P 500 Total Return benchmark index
- **API**: `GET https://eodhd.com/api/eod/SP500TR.INDX`
- **Cost**: 1 API call/day

### 5. Fundamentals Sync (Mon-Sat 04:30)
- **File**: `/app/backend/scheduler_service.py` → `run_fundamentals_changes_sync()`
- **Purpose**: Sync company fundamentals for tickers with pending events
- **API**: `GET https://eodhd.com/api/fundamentals/{TICKER}.US`
- **Cost**: 0-50 API calls/day (event-driven)
- **Condition**: Only runs if `fundamentals_events.status = "pending"` exists

### 6. Backfill Gaps (Mon-Sat 04:45)
- **File**: `/app/backend/scheduler_service.py` → `run_price_backfill_gaps()`
- **Purpose**: Fill missing price data gaps
- **API**: `GET https://eodhd.com/api/eod/{TICKER}.US`
- **Cost**: 0-50 API calls/day
- **Condition**: Only runs if tickers have detected price gaps

### 7. Backfill All (Mon-Sat 05:00)
- **File**: `/app/backend/parallel_batch_service.py` → `run_scheduled_backfill_all_prices()`
- **Purpose**: Full parallel price backfill for tickers without complete history
- **API**: `GET https://eodhd.com/api/eod/{TICKER}.US`
- **Cost**: 0-N API calls/day (0 after all tickers backfilled)
- **Safety**: Rate-limit backoff >30s, error rate >5%, max 4 hours runtime

### 8. Key Metrics (Mon-Sat 05:00)
- **File**: `/app/backend/scheduler_service.py`
- **Purpose**: Compute per-ticker metrics (52w high/low, etc.)
- **API**: None (DB-only computation)
- **Cost**: 0 API calls

### 9. Peer Medians (Mon-Sat 05:30)
- **File**: `/app/backend/scheduler_service.py`
- **Purpose**: Compute peer/sector median values
- **API**: None (DB-only computation)
- **Cost**: 0 API calls

### 10. PAIN Cache (Mon-Sat 05:00)
- **File**: `/app/backend/scheduler_service.py`
- **Purpose**: Refresh max drawdown cache
- **API**: None (DB-only computation)
- **Cost**: 0 API calls

## Daily API Cost Estimate (Steady State)

| Job | Daily Calls |
|-----|-------------|
| Universe Seed | 0 (Sunday only: 1/week) |
| Price Sync | 1 |
| SP500TR | 1 |
| Fundamentals | ~0 (event-driven) |
| Backfill Gaps | ~0 (after setup) |
| Backfill All | ~0 (after setup) |
| News | ~50-100 (depends on unique tickers) |
| **TOTAL** | **~55-105 calls/day** |

## Audit

Run `/app/scripts/audit_scheduler.py` to verify scheduler.py matches this spec.

---

**Last Updated**: 2026-02-23
**Approved By**: Richard (kurtarichard@gmail.com)
