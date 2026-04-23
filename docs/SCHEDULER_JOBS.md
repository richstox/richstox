# RICHSTOX SCHEDULER JOBS — BINDING SPECIFICATION

> **IMMUTABLE**: Do not change schedule, endpoints, or conditions without Richard's explicit approval (kurtarichard@gmail.com).

## Job Schedule Overview

| # | Job Name | Day | Time (Prague) | EODHD Endpoint | API Calls | Condition |
|---|----------|-----|---------------|----------------|-----------|-----------|
| 0 | Market Calendar | Mon-Sat | 02:00 | `/exchange-details/US` | 1 | `not run today` |
| 1 | **Universe Seed** | Mon-Sat | 03:00 | `/exchange-symbol-list/US` | 1 | `not run today` |
| 2 | **Price Sync** | Mon-Sat | after Step 1 | `/eod-bulk-last-day/US` | 1 (bulk) | `universe_seed completed today` |
| 3 | **Fundamentals Sync** | Mon-Sat | after Step 2 | `/fundamentals/{TICKER}.US` | 0-50 | `price_sync completed today` |
| 4 | SP500TR Update | Mon-Sat | 04:15 | `/eod/SP500TR.INDX` | 1 | `not run today` |
| 5 | Backfill Gaps | Mon-Sat | 04:45 | `/eod/{TICKER}.US` | 0-50 | `tickers with gaps exist` |
| 6 | Upcoming Dividend Calendar | Mon-Sat | 04:50 | `/calendar/dividends?from=..&to=..` | 1 | `not run today` |
| 7 | Upcoming Earnings Calendar | Mon-Sat | 04:55 | `/calendar/earnings?from=..&to=..` | 1 | `not run today` |
| 8 | Upcoming Splits Calendar | Mon-Sat | 04:57 | `/calendar/splits?from=..&to=..` | 1 | `not run today` |
| 9 | Upcoming IPOs Calendar | Mon-Sat | 04:58 | `/calendar/ipos?from=..&to=..` | 1 | `not run today` |
| 10 | Backfill All | Mon-Sat | 05:00 | `/eod/{TICKER}.US` | 0-N | `tickers without full history` |
| 11 | Key Metrics | Mon-Sat | 05:00 | None (DB only) | 0 | `not run today` |
| 12 | PAIN Cache | Mon-Sat | 05:00 | None (DB only) | 0 | `not run today` |
| 13 | Peer Medians | Mon-Sat | 05:30 | None (DB only) | 0 | `not run today` |
| 14 | **Admin Report** | Mon-Sat | 06:00 | None (DB only) | 0 | `not run today` |
| 15 | **News Refresh** | Sun-Sat | 13:00 | `/news?s={TICKER}.US` | N unique tickers | `not run today` |

## Configuration Constants

```python
TIMEZONE = "Europe/Prague"
UNIVERSE_SEED_HOUR = 3
UNIVERSE_SEED_MINUTE = 0
UNIVERSE_SEED_DAY = 6  # Sunday (exclusion day — news-only)
PRICE_SYNC_HOUR = 4          # legacy; price_sync uses dependency chain
PRICE_SYNC_MINUTE = 0        # legacy; price_sync uses dependency chain
FUNDAMENTALS_SYNC_HOUR = 4   # legacy; fundamentals uses dependency chain
FUNDAMENTALS_SYNC_MINUTE = 30 # legacy; fundamentals uses dependency chain
BACKFILL_HOUR = 4
BACKFILL_MINUTE = 45
UPCOMING_DIVIDEND_CALENDAR_HOUR = 4
UPCOMING_DIVIDEND_CALENDAR_MINUTE = 50
UPCOMING_EARNINGS_CALENDAR_HOUR = 4
UPCOMING_EARNINGS_CALENDAR_MINUTE = 55
UPCOMING_SPLITS_CALENDAR_HOUR = 4
UPCOMING_SPLITS_CALENDAR_MINUTE = 57
UPCOMING_IPOS_CALENDAR_HOUR = 4
UPCOMING_IPOS_CALENDAR_MINUTE = 58
BACKFILL_ALL_HOUR = 5
BACKFILL_ALL_MINUTE = 0
NEWS_REFRESH_HOUR = 13
NEWS_REFRESH_MINUTE = 0
KEY_METRICS_HOUR = 5
KEY_METRICS_MINUTE = 0
PEER_MEDIANS_HOUR = 5
PEER_MEDIANS_MINUTE = 30
PAIN_CACHE_HOUR = 5
MARKET_CALENDAR_HOUR = 2
MARKET_CALENDAR_MINUTE = 0
ADMIN_REPORT_HOUR = 6
ADMIN_REPORT_MINUTE = 0
```

## Job Details

### 1. Universe Seed (Mon-Sat 03:00)
- **File**: `/app/backend/whitelist_service.py` → `sync_ticker_whitelist()`
- **Purpose**: Refresh tracked_tickers from EODHD exchange-symbol-list (NYSE/NASDAQ, Common Stock only)
- **API**: `GET https://eodhd.com/api/exchange-symbol-list/US`
- **Cost**: 1 API call/day (Mon-Sat)

### 2. News Refresh (Daily 13:00, including Sunday)
- **File**: `/app/backend/services/news_service.py` → `news_daily_refresh()` → `refresh_hot_tickers_news()`
- **Purpose**: Fetch 3 newest articles per ticker for ALL users' followed tickers (global union)
- **API**: `GET https://eodhd.com/api/news?s={TICKER}.US&limit=3`
- **Cost**: N API calls/day (N = unique tickers across all users)
- **Dedup**: `$setOnInsert` by article_id prevents duplicate storage

### 3. Price Sync (Mon-Sat, after Step 1 completion)
- **File**: `/app/backend/scheduler_service.py` → `run_daily_price_sync()`
- **Purpose**: Sync latest daily prices for all tracked tickers (bulk endpoint)
- **API**: `GET https://eodhd.com/api/eod-bulk-last-day/US`
- **Cost**: 1 API call/day

### 4. Benchmark Update (Mon-Sat 04:15) — Standalone
- **File**: `/app/backend/benchmark_service.py` → `update_all_benchmarks()`
- **Purpose**: Update benchmark index price history (SP500TR.INDX and future benchmarks)
- **API**: `GET https://eodhd.com/api/eod/{SYMBOL}` per benchmark
- **Cost**: 1 API call per benchmark/day (currently 1)
- **Design**: Completely independent of the bulk ticker pipeline. Not subject to
  universe seed, visibility rules, or ticker filters. Extensible via
  `BENCHMARK_SYMBOLS` registry in `benchmark_service.py`.
- **Admin**: Can be triggered manually via `POST /api/v1/admin/job/benchmark_update/run`

### 5. Fundamentals Sync (Mon-Sat, after Step 2 completion)
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

### 7. Upcoming Dividend Calendar (Mon-Sat 04:50)
- **File**: `/app/backend/dividend_history_service.py` → `sync_upcoming_dividend_calendar_for_visible_tickers()`
- **Purpose**: Fetch date-window upcoming ex-dividend events (today..+90d) and persist per ticker for UI next-dividend display
- **API**: `GET https://eodhd.com/api/calendar/dividends?from={YYYY-MM-DD}&to={YYYY-MM-DD}`
- **Cost**: 1 API call/day
- **Persistence**: `upcoming_dividends` collection with one document per visible ticker (upsert/null-safe)

### 8. Upcoming Earnings Calendar (Mon-Sat 04:55)
- **File**: `/app/backend/dividend_history_service.py` → `sync_upcoming_earnings_calendar_for_visible_tickers()`
- **Purpose**: Fetch date-window upcoming earnings report dates (today..+90d) and persist per visible ticker for UI display. **Independent of Step 2.6** (`_detect_earnings_candidates_eodhd` in `scheduler_service.py`), which solely flags tickers for fundamentals refresh.
- **API**: `GET https://eodhd.com/api/calendar/earnings?from={YYYY-MM-DD}&to={YYYY-MM-DD}`
- **Cost**: 1 API call/day
- **Persistence**: `upcoming_earnings` collection with one document per visible ticker (upsert/null-safe)
- **Window**: Europe/Prague date-only (not UTC)
- **Served by**: `GET /v1/ticker/{ticker}/earnings`

### 9. Upcoming Splits Calendar (Mon-Sat 04:57)
- **File**: `/app/backend/dividend_history_service.py` → `sync_upcoming_splits_calendar_for_visible_tickers()`
- **Purpose**: Fetch date-window upcoming stock split events (today..+90d) and persist per visible ticker for UI display.
- **API**: `GET https://eodhd.com/api/calendar/splits?from={YYYY-MM-DD}&to={YYYY-MM-DD}`
- **Cost**: 1 API call/day
- **Persistence**: `upcoming_splits` collection with one document per visible ticker (upsert/null-safe)
- **Window**: Europe/Prague date-only (not UTC)
- **Served by**: `GET /v1/ticker/{ticker}/splits`

### 10. Upcoming IPOs Calendar (Mon-Sat 04:58)
- **File**: `/app/backend/dividend_history_service.py` → `sync_upcoming_ipos_calendar_for_visible_tickers()`
- **Purpose**: Fetch date-window upcoming IPO events (today..+90d) and persist per visible ticker for UI display. IPO tickers must be pre-seeded into tracked_tickers with is_visible=True before the listing date.
- **API**: `GET https://eodhd.com/api/calendar/ipos?from={YYYY-MM-DD}&to={YYYY-MM-DD}`
- **Cost**: 1 API call/day
- **Persistence**: `upcoming_ipos` collection with one document per visible ticker (upsert/null-safe)
- **Window**: Europe/Prague date-only (not UTC)
- **Served by**: `GET /v1/ticker/{ticker}/ipo`

### 11. Backfill All (Mon-Sat 05:00)
- **File**: `/app/backend/parallel_batch_service.py` → `run_scheduled_backfill_all_prices()`
- **Purpose**: Full parallel price backfill for tickers without complete history
- **API**: `GET https://eodhd.com/api/eod/{TICKER}.US`
- **Cost**: 0-N API calls/day (0 after all tickers backfilled)
- **Safety**: Rate-limit backoff >30s, error rate >5%, max 4 hours runtime

### 12. Key Metrics (Mon-Sat 05:00)
- **File**: `/app/backend/scheduler_service.py`
- **Purpose**: Compute per-ticker metrics (52w high/low, etc.)
- **API**: None (DB-only computation)
- **Cost**: 0 API calls

### 13. Peer Medians (Mon-Sat 05:30)
- **File**: `/app/backend/scheduler_service.py`
- **Purpose**: Compute peer/sector median values
- **API**: None (DB-only computation)
- **Cost**: 0 API calls

### 14. PAIN Cache (Mon-Sat 05:00)
- **File**: `/app/backend/scheduler_service.py`
- **Purpose**: Refresh max drawdown cache
- **API**: None (DB-only computation)
- **Cost**: 0 API calls

## Rollback Plan for `dividend_upcoming_calendar`

There is no runtime ops_config flag that disables the scheduler from running this job. The admin "Run Now" button respects `ops_config.job_dividend_upcoming_calendar_enabled`, but that setting does **not** suppress the scheduled 04:50 run.

To disable the scheduled job:

1. **Code change (preferred)**: Comment out or remove the `should_run("dividend_upcoming_calendar", …)` block in `backend/scheduler.py` (~lines 1254–1278) and redeploy.
2. **Full revert**: `git revert <commit-sha>` for the two commits on this PR branch, then redeploy.
3. **UI hide (frontend only)**: The Next dividend card already renders the empty-state ("No upcoming dividend information available.") when `next_dividend.next_ex_date` is null — no code change needed if you only want to suppress the UI element.

The `upcoming_dividends` collection is additive and can be left in place or dropped with `db.upcoming_dividends.drop()` without affecting any other pipeline.

## Daily API Cost Estimate (Steady State)

| Job | Daily Calls |
|-----|-------------|
| Universe Seed | 1 (Mon-Sat) |
| Price Sync | 1 |
| SP500TR | 1 |
| Upcoming Dividends Calendar | 1 |
| Upcoming Earnings Calendar | 1 |
| Upcoming Splits Calendar | 1 |
| Upcoming IPOs Calendar | 1 |
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
