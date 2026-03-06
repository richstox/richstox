# HANDOVER.md

> Last updated: 2026-03-06 by Cursor Agent session (P0 Security + Progress Tracking)

## 1. What is done

### Security (P0 — completed)

- **AdminAuthMiddleware** (`backend/admin_middleware.py`): Global ASGI middleware protects ALL 85 `/api/admin/*` endpoints. No token → 401, non-admin → 403. Audit logging to `ops_security_log` with per-IP rate limiting (10/min).
- **UserAuthMiddleware** (`backend/auth_guard.py`): Protects `/api/portfolios`, `/api/positions`, `/api/v1/watchlist/*`, `/api/v1/positions/*`. Injects verified `user_id` into `request.state.user`. No client-supplied `user_id` accepted.
- **IDOR prevention**: All ownership checks use anti-enumeration pattern (404, not 403). Queries always scope to `{resource_id, user_id}`.
- **Dynamic bootstrap guard**: `POST /api/admin/auth/seed-admin` only works when zero admins exist in DB.
- **Watchlist multi-user**: Removed hardcoded `ADMIN_EMAIL`. All watchlist operations scoped to `user_id` from session. Unique compound index `{user_id: 1, ticker: 1}` created on production.
- **10 automated security tests**: `test_admin_auth_middleware.py` (4) + `test_user_auth_guard.py` (6).

### Progress tracking (completed)

- **`full_sync_service.py`**: Both `run_full_fundamentals_sync` and `run_full_price_history_sync` now insert a "running" sentinel into `ops_job_runs` at job start and update progress every 25 tickers with ETA.
- **`scheduler_service.py`**: Step 3 (`run_fundamentals_changes_sync`) progress update frequency increased to every 25 tickers. Added progress messages for post-sync phases (exclusion report, Step 4 auto-chain). Enqueue function optimized with `bulk_write` (~10x faster).
- **Frontend polling fix** (`pipeline.tsx`): Fixed stale closure bug — `fetchData()` now reliably refreshes overview every ~9s during running jobs.

### DB schema (`tracked_tickers` state machine flags)

These fields exist and are being set by the pipeline:

| Field | Set by | Meaning |
|-------|--------|---------|
| `has_price_data` | Step 2 | Ticker appeared in daily bulk prices |
| `has_classification` | Step 3 | `sector` AND `industry` are non-empty |
| `fundamentals_complete` | Full Sync | Full fundamentals downloaded |
| `fundamentals_updated_at` | Step 3 / Full Sync | Last fundamentals fetch timestamp |
| `price_history_complete` | Full Price Sync | Complete OHLCV history downloaded |
| `price_history_complete_as_of` | Full Price Sync | Latest date in price history |
| `needs_fundamentals_refresh` | Step 2 detectors | Dividend/earnings event flagged ticker |
| `needs_price_redownload` | Step 2 split detector | Stock split detected |
| `is_visible` | Step 4 | Passes all eligibility gates |
| `visibility_failed_reason` | Step 4 | Exact reason for exclusion |
| `financial_currency` | Step 3 | Extracted from fundamentals payload |

## 2. Where we are stuck

**Step 3 is the bottleneck.** Current state:

- Step 1 ✅ — 11,844 raw → 6,460 seeded
- Step 2 ✅ — 6,435 tickers with price data
- **Step 3 ⚠️ — Only 49/6,435 classified** (0.8%). The scheduled sync (`run_fundamentals_changes_sync`) processes tickers but in MOCK mode (no real EODHD API key in dev) all fail. In production with a real key, the **Full Fundamentals Sync** button must be used to download all 6,435 tickers.
- Step 4 ❌ — 0 visible (depends on Step 3 completing)
- Step 5 ❌ — Peer medians not computed (depends on Step 4)

The current sync logic does not correctly utilize the state machine flags for incremental refresh. UI progress was static before this session's fixes.

## 3. Immediate next phase (The Master Plan)

### Step 3 — Fundamentals Sync (two parts)

**3A — Initial/full backfill:**
- Download fundamentals for ALL tickers with `has_price_data == true` that do NOT yet have `fundamentals_complete == true`.
- Extract `sector` and `industry` from EODHD payload.
- Set `has_classification = true` ONLY IF both conditions hold: `sector != null && sector.trim() != ""` AND `industry != null && industry.trim() != ""`.
- Set `fundamentals_complete = true` and `fundamentals_updated_at` on success.

**3B — Event-driven refresh:**
- Step 2 detectors (dividends, earnings, splits) flag tickers with `needs_fundamentals_refresh = true`.
- Step 3B re-downloads fundamentals ONLY for flagged tickers.
- After refresh, clear the flag: `needs_fundamentals_refresh = false`.

### Step 4 — Visible Universe (eligibility gates)

Purely a database computation step. No API calls. Apply strict filters:

1. `is_delisted != true`
2. `shares_outstanding > 0`
3. `financial_currency` exists and is non-empty
4. `has_price_data == true`
5. `has_classification == true`

For each ticker:
- If ALL gates pass → `is_visible = true`, `visibility_failed_reason = null`
- If ANY gate fails → `is_visible = false`, `visibility_failed_reason = "<exact_reason>"`

### Cleanup policy (NO DATA DELETION)

Do NOT delete raw data for failed tickers. Simply mark them as ineligible (`is_visible = false`). Keep all data in `company_fundamentals_cache`, `financials_cache`, `stock_prices`, etc. for audit/debug purposes.

### Price backfill (post-Step 4)

Only for tickers that passed Step 4 (`is_visible = true`):
- Download complete historical OHLCV prices from IPO date to today.
- Set `price_history_complete = true` and `price_history_complete_as_of = <latest_date>`.
- Tickers flagged `needs_price_redownload = true` (splits) get full re-download.

### Tracking (no redundant downloads)

Strictly update completion flags and timestamps in `tracked_tickers` so we never re-download identical data:
- `fundamentals_complete` + `fundamentals_updated_at` — skip if already complete and not flagged for refresh
- `price_history_complete` + `price_history_complete_as_of` — skip if already complete and no split detected
- `needs_fundamentals_refresh` / `needs_price_redownload` — clear after successful re-download
