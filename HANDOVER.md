# HANDOVER.md

> Last updated: 2026-03-07 by Cursor Agent session (Fundamentals Sync Progress + Audit)

---

## 0. CRITICAL: Uncommitted Work — Do First

**`backend/server.py` has ONE uncommitted change that is staged and ready.**

The change is an extension to `GET /api/admin/ticker/{ticker}/fundamentals-audit`:
- Widened `company_fundamentals_cache` projection to include: `ticker, code, name, exchange, currency_code, country_iso, website, logo_url, description, sector, industry, full_time_employees, ipo_date, fiscal_year_end, is_delisted`
- Added new `cache_snapshot` block in the response (description truncated to 200 chars preview + length)
- Added `missing[]` checks for required cache fields: `name, sector, industry, website, description` (logo_url reported but marked optional)

**Instruction for new agent:**
1. `git diff backend/server.py` to confirm the change looks right
2. `git add backend/server.py && git commit -m "feat(audit): cache_snapshot in fundamentals-audit endpoint" && git push`
3. Then continue with the next tasks below.

---

## 1. Current Branch & Git State

```
branch:   cursor/fundamentals-sync-progress-aee1
remote:   origin/cursor/fundamentals-sync-progress-aee1 (up to date except server.py above)
```

Recent commits (newest first):
```
89fab6b feat: POST /api/admin/ticker/{ticker}/run-fundamentals-sync
dd9c20a feat: shares_outstanding+financial_currency persist + fundamentals-audit endpoint
e9371aa fix(parser): strip sector/industry at source + has_classification in all paths + PARSER DEBUG log
f646ab6 fix(pipeline): STOP is instant + clock-skew fix + progress poll for both Step 3 jobs
c8343e0 fix(step3): reset error/zombie/stale→pending at run init + expose zombies_reclaimed
f19b136 fix(step3): global cancel_event soft stop for fundamentals sync
5e18927 fix(pipeline): instant running state + sub-step last-run timestamps
b92f7f6 fix: Step 2.1 — remove 30-day gap loop, single latest-day fetch with soft stop
66db1ae fix: run_id architecture for fundamentals progress tracking
7ce15e9 feat: fundamentals progress endpoint, state-machine worker, refresh tokens, auth client
```

---

## 2. Pipeline Status (as of this session)

| Step | Status | Count |
|------|--------|-------|
| Step 1 — Universe Seed | ✅ Done | 11,844 raw → 6,460 seeded |
| Step 2 — Price Sync | ✅ Done | 6,435 with price data |
| Step 3 — Fundamentals | ⚠️ In Progress | Only 49/6,435 classified (0.8%) |
| Step 4 — Visible Universe | ❌ Blocked by Step 3 | 0 visible |
| Step 5 — Peer Medians | ❌ Blocked by Step 4 | Not run |

**Step 3 is the active bottleneck.** In production (real EODHD API key), the Full Fundamentals Sync button must be used.

---

## 3. What Was Done This Session

### 3A. Fundamentals Progress Endpoint (run_id architecture)
- `GET /api/admin/pipeline/fundamentals-progress` — returns `{total_queued, pending, processing, complete, error, percentage, run_active, run_id, zombies_reclaimed}`
- `run_full_fundamentals_sync` uses run_id state machine: `updateMany` tags all queued tickers, workers atomically claim via `findOneAndUpdate`, progress endpoint aggregates by `fundamentals_run_id`
- Op B reset: resets `null / "error" / zombie-processing (>15 min) / stale-pending (>60 min)` → `"pending"` at each job start
- `zombies_reclaimed` stored in `ops_config` and returned in progress response

### 3B. Cancel Event Architecture
- `run_full_fundamentals_sync`: background `_cancel_monitor` polls `ops_config` every 2s, deletes `cancel_job_full_fundamentals_sync` ONCE, sets `asyncio.Event`
- `_process_fundamentals_ticker`: checks `cancel_event.is_set()` at 3 points: after API fetch, before financials `bulk_write`, before earnings `bulk_write` — never interrupts active write
- `run_fundamentals_changes_sync` (scheduler): same pattern with `_cancel_monitor_sched`
- Both jobs return `status="cancelled"` + `cancelled_at` + Prague timestamps

### 3C. Step 2.1 Fix
- `run_daily_bulk_catchup` in `price_ingestion_service.py`: removed 30-day gap loop entirely
- Now: single `fetch_bulk_eod_latest("US")` call (latest day only)
- Cancel checks: after API call returns (2a) + before each `bulk_write` batch (2b)

### 3D. Refresh Token System
- `backend/auth_service.py`: `create_refresh_token`, `consume_refresh_token` (30-day, one-time use, HttpOnly cookie)
- `POST /api/auth/refresh`: reads cookie, rotates both tokens, returns `{"token": "..."}`
- Google OAuth callback + dev-login now also issue refresh token cookie
- `frontend/utils/api_client.ts`: `authenticatedFetch` with single-flight refresh lock + explicit `/api/auth/refresh` guard

### 3E. Parser Fix
- `parse_company_fundamentals`: sector/industry now stripped at source (`(general.get("Sector") or "").strip() or None`)
- `sync_ticker_fundamentals`: was missing `has_classification` — fixed
- `PARSER DEBUG` critical logs in all three sync paths

### 3F. Persistence Fixes
- `_process_fundamentals_ticker` now also persists to `tracked_tickers`:
  - `shares_outstanding` from `data["SharesStats"]["SharesOutstanding"]` (raw EODHD, not company_doc)
  - `financial_currency` from `extract_statement_currency(data)`

### 3G. Admin Audit & Debug Endpoints
- `GET /api/admin/ticker/{TICKER}/fundamentals-audit?live=0|1`
  - `live=0` (default): DB-only check, 0 credits
  - `live=1`: live EODHD call (10 credits) + fetched block + mismatch
  - Returns: `fetched, persisted, cache_snapshot, values_check, missing, mismatch, verdict`
- `POST /api/admin/ticker/{TICKER}/run-fundamentals-sync`
  - Single-ticker Step 3 runner for verifiable persistence
  - Uses same logic as bulk worker (`_process_fundamentals_ticker`)
  - Writes: `company_fundamentals_cache`, `company_financials`, `company_earnings_history`, `insider_activity`, `tracked_tickers`
  - Returns row counts (upserted_count, modified_count, matched_count per collection)

### 3H. Pipeline UI Fixes
- `handleCancelJob`: immediate `clearInterval` + `setRunningJob(null)` + deferred 4s confirmation
- `startPolling`: clock-skew fix (`runStart >= startedAt - 10_000`), progress poll for both fundamentals jobs
- `FundamentalsProgress` interface: `zombies_reclaimed` field
- Progress display: header row with `total_queued + zombies_reclaimed`

---

## 4. Audit Proof for A.US (production)

**live=0 before this session's runner:**
- sector=Healthcare ✓, industry=Diagnostics & Research ✓
- financial_currency=USD ✓, has_classification=true ✓
- `shares_outstanding = null` ❌ (now fixed via `_process_fundamentals_ticker`)
- `company_financials rows = 0` ❌ (now fixed via `/run-fundamentals-sync`)
- `company_earnings_history rows = 0` ❌ (now fixed via `/run-fundamentals-sync`)
- `fundamentals_status = "pending"` ❌

**After running `POST /api/admin/ticker/A.US/run-fundamentals-sync`:**
- Expected: all fields populated, verdict PASS

---

## 5. Immediate Next Tasks (in priority order)

### TASK 1 (do first): Commit pending server.py change
```bash
git add backend/server.py
git commit -m "feat(audit): cache_snapshot in fundamentals-audit endpoint"
git push
```

### TASK 2: Validate A.US end-to-end in production
1. `POST /api/admin/ticker/A.US/run-fundamentals-sync` — expect rows written > 0
2. `GET /api/admin/ticker/A.US/fundamentals-audit?live=0` — expect verdict PASS
   - shares_outstanding != null
   - financial_rows > 0
   - earnings_rows > 0
   - cache_snapshot.name = "Agilent Technologies Inc"

### TASK 3: Remove PARSER DEBUG logs (after validation)
The `logger.critical("PARSER DEBUG: ...")` logs in:
- `backend/fundamentals_service.py:parse_company_fundamentals`
- `backend/full_sync_service.py:_process_fundamentals_ticker`
- `backend/batch_jobs_service.py:sync_single_ticker_fundamentals`
- `backend/server.py:admin_run_single_fundamentals_sync`

These are temporary diagnostic logs. Remove them after A.US audit passes.

### TASK 4: Run Full Fundamentals Sync for all 6,435 tickers
Once single-ticker validation passes, press "⬇ Full Sync" in the admin pipeline UI.
The progress bar (via `GET /api/admin/pipeline/fundamentals-progress`) will show live status.
Expected duration: ~20-30 min in production.

### TASK 5: Step 4 — Compute Visible Universe
After Step 3 completes, run `POST /api/admin/scheduler/run/compute-visible-universe`.
Eligibility gates: `is_delisted != true`, `shares_outstanding > 0`, `financial_currency` exists, `has_price_data == true`, `has_classification == true`.

### TASK 6: Step 5 — Peer Medians
After Step 4, run `POST /api/admin/scheduler/run/peer-medians`.

---

## 6. Key Files Changed This Session

| File | What Changed |
|------|-------------|
| `backend/full_sync_service.py` | run_id arch, cancel_event, shares_outstanding, PARSER DEBUG |
| `backend/scheduler_service.py` | cancel_event for fundamentals_sync, Prague timestamps |
| `backend/server.py` | Progress endpoint, audit endpoint, run-fundamentals-sync endpoint, cache_snapshot (uncommitted) |
| `backend/auth_service.py` | Refresh token functions |
| `backend/fundamentals_service.py` | Parser strip fix, has_classification fix, PARSER DEBUG |
| `backend/batch_jobs_service.py` | PARSER DEBUG log |
| `backend/price_ingestion_service.py` | Step 2.1 single-fetch + cancel checks |
| `frontend/app/admin/pipeline.tsx` | STOP fix, clock-skew fix, progress UI, zombies_reclaimed |
| `frontend/utils/api_client.ts` | authenticatedFetch + single-flight refresh lock (NEW FILE) |

---

## 7. Architecture Reminders

- **Frontend NEVER calls EODHD** — all data from scheduled jobs → MongoDB
- **Visible universe filter** = `is_visible == true` (set by Step 4)
- **Peer medians** = `compute_peer_benchmarks_v3` → `peer_benchmarks` collection
- **Cancel pattern**: `ops_config.key = "cancel_job_{job_name}"` → worker `cancel_event` via background monitor, flag deleted ONCE
- **run_id pattern**: `ops_config.key = "active_fundamentals_run"` → `{run_id, started_at, zombies_reclaimed}`
- **Progress endpoint**: aggregates `tracked_tickers` by `fundamentals_run_id = active_run_id`
- **Refresh tokens**: HttpOnly cookie `refresh_token`, path `/api/auth/refresh`, 30-day expiry, rotate on use

---

## 8. Admin Endpoints Reference (new this session)

| Method | Path | Purpose | Credits |
|--------|------|---------|---------|
| GET | `/api/admin/pipeline/fundamentals-progress` | Live run_id progress | 0 |
| GET | `/api/admin/ticker/{T}/fundamentals-audit?live=0` | DB audit | 0 |
| GET | `/api/admin/ticker/{T}/fundamentals-audit?live=1` | DB + EODHD audit | 10 |
| POST | `/api/admin/ticker/{T}/run-fundamentals-sync` | Single-ticker Step 3 | 10 |
| POST | `/api/auth/refresh` | Session refresh (HttpOnly cookie) | 0 |
