# Dashboard Metric Audit

**Date:** 2026-03-22
**Scope:** Every metric, widget, card, row, badge, status, count, ratio, and timestamp currently displayed on the Admin Dashboard tab (`frontend/app/(tabs)/admin.tsx`).

---

## 1. Full Metric Mapping Table

The dashboard fetches two endpoints on mount:
- `GET /api/admin/overview` → `overview` (from `get_admin_overview` in `admin_overview_service.py`)
- `GET /api/admin/stats` → `stats` (from `get_admin_stats` in `server.py`)

### Alert Banner (conditional, top of page)

| # | Widget / Label (exact UI text) | Business Reason | DB Source / Query | Semantic Meaning | Classification | Invalidation Rule | Recommendation |
|---|-------------------------------|-----------------|-------------------|------------------|----------------|-------------------|----------------|
| A1 | `"{N} pipeline job(s) failed"` | Know immediately if scheduled jobs are failing | `overview.health.jobs_failed` — count of `ops_job_runs` docs with `status∈{failed,error}` started today (≥ Prague midnight UTC) | Number of distinct job runs that finished with failure status since today's Prague midnight | **operational status** | Stale once a new day starts or jobs re-run successfully | **keep** |
| A2 | `"Scheduler is paused"` | Critical: if scheduler is off, no data updates happen | `overview.health.scheduler_active` — `ops_config` doc `{key: "scheduler_enabled"}` value field | Whether the APScheduler-based job scheduler is currently enabled | **operational status** | Reflects live config; toggling the scheduler invalidates it | **keep** |
| A3 | `"0 visible tickers — universe not seeded"` | No visible universe means the product shows nothing to users | `overview.price_integrity.today_visible == 0` — derived from `tracked_tickers` `{is_visible: true}` count, anchored to latest completed `pipeline_chain_runs` doc | Zero tickers passed the 3-step pipeline sieve | **strict truth** (count from DB flags set by pipeline) | Next successful pipeline run updates `is_visible` flags | **keep** |
| A4 | `"{N} date(s) with incomplete price coverage"` | Warns about trading dates where not all visible tickers have price data | `overview.price_integrity.missing_expected_dates` — see row C2 below | Count of canonical bulk-ingested dates with coverage < 100% of visible tickers | **strict truth** (derived from `ops_job_runs` bulk gapfill + `stock_prices` counts) | New bulk ingestion or price re-downloads change it | **keep** |
| A5 | `"{N} ticker(s) need price re-download"` | Flags tickers whose prices were invalidated (split/dividend detected) | `overview.price_integrity.needs_price_redownload` — see row C3 below | Count of visible tickers with `needs_price_redownload=true` | **strict truth** (flag set by split/dividend detectors) | Cleared when full re-download completes and sets proof marker | **keep** |

### Section A — Business

| # | Widget / Label (exact UI text) | Business Reason | DB Source / Query | Semantic Meaning | Classification | Invalidation Rule | Recommendation |
|---|-------------------------------|-----------------|-------------------|------------------|----------------|-------------------|----------------|
| B1 | **Users** (count) | Core business KPI: how many users have registered | `stats.users` — `db.users.aggregate([{$facet: {users: [{$count: "n"}]}}])` | Total documents in `users` collection | **strict truth** | New user signup / account deletion | **keep** |
| B2 | **Portfolios** (count) | Business KPI: user engagement — how many portfolios created | `stats.portfolios` — `db.portfolios.aggregate([{$facet: {portfolios: [{$count: "n"}]}}])` | Total documents in `portfolios` collection | **strict truth** | User creates/deletes portfolio | **keep** |
| B3 | **Positions** (count) | Business KPI: user engagement depth — total stock positions held | `stats.positions` — `db.positions.aggregate([{$facet: {positions: [{$count: "n"}]}}])` | Total documents in `positions` collection | **strict truth** | User adds/removes position | **keep** |

### Section B — Ops Health

| # | Widget / Label (exact UI text) | Business Reason | DB Source / Query | Semantic Meaning | Classification | Invalidation Rule | Recommendation |
|---|-------------------------------|-----------------|-------------------|------------------|----------------|-------------------|----------------|
| O1 | **Pipeline (1–3)** (e.g. `"5.2h ago"`, green/yellow/red dot) | Know if the full 3-step pipeline (Universe → Price → Fundamentals) is running on schedule | `overview.pipeline_age.pipeline_hours_since_success` — `ops_job_runs.find_one({job_name: "fundamentals_sync", status: {$in: ["success","completed"]}}, sort: finished_at desc)` → hours since `finished_at` | Hours elapsed since the last successful `fundamentals_sync` (Step 3) job finished | **run-status (stale)** — only tells you when last success was, not whether data is complete | A new successful Step 3 run updates `finished_at`; thresholds: <25h green, ≤48h yellow, >48h red | **keep** — useful operational signal |
| O2 | **Morning Refresh** (e.g. `"3.1h ago"`, green/yellow/red dot) | Know if daily price sync ran recently | `overview.pipeline_age.morning_refresh_hours_since_success` — `ops_job_runs.find_one({job_name: "price_sync", status: {$in: ["success","completed"]}}, sort: finished_at desc)` → hours since `finished_at` | Hours elapsed since the last successful `price_sync` (Step 2) job finished | **run-status (stale)** — same caveat | Same thresholds | **keep** — useful operational signal |
| O3 | **Scheduler** (`"Running"` / `"Paused"`, green/red dot) | Critical: scheduler off = no automation | `overview.health.scheduler_active` — `ops_config.find_one({key: "scheduler_enabled"})` value field | Whether the APScheduler is currently enabled | **operational status** | Toggling scheduler config | **keep** |
| O4 | **Failed Jobs** (count, green/red dot) | Immediate awareness of pipeline failures | `overview.health.jobs_failed` — count of today's `ops_job_runs` with failure status | Number of job runs that failed today (Prague time) | **operational status** | New day boundary / re-run succeeds | **keep** |

### Section C — Price Integrity / Coverage

#### Key Metrics Row

| # | Widget / Label (exact UI text) | Business Reason | DB Source / Query | Semantic Meaning | Classification | Invalidation Rule | Recommendation |
|---|-------------------------------|-----------------|-------------------|------------------|----------------|-------------------|----------------|
| C1 | **Last Bulk Date** (date string, e.g. `"2026-03-21"`) | Know the latest EODHD bulk payload date processed | `overview.price_integrity.last_bulk_trading_date` — `pipeline_state.find_one({_id: "price_bulk"})` → `global_last_bulk_date_processed` | The most recent trading date for which a bulk EODHD EOD file was successfully ingested | **strict truth** (written atomically by `price_ingestion_service.py` on successful bulk ingest) | Next bulk ingest writes a newer date | **keep** |
| C2 | **Missing Bulk Dates** (integer count) | How many ingested trading dates have incomplete ticker coverage | `overview.price_integrity.missing_expected_dates` — aggregation: collect all successful `processed_date` values from last 10 `price_sync` `ops_job_runs` bulk gapfill days → count dates where `stock_prices` coverage < `today_visible` for visible tickers | Number of canonical bulk-processed dates where at least one visible ticker lacks a price row | **strict truth** (derived from canonical bulk ingestion log + actual price rows) | New price inserts or bulk re-ingestion reduce count | **keep** |
| C3 | **Need Re-download** (integer count) | Tickers with invalidated prices needing full re-download | `overview.price_integrity.needs_price_redownload` — `tracked_tickers.aggregate([{$match: {is_visible: true}}, {$facet: {needs_redownload: [{$match: {needs_price_redownload: true}}, {$count: "n"}]}}])` | Count of visible tickers where `needs_price_redownload=true` (set by split/dividend detectors) | **strict truth** (flag set by detectors, cleared by full re-download + proof write) | Full re-download + `history_download_proven_at` write clears it | **keep** |
| C4 | **Incomplete History (remediation)** (integer count) | Tickers that haven't completed initial historical price download | `overview.price_integrity.price_history_incomplete` — `tracked_tickers.aggregate([..., {$facet: {incomplete_history: [{$match: {price_history_complete: {$ne: true}}}, {$count: "n"}]}}])` | Count of visible tickers where legacy `price_history_complete != true` | **heuristic** — `price_history_complete` is a legacy operational flag, NOT the strict proof model (`history_download_proven_at`). It is set by the old download path and may be stale. | Becomes stale if ticker goes through new download path but legacy flag isn't set | **rename** → "Incomplete History (legacy flag)" or **move to technical-debug section** — the canonical truth is `Complete Prices (strict proof)` (row C9) |
| C5 | **Complete Fundamentals** (`count/total (pct%)`) | How many visible tickers have completed fundamentals sync (all sub-collections written + shares_outstanding verified) | `overview.price_integrity.fundamentals_complete_count` / `today_visible` — `tracked_tickers.aggregate([{$match: {is_visible: true}}, {$facet: {fundamentals_complete: [{$match: {fundamentals_complete: true}}, {$count: "n"}]}}])` | Count of visible tickers where `fundamentals_complete=true`. This is an **operational completion flag** set by `batch_jobs_service.sync_single_ticker_fundamentals()` after all sub-collection writes succeed + shares_outstanding verification. **Not** a strict proof marker — there is no `fundamentals_proven_at` timestamp equivalent. | **operational flag** — set at write time; no post-write row-count verification persisted as proof; no continuity anchor for staleness | Cleared to `false` by `batch_jobs_service` on failure; `needs_fundamentals_refresh=true` (from split/dividend/earnings detectors) triggers re-sync that resets this flag | **keep but label clearly** — this is the best available fundamentals completeness indicator until strict proof fields are implemented |

#### Recent Bulk Coverage Subsection

| # | Widget / Label (exact UI text) | Business Reason | DB Source / Query | Semantic Meaning | Classification | Invalidation Rule | Recommendation |
|---|-------------------------------|-----------------|-------------------|------------------|----------------|-------------------|----------------|
| C6 | **Section header:** `"Recent Bulk Coverage ({N} visible · run {hash})"` | Context: how many tickers + which pipeline run the visible count comes from | `overview.price_integrity.today_visible` + `overview.price_integrity.today_visible_source.chain_run_id` — visible = `tracked_tickers.distinct("ticker", {is_visible: true})`, source = latest completed `pipeline_chain_runs` doc `chain_run_id` | The count of tickers currently marked `is_visible=true`, with provenance to the pipeline run that set those flags | **strict truth** (is_visible flags are written by Step 3, stable between runs) | Next successful pipeline chain run may change visible set | **keep** |
| C7 | **Last closing day** checkpoint row (date + `have/total (pct%)`) | Can users see today's prices? | `overview.price_integrity.coverage_checkpoints.last_closing_day` — target = `last_bulk_trading_date` or today; nearest actual = `stock_prices.find_one({date: {$lte: target}}, sort: date desc)`; count = `stock_prices.aggregate({$match: {date: actual, ticker: {$in: visible}}}, {$group: {_id: "$date", count: {$sum: 1}}})` | Of all visible tickers, how many have a `stock_prices` row on the most recent bulk-ingested trading date | **strict truth** (direct count from `stock_prices`) | New price inserts for that date increase coverage | **keep** |
| C8 | **1 week ago** checkpoint row (date + `have/total (pct%)`) | Were prices ingested for last week? | Same mechanism as C7 with target = `today - 7 days` | Same semantics, for the nearest trading date ≤ 7 days ago | **strict truth** | Same | **keep** |

#### Price Completeness (process truth) Subsection

| # | Widget / Label (exact UI text) | Business Reason | DB Source / Query | Semantic Meaning | Classification | Invalidation Rule | Recommendation |
|---|-------------------------------|-----------------|-------------------|------------------|----------------|-------------------|----------------|
| C9 | **Complete Prices (strict proof)** (`count/total (pct%)`) | Core truth: how many tickers have proven complete historical price data | `overview.price_integrity.history_download_completed_count` / `today_visible` — `tracked_tickers.aggregate([{$match: {is_visible: true}}, {$facet: {history_download_completed: [{$match: {history_download_proven_at: {$exists: true, $type: "date"}, history_download_proven_anchor: {$exists: true, $ne: null}}}, {$count: "n"}]}}])` | Count of visible tickers where BOTH `history_download_proven_at` (datetime) AND `history_download_proven_anchor` (date string) exist. Derived inline from proof markers — does NOT read the pre-computed `history_download_completed` boolean (which requires the manual backfill). | **strict truth** (strict proof model: requires explicit proof markers written by `full_sync_service.py` or `scheduler_service.py` split/dividend remediation) | Proof markers cleared (`$unset`) by split/dividend detectors in `scheduler_service.py`; re-set by full re-download in `full_sync_service.py` or remediation in `scheduler_service.py` | **keep** — this is the canonical price completeness metric |
| C10 | **Gap-Free Since Download** (`count/total (pct%)`) | Strongest truth: complete history AND no missing bulk dates since download anchor | `overview.price_integrity.gap_free_since_history_download_count` / `today_visible` — computed inline in `get_price_integrity_metrics()`: (1) fetch proven tickers from `tracked_tickers.find({is_visible: true, history_download_proven_at: {$exists: true, $type: "date"}, history_download_proven_anchor: {$exists: true, $ne: null}})`, (2) fetch coverage from `stock_prices.aggregate({$match: {ticker: {$in: proven_tickers}, date: {$in: expected_dates}}}, {$group: {_id: "$ticker", dates: {$addToSet: "$date"}}})`, (3) count tickers where ALL canonical bulk dates after anchor are covered | Count of proven-download tickers that have price data for every canonical bulk-processed date after their anchor. Computed inline — does NOT read the pre-computed `gap_free_since_history_download` boolean. | **strict truth** (strictest metric: proven download + zero gaps vs canonical bulk dates from `ops_job_runs`) | Any new bulk date not covered by a ticker invalidates gap-free status; proof marker invalidation (split/dividend) resets to 0 | **keep** — this is the gold-standard price completeness metric |

#### Historical Depth (heuristic) Subsection

| # | Widget / Label (exact UI text) | Business Reason | DB Source / Query | Semantic Meaning | Classification | Invalidation Rule | Recommendation |
|---|-------------------------------|-----------------|-------------------|------------------|----------------|-------------------|----------------|
| C11 | **Full Price History (heuristic)** (`count/total (pct%)`) | Secondary depth indicator (≥252 rows, min date ≥1yr ago) | `overview.price_integrity.full_price_history_count` / `today_visible` — `tracked_tickers.aggregate([..., {$facet: {full_price_history: [{$match: {full_price_history: true}}, {$count: "n"}]}}])` | Count of visible tickers where `full_price_history=true` (heuristic: `row_count ≥ 252 AND min_date ≤ today-365d`) | **heuristic** — this does NOT require the strict proof marker; it is a row-count/date-range heuristic computed by `backfill_full_price_history()` | Re-computed on each backfill run; can be true even without proof marker | **move to technical-debug section** — clearly labeled "heuristic" already, but could mislead if shown alongside strict truth metrics |
| C12 | **1 month ago** checkpoint row (date + `have/total (pct%)`) | Historical depth: do tickers have prices from a month ago? | Same mechanism as C7 with target = `today - 30 days` | Of visible tickers, how many have a price row on nearest trading date ≤ 30 days ago | **strict truth** (direct count from `stock_prices`), but labeled under "Historical Depth (heuristic)" section | Same | **keep** — but **move** out of "heuristic" section since it IS strict truth |
| C13 | **1 year ago** checkpoint row (date + `have/total (pct%)`) | Historical depth: do tickers have prices from a year ago? | Same mechanism as C7 with target = `today - 365 days` | Of visible tickers, how many have a price row on nearest trading date ≤ 365 days ago | **strict truth** (direct count), but low ratios reflect genuinely incomplete historical backfill, not a bug | Same | **keep** — same as C12, move out of "heuristic" section |

---

## 2. Proposed Reduced "Business Truth" Dashboard

Focused ONLY on:
- **A) Complete Prices for visible tickers**
- **B) Complete Fundamentals for visible tickers**

### A) Price Completeness — Strict-Proof Fields That Exist Today

The price truth model is fully implemented. These fields exist on `tracked_tickers` and are used on the dashboard:

| Field on `tracked_tickers` | Written By | Meaning | Dashboard Metric |
|---------------------------|------------|---------|-----------------|
| `history_download_proven_at` (datetime) | `full_sync_service.py` (after successful full download), `scheduler_service.py` (after split/dividend remediation re-download) | Proof timestamp: full historical price download was completed | Backing field for `history_download_completed` |
| `history_download_proven_anchor` (date string) | Same writers | Continuity anchor: latest price date covered by the historical download | Used to check for gaps in bulk dates after this anchor |
| `history_download_completed` (bool, computed) | `full_sync_service.py` and `scheduler_service.py` (set alongside proof markers at download/remediation time); cleared to `false` by split/dividend detectors | Pre-computed for backward compat. Dashboard facet now derives from proof markers directly. | Legacy backing for **"Complete Prices (strict proof)"** (C9) — dashboard no longer reads this field |
| `gap_free_since_history_download` (bool, computed) | Same writers as above | Pre-computed for backward compat. Dashboard computes gap-free inline from proof markers + bulk coverage. | Legacy backing for **"Gap-Free Since Download"** (C10) — dashboard no longer reads this field |
| `missing_bulk_dates_since_history_download` (int, computed) | Same | Count of canonical bulk dates after anchor where ticker lacks a price row | Used internally for gap-free computation |

**Proposed reduced price truth metric for "Business Truth" dashboard:**

> **"Complete Prices"** = `gap_free_since_history_download == true`
> Displayed as: `{count}/{today_visible} ({pct}%)`
> Single metric. If this is 100%, every visible ticker has proven historical download + zero gaps vs all bulk-ingested dates.

**Supporting context (keep but secondary):**
- Last Bulk Date (C1) — when was last data ingested
- Missing Bulk Dates (C2) — are there date-level gaps
- Need Re-download (C3) — how many tickers have invalidated prices

### B) Fundamentals Completeness — Current State

**Fields that exist today on `tracked_tickers`:**

| Field | Written By | Meaning | Strict-Proof? |
|-------|------------|---------|---------------|
| `fundamentals_complete` (bool) | `batch_jobs_service.sync_single_ticker_fundamentals()` — set to `true` after all sub-collection writes succeed + shares_outstanding verification | All fundamentals sub-collections written successfully | **NO** — this is an operational completion flag, not a strict proof marker. There is no `fundamentals_proven_at` timestamp equivalent. |
| `fundamentals_status` (string: `"complete"` / other) | Same writer | Status enum for fundamentals processing | **NO** — same issue: operational flag, not timestamp-anchored proof |
| `fundamentals_updated_at` (datetime) | Same writer | When fundamentals were last synced | **Partial** — timestamps when last written, but there's no anchor or verification that the data is still valid |
| `needs_fundamentals_refresh` (bool) | Set to `true` by event detectors (split/dividend/earnings); cleared by `sync_single_ticker_fundamentals` | Whether fundamentals need re-fetch | **YES** — this is a strict invalidation flag |
| `has_classification` (bool) | Same writer | `= sector AND industry both non-empty` | **YES** — directly verifiable from the stored fields |
| `sector` (string) | Same writer | From EODHD fundamentals General.Sector | Raw fact |
| `industry` (string) | Same writer | From EODHD fundamentals General.Industry | Raw fact |
| `shares_outstanding` (float) | Same writer | From EODHD fundamentals, verified post-write | Raw fact |
| `financial_currency` (string) | Same writer | Extracted from financial statements | Raw fact |

**The honest answer: Fundamentals do NOT yet have strict-proof equivalents.**

A proposed "Fundamentals Complete" metric for the Business Truth dashboard would need to be:

> **"Complete Fundamentals"** = strict proof that ALL required sub-collections exist with valid data for each visible ticker.

This does NOT exist today. Here is the gap:

---

## 3. Fundamentals Strict-Proof Gap

### What "Complete Fundamentals" Should Mean (Strict Truth)

For each visible ticker, fundamentals are complete if and only if ALL of the following are provably true:

1. **Classification** — `sector` AND `industry` non-empty on `tracked_tickers` ✅ (exists: `has_classification`)
2. **Shares outstanding** — `shares_outstanding > 0` on `tracked_tickers` ✅ (exists, verified post-write)
3. **Financial currency** — `financial_currency` non-empty on `tracked_tickers` ✅ (exists)
4. **Income statements** — at least 4 quarterly rows exist in `company_financials` (or equivalent) for TTM calculations ❌ (no proof field)
5. **Balance sheets** — at least 1 quarterly row exists in `company_financials` ❌ (no proof field)
6. **Cash flow statements** — at least 4 quarterly rows exist in `company_financials` for TTM calculations ❌ (no proof field)
7. **Earnings history** — rows exist in `company_earnings_history` ❌ (no proof field)
8. **Company profile** — doc exists in `company_fundamentals_cache` ❌ (no proof field)

### Missing Strict-Proof Fields Needed

| Missing Field | Should Be Written On | Written By | When | Purpose |
|--------------|---------------------|------------|------|---------|
| `fundamentals_proven_at` (datetime) | `tracked_tickers` | `batch_jobs_service.sync_single_ticker_fundamentals()` | After ALL sub-collection writes succeed AND row-count verification passes | Strict proof timestamp (like `history_download_proven_at` for prices) |
| `fundamentals_proven_anchor` (date string) | `tracked_tickers` | Same | Same | The latest financial period date covered (e.g., latest quarterly report date), for staleness checking |
| `fundamentals_income_stmt_quarterly_count` (int) | `tracked_tickers` | Same | Same | Verified count of quarterly income statement rows written |
| `fundamentals_balance_sheet_quarterly_count` (int) | `tracked_tickers` | Same | Same | Verified count of quarterly balance sheet rows written |
| `fundamentals_cash_flow_quarterly_count` (int) | `tracked_tickers` | Same | Same | Verified count of quarterly cash flow rows written |
| `fundamentals_earnings_count` (int) | `tracked_tickers` | Same | Same | Verified count of earnings history rows written |
| `fundamentals_download_completed` (bool, computed) | `tracked_tickers` | Backfill / audit job (like `backfill_full_price_history`) | Recomputed periodically | `= fundamentals_proven_at IS NOT NULL AND income_stmt_count >= 4 AND balance_sheet_count >= 1 AND cash_flow_count >= 4` |

### Invalidation Rules for Fundamentals Proof

The fundamentals proof markers should be cleared (`$unset`) when:
- `needs_fundamentals_refresh` is set to `true` (split/dividend/earnings event detected)
- A re-sync is triggered

This mirrors the price proof model where `history_download_proven_at` is cleared by split/dividend detectors.

### What the Business Truth Dashboard Would Show

| Metric | Source | Exists Today? |
|--------|--------|---------------|
| **Complete Prices** (gap-free since download) | `gap_free_since_history_download == true` on `tracked_tickers` | ✅ YES |
| **Complete Fundamentals** (proven download with verified sub-collections) | `fundamentals_download_completed == true` on `tracked_tickers` | ❌ NO — field does not exist; `fundamentals_complete` is operational, not strict proof |

### Summary

**Prices:** The strict-proof regime is fully implemented. The dashboard already shows the correct metrics (`History Download Completed`, `Gap-Free Since Download`). A reduced Business Truth dashboard can use `gap_free_since_history_download` as the single price completeness metric.

**Fundamentals:** There is NO strict-proof equivalent today. The existing `fundamentals_complete` / `fundamentals_status` fields are operational completion flags set at write time, without:
- Post-write row-count verification persisted as proof
- A continuity anchor (latest period date) for staleness detection
- An explicit proof timestamp analogous to `history_download_proven_at`

Before building a "Complete Fundamentals" metric on the Business Truth dashboard, the strict-proof fields listed above must be implemented in `batch_jobs_service.sync_single_ticker_fundamentals()` and a corresponding backfill/audit job must be created (analogous to `backfill_full_price_history()`).

---

## 4. Structured Metric Reference (Name / Reason / Description / Source of Truth)

### Complete Fundamentals

- **Name:** Complete Fundamentals
- **Reason:** Shows how many visible tickers have successfully completed the full fundamentals sync pipeline (income statements, balance sheets, cash flow, earnings, sector/industry classification, shares outstanding). This is the best available fundamentals completeness indicator; used to detect tickers stuck in "pending" or "error" states.
- **Description:** Count of visible tickers where `fundamentals_complete = true`, displayed as `count/today_visible (pct%)`. The `fundamentals_complete` field is an operational flag — it is set to `true` by `batch_jobs_service.sync_single_ticker_fundamentals()` after all sub-collection writes succeed AND the post-write `shares_outstanding` verification passes. It is set to `false` on failure. **Caveat:** this is NOT a strict proof marker (no `fundamentals_proven_at` timestamp, no continuity anchor, no persisted row-count verification). It indicates "the last sync succeeded" rather than "the data is provably complete right now."
- **Source of truth:**
  - **Mongo collection:** `tracked_tickers`
  - **Field:** `fundamentals_complete` (boolean)
  - **Computed aggregation:** `tracked_tickers.aggregate([{$match: {is_visible: true}}, {$facet: {fundamentals_complete: [{$match: {fundamentals_complete: true}}, {$count: "n"}]}}])`
  - **Backend service:** `get_price_integrity_metrics()` in `backend/services/admin_overview_service.py`
  - **Endpoint:** `GET /api/admin/overview` → `overview.price_integrity.fundamentals_complete_count`
  - **Written by:** `batch_jobs_service.sync_single_ticker_fundamentals()` in `backend/batch_jobs_service.py` (line ~344) — sets `fundamentals_complete: true` after successful sync; sets `fundamentals_complete: false` on failure (line ~146)
  - **Invalidation:** `needs_fundamentals_refresh` set to `true` by split/dividend/earnings detectors triggers re-sync

### Complete Prices (strict proof)

- **Name:** Complete Prices (strict proof)
- **Reason:** The canonical price completeness metric. Shows how many visible tickers have a provably complete full historical price download. This uses the strict proof regime — legacy operational flags like `price_history_complete` are NOT sufficient. Only tickers with explicit proof markers written after a successful full download are counted.
- **Description:** Count of visible tickers where BOTH `history_download_proven_at` (datetime) AND `history_download_proven_anchor` (date string) exist on the `tracked_tickers` document, displayed as `count/today_visible (pct%)`. The proof marker `history_download_proven_at` is a UTC timestamp recording when a full historical price download was completed. The anchor `history_download_proven_anchor` is a `"YYYY-MM-DD"` date string representing the latest price date covered by that download (used for gap-checking). The dashboard derives this count directly from the proof markers via facet query — it does NOT read the pre-computed `history_download_completed` boolean.
- **Source of truth:**
  - **Mongo collection:** `tracked_tickers`
  - **Fields:** `history_download_proven_at` (datetime), `history_download_proven_anchor` (date string `"YYYY-MM-DD"`)
  - **Computed aggregation:** `tracked_tickers.aggregate([{$match: {is_visible: true}}, {$facet: {history_download_completed: [{$match: {history_download_proven_at: {$exists: true, $type: "date"}, history_download_proven_anchor: {$exists: true, $ne: null}}}, {$count: "n"}]}}])`
  - **Backend service:** `get_price_integrity_metrics()` in `backend/services/admin_overview_service.py`
  - **Endpoint:** `GET /api/admin/overview` → `overview.price_integrity.history_download_completed_count`
  - **Written by:**
    - `full_sync_service.py` (line ~204): after successful full historical download — sets `history_download_proven_at` + `history_download_proven_anchor`
    - `scheduler_service.py` (line ~804): after successful split/dividend remediation re-download — sets same fields
  - **Invalidation:** Split detector (`scheduler_service.py` line ~466) and dividend detector (line ~569) `$unset` both proof markers + set computed fields to `false`

### Gap-Free Since Download

- **Name:** Gap-Free Since Download
- **Reason:** The gold-standard price completeness metric. Shows how many visible tickers have BOTH a proven historical download AND zero missing bulk dates since the download anchor. This is stronger than "Complete Prices" because it additionally verifies that every canonical bulk-ingested trading date after the ticker's download anchor has price data for that ticker.
- **Description:** Count of proven-download tickers (per "Complete Prices" criteria above) that also have price data for every canonical bulk-processed date after their anchor, displayed as `count/today_visible (pct%)`. Computed inline: (1) fetch all proven tickers and their anchors from `tracked_tickers`, (2) get canonical bulk-processed dates from `ops_job_runs` bulk gapfill history, (3) query `stock_prices` for each proven ticker's coverage of those dates, (4) count tickers where ALL bulk dates after anchor are covered. If no bulk dates exist, all proven tickers are trivially gap-free.
- **Source of truth:**
  - **Mongo collections:** `tracked_tickers` (proof markers + anchors), `stock_prices` (actual coverage), `ops_job_runs` (canonical bulk-processed dates)
  - **Fields:** `tracked_tickers.history_download_proven_at`, `tracked_tickers.history_download_proven_anchor`, `stock_prices.date` + `stock_prices.ticker`, `ops_job_runs.details.price_bulk_gapfill.days[].processed_date` (where `status == "success"`)
  - **Computed aggregation:** Inline in `get_price_integrity_metrics()` — NOT a facet read of a pre-computed field. Steps: (a) `tracked_tickers.find({is_visible: true, history_download_proven_at: {$exists: true, $type: "date"}, history_download_proven_anchor: {$exists: true, $ne: null}})`, (b) `_get_bulk_processed_dates(db)` from `ops_job_runs`, (c) `stock_prices.aggregate([{$match: {ticker: {$in: proven_tickers}, date: {$in: expected_dates}}}, {$group: {_id: "$ticker", dates: {$addToSet: "$date"}}}])`, (d) count tickers where `∀ date ∈ expected_dates WHERE date > anchor: date ∈ ticker_dates`
  - **Backend service:** `get_price_integrity_metrics()` in `backend/services/admin_overview_service.py`
  - **Endpoint:** `GET /api/admin/overview` → `overview.price_integrity.gap_free_since_history_download_count`
  - **Written by:** No single writer — this is a live computation from three collections. The underlying proof markers are written by `full_sync_service.py` and `scheduler_service.py` (same as "Complete Prices"). Bulk dates come from successful `price_sync` jobs recorded in `ops_job_runs`.
  - **Invalidation:** Proof marker invalidation (split/dividend detectors) resets the ticker to non-proven. Any new bulk-processed date not covered by a ticker changes the count.

---

## 5. "Missing Bulk Dates" — Deep-Dive Investigation

### Executive Summary

**"Missing Bulk Dates"** counts the number of trading dates for which bulk
ingestion reported success (in `ops_job_runs`) but where at least one currently
visible ticker is missing a `stock_prices` row.  The metric reads from
`ops_job_runs.details.price_bulk_gapfill.days[]` — a field written **only** by
the scheduled / full-pipeline code path
(`scheduler_service.run_daily_price_sync`), **not** by the legacy manual
endpoint (`POST /admin/prices/sync-daily` → `sync_daily_prices`).

If an admin ran prices via the legacy `sync_daily_prices` endpoint, prices are
written to `stock_prices` but the canonical gapfill metadata is never recorded.
The metric therefore cannot "see" the manual run's contribution and stays
unchanged.

If an admin used **"Run Full Pipeline Now"** (which calls
`run_daily_price_sync`), the metric IS updated — but it may still show ≥ 1 if
any visible ticker lacks a price row for any previously recorded bulk date.
This happens when new tickers are added to the visible universe after a bulk
date was already ingested — those tickers naturally lack data for older dates.

### Q1 — Exact Data Sources and Fields

#### Last Bulk Date

| Item | Value |
|------|-------|
| Collection | `pipeline_state` |
| Document | `{_id: "price_bulk"}` |
| Field | `global_last_bulk_date_processed` (string `"YYYY-MM-DD"`) |
| Written by | `price_ingestion_service._write_price_bulk_state()` (called from `scheduler_service.run_daily_price_sync` after successful bulk write) |
| Backend code | `admin_overview_service.get_price_integrity_metrics()` line ~751: `bulk_state = await db.pipeline_state.find_one({"_id": "price_bulk"})` |

#### Missing Bulk Dates

| Item | Value |
|------|-------|
| Response field | `overview.price_integrity.missing_expected_dates` |
| Source A — expected dates | `ops_job_runs` collection: `{job_name: "price_sync", status: {$in: ["success","completed"]}, "details.price_bulk_gapfill.days": {$exists: true}}` → last 10 docs by `finished_at` desc → extract `days[].processed_date` where `days[].status == "success"` |
| Source B — actual coverage | `stock_prices` collection: aggregate by `date` for `{date: {$in: expected_dates}, ticker: {$in: visible_tickers}}` → count tickers per date |
| Source C — visible ticker count | `tracked_tickers.distinct("ticker", {is_visible: true})` (anchored to latest completed `pipeline_chain_runs` doc) |
| Backend code | `admin_overview_service._get_bulk_processed_dates()` (lines ~590–620) + `get_price_integrity_metrics()` (lines ~859–893) |

#### Need Re-download

| Item | Value |
|------|-------|
| Collection | `tracked_tickers` |
| Query | `{is_visible: true, needs_price_redownload: true}` → `$count` |
| Written by | Split/dividend detectors in `scheduler_service.py` set `needs_price_redownload: true`; cleared by full re-download in `full_sync_service.py` |
| Backend code | `get_price_integrity_metrics()` facet query line ~763 |

### Q2 — Exact Formula for "Missing Bulk Dates = 1"

**Step 1: Collect expected dates**

```
_get_bulk_processed_dates(db):
  ops_job_runs.aggregate([
    {$match: {job_name: "price_sync",
              status: {$in: ["success","completed"]},
              "details.price_bulk_gapfill.days": {$exists: true}}},
    {$sort: {finished_at: -1}},
    {$limit: 10},
    {$project: {"details.price_bulk_gapfill.days": 1}},
  ])
  → For each doc: extract days where status == "success" and processed_date exists
  → Return sorted set of date strings
```

**Step 2: Count dates with incomplete coverage (`gap_count`)**

```
stock_prices.aggregate([
  {$match: {date: {$in: expected_dates}, ticker: {$in: visible_tickers}}},
  {$group: {_id: "$date", count: {$sum: 1}}},
  {$match: {count: {$lt: today_visible}}},   ← date has SOME rows but fewer than visible count
  {$count: "n"},
])
→ gap_count = result[0].n or 0
```

**Step 3: Count dates with zero coverage**

```
stock_prices.aggregate([
  {$match: {date: {$in: expected_dates}, ticker: {$in: visible_tickers}}},
  {$group: {_id: "$date"}},
])
→ dates_with_data = set of dates that have at least one price row
→ dates_with_zero_coverage = count of expected_dates NOT in dates_with_data
```

**Final formula:**

```
missing_expected_dates = gap_count + dates_with_zero_coverage
```

**"Missing Bulk Dates = 1" means:** there is exactly **one** date in the
last 10 successful `price_sync` runs' gapfill days where either:

- (a) some visible tickers have price rows but the count is less than the
  current `today_visible` count (partial coverage), **or**
- (b) no visible tickers have any price row for that date at all (zero
  coverage).

**Which dates are expected?** Only dates explicitly recorded in
`ops_job_runs.details.price_bulk_gapfill.days[]` with `status: "success"`.
No calendar heuristic or rolling window is used.

**Which dates are present?** Dates with at least one `stock_prices` row for a
currently visible ticker.

**Which date is considered missing?** Any expected date where the number of
visible tickers with a price row is strictly less than `today_visible`.

### Q3 — Manual Full Pipeline vs Scheduled Flow

#### Does manual full pipeline execute the same bulk download path?

**Yes.** The "Run Full Pipeline Now" button (`POST /admin/pipeline/run-full-now`)
calls **the exact same function** as the scheduler:
`scheduler_service.run_daily_price_sync()`.

| Aspect | Scheduled (scheduler loop) | Manual Full Pipeline (`run-full-now`) |
|--------|---------------------------|---------------------------------------|
| Entry point | `scheduler_service.scheduler_loop()` | `server.py:admin_run_full_pipeline_now()` |
| Step 2 function | `scheduler_service.run_daily_price_sync(db)` | `scheduler_service.run_daily_price_sync(db, ignore_kill_switch=True, parent_run_id=s1_run_id, chain_run_id=chain_id, run_doc_id=_s2_run_doc_id)` |
| Bulk download | `price_ingestion_service.run_daily_bulk_catchup()` | Same |
| Writes `pipeline_state.price_bulk` | ✅ via `_write_price_bulk_state()` | ✅ Same |
| Writes `ops_job_runs.details.price_bulk_gapfill.days` | ✅ | ✅ Same |
| Job name in `ops_job_runs` | `"price_sync"` | `"price_sync"` |

**Exact code path for full pipeline (manual):**
1. `server.py:7222` → `admin_run_full_pipeline_now()` → `_run_chain()`
2. Step 2: `server.py:7416` → `run_daily_price_sync(db, ...)`
3. Inside: `scheduler_service.py:1438` → `run_daily_bulk_catchup(db, ...)`
4. Bulk writes: `price_ingestion_service.py:1075`
5. State update: `scheduler_service.py:1520` → `_write_price_bulk_state()`
6. Gapfill days: `scheduler_service.py:1544` → `ops_job_runs.update_one({$set: {"details.price_bulk_gapfill.days": days}})`

**Exact code path for scheduled:**
1. `scheduler.py` → `scheduler_loop()` → calls `run_daily_price_sync(db)`
2. Same as steps 2–6 above.

#### Does manual price update (legacy endpoint) use the same path?

**No.** The legacy endpoint `POST /admin/prices/sync-daily` calls
`price_ingestion_service.sync_daily_prices(db)` — a completely different
function.

| Aspect | `run_daily_price_sync` (scheduled/pipeline) | `sync_daily_prices` (legacy manual) |
|--------|---------------------------------------------|--------------------------------------|
| File | `scheduler_service.py:1158` | `price_ingestion_service.py:349` |
| Writes `pipeline_state.price_bulk` | ✅ | ❌ |
| Writes `ops_job_runs.details.price_bulk_gapfill.days` | ✅ | ❌ |
| `ops_job_runs` field for job identity | `job_name: "price_sync"` | `job_type: "daily_price_sync"` |
| Writes prices to `stock_prices` | ✅ | ✅ |
| Ticker source | `tracked_tickers` with `_STEP2_QUERY` | `company_fundamentals_cache.distinct("ticker")` |

### Q4 — Why "Missing Bulk Dates" Remains 1 After Manual Run

There are **two distinct scenarios** depending on which manual path was used:

#### Scenario A: Legacy `sync_daily_prices` endpoint was used

The metric remains unchanged because the legacy function does **not** write the
persisted state that `_get_bulk_processed_dates()` reads:

| Missing persisted state | Expected location | What legacy `sync_daily_prices` writes instead |
|------------------------|-------------------|-----------------------------------------------|
| `ops_job_runs.details.price_bulk_gapfill.days` | `ops_job_runs` doc with `job_name: "price_sync"` | A doc with `job_type: "daily_price_sync"` and no `price_bulk_gapfill` field |
| `pipeline_state.price_bulk.global_last_bulk_date_processed` | `pipeline_state` collection | Nothing — `pipeline_state` not touched |

Even though `stock_prices` rows are written, the metric does not query
`stock_prices` to discover dates — it reads the **canonical list of expected
dates** from `ops_job_runs.details.price_bulk_gapfill.days[]`. Since the legacy
path never writes that field, the expected-dates set is unchanged, and the
coverage check against it yields the same result.

#### Scenario B: "Run Full Pipeline Now" was used

The gapfill days ARE written. The metric may still show ≥ 1 if:

1. **New tickers in the visible universe:** Tickers added after a bulk date was
   ingested will not have price rows for that older date. The metric compares
   coverage against the **current** `today_visible` count, not the count at
   ingestion time. A single newly added ticker causes every prior bulk date to
   show as having incomplete coverage.

2. **Ticker not in EODHD bulk payload:** Some tracked tickers may not appear in
   the EODHD `eod-bulk-last-day/US` endpoint response (e.g., recently delisted,
   OTC tickers, tickers with pending data on the provider side).

3. **Date mismatch:** The metric checks **all** successful bulk dates from the
   last 10 `price_sync` runs. If any older date has even one ticker missing,
   that date counts.

### Q5 — Is "Missing Bulk Dates" Used as Input/Filter for Step 2?

**No.** The `missing_expected_dates` metric is purely observational. Step 2
does not read or filter by it.

**What Step 2 actually uses as input:**

- **Ticker set:** `tracked_tickers.distinct("ticker", {exchange: {$in: ["NYSE","NASDAQ"]}, asset_type: "Common Stock"})` via `_STEP2_QUERY` in `price_ingestion_service.run_daily_bulk_catchup()` (or `seeded_tickers_override` when called from the full pipeline chain)
- **Date to fetch:** The EODHD `eod-bulk-last-day/US` endpoint returns the provider's latest available trading day. No date parameter is passed by default.
- **Gap detection:** `scheduler_service._get_missed_trading_dates()` determines missed dates by comparing `pipeline_state.price_bulk.global_last_bulk_date_processed` against the current date, using NYSE trading calendar rules.

The `missing_expected_dates` metric is computed in `admin_overview_service.py`
for display purposes only and is never imported or referenced by
`scheduler_service.py` or `price_ingestion_service.py`.

### Exact Files Involved

| File | Role |
|------|------|
| `backend/services/admin_overview_service.py` | `_get_bulk_processed_dates()` (lines 590–620), `get_price_integrity_metrics()` (lines 735–942) — computes the metric |
| `backend/scheduler_service.py` | `run_daily_price_sync()` (lines 1158–1835) — writes `price_bulk_gapfill.days` to `ops_job_runs`, calls `_write_price_bulk_state()` |
| `backend/price_ingestion_service.py` | `_write_price_bulk_state()` (lines 93–103) — updates `pipeline_state.price_bulk`; `run_daily_bulk_catchup()` (lines 818–1100) — performs actual bulk download and writes to `stock_prices`; `sync_daily_prices()` (lines 349–452) — legacy manual endpoint that does NOT write gapfill metadata |
| `backend/server.py` | `admin_run_full_pipeline_now()` (line 7222) — "Run Full Pipeline Now" entry point; `admin_sync_daily_prices()` (line 2676) — legacy manual sync endpoint |
| `frontend/app/(tabs)/admin.tsx` | Lines 219–222 — reads `missing_expected_dates`, displays as "Missing Bulk Dates" with red/green status |
| `backend/tests/test_price_integrity_metrics.py` | Tests for `missing_expected_dates` calculation |
