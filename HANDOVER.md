# HANDOVER.md

> Last updated: 2026-03-09 — Full chain run + unified CSV export (d2fbcfe)

---

## 0. Why we are switching agents

The previous agent session ran all day (2026-03-09) and accumulated a very long
context with many back-and-forth corrections. A fresh agent starts from clean
state, reads only `main` and this document, and is less likely to repeat the
same mistakes.

**Current deploy status:** Railway auto-deploys from `main`.
Verify the Railway Deployments tab to confirm the active commit hash.
During incidents or outages, new commits may not be live even after push.
Latest commit on `main` as of this handover: `d2fbcfe`.

---

## 1. Git State

```
branch: main (clean, no uncommitted work)
latest commit: d2fbcfe
```

Recent commits (newest → oldest):

| Hash | Description |
|------|-------------|
| `d2fbcfe` | feat(pipeline): POST /run-full-now + GET /export/full unified CSV |
| `532b707` | feat(pipeline-export): deterministic parent_run_id chain for Steps 2-4 exports |
| `5c3f2fd` | feat(step1-export): run-scoped raw rows + seeded set; duplicate handling before seeding |
| `9cfac41` | fix(step1-card): s1In = seeded_count + step1Filtered |
| `dc4636f` | fix(pipeline-export): use QUOTE_ALL so names with commas don't break CSV |
| `a8d69ed` | fix(pipeline-export): all steps use exclusion-report as source for filtered rows |
| `6a03b7d` | feat(pipeline-card): add Export CSV button to Steps 1-4 |
| `510a8aa` | fix(pipeline-funnel): arithmetic chain from exclusion-report counts |
| `607e83c` | fix(step2): coverage via $lookup — correct STEP2_QUERY; fix stale var |

---

## 1a. Branches / PRs

`main` is the only long-lived branch and single source of truth.
The following `cursor/*` remote branches exist but should be **ignored unless
explicitly revisited by Richard**:

- `origin/cursor/step-3-queue-proposal-4984`
- `origin/cursor/fundamentals-sync-progress-aee1`
- `origin/cursor/development-environment-setup-dcbf`

---

## 2. Canonical pipeline math (Steps 1–4)

The funnel chain is strictly sequential. Input(step N) = Output(step N-1)
for the **same run**. Never mix live-DB counts with run-scoped counts.

```
Step 1: raw_rows_total  →  seeded_count        (filtered = raw - seeded)
Step 2: seeded_count    →  with_price_count     (filtered = seeded - price)
Step 3: with_price      →  classified_count     (filtered = price - classified)
Step 4: classified      →  visible_count        (filtered = classified - visible)
```

**Admin pipeline card** reads these numbers from
`exclusion-report.by_step[step_label]` (authoritative) and computes:
- `s1In  = seeded_count + step1Filtered`
- `s1Out = s1In - step1Filtered = seeded_count`
- `s2Out = s1Out - step2Filtered`
- `s3Out = s2Out - step3Filtered`
- `s4Out = s3Out - step4Filtered`

Step labels in `pipeline_exclusion_report`:
- `"Step 1 - Universe Seed"`
- `"Step 2 - Price Sync"`
- `"Step 3 - Fundamentals Sync"`
- `"Step 4 - Visible Universe"`

---

## 2a. Canonical debug flow (run full pipeline now)

Use this flow to run the full pipeline immediately AND get a unified audit CSV.
This avoids "chain broken" errors that occur with manual single-step triggers.

### Step-by-step

**1. Start the full chain:**
```
POST /api/admin/pipeline/run-full-now
```
Returns `{ "chain_run_id": "chain_abc123...", "status": "started" }`.

**2. Poll until complete:**
```
GET /api/admin/pipeline/chain-status/{chain_run_id}
```
Poll every 30–60 s. Wait for `"status": "completed"`.
Confirm `step_run_ids` contains non-null values for step1–step4.

**3. Download unified CSV:**
```
GET /api/admin/pipeline/export/full?chain_run_id={chain_run_id}
```
Columns: `ticker, name, step, reason`.
One row per Step 1 raw row — row count equals `raw_rows_total`.
`step` = first step where the ticker was filtered, or `OK` if it passed all steps.

### Why manual single-step runs show "chain broken" in CSV exports

When a step is triggered manually (Admin Panel button) without a preceding step
in the same session, `parent_run_id = NULL` is stored. The export cannot guess
which prior run was the correct parent — doing so would silently reintroduce
drift. The job data in MongoDB is correct; only the run-scoped CSV export is
unavailable for that run. Use `/run-full-now` to get a fully linked chain.

---

## 2b. Post-deploy verification checklist

After deploying a new backend version, verify with:

1. `POST /api/admin/pipeline/run-full-now` → get `chain_run_id`
2. `GET /api/admin/pipeline/chain-status/{chain_run_id}` → wait for `status=completed`;
   confirm `step_run_ids.step1`–`step4` are all non-null strings.
3. `GET /api/admin/pipeline/export/full?chain_run_id=<id>` → download CSV;
   confirm row count equals `raw_rows_total` from the Step 1 run.
4. Spot-check 5–10 filtered tickers: verify `step` and `reason` in the CSV
   match `pipeline_exclusion_report` rows for the same `run_id`.
5. Spot-check 2–3 `OK` tickers: confirm they are in `universe_seed_seeded_tickers`
   and in `tracked_tickers` with `is_visible=True`.

---

## 3. Current blockers

**Step 2–4 run-scoped exports require a scheduler chain or `/run-full-now`.**
Manual single-step triggers set `parent_run_id=NULL` → exports return "chain broken".
This is by design. Use `/run-full-now` for audit exports.

---

## 4. Canonical universe queries

```python
# backend/scheduler_service.py
SEED_QUERY  = {"exchange": {"$in": ["NYSE", "NASDAQ"]}, "asset_type": "Common Stock"}
STEP3_QUERY = {**SEED_QUERY, "has_price_data": True}
```

- **Never invent a new filter.** Always reuse `STEP3_QUERY`.
- Step 4 universe: `get_canonical_sieve_query()` in `backend/visibility_rules.py`.
- Gate 7 (shares_outstanding) reads the **flat field** `tracked_tickers.shares_outstanding`.

---

## 5. Agreed technical design — Step 1 RAW export

### New MongoDB collections

**`universe_seed_raw_rows`**
```
{ run_id, exchange, global_raw_row_id, raw_row_index, raw_symbol (verbatim EODHD object), created_at (UTC datetime) }
```
- Retention: keep last 3 run_ids; purge consistent with other Step 1 collections.

**`universe_seed_seeded_tickers`**
```
{ run_id, ticker, created_at (UTC datetime) }
```
- Run-scoped seeded set; export uses this, never live `tracked_tickers`.

**`pipeline_chain_runs`**
```
{ chain_run_id, status, started_at, finished_at, step_run_ids: {step1, step2, step3, step4}, error }
```
- Created by `POST /api/admin/pipeline/run-full-now`.

### run_id contract

- `run_id` generated once at job start and passed explicitly as `parent_run_id`
  to the next step in the chain.
- Stored in `ops_job_runs.details.parent_run_id` and `details.exclusion_report_run_id`.
- Manual triggers: `parent_run_id=None` — exports show "chain broken".

---

## 6. Step 3 architecture (event-driven — HARD RULE)

`run_fundamentals_changes_sync` in `backend/scheduler_service.py`:
1. `purge_orphaned_fundamentals_events(db)` — removes out-of-universe events.
2. `_deduplicate_pending_events` — marks duplicate events as `deduped`.
3. `_skip_already_complete_tickers` — marks events for `complete` tickers as `skipped`.
4. Fetches remaining `pending` events; filters to `STEP3_QUERY`.
5. Processes with `sync_single_ticker_fundamentals`.
**Step 3 must NOT iterate all tickers. It is event-driven.**

---

## 7. Known failure modes to avoid

- **Silent dedup or silent filtering** in Step 1 raw collection — forbidden.
- **Using live `tracked_tickers` for run-scoped exports** — forbidden.
- **Committing or pushing without explicit `GO (commit+push)` from Richard** — forbidden.
- **Per-row DB queries in export** — forbidden; preload reasons into a dict once.
- **Mixing run_ids** — raw rows, seeded set, exclusion rows must share the same `run_id`.
- **Inconsistent purge** — when purging old runs, always purge all three Step 1 collections together.
- **Inventing new fields/endpoints without approval** — forbidden; ask first.

---

## 8. Admin endpoints reference (current)

| Method | Path | Purpose | Credits |
|--------|------|---------|---------|
| POST | `/api/admin/pipeline/run-full-now` | Run full Step 1→4 chain immediately | varies |
| GET  | `/api/admin/pipeline/chain-status/{id}` | Poll chain run status | 0 |
| GET  | `/api/admin/pipeline/export/full?chain_run_id=` | Unified CSV (all steps) | 0 |
| GET  | `/api/admin/pipeline/export/step/{1-4}?run_id=` | Per-step CSV export | 0 |
| GET  | `/api/admin/pipeline/exclusion-report?run_id=` | Exclusion rows + step1_counts | 0 |
| GET  | `/api/admin/pipeline/exclusion-report/download` | CSV download of exclusion report | 0 |
| GET  | `/api/admin/pipeline/funnel-gap` | Identify tickers unaccounted in funnel | 0 |
| GET  | `/api/admin/pipeline/fundamentals-progress` | Live run_id progress (full sync) | 0 |
| GET  | `/api/admin/ticker/{T}/fundamentals-audit?live=0` | DB-only audit | 0 |
| GET  | `/api/admin/ticker/{T}/fundamentals-audit?live=1` | DB + live EODHD audit | 10 |
| POST | `/api/admin/ticker/{T}/run-fundamentals-sync` | Single-ticker Step 3 runner | 10 |
| GET  | `/api/admin/pipeline/fundamentals-health` | Corruption sample probe | 0–500 |
| GET  | `/api/admin/jobs/{job_name}/status` | Job status + step3_funnel counts | 0 |
| POST | `/api/admin/jobs/enqueue-manual-refresh/run` | Re-enqueue 28 stuck tickers | 0 |

---

## 9. Open questions

1. Step 2–4 per-step exports (`/export/step/{2,3,4}`) return "chain broken" for manual
   runs. The unified `/export/full` via `/run-full-now` is the preferred alternative.
2. The old per-step Export CSV buttons in the Admin Panel still exist — can be hidden
   or replaced with a "Run Full Pipeline" button if Richard requests it.
