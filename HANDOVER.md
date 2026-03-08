# HANDOVER.md

> Last updated: 2026-03-08 — Step 3/Step 4 universe alignment + orphan purge + docs

---

## 0. Git State

```
branch: main (clean, no uncommitted work)
```

Recent commits (newest → oldest):

| Hash | Description |
|------|-------------|
| `4ca7b18` | docs: rewrite HANDOVER.md + fix AGENTS.md (remove unverified claims) |
| `29e3353` | feat(step3): purge orphaned fundamentals_events at job start |
| `cd56086` | fix(step3): scope fundamentals_sync universe to canonical STEP3_QUERY |
| `72b1304` | fix(step4): use get_canonical_sieve_query() for classified filter + remove emojis |
| `351c5a1` | feat(step4): bulk_write recompute + pre-snapshot safe cleanup + progress reporting |
| `5b4941b` | fix(pipeline): Step 4 Run calls /admin/job/recompute_visibility_all/run |
| `8df9808` | fix(step4): Gate 7 reads shares_outstanding from flat field not nested fundamentals |

---

## 1. Canonical Step 3 Universe

```python
# backend/scheduler_service.py
SEED_QUERY  = {"exchange": {"$in": ["NYSE", "NASDAQ"]}, "asset_type": "Common Stock"}
STEP3_QUERY = {**SEED_QUERY, "has_price_data": True}
```

- Same filter as `step3_query` in `backend/services/universe_counts_service.py`.
- Source of truth for the "Tickers with prices" count on the Step 3 pipeline card.
- **Never invent a new filter.** Always reuse `STEP3_QUERY`.

---

## 2. Step 3 Architecture — Event-Driven

`run_fundamentals_changes_sync` in `backend/scheduler_service.py`:

1. `purge_orphaned_fundamentals_events(db)` — deletes events for tickers outside `STEP3_QUERY`.
2. Enqueues `classification_missing` events for tickers missing sector/industry.
3. Fetches `fundamentals_events` where `status = "pending"`.
4. Filters `tickers_to_sync` to `STEP3_QUERY` via targeted `distinct`.
5. Processes tickers with `sync_single_ticker_fundamentals`.
6. Progress denominator = `len(tickers_to_sync)` (event batch).
7. `universe_total` in `ops_job_runs.details` — informational only.

**HARD RULE: Step 3 must NOT iterate all tickers. It is event-driven.**

---

## 3. Canonical Step 4 Universe

```python
# backend/visibility_rules.py
get_canonical_sieve_query()
# exchange in [NYSE,NASDAQ], asset_type=Common Stock, has_price_data=True,
# sector/industry present, is_delisted != true, financial_currency present,
# shares_outstanding > 0 (flat field: tracked_tickers.shares_outstanding)
```

- `recompute_visibility_all` uses batched `bulk_write` (500/batch) with pre-snapshot safe cleanup.
- Gate 7 reads `tracked_tickers.shares_outstanding` (flat field) — NOT the nested `fundamentals` sub-doc.
- Step 4 Run button calls `POST /api/admin/job/recompute_visibility_all/run`.

---

## 4. Known Issue — fundamentals_events Queue Pollution (NOT YET FIXED)

**Symptom:** `fundamentals_events` contains duplicates and stale events, causing:
- `db_pending_count` much larger than the actual work needed.
- Job runs longer than necessary (re-processing already-complete tickers).

**Root cause:** No deduplication; events not skipped for tickers with `fundamentals_status = "complete"`.

**Next planned fix (NOT implemented — must propose to Richard first):**
- Skip tickers where `fundamentals_status = "complete"` and no refresh is needed.
- Deduplicate: keep only the most recent event per `(ticker, event_type)`.

---

## 5. Admin Endpoints Reference

| Method | Path | Purpose | Credits |
|--------|------|---------|---------|
| GET | `/api/admin/pipeline/fundamentals-progress` | Live run_id progress (full sync) | 0 |
| GET | `/api/admin/ticker/{T}/fundamentals-audit?live=0` | DB-only audit | 0 |
| GET | `/api/admin/ticker/{T}/fundamentals-audit?live=1` | DB + live EODHD audit | 10 |
| POST | `/api/admin/ticker/{T}/run-fundamentals-sync` | Single-ticker Step 3 runner | 10 |
| GET | `/api/admin/pipeline/fundamentals-health` | Corruption sample probe | 0–500 |
| GET | `/api/admin/jobs/{job_name}/status` | Job status + STEP3_QUERY-scoped DB counts | 0 |

---

## 6. Governance (see AGENTS.md for full rules)

1. One task at a time — close before starting next.
2. No code changes without explicit GO from Richard.
3. Never invent new universe filters — reuse `STEP3_QUERY` or `get_canonical_sieve_query()`.
4. Prague timezone for all `ops_job_runs` fields (`started_at_prague`, `finished_at_prague`).
5. Step 3 is event-driven — never iterate all tickers.
6. Do not change UI copy without explicit request; flag if logic changes affect displayed text.
