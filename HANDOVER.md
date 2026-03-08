# HANDOVER.md

> Last updated: 2026-03-08 — Step 3/Step 4 universe alignment + orphan purge

---

## 0. Git State (clean, on `main`)

```
branch: main
last commit: 29e3353 feat(step3): purge orphaned fundamentals_events at job start
```

No uncommitted work. All changes are on `main`.

---

## 1. Canonical Pipeline Definitions

### Step 3 Universe — STEP3_QUERY (single source of truth)

```python
# backend/scheduler_service.py
SEED_QUERY   = {"exchange": {"$in": ["NYSE", "NASDAQ"]}, "asset_type": "Common Stock"}
STEP3_QUERY  = {**SEED_QUERY, "has_price_data": True}
```

- This exact constant is also used by `universe_counts_service.py` as `step3_query`.
- It is the source of truth for the "Tickers with prices" count on the Step 3 pipeline card.
- **Do NOT invent a new filter** for Step 3 universe — always reuse `STEP3_QUERY`.

### Step 4 Universe — get_canonical_sieve_query()

```python
# backend/visibility_rules.py
def get_canonical_sieve_query() -> dict:
    # Returns: exchange in [NYSE,NASDAQ], asset_type=Common Stock,
    #          has_price_data=True, sector/industry present,
    #          is_delisted != true, financial_currency present,
    #          shares_outstanding > 0 (flat field: tracked_tickers.shares_outstanding)
```

- Used by `recompute_visibility_all` for both `total` count and iteration cursor.
- Imported in `server.py` via `from visibility_rules import get_canonical_sieve_query`.

---

## 2. Step 3 Architecture — Event-Driven, NOT Full Re-Download

`run_fundamentals_changes_sync` in `backend/scheduler_service.py`:
1. Calls `purge_orphaned_fundamentals_events(db)` — deletes events for tickers outside `STEP3_QUERY`.
2. Enqueues `classification_missing` events for tickers in `STEP3_QUERY` missing sector/industry.
3. Reads `fundamentals_events` where `status = "pending"`.
4. Filters `tickers_to_sync` to `STEP3_QUERY` universe via targeted `distinct`.
5. Processes up to `batch_size` tickers with `sync_single_ticker_fundamentals`.
6. Progress denominator = `len(tickers_to_sync)` (event batch), NOT the full universe.
7. `universe_total` stored in `ops_job_runs.details` for informational display only.

**HARD RULE: Step 3 is event-driven. Never change it to re-download all tickers.**

---

## 3. Recently Merged Commits (Step 3/Step 4)

| Hash | Description |
|------|-------------|
| `29e3353` | `purge_orphaned_fundamentals_events()` — deletes events outside STEP3_QUERY at job start |
| `cd56086` | Progress based on event batch size; status endpoint no longer rewrites progress string; STEP3_QUERY introduced |
| `72b1304` | Step 4 uses `get_canonical_sieve_query()`; emojis removed from pipeline UI |
| `351c5a1` | Step 4 `recompute_visibility_all` uses batched `bulk_write` + pre-snapshot safe cleanup + progress |
| `8df9808` | Step 4 Gate 7 reads `shares_outstanding` from flat field (not nested `fundamentals` sub-doc) |

---

## 4. Known Issue — fundamentals_events Queue Pollution (NOT YET FIXED)

**Symptom:** `fundamentals_events` contains duplicates and stale events, causing:
- `db_pending_count` much larger than the actual work needed
- Job runs longer than necessary processing already-complete tickers

**Root cause:** The queue is never deduped and events are not skipped for tickers that already have `fundamentals_status = "complete"`.

**Next planned fix (NOT implemented):**
- When reading pending events, skip tickers where `fundamentals_status = "complete"` AND no refresh is needed
- Deduplicate: keep only the most recent event per (ticker, event_type)
- This must be proposed to Richard before implementation

---

## 5. Admin Endpoints Reference (added this session)

| Method | Path | Purpose | Credits |
|--------|------|---------|---------|
| GET | `/api/admin/pipeline/fundamentals-progress` | Live run_id progress for full sync | 0 |
| GET | `/api/admin/ticker/{T}/fundamentals-audit?live=0` | DB audit | 0 |
| GET | `/api/admin/ticker/{T}/fundamentals-audit?live=1` | DB + live EODHD audit | 10 |
| POST | `/api/admin/ticker/{T}/run-fundamentals-sync` | Single-ticker Step 3 runner | 10 |
| GET | `/api/admin/pipeline/fundamentals-health` | Corruption sample probe | 0 / up to 500 |
| GET | `/api/admin/jobs/{job_name}/status` | Job status + scoped DB counts | 0 |

---

## 6. Governance Rules for Next Agent

See **AGENTS.md** for full rules. Key points for this domain:

1. **One task at a time.** Close the current task before starting the next.
2. **No code changes without explicit GO from Richard.**
3. **Never invent new universe filters** — reuse `STEP3_QUERY` or `get_canonical_sieve_query()`.
4. **Prague timezone** for all `ops_job_runs` timestamp fields (`started_at_prague`, `finished_at_prague`).
5. **Step 3 is event-driven** — do not change it to iterate all tickers.
6. **UI copy** — do not change UI copy unless explicitly requested; flag it if logic changes affect displayed text.
7. **No emojis** in UI run-result strings (replaced with plain text in commit `72b1304`).
