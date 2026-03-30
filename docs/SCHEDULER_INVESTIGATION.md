# Scheduler Investigation: AUTO Pipeline + Gap Creation

## 1) Executive Summary

Steps 2 (price_sync) and 3 (fundamentals_sync) in `scheduler_loop()` called
their job functions **without `ignore_kill_switch=True`**, combined with a
failure-detection check that did not catch `"skipped"` status. This allowed
the scheduler to advance `last_run` and chain progress when no data was
actually fetched, creating price/fundamental gaps.

## 2) PROVEN Gap-Creation Root Cause

### Bug 1: Missing `ignore_kill_switch=True` on Steps 2/3

The scheduler already checks the kill switch at the top of each loop tick
(`scheduler.py:603-608`). Steps 2/3 then call `run_daily_price_sync` and
`run_fundamentals_changes_sync` which **internally re-check** the kill switch
(`scheduler_service.py:1229` and `scheduler_service.py:2935`). Without
`ignore_kill_switch=True`, a kill-switch toggle between the outer check and
the inner check causes the job to return `{"status": "skipped"}` without
fetching any data.

The manual endpoint (`server.py:7481,7537`) correctly passes
`ignore_kill_switch=True`. The scheduler did not.

**Files + lines (before fix):**
- `scheduler.py:697-700` — Step 2 calls `run_daily_price_sync(_db, parent_run_id=_pid, chain_run_id=_cid)` without `ignore_kill_switch=True`
- `scheduler.py:764-766` — Step 3 calls `run_fundamentals_changes_sync(_db, parent_run_id=_pid, chain_run_id=_cid)` without `ignore_kill_switch=True`
- `scheduler_service.py:1228-1239` — `run_daily_price_sync` internal kill-switch check returns `{"status": "skipped", "reason": "kill_switch_engaged"}`
- `scheduler_service.py:2934-2945` — `run_fundamentals_changes_sync` internal kill-switch check returns `{"status": "skipped", "reason": "kill_switch_engaged"}`

### Bug 2: Failure detection did not catch `"skipped"` status

The original `_s2_failed` / `_s3_failed` checks:
```python
# scheduler.py:706-708 (BEFORE fix)
_s2_failed = (
    isinstance(_s2_result, dict)
    and (_s2_result.get("error") or _s2_result.get("status") == "failed")
)
```

For a result of `{"status": "skipped", "reason": "kill_switch_engaged"}`:
- `isinstance(_s2_result, dict)` → `True`
- `_s2_result.get("error")` → `None` (falsy)
- `_s2_result.get("status") == "failed"` → `False` (it's `"skipped"`, not `"failed"`)
- `True and (None or False)` → `True and False` → **`False`**

So `_s2_failed = False`, meaning the scheduler treats `"skipped"` as success.

### How this creates price/fundamental gaps

When `_s2_failed = False`:
1. `last_run["price_sync"] = today_str` is set (`scheduler.py:719`)
2. `set_last_run_state(last_run)` persists it to `ops_config` (`scheduler.py:720`)
3. `pipeline_chain_runs` advances to Step 3 (`scheduler.py:725-732`)
4. Step 3 triggers via `should_run_after_dependency` (`scheduler.py:753`)
5. Same bug repeats: Step 3 returns `"skipped"`, `last_run["fundamentals_sync"]` is set
6. `pipeline_chain_runs` is marked `"completed"` (`scheduler.py:798`)

**Result:** The pipeline appears fully completed for today. No data was fetched.
The next day's run starts fresh — the missed day's prices/fundamentals are never
backfilled, creating a gap.

### Exact statuses wrongly treated as success

| Status | Returned by | Was caught? | Should be caught? |
|--------|-------------|-------------|-------------------|
| `"skipped"` | Kill-switch internal check | ❌ No | ✅ Yes |
| `"cancelled"` | Cancel-check internal logic | ❌ No | ✅ Yes |
| `"failed"` | Job failure | ✅ Yes | ✅ Yes |
| Non-dict result | `run_job_with_retry` max retries | ❌ No | ✅ Yes |

## 3) 03:00 Non-Run Investigation

### Status: UNPROVEN

The trigger logic (`should_run`, weekday gating, DST handling) is **code-correct**.
Cannot prove or disprove the 03:00 non-run from code alone.

### Exact records needed to prove/disprove

| Collection | Field(s) | Missed date/time to inspect | What value proves |
|---|---|---|---|
| `ops_config` | `key: "scheduler_enabled"`, `value` | Value at 03:00 on missed date | `false` → (f) auto disabled |
| `ops_config` | `key: "scheduler_last_run"`, `value.universe_seed` | Value at 03:00 on missed date | `== missed_date` → (e) blocked by last_run |
| `system_job_logs` | `job_name: "scheduler_heartbeat"`, `start_time` | Entries around 03:00 on missed date | No entries 02:45-03:15 → (a) never fired (process dead) |
| `system_job_logs` | `job_name: "universe_seed"`, `status`, `start_time` | Entry for missed date | `"error"` → (c) fired and failed |
| `ops_job_runs` | `job_name: "universe_seed"`, `status`, `started_at` | Entry for missed date | `"skipped"` → (b) fired and skipped |
| `ops_job_runs` | `job_name: "universe_seed"`, `status: "running"` | Stuck entry from prior day | exists → (d) blocked by lock/stuck run |

### What each value proves

- **(a) never fired**: No heartbeat logs around 03:00 → scheduler process was not running
- **(b) fired and skipped**: `ops_job_runs` shows `universe_seed` with `status: "skipped"` → kill switch was engaged
- **(c) fired and failed**: `system_job_logs` or `ops_job_runs` shows `universe_seed` with error status
- **(d) blocked by lock**: `ops_job_runs` has a stuck `"running"` entry from a prior day
- **(e) blocked by last_run**: `scheduler_last_run.value.universe_seed == missed_date` → already marked as ran today
- **(f) auto disabled**: `ops_config.scheduler_enabled.value == false` at 03:00

## 4) Minimal Fix (proven gap bug only)

Two changes in `backend/scheduler.py`:

1. **Pass `ignore_kill_switch=True`** to Steps 2/3 job calls (lines 697, 764).
   The scheduler already verified the kill switch at line 603 — the internal
   re-check is redundant and creates a race condition.

2. **Whitelist-based success detection** for `_s2_failed`/`_s3_failed` (lines 708, 777).
   Only `"completed"` or `"success"` statuses advance `last_run`. Any other
   status (including `"skipped"`, `"cancelled"`, non-dict) is treated as failure
   and retried next tick.
