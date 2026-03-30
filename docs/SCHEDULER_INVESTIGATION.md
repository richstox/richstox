# Scheduler Investigation

## 1) Executive Summary

Steps 2 and 3 in `scheduler_loop()` treated `{"status": "skipped"}` as success, advancing
`last_run` and chain progress without fetching data — creating price/fundamental gaps.

## 2) PROVEN Gap-Creation Root Cause

### Bug A — Missing `ignore_kill_switch=True` on Steps 2/3

The scheduler checks the kill switch once at the top of each tick (`scheduler.py:603-608`).
Steps 2/3 then call `run_daily_price_sync` / `run_fundamentals_changes_sync` which **internally
re-check** the kill switch:

- `scheduler_service.py:1228-1239` — `run_daily_price_sync` checks `get_scheduler_enabled(db)` →
  returns `{"status": "skipped", "reason": "kill_switch_engaged"}` when `ignore_kill_switch=False`
- `scheduler_service.py:2934-2945` — `run_fundamentals_changes_sync` same pattern

The manual endpoint (`server.py:7481,7537`) correctly passes `ignore_kill_switch=True`.
The scheduler did **not** — creating a race window where a kill-switch toggle between line 603
and line 1229/2935 causes a silent skip.

### Bug B — `_s2_failed` / `_s3_failed` did not catch `"skipped"` status

Original failure detection (before fix):

```python
# scheduler.py:706-708 (Step 2), scheduler.py:773-775 (Step 3)
_s2_failed = (
    isinstance(_s2_result, dict)
    and (_s2_result.get("error") or _s2_result.get("status") == "failed")
)
```

For `{"status": "skipped", "reason": "kill_switch_engaged"}`:
- `isinstance(dict)` → `True`
- `.get("error")` → `None` (falsy)
- `.get("status") == "failed"` → `False` (it is `"skipped"`, not `"failed"`)
- `True and (None or False)` → **`False`**

Result: `_s2_failed = False` — **scheduler treats `"skipped"` as success**.

### Exact statuses wrongly treated as success

| Status returned | Source | Caught by old check? |
|---|---|---|
| `"skipped"` | Internal kill-switch re-check | ❌ No |
| `"cancelled"` | Cancel callback | ❌ No |
| Non-dict | `run_job_with_retry` max retries | ❌ No |
| `"failed"` | Job failure | ✅ Yes |

### Exact code path where chain progress advances incorrectly

1. `scheduler.py:603` — outer kill switch: **enabled** (passes)
2. `scheduler.py:697-700` — Step 2 calls `run_daily_price_sync()` **without** `ignore_kill_switch=True`
3. `scheduler_service.py:1229` — inner kill switch toggled **off** between step 603 and here
4. `scheduler_service.py:1234-1237` — returns `{"status": "skipped", "reason": "kill_switch_engaged"}`
5. `scheduler.py:706-708` — `_s2_failed` evaluates to `False` (bug B)
6. `scheduler.py:719` — `last_run["price_sync"] = today_str` ← **advances without data**
7. `scheduler.py:720` — `set_last_run_state(last_run)` ← **persisted to `ops_config`**
8. `scheduler.py:725-732` — `pipeline_chain_runs` updated: `steps_done=[1,2]`, `current_step=3`
9. `scheduler.py:753` — `should_run_after_dependency("fundamentals_sync","price_sync",...)` → `True`
10. Steps 6-8 repeat for Step 3 (`scheduler.py:767-800`)
11. `pipeline_chain_runs.status = "completed"` ← **pipeline falsely completed**

### Why this creates price/fundamental gaps

- `last_run["price_sync"]` and `last_run["fundamentals_sync"]` are set to today
- Next day's run starts fresh — the missed day's data is never backfilled
- `pipeline_chain_runs` shows `"completed"` — admin sees no failure
- Gap persists in `stock_prices` and `company_fundamentals_cache` collections

## 3) Step 1 Non-Run Root Cause: INVISIBLE KILL SWITCH + MISSING OBSERVABILITY

### Status: IDENTIFIED AND FIXED

The scheduler loop checks the kill switch at the top of each tick. When engaged,
it skips ALL jobs including Step 1 — but the log was `logger.debug` (invisible at
INFO level) and NO database record was written. The heartbeat (every 15 min) proved
the process was alive, but there was ZERO evidence of the kill switch blocking.

Additionally, the spec (SCHEDULER_JOBS.md), audit script, and code comment ALL said
"Universe Seed: Sunday only". The actual code runs it **Mon-Sat** — Sunday is the
exclusion day (news-only). This documentation mismatch has also been fixed.

### Three observability gaps (all now fixed)

| # | Gap | Before | After |
|---|-----|--------|-------|
| 1 | Kill switch log level | `logger.debug` (invisible at INFO) | `logger.warning` (visible) |
| 2 | Kill switch in heartbeat | Not included | `details.kill_switch_engaged: true/false` in every heartbeat |
| 3 | Step 1 decision log | No DB record | `system_job_logs.scheduler_step1_decision` once per day |

### How to check Step 1 decisions going forward

Query `system_job_logs` for the new decision record:
```js
db.system_job_logs.find({
  job_name: "scheduler_step1_decision",
  "details.today_str": "2026-03-30"
})
```

Response shows:
- `details.decision`: `true` (will fire) or `false` (skipped)
- `details.reason`: `"will_trigger_now"`, `"already_completed_today"`, or `"not_yet_scheduled_time"`
- `details.last_run_universe_seed`: the value that blocked or allowed the run

### How to check if kill switch was engaged

Query heartbeats:
```js
db.system_job_logs.find({
  job_name: "scheduler_heartbeat",
  "details.kill_switch_engaged": true,
  start_time: {$gte: ISODate("2026-03-30T00:00:00Z")}
})
```

If `kill_switch_engaged: true` appears in heartbeats around 03:00, that is why Step 1 did not run.

## 4) Minimal Unified Diff (proven gap bug only)

```diff
diff --git a/backend/scheduler.py b/backend/scheduler.py
--- a/backend/scheduler.py
+++ b/backend/scheduler.py
@@ -695,15 +695,20 @@ async def scheduler_loop():
                     _s2_result = await run_job_with_retry(
                         "price_sync",
                         lambda _db, _pid=_s1_excl_run_id, _cid=_s1_chain_run_id: run_daily_price_sync(
-                            _db, parent_run_id=_pid, chain_run_id=_cid
+                            _db, parent_run_id=_pid, chain_run_id=_cid,
+                            ignore_kill_switch=True,
                         ),
                         db,
                     )
-                    # Only advance last_run on success so the step retries
-                    # next tick on failure (matching Step 1 pattern).
+                    # Only advance last_run on explicit success so the step
+                    # retries next tick on failure/skip/cancel.
+                    # NOTE: ignore_kill_switch=True above prevents the job's
+                    # internal kill-switch check from returning "skipped" after
+                    # the scheduler already verified the switch at loop top.
                     _s2_failed = (
-                        isinstance(_s2_result, dict)
-                        and (_s2_result.get("error") or _s2_result.get("status") == "failed")
+                        not isinstance(_s2_result, dict)
+                        or _s2_result.get("status") not in ("completed", "success")
+                        or _s2_result.get("error")
                     )
                     if _s2_failed:
                         logger.warning(
@@ -762,15 +767,17 @@ async def scheduler_loop():
                     _s3_result = await run_job_with_retry(
                         "fundamentals_sync",
                         lambda _db, _pid=_s2_run_id_for_s3, _cid=_s2_chain_run_id: run_fundamentals_changes_sync(
-                            _db, parent_run_id=_pid, chain_run_id=_cid
+                            _db, parent_run_id=_pid, chain_run_id=_cid,
+                            ignore_kill_switch=True,
                         ),
                         db,
                     )
-                    # Only advance last_run on success so the step retries
-                    # next tick on failure (matching Step 1 pattern).
+                    # Only advance last_run on explicit success so the step
+                    # retries next tick on failure/skip/cancel.
                     _s3_failed = (
-                        isinstance(_s3_result, dict)
-                        and (_s3_result.get("error") or _s3_result.get("status") == "failed")
+                        not isinstance(_s3_result, dict)
+                        or _s3_result.get("status") not in ("completed", "success")
+                        or _s3_result.get("error")
                     )
                     if _s3_failed:
                         logger.warning(
```
