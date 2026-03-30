# Scheduler Investigation: AUTO Pipeline Did Not Run at 3 AM

## 1) Executive Summary

Steps 2 (price_sync) and 3 (fundamentals_sync) are called from the scheduler
**without `ignore_kill_switch=True`**, while the manual endpoint passes it.
Combined with a success-detection check that only catches `status=="failed"` and
ignores `"skipped"`/`"cancelled"`, a mid-pipeline kill-switch toggle causes
Steps 2/3 to silently skip data fetching while the scheduler treats them as
successful — advancing `last_run` and creating price data gaps.

## 2) Exact Failure Mode

**UNPROVEN** for the 03:00 trigger itself (need DB state: `ops_config.scheduler_enabled`,
`ops_config.scheduler_last_run`, runtime logs).

**PROVEN** for gap creation: `fired + skipped` — Steps 2/3 can silently return
`{"status": "skipped"}` due to redundant internal kill-switch check, and the
scheduler treats this as success.

## 3) Exact Code Path

### Where scheduler loop/tick runs
- `scheduler.py:582-951` — `while True` loop, 60-second sleep per tick

### Where "now" and timezone are computed
- `scheduler.py:159-161` — `get_prague_time()` → `datetime.now(ZoneInfo("Europe/Prague"))`
- `scheduler.py:584-587` — `now = get_prague_time()`, `today_str`, `current_hour`, `current_minute`

### Where 03:00 eligibility is checked
- `scheduler.py:642` — `should_run("universe_seed", 3, 0, last_run, today_str, hour, minute)`
- `scheduler.py:543-557` — `should_run()`: returns False if `last_run[job] == today_str`,
  True if `hour > scheduled` or `hour == scheduled and minute >= scheduled`

### Where Step 1/2/3 auto-run decisions are made
- Step 1: `scheduler.py:642` — `should_run("universe_seed", 3, 0, ...)`
- Step 2: `scheduler.py:678` — `should_run_after_dependency("price_sync", "universe_seed", ...)`
- Step 3: `scheduler.py:753` — `should_run_after_dependency("fundamentals_sync", "price_sync", ...)`

### Where last_run is read and written
- Read: `scheduler.py:568` — `last_run = await get_last_run_state()` at startup
- Write: `scheduler.py:657` (Step 1), `scheduler.py:719` (Step 2), `scheduler.py:788` (Step 3)
- Persistence: `ops_config` collection, key `"scheduler_last_run"`, value is dict
  `{"universe_seed": "YYYY-MM-DD", "price_sync": "YYYY-MM-DD", ...}`

### Where lock/in_progress guards are checked
- Kill switch: `scheduler.py:603-608` — `get_scheduler_enabled(db)` → `ops_config.scheduler_enabled`
- **Redundant check (BUG):** `scheduler_service.py:1229` (Step 2) and `scheduler_service.py:2935` (Step 3)
  both check `get_scheduler_enabled(db)` internally when `ignore_kill_switch=False`
- No other lock/in_progress/mutex guards exist

### Where gaps/backfill eligibility is decided
- Gaps are created when `last_run["price_sync"]` is set (line 719) without actual data fetch
- This happens when Steps 2/3 return `{"status": "skipped"}` (kill-switch internal check)
  and the scheduler's `_s2_failed` check (line 708-712) does NOT catch it

## 4) Exact Persisted State Used in the Decision

| Collection | Key/Field | Meaning | Skip/Block Condition |
|---|---|---|---|
| `ops_config` | `scheduler_enabled.value` | Kill switch | `False` → all jobs skip at line 605 |
| `ops_config` | `scheduler_last_run.value.universe_seed` | Last Step 1 date | `== today_str` → Step 1 skips |
| `ops_config` | `scheduler_last_run.value.price_sync` | Last Step 2 date | `== today_str` → Step 2 skips |
| `ops_config` | `scheduler_last_run.value.fundamentals_sync` | Last Step 3 date | `== today_str` → Step 3 skips |
| `ops_config` | `job_backfill_all_enabled.value` | Backfill toggle | `False` → backfill_all skips |

## 5) Proof

**Proven code bug causing silent data gaps:**

The scheduler calls `run_daily_price_sync` at line 697 **without** `ignore_kill_switch=True`:
```python
# scheduler.py:695-701 (BEFORE fix)
lambda _db, ...: run_daily_price_sync(_db, parent_run_id=_pid, chain_run_id=_cid)
```

While the manual endpoint at `server.py:7480-7481` correctly passes it:
```python
s2_result = await run_daily_price_sync(db, ignore_kill_switch=True, ...)
```

`run_daily_price_sync` (scheduler_service.py:1229) returns `{"status": "skipped"}` when
the kill switch is engaged. The scheduler's failure detection (scheduler.py:708-712):
```python
# BEFORE fix — only catches "failed"
_s2_failed = (
    isinstance(_s2_result, dict)
    and (_s2_result.get("error") or _s2_result.get("status") == "failed")
)
```
does NOT catch `status == "skipped"`. Result: `_s2_failed = False`, `last_run["price_sync"]`
is set to today, Step 3 triggers, same bug repeats. Pipeline appears complete but no data
was fetched.

**For the 03:00 trigger itself:** UNPROVEN. The trigger logic (`should_run`, weekday
gating, DST handling) is code-correct. To prove the exact 03:00 failure requires:
- `ops_config.scheduler_enabled.value` at 03:00 on the failure date
- `ops_config.scheduler_last_run` state at 03:00
- Scheduler process uptime / heartbeat logs from `system_job_logs`

## 6) Minimal Fix

Two changes in `scheduler.py`:

1. **Pass `ignore_kill_switch=True`** to Steps 2/3 (matching manual endpoint pattern).
   The scheduler already verified the kill switch at line 603.

2. **Tighten `_s2_failed`/`_s3_failed` detection** to catch any non-success status
   (not just `"failed"`). Uses whitelist pattern: only `"completed"` or `"success"`
   advances `last_run`.

Files changed:
- `backend/scheduler.py` lines 695-712 (Step 2) and 767-781 (Step 3)

## 7) DST / Timezone Verification

Verified with Python `ZoneInfo("Europe/Prague")`:
- DST spring-forward (2026-03-29 Sunday): 2:00 AM CET jumps to 3:00 AM CEST
- `get_prague_time()` correctly returns CEST after transition
- `should_run()` uses integer hour/minute comparison — unaffected by DST
- `last_run` date strings use Prague-local dates — no UTC rollover bugs
- EU DST always falls on Sunday — excluded from Mon-Sat pipeline

**DST is NOT the root cause.**
