# STEP 1 FORENSIC INVESTIGATION — WHY DID UNIVERSE SEED NEVER RUN?

## 1) Executive Summary

**UNPROVEN.** I cannot access the production database. However, code analysis reveals
exactly **6 possible failure modes** for Step 1, all of which can be confirmed or eliminated
with the exact DB queries listed in section 4. The most likely root cause is one of:
**(A) kill switch engaged**, **(B) scheduler process never started**, or **(C) both**.

## 2) Failure Mode for Step 1 at 03:00 (e.g., 2026-03-30): **UNPROVEN**

Cannot determine without production DB access. The 6 candidates are:

| # | Mode | What proves it | Likelihood |
|---|------|---------------|------------|
| A | **kill switch engaged** | `ops_config.scheduler_enabled.value == false` | HIGH — silent, no DB evidence before this PR |
| B | **scheduler process never started** | Zero heartbeat records in `system_job_logs` ever | HIGH — no Procfile/render.yaml/docker-compose in repo |
| C | **A+B combined** | Both queries empty | HIGH |
| D | blocked by last_run | `ops_config.scheduler_last_run.value.universe_seed == "2026-03-30"` already | LOW |
| E | fired+failed | `ops_job_runs.{job_name:"universe_seed", status:"failed"}` exists | LOW — would have logs |
| F | timezone/day mismatch | heartbeats exist but Step 1 decision shows `weekday==6` (Sunday) | VERY LOW — code is correct |

## 3) Exact Code Path (files + line ranges)

### 3a. Scheduler daemon entry point
- **`scheduler.py:1019-1026`** — `main()` calls `asyncio.run(scheduler_loop())`
- **`scheduler.py:1025`** — `if __name__ == "__main__": main()`
- ⚠️ **CRITICAL**: This is a standalone daemon. `server.py` does NOT import or start it.
  There is **no Procfile, no render.yaml, no docker-compose, no systemd service, no cron entry**
  in this repository that would start `python scheduler.py` as a separate process.

### 3b. Scheduler loop — each tick (every 60s)
- **`scheduler.py:588-591`** — `now = get_prague_time()`, compute `today_str`, `current_hour`, `current_minute`

### 3c. now/tz computation
- **`scheduler.py:159-161`** — `get_prague_time()` → `datetime.now(ZoneInfo("Europe/Prague"))`
- Correct — uses `ZoneInfo` which handles DST properly.

### 3d. Kill switch check (BEFORE any job evaluation)
- **`scheduler.py:600`** — `scheduler_enabled = await get_scheduler_enabled(db)` — reads `ops_config.scheduler_enabled`
- **`scheduler.py:616-619`** — if `not scheduler_enabled`: `logger.warning(...)`, `sleep(60)`, `continue`
- ⚠️ Before this PR, line 617 was `logger.debug(...)` — invisible at INFO level.
  **No database record** was written when kill switch blocked. Zero evidence.

### 3e. Sunday exclusion
- **`scheduler.py:631-643`** — if `weekday == 6` (Sunday): only news runs, then `continue`
- Step 1 only reaches evaluation on Mon-Sat (weekday 0-5).

### 3f. should_run for universe_seed
- **`scheduler.py:656`** — `_step1_should = should_run("universe_seed", 3, 0, last_run, today_str, hour, minute)`
- **`scheduler.py:547-561`** — `should_run()` logic:
  1. If `last_run.get("universe_seed") == today_str` → `False` (already ran)
  2. If `current_hour > 3` → `True` (catch-up)
  3. If `current_hour == 3 and current_minute >= 0` → `True` (scheduled time)
  4. Otherwise → `False`

### 3g. Where ops_job_runs for universe_seed is created (or not)
- **`scheduler.py:692-708`** — if `_step1_should` is True:
  - Calls `_run_universe_seed_scheduled(db)` at line 695
  - **`scheduler.py:361-374`** — `_run_universe_seed_scheduled()` inserts an `ops_job_runs` document
    with `job_name: "universe_seed"`, `status: "running"` BEFORE calling `sync_ticker_whitelist`
  - If should_run returns False — **no document is ever created**. This is the "never fired" case.

### 3h. Lock/in_progress check
- **NONE.** Step 1 has no lock. If `should_run()` returns True, it fires immediately.
  There is no check for a stuck "running" document blocking the next run.

## 4) Exact DB Evidence Required

I cannot access the production database. Run these queries in MongoDB to determine the exact failure mode.

### Query 1: Is the scheduler process alive? (heartbeats)
```js
// If this returns 0, the scheduler process was NEVER RUNNING.
db.system_job_logs.countDocuments({
  job_name: "scheduler_heartbeat"
})

// For a specific date range (03:00 window on 2026-03-30):
db.system_job_logs.find({
  job_name: "scheduler_heartbeat",
  start_time: {
    $gte: ISODate("2026-03-30T01:00:00Z"),
    $lte: ISODate("2026-03-30T03:00:00Z")
  }
}).sort({start_time: 1})
```
- **0 documents ever** → Failure mode B: scheduler process never started
- **Documents exist but gap at 03:00** → process crashed/restarted
- **Documents exist at 03:00** → process was alive, check kill switch

### Query 2: Was the kill switch engaged?
```js
db.ops_config.findOne({key: "scheduler_enabled"})
```
- `value: false` → **Failure mode A: kill switch engaged** (all jobs blocked)
- `value: true` or document missing → kill switch was NOT the blocker

### Query 3: Did Step 1 ever run? (any date)
```js
db.ops_job_runs.find({
  job_name: "universe_seed"
}).sort({started_at: -1}).limit(5)
```
- **0 documents** → Step 1 has NEVER fired from the scheduler (mode A, B, or C)
- Documents with `source: "scheduled"` → it did fire at least once
- Documents with only `source: "manual"` → only admin-button runs, scheduler never triggered

### Query 4: What does the scheduler think already ran today?
```js
db.ops_config.findOne({key: "scheduler_last_run"})
```
- If `value.universe_seed == "2026-03-30"` → Step 1 was already marked as "ran today" (mode D)
- If `value.universe_seed` is absent or an old date → scheduler hasn't run Step 1 recently

### Query 5: Step 1 decision log (only exists after this PR deploys)
```js
db.system_job_logs.find({
  job_name: "scheduler_step1_decision",
  "details.today_str": "2026-03-30"
})
```
- If present: shows `details.decision` (true/false) and `details.reason` explaining exactly why

### Query 6: Pipeline chain runs (did a full pipeline ever complete?)
```js
db.pipeline_chain_runs.find({
  source: "scheduled"
}).sort({started_at: -1}).limit(5)
```
- **0 documents** → no scheduled pipeline has ever started (confirms mode B or C)

## 5) Most Likely Root Cause (from code analysis alone)

### ⚠️ CRITICAL FINDING: No deployment configuration for scheduler process

The scheduler daemon is `scheduler.py` with `if __name__ == "__main__": main()`.
It requires a **separate process** to be started (`python scheduler.py`).

The web server (`server.py`) is a FastAPI app served by uvicorn. Its startup event
(`server.py:8522-8542`) does NOT import, start, or create_task for the scheduler.

**There is NO file in this repository that starts the scheduler process:**
- No `Procfile` (Heroku/Render worker definition)
- No `render.yaml` (Render background worker)
- No `docker-compose.yml` (Docker worker service)
- No `Dockerfile` (container build)
- No `fly.toml` (Fly.io process)
- No `railway.toml` (Railway worker)
- No `supervisord.conf` (process manager)
- No `systemd` service file
- No cron entry
- No shell script that runs `python scheduler.py`

**Zero files in the entire repository import `from scheduler import` or `import scheduler`**
(excluding test files).

This means the scheduler daemon exists as dead code — perfectly written, tested, and
audited, but **never actually started as a process** in any deployment.

### What would fix it

The hosting platform needs a **worker process** definition. Example for Render:

```yaml
# render.yaml (does not exist in repo)
services:
  - type: worker
    name: scheduler
    env: python
    buildCommand: pip install -r backend/requirements.txt
    startCommand: cd backend && python scheduler.py
```

Or if using a single dyno with process management, server.py needs to launch the
scheduler as an asyncio background task in its startup event.

## Prior fixes (already in this branch)

### Steps 2/3 kill switch bypass (already applied)
- `scheduler.py:749` — Step 2 now passes `ignore_kill_switch=True`
- `scheduler.py:821` — Step 3 now passes `ignore_kill_switch=True`
- `scheduler.py:758-762` — `_s2_failed` uses whitelist: `status not in ("completed","success")`
- `scheduler.py:827-831` — `_s3_failed` same pattern

### Step 1 observability (already applied)
- `scheduler.py:617` — Kill switch log changed from `logger.debug` to `logger.warning`
- `scheduler.py:531-545` — Heartbeat now includes `kill_switch_engaged` in details
- `scheduler.py:652-690` — Step 1 decision logged to `system_job_logs` once per day
