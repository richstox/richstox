# Scheduler Investigation: AUTO Pipeline Did Not Run at 3 AM

## 1) Executive Summary

The binding specification (`SCHEDULER_JOBS.md`) is out of sync with the actual scheduler
code in three critical ways: (a) it documents Universe Seed as **Sunday-only** while the
code runs it **Mon-Sat**, (b) it documents `backfill_gaps` at 04:45 but no implementation
exists in `scheduler.py`, and (c) the comment `# SUNDAY ONLY - Universe seed` on the
`UNIVERSE_SEED_DAY` constant is misleading. Together these cause the audit script to fail,
the admin panel to show a phantom "overdue" `backfill_gaps` job every day, and operators
to misinterpret the actual schedule.

## 2) Exact Root Cause

**Spec-vs-code mismatch in three places:**

| What the spec says | What the code does | Impact |
|---|---|---|
| Universe Seed runs **Sunday** 03:00 | Runs **Mon-Sat** 03:00 (line 642) | Operator expects Sunday run; it never fires on Sunday |
| `backfill_gaps` runs at 04:45 daily | **Not implemented** in scheduler loop | Admin panel marks it "overdue" every day after 04:45; health score degrades |
| `BACKFILL_HOUR=4, BACKFILL_MINUTE=45` | Constants **do not exist** | `audit_scheduler.py` reports 2 ERRORS, always fails CI |

**Secondary:** `PRICE_SYNC_HOUR/MINUTE` and `FUNDAMENTALS_SYNC_HOUR/MINUTE` are defined
but never used - both jobs use dependency-based scheduling (`should_run_after_dependency`),
not time-based scheduling.

## 3) Exact Files and Lines Involved

| File | Lines | Issue |
|---|---|---|
| `backend/scheduler.py` | 108 | Comment `# SUNDAY ONLY` is misleading; `UNIVERSE_SEED_DAY=6` is the Sunday **exclusion** day |
| `backend/scheduler.py` | 622-632 | Sunday branch: skips Universe Seed, only runs news at 13:00 |
| `backend/scheduler.py` | 641-676 | Step 1 Universe Seed actually runs **Mon-Sat** at 03:00 |
| `backend/scheduler.py` | 116-120 | `PRICE_SYNC_HOUR=4`, `FUNDAMENTALS_SYNC_HOUR=4` - dead code (dependency-based) |
| `docs/SCHEDULER_JOBS.md` | 10 | Universe Seed listed as "Sunday" - should be "Mon-Sat" |
| `docs/SCHEDULER_JOBS.md` | 15 | `backfill_gaps` at 04:45 - not implemented in scheduler |
| `docs/SCHEDULER_JOBS.md` | 52 | Section title says "Sunday 03:00" - should be Mon-Sat |
| `scripts/audit_scheduler.py` | 19 | `universe_seed` day listed as "Sunday" - should be "Mon-Sat" |
| `scripts/audit_scheduler.py` | 38-39 | Expects `BACKFILL_HOUR/MINUTE` that do not exist |
| `backend/services/admin_overview_service.py` | 368-371 | `backfill_gaps` in JOB_REGISTRY at 04:45 - scheduler never runs it |

## 4) Problem Classification

**Scheduling configuration** - the spec, audit, and admin registry are out of sync with
the actual scheduler implementation.

## 5) DST / Timezone Verification

Verified with Python `ZoneInfo("Europe/Prague")`:
- DST spring-forward (2026-03-29 Sunday): 2:00 AM CET jumps to 3:00 AM CEST
- `get_prague_time()` correctly returns CEST after transition
- `should_run()` uses integer hour/minute comparison - unaffected by DST
- `last_run` date strings use Prague-local dates - no UTC rollover bugs
- DST always falls on Sunday in EU - excluded from Mon-Sat pipeline anyway

**DST is NOT the root cause.**

## 6) Remediation Applied

1. Fixed misleading comment in `scheduler.py` line 108
2. Updated `SCHEDULER_JOBS.md` to match actual Mon-Sat Universe Seed schedule
3. Updated `audit_scheduler.py` to match actual code (Universe Seed day, backfill constants)
4. Removed phantom `backfill_gaps` from admin JOB_REGISTRY (no scheduler implementation exists)
