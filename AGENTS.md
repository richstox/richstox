# AGENTS.md

## Cursor Cloud specific instructions

### Communication & Language
- Communicate with the user (Richard) in **Czech**
- All application code, UI strings, comments, and commit messages must be in **English**
- The app UI must be fully English — no Czech strings in the frontend

### Core rules
- **Frontend NEVER calls EODHD API** — all EODHD data is fetched by scheduled backend jobs, stored in MongoDB, and served from there
- **Raw facts only** — EODHD provides raw data (prices, dividends, statements). All derived metrics (market cap, P/E, margins, etc.) are computed locally by the backend
- **Canonical pipeline only** — peer medians come from `compute_peer_benchmarks_v3` → `peer_benchmarks` collection, never computed on-the-fly in API routes

### Admin approval authority (top priority)
- **Final decision authority is Admin Richard** (`kurtarichard@gmail.com`).
- **Any logic, architecture, workflow, or product decision MUST be explicitly approved by Richard before implementation.**
- Agents must not invent or enforce their own policy decisions without Richard's approval.
- If requirements are ambiguous, agents must ask Richard for approval/clarification before proceeding.

### Working protocol (MANDATORY — top priority)

**ZERO TRUST & PROPOSE BEFORE CODING**
- The AI must NEVER execute large code changes, DB migrations, or pipeline logic without explicitly proposing the architecture, queries, or schema changes FIRST and waiting for Richard's explicit "GO".
- Every code change MUST be proposed to Richard — describe WHAT will change, WHY, and WHICH files are affected.
- Richard will review the proposal (optionally with a Dev AI reviewer) and explicitly approve or reject.
- Only after Richard's explicit approval may the agent implement and commit.
- This applies to ALL changes — bug fixes, refactors, new features, config changes, migrations.
- The only exception is trivial formatting fixes (whitespace, typos in comments) that do not affect behavior.

**NO SILENT ASSUMPTIONS**
- If a requirement is ambiguous, STOP and ASK. Do not guess, infer, or make architectural decisions unilaterally.
- If the agent is unsure about scope, impact, or intent — ask Richard for clarification before writing any code.

**ONE TASK AT A TIME**
- Do not open or attempt to solve a second problem until the first one is fully resolved, tested, and explicitly closed by Richard.
- Each task follows the cycle: Propose → Approve → Implement → Test → Richard confirms done.

### UI copy policy

- **Do NOT change UI copy (labels, descriptions, strings) unless Richard explicitly requests it.**
- If a backend logic change affects what the UI displays (e.g. new fields, renamed statuses, changed counts), flag the impact to Richard and wait for explicit GO before updating frontend copy.
- No emojis in UI run-result strings — use plain text (e.g. "Cancelled", "Error", "Running").

### Step 3/Step 4 canonical definitions and governance

**Canonical Step 3 universe — STEP3_QUERY (single source of truth)**
```python
SEED_QUERY  = {"exchange": {"$in": ["NYSE", "NASDAQ"]}, "asset_type": "Common Stock"}
STEP3_QUERY = {**SEED_QUERY, "has_price_data": True}
```
- Defined in `backend/scheduler_service.py`. Also matches `step3_query` in `universe_counts_service.py`.
- **Never invent a new filter for Step 3 universe.** Always reuse `STEP3_QUERY`.

**Step 3 is event-driven — HARD RULE**
- `run_fundamentals_changes_sync` processes from `fundamentals_events` queue. It must NOT iterate all tickers.
- Progress denominator = event batch size, not full universe.
- `universe_total` goes in `ops_job_runs.details` as informational only.

**Canonical Step 4 universe — get_canonical_sieve_query()**
- Defined in `backend/visibility_rules.py`. Used by `recompute_visibility_all`.
- Gate 7 (shares_outstanding) reads the flat field `tracked_tickers.shares_outstanding`, NOT the nested `fundamentals` sub-doc.

**ops_job_runs timestamps must use Prague timezone**
- All `ops_job_runs` documents must include `started_at_prague` and `finished_at_prague` fields (ISO format, Europe/Prague).
- Use `_to_prague_iso()` helper from `scheduler_service.py`.

### Pipeline steps (canonical definition)

The Universe Pipeline has sequential steps. Each step runs ONLY after the previous completes successfully.

**Step 1 — Universe Seed**
- Call EODHD API: get all symbols from NYSE
- Call EODHD API: get all symbols from NASDAQ
- Count TOTAL RAW (both exchanges combined)
- Filter locally (NO additional API calls):
  - Remove empty codes
  - Remove codes containing dots (ADRs, preferred with dot notation)
  - Remove exclude patterns (warrants -WT/-WS, units -U/-UN, preferred -PA/-PB/etc, rights -R/-RI)
  - Remove Type != "Common Stock"
- Result = SEEDED tickers
- Log every filtered-out ticker to a report: Ticker, Name, Step, Reason
- Frontend pipeline view must show: RAW count → SEEDED count → filtered out count (same visual style as Step 2)

**Step 2 — Price Sync** (after Step 1 completes)
- Fetch daily prices from EODHD for all seeded tickers
- Mark `has_price_data = true` for tickers with price data
- Tickers without price data are filtered out (logged to report)

**Steps 3-5** — Fundamentals, Visible Universe, Peer Medians (each after previous completes).
See the **Step 3/Step 4 canonical definitions** section above for verified details.

**Report**: ONE file across ALL steps. Each row: Ticker | Name | Step | Reason for exclusion. Admin can download/view this report.

**CRITICAL RULES**:
- Frontend NEVER calls EODHD — all data comes from scheduled jobs → MongoDB
- EODHD provides raw data only — all derived metrics (P/E, market cap, margins) computed by backend

### Architecture

RICHSTOX is a two-component application (not a monorepo):
- **Backend** (`/workspace/backend`): FastAPI (Python 3.12) served by uvicorn on port 8000
- **Frontend** (`/workspace/frontend`): Expo 54 / React Native Web served on port 8081

### Running services

**MongoDB** must be running before the backend can start. In the cloud environment, MongoDB 7.0 is installed locally:
```
mongod --fork --logpath /var/log/mongod.log --dbpath /data/db
```

**Backend**: `cd backend && uvicorn server:app --host 0.0.0.0 --port 8000`
- Requires `backend/.env` with at minimum: `MONGO_URL=mongodb://localhost:27017`, `DB_NAME=richstox_dev`, `ENV=development`
- Set `DEV_LOGIN_ENABLED=1` to enable `POST /api/auth/dev-login` (bypasses Google OAuth)
- The `config.py` module enforces an ENV/DB_NAME safety guard: dev environments must NOT reference a DB name containing "prod"

**Frontend**: `cd frontend && BROWSER=none yarn web`
- Requires `frontend/.env` with `EXPO_PUBLIC_BACKEND_URL=http://localhost:8000`

### Linting

- **Backend**: `ruff check . --ignore E501` (run from `backend/`). Note: `ruff` is installed in `~/.local/bin`; ensure PATH includes it.
- **Frontend**: `yarn lint` or `npx expo lint` (run from `frontend/`)
- The `Makefile` at the repo root has a `ci` target, but it references `/app/` paths (production). For local dev, run lint commands directly from each directory.

### Testing

- **Backend**: `pytest` (run from `backend/`). Existing test suites:
  - `tests/test_admin_auth_middleware.py` — 4 tests (admin middleware: unauth, forbidden, happy, bootstrap guard)
  - `tests/test_user_auth_guard.py` — 6 tests (user middleware: portfolio unauth/happy/IDOR, watchlist unauth/IDOR/happy)
  - `tests/test_provider_debug_snapshot.py` — provider debug snapshot tests
- **Frontend**: `yarn test` or `jest` (run from `frontend/`). No test files exist at this time.

### Gotchas

- The backend runs in MOCK mode when no real EODHD API key is configured — API responses use mock/demo data, which is sufficient for UI development and basic testing.
- `yarn.lock` is the lockfile for the frontend; always use `yarn install` (not npm).
- A `package-lock.json` may also exist in `frontend/` — yarn will emit a warning about it; this is harmless.
- The `Makefile` at the repo root uses `/app/` paths (Railway production paths), not `/workspace/`. For local development, run commands directly.
- `~/.local/bin` must be on PATH for `ruff`, `uvicorn`, and other pip-installed CLI tools.

### Git workflow policy (top priority)

Use **trunk-based development** as the default and canonical workflow.

- `main` is the single source of truth and the only long-lived branch.
- Do **not** create a new `cursor/*` branch unless Richard explicitly requests branch isolation for a risky or parallel task.
- Follow-up tasks should continue from the current active branch; after merge, consolidate back to `main`.
- Short-lived task branches are allowed only for isolated experiments and must be deleted immediately after merge.
- Never leave stale remote branches behind.
- Before committing, always verify branch/tracking with:
  - `git status --branch`
  - `git branch -vv`
