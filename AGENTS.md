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

### Pipeline steps (canonical definition)

The Universe Pipeline has sequential steps. Each step runs ONLY after the previous completes successfully.

**Step 1 — Universe Seed** (23:00 Prague, Mon-Sat)
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

**Steps 3-5** — Fundamentals, Visible Universe, Peer Medians (each after previous completes)

**Report**: ONE file across ALL steps. Each row: Ticker | Name | Step | Reason for exclusion. Admin can download/view this report.

**CRITICAL RULES**:
- Frontend NEVER calls EODHD — all data comes from scheduled jobs → MongoDB
- EODHD provides raw data only — all derived metrics (P/E, market cap, margins) computed by backend
- Step timing: NYSE closes 22:00 Prague, EODHD processes data, 23:00 Prague is the correct start time

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

- **Backend**: `pytest` (run from `backend/`). No test files exist at this time.
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
