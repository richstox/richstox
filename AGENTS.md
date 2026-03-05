# AGENTS.md

## Cursor Cloud specific instructions

### Communication & Language
- Communicate with the user (Richard) in **Czech**
- All application code, UI strings, comments, and commit messages must be in **English**
- The app UI must be fully English — no Czech strings in the frontend

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
