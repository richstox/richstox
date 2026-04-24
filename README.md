# RICHSTOX

Stock research platform — backend (FastAPI/Python) + frontend (Expo/React Native Web).

## Runtime Data Rule (Binding)

- Frontend MUST NEVER call external providers directly.
- User-facing backend endpoints MUST NEVER call external providers directly.
- All third-party/provider data must be ingested first, stored in MongoDB, and only then served through internal backend endpoints.
- If data is not already in MongoDB, fix the ingestion pipeline/admin backfill — do not add a live provider call to frontend runtime or user-facing API routes.

## Quick Start

See **[docs/OPERATIONS.md](docs/OPERATIONS.md)** for local dev setup, deploy targets, pipeline usage, and log locations.

## Where to Read Next

| Document | Purpose |
|---|---|
| [docs/OPERATIONS.md](docs/OPERATIONS.md) | Local dev, deploy, pipeline, logs |
| [DB_SCHEMA.md](DB_SCHEMA.md) | Canonical DB collections and field contracts |
| [docs/TICKER_PIPELINE.md](docs/TICKER_PIPELINE.md) | Pipeline step internals |
| [docs/SCHEDULER_JOBS.md](docs/SCHEDULER_JOBS.md) | Scheduled job catalogue |
| `backend/tests/` | Backend test suite (`pytest`) |
