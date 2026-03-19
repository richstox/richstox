# RICHSTOX — Operations Guide

## 1. Local Development

### Prerequisites
- Python 3.12, Node.js 18+, Yarn, MongoDB 7.0

### Backend
```bash
# Start MongoDB (first time or after restart)
mongod --fork --logpath /var/log/mongod.log --dbpath /data/db

# Install dependencies
cd backend && pip install -r requirements.txt

# Minimum .env (create backend/.env)
MONGO_URL=mongodb://localhost:27017
DB_NAME=richstox_dev
ENV=development
DEV_LOGIN_ENABLED=1          # enables POST /api/auth/dev-login (bypasses Google OAuth)

# Start
uvicorn server:app --host 0.0.0.0 --port 8000
```

### Frontend
```bash
# Install dependencies
cd frontend && yarn install

# Minimum .env (create frontend/.env)
EXPO_PUBLIC_BACKEND_URL=http://localhost:8000

# Start
BROWSER=none yarn web        # serves on http://localhost:8081
```

### Lint & Tests
```bash
# Backend lint
cd backend && ~/.local/bin/ruff check . --ignore E501

# Backend tests
cd backend && pytest

# Frontend lint
cd frontend && yarn lint
```

---

## 2. Deploy Targets

| Component | Platform  | Trigger           |
|-----------|-----------|-------------------|
| Backend   | Railway   | Push to `main`    |
| Frontend  | Netlify   | Push to `main`    |

### Railway (Backend)
- Dashboard: <https://railway.app> → project **richstox**
- Manual redeploy: **Deployments** → **Redeploy** on latest build, or push any commit to `main`.
- Required env vars (set in Railway service settings): `MONGO_URL`, `DB_NAME=richstox_prod`, `ENV=production`, `EODHD_API_KEY`, `GOOGLE_CLIENT_ID`, `GOOGLE_CLIENT_SECRET`, `JWT_SECRET`.

### Netlify (Frontend)
- Dashboard: <https://app.netlify.com> → site **richstox**
- Manual redeploy: **Deploys** → **Trigger deploy** → **Deploy site**.
- Required env var: `EXPO_PUBLIC_BACKEND_URL=https://<railway-backend-url>`.

---

## 3. Pipeline Usage

The data pipeline runs nightly (cron) and can be triggered manually from the Admin panel (`/admin` → Pipeline).

### Prefer "Run full"
- **Run full** (`POST /api/admin/pipeline/run-full-now`) executes Steps 1–3 end-to-end in a single audited run.
- Use this for any production data refresh. A `run_id` ties all steps together for auditability.

### Per-step runs (Advanced / debug only)
- Steps 1–3 can be triggered individually via the "Advanced" section in the Admin pipeline panel.
- Intended for targeted debugging — **not** for production refreshes.
- If a full chain is already running, per-step endpoints return **HTTP 409** (busy).

### Concurrency guard
- Only one full-chain run can execute at a time; a sentinel in `ops_job_runs` prevents duplicates.
- Check the current status via `GET /api/admin/pipeline/status`.

### Audit export
- `GET /api/admin/pipeline/export/full` → CSV (`ticker,name,status,failed_step,reason_code,reason_text`) for the latest completed full run.
- Each row is traceable back to the same `run_id`.

---

## 4. Logs & Primary Admin Endpoints

### Logs
| Source            | Where                                                   |
|-------------------|---------------------------------------------------------|
| Backend runtime   | Railway → Deployments → active deploy → **View logs**  |
| Frontend build    | Netlify → Deploys → active deploy → **Deploy log**     |
| Pipeline runs     | `ops_job_runs` collection (also shown in Admin panel)   |

### Key admin endpoints
| Endpoint                                      | Purpose                              |
|-----------------------------------------------|--------------------------------------|
| `GET  /api/admin/pipeline/status`             | Current pipeline state               |
| `POST /api/admin/pipeline/run-full-now`       | Trigger full pipeline run            |
| `GET  /api/admin/pipeline/export/full`        | Unified audit CSV (full run)         |
| `GET  /api/admin/ops-jobs`                    | Recent job run history               |
| `GET  /api/admin/universe/counts`             | Universe size per step               |

All admin endpoints require an authenticated admin session (`kurtarichard@gmail.com`).

---

## 5. Further Reading

| Document                | Purpose                                             |
|-------------------------|-----------------------------------------------------|
| `DB_SCHEMA.md`          | Canonical collection contracts and field rules      |
| `backend/tests/`        | Backend test suite (`pytest`)                       |
| `docs/TICKER_PIPELINE.md` | Step-by-step pipeline internals                   |
| `docs/SCHEDULER_JOBS.md`  | Scheduled job catalogue and cron schedule          |
