# Code Review Checklist - API Call Prevention

## LAYER 5: Code Review Rules

### BEFORE MERGE (MANDATORY)

#### 1. API Call Audit
- [ ] `python /app/scripts/audit_external_calls.py` = exit 0
- [ ] `make audit-api` = PASS

#### 2. Import Check
- [ ] New file does NOT import `httpx`, `requests`, `aiohttp`
- [ ] If YES -> file IS in ALLOWLIST (`/app/scripts/audit_external_calls.py`)
- [ ] If NO -> REJECT

#### 3. URL Pattern Check
- [ ] No new reference to `eodhd.com/api` outside scheduler/backfill
- [ ] If YES -> has BINDING comment explaining context
- [ ] If NO -> REJECT

#### 4. Runtime Endpoint Check
- [ ] New `@api_router` endpoint does NOT call external API
- [ ] Data is ONLY from MongoDB
- [ ] If endpoint needs fresh data -> returns "stale" warning, NOT API call

#### 5. Scheduler Integration Check
- [ ] New job is registered in `JOB_REGISTRY` (`admin_overview_service.py`)
- [ ] Job has `has_api_calls: True/False` correctly set
- [ ] Job logs `api_calls` to `ops_job_runs.details`

---

## REJECTION CRITERIA (AUTOMATIC REJECT)

1. Import `httpx` in file outside ALLOWLIST
2. Direct URL `eodhd.com` in runtime code
3. `async with httpx.AsyncClient` outside scheduler context
4. New endpoint without `# RUNTIME: DB-ONLY` comment

---

## ALLOWLIST FILES (may contain EODHD calls)

See `/app/scripts/audit_external_calls.py` for current list.

Current allowlist:
- `scheduler.py` - orchestrator
- `whitelist_service.py` - universe seed
- `price_ingestion_service.py` - price sync
- `parallel_batch_service.py` - batch backfill
- `dividend_history_service.py` - dividend sync
- `news_service.py` - news refresh
- `backfill_fundamentals_*.py` - fundamentals backfill
- `admin_overview_service.py` - UI display strings only
- `server.py` - EODHDService class + constants

---

## VERIFICATION COMMANDS

```bash
# API Call Audit
make audit-api

# Full Audit
make audit

# Check Admin Panel
curl -s https://ticker-detail-v2.preview.emergentagent.com/api/admin/overview | jq '.health.api_guard'
```
