# FORK RULES — BINDING FOR ALL AGENTS

**Version:** 1.1  
**Effective:** Immediately  
**Scope:** All fork instances, all agents

---

## Rule 0: ALWAYS PROVIDE TEST URL

**MANDATORY:** Every report/completion message MUST include the test URL:

```
https://ticker-detail-v2.preview.emergentagent.com
```

No exceptions. User must always know where to test.

---

## Rule 1: ONE PROBLEM AT A TIME

- Only ONE issue is approved per fork session
- No additional tasks, no "improvements", no opportunistic fixes
- If you touch anything outside the approved scope = **VIOLATION**

---

## Rule 2: NO UNAPPROVED CHANGES / NO ROADMAPS

- Do NOT ask "Should I start?"
- Do NOT propose a roadmap or future plans
- Do the single approved task, report facts, then wait

---

## Rule 3: DATA INTEGRITY — NO EXTERNAL API CALLS IN FRONTEND

- **Frontend MUST NEVER call EODHD (or any external provider) directly**
- All external calls must be backend scheduler jobs only
- All UI must read from backend endpoints backed by DB

### Audit Requirement
Before any work, grep `/app/frontend` for:
- `eodhd`
- `api_token`
- `fundamentals` (direct provider URLs)
- `news` (direct provider URLs)

Report "0 matches" or exact file+line if found.

---

## Rule 4: NO LIVE PROVIDER CALLS IN USER-FACING ENDPOINTS

- User-facing backend endpoints (routes) MUST NOT call external APIs
- All data must come from MongoDB collections
- External API calls are allowed ONLY in:
  - `/app/backend/scheduler.py`
  - `/app/backend/jobs/*.py`
  - `/app/backend/*_service.py` (marked with `# SCHEDULER-ONLY SERVICE`)

---

## Rule 5: DEFINITION OF "DONE"

A task is "done" only when ALL of these are true:
1. DB is updated (verified with count_documents, max(date), sample records)
2. Backend endpoint returns the correct data
3. UI displays the data correctly
4. Tests pass (if applicable)

---

## Rule 6: FACTS ONLY IN REPORTS

All reports must include:
- File paths
- Line numbers
- Counts
- max(date) values
- Sample records

No speculation, no assumptions.

---

## Rule 7: ACKNOWLEDGEMENT REQUIRED

Every agent must reply with:
```
"I have read and will comply with /app/docs/FORK_RULES.md. I will work ONLY on [APPROVED_TASK]."
```

No other text in that message.

---

## Approved Files for External Calls

| File | Purpose |
|------|---------|
| `/app/backend/scheduler.py` | Scheduler daemon |
| `/app/backend/jobs/news_daily_refresh.py` | News refresh job |
| `/app/backend/whitelist_service.py` | Universe seed |
| `/app/backend/price_ingestion_service.py` | Price sync |
| `/app/backend/backfill_fundamentals_raw.py` | Fundamentals backfill |
| `/app/backend/benchmark_service.py` | S&P 500 benchmark |

---

*Any deviation requires explicit written approval from Richard (kurtarichard@gmail.com).*
