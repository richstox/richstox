# Executive summary
This onboarding is a **hard guardrail spec** for any new agent working on RICHSTOX after a fork. It defines **what the agent is allowed to do, what it must never do, and the exact workflow** for proposing and implementing changes—so nobody improvises, creates new databases, or "quick-fixes" around the canonical pipeline.

# 1) Mission and scope (what the agent is here to do)
- Maintain and improve RICHSTOX **without breaking data integrity, auditability, or canonical calculations**.
- Prefer **correct, auditable, deterministic** behavior over speed.
- Work **one problem at a time**. Do not start a new task until the current one is closed/approved.

# 2) Non-negotiable rules (must follow)
## 2.1 Code-change approval gate
- **No code changes without explicit approval from Richard.**
- Workflow:
  1. Audit current state (files, line ranges, current behavior).
  2. Propose a diff (exact files + line ranges + pseudocode or patch).
  3. Wait for Richard's approval.
  4. Only then implement.
- Every DEV prompt must start with: **"DEV AI nesmí měnit kód bez schválení."**

## 2.2 One-pass prompt discipline
- Never send incremental addendums to DEV AI/agents.
- Every prompt must be **complete, self-contained**, including edge cases and acceptance criteria.

## 2.3 One-problem-at-a-time
- Do not mix tasks.
- If you discover a second issue, log it as "next" and stop.

## 2.4 Raw-facts-only (no provider-computed metrics)
- **Do not store or treat provider ratios as truth** (P/E, PEG, margins, etc.).
- Store **raw facts** (prices, dividends, statements) and compute metrics locally.

## 2.5 Canonical pipeline only (no ad-hoc medians)
- Peer medians must come from **`compute_peer_benchmarks_v3` → `peer_benchmarks`**.
- **No on-the-fly aggregation queries** for medians in API routes.
- Frontend/backend must read the same canonical benchmark fields.

## 2.6 No new databases / no schema freelancing
- Do **not** create new databases, new collections, or parallel schemas to "make it work".
- Any schema change must be:
  - Minimal
  - Backward-aware
  - Approved
  - Added to the audit/verification plan

## 2.7 Deterministic fallbacks (never "peer set unavailable")
- Always use the canonical fallback chain:
  - **Industry → Sector → Market**
- Do not introduce new fallback logic without approval.
- The string **"Peer set unavailable"** is banned.

## 2.8 Guardrails for math and data quality
- Use the canonical helpers (e.g., `safe_divide` guardrails) and status codes.
- Never hide bad data by "clamping" or silently substituting values.

# 3) Required knowledge (what the agent must understand)
## 3.1 System architecture (minimum)
- Backend calculates metrics from raw data.
- Canonical peer medians are computed by `compute_peer_benchmarks_v3` and stored in `peer_benchmarks`.
- API reads precomputed values; it should not recompute medians.

## 3.2 Ticker universe rules
- Visible universe is produced by the canonical sieve:
  1. NYSE/NASDAQ common stock
  2. Must have price data
  3. Sector + industry present
  4. Not delisted
  5. **shares_outstanding is not null/missing**
  6. **financial_currency is not null/missing**
- Exclude patterns (warrants/units/preferred/rights) are canonical and approved.
- Whitelist-only UX: unsupported tickers must not appear.

## 3.3 Currency rule for peer medians
- Peer medians are currently **USD-only** until FX normalization is implemented.

## 3.4 Observability and audit expectations
- Every change must be verifiable via:
  - Admin panel / job runs
  - A reproducible test plan
  - Raw JSON samples for specific tickers

# 4) Mandatory workflow template (agent must follow)
## 4.1 For any task
1. **Restate the single task** in one sentence.
2. **Audit**: where it lives (files + line ranges), current behavior, why it's wrong.
3. **Proposed diff**: exact changes, minimal surface area.
4. **Test plan**: how to prove correctness (jobs to run, tickers to check, expected outputs).
5. **Wait for approval**.

## 4.2 Definition of done (DoD)
- Canonical source used (no ad-hoc queries).
- Fallback chain implemented.
- Edge cases handled.
- Raw JSON proof provided for at least 2 representative tickers.
- No new collections/databases created.

# 5) "Fork banner" — put this into code (must be hard to ignore)
Add a short, unavoidable banner comment at the top of key canonical files (e.g., `server.py`, `key_metrics_service.py`, scheduler entrypoint):
- "No code changes without Richard approval"
- "Canonical peer medians only"
- "Raw facts only"
- "One problem at a time"

Suggested snippet (comment only; exact placement approved separately):
- **DEV AI nesmí měnit kód bez schválení (Richard).**
- **Do not add new DB/collections without approval.**
- **Do not compute peer medians on-the-fly; use peer_benchmarks only.**
- **One problem at a time.**

# 6) Immediate next task (current open item)
## Dual dividend medians (backend)
Goal: avoid the "0.0% median trap" by storing and returning both:
- `dividend_yield_median_all` (includes zeros)
- `dividend_yield_median_payers` (values > 0 only, requires ≥5 payers)
Plus counts and deterministic fallback.

Acceptance proof:
- Provide raw JSON for AAPL and COTY showing both medians, counts, and which fallback level was used.
