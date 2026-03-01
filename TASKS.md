# RICHSTOX — Task Registry (Canonical, 2026-03-01)

## Overview
This is the **single source of truth** for all open, in-progress, and completed work on RICHSTOX. **This is a living document and may be updated only with explicit Richard approval.** Every task follows the workflow:
1. **Proposed** (audit + diff)
2. **Approved** (Richard signs off)
3. **Implemented** (code merged, tested, verified)
4. **Verified** (raw JSON proof, admin panel audit)
5. **Closed**

---

## CURRENT FOCUS (Single Task)

### CHART-TOOLTIP: Interactive Tooltip on Hover
**Status:** OPEN (audit phase)

**Goal:** When user moves cursor/mouse over the Price History chart, display a tooltip showing:
- Exact price at that point
- Exact date
- Tooltip follows cursor and stays within chart bounds

**Requirements:**
- Works on both desktop (mouse) and mobile (touch)
- Tooltip appears near cursor, doesn't obscure the line
- Shows nearest data point to cursor position
- Disappears when cursor leaves chart

**Next step:** Audit chart interaction code (file paths + line ranges), propose tooltip component design, then wait for approval.

---

## COMPLETED (Verified & Closed)

### CHART-LABELS: Price Label Overlap Fix
**Status:** CLOSED (2026-03-01)

**What was done:**
- Implemented deterministic label stacking with merge rules
- HIGH label always top-most, LOW always bottom-most, CURRENT between
- Merge rule: If CURRENT == LOW (same formatted value), drop CURRENT label
- Merge rule: If CURRENT == HIGH (same formatted value), drop CURRENT label
- Labels never overlap due to hard constraint positioning

**Verification:**
- COTY (LOW≈CURRENT): Shows only HIGH ($32) + LOW ($3) - CURRENT merged ✅
- AAPL (HIGH≈CURRENT): Shows all 3 labels with proper spacing ✅
- No overlapping labels in any scenario

**Files changed:**
- `/app/frontend/app/stock/[ticker].tsx` (lines ~1513-1730, `computeChartLabels` function)

**No further changes needed.**

### FIX-2: Peer Benchmarks & Dividend Dual Medians (Full Consistency)
**Status:** CLOSED (2026-03-01)

**What was done:**
- Implemented dual dividend medians (`median_all` + `median_payers`) in `compute_peer_benchmarks_v3`
- Added independent fallback chains (Industry → Sector → Market) for each median
- Updated API response to return both medians + counts + fallback levels
- Maintained backward compatibility with existing `dividend_yield_median` field
- Implemented MIN_DIVIDEND_PAYERS = 5 safeguard for payers median

**Verification:**
- AAPL (Consumer Electronics): `dividend_payers_count=1`, `median_payers` fallback to sector (1.49%), `median_all=0.0%` ✅
- COTY (Household & Personal Products): `dividend_payers_count=14`, `median_payers` at industry level (2.83%), `median_all=0.0%` ✅
- All counts match database reality
- Fallback logic working correctly (triggers when payers < 5)

**Files changed:**
- `/app/backend/key_metrics_service.py` (dual median computation + fallback pass)
- `/app/backend/server.py` (API response extension)

**No further changes needed.**

---

## BACKLOG (Approved, Ready to Start)

### FIX-3: Revenue Growth (3Y CAGR) — Guardrails & Edge Cases
**Status:** APPROVED (waiting for FIX-2 to close)

**Goal:**
Ensure Revenue Growth (3Y CAGR) is calculated correctly for all visible tickers with proper guardrails and NA reason codes.

**Scope:**
- Requires 4 valid annual revenue points (3-year span)
- Start revenue > 0, end revenue > 0
- Returns NA with explicit reason if not possible
- Reason codes: `insufficient_annual_history`, `negative_or_zero_base_revenue`, `negative_end_revenue`

**Acceptance Criteria:**
- All 10 test tickers (AAPL, MSFT, COTY, TSLA, GOOGL, AMZN, META, NVDA, JPM, BAC) have calculated values or explicit NA reasons
- Raw JSON proof for 2 tickers (1 with value, 1 with NA)
- No hardcoded nulls

---

### FIX-4: Valuation Math Guardrails — Full Audit & Extreme Outlier Count
**Status:** APPROVED (waiting for FIX-2 to close)

**Goal:**
Verify all valuation metrics (PE, PS, PB, EV/EBITDA, EV/Revenue) use canonical safe_divide guardrails and extreme outliers are correctly flagged.

**Scope:**
- Regenerate valuation cache for all 5,374 visible tickers
- Count extreme outliers (result > 10,000)
- Expected: 0 or near 0 (if >5, investigate root cause)
- Produce master verification table (CSV + JSON)

**Acceptance Criteria:**
- Valuation cache regenerated
- Extreme outlier count ≤ 5 (or documented reason if higher)
- Master verification table (all visible tickers, all metrics, status codes, warnings)

---

### FIX-5: FX Layer & USD Normalization (Deferred)
**Status:** APPROVED (deferred until after FIX-2, FIX-3, FIX-4)

**Goal:**
Implement FX normalization so peer medians can include non-USD tickers (currently excluded).

**Scope:**
- Fetch FX rates (daily)
- Normalize all fundamentals to USD
- Update peer median logic to include normalized tickers
- Update currency audit

**Acceptance Criteria:**
- Non-USD tickers included in peer medians
- Currency audit shows 0 null-currency tickers in visible universe
- Raw JSON proof for 2 non-USD tickers showing normalized values

---

### FIX-6: Admin Panel Audit UI — Visibility Sieve & Peer Benchmarks
**Status:** APPROVED (deferred until after FIX-2, FIX-3, FIX-4)

**Goal:**
Admin panel must display:
- Visibility sieve funnel (step counts, exclusion reasons)
- Peer benchmark audit (counts, fallback levels, USD-only note)
- Canonical exclude patterns
- Job run logs

**Scope:**
- Backend: expose sieve step counts + peer benchmark audit data
- Admin UI: render sieve funnel + peer audit + job logs
- Ensure all URLs and API endpoints are visible

**Acceptance Criteria:**
- Admin panel shows full sieve funnel
- Peer benchmark audit visible (sample industries)
- Job logs visible with timestamps and status

---

### FIX-7: 404 / Invalid Ticker Page Redesign
**Status:** APPROVED (deferred until after FIX-2, FIX-3, FIX-4)

**Goal:**
Redesign generic 404 page for invalid/unsupported tickers with calm UX and request flow.

**Scope:**
- Show "Ticker not found" message
- Explain whitelist-only policy
- Provide one-click request flow (integrate with RICHIE or email)

**Acceptance Criteria:**
- Page renders for unsupported ticker
- Request flow works (email or RICHIE)
- Calm, educational tone

---

### FIX-8: Calendar Performance — Optimization & Benchmarking
**Status:** APPROVED (deferred until after FIX-2, FIX-3, FIX-4)

**Goal:**
Investigate and optimize slow calendar loading (market_daily_counts fetch, range queries, caching).

**Scope:**
- Profile calendar load times
- Identify bottlenecks (DB queries, API latency, frontend rendering)
- Benchmark against finfab.pro, stockanalysis.com, finviz.com
- Propose optimizations (caching, query optimization, pagination)

**Acceptance Criteria:**
- Load time < 2 seconds for typical date range
- Benchmarking report (vs. competitors)
- Optimization plan approved

---

### FIX-9: Missing Ticker Request Flow
**Status:** APPROVED (deferred until after FIX-2, FIX-3, FIX-4)

**Goal:**
Implement in-app flow for users to request missing/unsupported tickers.

**Scope:**
- Add "Request this ticker" button/flow on 404 page
- Integrate with RICHIE or email backend
- Track requests in admin panel

**Acceptance Criteria:**
- Request flow works end-to-end
- Admin can see requests
- User receives confirmation

---

## COMPLETED (Verified & Closed)

### FIX-1: Canonical Sieve & Exclude Patterns (Approved & Implemented)
**Status:** CLOSED (2026-02-24)

**What was done:**
- Canonical sieve steps defined and enforced
- Exclude patterns (warrants/units/preferred/rights) approved and implemented
- Startup guard validates is_visible count
- Admin panel shows sieve funnel

**Verification:**
- Visible universe: ~5,374 tickers
- Excluded: ~141 tickers (patterns)
- Audit: all steps reproducible

---

## Notes
- **One problem at a time:** Do not start FIX-3+ until FIX-2 is closed and verified.
- **Approval gate:** Every task must be audited, diffed, and approved before implementation.
- **Verification:** Every completed task must have raw JSON proof and admin audit visibility.
