# RICHSTOX — Project State (Canonical, 2026-03-01)

## 1) What RICHSTOX is
- Mobile-first, long-term investing + portfolio tracking app for everyday people.
- Calm, educational, no hype, no buy/sell signals, no trading.
- Evidence-based UI: show context and history, not predictions.

## 2) Non-negotiable product rules
- **Raw facts only**: store prices, dividends, statements; compute all ratios locally.
- **No provider-computed metrics** stored as truth (P/E, margins, yields, etc.).
- **Whitelist-only UX**: only supported/visible tickers appear; others show "Ticker not found" with request flow.
- **One-problem-at-a-time**: do not start new tasks until the current one is closed.
- **No code changes without Richard approval** (proposal → approval → implement).

## 2) Non-negotiable product rules
- **Raw facts only**: store prices, dividends, statements; compute all ratios locally.
- **No provider-computed metrics** stored as truth (P/E, margins, yields, etc.).
- **Whitelist-only UX**: only supported/visible tickers appear; others show "Ticker not found" with request flow.
- **One-problem-at-a-time**: do not start new tasks until the current one is closed.
- **No code changes without Richard approval** (proposal → approval → implement).

## 3) Canonical ticker universe (visibility sieve)
Visible universe is produced by the canonical sieve:
1. Seed: NYSE + NASDAQ, Type="Common Stock"
2. Must have price data
3. Must have sector + industry
4. Must not be delisted
5. **shares_outstanding must be present**
6. **financial_currency must be present**
7. Exclude patterns (approved):
   - Warrants: -WT, -WS, -WI
   - Units: -U, -UN
   - Preferred: -P-, -PA...-PJ
   - Rights: -R, -RI

Admin panel must show:
- step counts for each sieve step
- the API URLs used
- excluded pattern list
- failed reasons counts (e.g., NO_PRICE_DATA, MISSING_SECTOR, MISSING_SHARES, MISSING_FINANCIAL_CURRENCY)

## 4) Data ingestion & jobs (high level)
- Daily/weekly jobs fetch:
  - prices (full history + gap fill)
  - splits
  - dividends
  - fundamentals (annual + quarterly statements)
  - universe maintenance (zombie tickers cleanup)
- All jobs log runs into ops/job-runs and are visible in Admin Panel.

## 5) Canonical peer benchmarks (medians)
- Computed by `compute_peer_benchmarks_v3`
- Stored in `peer_benchmarks`
- Backend + frontend must read these canonical values (no ad-hoc median queries).
- Fallback chain is deterministic:
  - **Industry → Sector → Market**
- Current policy: **USD-only** peers for medians until FX normalization exists.

### Dividend yield (forward) — dual median (canonical)
- Source of truth: `ForwardAnnualDividendYield`
- Store and return BOTH:
  - `dividend_yield_median_all` (includes zeros)
  - `dividend_yield_median_payers` (yield > 0 only; requires ≥5 payers at that level, else fallback)
- Track counts:
  - `dividend_peer_count` (all peers)
  - `dividend_payers_count` (payers only)

## 6) Valuation math guardrails (canonical)
- All valuation ratios use one safe division helper with statuses:
  - missing_raw_data
  - near_zero_denominator
  - non_positive_value (where required)
  - missing_shares
  - extreme_outlier
- Goal: toxic ratios become null with explicit reason, not misleading numbers.

## 7) Key UX changes (already decided)
- Financials table uses unified "6-rule matrix" for coloring/labels.
- Total Debt coloring is inverted (decrease is green).
- Cash/Total Debt TTM shows "—" with tooltip (point-in-time values).
- Revenue Growth (3Y CAGR) computed from last 4 annual revenue points with NA reason codes.
- RRR redefinition: always ≥0 or null; if risk_hist ≤ 0 then null.

## 8) Current open work (single focus)
- Ensure peer benchmarks + dividend dual medians are fully consistent across:
  - compute job
  - API payloads
  - frontend display
  - admin audit visibility
- No new tasks until this is closed.
