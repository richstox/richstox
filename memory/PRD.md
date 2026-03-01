# RICHSTOX - Product Requirements Document

## Original Problem Statement
Building RICHSTOX, an educational investing platform. User enforces strict "Propose & Wait" workflow.

## User Personas
- **Primary User**: Individual investors seeking educational stock analysis
- **Admin**: kurtarichard@gmail.com (auto-login enabled)

## Core Requirements
- Stock detail pages with valuation metrics, peer comparisons, price charts
- RRR (Risk/Reward Ratio) calculation and display
- Reality Check section with CAGR, drawdown, benchmark comparison
- Clean valuation table with peer median comparisons
- Admin panel for data management

## Architecture

### Frontend
- Expo/React Native Web
- Path: `/app/frontend/app/stock/[ticker].tsx`

### Backend
- FastAPI
- MongoDB
- Path: `/app/backend/server.py`

### Key Files
- `/app/backend/valuation_precompute.py` - Valuation calculations with math guardrails
- `/app/backend/visibility_rules.py` - Ticker visibility sieve
- `/app/backend/config.py` - Unified configuration
- `/app/frontend/components/FinancialHub.tsx` - Financials table & chart with 6-rule color system

## Data Sources
- EODHD API - Financial data provider

---

# CHANGELOG

## 2026-02-28: Financials Clarity - Complete Implementation ✅
### Implemented:
- **USER'S EXACT 6 RULES FOR COLORS:**
  1. PREVIOUS POSITIVE → NOW MORE POSITIVE = **DARK GREEN** (#059669)
  2. PREVIOUS POSITIVE → NOW LESS POSITIVE = **LIGHT GREEN** (#D1FAE5)
  3. PREVIOUS NEGATIVE → NOW MORE NEGATIVE = **DARK RED** (#DC2626)
  4. PREVIOUS NEGATIVE → NOW LESS NEGATIVE = **LIGHT RED** (#FEE2E2)
  5. PREVIOUS NEGATIVE → NOW POSITIVE = **↗ Profit** badge (DARK GREEN)
  6. PREVIOUS POSITIVE → NOW NEGATIVE = **↘ Loss** badge (DARK RED)

- **6-Rule Color Logic Extended to ALL 5 Metrics**: Revenue, Net Income, FCF, Cash, Total Debt
- **Total Debt INVERTED Logic**: Debt decrease = GREEN (good), Debt increase = RED (bad)

- **Detail Popup on Cell Click**: When user clicks any value in financials table, shows modal with:
  - Previous Period + Previous Value
  - Arrow (↓)
  - Current Period + Current Value
  - "Tap anywhere to close" hint

- **Compact USD Formatting (2026-02-28)**:
  - New `formatCurrency()` function with ~3 significant digits + K/M/B/T suffix
  - Rules: >=100B → 0 dec, 10-99B → 1 dec, 1-9.9B → 2 dec, same for M and K
  - Examples: $259.50M → $260M, $508.20M → $508M, $5.53B unchanged
  - Applied to: table cells, TTM cells, popup values
  - Preserves negative sign (-$368M)

- **P1 PHASE 1: Dividend Yield Industry Median from peer_benchmarks (2026-02-28)**:
  - Backend job: Added `dividend_yield` to `compute_peer_benchmarks_v3` in `key_metrics_service.py`
  - Allows >= 0 values (0% is valid for non-dividend paying companies)
  - Server.py: Replaced on-the-fly query with pre-computed `peer_bench_doc.metric_values.dividend_yield`
  - Frontend: Shows "(Peer set unavailable)" when industry median is missing
  - Note: In preview environment, peer_benchmarks is empty - will work in production with full data

- **P0 FIX: Net Debt/EBITDA Missing Data (2026-02-28)**:
  - ROOT CAUSE: EBITDA TTM was not being calculated from quarterly data in mobile_data endpoint
  - FIX 1: Added `ttm_ebitda` calculation from quarterly Income Statement data (server.py)
  - FIX 2: Added `raw_inputs.ebitda_ttm` storage in valuation_precompute.py for future use
  - FIX 3: Enhanced N/A reason codes: `ebitda_missing`, `missing_debt_data`, `missing_cash_data`
  - RESULT: 10/10 test tickers now show Net Debt/EBITDA values (e.g., AAPL: 0.3x, COTY: 35.9x)

- **TTM Column USD Mode Coloring (2026-02-28)**:
  - TTM cells for FLOW metrics (Revenue, Net Income, FCF) are now colored in BOTH USD and % modes
  - Same 6-rule color matrix applied comparing prior_ttm → current_ttm values
  - Popup shows "Prior TTM: $X" → "TTM: $Y" on click
  - Cash/Total Debt TTM remain neutral (snapshot metrics, no TTM YoY)
  - Change: Removed `displayMode === 'pct'` guard from renderTTMCell() function

- **TTM Cash/Debt N/A Fix (2026-02-28)**: 
  - Cash and Total Debt are snapshot (point-in-time) balance sheet values - TTM YoY is not applicable
  - TTM column now shows **"—"** dash instead of misleading "N/A"
  - Clicking "—" shows explanatory popup: "Not applicable: Cash and Total Debt are point-in-time balance sheet values (no TTM)."
  - In USD mode: shows absolute TTM value ($448.00M, $2.99B)
  - Bug fixed: `displayMode === '%'` → `displayMode === 'pct'` (type mismatch)

- **Key Metrics Section Collapsed by Default**
- **Comprehensive YoY Audit Completed** for COTY (Annual & Quarterly)

### Key Files Modified:
- `/app/frontend/components/FinancialHub.tsx` - Complete rewrite of color logic with `getCellAppearance()`, TTM snapshot handling

## 2026-02-28: P1 UX Polish - Top 1% Experience
### Implemented:
- **Chart-first Flow**: New section order - Price Card → Price History → Performance Check → Valuation Overview → Key Metrics
- **Native English Tooltips**: BottomSheet component (`MetricTooltip.tsx`) with English-only microcopy for all metrics
- **Valuation Pulse**: Collapsible Valuation Overview with header showing "DISCOUNTED/ALIGNED/OVERHEATED (~X% vs peers)" in green/yellow/red
- **Data-Empathetic N/A Reasons**: Replaced all "Missing data" with context-aware labels (e.g., "N/A (Negative EBITDA)", "N/A (Insufficient history)")
- **Dynamic Sector Dividend Context**: Backend calculates sector dividend yield median; frontend shows "0.00% (Sector avg: X.XX%)"
- **Info Icons**: All Key Metrics have tappable info icons (ⓘ) triggering BottomSheet tooltips
- **Two-column Valuation Table**: Expanded view shows both "vs Peers" and "vs 5Y Avg" columns

### Key Files Modified:
- `/app/frontend/app/stock/[ticker].tsx` - Section reorder, tooltips, empathetic N/A, Valuation Pulse
- `/app/frontend/components/MetricTooltip.tsx` - New BottomSheet tooltip component
- `/app/backend/server.py` - Sector dividend median calculation

## 2026-02-27: P1 UI Overhaul & RRR Redefinition
### Implemented:
- **RRR Redefinice**: New formula `RRR = (P_max - P_start) / (P_start - P_min)` - always >= 0
- **computeRRR()**: Frontend function for dynamic RRR calculation
- **RRR in Reality Check**: Displays MAX period RRR with tooltip
- **RRR under chart**: Dynamic RRR for selected period
- **Valuation Table Layout**: "~X% above/below median" under VS PEERS column
- **VS 5Y AVG column**: Header visible, cells empty (no data available)
- **vs S&P 500 TR format**: Simplified to "vs S&P 500 TR: ±X%"
- **Unprofitable labels**: Simplified to single word "Unprofitable"

### Fixed:
- COTY bug - RRR now displays (1.03) instead of being hidden due to negative efficiency_score
- Removed "Needs to fall/rise" misleading financial advice

## Previous Sessions:
- P0 Valuation Math Guardrails (safe_divide helper)
- P0 Visibility Sieve Filters (shares, currency)
- N/A Reason Transparency Layer
- Peer Label Mismatch Fix
- Valuation Summary Logic Fix
- 6-Layer API Call Prevention System
- Ad-Hoc Backfill Job conversion
- Unified Config module
- Startup Guard implementation
- MAX Chart Fix (smart downsampling)

---

# ROADMAP

## P0 (Critical)
- [x] RRR Redefinition (always non-negative)
- [x] UI Hierarchy & Visual Clarity Overhaul
- [x] P1 UX Polish - Chart-first flow, English Tooltips, Valuation Pulse, Data Empathy
- [x] Financials Clarity - User's 6-rule color system + Detail popup
- [ ] Fix floating-point TTM summation root cause

## P1 (High Priority - TOP 1% TRUST PASS remaining items)
- [ ] 5Y AVERAGES - Real backend implementation for valuation metrics
- [ ] HUMAN INTERPRETATION LABELS - Add "Excellent", "Good", "Fair" labels to metrics
- [ ] DIVIDEND GRADIENT - Color relative to industry average
- [ ] TRUSTWORTHY N/A REASONS - Audit all remaining N/A strings
- [ ] WEALTH GAP LABELING - Correct formula and tooltip

## P2 (Medium Priority)
- [ ] Funnel Inconsistency (is_visible vs passes_visibility_rule)
- [ ] api_calls logging for scheduler jobs
- [ ] FX Conversion Implementation
- [ ] BRK.B ticker format normalization
- [ ] Markets and Leagues pages
- [ ] Public/Private Portfolio management

## Future
- Offline Article Saving
- Stripe Integration
- @Mentions in posts
- User Filter on Talk page
- Apple Sign-In
- User Onboarding flow
