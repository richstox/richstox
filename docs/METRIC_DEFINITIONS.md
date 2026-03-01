# RICHSTOX Metric Definitions & Inputs
## Single Source of Truth Document

**Version:** 1.0  
**Last Updated:** December 2025  
**Maintainer:** RICHSTOX Engineering

---

## Table of Contents

1. [Overview](#overview)
2. [Hybrid 7 Metrics](#hybrid-7-metrics)
   - [Market Cap](#1-market-cap)
   - [Shares Outstanding](#2-shares-outstanding)
   - [Net Margin (TTM)](#3-net-margin-ttm)
   - [Free Cash Flow Yield](#4-free-cash-flow-yield)
   - [Net Debt / EBITDA](#5-net-debt--ebitda)
   - [Revenue Growth (3Y CAGR)](#6-revenue-growth-3y-cagr)
   - [Dividend Yield (TTM)](#7-dividend-yield-ttm)
3. [Valuation 5 Multiples](#valuation-5-multiples)
   - [P/E Ratio (TTM)](#8-pe-ratio-ttm)
   - [P/S Ratio (TTM)](#9-ps-ratio-ttm)
   - [P/B Ratio](#10-pb-ratio)
   - [EV/EBITDA (TTM)](#11-evebitda-ttm)
   - [EV/Revenue (TTM)](#12-evrevenue-ttm)
4. [N/A Reason Codes Reference](#na-reason-codes-reference)
5. [Data Integrity Rules](#data-integrity-rules)

---

## Overview

All metrics in RICHSTOX are computed **locally from raw stored data**. We never use pre-computed values from external providers (e.g., EODHD Highlights/Technicals).

### Core Principles
- **RAW FACTS ONLY**: All calculations use raw financial data stored in MongoDB
- **NO LIVE API CALLS**: User-facing endpoints never call external APIs
- **HONEST N/A**: If data is missing or invalid, return explicit reason code
- **DETERMINISTIC**: Same inputs always produce same outputs

### Data Flow
```
EODHD API → Scheduled Jobs → MongoDB → Local Computation → API Response
```

---

## Hybrid 7 Metrics

### 1. Market Cap

**Display Name:** Market Cap  
**Unit:** Currency (USD)  
**Format:** `$XXX.XXB` / `$XXX.XXM`

#### Formula
```
Market Cap = Current Price × Shares Outstanding
```

#### DB Tables/Fields
| Table | Field | Description |
|-------|-------|-------------|
| `eod_data` | `adjusted_close` | Latest adjusted closing price |
| `fundamentals` | `shares_outstanding` | Direct number field |
| `fundamentals` | `shares_outstanding_quarterly.value` | Fallback: latest quarterly value |

#### Time Window
- **Price**: Latest available (real-time during market hours)
- **Shares**: Latest quarterly report

#### Fallback Hierarchy
1. `fundamentals.shares_outstanding` (direct number)
2. `fundamentals.shares_outstanding_quarterly.value` (latest quarter)
3. If neither available → N/A

#### N/A Reason Codes
| Code | Trigger |
|------|---------|
| `missing_data` | No shares outstanding data available |

#### Sanity Checks
- Shares outstanding must be > 0
- Price must be > 0

#### Example Calculation (AAPL.US)
```
Current Price: $178.50
Shares Outstanding: 15,334,000,000

Market Cap = $178.50 × 15,334,000,000 = $2,737,119,000,000
Formatted: $2.74T
```

---

### 2. Shares Outstanding

**Display Name:** Shares Outstanding  
**Unit:** Count  
**Format:** `XX.XXB` / `XX.XXM`

#### Formula
```
Shares Outstanding = Direct value from fundamentals (no calculation)
```

#### DB Tables/Fields
| Table | Field | Description |
|-------|-------|-------------|
| `fundamentals` | `shares_outstanding` | Primary: direct number |
| `fundamentals` | `shares_outstanding_quarterly.value` | Fallback: latest quarterly |

#### Time Window
- Latest quarterly report

#### Fallback Hierarchy
1. `fundamentals.shares_outstanding`
2. `fundamentals.shares_outstanding_quarterly.value`
3. N/A

#### N/A Reason Codes
| Code | Trigger |
|------|---------|
| `missing_data` | No shares data in fundamentals |

#### Sanity Checks
- Value must be > 0

#### Example Calculation (AAPL.US)
```
shares_outstanding: 15,334,000,000
Formatted: 15.33B
```

---

### 3. Net Margin (TTM)

**Display Name:** Net Margin (TTM)  
**Unit:** Percentage  
**Format:** `XX.X%`

#### Formula
```
Net Margin (TTM) = (Sum of Net Income, last 4 quarters) / (Sum of Revenue, last 4 quarters) × 100
```

#### DB Tables/Fields
| Table | Field | Description |
|-------|-------|-------------|
| `fundamentals` | `income_statement_quarterly[].netIncome` | Net income per quarter |
| `fundamentals` | `income_statement_quarterly[].totalRevenue` | Revenue per quarter |

#### Time Window
- **TTM (Trailing Twelve Months)**: Last 4 quarterly reports

#### Fallback Hierarchy
1. Sum of last 4 quarters (TTM)
2. If < 4 quarters available → N/A (insufficient_history)

#### N/A Reason Codes
| Code | Trigger |
|------|---------|
| `insufficient_history` | Less than 4 quarters of data |
| `missing_revenue` | Revenue data is null or zero |
| `unprofitable` | Calculated margin is negative (still shows value) |

#### Sanity Checks
- Total revenue must be > 0 (avoid division by zero)
- If margin < 0, set `na_reason: unprofitable` but STILL return the negative value

#### Example Calculation (XXII.US)
```
Q1 Net Income: -$8,500,000    Q1 Revenue: $12,000,000
Q2 Net Income: -$7,200,000    Q2 Revenue: $11,500,000
Q3 Net Income: -$9,100,000    Q3 Revenue: $10,800,000
Q4 Net Income: -$6,800,000    Q4 Revenue: $10,200,000

Total Net Income (TTM): -$31,600,000
Total Revenue (TTM): $44,500,000

Net Margin = (-31,600,000 / 44,500,000) × 100 = -71.01%
na_reason: "unprofitable"
```

---

### 4. Free Cash Flow Yield

**Display Name:** FCF Yield  
**Unit:** Percentage  
**Format:** `XX.X%`

#### Formula
```
FCF Yield = (Sum of Free Cash Flow, last 4 quarters) / Market Cap × 100

Where:
Free Cash Flow = Operating Cash Flow - |CapEx|
```

**IMPORTANT - CapEx Sign Convention:**
- EODHD stores `capitalExpenditures` inconsistently (sometimes negative, sometimes positive)
- Our code uses `abs(capex)` to normalize this
- **Actual formula in code:** `FCF = OCF - abs(CapEx)`
- This ensures CapEx is always subtracted regardless of sign in raw data

#### DB Tables/Fields
| Table | Field | Description |
|-------|-------|-------------|
| `fundamentals` | `cash_flow_quarterly[].totalCashFromOperatingActivities` | Operating cash flow |
| `fundamentals` | `cash_flow_quarterly[].capitalExpenditures` | CapEx (usually negative) |
| Computed | `market_cap` | Current price × shares |

#### Time Window
- **TTM**: Last 4 quarterly cash flow statements

#### Fallback Hierarchy
1. Sum of last 4 quarters FCF / Market Cap
2. If < 4 quarters → N/A

#### N/A Reason Codes
| Code | Trigger |
|------|---------|
| `insufficient_history` | Less than 4 quarters of cash flow data |
| `negative_fcf` | FCF is negative (still shows value) |
| `missing_data` | Market cap unavailable |

#### Sanity Checks
- Market cap must be > 0
- **CapEx sign handling:** Code uses `abs(capex)` - always treated as positive outflow
- If CapEx is null/missing, FCF = OCF (capital-light business assumption)

#### Example Calculation (XXII.US)
```
Q1: OCF = $500,000, CapEx = -$200,000 → FCF = $300,000
Q2: OCF = -$1,200,000, CapEx = -$150,000 → FCF = -$1,350,000
Q3: OCF = -$800,000, CapEx = -$100,000 → FCF = -$900,000
Q4: OCF = -$600,000, CapEx = -$180,000 → FCF = -$780,000

Total FCF (TTM): -$2,730,000
Market Cap: $12,330,000

FCF Yield = (-2,730,000 / 12,330,000) × 100 = -22.15%
na_reason: "negative_fcf"
```

---

### 5. Net Debt / EBITDA

**Display Name:** Net Debt/EBITDA  
**Unit:** Ratio (multiple)  
**Format:** `X.Xx`

#### Formula
```
Net Debt / EBITDA = Net Debt / EBITDA (TTM)

Where:
Net Debt = Total Debt - Cash & Cash Equivalents
EBITDA = Operating Income + Depreciation & Amortization (TTM)
```

#### DB Tables/Fields
| Table | Field | Description |
|-------|-------|-------------|
| `fundamentals` | `balance_sheet_quarterly[0].shortLongTermDebt` | Short-term debt |
| `fundamentals` | `balance_sheet_quarterly[0].longTermDebt` | Long-term debt |
| `fundamentals` | `balance_sheet_quarterly[0].cash` | Cash & equivalents |
| `fundamentals` | `income_statement_quarterly[].operatingIncome` | Operating income |
| `fundamentals` | `cash_flow_quarterly[].depreciation` | D&A |

#### Time Window
- **Debt/Cash**: Latest quarterly balance sheet
- **EBITDA**: TTM (last 4 quarters)

#### Fallback Hierarchy
1. Latest balance sheet for debt/cash, TTM for EBITDA
2. If any component missing → N/A

#### N/A Reason Codes
| Code | Trigger |
|------|---------|
| `unprofitable` | EBITDA ≤ 0 (cannot divide by zero/negative) |
| `missing_data` | Debt, cash, or EBITDA data unavailable |
| `missing_debt_data` | Specifically debt fields are null |

#### Sanity Checks
- EBITDA must be > 0 (if ≤ 0, company is unprofitable at EBITDA level)
- Net Debt can be negative (net cash position = good)

#### Example Calculation (Healthy Company)
```
Short-term Debt: $5,000,000
Long-term Debt: $20,000,000
Total Debt: $25,000,000
Cash: $8,000,000

Net Debt = $25,000,000 - $8,000,000 = $17,000,000

EBITDA (TTM):
Q1: OpIncome $4M + D&A $1M = $5M
Q2: OpIncome $4.2M + D&A $1M = $5.2M
Q3: OpIncome $3.8M + D&A $1M = $4.8M
Q4: OpIncome $4.5M + D&A $1M = $5.5M
EBITDA (TTM) = $20,500,000

Net Debt / EBITDA = 17,000,000 / 20,500,000 = 0.83x
```

#### Example (XXII.US - Unprofitable)
```
EBITDA (TTM): -$15,000,000 (negative operating income)

Result: N/A
na_reason: "unprofitable"
```

---

### 6. Revenue Growth (3Y CAGR)

**Display Name:** Revenue Growth (3Y CAGR)  
**Unit:** Percentage  
**Format:** `+XX.X%` / `-XX.X%`

#### Formula
```
Revenue Growth (3Y CAGR) = ((Revenue_Current / Revenue_3Y_Ago) ^ (1/3) - 1) × 100
```

#### DB Tables/Fields
| Table | Field | Description |
|-------|-------|-------------|
| `fundamentals` | `income_statement_annual[0].totalRevenue` | Current year revenue |
| `fundamentals` | `income_statement_annual[3].totalRevenue` | Revenue 3 years ago |

#### Time Window
- **3 Years**: Compare latest annual to 3 years prior

#### Fallback Hierarchy
1. Annual revenue comparison (current vs 3 years ago)
2. If < 4 years of annual data → N/A

#### N/A Reason Codes
| Code | Trigger |
|------|---------|
| `insufficient_history` | Less than 4 years of annual data |
| `missing_revenue` | Revenue data is null or zero for comparison years |

#### Sanity Checks
- Revenue 3 years ago must be > 0 (avoid division by zero)
- Handle negative growth (declining revenue) correctly

#### Example Calculation (XXII.US)
```
Revenue 2024 (Current): $44,500,000
Revenue 2021 (3Y Ago): $115,000,000

CAGR = ((44,500,000 / 115,000,000) ^ (1/3) - 1) × 100
CAGR = (0.387 ^ 0.333 - 1) × 100
CAGR = (0.729 - 1) × 100
CAGR = -27.1%

Formatted: -27.1%
```

---

### 7. Dividend Yield (TTM)

**Display Name:** Dividend Yield (TTM)  
**Unit:** Percentage  
**Format:** `X.XX%`

#### Formula
```
Dividend Yield (TTM) = (Sum of Dividends, last 365 days) / Current Price × 100
```

#### DB Tables/Fields
| Table | Field | Description |
|-------|-------|-------------|
| `dividend_history` | `value` | Dividend amount per payment |
| `dividend_history` | `date` | Payment date (filter last 365 days) |
| `eod_data` | `adjusted_close` | Current stock price |

#### Time Window
- **TTM**: Last 365 calendar days of dividend payments

#### Fallback Hierarchy
1. Sum dividends from `dividend_history` collection
2. If no dividends in last 365 days → return `0` (not N/A)

#### N/A Reason Codes
| Code | Trigger |
|------|---------|
| `missing_data` | Cannot retrieve price or dividend data |

#### Sanity Checks
- Current price must be > 0
- Dividend values must be > 0 (filter out negative adjustments)
- If no dividends, yield = 0% (not N/A)

#### Example Calculation (AAPL.US)
```
Dividends in last 365 days:
  Feb 2024: $0.24
  May 2024: $0.25
  Aug 2024: $0.25
  Nov 2024: $0.25
  
Total Dividends: $0.99
Current Price: $178.50

Dividend Yield = (0.99 / 178.50) × 100 = 0.55%
```

#### Example (XXII.US - No Dividends)
```
Dividends in last 365 days: $0

Dividend Yield = 0.00%
na_reason: null (not N/A, just zero)
```

---

## Valuation 5 Multiples

### 8. P/E Ratio (TTM)

**Display Name:** P/E  
**Unit:** Ratio (multiple)  
**Format:** `XX.X`

#### Formula
```
P/E Ratio = Current Price / EPS (TTM)

Where:
EPS (TTM) = Sum of Net Income (last 4 quarters) / Shares Outstanding
```

#### DB Tables/Fields
| Table | Field | Description |
|-------|-------|-------------|
| `eod_data` | `adjusted_close` | Current price |
| `fundamentals` | `income_statement_quarterly[].netIncome` | Quarterly net income |
| `fundamentals` | `shares_outstanding` | Share count |

#### Time Window
- **TTM**: Last 4 quarters of earnings

#### Fallback Hierarchy
1. TTM earnings from quarterly data
2. If < 4 quarters → N/A

#### N/A Reason Codes
| Code | Trigger |
|------|---------|
| `unprofitable` | EPS (TTM) ≤ 0 |
| `missing_data` | Insufficient quarterly data |

#### Sanity Checks
- EPS must be > 0 (P/E meaningless for unprofitable companies)
- Shares outstanding must be > 0

#### Example Calculation (AAPL.US)
```
Net Income (TTM): $96,995,000,000
Shares Outstanding: 15,334,000,000
EPS (TTM) = 96,995,000,000 / 15,334,000,000 = $6.33

Current Price: $178.50
P/E = 178.50 / 6.33 = 28.2x
```

---

### 9. P/S Ratio (TTM)

**Display Name:** P/S  
**Unit:** Ratio (multiple)  
**Format:** `XX.X`

#### Formula
```
P/S Ratio = Market Cap / Revenue (TTM)
```

#### DB Tables/Fields
| Table | Field | Description |
|-------|-------|-------------|
| Computed | `market_cap` | Price × Shares |
| `fundamentals` | `income_statement_quarterly[].totalRevenue` | Quarterly revenue |

#### Time Window
- **TTM**: Last 4 quarters of revenue

#### Fallback Hierarchy
1. TTM revenue from quarterly data
2. Annual revenue if quarterly unavailable
3. N/A if no revenue data

#### N/A Reason Codes
| Code | Trigger |
|------|---------|
| `missing_data` | No revenue data available |

#### Sanity Checks
- Revenue (TTM) must be > 0
- Market cap must be > 0

#### Example Calculation (XXII.US)
```
Market Cap: $12,330,000
Revenue (TTM): $44,500,000

P/S = 12,330,000 / 44,500,000 = 0.28x
```

---

### 10. P/B Ratio

**Display Name:** P/B  
**Unit:** Ratio (multiple)  
**Format:** `XX.X`

#### Formula
```
P/B Ratio = Market Cap / Book Value

Where:
Book Value = Total Stockholder Equity (from latest balance sheet)
```

#### DB Tables/Fields
| Table | Field | Description |
|-------|-------|-------------|
| Computed | `market_cap` | Price × Shares |
| `fundamentals` | `balance_sheet_quarterly[0].totalStockholderEquity` | Book value |

#### Time Window
- **Latest Quarter**: Most recent balance sheet

#### Fallback Hierarchy
1. Latest quarterly balance sheet
2. Latest annual balance sheet
3. N/A

#### N/A Reason Codes
| Code | Trigger |
|------|---------|
| `missing_data` | No book value data |
| `negative_equity` | Book value ≤ 0 |

#### Sanity Checks
- Book value must be > 0
- Market cap must be > 0

#### Example Calculation (XXII.US)
```
Market Cap: $12,330,000
Book Value (Stockholder Equity): $28,500,000

P/B = 12,330,000 / 28,500,000 = 0.43x
```

---

### 11. EV/EBITDA (TTM)

**Display Name:** EV/EBITDA  
**Unit:** Ratio (multiple)  
**Format:** `XX.X`

#### Formula
```
EV/EBITDA = Enterprise Value / EBITDA (TTM)

Where:
Enterprise Value = Market Cap + Total Debt - Cash
EBITDA = Operating Income + Depreciation & Amortization (TTM)
```

#### DB Tables/Fields
| Table | Field | Description |
|-------|-------|-------------|
| Computed | `market_cap` | Price × Shares |
| `fundamentals` | `balance_sheet_quarterly[0].shortLongTermDebt` | Short-term debt |
| `fundamentals` | `balance_sheet_quarterly[0].longTermDebt` | Long-term debt |
| `fundamentals` | `balance_sheet_quarterly[0].cash` | Cash & equivalents |
| `fundamentals` | `income_statement_quarterly[].operatingIncome` | Operating income |
| `fundamentals` | `cash_flow_quarterly[].depreciation` | Depreciation & amortization |

#### Time Window
- **Debt/Cash**: Latest balance sheet
- **EBITDA**: TTM (last 4 quarters)

#### Fallback Hierarchy
1. Full calculation from quarterly data
2. N/A if any component missing

#### N/A Reason Codes
| Code | Trigger |
|------|---------|
| `unprofitable` | EBITDA ≤ 0 |
| `missing_data` | Any required field unavailable |

#### Sanity Checks
- EBITDA must be > 0
- Market cap must be > 0

#### Example Calculation (Healthy Company)
```
Market Cap: $2,737,000,000,000
Total Debt: $111,000,000,000
Cash: $62,000,000,000

EV = 2,737B + 111B - 62B = $2,786B

EBITDA (TTM): $130,000,000,000

EV/EBITDA = 2,786B / 130B = 21.4x
```

---

### 12. EV/Revenue (TTM)

**Display Name:** EV/Revenue  
**Unit:** Ratio (multiple)  
**Format:** `XX.X`

#### Formula
```
EV/Revenue = Enterprise Value / Revenue (TTM)

Where:
Enterprise Value = Market Cap + Total Debt - Cash
```

#### DB Tables/Fields
| Table | Field | Description |
|-------|-------|-------------|
| Computed | `market_cap` | Price × Shares |
| `fundamentals` | `balance_sheet_quarterly[0].shortLongTermDebt` | Short-term debt |
| `fundamentals` | `balance_sheet_quarterly[0].longTermDebt` | Long-term debt |
| `fundamentals` | `balance_sheet_quarterly[0].cash` | Cash & equivalents |
| `fundamentals` | `income_statement_quarterly[].totalRevenue` | Quarterly revenue |

#### Time Window
- **Debt/Cash**: Latest balance sheet
- **Revenue**: TTM (last 4 quarters)

#### Fallback Hierarchy
1. Full calculation from quarterly data
2. N/A if any component missing

#### N/A Reason Codes
| Code | Trigger |
|------|---------|
| `missing_data` | Revenue or balance sheet data unavailable |

#### Sanity Checks
- Revenue (TTM) must be > 0
- Market cap must be > 0

#### Example Calculation (XXII.US)
```
Market Cap: $12,330,000
Total Debt: $2,500,000
Cash: $1,200,000

EV = 12.33M + 2.5M - 1.2M = $13.63M

Revenue (TTM): $44,500,000

EV/Revenue = 13.63M / 44.5M = 0.31x
```

---

## N/A Reason Codes Reference

| Code | Description | Used By |
|------|-------------|---------|
| `missing_data` | Required data not available in database | All metrics |
| `insufficient_history` | Not enough historical data points | Net Margin, FCF Yield, Revenue Growth |
| `unprofitable` | Company has negative earnings/EBITDA | Net Margin, P/E, EV/EBITDA, Net Debt/EBITDA |
| `negative_fcf` | Free cash flow is negative | FCF Yield |
| `missing_revenue` | Revenue data specifically missing | Net Margin, Revenue Growth |
| `missing_debt_data` | Debt fields unavailable | Net Debt/EBITDA |
| `negative_equity` | Book value is negative | P/B Ratio |
| `no_history` | No historical data for 5Y average | Valuation multiples (5Y comparison) |

---

## Data Integrity Rules

### Rule 1: No External API Calls
User-facing endpoints MUST NOT make live calls to EODHD or any external API. All data comes from MongoDB.

### Rule 2: No Pre-computed Values
We NEVER use pre-computed values from EODHD (e.g., `Highlights.PERatio`, `Technicals.52WeekHigh`). All metrics are calculated locally.

### Rule 3: Explicit N/A Handling
If a metric cannot be calculated, return `null` value with explicit `na_reason` code. NEVER guess or interpolate missing data.

### Rule 4: TTM Consistency
TTM metrics always use exactly 4 quarters. If < 4 quarters available, return N/A with `insufficient_history`.

### Rule 5: Sign Convention
- Percentages: Positive = good (for margins, growth), Negative = bad
- Multiples: Lower = cheaper (for valuation ratios)
- Net Debt/EBITDA: Lower = less leveraged, Negative = net cash (good)

---

## Implementation Reference

**Backend Service:** `/app/backend/local_metrics_service.py`

| Function | Metrics |
|----------|---------|
| `compute_hybrid_7_metrics()` | All Hybrid 7 metrics |
| `compute_valuation_metrics_v2()` | All Valuation 5 multiples |
| `calculate_local_pe()` | P/E Ratio |
| `compute_ps_ratio()` | P/S Ratio |
| `compute_pb_ratio()` | P/B Ratio |
| `compute_ev_ebitda_ratio()` | EV/EBITDA |
| `compute_ev_revenue_ratio()` | EV/Revenue |
| `get_shares_outstanding()` | Shares helper |

---

*End of Document*
