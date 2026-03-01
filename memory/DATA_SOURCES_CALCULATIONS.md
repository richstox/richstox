# RICHSTOX: Data Sources & Local Calculations
## ⚠️ SOURCE OF TRUTH - DO NOT VIOLATE THESE RULES ⚠️

**Key Rules:**
1. Never use EODHD's pre-calculated metrics (PE, PEG, PERatio, DividendYield from Highlights)
2. Download only raw data (prices, dividends, financials, earnings, insiders, general company info)
3. All calculations are local (P/E, P/S, margins, returns, benchmarks, peer medians)
4. When confused about "should we download X or calculate X", refer to this document first

---

## What We DOWNLOAD from EODHD (Raw Data Only)

### 1. Stock Prices
- **Endpoint:** `/api/eod/{ticker}.US`
- **Fields stored:** `open`, `high`, `low`, `close`, `adjusted_close`, `volume`
- **Frequency:** Daily (bulk sync after US market close)
- **Table:** `stock_prices`
- **Note:** Use `adjusted_close` (splits + dividends adjusted)

### 2. Fundamentals (General Info Only)
- **Endpoint:** `/api/fundamentals/{ticker}.US`
- **Fields stored:** `name`, `exchange`, `sector`, `industry`, `website`, `logo_url`, `address`, `full_time_employees`, `description`, `market_cap`, `shares_outstanding`
- **Frequency:** On-demand (when ticker activated) + nightly sync for changes
- **Table:** `company_fundamentals_cache`
- **⚠️ Note:** We store raw values; do NOT use EODHD's pre-calculated metrics (PE, PEG, etc.)

### 3. Dividend History
- **Endpoint:** `/api/fundamentals/{ticker}.US` (SplitsDividends section)
- **Fields stored:** `ex_date`, `payment_date`, `amount` (per share)
- **Frequency:** Daily sync
- **Table:** `dividend_history`

### 4. Financial Statements (Quarterly & Annual)
- **Endpoint:** `/api/fundamentals/{ticker}.US` (Financials section)
- **Fields stored:**
  - **Income Statement:** `revenue`, `gross_profit`, `operating_income`, `net_income`, `eps`
  - **Balance Sheet:** `total_assets`, `total_liabilities`, `shareholders_equity`, `cash`, `total_debt`
  - **Cash Flow:** `operating_cash_flow`, `free_cash_flow`, `capital_expenditures`
- **Frequency:** Quarterly updates (when new Q released)
- **Table:** `financials_cache`
- **Note:** Store raw figures; all TTM/trend calculations are local

### 5. Earnings History
- **Endpoint:** `/api/fundamentals/{ticker}.US` (Earnings section)
- **Fields stored:** `report_date`, `eps_estimate`, `eps_actual`, `surprise_pct`
- **Frequency:** After each earnings release
- **Table:** `earnings_history_cache`

### 6. Insider Transactions
- **Endpoint:** `/api/fundamentals/{ticker}.US` (InsiderTransactions section)
- **Fields stored:** `transaction_date`, `insider_name`, `relation`, `shares`, `price`, `transaction_type` (buy/sell)
- **Frequency:** Daily sync
- **Table:** `insider_activity_cache`

### 7. Whitelist (Symbol List)
- **Endpoint:** `/api/exchange-symbol-list/NYSE` + `/api/exchange-symbol-list/NASDAQ`
- **Fields stored:** `ticker`, `exchange`, `name`, `type` (filter: Common Stock only)
- **Frequency:** Weekly or on-demand
- **Table:** `tracked_tickers`
- **Note:** Used to activate/deactivate tickers; soft-delete on delisting

---

## What We CALCULATE Locally (Never from EODHD)

### Valuation Multiples (Per Ticker)

| Metric | Formula | Data Source |
|--------|---------|-------------|
| **P/E (TTM)** | `Price / EPS_TTM` | Price: latest `adjusted_close`; EPS_TTM: sum of last 4 quarterly EPS from `financials_cache` |
| **P/S (TTM)** | `MarketCap / Revenue_TTM` | MarketCap: `Price * SharesOutstanding`; Revenue_TTM: sum of last 4 quarterly revenue |
| **P/B (MRQ)** | `MarketCap / BookValue` | BookValue: latest quarterly `shareholders_equity` |
| **EV/EBITDA (TTM)** | `(MarketCap + TotalDebt - Cash) / EBITDA_TTM` | EBITDA_TTM: sum of last 4 quarterly (operating_income + depreciation/amortization) |
| **EV/Revenue (TTM)** | `EV / Revenue_TTM` | EV from above formula |

### Profitability Metrics (Per Ticker)

| Metric | Formula |
|--------|---------|
| **Net Margin (TTM)** | `(sum of last 4Q net_income) / (sum of last 4Q revenue) * 100` |
| **Operating Margin (TTM)** | `(sum of last 4Q operating_income) / (sum of last 4Q revenue) * 100` |
| **Gross Margin (TTM)** | `(sum of last 4Q gross_profit) / (sum of last 4Q revenue) * 100` |
| **ROE (TTM)** | `(sum of last 4Q net_income) / (average shareholders_equity) * 100` |
| **ROA (TTM)** | `(sum of last 4Q net_income) / (average total_assets) * 100` |

### Income Metrics (Per Ticker)

| Metric | Formula | Data Source |
|--------|---------|-------------|
| **Dividend Yield (TTM)** | `(sum of dividends in last 365 days) / current_price * 100` | Dividends from `dividend_history` |
| **Payout Ratio (TTM)** | `(sum of last 4Q dividends paid) / (sum of last 4Q net_income) * 100` | |

### Performance Metrics (Per Ticker, Per Date Range)

| Metric | Formula | Notes |
|--------|---------|-------|
| **Price Return** | `(end_price - start_price) / start_price * 100` | Uses `adjusted_close`; does NOT include reinvested dividends |
| **Total Return (with Reinvestment)** | `(end_value - start_value) / start_value * 100` | Assumes dividends are reinvested at ex-date price |
| **52W High/Low** | `MAX/MIN(adjusted_close)` over last 252 trading days | ⚠️ Never use EODHD's 52W fields (intraday, unreliable) |
| **Drawdown (Peak-to-Trough)** | `(min_price - peak_price) / peak_price * 100` | Computed from full price history |
| **CAGR** | `(end_value / start_value) ^ (1 / years) - 1` | |

### Peer Benchmarks (Per Industry)

**Industry Medians:**
- Metrics: P/E, P/S, P/B, EV/EBITDA, EV/Revenue, Net Margin, Operating Margin, Dividend Yield, ROE
- Calculation: median of all tickers in same industry (min. 5 peers)
- Peer count: stored per metric (coverage differs)
- Updated nightly after price sync
- Table: `industry_benchmarks`

### Benchmark Comparisons (S&P 500 Total Return)

**S&P 500 Total Return Index:**
- Formula: `TR_t = TR_{t-1} * (Price_t + Dividend_t) / Price_{t-1}`
- Uses SPX prices + dividends from `stock_prices` + `dividend_history`
- Recomputed daily

**Outperformance vs. Benchmark:**
- Formula: `Ticker_TR - SPX_TR` (in percentage points)
- Example: AAPL +9.45% vs SPX +18.2% = underperformed by 8.77pp

---

## Data Completeness Flags

### Per Ticker
- `status`: `active` / `pending_fundamentals` / `no_fundamentals`
- `fundamentals_pending`: bool (has prices but missing fundamentals)
- `last_price_date`: latest date in `stock_prices`
- `last_fundamentals_date`: latest date fundamentals were synced

### Per Industry
- `peer_count` (for each metric): number of tickers with valid data for that metric
- Show "Limited peer set" if `peer_count < 5`

---

## API Call Budget & Optimization

### Daily EODHD API Limit
- 100,000 credits/day
- Each `/api/eod/{ticker}` call = 1 credit
- Each `/api/fundamentals/{ticker}` call = 10 credits

### Nightly Schedule (Europe/Prague, Mon–Sat)
| Time | Task | Credits |
|------|------|---------|
| 04:00 | Daily price sync (bulk API) | ~1 |
| 04:30 | Fundamentals sync (changes only) | ~100 |
| 04:45 | Price backfill gaps (parallel) | ~500 |
| 05:00 | Full price backfill (parallel) | ~1,000 |
| 05:30 | Compute industry benchmarks (local) | 0 |

**Total daily: ~1,600 credits** (well under 100,000 limit)

---

## Summary Table

| Data | Source | Calculation | Storage | Frequency |
|------|--------|-------------|---------|-----------|
| Prices | EODHD | None (raw) | `stock_prices` | Daily |
| Dividends | EODHD | None (raw) | `dividend_history` | Daily |
| Financials | EODHD | None (raw) | `financials_cache` | Quarterly |
| P/E, P/S, P/B, EV/* | Local | Formulas above | Computed on-demand | Per request |
| Margins, ROE, ROA | Local | Formulas above | Computed on-demand | Per request |
| Dividend Yield | Local | Formulas above | Computed on-demand | Per request |
| Price/Total Return | Local | Formulas above | Computed on-demand | Per request |
| Industry Benchmarks | Local | Median per industry | `industry_benchmarks` | Nightly |
| S&P 500 TR | Local | Formulas above | `benchmark_prices` | Daily |

---

## Code Validation Checklist

When writing code, always verify:
1. [ ] Which table/field does this use?
2. [ ] Is this a raw EODHD field or a calculated metric?
3. [ ] If calculated, does the formula match this document?
4. [ ] Am I accidentally using EODHD's pre-calculated PE, PEG, etc.?
