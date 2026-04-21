# RICHSTOX — DB Schema (Canonical Notes)

This is not a full ERD. It is a **canonical contract** describing the collections/fields that matter for correctness, auditability, and the "raw facts only" rule.

## 1) Global rules
- Store **raw facts** only (prices, dividends, statements, identifiers).
- Do **not** store provider-computed ratios/metrics as source-of-truth.
- Canonical computed outputs (benchmarks, caches) must be:
  - reproducible
  - auditable
  - versioned/traceable by job runs

## 2) Core collections (conceptual)
### 2.1 `tickers` (or equivalent master security collection)
Key fields (minimum):
- `ticker` (string, unique)
- `exchange` (string)
- `name` (string)
- `type` (string; must support "Common Stock" filtering)
- `is_delisted` (bool)
- `is_visible` (bool) — must match canonical sieve output
- `sector` (string|null)
- `industry` (string|null)
- `shares_outstanding` (number|null) — **required for visibility**
- `financial_currency` (string|null) — **required for visibility**
- optional: `country`, `website`, `logo_url`, `updated_at`

Visibility sieve depends on:
- price existence
- sector+industry
- not delisted
- shares_outstanding present
- financial_currency present
- exclude-pattern checks (warrants/units/preferred/rights)

### 2.2 `prices` (EOD time series)
- `ticker`
- `date`
- `open`, `high`, `low`, `close`
- `adj_close` (if stored; must be clearly defined)
- `volume`
Indexes:
- (ticker, date) unique

### 2.3 `dividends`
- `ticker`
- `ex_date`
- `pay_date` (optional)
- `amount`
- `currency` (if available)
Indexes:
- (ticker, ex_date)

### 2.3b `upcoming_dividends`
- `ticker` (string, unique)
- `next_ex_date` (date string, nullable)
- `next_pay_date` (date string, nullable)
- `next_dividend_amount` (number, nullable)
- `next_dividend_currency` (string, nullable)
- `source` (string)
- `fetched_at` (datetime)
- `window_start` (date string)
- `window_end` (date string)
- optional flags: `is_special` (bool), `is_irregular` (bool), `dividend_type` (string|null), `period` (string|null)
Indexes (created once at server startup via `create_upcoming_dividends_indexes(db)`, not during job runs):
- unique (ticker)
- (next_ex_date)

### 2.4 `splits` / `corporate_actions`
- `ticker`
- `date`
- `split_ratio` (or equivalent)

### 2.5 `fundamentals_raw` (provider JSON blobs, raw)
Purpose: store raw provider payloads for statements and metadata.
- `ticker`
- `source` (e.g., eodhd)
- `fetched_at`
- `payload` (raw JSON)
Rule:
- This is allowed to contain provider-computed fields **only as raw payload**, but app logic must not treat them as truth unless explicitly approved.

### 2.6 `financial_statements` (normalized raw facts)
Annual + quarterly:
- `ticker`
- `period_type` (annual|quarterly)
- `period_end` (date)
- Income statement raw facts (revenue, net_income, etc.)
- Balance sheet raw facts (cash, total_debt, etc.)
- Cashflow raw facts (free_cash_flow, etc.)
Indexes:
- (ticker, period_type, period_end)

### 2.7 `peer_benchmarks` (canonical computed medians)
- `level` (industry|sector|market)
- `key` (industry name or sector name or "market")
- `as_of` (date or job run timestamp)
- `currency_policy` (e.g., USD_ONLY)
- `peer_count` (number)
- Per-metric medians (examples):
  - valuation medians (pe, ps, pb, ev_ebitda, ev_revenue) as applicable
  - growth/profitability medians as applicable
  - **dividend dual medians**:
    - `dividend_yield_median_all`
    - `dividend_yield_median_payers`
    - `dividend_peer_count`
    - `dividend_payers_count`
Rules:
- Must be produced only by canonical job `compute_peer_benchmarks_v3`.
- API must read from here; do not recompute medians in request-time routes.

### 2.8 `valuation_cache` (canonical computed per-ticker outputs)
- `ticker`
- `as_of`
- computed valuation metrics (nullable)
- per-metric status codes (missing_raw_data, near_zero_denominator, etc.)
Rule:
- Must be reproducible from raw facts + canonical helpers.

### 2.9 `ops_job_runs` (observability)
- `job_name`
- `started_at`, `finished_at`
- `status` (ok|error)
- `details` (counts, warnings, URLs used)
- `version` / `git_sha` (if available)
Rule:
- Admin panel reads from here.

## 3) Required status codes (canonical)
Valuation and metric computations must surface explicit reasons:
- `missing_raw_data`
- `near_zero_denominator`
- `non_positive_value`
- `missing_shares`
- `extreme_outlier`
Plus any metric-specific NA codes (e.g., revenue CAGR).

## 4) Indexing (minimum expectations)
- prices: unique (ticker, date)
- statements: (ticker, period_type, period_end)
- dividends: (ticker, ex_date)
- peer_benchmarks: (level, key, as_of) or equivalent
- valuation_cache: (ticker, as_of)
