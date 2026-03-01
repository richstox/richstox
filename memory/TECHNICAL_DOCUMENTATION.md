# RICHSTOX - Kompletní Technická Dokumentace

## 1. PŘEHLED ARCHITEKTURY

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              EODHD API                                       │
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌──────────────┐        │
│  │ exchange-    │ │ fundamentals │ │ eod-bulk-    │ │ dividends    │        │
│  │ symbol-list  │ │ /{ticker}    │ │ last-day/US  │ │ /{ticker}    │        │
│  │ (1 credit)   │ │ (10 credits) │ │ (1 credit)   │ │ (1 credit)   │        │
│  └──────┬───────┘ └──────┬───────┘ └──────┬───────┘ └──────┬───────┘        │
└─────────┼────────────────┼────────────────┼────────────────┼────────────────┘
          │                │                │                │
          ▼                ▼                ▼                ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           BACKEND (FastAPI)                                  │
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌──────────────┐        │
│  │ whitelist_   │ │ fundamentals_│ │ batch_jobs_  │ │ eodhd_       │        │
│  │ service.py   │ │ service.py   │ │ service.py   │ │ service.py   │        │
│  └──────┬───────┘ └──────┬───────┘ └──────┬───────┘ └──────┬───────┘        │
└─────────┼────────────────┼────────────────┼────────────────┼────────────────┘
          │                │                │                │
          ▼                ▼                ▼                ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           MongoDB Collections                                │
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌──────────────┐        │
│  │ tracked_     │ │ company_     │ │ financials_  │ │ earnings_    │        │
│  │ tickers      │ │ fundamentals_│ │ cache        │ │ history_     │        │
│  │              │ │ cache        │ │              │ │ cache        │        │
│  └──────────────┘ └──────────────┘ └──────────────┘ └──────────────┘        │
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌──────────────┐        │
│  │ insider_     │ │ stock_prices │ │ dividend_    │ │ ops_job_runs │        │
│  │ activity_    │ │              │ │ history      │ │              │        │
│  │ cache        │ │              │ │              │ │              │        │
│  └──────────────┘ └──────────────┘ └──────────────┘ └──────────────┘        │
└─────────────────────────────────────────────────────────────────────────────┘
          │
          ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           FRONTEND (Expo/React Native)                       │
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌──────────────┐        │
│  │ Dashboard    │ │ Stock Detail │ │ Calculators  │ │ Search       │        │
│  │              │ │              │ │              │ │              │        │
│  └──────────────┘ └──────────────┘ └──────────────┘ └──────────────┘        │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. DATA SOURCES - CO STAHUJEME

### 2.1 EODHD API Endpoints

| Endpoint | Cost | Kadence | Účel |
|----------|------|---------|------|
| `exchange-symbol-list/{EXCHANGE}` | 1 credit | Týdně | Whitelist kandidátů (NYSE, NASDAQ) |
| `fundamentals/{TICKER}.US` | 10 credits | Denně (~200/den) | Fundamentální data společnosti |
| `eod-bulk-last-day/US` | 1 credit | Denně | Bulk update cen všech tickerů |
| `eod/{TICKER}.US` | 1 credit | Jednorázově | Historický backfill cen |
| `div/{TICKER}.US` | 1 credit | Týdně | Historie dividend |

### 2.2 Co EODHD vrací pro Fundamentals

```python
# EODHD /fundamentals/{TICKER}.US response structure:
{
    "General": {
        "Code": "AAPL",
        "Name": "Apple Inc",
        "Exchange": "NASDAQ",
        "Sector": "Technology",
        "Industry": "Consumer Electronics",
        "Description": "Apple Inc. designs...",
        "WebURL": "https://www.apple.com",
        "LogoURL": "/img/logos/US/aapl.png",
        "FullTimeEmployees": 164000,
        "IPODate": "1980-12-12",
        "Address": "One Apple Park Way...",
        "AddressData": {"City": "Cupertino", "State": "CA", ...},
        "CurrencyCode": "USD",
        "CountryISO": "US",
        "IsDelisted": false,
        ...
    },
    "Highlights": {
        "MarketCapitalization": 3458400518144,
        "PERatio": 37.86,
        "EarningsShare": 5.88,  # EPS
        "DividendYield": 0.0043,
        "DividendShare": 0.98,
        "ProfitMargin": 0.2397,
        "OperatingMarginTTM": 0.3117,
        "ReturnOnAssetsTTM": 0.2146,
        "ReturnOnEquityTTM": 1.5741,
        "RevenueTTM": 391034994688,
        "EBITDA": 134660997120,
        "BookValue": 3.767,
        ...
    },
    "Valuation": {
        "TrailingPE": 37.86,
        "ForwardPE": 30.77,
        "PriceSalesTTM": 8.84,
        "PriceBookMRQ": 60.73,
        "EnterpriseValue": 3499868262520,
        "EnterpriseValueRevenue": 8.95,
        "EnterpriseValueEbitda": 25.99,
        ...
    },
    "Technicals": {
        "Beta": 1.24,
        "52WeekHigh": 260.1,
        "52WeekLow": 163.49,
        "50DayMA": 239.30,
        "200DayMA": 217.02,
        ...
    },
    "SharesStats": {
        "SharesOutstanding": 15037899776,
        "SharesFloat": 15091184209,
        "PercentInsiders": 2.066,
        "PercentInstitutions": 62.25,
        ...
    },
    "SplitsDividends": {
        "ForwardAnnualDividendRate": 1.0,
        "ForwardAnnualDividendYield": 0.0043,
        "PayoutRatio": 0.1467,
        "DividendDate": "2024-11-14",
        "ExDividendDate": "2024-11-08",
        ...
    },
    "Financials": {
        "Income_Statement": {
            "quarterly": {
                "2024-09-30": {
                    "totalRevenue": 94930000000,
                    "costOfRevenue": 52552000000,
                    "grossProfit": 42378000000,
                    "operatingIncome": 29589000000,
                    "netIncome": 14736000000,
                    ...
                },
                ...
            },
            "annual": {...}
        },
        "Balance_Sheet": {...},
        "Cash_Flow": {...}
    },
    "Earnings": {
        "History": {
            "0": {
                "reportDate": "2024-10-31",
                "epsActual": 1.64,
                "epsEstimate": 1.60,
                "epsDifference": 0.04,
                "surprisePercent": 2.5
            },
            ...
        }
    },
    "InsiderTransactions": {
        "0": {
            "date": "2024-12-16",
            "ownerName": "Jeffrey E Williams",
            "transactionCode": "S",  # S=Sale, P=Purchase
            "transactionAmount": 100000,
            "transactionPrice": 249.97,
            "transactionAcquiredDisposed": "D"  # D=Disposed, A=Acquired
        },
        ...
    }
}
```

---

## 3. STORAGE - CO UKLÁDÁME DO DATABÁZE

### 3.1 `tracked_tickers` - Whitelist

**Účel:** Kanonický seznam tickerů, které RICHSTOX podporuje.

```python
{
    "ticker": "AAPL.US",           # PK - vždy s .US suffixem
    "code": "AAPL",                # Symbol bez suffixu
    "name": "Apple Inc",
    "exchange": "NASDAQ",
    "isin": "US0378331005",
    "type": "Common Stock",
    "currency": "USD",
    "country": "USA",
    
    # Status
    "status": "active",            # active | pending_fundamentals | no_fundamentals | delisted
    "is_active": true,             # Pouze true pokud má fundamentals
    
    # Metadata
    "sector": "Technology",
    "industry": "Consumer Electronics",
    "classification_source": "exchange_symbol_list",
    
    # Timestamps
    "first_seen_date": datetime,
    "last_seen_date": datetime,
    "fundamentals_updated_at": datetime,
    "created_at": datetime,
    "updated_at": datetime
}
```

**Logika:**
- Ticker může být `is_active=true` POUZE pokud má úspěšně stažená fundamentals
- Search a zobrazení v aplikaci funguje pouze pro `status='active'`

---

### 3.2 `company_fundamentals_cache` - Fundamentální Data

**Účel:** Hlavní cache pro všechna fundamentální data společnosti.

```python
{
    "ticker": "AAPL.US",           # PK
    "code": "AAPL",
    
    # IDENTITA
    "name": "Apple Inc",
    "exchange": "NASDAQ",
    "currency_code": "USD",
    "country_iso": "US",
    "country_name": "USA",
    
    # KLASIFIKACE
    "sector": "Technology",
    "industry": "Consumer Electronics",
    "gic_sector": "Information Technology",
    "gic_industry": "Technology Hardware, Storage & Peripherals",
    "security_type": "Common Stock",
    
    # DETAILY SPOLEČNOSTI
    "description": "Apple Inc. designs, manufactures...",  # Max 2000 znaků
    "website": "https://www.apple.com",
    "logo_url": "/img/logos/US/aapl.png",
    "full_time_employees": 164000,
    "ipo_date": "1980-12-12",
    "fiscal_year_end": "September",
    "is_delisted": false,
    
    # ADRESA
    "address": "One Apple Park Way, Cupertino, CA, United States, 95014",
    "city": "Cupertino",
    "state": "CA",
    "zip_code": "95014",
    
    # KLÍČOVÉ METRIKY
    "market_cap": 3458400518144,          # Tržní kapitalizace
    "enterprise_value": 3499868262520,     # Enterprise value
    
    # VALUAČNÍ POMĚRY
    "pe_ratio": 37.86,                     # P/E ratio (TTM)
    "eps_ttm": 7.89,                       # Earnings per share (TTM) - POČÍTÁME
    "ps_ratio": 8.84,                      # Price/Sales
    "pb_ratio": 60.73,                     # Price/Book
    "ev_ebitda": 25.99,                    # EV/EBITDA
    "ev_revenue": 8.95,                    # EV/Revenue
    "peg_ratio": 2.10,                     # PEG ratio
    "forward_pe": 30.77,                   # Forward P/E
    "trailing_pe": 37.86,                  # Trailing P/E
    
    # PROFITABILITA
    "profit_margin": 0.2397,               # Zisková marže
    "operating_margin": 0.3117,            # Operační marže
    "gross_margin": null,                  # POČÍTÁME z financials
    "net_margin_ttm": null,                # POČÍTÁME z financials
    "roe": 1.5741,                         # Return on Equity
    "roa": 0.2146,                         # Return on Assets
    
    # RŮST
    "revenue_ttm": 391034994688,           # Tržby TTM
    "revenue_per_share": 25.485,
    "quarterly_revenue_growth": 0.061,     # YoY růst tržeb
    "quarterly_earnings_growth": -0.341,   # YoY růst zisku
    
    # DIVIDENDY
    "dividend_yield": 0.0043,              # Dividendový výnos (EODHD)
    "dividend_yield_ttm": null,            # POČÍTÁME z dividend_history
    "dividend_share": 0.98,
    "forward_dividend_rate": 1.0,
    "forward_dividend_yield": 0.0043,
    "payout_ratio": 0.1467,
    "ex_dividend_date": "2024-11-08",
    "dividend_date": "2024-11-14",
    
    # AKCIE
    "shares_outstanding": 15037899776,
    "shares_float": 15091184209,
    "pct_insiders": 2.066,                 # % vlastnictví insiderů
    "pct_institutions": 62.25,             # % institucionálního vlastnictví
    
    # TECHNICKÉ INDIKÁTORY
    "beta": 1.24,                          # Volatilita vs trh
    "fifty_two_week_high": 260.1,          # 52W high
    "fifty_two_week_low": 163.49,          # 52W low
    "fifty_day_ma": 239.30,                # 50-day MA
    "two_hundred_day_ma": 217.02,          # 200-day MA
    
    # DALŠÍ
    "book_value": 3.767,
    "ebitda": 134660997120,
    
    # CENA (aktualizuje price sync)
    "price_last_close": 228.50,
    "price_updated_at": datetime,
    
    # METADATA
    "eodhd_updated_at": "2025-01-21",
    "created_at": datetime,
    "updated_at": datetime
}
```

---

### 3.3 `financials_cache` - Finanční Výkazy

**Účel:** Normalizovaná tabulka finančních výkazů (Income Statement, Balance Sheet, Cash Flow).

```python
{
    "ticker": "AAPL.US",
    "period_type": "quarterly",    # quarterly | annual
    "period_date": "2024-09-30",   # Konec období
    
    # INCOME STATEMENT
    "revenue": 94930000000,                    # Tržby
    "cost_of_revenue": 52552000000,            # Náklady na prodané zboží
    "gross_profit": 42378000000,               # Hrubý zisk
    "operating_income": 29589000000,           # Provozní zisk
    "operating_expenses": 12789000000,         # Provozní náklady
    "net_income": 14736000000,                 # Čistý zisk
    "ebitda": 32000000000,                     # EBITDA
    "ebit": 29589000000,                       # EBIT
    "interest_expense": 1000000000,            # Úrokové náklady
    "income_tax_expense": 3000000000,          # Daň z příjmu
    "diluted_eps": 0.97,                       # Zředěný EPS
    
    # BALANCE SHEET
    "total_assets": 352755000000,              # Celková aktiva
    "total_liabilities": 308030000000,         # Celkové závazky
    "total_equity": 44725000000,               # Vlastní kapitál
    "total_debt": 100000000000,                # Celkový dluh
    "cash_and_equivalents": 30000000000,       # Hotovost
    "short_term_investments": 40000000000,     # Krátkodobé investice
    "total_current_assets": 150000000000,      # Oběžná aktiva
    "total_current_liabilities": 120000000000, # Krátkodobé závazky
    "retained_earnings": 10000000000,          # Nerozdělený zisk
    
    # CASH FLOW
    "operating_cash_flow": 26000000000,        # Provozní CF
    "investing_cash_flow": -5000000000,        # Investiční CF
    "financing_cash_flow": -20000000000,       # Finanční CF
    "capital_expenditures": -3000000000,       # CapEx
    "free_cash_flow": 23000000000,             # Free CF
    "dividends_paid": -4000000000,             # Vyplacené dividendy
    
    "created_at": datetime,
    "updated_at": datetime
}
```

**Index:** `(ticker, period_type, period_date)` - unique

---

### 3.4 `earnings_history_cache` - Historie Zisků

**Účel:** Kvartální EPS data pro earnings beat/miss analýzu.

```python
{
    "ticker": "AAPL.US",
    "quarter_date": "2024-10-31",       # Datum reportu
    "reported_eps": 1.64,               # Skutečný EPS
    "estimated_eps": 1.60,              # Odhadovaný EPS
    "eps_difference": 0.04,             # Rozdíl
    "surprise_pct": 2.5,                # Překvapení v %
    "beat_miss": "beat",                # beat | miss | null
    
    "created_at": datetime,
    "updated_at": datetime
}
```

**Index:** `(ticker, quarter_date)` - unique

**Logika:**
- `beat_miss = "beat"` pokud `surprise_pct > 0`
- `beat_miss = "miss"` pokud `surprise_pct < 0`
- Ukládáme max 32 kvartálů (8 let)

---

### 3.5 `insider_activity_cache` - Insider Transakce

**Účel:** Agregovaná 6měsíční insider aktivita.

```python
{
    "ticker": "AAPL.US",
    
    # AGREGOVANÉ STATISTIKY (posledních 6 měsíců)
    "buyers_count": 2,                  # Počet unikátních kupujících
    "sellers_count": 5,                 # Počet unikátních prodávajících
    "total_buy_shares_6m": 50000,       # Celkem nakoupené akcie
    "total_sell_shares_6m": 200000,     # Celkem prodané akcie
    "total_buy_value_6m": 10000000,     # Celková hodnota nákupů ($)
    "total_sell_value_6m": 50000000,    # Celková hodnota prodejů ($)
    "net_value_6m": -40000000,          # Net value (buy - sell)
    "avg_buy_price": 200.00,            # Průměrná nákupní cena
    "avg_sell_price": 250.00,           # Průměrná prodejní cena
    "last_activity_date": "2024-12-16", # Poslední transakce
    
    # STATUS
    "status": "net_selling",            # net_buying | net_selling | neutral
    
    "created_at": datetime,
    "updated_at": datetime
}
```

**Logika statusu:**
- `net_buying` pokud `net_value_6m > 10000`
- `net_selling` pokud `net_value_6m < -10000`
- `neutral` jinak

---

### 3.6 `stock_prices` - Cenová Historie

**Účel:** Historická cenová data pro grafy a výpočty.

```python
{
    "ticker": "AAPL.US",
    "date": "2024-12-16",
    "open_price": 248.00,
    "high_price": 251.00,
    "low_price": 247.00,
    "close_price": 250.00,
    "adjusted_close": 250.00,           # Adjusted pro splity/dividendy
    "volume": 50000000,
    
    "created_at": datetime
}
```

**Index:** `(ticker, date)` - unique

---

### 3.7 `dividend_history` - Historie Dividend

**Účel:** Historie dividendových plateb.

```python
{
    "ticker": "AAPL.US",
    "ex_date": "2024-11-08",            # Ex-dividend date
    "payment_date": "2024-11-14",
    "amount": 0.25,                     # Dividenda na akcii
    "currency": "USD",
    
    "created_at": datetime
}
```

---

### 3.8 `ops_job_runs` - Log Jobů

**Účel:** Logování batch jobů a synchronizací.

```python
{
    "job_name": "fundamentals_batch_500",
    "status": "completed",              # completed | failed | killed
    "details": {
        "total_tickers": 500,
        "processed": 500,
        "success": 480,
        "failed": 20,
        "api_calls_used": 5000,
        ...
    },
    "started_at": datetime,
    "finished_at": datetime,
    "created_at": datetime
}
```

---

## 4. VÝPOČTY - CO POČÍTÁME

### 4.1 EPS TTM (Trailing Twelve Months)

```python
# Součet EPS za poslední 4 kvartály
def calculate_eps_ttm(earnings_history):
    quarters = sorted(earnings_history, key=lambda x: x['reportDate'], reverse=True)[:4]
    eps_values = [q['epsActual'] for q in quarters if q.get('epsActual')]
    if len(eps_values) == 4:
        return sum(eps_values)
    return None
```

**Zdroj:** `earnings_history_cache`
**Uloženo v:** `company_fundamentals_cache.eps_ttm`

---

### 4.2 P/E Ratio (lokální výpočet)

```python
# P/E = Cena / EPS
def calculate_pe_ratio(price, eps_ttm):
    if eps_ttm and eps_ttm > 0:
        return price / eps_ttm
    return None
```

**Zdroj:** `stock_prices.close_price`, `company_fundamentals_cache.eps_ttm`
**Poznámka:** EODHD vrací P/E, ale můžeme přepočítat s aktuální cenou

---

### 4.3 Dividend Yield TTM

```python
# Dividendový výnos = Suma dividend za 365 dní / Cena × 100
def calculate_dividend_yield_ttm(dividends_last_year, current_price):
    if current_price and current_price > 0:
        total_dividends = sum(d['amount'] for d in dividends_last_year)
        return (total_dividends / current_price) * 100
    return None
```

**Zdroj:** `dividend_history`, `stock_prices`
**Uloženo v:** `company_fundamentals_cache.dividend_yield_ttm`

---

### 4.4 Net Margin TTM

```python
# Čistá marže = (Čistý zisk TTM / Tržby TTM) × 100
def calculate_net_margin_ttm(net_income_ttm, revenue_ttm):
    if revenue_ttm and revenue_ttm != 0:
        margin = (net_income_ttm / revenue_ttm) * 100
        return max(-100, min(100, margin))  # Clamp
    return None
```

**Zdroj:** `financials_cache` (poslední 4 kvartály)
**Uloženo v:** `company_fundamentals_cache.net_margin_ttm`

---

### 4.5 Earnings Surprise %

```python
# Překvapení = ((Skutečný - Odhadovaný) / |Odhadovaný|) × 100
def calculate_surprise_pct(reported_eps, estimated_eps):
    if estimated_eps and estimated_eps != 0:
        return ((reported_eps - estimated_eps) / abs(estimated_eps)) * 100
    return None
```

**Počítáme při:** Parsování earnings history
**Uloženo v:** `earnings_history_cache.surprise_pct`

---

### 4.6 Insider Activity Status

```python
def calculate_insider_status(net_value_6m):
    if net_value_6m > 10000:
        return "net_buying"
    elif net_value_6m < -10000:
        return "net_selling"
    return "neutral"
```

**Počítáme při:** Agregace insider transakcí
**Uloženo v:** `insider_activity_cache.status`

---

### 4.7 52-Week High/Low Distance

```python
# Distance od 52W high/low v %
def calculate_52w_distance(current_price, high_52w, low_52w):
    distance_from_high = ((high_52w - current_price) / current_price) * 100
    distance_from_low = ((current_price - low_52w) / low_52w) * 100
    return distance_from_high, distance_from_low
```

**Počítáme při:** Zobrazení v UI
**Zobrazeno:** Stock Detail → Key Metrics

---

### 4.8 Market Cap Classification

```python
def classify_market_cap(market_cap):
    if market_cap >= 300e9:
        return "Mega Cap"      # >= $300B
    elif market_cap >= 10e9:
        return "Large Cap"     # >= $10B
    elif market_cap >= 2e9:
        return "Mid Cap"       # >= $2B
    elif market_cap >= 300e6:
        return "Small Cap"     # >= $300M
    else:
        return "Micro Cap"     # < $300M
```

**Počítáme při:** Zobrazení v UI
**Zobrazeno:** Stock Detail → Market Cap badge

---

## 5. DISPLAY - CO ZOBRAZUJEME

### 5.1 Dashboard

| Komponenta | Data Source | Zobrazeno |
|------------|-------------|-----------|
| Portfolio List | Mock data (TODO) | Název, P/L % |
| Positions | Mock data (TODO) | Ticker, změna % |
| Investor Score | Mock data (TODO) | Score 0-100 |
| Watchlist | `tracked_tickers` + `stock_prices` | Ticker, cena, změna |

---

### 5.2 Stock Detail - Header

| Pole | Source | Výpočet |
|------|--------|---------|
| Ticker | `company_fundamentals_cache.code` | Přímo |
| Name | `company_fundamentals_cache.name` | Přímo |
| Price | `stock_prices.close_price` (latest) | Přímo |
| Change | `stock_prices` (latest - previous) | `close - prev_close` |
| Change % | Výpočet | `(change / prev_close) × 100` |
| Sector Badge | `company_fundamentals_cache.sector` | Přímo |
| Industry Badge | `company_fundamentals_cache.industry` | Přímo |
| Exchange Badge | `company_fundamentals_cache.exchange` | Přímo |

---

### 5.3 Stock Detail - Key Metrics

| Metrika | Source | UI Element |
|---------|--------|------------|
| Market Cap | `company_fundamentals_cache.market_cap` | Badge + Classification |
| P/E (TTM) | `company_fundamentals_cache.pe_ratio` | Number + Industry comparison |
| EPS (TTM) | `company_fundamentals_cache.eps_ttm` | Currency format |
| Net Margin | `company_fundamentals_cache.net_margin_ttm` | Percentage |
| Dividend Yield | `company_fundamentals_cache.dividend_yield_ttm` | Percentage |
| Beta | `company_fundamentals_cache.beta` | Number + Risk badge |
| 52W High | `company_fundamentals_cache.fifty_two_week_high` | Price + Distance % |
| 52W Low | `company_fundamentals_cache.fifty_two_week_low` | Price + Distance % |

---

### 5.4 Stock Detail - Financials Tab

| Výkaz | Periody | Metriky |
|-------|---------|---------|
| Income Statement | 4 annual, 8 quarterly | Revenue, Gross Profit, Operating Income, Net Income |
| Balance Sheet | 4 annual, 8 quarterly | Total Assets, Liabilities, Equity, Debt, Cash |
| Cash Flow | 4 annual, 8 quarterly | Operating CF, Investing CF, Financing CF, Free CF |

**UI:** Tabulka + Mini bar charts s YoY trend barvami

---

### 5.5 Stock Detail - Earnings Tab

| Pole | Source | UI |
|------|--------|-----|
| Quarter Date | `earnings_history_cache.quarter_date` | Date |
| Reported EPS | `earnings_history_cache.reported_eps` | Currency |
| Estimated EPS | `earnings_history_cache.estimated_eps` | Currency |
| Surprise % | `earnings_history_cache.surprise_pct` | Colored % |
| Beat/Miss | `earnings_history_cache.beat_miss` | Badge (green/red) |

**UI:** Bar chart (surprise %) + tabulka

---

### 5.6 Stock Detail - Insider Tab

| Pole | Source | UI |
|------|--------|-----|
| Status | `insider_activity_cache.status` | Badge (Net Buying/Selling/Neutral) |
| Buyers | `insider_activity_cache.buyers_count` | Number |
| Sellers | `insider_activity_cache.sellers_count` | Number |
| Buy Value | `insider_activity_cache.total_buy_value_6m` | Formatted ($2.4M) |
| Sell Value | `insider_activity_cache.total_sell_value_6m` | Formatted ($5.1M) |
| Net Value | `insider_activity_cache.net_value_6m` | Colored (+/-) |

---

## 6. DATA PRO AI RICHIE (Future Use)

### 6.1 Co budeme používat pro AI

| Data | Collection | AI Use Case |
|------|------------|-------------|
| Company Description | `company_fundamentals_cache.description` | Context pro odpovědi |
| Sector/Industry | `company_fundamentals_cache.sector/industry` | Peer comparison |
| All Financials | `financials_cache` | Trend analýza |
| All Earnings | `earnings_history_cache` | Earnings reliability |
| Insider Activity | `insider_activity_cache` | Sentiment signál |
| Price History | `stock_prices` | Technická analýza |
| Dividend History | `dividend_history` | Income analýza |

### 6.2 Plánované AI Features

1. **Stock Summary:** AI-generated shrnutí akcie na základě všech dat
2. **Investment Signals:** Automatické vyhodnocení 6 signálů (Business Performance, Valuation, Dividends, Insider, Volatility, News)
3. **Peer Comparison:** Porovnání s industry benchmarkem
4. **Chat:** Konverzační AI pro dotazy o akciích

---

## 7. SYNC JOBS - JAK STAHUJEME

### 7.1 Weekly: Whitelist Sync

```
Trigger: Každé pondělí 2:00 CET
Endpoint: /api/admin/whitelist/sync

Flow:
1. Fetch exchange-symbol-list pro NYSE a NASDAQ
2. Filter: Type='Common Stock', Currency='USD'
3. Exclude: -WT, -WS, -U, -P- (warrants, units, preferred)
4. Upsert do tracked_tickers se status='pending_fundamentals'
5. Vytvoř fundamentals_events pro nové tickery
6. Deaktivuj tickery nenalezené v EODHD (delisted)
```

### 7.2 Daily: Fundamentals Sync

```
Trigger: Každý den 3:00 CET (po market close)
Endpoint: /api/admin/batch/sync-fundamentals

Flow:
1. Vezmi ~200 tickerů k aktualizaci (rotace)
2. Pro každý ticker:
   a. Fetch /fundamentals/{ticker}.US (10 credits)
   b. Parse a ulož do company_fundamentals_cache
   c. Parse a ulož do financials_cache
   d. Parse a ulož do earnings_history_cache
   e. Agreguj a ulož do insider_activity_cache
   f. Nastav status='active' v tracked_tickers
3. Loguj výsledek do ops_job_runs
```

### 7.3 Daily: Price Sync

```
Trigger: Každý obchodní den 22:00 CET
Endpoint: /api/admin/prices/sync

Flow:
1. Fetch eod-bulk-last-day/US (1 credit)
2. Pro každý ticker v odpovědi:
   a. Upsert do stock_prices
   b. Update price_last_close v company_fundamentals_cache
3. Update market_daily_counts
```

### 7.4 Kill Switch

```python
# Globální proměnná pro zastavení batch jobů
BATCH_JOB_KILL_SWITCH = False

# Kontrola před každým tickerem
if get_kill_switch():
    result["killed"] = True
    result["kill_reason"] = "Kill switch enabled"
    break
```

---

## 8. API ENDPOINTS

### 8.1 Public Endpoints

| Method | Endpoint | Popis |
|--------|----------|-------|
| GET | `/api/stock-overview/{ticker}` | Kompletní overview (lite/full mode) |
| GET | `/api/whitelist/search?q={query}` | Hledání v aktivních tickerech |
| GET | `/api/whitelist/check/{ticker}` | Je ticker v whitelistu? |
| GET | `/api/whitelist/stats` | Statistiky whitelistu |

### 8.2 Admin Endpoints

| Method | Endpoint | Popis |
|--------|----------|-------|
| POST | `/api/admin/whitelist/sync` | Sync whitelist s EODHD |
| POST | `/api/admin/batch/sync-fundamentals` | Batch sync fundamentals |
| POST | `/api/admin/batch/kill-switch` | Zapnout/vypnout kill switch |
| GET | `/api/admin/batch/status` | Status batch jobů |
| GET | `/api/admin/fundamentals/stats` | Statistiky cached dat |
| POST | `/api/admin/fundamentals/sync-ticker/{ticker}` | Sync jednoho tickeru |

---

## 9. CREDIT COST SUMMARY

| Operace | Credits | Frekvence | Měsíční Cost |
|---------|---------|-----------|--------------|
| Whitelist sync | ~2 | Týdně | ~8 |
| Fundamentals (6500 × 10) | 65,000 | Měsíčně (full) | 65,000 |
| Daily fundamentals (~200 × 10) | 2,000 | Denně | 40,000 |
| Bulk prices | 1 | Denně | 20 |
| **CELKEM** | | | **~105,000/měsíc** |

S 100k credits/den je limit dostatečný.
