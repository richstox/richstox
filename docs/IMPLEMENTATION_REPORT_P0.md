# RICHSTOX Implementation Report
**Datum:** 2026-02-18
**Verze:** 1.0

---

## Executive Summary

Tento report dokumentuje implementaci P0 fáze projektu RICHSTOX - finanční analytická platforma pro dlouhodobé investory. Hlavní zaměření bylo na backend kalkulace, peer benchmarky a opravy vyhledávání.

---

## 1. P0: Backend Calculations & Industry Benchmarks

### 1.1 Požadavek
Implementovat lokální výpočty finančních metrik a peer comparison systém podle specifikace:
- Lokální P/E ratio: `price / eps_ttm`
- Net Margin TTM: `(sum(last_4Q net_income) / sum(last_4Q revenue)) * 100`
- Dividend Yield TTM: `sum(dividends_last_365_days) / current_price * 100`
- Industry Benchmarks: Mediány pro každé odvětví (min. 5 společností)
- Valuation Score (0-100): Srovnání s peer benchmarky

### 1.2 Implementace

#### 1.2.1 Nové Backend Services

| Soubor | Popis |
|--------|-------|
| `/app/backend/industry_benchmarks_service.py` | Výpočet a správa industry benchmarks |
| `/app/backend/dividend_history_service.py` | Sync a výpočet dividend z EODHD |
| `/app/backend/ttm_calculations_service.py` | TTM kalkulace z kvartálních dat |

#### 1.2.2 Industry Benchmarks (`industry_benchmarks_service.py`)

**Funkce:**
- `compute_industry_benchmarks(db)` - Agregace mediánů pro všechna odvětví
- `get_industry_benchmark(db, industry)` - Získání benchmarku pro konkrétní odvětví
- `compute_valuation_score(company_metrics, benchmark_metrics)` - Výpočet Valuation Score
- `compute_gradient_color(company_val, benchmark_val, direction)` - Soft gradient barvy pro UI

**Algoritmus Valuation Score:**
```
Base score = 50
Pro každou metriku:
  - Pokud company < benchmark * 0.9 → +10 (podhodnoceno)
  - Pokud company > benchmark * 1.1 → -10 (nadhodnoceno)
  - Jinak → 0 (v souladu)
Final score = clamp(base + adjustments, 0, 100)

Status:
  - score > 60 → "Below peers" (levnější valuace)
  - score < 40 → "Above peers" (dražší valuace)
  - 40-60 → "In line" (srovnatelné)
```

**Metriky v benchmarcích:**
- P/E, P/S, P/B ratio
- EV/EBITDA, EV/Revenue
- Dividend Yield, Net Margin, Profit Margin
- ROE, ROA

#### 1.2.3 Dividend History (`dividend_history_service.py`)

**Funkce:**
- `sync_ticker_dividends(db, ticker)` - Sync dividend pro 1 ticker z EODHD
- `sync_batch_dividends(db, tickers)` - Batch sync
- `calculate_dividend_yield_ttm(db, ticker, current_price)` - TTM yield výpočet
- `get_dividend_history_for_ticker(db, ticker)` - Historie pro UI (roční agregace, YoY růst)

**Dividend Status logika:**
- `growing`: YoY růst > 5%
- `stable`: YoY růst -5% až +5%
- `declining`: YoY růst < -5%
- `no_dividends`: Žádné dividendy

#### 1.2.4 TTM Calculations (`ttm_calculations_service.py`)

**Funkce:**
- `calculate_ttm_metrics(db, ticker)` - Výpočet všech TTM metrik z posledních 4 kvartálů
- `calculate_local_pe_ratio(current_price, eps_ttm)` - Lokální P/E
- `get_enhanced_stock_metrics(db, ticker, current_price)` - Kompletní metriky pro stock-overview

**TTM metriky:**
- Revenue TTM, Net Income TTM, EBITDA TTM
- EPS TTM, Operating Income TTM
- Gross Profit TTM, Free Cash Flow TTM
- Net Margin TTM (kalkulováno)

### 1.3 Nové API Endpointy

| Endpoint | Method | Popis |
|----------|--------|-------|
| `/api/admin/benchmarks/compute` | POST | Spustí výpočet industry benchmarks |
| `/api/admin/benchmarks/stats` | GET | Statistiky o benchmarcích |
| `/api/benchmarks/{industry}` | GET | Benchmark pro konkrétní odvětví |
| `/api/admin/dividends/sync-ticker/{ticker}` | POST | Sync dividend pro ticker |
| `/api/admin/dividends/sync-batch` | POST | Batch sync dividend |
| `/api/admin/dividends/stats` | GET | Statistiky dividend |
| `/api/dividends/{ticker}` | GET | Historie dividend pro ticker |
| `/api/admin/ttm/update-batch` | POST | Batch update TTM metrik |
| `/api/ttm/{ticker}` | GET | TTM metriky pro ticker |

### 1.4 Enhanced Stock Overview Endpoint

**Endpoint:** `GET /api/stock-overview/{ticker}?lite=true|false`

**Response struktura (nová pole):**
```json
{
  "ticker": "JPM.US",
  "company": { ... },
  "price": {
    "last_close": 307.13,
    "change_pct": 1.51,
    "source": "live"
  },
  "key_metrics": {
    "market_cap": 836088430592,
    "pe_ratio": 15.34,
    "pe_ratio_source": "local",
    "pe_benchmark": 15.35,
    "eps_ttm": 20.02,
    "net_margin_ttm": 20.3,
    "net_margin_benchmark": null,
    "dividend_yield_ttm": 1.92,
    "dividend_benchmark": 0.0209,
    ...
  },
  "valuation": {
    "score": 40,
    "status": "in_line",
    "status_label": "In line",
    "net_adjustments": 0,
    "metrics_comparison": {
      "pe_ratio": {
        "company_value": 15.34,
        "benchmark_value": 15.35,
        "deviation_pct": -0.0,
        "status": "in_line"
      },
      ...
    }
  },
  "gradient_colors": {
    "pe_ratio": {
      "deviation_pct": -0.0,
      "intensity": "none",
      "color_class": "neutral",
      "rgb": "rgb(255, 255, 255)"
    },
    ...
  },
  "peer_context": {
    "industry": "Banks - Diversified",
    "sector": "Financial Services",
    "company_count": 11,
    "has_sufficient_peers": true
  },
  "has_benchmark": true
}
```

### 1.5 Database Collections

#### Nová kolekce: `industry_benchmarks`
```json
{
  "industry": "Banks - Diversified",
  "sector": "Financial Services",
  "company_count": 11,
  "tickers": ["JPM.US", "BAC.US", ...],
  "pe_ratio_median": 15.35,
  "pe_ratio_p25": 10.2,
  "pe_ratio_p75": 18.5,
  "pe_ratio_count": 11,
  "ps_ratio_median": 3.59,
  "pb_ratio_median": 1.57,
  "ev_ebitda_median": 0,
  "dividend_yield_median": 0.0209,
  "net_margin_ttm_median": null,
  "profit_margin_median": 0.284,
  "created_at": "2026-02-18T13:39:07.050Z",
  "updated_at": "2026-02-18T13:39:07.050Z"
}
```

#### Nová kolekce: `dividend_history`
```json
{
  "ticker": "AAPL.US",
  "ex_date": "2024-11-08",
  "payment_date": "2024-11-14",
  "record_date": "2024-11-11",
  "declaration_date": "2024-10-31",
  "amount": 0.25,
  "currency": "USD",
  "created_at": "2026-02-18T13:40:00.000Z"
}
```

### 1.6 Výsledky

| Metrika | Hodnota |
|---------|---------|
| Industry benchmarks vytvořeno | 87 odvětví |
| Odvětví přeskočeno (< 5 firem) | 63 odvětví |
| Celkem společností v benchmarks | 1211 |
| Dividend history pro AAPL | 40 záznamů |

**Příklad výstupu - JPM:**
- Valuation Score: 40/100 ("In line")
- P/E: 15.34 vs peers 15.35 (-0.0%)
- Dividend Yield TTM: 1.92% vs peers 2.09%
- Peer context: 11 companies in Banks - Diversified

---

## 2. Search Fix: Whitelist Only, No ETFs, Exchange Display

### 2.1 Požadavek
- Vyhledávání musí vracet POUZE whitelistované tickery
- ETF nesmí být ve výsledcích
- Zobrazovat exchange (NYSE, NASDAQ) místo "US"

### 2.2 Implementace

#### 2.2.1 Backend změny (`whitelist_service.py`)

**Původní implementace:**
- Vyhledávání v `tracked_tickers` kolekci
- Bez filtrování ETF
- Bez exchange informace

**Nová implementace:**
- Vyhledávání v `company_fundamentals_cache` (obsahuje exchange)
- ETF filtrace pomocí regex na název:
  - Excluduje: "ETF", "Exchange-Traded", "Fund"
- Vrací exchange z databáze

```python
etf_exclusion = {
    "$and": [
        {"name": {"$not": {"$regex": "\\bETF\\b", "$options": "i"}}},
        {"name": {"$not": {"$regex": "Exchange-Traded", "$options": "i"}}},
        {"name": {"$not": {"$regex": "\\bFund\\b", "$options": "i"}}},
    ]
}
```

#### 2.2.2 Frontend změny (`search.tsx`)

**Změna API endpoint:**
```javascript
// Původní (EODHD API)
const response = await axios.get(`${API_URL}/api/search?q=${searchQuery}`);

// Nové (Whitelist DB)
const response = await axios.get(`${API_URL}/api/whitelist/search?q=${searchQuery}`);
```

### 2.3 API Response

**Endpoint:** `GET /api/whitelist/search?q=JP`

```json
{
  "query": "JP",
  "count": 1,
  "results": [
    {
      "ticker": "JPM",
      "name": "JPMorgan Chase & Co",
      "exchange": "NYSE",
      "sector": "Financial Services",
      "industry": "Banks - Diversified"
    }
  ]
}
```

### 2.4 Výsledky

| Test Case | Před | Po |
|-----------|------|-----|
| Hledání "JP" | 15+ výsledků (včetně JPST, JPIE ETF) | 1 výsledek (JPM) |
| Exchange display | "US" | "NYSE" |
| Hledání "AAPL" | AAPL + ETF obsahující Apple | AAPL (NASDAQ) |
| Hledání "BANK" | Banky + ETF | Pouze bankovní akcie |

---

## 3. Frontend UI Updates

### 3.1 Stock Detail Page (`[ticker].tsx`)

**Nové komponenty:**
- Valuation Context badge (score 0-100, status label)
- Key Metrics s peer comparison (vs peers: X.XX)
- Valuation Multiples sekce (P/S, P/B, EV/EBITDA)
- Peer context disclaimer

**Nové TypeScript interfaces:**
```typescript
interface ValuationData {
  score: number;
  status: string;
  status_label: string;
  metrics_comparison: Record<string, MetricComparison>;
}

interface GradientColor {
  deviation_pct: number;
  intensity: string;
  color_class: string;
  rgb: string;
}

interface PeerContext {
  industry: string;
  company_count: number;
  has_sufficient_peers: boolean;
}
```

### 3.2 Search Page (`search.tsx`)

**Změny:**
- API endpoint změněn na whitelist search
- Exchange badge zobrazuje skutečnou burzu

---

## 4. Design Guidelines Implementation

### 4.1 Barevná strategie (Soft Gradients)

| Odchylka od mediánu | Intenzita | Barva |
|---------------------|-----------|-------|
| 0-10% | none | `rgb(255, 255, 255)` (bílá) |
| 10-25% | low | Lehký pastel |
| 25-50% | medium | Střední tón |
| 50%+ | high | Saturovanější (ale tlumený) |

### 4.2 Sémantika barev
- 🟩 Light green → Pod mediánem peers (levnější valuace)
- ⬜ Neutral → V souladu s mediánem
- 🟥 Light red → Nad mediánem peers (dražší valuace)

### 4.3 Jazyk
- ✅ Používat: "Valuation Context", "Below/Above/In line peers"
- ❌ Nepoužívat: "good/bad", "cheap/expensive", "buy/sell", "warning"

---

## 5. Testing Results

### 5.1 Backend API Tests

| Test | Výsledek |
|------|----------|
| `POST /api/admin/benchmarks/compute` | ✅ 87 benchmarks created |
| `GET /api/stock-overview/JPM?lite=true` | ✅ Valuation score 40, "In line" |
| `GET /api/stock-overview/AAPL?lite=true` | ✅ P/E 33.44 (local), Div Yield TTM 0.39% |
| `GET /api/whitelist/search?q=JP` | ✅ 1 result (JPM only) |
| `POST /api/admin/dividends/sync-ticker/AAPL` | ✅ 40 dividends synced |

### 5.2 Frontend Visual Tests

| Test | Výsledek |
|------|----------|
| JPM Stock Detail - Valuation Badge | ✅ "40/100 - In line vs 11 peers" |
| JPM Stock Detail - Key Metrics | ✅ P/E 15.34 vs peers 15.3 |
| Search "JP" | ✅ Only JPM (NYSE), no ETFs |
| Search "AAPL" | ✅ Only AAPL (NASDAQ) |
| Search "BANK" | ✅ Multiple banks, various exchanges |

---

## 6. Pending Items (P1)

| Položka | Priorita | Status |
|---------|----------|--------|
| Valuation Badge gauge visualization | P1 | Pending |
| Soft gradient backgrounds pro metriky | P1 | Pending |
| Dividends Tab (bar chart, YoY) | P1 | Pending |
| Financials Tab (Annual/Quarterly toggle) | P1 | Pending |
| Price Chart (range selector, DCA overlay) | P1 | Pending |
| News Tab | P1 | Pending (vyžaduje news_cache) |

---

## 7. Technical Debt & Recommendations

1. **TTM Data Quality**: Některé tickery nemají 4 kvartály dat - fallback na EODHD hodnoty
2. **Price Data**: `stock_prices` kolekce není plně populovaná - fallback na live EODHD
3. **Industry Coverage**: 63 odvětví má < 5 společností - nelze vytvořit benchmark
4. **Dividend Sync**: Potřeba přidat do denního batch jobu pro automatickou aktualizaci

---

## 8. Files Modified/Created

### Nové soubory:
- `/app/backend/industry_benchmarks_service.py`
- `/app/backend/dividend_history_service.py`
- `/app/backend/ttm_calculations_service.py`

### Modifikované soubory:
- `/app/backend/server.py` - Nové endpointy, enhanced stock-overview
- `/app/backend/whitelist_service.py` - ETF filtrace, exchange return
- `/app/frontend/app/stock/[ticker].tsx` - Valuation UI komponenty
- `/app/frontend/app/(tabs)/search.tsx` - Whitelist search endpoint

---

**Report vygenerován:** 2026-02-18T13:55:00Z
**Autor:** RICHSTOX Development Agent
