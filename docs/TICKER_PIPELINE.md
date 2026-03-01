<!--
================================================================================
UNIVERSE SYSTEM — PERMANENT & BINDING FOR ALL FUTURE INSTANCES
================================================================================
This is the ONLY way the app defines its ticker universe. No exceptions.
No agent, fork, or future instance may deviate from this.

ALLOWED EODHD API ENDPOINTS (ONLY THESE 3):
1. SEED:         https://eodhd.com/api/exchange-symbol-list/{NYSE|NASDAQ}
2. PRICES:       https://eodhd.com/api/eod-bulk-last-day/US
3. FUNDAMENTALS: https://eodhd.com/api/fundamentals/{TICKER}.US

VISIBLE UNIVERSE RULE:
is_visible = is_seeded && has_price_data && has_classification
Where:
  - is_seeded: NYSE/NASDAQ + Type == "Common Stock"
  - has_price_data: appears in daily bulk prices
  - has_classification: sector AND industry are non-empty

APP RUNTIME NEVER CALLS EODHD. All data comes from MongoDB only.

Any deviation requires explicit written approval from Richard (kurtarichard@gmail.com).
================================================================================
-->

# RICHSTOX Universe System - Single Source of Truth

## KRITICKÁ PRAVIDLA

1. **NIKDY** nevolat EODHD za běhu aplikace
2. **tracked_tickers** je jediný source of truth
3. **is_visible** = jediný filter pro app queries
4. Pouze **3 EODHD endpointy** jsou povoleny (viz níže)

---

## POVOLENÉ EODHD API ENDPOINTY

| Step | EODHD URL | Purpose | Called By |
|------|-----------|---------|-----------|
| **SEED** | `https://eodhd.com/api/exchange-symbol-list/NYSE` | Get NYSE tickers | scheduler.py |
| **SEED** | `https://eodhd.com/api/exchange-symbol-list/NASDAQ` | Get NASDAQ tickers | scheduler.py |
| **PRICES** | `https://eodhd.com/api/eod-bulk-last-day/US` | Daily bulk prices | scheduler.py |
| **FUNDAMENTALS** | `https://eodhd.com/api/fundamentals/{TICKER}.US` | Sector, industry, financials | scheduler.py |
| **BENCHMARK** | `https://eodhd.com/api/eod/SP500TR.INDX` | S&P 500 TR benchmark | benchmark_service.py (04:15) |

**ŽÁDNÉ JINÉ EODHD VOLÁNÍ NEJSOU POVOLENA.**

**PRAVIDLO PRO NOVÉ EODHD ENDPOINTY:**
1. Musí být schváleno Richardem
2. Volání POUZE v scheduler/backfill souborech (nikdy v runtime)
3. Soubor musí být přidán do allowlistu v `/app/scripts/audit_external_calls.py`
4. Soubor musí obsahovat hlavičku `# SCHEDULER-ONLY SERVICE`

---

## VISIBLE UNIVERSE RULE

```python
# JEDINÝ filter pro všechny app queries
VISIBLE_UNIVERSE_QUERY = {"is_visible": True}

# is_visible je computed field:
is_visible = is_seeded AND has_price_data AND has_classification

# Where:
is_seeded = (exchange IN ["NYSE", "NASDAQ"]) AND (asset_type == "Common Stock")
has_price_data = ticker appears in daily bulk prices
has_classification = (sector != "") AND (industry != "")
```

---

## Step 1 — Weekly Universe Seed (Neděle ráno)

**Schedule:** Neděle 04:00 Europe/Prague  
**Job:** `weekly_universe_seed`
**EODHD:** `GET /api/exchange-symbol-list/{NYSE|NASDAQ}`

### Hard filtr (POVINNÝ):
```python
# POUZE tickery kde Type == "Common Stock"
if row["Type"] != "Common Stock":
    SKIP  # Nepokračuje dál v pipeline
```

### Uložení do `tracked_tickers`:
| Field | Hodnota |
|-------|---------|
| `is_seeded` | true |
| `has_price_data` | false (dokud nepřijdou ceny) |
| `has_classification` | false (dokud nepřijdou fundamenty) |
| `is_visible` | false |

---

## Step 2 — Daily Bulk Prices (04:00 Europe/Prague)

**Schedule:** Denně 04:00 Europe/Prague (Mon-Sat)  
**Job:** `daily_price_sync`
**EODHD:** `GET /api/eod-bulk-last-day/US`

### Pravidlo:
```python
if ticker in bulk_prices:
    has_price_data = true
    # Recalculate is_visible
    is_visible = is_seeded AND has_price_data AND has_classification
```

---

## Step 3 — Fundamentals

**Schedule:** Denně 04:30 Europe/Prague  
**Job:** `daily_fundamentals_sync`
**EODHD:** `GET /api/fundamentals/{TICKER}.US`

### Pravidlo:
```python
if sector != "" AND industry != "":
    has_classification = true
    # Recalculate is_visible
    is_visible = is_seeded AND has_price_data AND has_classification
```

---

## excluded_tickers Collection

Tickers not in visible universe are tracked with reasons:

| Reason | Description |
|--------|-------------|
| `NOT_IN_SEED_LIST` | Not NYSE/NASDAQ |
| `NOT_COMMON_STOCK` | asset_type != "Common Stock" |
| `NO_PRICE_DATA` | No price data in stock_prices |
| `MISSING_SECTOR_INDUSTRY` | Empty sector or industry |
| `DELISTED` | Ticker is delisted |

---

## tracked_tickers Schema

| Field | Typ | Popis |
|-------|-----|-------|
| `symbol` | string | Kanonický symbol (AAPL) |
| `is_seeded` | bool | From weekly seed (NYSE/NASDAQ Common Stock) |
| `has_price_data` | bool | Has price data |
| `has_classification` | bool | Has sector AND industry |
| `is_visible` | bool | **= is_seeded && has_price_data && has_classification** |
| `sector` | string | From fundamentals |
| `industry` | string | From fundamentals |

---

## Soubory s permanent lock

| Soubor | Popis |
|--------|-------|
| `/app/backend/scheduler.py` | Scheduler daemon |
| `/app/backend/whitelist_service.py` | Universe seed |
| `/app/backend/price_ingestion_service.py` | Price sync |
| `/app/backend/backfill_fundamentals_raw.py` | Fundamentals backfill |
| `/app/docs/TICKER_PIPELINE.md` | This document |

**All files contain binding comment block at top.**
