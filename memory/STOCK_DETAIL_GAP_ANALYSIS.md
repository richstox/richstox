# RICHSTOX Stock Detail - GAP Analýza

## Porovnání: Můj Prototyp vs Lovable App

---

## 1. HEADER SEKCE

| Funkce | Lovable ✅ | Můj Prototyp | Status |
|--------|-----------|--------------|--------|
| Ticker + Exchange badge | AAPL NASDAQ | ✅ Ano | OK |
| Název společnosti | Apple Inc | ✅ Ano | OK |
| Cena | $263.88 | ✅ Ano | OK |
| Změna ($ a %) | +$8.10 (+3.17%) | ✅ Ano | OK |
| Market status | "Closed • Opens in 2h 36m" | ❌ CHYBÍ | TODO |
| **Valuation Badge (0-100)** | "Overvalued (10)" s gauge | ❌ CHYBÍ | TODO |
| Sector/Industry | Technology / Consumer Electronics | ✅ Ano | OK |

---

## 2. COMPANY IDENTITY CARD

| Funkce | Lovable | Můj Prototyp | Status |
|--------|---------|--------------|--------|
| Logo | ✅ | ✅ | OK |
| Název | ✅ | ✅ | OK |
| IPO rok | IPO: 1980 | ✅ (1980-12-12) | OK |
| Lokace | ❌ (jen v description) | ✅ Cupertino, CA, USA | LEPŠÍ |
| Website | ❌ | ✅ www.apple.com | LEPŠÍ |
| Zaměstnanci | ❌ | ✅ 150K | LEPŠÍ |

---

## 3. AI SUMMARY (CHYBÍ KOMPLET)

```
Lovable má:
"Apple (AAPL) is a technology company in the consumer electronics industry. 
It has a P/E ratio of 32.34 and a net margin of 27.04%. 
The stock offers a dividend yield of 0.41% and is trading closer to its 52-week high."
```

**Můj prototyp:** Pouze statický description z EODHD, žádné AI shrnutí.

**TODO:** Implementovat AI summary generování (vyžaduje LLM integraci)

---

## 4. PRICE CHART (CHYBÍ KOMPLET)

| Funkce | Lovable | Můj Prototyp | Status |
|--------|---------|--------------|--------|
| Interaktivní cenový graf | ✅ | ❌ CHYBÍ | TODO |
| Buy & Hold overlay | ✅ (+8.3%, +$20.24) | ❌ | TODO |
| Rec. Inv. (DCA) overlay | ✅ (+12.3%, +$28.07) | ❌ | TODO |
| Range selector (1Y, 3Y...) | ✅ | ❌ | TODO |
| Period High/Low markers | ✅ | ❌ | TODO |
| Overlays toggle | ✅ | ❌ | TODO |

---

## 5. INVESTMENT SIMULATOR (CHYBÍ)

| Funkce | Lovable | Můj Prototyp | Status |
|--------|---------|--------------|--------|
| Lump Sum presets ($1K-$100K) | ✅ | ❌ | TODO |
| "You'd have about" výpočet | ✅ $10,874 | ❌ | TODO |
| Price return breakdown | ✅ +$874 (+8.7%) | ❌ | TODO |
| Dividends received | ✅ $43 | ❌ | TODO |
| Avg Profit p.a. | ✅ +8.7% | ❌ | TODO |
| Recurring Investments sekce | ✅ | ❌ | TODO |

---

## 6. RISK (HISTORY) (CHYBÍ)

| Funkce | Lovable | Můj Prototyp | Status |
|--------|---------|--------------|--------|
| Current drop from 52w high | ✅ -11.3% | ❌ | TODO |
| Worst drop from past peak | ✅ -81.8% (Mar 2000 → Apr 2003) | ❌ | TODO |

---

## 7. TABS - Porovnání

| Tab | Lovable | Můj Prototyp | Status |
|-----|---------|--------------|--------|
| Overview | ✅ | ✅ | Částečně |
| Financials | ✅ s grafy | ⚠️ Pouze text | VYLEPŠIT |
| Dividends | ✅ s "Growing" badge | ❌ CHYBÍ | TODO |
| Earnings | ✅ | ✅ | OK |
| Insiders | ✅ | ✅ | OK |
| News | ✅ | ❌ CHYBÍ | TODO |

---

## 8. KEY METRICS - Detailní Porovnání

### Lovable má pro každou metriku:
1. **Hodnotu**
2. **Barvu (zelená/červená)** podle porovnání s industry
3. **Popis vysvětlující metriku**
4. **Porovnání s industry** (↑ vs Industry: 24.6)

### Můj prototyp:
- ✅ Hodnoty
- ❌ Barvy chybí
- ❌ Popisy chybí
- ❌ Industry porovnání chybí

### Konkrétní metriky:

| Metrika | Lovable | Můj Prototyp | Rozdíl | Poznámka |
|---------|---------|--------------|--------|----------|
| Market Cap | $3.85T | $3.88T | ~0.8% | OK (rozdíl v čase) |
| P/E (TTM) | 32.34 (zelená, ↑ vs Industry: 24.6) | 33.44 | ~3.4% | **Potřeba ověřit výpočet** |
| EPS (TTM) | $7.91 | $7.89 | ~0.3% | OK |
| Net Margin (TTM) | 27.0% (zelená, Higher than peers) | ❌ CHYBÍ | - | **TODO: Přidat** |
| Dividend Yield (TTM) | 0.41% (↓ vs Industry: 0.41%) | 0.40% | - | **TODO: Počítat TTM** |
| Beta | 1.11 | 1.11 | 0% | OK |
| 52W High | $288.62 (+9.4% from current) | $288.35 | ~0.1% | OK |
| 52W Low | $169.21 (-35.9% from current) | $168.48 | ~0.4% | OK |

### Chybí úplně:
- ❌ Net Margin (TTM) s industry porovnáním
- ❌ "How to read this" vysvětlení

---

## 9. VALUATION MULTIPLES (CHYBÍ SEKCE)

| Metrika | Lovable | Můj Prototyp | Status |
|---------|---------|--------------|--------|
| P/S Ratio | 8.83 (↑ ~329% above industry avg) | ❌ | TODO |
| P/B Ratio | 45.86 (↑ ~323% above industry avg) | ❌ | TODO |
| EV/EBITDA | 26.61 (↑ ~79% above industry avg) | ❌ | TODO |
| EV/Revenue | 9.34 (↑ ~263% above industry avg) | ❌ | TODO |

---

## 10. TRADING SUMMARY BOX (CHYBÍ)

```
Lovable má:
"Trading Above Industry Averages"
"This company trades above industry averages on several valuation metrics. 
This may reflect higher growth expectations or market confidence, though higher valuations carry their own considerations."

Category: Consumer Electronics
Peer set used: Consumer Electronics (16 companies) — limited peer set

"This summary provides context only and is not investment advice."
```

**Můj prototyp:** ❌ Kompletně chybí

---

## 11. OWNERSHIP SEKCE

| Funkce | Lovable | Můj Prototyp | Status |
|--------|---------|--------------|--------|
| Insiders % | ✅ | ✅ | OK |
| Institutions % | ✅ | ✅ | OK |
| Retail % (šedá) | ✅ s popisem | ⚠️ Zobrazeno, ale bez popisu | VYLEPŠIT |
| Legenda | ✅ | ⚠️ Částečná | VYLEPŠIT |

---

## 12. FINANCIALS TAB - Požadavky

| Funkce | Lovable | Můj Prototyp | Status |
|--------|---------|--------------|--------|
| Annual vs Quarterly přepínač | ✅ | ❌ | TODO |
| TTM výpočty | ✅ | ❌ | TODO |
| Bar charty pro každou metriku | ✅ | ❌ (pouze text) | TODO |
| YoY % změna | ✅ | ❌ | TODO |
| Trend barvy (zelená/červená) | ✅ | ❌ | TODO |
| Income Statement | ✅ | ⚠️ Částečně | VYLEPŠIT |
| Balance Sheet | ✅ | ⚠️ Částečně | VYLEPŠIT |
| Cash Flow | ✅ | ⚠️ Částečně | VYLEPŠIT |

---

## 13. DIVIDENDS TAB (CHYBÍ KOMPLET)

| Funkce | Lovable | Můj Prototyp | Status |
|--------|---------|--------------|--------|
| Annual bar chart | ✅ | ❌ | TODO |
| YoY growth % | ✅ | ❌ | TODO |
| "Growing" badge | ✅ | ❌ | TODO |
| DCA Income Calculator | ✅ | ❌ | TODO |
| Partial year indicator | ✅ | ❌ | TODO |

---

## 14. NEWS TAB (CHYBÍ KOMPLET)

| Funkce | Lovable | Můj Prototyp | Status |
|--------|---------|--------------|--------|
| News list | ✅ | ❌ | TODO |
| Sentiment badge | ✅ | ❌ | TODO |
| Source, date | ✅ | ❌ | TODO |

---

## 15. VÝPOČTY - CO POČÍTÁM vs CO BY MĚLO BÝT

### EPS TTM
```python
# Můj výpočet:
eps_ttm = sum(last_4_quarters_epsActual)

# Lovable: Stejný
# Status: ✅ OK
```

### P/E Ratio
```python
# Můj výpočet:
pe_ratio = přímo z EODHD Highlights.PERatio

# Lovable výpočet:
pe_ratio = price_last_close / eps_ttm

# Rozdíl: Lovable počítá lokálně s aktuální cenou
# Status: ⚠️ UPRAVIT - počítat lokálně
```

### Dividend Yield TTM
```python
# Můj výpočet:
dividend_yield = přímo z EODHD (není TTM)

# Lovable výpočet:
dividend_yield_ttm = sum(dividends_last_365_days) / current_price * 100

# Status: ❌ CHYBÍ - musím počítat z dividend_history
```

### Net Margin TTM
```python
# Můj výpočet:
net_margin_ttm = None  # nepočítám

# Lovable výpočet:
net_margin_ttm = (sum(last_4Q_net_income) / sum(last_4Q_revenue)) * 100

# Status: ❌ CHYBÍ - musím přidat
```

### Valuation vs Industry
```python
# Můj výpočet:
# Nepočítám

# Lovable výpočet:
# Pro každou metriku (PE, PS, PB, EV/EBITDA):
# 1. Získej industry median z industry_benchmarks tabulky
# 2. Porovnej: 
#    - val < avg * 0.9 → "better" (zelená)
#    - val > avg * 1.1 → "worse" (červená)  
#    - jinak → "neutral"
# 3. Zobraz % rozdíl: ((val - avg) / avg) * 100

# Status: ❌ CHYBÍ - musím implementovat
```

### Valuation Score (0-100 gauge)
```python
# Můj výpočet:
# Nepočítám

# Lovable algoritmus (computeValuationAnalysis):
base_score = 50
for each_metric in [PE, PS, PB, EV_EBITDA, EV_Revenue, DivYield, NetMargin]:
    if val < industry_avg * 0.9:
        score += 10  # better (undervalued)
    elif val > industry_avg * 1.1:
        score -= 10  # worse (overvalued)
score = clamp(score, 0, 100)

# Status: netScore >= 2 → "undervalued"
# Status: netScore <= -2 → "overvalued"
# Status: else → "fairly_valued"

# Status: ❌ CHYBÍ - musím implementovat
```

---

## 16. PRIORITY SEZNAM

### P0 (Kritické - před scalingem):
1. ❌ Ověřit výpočty P/E, EPS, Dividend Yield
2. ❌ Přidat Net Margin TTM výpočet
3. ❌ Přidat Valuation vs Industry porovnání
4. ❌ Přidat Valuation Score (0-100 gauge)

### P1 (Důležité):
5. ❌ Dividends tab s historickým grafem
6. ❌ Financials tab s grafy a Annual/Quarterly toggle
7. ❌ News tab (vyžaduje news_cache naplnění)
8. ❌ Price chart s overlays

### P2 (Nice to have):
9. ❌ Investment Simulator
10. ❌ Risk (history) sekce
11. ❌ AI Summary (vyžaduje LLM)
12. ❌ Market status (Closed/Open)

---

## 17. DATOVÉ ZDROJE - Odkud co beru

| Data | Zdroj | Tabulka | Pole |
|------|-------|---------|------|
| Základní info | EODHD fundamentals | company_fundamentals_cache | name, sector, industry, description, logo_url, website, city, state, country, ipo_date, full_time_employees |
| Market Cap | EODHD fundamentals | company_fundamentals_cache | market_cap |
| P/E, EPS | EODHD fundamentals + výpočet | company_fundamentals_cache | pe_ratio, eps_ttm |
| Beta | EODHD fundamentals | company_fundamentals_cache | beta |
| 52W High/Low | EODHD fundamentals | company_fundamentals_cache | fifty_two_week_high, fifty_two_week_low |
| Div Yield | EODHD fundamentals | company_fundamentals_cache | dividend_yield |
| Div Yield TTM | **POČÍTÁM** z dividend_history | dividend_history | amount, ex_date |
| Net Margin TTM | **POČÍTÁM** z financials | financials_cache | net_income, revenue (4Q) |
| Ownership | EODHD fundamentals | company_fundamentals_cache | pct_insiders, pct_institutions |
| Financials | EODHD financials | financials_cache | revenue, net_income, etc. |
| Earnings | EODHD earnings | earnings_history_cache | reported_eps, estimated_eps |
| Insider | **AGREGUJI** z EODHD | insider_activity_cache | buyers_count, sellers_count, net_value |
| Industry Benchmark | **CHYBÍ** - musím vytvořit | industry_benchmarks | pe_median, ps_median, etc. |
