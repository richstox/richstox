# Performance Check — Audit s důkazy

**Datum:** 2026-03-30
**Scope:** Performance Check karta na `[ticker].tsx` — bottleneck analýza + audit každé metriky s přesnými řádky kódu.

---

## 1. Bottleneck: přesná cesta změna periody → fetch → setState

### Krok 1: Uživatel tapne period button

```
frontend/app/stock/[ticker].tsx:1674
  onPress={() => setPriceRange(range)}
```

`priceRange` je state deklarovaný na řádku 369:
```
frontend/app/stock/[ticker].tsx:369
  const [priceRange, setPriceRange] = useState<PriceRange>('MAX');
```

### Krok 2: useEffect reaguje na změnu `priceRange`

```
frontend/app/stock/[ticker].tsx:569-574
  useEffect(() => {
    if (ticker) {
      fetchChartData(priceRange);      // → setChartData
      fetchMobileDetail(priceRange);   // → setMobileData
    }
  }, [ticker, priceRange]);
```

### Krok 3a: fetchChartData — URL + setState

```
frontend/app/stock/[ticker].tsx:482-548
  const fetchChartData = async (range: PriceRange) => {
    // Cache check (L484-489):
    const cached = chartCacheRef.current[range];
    if (cached) { setChartData(cached.prices); return; }

    // Network fetch (L498):
    const response = await axios.get(
      `${API_URL}/api/v1/ticker/${ticker}/chart?period=${range}&include_benchmark=true`
    );

    // Downsample ~400 pts (L503-505):
    const step = Math.max(1, Math.floor(prices.length / targetPoints));
    const downsampled = prices.filter((_, i) => i % step === 0 || i === prices.length - 1);

    // Force-include true extrema (L509-523) — ensures HIGH/LOW are accurate

    // Cache + setState (L546-548):
    chartCacheRef.current[range] = { prices: formattedPrices, benchmark: benchData };
    setChartData(formattedPrices);
  };
```

### Krok 3b: fetchMobileDetail — URL + setState

```
frontend/app/stock/[ticker].tsx:434-448
  const fetchMobileDetail = async (period: PriceRange = '1Y') => {
    // Cache check (L436-439):
    const cached = mobileDataCacheRef.current[period];
    if (cached) { setMobileData(cached); return; }

    // Network fetch (L443):
    const response = await axios.get(
      `${API_URL}/api/v1/ticker/${ticker}/detail?period=${period}`
    );

    // Cache + setState (L444-445):
    mobileDataCacheRef.current[period] = response.data;
    setMobileData(response.data);
  };
```

### Krok 4: Backend endpoint → `calculate_period_stats`

```
backend/server.py:3349-3355
  @api_router.get("/v1/ticker/{ticker}/detail")
  async def get_ticker_detail_mobile(
      ticker: str,
      period: str = Query("1Y", ...)
  ):

backend/server.py:3455
  period_stats = await calculate_period_stats(db, ticker_full, period)
```

`calculate_period_stats` je v `backend/local_metrics_service.py:249-380`.

### Bottleneck identifikace

**PŘED opravou:** `fetchMobileDetail` neměl žádný cache — každá změna periody = síťový round-trip na `/api/v1/ticker/{ticker}/detail` (backend dělá full DB scan → ~2s).

**PO opravě:** `mobileDataCacheRef` (L394-395) zrcadlí existující `chartCacheRef` (L392) pattern. Cache se maže při:
- Změně tickeru: `[ticker].tsx:563-564` (`chartCacheRef.current = {}; mobileDataCacheRef.current = {};`)
- Pull-to-refresh: `[ticker].tsx:725-726` (totéž)

### Kompletní cesta (po opravě):

```
Tap "3Y" → setPriceRange('3Y')           [ticker].tsx:1674
  → useEffect [ticker, priceRange]        [ticker].tsx:569-574
    → fetchChartData('3Y')                [ticker].tsx:571
      → cache hit? setChartData(cached)   [ticker].tsx:484-486
      → cache miss? GET /api/v1/ticker/{ticker}/chart?period=3Y
                    → setChartData(...)   [ticker].tsx:548
    → fetchMobileDetail('3Y')             [ticker].tsx:572
      → cache hit? setMobileData(cached)  [ticker].tsx:436-438
      → cache miss? GET /api/v1/ticker/{ticker}/detail?period=3Y
                    → backend: calculate_period_stats(db, ticker, '3Y')
                                          local_metrics_service.py:249
                    → setMobileData(...)  [ticker].tsx:445
```

---

## 2. Metriky: přesné výrazy, zdrojová pole, hranice dat

### 2.1 Total Profit (%)

| | |
|---|---|
| **UI zobrazení** | `[ticker].tsx:2294` → `formatLargePercent(mobileData.period_stats.profit_pct)` |
| **Backend výpočet** | `local_metrics_service.py:322` |
| **Přesný vzorec** | `profit_pct = ((end_price / start_price) - 1) * 100 if start_price > 0 else 0` |
| **Zdrojová pole** | `start_price = prices[0].get("adjusted_close") or prices[0]["close"]` (L318) |
| | `end_price = prices[-1].get("adjusted_close") or prices[-1]["close"]` (L319) |
| **DB query** | `db.stock_prices.find({"ticker": ticker_full, "date": {"$gte": start_date}}, {"date":1, "close":1, "adjusted_close":1}).sort("date",1)` (L308-311) |
| **Hranice dat** | `start_date` závisí na periodě: 3M→-90d, 6M→-180d, 1Y→-365d, MAX→earliest DB record (L270-302) |
| | `actual_start_date = prices[0]["date"]`, `actual_end_date = prices[-1]["date"]` (L316-317) |
| **Guard** | `start_price > 0` (L322); `len(prices) < 2` → `return None` (L313-314) |
| **Zaokrouhlení** | `round(profit_pct, 1)` (L373) |
| **Formát** | `formatLargePercent`: ≥100→`toEU(abs,0)%`, <100→`toEU(abs,1)%` (`[ticker].tsx:860-871`) |

**Verdikt:** Standardní total-return vzorec `(P_end/P_start - 1) × 100`. Zdrojová data = `adjusted_close` (preferovaný) s fallback na `close`. Přesně definované hranice dat.

---

### 2.2 Avg. per Year (CAGR %)

| | |
|---|---|
| **UI zobrazení** | `[ticker].tsx:2307` → `formatLargePercent(mobileData.period_stats.cagr_pct)` |
| **UI guard** | `mobileData.period_stats.cagr_pct !== null` (L2298) — nezobrazí se pokud backend vrátí null |
| **Backend výpočet** | `local_metrics_service.py:326-329` |
| **Přesný vzorec** | `total_return = end_price / start_price` (L327) → `cagr_pct = (pow(total_return, 1.0/years) - 1) * 100` (L329) |
| **`years` výpočet** | Hardcoded per perioda: 3M→0.25, 6M→0.5, YTD→`(now-start).days/365.0`, 1Y→1.0, 3Y→3.0, 5Y→5.0, MAX→`(now-start).days/365.0` (L270-302) |
| **Guard** | `start_price > 0 and years > 0 and total_return > 0` (L326-328) — vrátí null pokud jakákoliv podmínka selže |
| **Zaokrouhlení** | `round(cagr_pct, 1)` (L375) |

**Verdikt:** Standardní CAGR = `(P_end/P_start)^(1/years) - 1`. Guard na `total_return > 0` zabraňuje chybě u záporného total returnu v pow (stock padl na 0).

---

### 2.3 Reward / Risk (RRR)

| | |
|---|---|
| **UI zobrazení** | `[ticker].tsx:2328` → `formatRRR(computedRRR)` |
| **UI guard** | `computedRRR !== null` (L2314) — nezobrazí se pokud null |
| **Frontend výpočet** | `[ticker].tsx:883-903` (useMemo na `[chartData]`) |
| **Přesný vzorec** | |

```typescript
// [ticker].tsx:886-900
const closes = chartData.map(p => p.adjusted_close).filter(c => c != null && c > 0);
const P_start = closes[0];           // L889
const P_max = Math.max(...closes);   // L890
const P_min = Math.min(...closes);   // L891
const reward_hist = P_max - P_start; // L893
const risk_hist = P_start - P_min;   // L894
const rrr = reward_hist / risk_hist; // L900
```

| | |
|---|---|
| **Zdrojová data** | `chartData` = downsampled ~400 bodů z `/api/v1/ticker/{ticker}/chart` (`[ticker].tsx:504-505`) |
| **Extrema ochrana** | Frontend forced-include true max/min bodů po downsampling (L509-523) — `P_max`/`P_min` reflektují skutečná extrema |
| **Guard** | `chartData.length < 2` → null (L884); `closes.length < 2` → null (L887); `risk_hist <= 0` → null (L897) — tj. stock nikdy neklesl pod startovní cenu |
| **Formát** | `formatRRR`: ≥100→integer `toEU(Math.round(rrr),0)`, <100→`toEU(rrr,1)` (`[ticker].tsx:831-836`) |
| **Barevné kódování** | `>2` zelená, `≥1` neutrální, `<1` červená (L2324-2326) |

**Verdikt:** RRR = (max-start)/(start-min). Počítáno na downsampled datech (~400 bodů), ale extrema jsou force-included, takže `P_max`/`P_min` odpovídají plné sérii.

---

### 2.4 Max Drawdown (%)

| | |
|---|---|
| **UI zobrazení** | `[ticker].tsx:2346` → `formatLargePercent(-Math.abs(mobileData.period_stats.max_drawdown_pct))` |
| **Backend výpočet** | `local_metrics_service.py:332` → `calculate_max_drawdown(...)` (L191-206) |
| **Přesný vzorec** | |

```python
# local_metrics_service.py:191-206
peak = prices[0]
max_dd = 0
for price in prices:
    if price > peak:
        peak = price
    dd = ((peak - price) / peak) * 100 if peak > 0 else 0
    if dd > max_dd:
        max_dd = dd
```

| | |
|---|---|
| **Zdrojová data** | `[p.get("adjusted_close") or p["close"] for p in prices]` (L332) — plná denní série, NE downsampled |
| **DB query** | Stejný jako profit_pct — `db.stock_prices.find(...)` (L308-311) |
| **Limit** | `to_list(length=None if period == "MAX" else 2000)` (L311) — MAX = neomezeno, jinak max 2000 dní |
| **Guard** | `len(prices) < 2` → return 0 (L193-194); `peak > 0` (L202) |
| **Zaokrouhlení** | `round(max_dd, 1)` (L374) |
| **Zobrazení** | Vždy záporné: `-Math.abs(...)` na frontendu (L2346) |

**Verdikt:** Standardní peak-to-trough drawdown na PLNÉ denní sérii (ne downsampled). Vzorec: `((peak - price) / peak) * 100`.

---

### 2.5 Drawdown Duration (days) + Recovery Date

| | |
|---|---|
| **UI zobrazení** | Duration: `[ticker].tsx:2358` → `drawdownDetails.durationDays` |
| | Recovery: `[ticker].tsx:2367-2369` → `formatDateDMY(drawdownDetails.recoveryDate)` nebo `'Not yet'` |
| **Frontend výpočet** | `[ticker].tsx:761-798` (useMemo na `[chartData]`) |
| **Přesný vzorec** | |

```typescript
// [ticker].tsx:770-781
chartData.forEach((d, i) => {
  if (d.adjusted_close > runningMax) { runningMax = d.adjusted_close; runningMaxIdx = i; }
  const drawdown = (runningMax - d.adjusted_close) / runningMax;
  if (drawdown > maxDrawdown) {
    maxDrawdown = drawdown;
    peak = { idx: runningMaxIdx, value: runningMax, date: chartData[runningMaxIdx]?.date };
    trough = { idx: i, value: d.adjusted_close, date: d.date };
  }
});

// Duration (L785-787):
const peakDate = new Date(peak.date + 'T00:00:00Z');
const troughDate = new Date(trough.date + 'T00:00:00Z');
const durationDays = Math.round((troughDate.getTime() - peakDate.getTime()) / (1000*60*60*24));

// Recovery (L789-795):
for (let i = trough.idx + 1; i < chartData.length; i++) {
  if (chartData[i].adjusted_close >= peak.value) { recoveryDate = chartData[i].date; break; }
}
```

| | |
|---|---|
| **Zdrojová data** | `chartData` = downsampled ~400 bodů (NE plná denní série) |
| **Guard** | `chartData.length <= 10` → null (L762); `maxDrawdown <= 0.01` → null (L783) |

**⚠️ POZOR:** Duration a Recovery se počítají z downsampled dat (~400 bodů), NE z plné denní série. Oproti backend `max_drawdown_pct` (plná série, L332) mohou být odchylky ±několik dní. Backend max_drawdown_pct % číslo je přesné (plná série), ale vizuální peak/trough/duration je aproximace.

---

### 2.6 S&P 500 TR Benchmark (%)

| | |
|---|---|
| **UI zobrazení** | `[ticker].tsx:2382-2384` → `toEU(mobileData.period_stats.benchmark_total_pct, 1)` |
| **UI guard** | `benchmark_total_pct !== null` (L2382) → `'N/A'` pokud null |
| **Backend výpočet** | `local_metrics_service.py:337-359` |
| **Přesný vzorec** | `benchmark_total_pct = ((sp500_end_price / sp500_start_price) - 1) * 100` (L359) |
| **Zdrojová data** | Ticker: `SP500TR.INDX` (L265) |
| **DB queries** | Start: `db.stock_prices.find_one({"ticker":"SP500TR.INDX", "date":{"$gte":actual_start_date}}, sort:date ASC)` (L340-344) |
| | End: `db.stock_prices.find_one({"ticker":"SP500TR.INDX", "date":{"$lte":actual_end_date}}, sort:date DESC)` (L347-351) |
| **Guard** | `sp500_start_price > 0 and sp500_end_price` (L357) |
| **Zaokrouhlení** | `round(benchmark_total_pct, 1)` (L376) |
| **Hranice dat** | `actual_start_date`/`actual_end_date` = skutečné obchodní dny tickeru (L316-317), NE kalkulované start/end |

**Verdikt:** Standardní total-return vzorec pro SP500TR.INDX. Používá `actual_start_date`/`actual_end_date` tickeru (ne teoretické) — přesné spárování period.

---

### 2.7 vs S&P 500 TR — Outperformance (Wealth Gap %)

| | |
|---|---|
| **UI zobrazení** | `[ticker].tsx:2388-2398` → `toEU(deltaClamped, 1)` kde `deltaClamped = Math.max(wealthGap, -100)` |
| **UI guard** | `wealthGap === null || wealthGap === undefined` → nezobrazí se (L2389) |
| **Backend výpočet** | `local_metrics_service.py:363-369` |
| **Přesný vzorec** | |

```python
# local_metrics_service.py:364-369
stock_multiplier = 1 + (profit_pct / 100)
bench_multiplier = 1 + (benchmark_total_pct / 100)
if bench_multiplier > 0:
    outperformance_pct = ((stock_multiplier / bench_multiplier) - 1) * 100
```

| | |
|---|---|
| **Guard (PŘED opravou)** | `benchmark_total_pct > 0` (starý kód) — **BUG**: skipped výpočet když S&P 500 měl záporný nebo nulový return |
| **Guard (PO opravě)** | Vnější: `profit_pct is not None and benchmark_total_pct is not None` (L364) |
| | Vnitřní: `bench_multiplier > 0` (L368) — matematicky správný: `benchmark > -100%` (dělení nulou) |
| **Frontend clamp** | `Math.max(wealthGap, -100)` (L2390) — zobrazení nikdy neklesne pod -100% |
| **Zaokrouhlení** | `round(outperformance_pct, 1)` (L377) |

**Verdikt po opravě:** Ratio-based wealth gap = `((1+stock%/100)/(1+bench%/100)-1)*100`. Guard `bench_multiplier > 0` zabraňuje dělení nulou (benchmark > -100%). Oprava rozšířila výpočet na případ kdy S&P 500 je flat nebo záporný.

---

## 3. Změny provedené v tomto PR

| # | Soubor | Řádky | Popis změny |
|---|--------|-------|-------------|
| 1 | `frontend/app/stock/[ticker].tsx` | L394-395 | Nový `mobileDataCacheRef = useRef<Record<string, MobileDetailData>>({})` |
| 2 | `frontend/app/stock/[ticker].tsx` | L434-440 | Cache-first check v `fetchMobileDetail` |
| 3 | `frontend/app/stock/[ticker].tsx` | L444 | Uložení do cache po fetch |
| 4 | `frontend/app/stock/[ticker].tsx` | L564 | Clear `mobileDataCacheRef` při změně tickeru |
| 5 | `frontend/app/stock/[ticker].tsx` | L726 | Clear `mobileDataCacheRef` při pull-to-refresh |
| 6 | `frontend/app/stock/[ticker].tsx` | L729 | Přidán `fetchMobileDetail(priceRange)` do `onRefresh` (chyběl) |
| 7 | `frontend/app/stock/[ticker].tsx` | L883-903 | `computeRRR()` → `computedRRR` useMemo(`[chartData]`) |
| 8 | `frontend/app/stock/[ticker].tsx` | L2314-2333 | RRR rendering: inline IIFE → přímý `{computedRRR !== null && (...)}` |
| 9 | `backend/local_metrics_service.py` | L364 | Odstraněn `and benchmark_total_pct > 0` — ponechán jen vnitřní guard `bench_multiplier > 0` (L368) |

---

## 4. Známá omezení

1. **Drawdown Duration/Recovery** (`[ticker].tsx:761-798`) — počítáno z downsampled ~400 bodů, NE plné denní série. Max drawdown % je z backendu (plná série), ale peak/trough datum a duration mohou mít odchylku ±několik dní.

2. **RRR** (`[ticker].tsx:883-903`) — počítáno z downsampled ~400 bodů, ALE s forced-include extrema (L509-523). `P_max`/`P_min` jsou přesné; `P_start` = first downsampled point = vždy `prices[0]` z API (first included in step filter).

3. **Period délky** (`local_metrics_service.py:270-302`) — 3M=90d, 6M=180d, 1Y=365d, 3Y=1095d, 5Y=1825d. Nepoužívají calendar months/years, jen fixní dny. YTD a MAX jsou dynamické.
