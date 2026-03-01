# Missing Sector/Industry Root Cause Analysis Report

## Executive Summary

**Total tickers with missing sector/industry:** 423 out of 6096 (6.94%)

**Root Cause:** EODHD API misclassifies warrants, rights, units, and notes as `Type: "Common Stock"` but returns empty strings `""` for `Sector` and `Industry` fields.

## Data Export

Full list exported to: `/app/docs/missing_sector_423.csv`

## Name Pattern Analysis

| Category | Count | % | Description |
|----------|-------|---|-------------|
| Warrants | 216 | 51.1% | Ticker ends with W, WS, or name contains "Warrant" |
| Other | 96 | 22.7% | Requires manual review |
| Rights | 41 | 9.7% | Ticker ends with R or name contains "Right" |
| Units | 22 | 5.2% | Ticker ends with U or name contains "Unit" |
| Notes/Senior | 19 | 4.5% | Bonds labeled as common stock |
| Acquisition/SPAC | 19 | 4.5% | SPACs without sector classification |
| Trust | 10 | 2.4% | Trust structures |

## EODHD API Proof (Top 5 Examples)

### 1. AACBR.US (Rights)
```
URL: https://eodhd.com/api/fundamentals/AACBR.US
General.Name: "Artius II Acquisition Inc. Rights"
General.Type: "Common Stock"  <-- WRONG! Should be "Rights"
General.Sector: ""            <-- Empty string
General.Industry: ""          <-- Empty string
```

### 2. ABLVW.US (Warrant)
```
URL: https://eodhd.com/api/fundamentals/ABLVW.US
General.Name: "Able View Global Inc. Warrant"
General.Type: "Common Stock"  <-- WRONG! Should be "Warrant"
General.Sector: ""            <-- Empty string
General.Industry: ""          <-- Empty string
```

### 3. BRK-B.US (Control - Real Common Stock)
```
URL: https://eodhd.com/api/fundamentals/BRK-B.US
General.Name: "Berkshire Hathaway Inc"
General.Type: "Common Stock"  <-- CORRECT
General.Sector: "Financial Services"  <-- Has value
General.Industry: "Insurance - Diversified"  <-- Has value
```

### 4. BBBY.US (Delisted)
```
URL: https://eodhd.com/api/fundamentals/BBBY.US
General.Name: "Bed Bath & Beyond, Inc."
General.Type: "Common Stock"
General.Sector: null  <-- None (delisted company)
General.Industry: null
```

### 5. APOS.US (Preferred/Subordinated)
```
URL: https://eodhd.com/api/fundamentals/APOS.US
General.Name: "Apollo Global Management Inc."
General.Type: "Common Stock"
General.Sector: null  <-- None (likely preferred shares)
General.Industry: null
```

## Extractor Code Verification

File: `/app/backend/backfill_fundamentals_raw.py`
Lines 121-122:

```python
# B) CLASSIFICATION (required for filters)
"sector": general.get("Sector", ""),
"industry": general.get("Industry", ""),
```

**Analysis:**
- Extractor reads `General.Sector` directly from EODHD response
- Extractor reads `General.Industry` directly from EODHD response
- Default is empty string `""` if key is missing
- NO transformation or overwriting occurs
- Data is stored AS-IS from EODHD API

## Conclusion

**This is NOT an extractor bug.** The issue is EODHD data quality:

1. EODHD incorrectly classifies warrants/rights/units/notes as `Type: "Common Stock"`
2. These instruments have no sector/industry data (empty string or null)
3. Our universe filter requires `asset_type: "Common Stock"` which matches EODHD's (incorrect) classification
4. Result: These non-common-stock instruments appear in our universe without sector/industry

## Recommended Fix Options

### Option A: Filter by Ticker Suffix (Recommended)
Add to universe query:
```python
"ticker": {"$not": {"$regex": "W\\.US$|R\\.US$|U\\.US$|WS\\.US$"}}
```
This excludes tickers ending in W, R, U, WS (warrants, rights, units).

### Option B: Filter by Name Pattern
Add to weekly seed job:
```python
# Skip if name contains warrant/right/unit/note keywords
skip_patterns = ['warrant', 'right', 'unit', 'note', 'senior', 'subordinate']
if any(p in name.lower() for p in skip_patterns):
    continue
```

### Option C: Require Sector (strict)
Add to universe query:
```python
"sector": {"$ne": "", "$ne": None, "$exists": True}
```
This would exclude all 423 tickers. May be too aggressive if some real common stocks have missing sector in EODHD.

### Option D: Manual Whitelist Exception
For the 96 "Other" category tickers, create a manual review list and decide case-by-case.
