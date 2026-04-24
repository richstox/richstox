## Step 4 historical dividends source

- **Collection used by Step 4:** `dividend_history`
- **Primary population path used before Step 4 runs:** `backend/dividend_history_service.py` → `sync_dividends_for_visible_tickers()`; this function is invoked as a hard dependency inside `backend/key_metrics_service.py` → `compute_peer_benchmarks_v3()`
- **External provider endpoint used for those historical records:** `GET https://eodhd.com/api/div/{TICKER}.US?api_token=...&fmt=json&from=...`
- **Other write paths to the same collection:** manual admin sync `POST /api/admin/dividends/sync-batch` and `backend/backfill_scripts/backfill_dividends.py`
- **Not used by Step 4:** `upcoming_dividends` / the `dividend_upcoming_calendar` job; upcoming calendar data is UI-facing only and is not read by `compute_peer_benchmarks_v3()`

This note is documentation-only. Step 4 logic was not changed in this PR.
