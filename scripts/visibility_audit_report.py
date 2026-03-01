#!/usr/bin/env python3
"""
VISIBILITY RULE AUDIT REPORT
============================

Audit all ticker filtering logic in the codebase.
Single source of truth: is_visible == true

Generated: 2026-02-22
"""

AUDIT_TABLE = """
| File | Function/Endpoint | Current Filter | Violates Rule | Action |
|------|-------------------|----------------|---------------|--------|
| server.py:1788 | get_stock_overview() | is_visible (FIXED) | NO | Already fixed |
| server.py:2234 | get_ticker_detail_mobile() | is_visible=True | NO | OK |
| server.py:1135 | get_homepage_data() | VISIBLE_UNIVERSE_QUERY | NO | OK |
| server.py:2874 | admin_visibility_report() | VISIBLE_UNIVERSE_QUERY | NO | OK |
| server.py:3557 | admin_full_audit() | VISIBLE_UNIVERSE_QUERY | NO | OK |
| server.py:3073-3075 | admin_seed_job_check() | is_active, exchange, asset_type | YES | FIX NEEDED |
| server.py:3151-3153 | admin_batch_status() | is_active, exchange, asset_type | YES | FIX NEEDED |
| server.py:3197-3199 | admin_fundamentals_check() | asset_type, exchange, is_active | YES | FIX NEEDED |
| server.py:3270-3272 | admin_prices_check() | asset_type, exchange, is_active | YES | FIX NEEDED |
| whitelist_service.py:614-618 | search_whitelist() | is_active, asset_type, exchange | YES | FIX NEEDED |
| whitelist_service.py:548 | get_whitelist_stats() | is_visible=True | NO | OK |
| key_metrics_service.py:66 | compute_daily_key_metrics() | is_visible=True | NO | OK |
| local_metrics_service.py:552 | get_peer_median() | is_visible=True | NO | OK |
| local_metrics_service.py:563 | get_peer_median() | is_visible=True | NO | OK |

VIOLATIONS FOUND: 5 locations
- server.py: 4 admin endpoints (internal, but should still use is_visible)
- whitelist_service.py: 1 search endpoint (USER-FACING - CRITICAL)

CRITICAL FIX NEEDED:
1. whitelist_service.py:search_whitelist() - User-facing search
2. server.py admin endpoints - Internal consistency
"""

print(AUDIT_TABLE)
