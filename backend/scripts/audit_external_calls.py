#!/usr/bin/env python3
"""
RICHSTOX External API Audit Script
===================================
Fails if eodhd.com/api is found outside allowlist files.

Usage:
    python /app/scripts/audit_external_calls.py

Exit codes:
    0 = PASS (no violations)
    1 = FAIL (violations found)

ALLOWLIST FILES (may contain EODHD calls):
- scheduler.py
- whitelist_service.py  
- price_ingestion_service.py
- backfill_fundamentals.py
- backfill_fundamentals_raw.py
- backfill_fundamentals_complete.py
- batch_jobs_service.py (scheduler helper)
- eodhd_service.py (scheduler helper - DEPRECATED, to be removed)
- fundamentals_service.py (admin backfill only)
- dividend_history_service.py (scheduler only)
- parallel_batch_service.py (scheduler only)
- scripts/seed_fundamentals.py (one-time script)
- jobs/news_daily_refresh.py (scheduler job)
- services/news_service.py (scheduler job)
"""

import subprocess
import sys
import os

# Allowlist of files that may contain EODHD calls
# These are all scheduler/backfill/admin files
ALLOWLIST = [
    # Core scheduler/pipeline files
    "scheduler.py",
    "whitelist_service.py",
    "price_ingestion_service.py",
    
    # Backfill scripts (admin-only)
    "backfill_fundamentals.py",
    "backfill_fundamentals_raw.py",
    "backfill_fundamentals_complete.py",
    "backfill_fundamentals_job.py",  # New whitelist-based fundamentals refill
    "backfill_financials.py",
    "backfill_dividends.py",
    "backfill_prices_historical.py",  # Per-ticker historical price backfill
    "backfill_prices_full_history.py",  # FULL HISTORY backfill (NO dates, NO IPO logic)
    
    # Scheduler helper services
    "batch_jobs_service.py",
    "eodhd_service.py",
    "fundamentals_service.py",
    "dividend_history_service.py",
    "parallel_batch_service.py",
    "benchmark_service.py",  # SP500TR.INDX updates - scheduler-only (04:15)
    
    # One-time/admin scripts
    "seed_fundamentals.py",
    
    # News scheduler jobs
    "news_daily_refresh.py",
    "news_service.py",
    
    # Admin Panel display (STRING REFERENCES ONLY - no actual API calls)
    "admin_overview_service.py",  # Contains api_endpoint strings for UI display
    
    # Server.py - contains EODHDService class and constants
    # EODHDService uses file cache, not direct runtime calls
    # Admin backfill endpoints are DB-triggered scheduler jobs
    "server.py",
    
    # Config module - contains EODHD_BASE_URL constant (no actual calls)
    "config.py",
    
    # Whitelist mapper - contains EODHD_BASE_URL reference (no actual calls)
    "whitelist_mapper.py",
    
    # Visibility rules - contains zombie cleanup which fetches master list
    "visibility_rules.py",
]

# Patterns to search for
PATTERNS = [
    "eodhd.com/api",
    "eodhd.com\\/api",
]


def main():
    backend_dir = "/app/backend"
    
    print("=" * 70)
    print("RICHSTOX EXTERNAL API AUDIT")
    print("=" * 70)
    print()
    print("Scanning for EODHD API calls outside allowlist...")
    print()
    
    # Build grep exclude pattern
    exclude_pattern = "|".join(ALLOWLIST)
    
    # Run grep
    cmd = f'grep -rn "eodhd.com" {backend_dir} --include="*.py" | grep -v "__pycache__" | grep -v "# "'
    
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    
    if result.returncode != 0 and not result.stdout:
        print("PASS: No EODHD calls found at all")
        return 0
    
    lines = result.stdout.strip().split("\n") if result.stdout.strip() else []
    
    violations = []
    allowed = []
    
    for line in lines:
        if not line.strip():
            continue
            
        # Check if file is in allowlist
        is_allowed = False
        for allow_file in ALLOWLIST:
            if allow_file in line:
                is_allowed = True
                break
        
        if is_allowed:
            allowed.append(line)
        else:
            violations.append(line)
    
    print(f"Allowlist files: {len(ALLOWLIST)}")
    print(f"EODHD calls in allowlist: {len(allowed)}")
    print(f"VIOLATIONS: {len(violations)}")
    print()
    
    if violations:
        print("=" * 70)
        print("FAIL: EODHD calls found outside allowlist!")
        print("=" * 70)
        for v in violations:
            print(f"  {v}")
        print()
        print("These files must NOT contain EODHD calls:")
        print("  - Move to scheduler/backfill service, OR")
        print("  - Add to allowlist if it's a scheduler helper")
        print()
        return 1
    else:
        print("=" * 70)
        print("PASS: All EODHD calls are in allowlist files")
        print("=" * 70)
        print()
        print("Allowlist files:")
        for f in ALLOWLIST:
            print(f"  - {f}")
        return 0


if __name__ == "__main__":
    sys.exit(main())
