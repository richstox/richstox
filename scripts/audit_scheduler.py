#!/usr/bin/env python3
"""
RICHSTOX Scheduler Audit Script

Verifies that /app/backend/scheduler.py matches the binding specification
in /app/docs/SCHEDULER_JOBS.md.

Run: python3 /app/scripts/audit_scheduler.py
Exit code 0 = PASS, Exit code 1 = FAIL

BINDING: Do not modify without Richard's approval (kurtarichard@gmail.com).
"""

import re
import sys

# Expected job configuration (from SCHEDULER_JOBS.md)
# NOTE: price_sync and fundamentals_sync are dependency-driven (after Step 1/2),
# not time-driven. They are tracked in last_run but not scheduled by hour/minute.
EXPECTED_JOBS = {
    "market_calendar": {"day": "Mon-Sat", "hour": 2, "minute": 0},
    "universe_seed": {"day": "Mon-Sat", "hour": 3, "minute": 0},
    "price_sync": {"day": "Mon-Sat", "scheduling": "dependency"},
    "fundamentals_sync": {"day": "Mon-Sat", "scheduling": "dependency"},
    "benchmark_update": {"day": "Mon-Sat", "hour": 4, "minute": 15},
    "backfill_all": {"day": "Mon-Sat", "hour": 5, "minute": 0},
    "key_metrics": {"day": "Mon-Sat", "hour": 5, "minute": 0},
    "pain_cache": {"day": "Mon-Sat", "hour": 5, "minute": 0},
    "peer_medians": {"day": "Mon-Sat", "hour": 5, "minute": 30},
    "admin_report": {"day": "Mon-Sat", "hour": 6, "minute": 0},
    "news_refresh": {"day": "Sun-Sat", "hour": 13, "minute": 0},
}

EXPECTED_CONSTANTS = {
    "UNIVERSE_SEED_HOUR": 3,
    "UNIVERSE_SEED_MINUTE": 0,
    "UNIVERSE_SEED_DAY": 6,  # Sunday (exclusion day — news-only)
    "MARKET_CALENDAR_HOUR": 2,
    "MARKET_CALENDAR_MINUTE": 0,
    "BENCHMARK_UPDATE_HOUR": 4,
    "BENCHMARK_UPDATE_MINUTE": 15,
    "BACKFILL_ALL_HOUR": 5,
    "BACKFILL_ALL_MINUTE": 0,
    "NEWS_REFRESH_HOUR": 13,
    "NEWS_REFRESH_MINUTE": 0,
    "KEY_METRICS_HOUR": 5,
    "KEY_METRICS_MINUTE": 0,
    "PEER_MEDIANS_HOUR": 5,
    "PEER_MEDIANS_MINUTE": 30,
    "PAIN_CACHE_HOUR": 5,
    "PAIN_CACHE_MINUTE": 0,
    "ADMIN_REPORT_HOUR": 6,
    "ADMIN_REPORT_MINUTE": 0,
}

# These constants exist in scheduler.py for legacy/documentation purposes but are
# NOT used for time-based scheduling (those jobs are dependency-driven).
# Treat as warnings only, not errors, if missing or mismatched.
LEGACY_CONSTANTS = {
    "PRICE_SYNC_HOUR": 4,
    "PRICE_SYNC_MINUTE": 0,
    "FUNDAMENTALS_SYNC_HOUR": 4,
    "FUNDAMENTALS_SYNC_MINUTE": 30,
}


def audit_scheduler():
    """Audit scheduler.py against the binding specification."""
    
    errors = []
    warnings = []
    
    # Read scheduler.py
    try:
        with open("/app/backend/scheduler.py", "r") as f:
            content = f.read()
    except FileNotFoundError:
        print("FAIL: /app/backend/scheduler.py not found")
        return 1
    
    # Check binding warning exists
    if "BINDING:" not in content:
        errors.append("Missing BINDING warning comment at top of file")
    
    if "Do not change schedule" not in content and "Do not change" not in content:
        warnings.append("Binding warning may be incomplete")
    
    # Check each expected constant
    for const_name, expected_value in EXPECTED_CONSTANTS.items():
        pattern = rf"{const_name}\s*=\s*(\d+)"
        match = re.search(pattern, content)
        
        if not match:
            errors.append(f"Missing constant: {const_name}")
        else:
            actual_value = int(match.group(1))
            if actual_value != expected_value:
                errors.append(f"{const_name}: expected {expected_value}, found {actual_value}")
    
    # Check legacy constants (warnings only — these exist but are not used for scheduling)
    for const_name, expected_value in LEGACY_CONSTANTS.items():
        pattern = rf"{const_name}\s*=\s*(\d+)"
        match = re.search(pattern, content)
        
        if not match:
            warnings.append(f"Legacy constant missing (not used for scheduling): {const_name}")
        else:
            actual_value = int(match.group(1))
            if actual_value != expected_value:
                warnings.append(f"Legacy constant {const_name}: expected {expected_value}, found {actual_value} (not used for scheduling)")
    
    # Check job names exist in last_run tracking
    for job_name in EXPECTED_JOBS.keys():
        if f'last_run["{job_name}"]' not in content and f"last_run['{job_name}']" not in content:
            if f'last_run.get("{job_name}")' not in content and f"last_run.get('{job_name}')" not in content:
                warnings.append(f"Job '{job_name}' may not be tracked in last_run")
    
    # Check for required function calls
    required_functions = [
        "sync_ticker_whitelist",  # Universe seed
        "news_daily_refresh",     # News refresh
        "run_daily_price_sync",   # Price sync
    ]
    
    for func in required_functions:
        if func not in content:
            errors.append(f"Missing required function call: {func}")
    
    # Check Sunday exclusion logic exists (weekday == UNIVERSE_SEED_DAY or is_sunday())
    has_sunday_check = "weekday == UNIVERSE_SEED_DAY" in content or "is_sunday()" in content
    if not has_sunday_check:
        errors.append("Missing Sunday exclusion check (weekday == UNIVERSE_SEED_DAY)")
    
    # Check news runs on Sunday - look for news_daily_refresh within the Sunday block
    if "weekday == UNIVERSE_SEED_DAY" in content:
        sunday_match = re.search(r'if\s+weekday\s*==\s*UNIVERSE_SEED_DAY:(.*?)await\s+asyncio\.sleep', content, re.DOTALL)
        if sunday_match:
            sunday_block = sunday_match.group(1)
            if "news_daily_refresh" not in sunday_block:
                errors.append("News refresh should run on Sunday but not found in Sunday block")
    elif "is_sunday()" in content:
        sunday_match = re.search(r'if\s+is_sunday\(\):(.*?)await\s+asyncio\.sleep', content, re.DOTALL)
        if sunday_match:
            sunday_block = sunday_match.group(1)
            if "news_daily_refresh" not in sunday_block:
                errors.append("News refresh should run on Sunday but not found in Sunday block")
    
    # Print results
    print("=" * 60)
    print("RICHSTOX SCHEDULER AUDIT")
    print("=" * 60)
    print(f"File: /app/backend/scheduler.py")
    print(f"Spec: /app/docs/SCHEDULER_JOBS.md")
    print("-" * 60)
    
    if errors:
        print(f"\n❌ ERRORS ({len(errors)}):")
        for e in errors:
            print(f"  - {e}")
    
    if warnings:
        print(f"\n⚠️  WARNINGS ({len(warnings)}):")
        for w in warnings:
            print(f"  - {w}")
    
    if not errors and not warnings:
        print("\n✅ All checks passed!")
    elif not errors:
        print("\n✅ No errors (warnings only)")
    
    print("-" * 60)
    
    if errors:
        print("RESULT: FAIL")
        print("\nAction required: Update scheduler.py to match SCHEDULER_JOBS.md")
        print("or get Richard's approval for changes.")
        return 1
    else:
        print("RESULT: PASS")
        return 0


if __name__ == "__main__":
    sys.exit(audit_scheduler())
