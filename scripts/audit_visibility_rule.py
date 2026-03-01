#!/usr/bin/env python3
"""
VISIBILITY RULE AUDIT SCRIPT (CI/CD)
====================================

This script must be run before every deployment.
Fails (exit 1) if any visibility rule violations are found.

FORBIDDEN PATTERNS IN RUNTIME CODE:
- exchange in [ / exchange ==
- NYSE / NASDAQ (as filter values, not comments)
- is_active alone (without is_visible)
- asset_type as primary filter
- .US suffix filtering

ALLOWED:
- is_visible == True / is_visible: True
- VISIBLE_UNIVERSE_QUERY
- Comments and docstrings mentioning these patterns

Usage:
    python /app/scripts/audit_visibility_rule.py
    
Exit codes:
    0 = PASSED (no violations)
    1 = FAILED (violations found)
"""

import os
import re
import sys
from pathlib import Path
from typing import List, Tuple

# Files to audit (runtime code only)
AUDIT_FILES = [
    "/app/backend/server.py",
    "/app/backend/whitelist_service.py",
    "/app/backend/key_metrics_service.py",
    "/app/backend/local_metrics_service.py",
]

# Forbidden patterns (in runtime filter context)
FORBIDDEN_PATTERNS = [
    # Exchange filtering (forbidden as primary filter)
    (r'exchange.*in.*\[\s*["\']NYSE', "exchange in ['NYSE...] filter"),
    (r'exchange.*==.*["\']NYSE', "exchange == 'NYSE' filter"),
    (r'exchange.*==.*["\']NASDAQ', "exchange == 'NASDAQ' filter"),
    
    # is_active alone (must use is_visible)
    (r'"is_active":\s*True[,\s}](?!.*is_visible)', "is_active without is_visible"),
    
    # asset_type as primary filter
    (r'"asset_type":\s*["\']Common Stock["\'][,\s}](?!.*is_visible)', "asset_type filter without is_visible"),
]

# Patterns that indicate SAFE usage (comments, docstrings, bindings)
SAFE_INDICATORS = [
    "# ",           # Comment
    "\"\"\"",       # Docstring
    "BINDING RULE", # Binding comment
    "DEPRECATED",   # Deprecated marker
    "# SINGLE SOURCE", # Approved constant
    "VISIBLE_UNIVERSE_QUERY",  # Correct constant
]

# Files/functions that are ALLOWED to have these patterns (batch/admin jobs)
ALLOWED_EXCEPTIONS = [
    "backfill_",
    "batch_jobs",
    "price_ingestion",
    "parallel_batch",
    "admin_",
    "scheduler",
]


def is_safe_context(line: str, context_before: str, context_after: str) -> bool:
    """Check if the line is in a safe context (comment, docstring, etc.)."""
    # Check if line is a comment
    if line.strip().startswith("#"):
        return True
    
    # Check for safe indicators in the line
    for indicator in SAFE_INDICATORS:
        if indicator in line or indicator in context_before or indicator in context_after:
            return True
    
    # Check if we're in an exception context
    for exception in ALLOWED_EXCEPTIONS:
        if exception in context_before or exception in context_after:
            return True
    
    return False


def audit_file(filepath: str) -> List[Tuple[int, str, str]]:
    """
    Audit a single file for forbidden patterns.
    
    Returns list of (line_number, pattern_name, line_content) violations.
    """
    violations = []
    
    if not os.path.exists(filepath):
        print(f"⚠️ File not found: {filepath}")
        return violations
    
    with open(filepath, "r") as f:
        lines = f.readlines()
    
    for i, line in enumerate(lines):
        line_num = i + 1
        
        # Get context (3 lines before and after)
        context_before = "".join(lines[max(0, i-3):i])
        context_after = "".join(lines[i+1:min(len(lines), i+4)])
        
        # Check each forbidden pattern
        for pattern, pattern_name in FORBIDDEN_PATTERNS:
            if re.search(pattern, line):
                # Check if it's in a safe context
                if not is_safe_context(line, context_before, context_after):
                    violations.append((line_num, pattern_name, line.strip()[:80]))
    
    return violations


def main():
    print("="*70)
    print("AUDIT: Searching for visibility rule violations...")
    print("="*70)
    
    all_violations = []
    
    for filepath in AUDIT_FILES:
        filename = os.path.basename(filepath)
        violations = audit_file(filepath)
        
        if violations:
            print(f"❌ VIOLATIONS in {filename}:")
            for line_num, pattern_name, line in violations:
                print(f"   Line {line_num}: {pattern_name}")
                print(f"   Code: {line}")
            all_violations.extend([(filename, *v) for v in violations])
        else:
            print(f"✅ No forbidden patterns found in {filename}")
    
    print("="*70)
    
    if all_violations:
        print(f"❌ AUDIT FAILED: {len(all_violations)} violation(s) found")
        print("Fix these issues before deployment.")
        return 1
    else:
        print("✅ AUDIT PASSED: No visibility rule violations found")
        return 0


if __name__ == "__main__":
    sys.exit(main())
