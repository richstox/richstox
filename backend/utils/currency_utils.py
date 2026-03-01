"""
Currency Extraction Utilities for RICHSTOX
==========================================
Extracts authoritative currency from EODHD fundamentals data.

The currency_symbol field location is inconsistent in EODHD data:
- Sometimes at top-level: Financials.Income_Statement.currency_symbol
- Sometimes nested: Financials.Income_Statement.quarterly[date].currency_symbol

This module provides a robust extraction function with fallback chain.
"""

from typing import Optional


def extract_statement_currency(fundamentals: dict) -> Optional[str]:
    """
    Extract authoritative currency from fundamentals with fallback chain.
    
    Priority order:
    1. Top-level: Financials.Income_Statement.currency_symbol
    2. Top-level: Financials.Balance_Sheet.currency_symbol
    3. Top-level: Financials.Cash_Flow.currency_symbol
    4. Nested: Income_Statement.quarterly[*].currency_symbol (newest→oldest)
    5. Nested: Income_Statement.yearly[*].currency_symbol (newest→oldest)
    6. Nested: Balance_Sheet.quarterly[*].currency_symbol (newest→oldest)
    7. Nested: Balance_Sheet.yearly[*].currency_symbol (newest→oldest)
    8. Nested: Cash_Flow.quarterly[*].currency_symbol (newest→oldest)
    9. Nested: Cash_Flow.yearly[*].currency_symbol (newest→oldest)
    
    Args:
        fundamentals: The fundamentals dict from tracked_tickers or EODHD API
        
    Returns:
        Currency code (e.g., "USD", "CNY") or None if not found.
    """
    if not fundamentals:
        return None
    
    financials = fundamentals.get("Financials", {})
    if not financials:
        return None
    
    STATEMENT_PRIORITY = ["Income_Statement", "Balance_Sheet", "Cash_Flow"]
    
    # Step 1-3: Check top-level currency_symbol
    for stmt_name in STATEMENT_PRIORITY:
        stmt = financials.get(stmt_name, {})
        if stmt and stmt.get("currency_symbol"):
            return stmt["currency_symbol"]
    
    # Step 4-9: Scan nested quarterly/yearly (newest→oldest)
    for stmt_name in STATEMENT_PRIORITY:
        stmt = financials.get(stmt_name, {})
        if not stmt:
            continue
        
        # Check quarterly first (usually more recent)
        quarterly = stmt.get("quarterly", {})
        if quarterly and isinstance(quarterly, dict):
            # Sort keys descending (newest first)
            for date_key in sorted(quarterly.keys(), reverse=True):
                entry = quarterly.get(date_key)
                if entry and isinstance(entry, dict) and entry.get("currency_symbol"):
                    return entry["currency_symbol"]
        
        # Then yearly
        yearly = stmt.get("yearly", {})
        if yearly and isinstance(yearly, dict):
            for date_key in sorted(yearly.keys(), reverse=True):
                entry = yearly.get(date_key)
                if entry and isinstance(entry, dict) and entry.get("currency_symbol"):
                    return entry["currency_symbol"]
    
    return None
