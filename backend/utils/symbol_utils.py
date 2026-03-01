"""
RICHSTOX Symbol Normalization Utilities
=======================================
Canonical symbol format: US-only base ticker, uppercase, no suffix.

Examples:
- "AAPL.US" -> "AAPL"
- "aapl" -> "AAPL"
- "GOOG.US" -> "GOOG"

This ensures consistency across:
- Subscriptions (talk_subscriptions)
- Post creation (talk_posts.symbol, talk_posts.symbols)
- Filtering
- Notifications
"""

import re
from typing import List, Optional

# Suffixes to remove (exchange identifiers)
EXCHANGE_SUFFIXES = [".US", ".NYSE", ".NASDAQ", ".AMEX"]


def normalize_symbol(symbol: Optional[str]) -> Optional[str]:
    """
    Normalize a stock symbol to canonical format.
    
    Canonical format:
    - Uppercase
    - No exchange suffix (e.g., .US, .NYSE)
    - Stripped of whitespace
    
    Args:
        symbol: Raw symbol input (e.g., "AAPL.US", "aapl", "  GOOG  ")
    
    Returns:
        Canonical symbol (e.g., "AAPL") or None if input is None/empty
    
    Examples:
        >>> normalize_symbol("AAPL.US")
        'AAPL'
        >>> normalize_symbol("goog")
        'GOOG'
        >>> normalize_symbol(None)
        None
    """
    if not symbol:
        return None
    
    # Strip whitespace and convert to uppercase
    result = symbol.strip().upper()
    
    # Remove known exchange suffixes
    for suffix in EXCHANGE_SUFFIXES:
        if result.endswith(suffix.upper()):
            result = result[:-len(suffix)]
            break
    
    # Final cleanup - only alphanumeric and some special chars like - and .
    # But we already removed the .US suffix
    result = result.strip()
    
    return result if result else None


def normalize_symbols(symbols: Optional[List[str]]) -> List[str]:
    """
    Normalize a list of stock symbols.
    
    Args:
        symbols: List of raw symbols
    
    Returns:
        List of canonical symbols (empty list if input is None/empty)
    
    Examples:
        >>> normalize_symbols(["AAPL.US", "goog", "MSFT"])
        ['AAPL', 'GOOG', 'MSFT']
    """
    if not symbols:
        return []
    
    result = []
    for sym in symbols:
        normalized = normalize_symbol(sym)
        if normalized:
            result.append(normalized)
    
    return result


def is_valid_symbol(symbol: Optional[str]) -> bool:
    """
    Check if a symbol is valid (after normalization).
    
    Args:
        symbol: Symbol to validate
    
    Returns:
        True if valid, False otherwise
    """
    if not symbol:
        return False
    
    normalized = normalize_symbol(symbol)
    if not normalized:
        return False
    
    # Basic validation: 1-10 alphanumeric characters
    return bool(re.match(r'^[A-Z0-9]{1,10}$', normalized))
