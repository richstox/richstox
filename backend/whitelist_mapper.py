# ==============================================================================
# RICHSTOX FUNDAMENTALS WHITELIST MAPPER
# ==============================================================================
# BINDING: This is the ONLY approved fields list for fundamentals storage.
# Status: LOCKED. No changes without explicit Richard approval (2026-02-25).
# ==============================================================================

"""
RICHSTOX FUNDAMENTALS - FINÁLNÍ ALLOWED FIELDS (LOCKED)

Tento seznam je ZÁKONEM pro whitelist_mapper.py. 
Vše, co zde není ✅ KEEP, bude nemilosrdně zahozeno.

1. VIZITKA FIRMY (General)
2. OWNERSHIP & TRANSACTIONS  
3. FINANČNÍ VÝKAZY & HISTORIE (RAW DATA)
4. CO SE SMAŽE (Zákaz ukládání)

Status: LOCKED. Žádné změny bez explicitního schválení.
"""

from typing import Dict, Any, List, Tuple
from datetime import datetime, timezone
import logging

logger = logging.getLogger("richstox.whitelist_mapper")

# ==============================================================================
# WHITELIST VERSION (for audit trail)
# ==============================================================================
WHITELIST_VERSION = "2026-02-25"
WHITELIST_STATUS = "LOCKED"

# ==============================================================================
# APPROVED FIELDS - KEEP ONLY THESE
# ==============================================================================

# 1. VIZITKA FIRMY (General)
GENERAL_ALLOWED_FIELDS = {
    "Name",
    "IPODate", 
    "Description",
    "Address",
    "City",
    "State",
    "CountryName",
    "CountryISO",
    "WebURL",
    "LogoURL",
    "FullTimeEmployees",
    "Phone",
    "Sector",
    "Industry",
    "GicSector",
    "GicIndustry",
    "ISIN",
    "CUSIP",
    "CIK",
    "Code",
    "Type",
    "Exchange",
    "CurrencyCode",
    "FiscalYearEnd",
    "IsDelisted",
}

# 2. OWNERSHIP & TRANSACTIONS - KEEP ENTIRE SECTIONS
KEEP_ENTIRE_SECTIONS = {
    "Holders",              # institutional + insider ownership
    "InsiderTransactions",  # buys/sells at market price
    "Financials",           # Income_Statement, Balance_Sheet, Cash_Flow
    "Earnings",             # History, Annual (but NOT Trend)
    "SplitsDividends",      # RAW dividend and split history
    "outstandingShares",    # historical share count evolution
}

# 3. SHARES STATS - ONLY RAW COUNTS
SHARES_STATS_ALLOWED = {
    "SharesOutstanding",    # RAW share count for Market Cap
    "SharesFloat",          # shares in circulation
}

# 4. EARNINGS - KEEP ONLY THESE
EARNINGS_ALLOWED_KEYS = {
    "History",
    "Annual",
    # "Trend" is DELETED (analyst estimates)
}

# ==============================================================================
# FORBIDDEN SECTIONS - DELETE ENTIRELY
# ==============================================================================
FORBIDDEN_SECTIONS = {
    "Highlights",       # P/E, Market Cap, EBITDA, Yield, Margins — all computed
    "Valuation",        # EV/EBITDA, Price/Book, Price/Sales — all ratios
    "Technicals",       # Beta, 52w High/Low, Moving Averages — all computed
    "AnalystRatings",   # analyst recommendations and target prices
    "ESGScores",        # third-party ESG scores
}

# FORBIDDEN FIELDS IN GENERAL
GENERAL_FORBIDDEN_FIELDS = {
    "Officers",         # USER REQUIREMENT: DELETE
    "Listings",
    "AddressData",
    "OpenFigi",
    "LEI",
    "PrimaryTicker",
    "EmployerIdNumber",
    "InternationalDomestic",
    "HomeCategory",
    "CurrencyName",
    "CurrencySymbol",
    "UpdatedAt",
    "GicGroup",
    "GicSubIndustry",
}


def apply_whitelist(raw_payload: Dict[str, Any], ticker: str) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Apply whitelist mapper to raw EODHD fundamentals payload.
    
    Args:
        raw_payload: Raw response from EODHD /fundamentals endpoint
        ticker: Ticker symbol for logging
        
    Returns:
        Tuple of (filtered_payload, audit_info)
        - filtered_payload: Only approved fields
        - audit_info: What was kept/stripped for logging
    """
    filtered = {}
    audit = {
        "ticker": ticker,
        "whitelist_version": WHITELIST_VERSION,
        "sections_kept": [],
        "sections_stripped": [],
        "fields_kept_count": 0,
        "fields_stripped_count": 0,
        "stripped_details": {},
    }
    
    # Process each top-level section
    for section, value in raw_payload.items():
        
        # FORBIDDEN SECTIONS - DELETE ENTIRELY
        if section in FORBIDDEN_SECTIONS:
            audit["sections_stripped"].append(section)
            if isinstance(value, dict):
                audit["fields_stripped_count"] += len(value)
            audit["stripped_details"][section] = "DELETED (forbidden section)"
            continue
        
        # GENERAL - Filter to allowed fields only
        if section == "General":
            filtered_general = {}
            stripped_general = []
            
            for field, field_value in (value or {}).items():
                if field in GENERAL_ALLOWED_FIELDS:
                    filtered_general[field] = field_value
                    audit["fields_kept_count"] += 1
                elif field in GENERAL_FORBIDDEN_FIELDS:
                    stripped_general.append(field)
                    audit["fields_stripped_count"] += 1
                else:
                    # Unknown field - strip for safety
                    stripped_general.append(field)
                    audit["fields_stripped_count"] += 1
            
            if filtered_general:
                filtered["General"] = filtered_general
                audit["sections_kept"].append("General")
            
            if stripped_general:
                audit["stripped_details"]["General"] = stripped_general
            continue
        
        # SHARES STATS - Only raw counts
        if section == "SharesStats":
            filtered_shares = {}
            stripped_shares = []
            
            for field, field_value in (value or {}).items():
                if field in SHARES_STATS_ALLOWED:
                    filtered_shares[field] = field_value
                    audit["fields_kept_count"] += 1
                else:
                    stripped_shares.append(field)
                    audit["fields_stripped_count"] += 1
            
            if filtered_shares:
                filtered["SharesStats"] = filtered_shares
                audit["sections_kept"].append("SharesStats")
            
            if stripped_shares:
                audit["stripped_details"]["SharesStats"] = stripped_shares
            continue
        
        # EARNINGS - Keep only History and Annual, not Trend
        if section == "Earnings":
            filtered_earnings = {}
            stripped_earnings = []
            
            for key, key_value in (value or {}).items():
                if key in EARNINGS_ALLOWED_KEYS:
                    filtered_earnings[key] = key_value
                    audit["fields_kept_count"] += 1
                else:
                    stripped_earnings.append(key)
                    audit["fields_stripped_count"] += 1
            
            if filtered_earnings:
                filtered["Earnings"] = filtered_earnings
                audit["sections_kept"].append("Earnings")
            
            if stripped_earnings:
                audit["stripped_details"]["Earnings"] = stripped_earnings
            continue
        
        # KEEP ENTIRE SECTIONS
        if section in KEEP_ENTIRE_SECTIONS:
            if value:
                filtered[section] = value
                audit["sections_kept"].append(section)
                # Count fields (rough estimate)
                if isinstance(value, dict):
                    audit["fields_kept_count"] += _count_fields(value)
                elif isinstance(value, list):
                    audit["fields_kept_count"] += len(value)
            continue
        
        # Unknown section - strip for safety
        audit["sections_stripped"].append(section)
        if isinstance(value, dict):
            audit["fields_stripped_count"] += len(value)
        audit["stripped_details"][section] = "DELETED (unknown section)"
    
    logger.info(
        f"[{ticker}] Whitelist applied: kept {audit['fields_kept_count']} fields, "
        f"stripped {audit['fields_stripped_count']} fields"
    )
    
    return filtered, audit


def _count_fields(obj: Any, depth: int = 0) -> int:
    """Recursively count fields in nested dict/list."""
    if depth > 5:  # Prevent infinite recursion
        return 1
    
    if isinstance(obj, dict):
        count = len(obj)
        for v in obj.values():
            if isinstance(v, (dict, list)):
                count += _count_fields(v, depth + 1)
        return count
    elif isinstance(obj, list):
        return len(obj)
    return 1


def get_whitelist_document() -> Dict[str, Any]:
    """
    Return the locked whitelist document for Admin Panel display.
    This is the OFFICIAL, IMMUTABLE specification.
    """
    return {
        "version": WHITELIST_VERSION,
        "status": WHITELIST_STATUS,
        "approved_by": "Richard (kurtarichard@gmail.com)",
        "approved_date": "2026-02-25",
        "is_locked": True,
        "sections": {
            "1_company_details": {
                "title": "VIZITKA FIRMY (General)",
                "fields": sorted(list(GENERAL_ALLOWED_FIELDS)),
                "status": "KEEP"
            },
            "2_ownership_transactions": {
                "title": "OWNERSHIP & TRANSACTIONS",
                "fields": [
                    "Holders (institutional + insider ownership)",
                    "InsiderTransactions (buys/sells at market price)",
                    "SharesStats.SharesOutstanding (RAW share count)",
                    "SharesStats.SharesFloat (shares in circulation)"
                ],
                "status": "KEEP"
            },
            "3_financial_statements": {
                "title": "FINANČNÍ VÝKAZY & HISTORIE (RAW DATA)",
                "fields": [
                    "Financials.Income_Statement (yearly + quarterly)",
                    "Financials.Balance_Sheet (yearly + quarterly)",
                    "Financials.Cash_Flow (yearly + quarterly)",
                    "Earnings.History (RAW EPS history)",
                    "Earnings.Annual (RAW annual earnings)",
                    "SplitsDividends (RAW dividend and split history)",
                    "outstandingShares (historical share count)"
                ],
                "status": "KEEP"
            },
            "4_forbidden": {
                "title": "CO SE SMAŽE (Zákaz ukládání)",
                "fields": [
                    "Highlights (P/E, Market Cap, EBITDA, Yield, Margins)",
                    "Valuation (EV/EBITDA, Price/Book, Price/Sales)",
                    "Technicals (Beta, 52w High/Low, Moving Averages)",
                    "AnalystRatings (recommendations, target prices)",
                    "General.Officers (USER REQUIREMENT)",
                    "ESGScores (third-party scores)",
                    "Earnings.Trend (analyst estimates)",
                    "All computed metrics, TTM aggregates"
                ],
                "status": "DELETE"
            }
        },
        "binding_rules": [
            "Whitelist je ZÁKONEM - žádné změny bez schválení Richardem",
            "Vše co není v KEEP bude zahozeno při ukládání",
            "Whitelist mapper je jediný povolený způsob ukládání fundamentů",
            "Audit trail se ukládá do ops_job_runs"
        ]
    }


def verify_whitelist_integrity() -> Tuple[bool, str]:
    """
    Verify whitelist mapper integrity for startup guard.
    Returns (passed, message).
    """
    # Check version is set
    if not WHITELIST_VERSION:
        return False, "WHITELIST_VERSION is not set"
    
    # Check status is LOCKED
    if WHITELIST_STATUS != "LOCKED":
        return False, f"WHITELIST_STATUS is {WHITELIST_STATUS}, expected LOCKED"
    
    # Check forbidden sections are defined
    if not FORBIDDEN_SECTIONS:
        return False, "FORBIDDEN_SECTIONS is empty"
    
    # Check Officers is in forbidden
    if "Officers" not in GENERAL_FORBIDDEN_FIELDS:
        return False, "Officers must be in GENERAL_FORBIDDEN_FIELDS (user requirement)"
    
    return True, f"Whitelist v{WHITELIST_VERSION} integrity OK"
