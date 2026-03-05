"""
Provider debug snapshot service.

Stores provider-only sections in a dedicated debug collection so they are
strictly separated from production fundamentals.
"""

from datetime import datetime, timezone
from typing import Any, Dict, Optional

import logging

logger = logging.getLogger("richstox.provider_debug")

# Sections removed by whitelist / raw-facts policy and stored for debug only.
PROVIDER_DEBUG_SECTIONS = (
    "Highlights",
    "Valuation",
    "Technicals",
    "AnalystRatings",
    "ESGScores",
)

GENERAL_DEBUG_FIELDS = (
    "Officers",
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
)


def _normalize_ticker(ticker: str) -> str:
    t = (ticker or "").upper().strip()
    if not t:
        return t
    return t if t.endswith(".US") else f"{t}.US"


def _strip_empty_map(data: Dict[str, Any]) -> Dict[str, Any]:
    return {k: v for k, v in data.items() if v not in (None, "", [], {})}


def build_provider_debug_snapshot(
    ticker: str,
    raw_payload: Dict[str, Any],
    source_job: str,
    audit_info: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Build a debug-only snapshot from provider payload.

    Production reads MUST NOT use this collection.
    """
    now = datetime.now(timezone.utc)
    ticker_full = _normalize_ticker(ticker)

    forbidden_sections: Dict[str, Any] = {}
    for section in PROVIDER_DEBUG_SECTIONS:
        value = raw_payload.get(section)
        if value not in (None, "", [], {}):
            forbidden_sections[section] = value

    general = raw_payload.get("General") or {}
    general_forbidden = _strip_empty_map(
        {field: general.get(field) for field in GENERAL_DEBUG_FIELDS if field in general}
    )
    if general_forbidden:
        forbidden_sections["GeneralForbiddenFields"] = general_forbidden

    estimated_fields = 0
    for value in forbidden_sections.values():
        if isinstance(value, dict):
            estimated_fields += len(value)
        elif isinstance(value, list):
            estimated_fields += len(value)
        else:
            estimated_fields += 1

    snapshot = {
        "ticker": ticker_full,
        "provider": "EODHD",
        "endpoint": "fundamentals",
        "source_job": source_job,
        "debug_only": True,
        "runtime_allowed": False,
        "forbidden_sections": forbidden_sections,
        "sections_present": sorted(list(forbidden_sections.keys())),
        "forbidden_fields_estimate": estimated_fields,
        "last_captured_at": now,
        "updated_at": now,
    }

    if audit_info:
        snapshot["whitelist_audit"] = {
            "whitelist_version": audit_info.get("whitelist_version"),
            "sections_stripped": audit_info.get("sections_stripped", []),
            "fields_stripped_count": audit_info.get("fields_stripped_count", 0),
        }

    return snapshot


async def upsert_provider_debug_snapshot(
    db,
    ticker: str,
    raw_payload: Dict[str, Any],
    source_job: str,
    audit_info: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Upsert provider debug snapshot.

    Best-effort only: failures are logged and never fail production sync.
    """
    try:
        snapshot = build_provider_debug_snapshot(
            ticker=ticker,
            raw_payload=raw_payload or {},
            source_job=source_job,
            audit_info=audit_info,
        )
        now = snapshot["last_captured_at"]

        await db.provider_debug_snapshot.update_one(
            {"ticker": snapshot["ticker"]},
            {
                "$set": snapshot,
                "$setOnInsert": {"first_captured_at": now, "capture_count": 0},
                "$inc": {"capture_count": 1},
            },
            upsert=True,
        )

        return {
            "stored": True,
            "ticker": snapshot["ticker"],
            "sections": snapshot["sections_present"],
        }
    except Exception as exc:
        logger.warning(f"provider_debug_snapshot upsert failed for {ticker}: {exc}")
        return {"stored": False, "ticker": _normalize_ticker(ticker), "error": str(exc)}
