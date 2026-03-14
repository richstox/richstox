"""
Pipeline Truth Models Tests
============================
Tests for:
1. compute_visibility() — all 7 hard gates, fixed precedence
2. compute_visibility_failed_reason() — deterministic reason codes
3. get_canonical_sieve_query() — matches the same 7 gates as compute_visibility

Run: cd /app/backend && python -m pytest tests/test_pipeline_truth_models.py -v
"""

import sys
sys.path.insert(0, '/app/backend')

import pytest
from visibility_rules import (
    compute_visibility,
    compute_visibility_failed_reason,
    compute_visibility_step4_only,
    get_canonical_sieve_query,
    VisibilityFailedReason,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _full_ticker(**overrides) -> dict:
    """Return a ticker doc that passes all 7 visibility gates."""
    base = {
        "ticker": "TEST.US",
        "is_seeded": True,
        "has_price_data": True,
        "sector": "Technology",
        "industry": "Software",
        "shares_outstanding": 1_000_000,
        "financial_currency": "USD",
        "is_delisted": False,
        "status": "active",
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# compute_visibility — happy path
# ---------------------------------------------------------------------------

class TestComputeVisibilityHappyPath:
    def test_fully_qualified_ticker_is_visible(self):
        doc = _full_ticker()
        is_visible, reason = compute_visibility(doc)
        assert is_visible is True
        assert reason is None

    def test_non_usd_currency_is_still_visible(self):
        """Any financial_currency value (EUR, CZK, …) is accepted."""
        for currency in ("EUR", "CZK", "GBP", "JPY"):
            doc = _full_ticker(financial_currency=currency)
            is_visible, reason = compute_visibility(doc)
            assert is_visible is True, f"Expected visible for currency={currency}"
            assert reason is None


# ---------------------------------------------------------------------------
# compute_visibility — each gate fails with the correct reason
# ---------------------------------------------------------------------------

class TestComputeVisibilityGates:
    def test_not_seeded_returns_not_seeded(self):
        doc = _full_ticker(is_seeded=False)
        is_visible, reason = compute_visibility(doc)
        assert is_visible is False
        assert reason == VisibilityFailedReason.NOT_SEEDED.value

    def test_missing_is_seeded_field_returns_not_seeded(self):
        doc = _full_ticker()
        del doc["is_seeded"]
        is_visible, reason = compute_visibility(doc)
        assert is_visible is False
        assert reason == VisibilityFailedReason.NOT_SEEDED.value

    def test_no_price_data_returns_no_price_data(self):
        doc = _full_ticker(has_price_data=False)
        is_visible, reason = compute_visibility(doc)
        assert is_visible is False
        assert reason == VisibilityFailedReason.NO_PRICE_DATA.value

    def test_empty_sector_returns_missing_sector(self):
        for val in (None, "", "  "):
            doc = _full_ticker(sector=val)
            is_visible, reason = compute_visibility(doc)
            assert is_visible is False, f"sector={val!r}"
            assert reason == VisibilityFailedReason.MISSING_SECTOR.value, f"sector={val!r}"

    def test_empty_industry_returns_missing_industry(self):
        for val in (None, "", "  "):
            doc = _full_ticker(industry=val)
            is_visible, reason = compute_visibility(doc)
            assert is_visible is False, f"industry={val!r}"
            assert reason == VisibilityFailedReason.MISSING_INDUSTRY.value, f"industry={val!r}"

    def test_zero_shares_returns_missing_shares(self):
        doc = _full_ticker(shares_outstanding=0)
        is_visible, reason = compute_visibility(doc)
        assert is_visible is False
        assert reason == VisibilityFailedReason.MISSING_SHARES.value

    def test_negative_shares_returns_missing_shares(self):
        doc = _full_ticker(shares_outstanding=-1)
        is_visible, reason = compute_visibility(doc)
        assert is_visible is False
        assert reason == VisibilityFailedReason.MISSING_SHARES.value

    def test_none_shares_returns_missing_shares(self):
        doc = _full_ticker(shares_outstanding=None)
        is_visible, reason = compute_visibility(doc)
        assert is_visible is False
        assert reason == VisibilityFailedReason.MISSING_SHARES.value

    def test_missing_currency_returns_missing_currency(self):
        for val in (None, "", "  "):
            doc = _full_ticker(financial_currency=val)
            is_visible, reason = compute_visibility(doc)
            assert is_visible is False, f"currency={val!r}"
            assert reason == VisibilityFailedReason.MISSING_CURRENCY.value, f"currency={val!r}"

    def test_is_delisted_true_returns_delisted(self):
        doc = _full_ticker(is_delisted=True)
        is_visible, reason = compute_visibility(doc)
        assert is_visible is False
        assert reason == VisibilityFailedReason.DELISTED.value

    def test_status_delisted_returns_delisted(self):
        doc = _full_ticker(status="delisted")
        is_visible, reason = compute_visibility(doc)
        assert is_visible is False
        assert reason == VisibilityFailedReason.DELISTED.value

    def test_status_delisted_mixed_case_returns_delisted(self):
        """status check must be case-insensitive: Delisted, DELISTED, etc."""
        for val in ("Delisted", "DELISTED", "DeLiStEd"):
            doc = _full_ticker(status=val)
            is_visible, reason = compute_visibility(doc)
            assert is_visible is False, f"Expected invisible for status={val!r}"
            assert reason == VisibilityFailedReason.DELISTED.value, f"status={val!r}"


# ---------------------------------------------------------------------------
# Deterministic precedence order
# ---------------------------------------------------------------------------

class TestComputeVisibilityPrecedence:
    """
    Fixed gate order:
      NOT_SEEDED > NO_PRICE_DATA > MISSING_SECTOR > MISSING_INDUSTRY
      > MISSING_SHARES > MISSING_CURRENCY > DELISTED
    """

    def test_not_seeded_beats_all_others(self):
        doc = _full_ticker(
            is_seeded=False,
            has_price_data=False,
            sector=None,
            is_delisted=True,
        )
        _, reason = compute_visibility(doc)
        assert reason == VisibilityFailedReason.NOT_SEEDED.value

    def test_no_price_data_beats_sector(self):
        doc = _full_ticker(has_price_data=False, sector=None)
        _, reason = compute_visibility(doc)
        assert reason == VisibilityFailedReason.NO_PRICE_DATA.value

    def test_missing_sector_beats_industry(self):
        doc = _full_ticker(sector=None, industry=None)
        _, reason = compute_visibility(doc)
        assert reason == VisibilityFailedReason.MISSING_SECTOR.value

    def test_missing_industry_beats_shares(self):
        doc = _full_ticker(industry=None, shares_outstanding=0)
        _, reason = compute_visibility(doc)
        assert reason == VisibilityFailedReason.MISSING_INDUSTRY.value

    def test_missing_shares_beats_currency(self):
        doc = _full_ticker(shares_outstanding=0, financial_currency=None)
        _, reason = compute_visibility(doc)
        assert reason == VisibilityFailedReason.MISSING_SHARES.value

    def test_missing_currency_beats_delisted(self):
        doc = _full_ticker(financial_currency=None, is_delisted=True)
        _, reason = compute_visibility(doc)
        assert reason == VisibilityFailedReason.MISSING_CURRENCY.value


# ---------------------------------------------------------------------------
# compute_visibility_failed_reason convenience helper
# ---------------------------------------------------------------------------

class TestComputeVisibilityFailedReason:
    def test_returns_none_for_visible_ticker(self):
        doc = _full_ticker()
        assert compute_visibility_failed_reason(doc) is None

    def test_returns_reason_string_for_invisible(self):
        doc = _full_ticker(has_price_data=False)
        reason = compute_visibility_failed_reason(doc)
        assert reason == VisibilityFailedReason.NO_PRICE_DATA.value

    def test_matches_compute_visibility_result(self):
        for failing_doc in [
            _full_ticker(is_seeded=False),
            _full_ticker(has_price_data=False),
            _full_ticker(sector=None),
            _full_ticker(industry=None),
            _full_ticker(shares_outstanding=0),
            _full_ticker(financial_currency=None),
            _full_ticker(is_delisted=True),
        ]:
            _, expected = compute_visibility(failing_doc)
            actual = compute_visibility_failed_reason(failing_doc)
            assert actual == expected


# ---------------------------------------------------------------------------
# compute_visibility_step4_only backward-compat alias
# ---------------------------------------------------------------------------

class TestComputeVisibilityStep4OnlyAlias:
    def test_visible_ticker(self):
        doc = _full_ticker()
        is_visible, reason = compute_visibility_step4_only(doc)
        assert is_visible is True
        assert reason is None

    def test_delegates_to_full_compute_visibility(self):
        doc = _full_ticker(has_price_data=False)
        result_alias = compute_visibility_step4_only(doc)
        result_full = compute_visibility(doc)
        assert result_alias == result_full


# ---------------------------------------------------------------------------
# get_canonical_sieve_query — gates must match compute_visibility
# ---------------------------------------------------------------------------

class TestGetCanonicalSieveQuery:
    """
    Verify that the Mongo query produced by get_canonical_sieve_query() would
    accept/reject the same documents as compute_visibility().

    We test this by checking that each failing condition also appears
    as an explicit filter in the sieve query.
    """

    def test_returns_dict(self):
        q = get_canonical_sieve_query()
        assert isinstance(q, dict)

    def test_sieve_requires_is_seeded(self):
        q = get_canonical_sieve_query()
        assert q.get("is_seeded") is True, "Sieve must require is_seeded == True"

    def test_sieve_requires_has_price_data(self):
        q = get_canonical_sieve_query()
        assert q.get("has_price_data") is True, "Sieve must require has_price_data == True"

    def test_sieve_excludes_empty_sector(self):
        q = get_canonical_sieve_query()
        assert "sector" in q, "Sieve must filter sector"
        sector_filter = q["sector"]
        assert "$nin" in sector_filter
        assert None in sector_filter["$nin"]
        assert "" in sector_filter["$nin"]

    def test_sieve_excludes_empty_industry(self):
        q = get_canonical_sieve_query()
        assert "industry" in q, "Sieve must filter industry"
        industry_filter = q["industry"]
        assert "$nin" in industry_filter
        assert None in industry_filter["$nin"]
        assert "" in industry_filter["$nin"]

    def test_sieve_requires_positive_shares(self):
        q = get_canonical_sieve_query()
        assert "shares_outstanding" in q, "Sieve must filter shares_outstanding"
        shares_filter = q["shares_outstanding"]
        assert "$gt" in shares_filter
        assert shares_filter["$gt"] == 0

    def test_sieve_excludes_empty_currency(self):
        q = get_canonical_sieve_query()
        assert "financial_currency" in q, "Sieve must filter financial_currency"
        currency_filter = q["financial_currency"]
        assert "$nin" in currency_filter
        assert None in currency_filter["$nin"]
        assert "" in currency_filter["$nin"]

    def test_sieve_excludes_delisted(self):
        q = get_canonical_sieve_query()
        assert "is_delisted" in q, "Sieve must filter is_delisted"
        delisted_filter = q["is_delisted"]
        assert "$ne" in delisted_filter
        assert delisted_filter["$ne"] is True

    def test_sieve_status_filter_is_case_insensitive(self):
        """
        The status gate must use a case-insensitive regex so that
        "Delisted", "DELISTED", etc. are excluded — matching compute_visibility
        which normalises status with .lower().
        """
        q = get_canonical_sieve_query()
        assert "status" in q, "Sieve must include a status filter"
        status_filter = q["status"]
        # Must use $not + $regex with case-insensitive option
        assert "$not" in status_filter, (
            f"status filter should use $not, got: {status_filter}"
        )
        regex_part = status_filter["$not"]
        assert "$regex" in regex_part, f"$not must contain $regex, got: {regex_part}"
        assert "$options" in regex_part and "i" in regex_part["$options"], (
            f"regex must be case-insensitive ($options: 'i'), got: {regex_part}"
        )

    def test_sieve_all_7_gates_present(self):
        q = get_canonical_sieve_query()
        required_keys = {
            "is_seeded",
            "has_price_data",
            "sector",
            "industry",
            "shares_outstanding",
            "financial_currency",
            "is_delisted",
        }
        missing = required_keys - set(q.keys())
        assert not missing, f"Sieve query is missing gates: {missing}"
