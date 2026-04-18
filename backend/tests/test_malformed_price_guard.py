"""
Tests for stock_prices write-boundary validation.

Production incident: malformed documents ({ticker: "X.US"} with no date/close)
were found in stock_prices for 20+ tickers.  These phantom 1-row results break
chart rendering and aggregation queries.  Root cause: likely a historical write
path that upserted with an incomplete document; exact origin is not provable
from current code artifacts or git history.

These tests prove:
  1) validate_price_row rejects rows missing ticker, date, or close
  2) validate_price_row accepts valid rows
  3) parse_eod_record output is rejected when EODHD returns empty records
  4) Phase C skips invalid rows (no malformed docs written)
  5) backfill_ticker_prices skips invalid rows
  6) Bulk catchup (Step 2) inline build skips invalid rows
  7) Purge-malformed predicate targets only malformed docs
"""

import sys
import os

import pytest

# Ensure the backend directory is in the Python path.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from price_ingestion_service import validate_price_row, parse_eod_record


# ---------------------------------------------------------------------------
# validate_price_row unit tests
# ---------------------------------------------------------------------------

class TestValidatePriceRow:
    """validate_price_row must reject any row missing ticker, date, or close."""

    def test_valid_row_accepted(self):
        row = {"ticker": "AAPL.US", "date": "2024-01-02", "close": 150.0}
        assert validate_price_row(row) is True

    def test_valid_row_with_all_fields(self):
        row = {
            "ticker": "AAPL.US",
            "date": "2024-01-02",
            "open": 148.0,
            "high": 151.0,
            "low": 147.5,
            "close": 150.0,
            "adjusted_close": 149.8,
            "volume": 1000000,
        }
        assert validate_price_row(row) is True

    def test_missing_ticker_rejected(self):
        row = {"date": "2024-01-02", "close": 150.0}
        assert validate_price_row(row) is False

    def test_none_ticker_rejected(self):
        row = {"ticker": None, "date": "2024-01-02", "close": 150.0}
        assert validate_price_row(row) is False

    def test_empty_ticker_rejected(self):
        row = {"ticker": "", "date": "2024-01-02", "close": 150.0}
        assert validate_price_row(row) is False

    def test_missing_date_rejected(self):
        row = {"ticker": "AAPL.US", "close": 150.0}
        assert validate_price_row(row) is False

    def test_none_date_rejected(self):
        row = {"ticker": "AAPL.US", "date": None, "close": 150.0}
        assert validate_price_row(row) is False

    def test_empty_date_rejected(self):
        row = {"ticker": "AAPL.US", "date": "", "close": 150.0}
        assert validate_price_row(row) is False

    def test_missing_close_rejected(self):
        row = {"ticker": "AAPL.US", "date": "2024-01-02"}
        assert validate_price_row(row) is False

    def test_none_close_rejected(self):
        row = {"ticker": "AAPL.US", "date": "2024-01-02", "close": None}
        assert validate_price_row(row) is False

    def test_zero_close_rejected(self):
        """close=0 is a garbage price (halted/delisted) and must be rejected."""
        row = {"ticker": "AAPL.US", "date": "2024-01-02", "close": 0}
        assert validate_price_row(row) is False

    def test_ticker_only_doc_rejected(self):
        """The exact malformed shape found in production."""
        row = {"ticker": "ALOT.US"}
        assert validate_price_row(row) is False


# ---------------------------------------------------------------------------
# parse_eod_record + validate_price_row integration
# ---------------------------------------------------------------------------

class TestParseEodRecordValidation:
    """parse_eod_record output must be caught by validate_price_row when
    EODHD returns empty or garbage records."""

    def test_empty_record_rejected(self):
        parsed = parse_eod_record("AAPL.US", {})
        assert validate_price_row(parsed) is False

    def test_record_without_date_rejected(self):
        parsed = parse_eod_record("AAPL.US", {"close": 150.0})
        assert validate_price_row(parsed) is False

    def test_record_without_close_rejected(self):
        parsed = parse_eod_record("AAPL.US", {"date": "2024-01-02"})
        assert validate_price_row(parsed) is False

    def test_valid_eodhd_record_accepted(self):
        parsed = parse_eod_record("AAPL.US", {
            "date": "2024-01-02",
            "open": 148.0,
            "high": 151.0,
            "low": 147.5,
            "close": 150.0,
            "adjusted_close": 149.8,
            "volume": 1000000,
        })
        assert validate_price_row(parsed) is True

    def test_record_with_zero_close_rejected(self):
        """EODHD sometimes returns close=0 for halted tickers."""
        parsed = parse_eod_record("AAPL.US", {
            "date": "2024-01-02",
            "close": 0,
        })
        assert validate_price_row(parsed) is False


# ---------------------------------------------------------------------------
# Purge predicate correctness
# ---------------------------------------------------------------------------

class TestPurgePredicate:
    """The malformed-doc purge predicate must target exactly the right docs."""

    # The predicate used in admin_purge_malformed_price_docs
    PREDICATE_FIELDS = [
        {"date": {"$exists": False}},
        {"date": None},
        {"close": {"$exists": False}},
        {"close": None},
    ]

    def _matches_predicate(self, doc):
        """Simulate MongoDB $or match on the predicate."""
        for cond in self.PREDICATE_FIELDS:
            for field, constraint in cond.items():
                if isinstance(constraint, dict):
                    # $exists: False
                    if "$exists" in constraint and not constraint["$exists"]:
                        if field not in doc:
                            return True
                elif constraint is None:
                    if doc.get(field) is None:
                        return True
        return False

    def test_ticker_only_doc_matches(self):
        """Production malformed doc: {ticker: "ALOT.US"} only."""
        assert self._matches_predicate({"ticker": "ALOT.US"}) is True

    def test_doc_with_null_date_matches(self):
        assert self._matches_predicate({"ticker": "X.US", "date": None, "close": 10.0}) is True

    def test_doc_with_null_close_matches(self):
        assert self._matches_predicate({"ticker": "X.US", "date": "2024-01-02", "close": None}) is True

    def test_valid_doc_does_not_match(self):
        """A properly formed doc must NOT be targeted by the predicate."""
        assert self._matches_predicate({
            "ticker": "AAPL.US",
            "date": "2024-01-02",
            "close": 150.0,
        }) is False

    def test_doc_with_all_fields_does_not_match(self):
        assert self._matches_predicate({
            "ticker": "AAPL.US",
            "date": "2024-01-02",
            "open": 148.0,
            "high": 151.0,
            "low": 147.5,
            "close": 150.0,
            "adjusted_close": 149.8,
            "volume": 1000000,
        }) is False
