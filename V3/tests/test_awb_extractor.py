"""Unit tests for V3/core/awb_extractor.py — pure logic, no external deps."""
from __future__ import annotations

import sys
from pathlib import Path

# Ensure project root is on sys.path regardless of CWD
_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import pytest

from V3.core.awb_extractor import (
    _is_disqualified_candidate,
    _norm_digits_12,
    extract_awb_from_filename_strict,
    extract_awb_from_400_pattern,
    extract_candidates_from_text,
    extract_candidates_near_keywords,
)


# =============================================================================
# _is_disqualified_candidate
# =============================================================================

class TestIsDisqualifiedCandidate:
    def test_valid_12_digit(self):
        assert _is_disqualified_candidate("123456789012") is False

    def test_leading_zero_disqualified_by_default(self):
        assert _is_disqualified_candidate("012345678901") is True

    def test_leading_zero_allowed_for_tolerance(self):
        assert _is_disqualified_candidate("012345678901", for_tolerance=True) is False

    def test_too_short(self):
        assert _is_disqualified_candidate("12345678901") is True

    def test_too_long(self):
        assert _is_disqualified_candidate("1234567890123") is True

    def test_non_digit(self):
        assert _is_disqualified_candidate("12345678901A") is True

    def test_none(self):
        assert _is_disqualified_candidate(None) is True

    def test_empty_string(self):
        assert _is_disqualified_candidate("") is True


# =============================================================================
# _norm_digits_12
# =============================================================================

class TestNormDigits12:
    def test_pure_digits_passthrough(self):
        assert _norm_digits_12("399617498819") == "399617498819"

    def test_letter_substitution(self):
        # O→0, I→1, S→5 etc.
        assert _norm_digits_12("3996I7498819") == "399617498819"

    def test_too_many_letters_returns_none(self):
        # fewer than 8 raw digits → not trustworthy
        assert _norm_digits_12("ABCD1234EFGH") is None

    def test_wrong_length(self):
        assert _norm_digits_12("39961749881") is None   # 11 chars
        assert _norm_digits_12("3996174988190") is None  # 13 chars

    def test_none_input(self):
        assert _norm_digits_12(None) is None

    def test_dash_stripped(self):
        # Separators are stripped; "3996-1749-8819" → "399617498819" (12 digits)
        assert _norm_digits_12("3996-1749-8819") == "399617498819"

    def test_space_stripped(self):
        assert _norm_digits_12("3996 1749 8819") == "399617498819"


# =============================================================================
# extract_awb_from_filename_strict
# =============================================================================

class TestExtractAWBFromFilenameStrict:
    def test_bare_12_digits(self):
        assert extract_awb_from_filename_strict("399617498819.pdf") == "399617498819"

    def test_12_digits_in_prefix(self):
        assert extract_awb_from_filename_strict("20260317_399617498819_scan.pdf") == "399617498819"

    def test_4_4_4_spaced(self):
        assert extract_awb_from_filename_strict("3996 1749 8819.pdf") == "399617498819"

    def test_no_match_returns_none(self):
        assert extract_awb_from_filename_strict("invoice_march.pdf") is None

    def test_11_digits_not_matched(self):
        assert extract_awb_from_filename_strict("39961749881.pdf") is None

    def test_none_input(self):
        assert extract_awb_from_filename_strict(None) is None

    def test_basename_only_used(self):
        # Path component should not leak into match
        result = extract_awb_from_filename_strict("/some/path/399617498819.pdf")
        assert result == "399617498819"


# =============================================================================
# extract_awb_from_400_pattern
# =============================================================================

class TestExtractAWBFrom400Pattern:
    def test_400_dash_prefix(self):
        assert extract_awb_from_400_pattern("400-399617498819") == "399617498819"

    def test_400_space_prefix(self):
        assert extract_awb_from_400_pattern("400 399617498819") == "399617498819"

    def test_400_bare_prefix(self):
        assert extract_awb_from_400_pattern("400399617498819") == "399617498819"

    def test_no_400_prefix_returns_none(self):
        assert extract_awb_from_400_pattern("399617498819") is None

    def test_none_input(self):
        assert extract_awb_from_400_pattern(None) is None

    def test_empty_input(self):
        assert extract_awb_from_400_pattern("") is None

    def test_invalid_after_400_returns_none(self):
        # Only 11 digits after 400
        assert extract_awb_from_400_pattern("400-39961749881") is None


# =============================================================================
# extract_candidates_from_text
# =============================================================================

class TestExtractCandidatesFromText:
    def test_plain_12_digit(self):
        result = extract_candidates_from_text("AWB 399617498819 for shipment")
        assert "399617498819" in result

    def test_no_digits_returns_empty(self):
        result = extract_candidates_from_text("no numbers here")
        assert len(result) == 0

    def test_none_input(self):
        result = extract_candidates_from_text(None)
        assert isinstance(result, set)
        assert len(result) == 0

    def test_multiple_candidates(self):
        result = extract_candidates_from_text("399617498819 and 473663888340")
        assert "399617498819" in result
        assert "473663888340" in result

    def test_11_digit_not_included(self):
        result = extract_candidates_from_text("39961749881")
        assert "39961749881" not in result


# =============================================================================
# extract_candidates_near_keywords
# =============================================================================

class TestExtractCandidatesNearKeywords:
    def test_awb_keyword_adjacent(self):
        result = extract_candidates_near_keywords("AWB 399617498819")
        assert "399617498819" in result

    def test_tracking_keyword(self):
        result = extract_candidates_near_keywords("TRACKING NUMBER 473663888340")
        assert "473663888340" in result

    def test_no_keyword_returns_empty(self):
        # Without an AWB keyword, the function returns nothing
        result = extract_candidates_near_keywords("399617498819")
        assert isinstance(result, set)

    def test_none_input(self):
        result = extract_candidates_near_keywords(None)
        assert isinstance(result, set)
