"""Unit tests for V3/core/awb_matcher.py — pure logic, no external deps."""
from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import pytest

from V3.core.awb_matcher import (
    hamming,
    _unique_awb_candidate_count,
    pick_unique_close_match,
    tolerance_match_with_tie_guard,
    tolerance_match_with_details,
)
from V3.core.file_ops import build_buckets


# ── Fixtures ─────────────────────────────────────────────────────────────────

AWB_A = "399617498819"
AWB_B = "473663888340"
AWB_C = "889653134980"

_DB = {AWB_A, AWB_B, AWB_C}


def _buckets(db=_DB):
    return build_buckets(db)


# =============================================================================
# hamming
# =============================================================================

class TestHamming:
    def test_identical(self):
        assert hamming("123456789012", "123456789012") == 0

    def test_one_diff(self):
        assert hamming("123456789012", "123456789013") == 1

    def test_all_diff(self):
        assert hamming("000000000000", "111111111111") == 12

    def test_two_diffs(self):
        assert hamming("399617498819", "399617498810") == 1  # last digit
        assert hamming("399617498819", "399617498800") == 2  # last two


# =============================================================================
# _unique_awb_candidate_count
# =============================================================================

class TestUniqueAWBCandidateCount:
    def test_empty(self):
        assert _unique_awb_candidate_count(set()) == 0

    def test_none(self):
        assert _unique_awb_candidate_count(None) == 0

    def test_valid(self):
        assert _unique_awb_candidate_count({AWB_A, AWB_B}) == 2

    def test_invalid_filtered_out(self):
        # 11-digit and non-digit strings don't count
        assert _unique_awb_candidate_count({"39961749881", "ABCD", AWB_A}) == 1


# =============================================================================
# pick_unique_close_match
# =============================================================================

class TestPickUniqueCloseMatch:
    def setup_method(self):
        self.by_prefix, self.by_suffix = _buckets()

    def test_exact_match(self):
        result = pick_unique_close_match(AWB_A, _DB, self.by_prefix, self.by_suffix)
        assert result == AWB_A

    def test_one_digit_off(self):
        # Change last digit by 1
        near = AWB_A[:-1] + str((int(AWB_A[-1]) + 1) % 10)
        result = pick_unique_close_match(near, _DB, self.by_prefix, self.by_suffix, max_distance=1)
        assert result == AWB_A

    def test_no_match(self):
        result = pick_unique_close_match("000000000000", _DB, self.by_prefix, self.by_suffix)
        assert result is None

    def test_empty_db(self):
        by_p, by_s = build_buckets(set())
        result = pick_unique_close_match(AWB_A, set(), by_p, by_s)
        assert result is None

    def test_ambiguous_returns_none(self):
        # Two AWBs equidistant from the candidate → ambiguous → None
        db = {"399617498810", "399617498811"}
        by_p, by_s = build_buckets(db)
        # candidate differs from both by 1 digit at position 11 depending on value
        # candidate "399617498812" is distance-1 from both "...810" and "...811"? No.
        # Let's use a candidate that is distance-2 from two different AWBs
        candidate = "399617498800"  # differs from "399617498810" by 1, "399617498811" by 2
        result = pick_unique_close_match(candidate, db, by_p, by_s, max_distance=2)
        # "399617498810" wins uniquely (distance 1 < 2)
        assert result == "399617498810"


# =============================================================================
# tolerance_match_with_tie_guard
# =============================================================================

class TestToleranceMatchWithTieGuard:
    def setup_method(self):
        self.by_prefix, self.by_suffix = _buckets()

    def test_exact_match(self):
        awb, ties = tolerance_match_with_tie_guard({AWB_A}, _DB, self.by_prefix, self.by_suffix)
        assert awb == AWB_A
        assert ties is None

    def test_no_match(self):
        awb, ties = tolerance_match_with_tie_guard({"000000000000"}, _DB, self.by_prefix, self.by_suffix)
        assert awb is None
        assert ties is None

    def test_empty_candidates(self):
        awb, ties = tolerance_match_with_tie_guard(set(), _DB, self.by_prefix, self.by_suffix)
        assert awb is None
        assert ties is None

    def test_invalid_candidate_skipped(self):
        awb, ties = tolerance_match_with_tie_guard({"short"}, _DB, self.by_prefix, self.by_suffix)
        assert awb is None

    def test_tie_returns_none_with_ties_list(self):
        # Two AWBs in DB that are equidistant from the candidate
        db = {"399617498810", "399617498820"}
        by_p, by_s = build_buckets(db)
        # Candidate "399617498800" is distance-1 from "...810" and distance-2 from "...820"
        # Actually distance from "399617498800" to "399617498810" = 1 (pos 10: 0 vs 1)
        # distance from "399617498800" to "399617498820" = 1 (pos 10: 0 vs 2)? No: 0 vs 2 → different
        # Let's pick a candidate equidistant from both
        # "399617498815": dist to "399617498810" = 1 (pos11: 5 vs 0), dist to "399617498820" = 2
        # Use "399617498800": dist to "399617498810" = 1, dist to "399617498820" = 1 (last digit: 0 vs 0 for 820)
        # Hmm, "399617498810": positions [0..11] = 3,9,9,6,1,7,4,9,8,8,1,0
        # "399617498820": positions [0..11] = 3,9,9,6,1,7,4,9,8,8,2,0
        # "399617498800": 3,9,9,6,1,7,4,9,8,8,0,0 → diff from "...10" at pos10 (0 vs 1) = 1
        #                                           diff from "...20" at pos10 (0 vs 2) = 1
        # Both at distance 1 → tie
        candidate = "399617498800"
        awb, ties = tolerance_match_with_tie_guard({candidate}, db, by_p, by_s, max_distance=1)
        assert awb is None
        assert ties is not None
        assert len(ties) == 2


# =============================================================================
# tolerance_match_with_details
# =============================================================================

class TestToleranceMatchWithDetails:
    def setup_method(self):
        self.by_prefix, self.by_suffix = _buckets()

    def test_exact_match_status(self):
        result = tolerance_match_with_details(
            {AWB_A}, _DB, self.by_prefix, self.by_suffix
        )
        assert result["status"] == "matched"
        assert result["awb"] == AWB_A
        assert result["distance"] == 0

    def test_one_digit_off(self):
        near = AWB_A[:-1] + str((int(AWB_A[-1]) + 1) % 10)
        result = tolerance_match_with_details(
            {near}, _DB, self.by_prefix, self.by_suffix, max_distance=1
        )
        assert result["status"] == "matched"
        assert result["awb"] == AWB_A
        assert result["distance"] == 1

    def test_no_match_returns_none_status(self):
        result = tolerance_match_with_details(
            {"000000000000"}, _DB, self.by_prefix, self.by_suffix
        )
        assert result["status"] == "none"

    def test_empty_candidates(self):
        result = tolerance_match_with_details(None, _DB, self.by_prefix, self.by_suffix)
        assert result["status"] == "none"

    def test_tie_returns_tie_status(self):
        db = {"399617498810", "399617498820"}
        by_p, by_s = build_buckets(db)
        # "399617498800" is distance-1 from both "...810" and "...820" (at pos 10)
        candidate = "399617498800"
        result = tolerance_match_with_details({candidate}, db, by_p, by_s, max_distance=1)
        assert result["status"] == "tie"
        assert len(result["ties"]) == 2
