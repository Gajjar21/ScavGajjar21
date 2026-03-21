# V3/core/awb_matcher.py
# All AWB matching and decision logic: Hamming distance, tolerance matching,
# tiered priority matching, and the top-level decide_from_candidates helper.
#
# Extracted from Scripts/awb_hotfolder_V2.py (monolith).
# Every function is a direct, complete port — no logic simplified or removed.
# All thresholds and edge-case handling are preserved exactly.

from __future__ import annotations

from typing import Any, Dict, List, Optional, Set, Tuple

from V3 import config
from V3.core.awb_extractor import _is_disqualified_candidate

# ── Config aliases ────────────────────────────────────────────────────────────
AWB_LEN = config.AWB_LEN
ALLOW_1_DIGIT_TOLERANCE = config.ALLOW_1_DIGIT_TOLERANCE

ALLOW_STANDARD_TOLERANCE = config.ALLOW_STANDARD_TOLERANCE
TOLERANCE_HIGH_MAX_DISTANCE = config.TOLERANCE_HIGH_MAX_DISTANCE
TOLERANCE_STANDARD_MAX_DISTANCE = config.TOLERANCE_STANDARD_MAX_DISTANCE
MIN_STAGE_HITS_HIGH_TOL1 = config.MIN_STAGE_HITS_HIGH_TOL1
MIN_STAGE_HITS_HIGH_TOL2 = config.MIN_STAGE_HITS_HIGH_TOL2
MIN_STAGE_HITS_STANDARD_TOL = config.MIN_STAGE_HITS_STANDARD_TOL
REQUIRE_SINGLE_STANDARD_CANDIDATE_FOR_TOL = config.REQUIRE_SINGLE_STANDARD_CANDIDATE_FOR_TOL


# =============================================================================
# HAMMING DISTANCE
# =============================================================================

def hamming(a: str, b: str) -> int:
    """Return the Hamming distance between two equal-length strings."""
    return sum(1 for x, y in zip(a, b) if x != y)


# =============================================================================
# UNIQUE CANDIDATE COUNT
# =============================================================================

def _unique_awb_candidate_count(candidates: Optional[Set[str]]) -> int:
    """Count unique valid 12-digit AWB candidates in a set."""
    return len(
        {
            c
            for c in (candidates or set())
            if isinstance(c, str) and len(c) == AWB_LEN and c.isdigit()
        }
    )


# =============================================================================
# SINGLE-CANDIDATE CLOSE MATCH
# =============================================================================

def pick_unique_close_match(
    candidate: str,
    awb_set: Set[str],
    by_prefix: Dict[str, List[str]],
    by_suffix: Dict[str, List[str]],
    max_distance: int = 2,
) -> Optional[str]:
    """Find a unique AWB in *awb_set* within *max_distance* of *candidate*.

    Uses prefix/suffix bucket lookup to narrow the search space.
    Returns ``None`` when there is no match or the best match is ambiguous
    (i.e., multiple AWBs share the same minimal distance).
    """
    pool: Set[str] = set()
    pool.update(by_prefix.get(candidate[:4], []))
    pool.update(by_suffix.get(candidate[-4:], []))
    if not pool:
        pool = awb_set
    scored = [
        (a, d) for a in pool for d in (hamming(candidate, a),) if d <= max_distance
    ]
    if not scored:
        return None
    scored.sort(key=lambda x: x[1])
    best_awb, best_d = scored[0]
    if len([a for a, d in scored if d == best_d]) != 1:
        return None
    return best_awb


# =============================================================================
# MULTI-CANDIDATE TOLERANCE MATCH (with tie guard)
# =============================================================================

def tolerance_match_with_tie_guard(
    candidates: Set[str],
    awb_set: Set[str],
    by_prefix: Dict[str, List[str]],
    by_suffix: Dict[str, List[str]],
    max_distance: int = 2,
) -> Tuple[Optional[str], Optional[List[str]]]:
    """Tolerance match across all *candidates* with a tie guard.

    Returns ``(awb, None)`` on unique best match, ``(None, ties)`` on tie,
    ``(None, None)`` on no match.
    """
    best_distance: Optional[int] = None
    best_awbs: Set[str] = set()

    for c in candidates:
        if len(c) != AWB_LEN or not c.isdigit():
            continue
        pool: Set[str] = set()
        pool.update(by_prefix.get(c[:4], []))
        pool.update(by_suffix.get(c[-4:], []))
        if not pool:
            pool = awb_set
        for a in pool:
            d = hamming(c, a)
            if d > max_distance:
                continue
            if best_distance is None or d < best_distance:
                best_distance = d
                best_awbs = {a}
            elif d == best_distance:
                best_awbs.add(a)

    if best_distance is None:
        return None, None
    if len(best_awbs) == 1:
        return next(iter(best_awbs)), None
    return None, sorted(best_awbs)


# =============================================================================
# MULTI-CANDIDATE TOLERANCE MATCH (with details)
# =============================================================================

def tolerance_match_with_details(
    candidates: Optional[Set[str]],
    awb_set: Set[str],
    by_prefix: Dict[str, List[str]],
    by_suffix: Dict[str, List[str]],
    max_distance: int = 2,
) -> Dict[str, Any]:
    """Tolerance match that returns detailed information about the result.

    Returns a dict with ``status`` in ``{"matched", "tie", "none"}``,
    plus ``awb``, ``distance``, ``evidence_candidates`` on match,
    or ``ties``, ``distance`` on tie.
    """
    best_distance: Optional[int] = None
    best_awbs: Set[str] = set()
    evidence: Dict[str, Set[str]] = {}

    for c in (candidates or set()):
        if len(c) != AWB_LEN or not c.isdigit():
            continue
        pool: Set[str] = set()
        pool.update(by_prefix.get(c[:4], []))
        pool.update(by_suffix.get(c[-4:], []))
        if not pool:
            pool = awb_set
        for a in pool:
            d = hamming(c, a)
            if d > max_distance:
                continue
            if best_distance is None or d < best_distance:
                best_distance = d
                best_awbs = {a}
                evidence = {a: {c}}
            elif d == best_distance:
                best_awbs.add(a)
                evidence.setdefault(a, set()).add(c)

    if best_distance is None:
        return {"status": "none"}
    if len(best_awbs) == 1:
        awb = next(iter(best_awbs))
        return {
            "status": "matched",
            "awb": awb,
            "distance": best_distance,
            "evidence_candidates": evidence.get(awb, set()),
        }
    return {"status": "tie", "distance": best_distance, "ties": sorted(best_awbs)}


# =============================================================================
# STAGE-HIT EVIDENCE HELPER
# =============================================================================

def _max_stage_hits_for_evidence(
    evidence_candidates: Optional[Set[str]],
    candidate_stage_hits: Dict[str, Set[str]],
) -> int:
    """Return the maximum number of distinct stage hits among the
    *evidence_candidates* (the OCR candidates that led to a tolerance
    match).  Used to gate tolerance acceptance on multi-stage evidence.
    """
    if not evidence_candidates or not isinstance(candidate_stage_hits, dict):
        return 0
    return max(
        len(candidate_stage_hits.get(c, set())) for c in evidence_candidates
    )


# =============================================================================
# PRIORITIZE DB MATCH (tiered exact + tolerance)
# =============================================================================

def prioritize_db_match(
    high_set: Optional[Set[str]],
    standard_set: Optional[Set[str]],
    awb_set: Set[str],
    by_prefix: Dict[str, List[str]],
    by_suffix: Dict[str, List[str]],
    include_tolerance: bool = True,
    candidate_stage_hits: Optional[Dict[str, Set[str]]] = None,
) -> Dict[str, Any]:
    """Run the full priority-matching cascade.

    Order:
    1. Exact-High (high_set intersect awb_set)
    2. Exact-Standard (standard_set intersect awb_set)
    3. Tolerance2-High (if include_tolerance)
    4. Tolerance2-Standard (if include_tolerance and ALLOW_STANDARD_TOLERANCE)

    Returns a dict with ``status`` in ``{"matched", "tie", "none"}``.
    """
    # 1. Exact HIGH
    exact_high = sorted((high_set or set()) & awb_set)
    if len(exact_high) == 1:
        return {"status": "matched", "awb": exact_high[0], "method": "Exact-High"}
    if len(exact_high) > 1:
        return {"status": "tie", "ties": exact_high, "method": "Exact-High"}

    # 2. Exact STANDARD
    exact_std = sorted((standard_set or set()) & awb_set)
    if len(exact_std) == 1:
        return {"status": "matched", "awb": exact_std[0], "method": "Exact-Standard"}
    if len(exact_std) > 1:
        return {"status": "tie", "ties": exact_std, "method": "Exact-Standard"}

    # 3-4. Tolerance (only if requested)
    if include_tolerance:
        # Build tolerance pools with leading-zero rule relaxed
        tol_high_pool = {
            c
            for c in (high_set or set())
            if not _is_disqualified_candidate(c, for_tolerance=True)
        }
        tol_high = tolerance_match_with_details(
            tol_high_pool,
            awb_set,
            by_prefix,
            by_suffix,
            max_distance=TOLERANCE_HIGH_MAX_DISTANCE,
        )
        if tol_high["status"] == "matched":
            dist = tol_high.get("distance", 99)
            stage_hits = _max_stage_hits_for_evidence(
                tol_high.get("evidence_candidates", set()),
                candidate_stage_hits or {},
            )
            required = (
                MIN_STAGE_HITS_HIGH_TOL1 if dist <= 1 else MIN_STAGE_HITS_HIGH_TOL2
            )
            if stage_hits >= required:
                return {
                    "status": "matched",
                    "awb": tol_high["awb"],
                    "method": "Tolerance2-High",
                    "distance": dist,
                    "stage_hits": stage_hits,
                }
        if tol_high["status"] == "tie":
            return {
                "status": "tie",
                "ties": tol_high.get("ties", []),
                "method": "Tolerance2-High",
            }

        if ALLOW_STANDARD_TOLERANCE:
            tol_std_pool = {
                c
                for c in (standard_set or set())
                if not _is_disqualified_candidate(c, for_tolerance=True)
            }
            tol_std = tolerance_match_with_details(
                tol_std_pool,
                awb_set,
                by_prefix,
                by_suffix,
                max_distance=TOLERANCE_STANDARD_MAX_DISTANCE,
            )
            if tol_std["status"] == "matched":
                stage_hits = _max_stage_hits_for_evidence(
                    tol_std.get("evidence_candidates", set()),
                    candidate_stage_hits or {},
                )
                std_count_ok = (
                    (len(standard_set or set()) == 1)
                    if REQUIRE_SINGLE_STANDARD_CANDIDATE_FOR_TOL
                    else True
                )
                if stage_hits >= MIN_STAGE_HITS_STANDARD_TOL and std_count_ok:
                    return {
                        "status": "matched",
                        "awb": tol_std["awb"],
                        "method": "Tolerance2-Standard",
                        "distance": tol_std.get("distance", 99),
                        "stage_hits": stage_hits,
                    }
            if tol_std["status"] == "tie":
                return {
                    "status": "tie",
                    "ties": tol_std.get("ties", []),
                    "method": "Tolerance2-Standard",
                }

    return {"status": "none"}


# =============================================================================
# SIMPLE DECIDE (exact + 1-digit tolerance)
# =============================================================================

def decide_from_candidates(
    candidates: Set[str],
    awb_set: Set[str],
    by_prefix: Dict[str, List[str]],
    by_suffix: Dict[str, List[str]],
    allow_tolerance: bool,
) -> Tuple[Optional[str], List[str]]:
    """Simple decision: exact match first, then optional tolerance (up to 2-digit).

    Returns ``(awb, exact_list)`` on exact unique match,
    ``(None, exact_list)`` on exact tie, or falls through to tolerance.

    This is the lighter-weight matcher used by individual sub-stages before
    the full :func:`prioritize_db_match` cascade is invoked.
    """
    exact = sorted(candidates & awb_set)
    if len(exact) == 1:
        return exact[0], exact
    if len(exact) > 1:
        return None, exact
    if not allow_tolerance or not ALLOW_1_DIGIT_TOLERANCE:
        return None, []
    close: Set[str] = set()
    for c in candidates:
        if len(c) == AWB_LEN and c.isdigit():
            cm = pick_unique_close_match(c, awb_set, by_prefix, by_suffix)
            if cm:
                close.add(cm)
    close_sorted = sorted(close)
    if len(close_sorted) == 1:
        return close_sorted[0], close_sorted
    return None, close_sorted
