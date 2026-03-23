# V3/core/awb_extractor.py
# All AWB candidate extraction, regex patterns, and text-mining logic.
#
# Extracted from Scripts/awb_hotfolder_V2.py (monolith).
# Every function is a direct, complete port — no logic simplified or removed.
# All thresholds, regex patterns, and edge-case handling are preserved exactly.

from __future__ import annotations

import re
from typing import Optional, Set, Tuple

from V3 import config

# ── Config aliases ────────────────────────────────────────────────────────────
AWB_LEN = config.AWB_LEN
CONTEXT_WINDOW_CHARS = config.CONTEXT_WINDOW_CHARS
AWB_CONTEXT_KEYWORDS = config.AWB_CONTEXT_KEYWORDS

# Pre-computed normalized keyword set (stripped of non-alnum, uppercased).
# Used by _promote_keyword_adjacent and extract_candidates_near_keywords
# instead of rebuilding per call.
_KEYWORDS_NORM: set = {
    re.sub(r"[^A-Z0-9]+", "", k.upper()) for k in AWB_CONTEXT_KEYWORDS if k
}

# Pre-compiled relaxed keyword patterns for _has_awb_label
_RELAXED_KW_PATTERNS = tuple(
    re.compile(p, re.IGNORECASE) for p in (
        r"AIR\w{0,6}WAY\w{0,6}BIL{1,2}",
        r"WAY\w{0,6}BIL{1,2}",
        r"TRACK\w{0,10}(NO|NUM|NUMBER)?",
        r"\bAWB\b",
        r"BIL{1,2}\w{0,8}(NO|NUM|NUMBER)",
    )
)


def _norm_kw(text: str) -> str:
    """Strip non-alnum and uppercase — shared by keyword proximity helpers."""
    return re.sub(r"[^A-Z0-9]+", "", (text or "").upper())


def _has_awb_label(window_text: str) -> bool:
    """Check if *window_text* contains any AWB-related keyword."""
    if any(k in window_text for k in AWB_CONTEXT_KEYWORDS):
        return True
    window_norm = _norm_kw(window_text)
    if any(kn and kn in window_norm for kn in _KEYWORDS_NORM):
        return True
    return any(p.search(window_text) for p in _RELAXED_KW_PATTERNS)


# =============================================================================
# COMPILED REGEX PATTERNS
# =============================================================================

# Filename patterns
_FILENAME_AWB_12DIGITS = re.compile(r"(?<!\d)(\d{12})(?!\d)")
_FILENAME_AWB_4SPACE4SPACE4 = re.compile(r"(?<!\d)(\d{4}\s\d{4}\s\d{4})(?!\d)")

# Keyword-proximity candidate pattern (12-digit or 4-4-4 grouped)
_DIGIT_CANDIDATE_PATTERN = re.compile(r"(?<!\d)(\d{12}|\d{4}[\s\-]\d{4}[\s\-]\d{4})(?!\d)")

# 400 tight pattern — the ONLY exception to DB check
_400_AWB_PATTERN = re.compile(
    r"(?<!\d)400(?:[\s\-]{0,2})(\d(?:[\s\-]?\d){11})(?!\d)", re.IGNORECASE
)

# Label patterns — DB check always applies (these feed into prioritize_db_match)
_400_LABELED_PATTERN = re.compile(
    r"(?<!\d)400\s*(?:NO\.?|NUM\.?|NUMBER|#)\s*[:\-]?\s*"
    r"(\d{12}|\d{4}[\s\-]\d{4}[\s\-]\d{4})",
    re.IGNORECASE,
)

_ACI_AWB_PATTERN = re.compile(
    r"(?<!\w)(?:A\s*[CGE6]\s*[I1L])\b[\D]{0,15}(\d(?:[\s\-]?\d){11})(?!\d)",
    re.IGNORECASE,
)

_ACI_LABELED_PATTERN = re.compile(
    r"(?<!\w)(?:A\s*[CGE6]\s*[I1L])\s*(?:NO\.?|NUM\.?|NUMBER|#)"
    r"\s*[:\-]?\s*(\d{12}|\d{4}[\s\-]\d{4}[\s\-]\d{4})",
    re.IGNORECASE,
)

_AWB_LABEL_PATTERN = re.compile(
    r"(?<!\w)(HAWB|MAWB|AWB(?:\s*(?:NO|NUMBER))?)\b[\D]{0,15}(\d[\d\-\s]{10,24})",
    re.IGNORECASE,
)

_TRACK_LABEL_PATTERN = re.compile(
    r"(?<!\w)(?:TRACK(?:ING)?(?:\s*(?:NO|NUMBER|#))?|TRK(?:\s*(?:NO|NUMBER|#))?)"
    r"\b[\D]{0,20}([A-Z0-9][A-Z0-9\-\s:/._]{10,30})",
    re.IGNORECASE,
)

_AIRWAY_BILL_LABEL_PATTERN = re.compile(
    r"(?<!\w)AIR\W*WAY\W*BIL{1,2}(?:\W*(?:NO|NUMBER|#))?\b[\D]{0,30}"
    r"([A-Z0-9][A-Z0-9\-\s:/._]{10,30})",
    re.IGNORECASE,
)

_FEDEX_CARRIER_ROW_PATTERN = re.compile(
    r"(?:FED[\s\-]*EX|FEDEX)[\D]{0,30}(\d{12}|\d{4}[\s\-]\d{4}[\s\-]\d{4})",
    re.IGNORECASE,
)


# =============================================================================
# OCR DIGIT CHARACTER MAP (letter -> digit substitution for noisy OCR)
# =============================================================================

_OCR_DIGIT_CHAR_MAP = {
    "O": "0",
    "Q": "0",
    "D": "0",
    "I": "1",
    "L": "1",
    "Z": "2",
    "S": "5",
    "G": "6",
    "B": "8",
    "T": "7",
}


# =============================================================================
# HELPER / FILTER FUNCTIONS
# =============================================================================

def _norm_digits_12(raw: Optional[str]) -> Optional[str]:
    """Normalize a raw OCR token to exactly 12 digits, mapping common
    letter-to-digit OCR confusions.  Returns ``None`` if the token cannot
    be normalised to exactly 12 digit characters or has fewer than 8 raw
    digits (i.e. too many letter substitutions to be trustworthy).
    """
    if not raw:
        return None
    cleaned = re.sub(r"[\s\-:/._]+", "", str(raw).upper())
    if len(cleaned) != AWB_LEN:
        return None
    raw_digit_count = sum(1 for ch in cleaned if ch.isdigit())
    if raw_digit_count < 8:
        return None
    out: list[str] = []
    for ch in cleaned:
        if ch.isdigit():
            out.append(ch)
            continue
        mapped = _OCR_DIGIT_CHAR_MAP.get(ch)
        if not mapped:
            return None
        out.append(mapped)
    return "".join(out)


def _strict_awb_from_fragment(text: Optional[str]) -> Optional[str]:
    """Return an exact 12-digit string from *text* only if it is a clean
    12-digit or 4-4-4 grouped number.  Any noise causes ``None``."""
    frag = (text or "").strip()
    if re.fullmatch(r"\d{12}", frag):
        return frag
    if re.fullmatch(r"\d{4}[\s\-]\d{4}[\s\-]\d{4}", frag):
        return re.sub(r"\D", "", frag)
    return None


def _is_likely_date_reference(candidate: Optional[str]) -> bool:
    """True when the 12-digit candidate looks like a date-based identifier
    (first 4 digits are a plausible year, next 2 are a plausible month)."""
    if not candidate or len(candidate) != AWB_LEN or not candidate.isdigit():
        return False
    try:
        year, month = int(candidate[:4]), int(candidate[4:6])
        return 2015 <= year <= 2035 and 1 <= month <= 12
    except Exception:
        return False


def _is_disqualified_candidate(
    candidate: Optional[str], for_tolerance: bool = False
) -> bool:
    """Hard disqualifier for an AWB candidate.

    *for_tolerance=True* relaxes the leading-zero rule so OCR near-misses
    like ``099617498819`` (true AWB ``399617498819``) can reach tolerance
    matching.  Exact matching, clean gate and EDM always pass
    ``for_tolerance=False``.
    """
    if not candidate or len(candidate) != AWB_LEN or not candidate.isdigit():
        return True
    if not for_tolerance and candidate.startswith("0"):
        return True
    return False


# =============================================================================
# FILENAME EXTRACTION
# =============================================================================

def extract_awb_from_filename_strict(filename: Optional[str]) -> Optional[str]:
    """Find a 12-digit AWB anywhere in *filename*.

    No DB check -- filename is treated as authoritative.
    Handles: bare 12 digits, 4-4-4 grouped, 400-prefix, any other
    surrounding text -- the lookbehind/lookahead isolates the number.

    Examples::

        400-399617498819.pdf        -> 399617498819
        20260317_399617498819.pdf   -> 399617498819
        1234 5678 9012_scan.pdf     -> 123456789012
        randomtext399617498819x.pdf -> 399617498819
    """
    import os

    base = os.path.basename(filename or "")
    m = _FILENAME_AWB_12DIGITS.search(base)
    if m:
        return m.group(1)
    m = _FILENAME_AWB_4SPACE4SPACE4.search(base)
    if m:
        return m.group(1).replace(" ", "")
    return None


# =============================================================================
# 400-PATTERN EXTRACTION (no DB check)
# =============================================================================

def extract_awb_from_400_pattern(text: Optional[str]) -> Optional[str]:
    """Returns a 400-prefix AWB **without** a DB check -- tight format only.

    Format: ``400-NNNNNNNNNNNN``, ``400 NNNNNNNNNNNN``,
    ``400:NNNNNNNNNNNN``, ``400NNNNNNNNNNNN``.

    Labeled variants (``400 NUMBER:``, ``400 NO:``) are intentionally
    excluded -- they go through :func:`extract_tiered_candidates` which
    always checks the DB first.
    """
    if not text:
        return None
    for m in _400_AWB_PATTERN.finditer(text):
        digits = re.sub(r"\D", "", m.group(1))
        if len(digits) == 12 and not _is_disqualified_candidate(digits):
            return digits
    for m in re.finditer(r"(?<!\d)400(\d{12})(?!\d)", text):
        d = m.group(1)
        if not _is_disqualified_candidate(d):
            return d
    return None


# =============================================================================
# CANDIDATE EXTRACTION (text-based)
# =============================================================================

def extract_candidates_from_alnum_ocr(text: Optional[str]) -> Set[str]:
    """Extract 12-digit candidates from alphanumeric OCR tokens, applying
    the letter-to-digit char map to normalise noisy OCR output."""
    out: Set[str] = set()
    if not text:
        return out
    for m in re.finditer(
        r"(?<![A-Z0-9])([A-Z0-9][A-Z0-9\-\s:/._]{8,30}[A-Z0-9])(?![A-Z0-9])",
        text.upper(),
    ):
        d = _norm_digits_12(m.group(1))
        if d:
            out.add(d)
    return out


def extract_awb_candidates_from_aci_pattern(text: Optional[str]) -> Set[str]:
    """Extract AWB candidates from ACI-style label patterns."""
    out: Set[str] = set()
    if not text:
        return out
    for m in _ACI_AWB_PATTERN.finditer(text):
        d = _norm_digits_12(m.group(1))
        if d:
            out.add(d)
    for m in _ACI_LABELED_PATTERN.finditer(text):
        d = _norm_digits_12(m.group(1))
        if d:
            out.add(d)
    return out


def extract_awb_from_fedex_carrier_row(text: Optional[str]) -> Set[str]:
    """Extract 12-digit candidates from FedEx carrier row patterns.

    Scans both the compiled regex pattern and a multi-line block search
    around lines mentioning FEDEX / FED-EX.
    """
    out: Set[str] = set()
    if not text:
        return out
    for m in _FEDEX_CARRIER_ROW_PATTERN.finditer(text):
        d = _norm_digits_12(m.group(1))
        if d:
            out.add(d)
    lines = (text or "").splitlines()
    for i, line in enumerate(lines):
        lu = re.sub(r"\s+", "", (line or "").upper())
        if "FEDEX" not in lu and "FED-EX" not in (line or "").upper():
            continue
        block = " ".join(lines[max(0, i - 1) : min(len(lines), i + 3)])
        for m in re.finditer(r"(?<!\d)(\d{12})(?!\d)", block):
            out.add(m.group(1))
        for m in re.finditer(
            r"(?<!\d)(\d{4}[\s\-]\d{4}[\s\-]\d{4})(?!\d)", block
        ):
            d = re.sub(r"\D", "", m.group(1))
            if len(d) == AWB_LEN:
                out.add(d)
    return out


def extract_awb_from_airway_bill_label(text: Optional[str]) -> Set[str]:
    """Extract 12-digit candidates from 'Airway Bill' label patterns."""
    out: Set[str] = set()
    if not text:
        return out
    for m in _AIRWAY_BILL_LABEL_PATTERN.finditer(text):
        d = _norm_digits_12(m.group(1))
        if d:
            out.add(d)
    lines = (text or "").splitlines()
    for i, line in enumerate(lines):
        lu = (line or "").upper()
        lu_norm = re.sub(r"[^A-Z0-9]+", "", lu)
        if not (
            ("AIRWAY" in lu_norm and "BILL" in lu_norm) or "AWAYBILL" in lu_norm
        ):
            continue
        block = " ".join(lines[max(0, i - 1) : min(len(lines), i + 3)])
        for m in re.finditer(r"(?<!\d)(\d{12})(?!\d)", block):
            out.add(m.group(1))
        for m in re.finditer(
            r"(?<!\d)(\d{4}[\s\-]\d{4}[\s\-]\d{4})(?!\d)", block
        ):
            d = re.sub(r"\D", "", m.group(1))
            if len(d) == AWB_LEN:
                out.add(d)
    return out


def extract_candidates_from_text(s: Optional[str]) -> Set[str]:
    """Broad candidate extraction from arbitrary text.

    Combines multiple strategies: alphanumeric OCR char-map, bare 12-digit
    regex, 4-4-4 grouped regex, 400-prefix fragments, and ACI patterns.
    """
    s = s or ""
    out: Set[str] = set()
    out.update(extract_candidates_from_alnum_ocr(s))
    for m in re.finditer(r"(?<!\d)(\d{12})(?!\d)", s):
        out.add(m.group(1))
    for m in re.finditer(r"(?<!\d)(\d{4}[\s\-]\d{4}[\s\-]\d{4})(?!\d)", s):
        d = re.sub(r"\D", "", m.group(1))
        if len(d) == AWB_LEN:
            out.add(d)
    for m in re.finditer(
        r"(?<!\d)400[\s\-:]{0,6}([0-9][0-9\-\s]{10,20})(?!\d)", s, re.IGNORECASE
    ):
        strict = _strict_awb_from_fragment(m.group(1))
        if strict:
            out.add(strict)
    for m in re.finditer(
        r"(?<!\d)(?:A\s*[CGE6]\s*[I1L])[\D]{0,15}([0-9][0-9\-\s]{10,22})(?!\d)",
        s,
        re.IGNORECASE,
    ):
        d = _norm_digits_12(m.group(1))
        if d:
            out.add(d)
    out.update(extract_awb_candidates_from_aci_pattern(s))
    return out


def extract_db_backed_candidates_from_text(
    s: Optional[str], awb_set: Set[str]
) -> Set[str]:
    """Extract candidates from *s* that are confirmed present in *awb_set*.

    Two passes:
    1. Pure digit runs -- extract every 12-digit window from digit-only
       sequences and check against awb_set.
    2. Alphanumeric OCR char-map -- normalise letter-contaminated tokens,
       then slide a 12-digit window and check against awb_set.
    """
    s = s or ""
    out: Set[str] = set()

    # Pass 1: pure digit runs
    for m in re.finditer(r"(?<!\d)(\d[\d\-\s]{10,40}\d)(?!\d)", s):
        digits = re.sub(r"\D", "", m.group(1))
        if len(digits) < AWB_LEN:
            continue
        if len(digits) == AWB_LEN:
            if digits in awb_set:
                out.add(digits)
            continue
        for i in range(len(digits) - AWB_LEN + 1):
            cand = digits[i : i + AWB_LEN]
            if cand in awb_set:
                out.add(cand)

    # Pass 2: alphanumeric OCR char-map normalisation
    for m in re.finditer(
        r"(?<![A-Z0-9])([A-Z0-9][A-Z0-9\-\s:/._]{8,36}[A-Z0-9])(?![A-Z0-9])",
        s.upper(),
    ):
        raw = m.group(1)
        norm_chars: list[str] = []
        raw_digit_count = 0
        invalid = False
        for ch in raw:
            if ch in " -:/._\t\r\n":
                continue
            if ch.isdigit():
                raw_digit_count += 1
                norm_chars.append(ch)
                continue
            mapped = _OCR_DIGIT_CHAR_MAP.get(ch)
            if mapped:
                norm_chars.append(mapped)
            else:
                invalid = True
                break
        if invalid:
            continue
        norm = "".join(norm_chars)
        if len(norm) < AWB_LEN or raw_digit_count < 8:
            continue
        if len(norm) == AWB_LEN:
            if norm in awb_set:
                out.add(norm)
            continue
        for i in range(len(norm) - AWB_LEN + 1):
            cand = norm[i : i + AWB_LEN]
            if cand in awb_set:
                out.add(cand)
    return out


def extract_candidates_near_keywords(
    s: Optional[str],
    line_lookahead: int = 3,
    line_lookback: int = 1,
) -> Set[str]:
    """Find 12-digit candidates that appear near AWB-related keywords.

    *line_lookahead* / *line_lookback* control how many lines around a
    keyword line are scanned.  Text-layer calls typically use wider windows
    (5/2); OCR calls use the default (3/1) to avoid noise.
    """
    s = s or ""
    su = s.upper()
    out: Set[str] = set()

    # Character-window check around each match
    for m in _DIGIT_CANDIDATE_PATTERN.finditer(s):
        d = _strict_awb_from_fragment(m.group(1))
        start = max(0, m.start() - CONTEXT_WINDOW_CHARS)
        end = min(len(su), m.end() + CONTEXT_WINDOW_CHARS)
        window = su[start:end]
        if _has_awb_label(window) and d and len(d) == AWB_LEN:
            out.add(d)

    # Line-based check
    lines = s.splitlines()
    for i, line in enumerate(lines):
        line_u = line.upper()
        line_norm = _norm_kw(line_u)
        has_label = _has_awb_label(line_u) or any(
            kn and kn in line_norm for kn in _KEYWORDS_NORM
        )
        if not has_label:
            continue
        block = " ".join(
            lines[max(0, i - line_lookback) : min(len(lines), i + line_lookahead + 1)]
        )
        for m in _DIGIT_CANDIDATE_PATTERN.finditer(block):
            d = _strict_awb_from_fragment(m.group(1))
            if d and len(d) == AWB_LEN:
                out.add(d)
    return out


# =============================================================================
# TIERED CANDIDATE EXTRACTION
# =============================================================================

def _candidates_from_label_prefixes(text: Optional[str]) -> Set[str]:
    """Extract HIGH-confidence candidates from label-prefixed patterns.

    Covers: AWB/HAWB/MAWB labels, tight 400-prefix, labeled 400 (NO/NUMBER),
    ACI patterns, TRACKING labels, and Airway Bill labels.
    """
    high: Set[str] = set()
    if not text:
        return high
    for m in _AWB_LABEL_PATTERN.finditer(text):
        d = _norm_digits_12(m.group(2))
        if d:
            high.add(d)
    # Tight 400 prefix (same pattern as the no-DB-check exemption but here
    # the result enters prioritize_db_match which checks DB before accepting)
    for m in _400_AWB_PATTERN.finditer(text):
        d = _norm_digits_12(m.group(1))
        if d:
            high.add(d)
    # Labeled 400 (400 NUMBER:, 400 NO:) -- DB-checked path only
    for m in _400_LABELED_PATTERN.finditer(text):
        d = _norm_digits_12(m.group(1))
        if d:
            high.add(d)
    for m in re.finditer(
        r"(?<!\d)(?:A\s*[CGE6]\s*[I1L])(?:\D{0,15})(\d[\d\-\s]{10,24})(?!\d)",
        text,
        re.IGNORECASE,
    ):
        d = _norm_digits_12(m.group(1))
        if d:
            high.add(d)
    for m in _TRACK_LABEL_PATTERN.finditer(text):
        d = _norm_digits_12(m.group(1))
        if d:
            high.add(d)
    high.update(extract_awb_from_airway_bill_label(text))
    return high


def _promote_keyword_adjacent(
    text: Optional[str], candidates: Optional[Set[str]]
) -> Set[str]:
    """Promote STANDARD candidates to HIGH when they appear near AWB keywords.

    Uses both a character-window check (CONTEXT_WINDOW_CHARS around each
    occurrence) and a line-based check (+/- 1 line around keyword lines).
    """
    s = text or ""
    su = s.upper()
    cands = {
        c
        for c in (candidates or set())
        if isinstance(c, str) and len(c) == AWB_LEN and c.isdigit()
    }
    promoted: Set[str] = set()
    if not cands:
        return promoted

    def _has_kw(t: str) -> bool:
        if any(k in t for k in AWB_CONTEXT_KEYWORDS):
            return True
        tn = _norm_kw(t)
        return any(kn and kn in tn for kn in _KEYWORDS_NORM)

    # Character-window promotion
    for c in cands:
        for m in re.finditer(rf"(?<!\d){re.escape(c)}(?!\d)", s):
            start = max(0, m.start() - CONTEXT_WINDOW_CHARS)
            end = min(len(su), m.end() + CONTEXT_WINDOW_CHARS)
            if _has_kw(su[start:end]):
                promoted.add(c)
                break

    # Line-based promotion
    lines = s.splitlines()
    line_cands: list[Set[str]] = []
    for line in lines:
        ln = re.sub(r"\D", " ", line)
        found = {
            tok
            for tok in ln.split()
            if len(tok) == AWB_LEN and tok.isdigit() and tok in cands
        }
        line_cands.append(found)
    for i, line in enumerate(lines):
        if not _has_kw(line.upper()):
            continue
        for j in [i - 1, i, i + 1]:
            if 0 <= j < len(lines):
                promoted.update(line_cands[j])
    return promoted


def extract_tiered_candidates(
    text: Optional[str], awb_set: Set[str]
) -> Tuple[Set[str], Set[str]]:
    """Run all extraction strategies and partition results into HIGH and
    STANDARD confidence tiers.

    Returns ``(high, standard)`` where *high* contains label-backed and
    keyword-promoted candidates, and *standard* contains the remainder
    (after removing disqualified and date-reference candidates).
    """
    s = text or ""
    high: Set[str] = set()
    standard: Set[str] = set()

    high.update(_candidates_from_label_prefixes(s))
    high.update(extract_awb_candidates_from_aci_pattern(s))
    high.update(extract_awb_from_fedex_carrier_row(s))

    standard.update(extract_candidates_from_text(s))
    standard.update(extract_db_backed_candidates_from_text(s, awb_set))

    promoted = _promote_keyword_adjacent(s, standard)
    high.update(promoted)
    standard.difference_update(high)

    disq = {c for c in (high | standard) if _is_disqualified_candidate(c)}
    high.difference_update(disq)
    standard.difference_update(disq)

    date_refs = {c for c in standard if _is_likely_date_reference(c)}
    standard.difference_update(date_refs)

    return high, standard


def extract_clean_candidates(text: Optional[str]) -> Set[str]:
    """Extract only clean, unambiguous 12-digit candidates (bare or 4-4-4).

    No char-map normalisation, no label patterns -- strict digit-only
    extraction used by the clean-gate priority check.
    """
    s = text or ""
    out: Set[str] = set()
    for m in re.finditer(r"(?<!\d)(\d{12})(?!\d)", s):
        out.add(m.group(1))
    for m in re.finditer(r"(?<!\d)(\d{4}[\s\-]\d{4}[\s\-]\d{4})(?!\d)", s):
        d = re.sub(r"\D", "", m.group(1))
        if len(d) == AWB_LEN:
            out.add(d)
    return out
