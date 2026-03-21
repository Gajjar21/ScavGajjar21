# V3/stages/pipeline.py
# Pipeline orchestrator — the multi-stage OCR pipeline that processes a PDF
# and extracts its AWB number.
#
# Faithful 1:1 port of Scripts/awb_hotfolder_V2.py process_pdf() with
# clean imports from the new V3 core modules.
#
# Stages 0-7 are preserved exactly.  All edge cases, quarantine logic,
# candidate stage tracking, snapshot logging, and timeout/resume state
# capture are kept verbatim.

from __future__ import annotations

import os
import re
import time
from typing import Any, Dict, List, Optional, Set, Tuple

try:
    import pymupdf as fitz  # PyMuPDF ≥ 1.24 preferred namespace
except ImportError:
    try:
        import fitz  # type: ignore[no-redef]
        fitz.open  # verify it's real PyMuPDF, not the stub package
    except (ImportError, AttributeError) as exc:
        raise RuntimeError(
            "PyMuPDF import failed. Install PyMuPDF and remove any conflicting 'fitz' package."
        ) from exc

from V3 import config

# ── Core module imports ──────────────────────────────────────────────────────
from V3.core.ocr_engine import (
    render_page,
    render_page_from_page,
    preprocess,
    preprocess_for_text,
    ocr_digits_only,
    ocr_text_general,
    digit_score,
    _upscale,
    remove_table_lines_image,
    extract_candidates_from_ocr_data,
    CV2_AVAILABLE,
)
from V3.core.awb_extractor import (
    extract_awb_from_filename_strict,
    extract_awb_from_400_pattern,
    extract_tiered_candidates,
    extract_clean_candidates,
    extract_candidates_near_keywords,
    extract_candidates_from_text,
    extract_db_backed_candidates_from_text,
    extract_awb_candidates_from_aci_pattern,
    extract_awb_from_fedex_carrier_row,
    extract_awb_from_airway_bill_label,
    _is_disqualified_candidate,
    _is_likely_date_reference,
)
from V3.core.awb_matcher import (
    prioritize_db_match,
    _unique_awb_candidate_count,
)
from V3.core.file_ops import (
    log,
    file_is_stable,
    move_to_processed_renamed,
    safe_move,
    append_to_awb_logs_excel,
    append_stage_cache_row,
)

# ── Optional cv2/numpy (used for pre-OCR angle pixel variance check) ────────
try:
    import numpy as np
except ImportError:
    np = None  # type: ignore[assignment]

# ── External integration stubs ───────────────────────────────────────────────
# These are optional modules that may or may not exist in V3 yet.
# Pipeline never fails if they are absent — it just skips the calls.
try:
    from V3.audit import audit_event  # type: ignore[import-untyped]
except Exception:
    def audit_event(*_args: Any, **_kwargs: Any) -> None:  # noqa: D401
        """No-op stub when audit module is not available."""

try:
    from V3.audit import write_hotfolder_event as _ca_write_hotfolder  # type: ignore[import-untyped]
except Exception:
    _ca_write_hotfolder = None

try:
    from V3.audit.tracker import (  # type: ignore[import-untyped]
        record_hotfolder_start,
        record_hotfolder_end,
        record_hotfolder_needs_review,
    )
except Exception:
    def record_hotfolder_start(*_a: Any, **_k: Any) -> None: ...  # noqa: E704
    def record_hotfolder_end(*_a: Any, **_k: Any) -> None: ...  # noqa: E704
    def record_hotfolder_needs_review(*_a: Any, **_k: Any) -> None: ...  # noqa: E704

# ── EDM existence fallback (dead code — structure preserved, never calls API) ─
try:
    from V3.services.edm_checker import edm_awb_exists_fallback  # type: ignore[import-untyped]
except Exception:
    def edm_awb_exists_fallback(_awb: str) -> Optional[bool]:  # noqa: D401
        """Dead-code stub — always returns None so the pipeline skips EDM."""
        return None


# ── Config aliases ───────────────────────────────────────────────────────────
AWB_LEN                = config.AWB_LEN
DPI_MAIN               = config.OCR_DPI_MAIN
DPI_STRONG             = config.OCR_DPI_STRONG
OCR_MAIN_PSMS          = config.OCR_MAIN_PSMS
OCR_STRONG_PSMS        = config.OCR_STRONG_PSMS
ROTATION_PROBE_DPI     = config.ROTATION_PROBE_DPI
NEEDS_REVIEW_DIR       = config.NEEDS_REVIEW_DIR
ENABLE_ROTATION_LAST_RESORT = config.ENABLE_ROTATION_LAST_RESORT
ENABLE_UPSCALED_RESCUE_PASS = config.ENABLE_UPSCALED_RESCUE_PASS
ENABLE_AIRWAY_LABEL_RESCUE  = config.ENABLE_AIRWAY_LABEL_RESCUE
LOG_STAGE_SNAPSHOTS    = config.LOG_STAGE_SNAPSHOTS
CANDIDATE_SNAPSHOT_LIMIT = config.CANDIDATE_SNAPSHOT_LIMIT
ROTATION_PROBE_MIN_FLIP_MARGIN  = config.ROTATION_PROBE_MIN_FLIP_MARGIN
ROTATION_PROBE_DIGIT_CLEAR_MARGIN = config.ROTATION_PROBE_DIGIT_CLEAR_MARGIN
ROTATION_PROBE_CERTAIN_MARGIN   = config.ROTATION_PROBE_CERTAIN_MARGIN
ROTATION_PROBE_LIKELY_MARGIN    = config.ROTATION_PROBE_LIKELY_MARGIN
LONG_PASS_TIMEOUT_SECONDS       = config.LONG_PASS_TIMEOUT_SECONDS


# =============================================================================
# TIMEOUT EXCEPTION
# =============================================================================
class _TimeoutDeferred(Exception):
    """Raised inside process_pdf when the per-file long-pass time budget is
    exceeded.  Caught at the top of process_pdf; state is captured and the file
    is queued for the third-pass tier."""


# =============================================================================
# ROTATION PROBE — keyword-scored rotation detection
# =============================================================================
ALLOWED_ROTATION_ANGLES = (0, 90, 180, 270)

_PROBE_KEYWORDS = (
    "AWB", "AWB NO", "AWB NUMBER", "AIRWAY", "WAYBILL", "AIRWAY BILL NUMBER",
    "AIR WAY BILL", "TRACKING", "TRACKING NO", "TRACKING #",
    "FDX", "FDE", "FDXE", "FEDEX", "FED-EX", "FDX TRACKING", "FDXE TRACKING",
    "FEDEX TRACKING", "AIRWAY BILL", "BILL NUMBER", "BILL NO",
    "HAWB", "MAWB", "ACI", "CARGO CONTROL NUMBER", "CCN",
    "COMMERCIAL INVOICE", "SHIPMENT", "SHIPPER", "CONSIGNEE", "TRK", "TRK#",
)


def rotation_probe_best(
    img_lowdpi,
    return_scores: bool = False,
    preferred_angles=None,
):
    """Raw-rotate-first probe with keyword scoring.

    Returns ``(best_rot, scores_dict, probe_texts_dict)`` when
    *return_scores=True*.  ``probe_texts_dict`` maps ``rot -> (digit_text,
    general_text)`` for reuse.

    *preferred_angles*: optional subset of ``ALLOWED_ROTATION_ANGLES`` to
    probe.  Use only when a strong external hint already narrows the likely
    angle.  Missing angles are filled with score=0 so downstream logic stays
    consistent.
    """
    angles = tuple(preferred_angles) if preferred_angles else ALLOWED_ROTATION_ANGLES
    digit_scores: Dict[int, int] = {}
    probe_texts: Dict[int, Tuple[str, str]] = {}

    for rot in angles:
        rimg = img_lowdpi.rotate(rot, expand=True) if rot else img_lowdpi
        t_digits = ocr_digits_only(preprocess(rimg, thr=175, invert=False), psm=6)
        digit_scores[rot] = digit_score(t_digits)
        probe_texts[rot] = (t_digits, "")  # general text filled lazily below

    # Fill scores for any angles not probed with 0
    for _fill_rot in ALLOWED_ROTATION_ANGLES:
        if _fill_rot not in digit_scores:
            digit_scores[_fill_rot] = 0
            probe_texts[_fill_rot] = ("", "")

    ranked = sorted(digit_scores.items(), key=lambda x: x[1], reverse=True)
    best_digit_rot, best_digit_sc = ranked[0]
    second_digit_sc = ranked[1][1] if len(ranked) > 1 else -1

    # Fast path: 0 deg clearly wins on digits -> skip expensive text OCR
    if (
        0 in angles
        and best_digit_rot == 0
        and (best_digit_sc - second_digit_sc) >= ROTATION_PROBE_DIGIT_CLEAR_MARGIN
    ):
        if return_scores:
            return 0, {k: float(v) for k, v in digit_scores.items()}, probe_texts
        return 0

    scores: Dict[int, float] = {}
    for rot in angles:
        rimg = img_lowdpi.rotate(rot, expand=True) if rot else img_lowdpi
        t_text = ocr_text_general(preprocess_for_text(rimg, invert=False), psm=6)
        tu = (t_text or "").upper()
        probe_texts[rot] = (probe_texts[rot][0], t_text)

        kw_hits = sum(1 for kw in _PROBE_KEYWORDS if kw in tu)
        coherent = sum(1 for w in re.findall(r"[A-Za-z]{4,}", t_text or "") if w.isalpha())
        scores[rot] = digit_scores[rot] + (kw_hits * 120) + (coherent * 2)

    # Fill scores for unprobed angles
    for _fill_rot in ALLOWED_ROTATION_ANGLES:
        if _fill_rot not in scores:
            scores[_fill_rot] = 0

    best_rot = max(scores, key=lambda r: scores[r])
    if best_rot != 0 and (scores[best_rot] - scores.get(0, 0)) < ROTATION_PROBE_MIN_FLIP_MARGIN:
        best_rot = 0
    if best_rot not in ALLOWED_ROTATION_ANGLES:
        best_rot = 0

    if return_scores:
        return best_rot, {k: float(v) for k, v in scores.items()}, probe_texts
    return best_rot


# =============================================================================
# PROCESS_PDF — the complete pipeline orchestrator
# =============================================================================

def process_pdf(
    pdf_path: str,
    awb_set: Set[str],
    by_prefix: Dict[str, List[str]],
    by_suffix: Dict[str, List[str]],
    allow_long_pass: bool = True,
    timeout_seconds: Optional[float] = None,
    resume_state: Optional[Dict[str, Any]] = None,
    _state_out: Optional[Dict[str, Any]] = None,
) -> str:
    """Run the multi-stage AWB extraction pipeline on a single PDF.

    Parameters
    ----------
    pdf_path : str
        Absolute path to the PDF file.
    awb_set : set[str]
        Master set of known 12-digit AWB numbers.
    by_prefix, by_suffix : dict
        Prefix/suffix bucket dicts for fast Hamming-distance matching.
    allow_long_pass : bool
        ``False`` = fast lane (Stages 0-3 only, defer after Stage 3 fail).
        ``True``  = full pipeline.
    timeout_seconds : float or None
        Abort and defer to third-pass if exceeded (long-pass only).
    resume_state : dict or None
        Dict from a prior timeout; skips Stages 0-3.1.
    _state_out : dict or None
        Mutable dict populated on ``TIMEOUT_DEFERRED`` with all accumulated
        state so the caller can persist and resume later.

    Returns
    -------
    str
        One of ``"MATCHED"``, ``"NEEDS_REVIEW"``, ``"DEFERRED"``, or
        ``"TIMEOUT_DEFERRED"``.
    """
    start_ts = time.perf_counter()
    name = os.path.basename(pdf_path)
    all_tried: Set[str]           = set()
    stage_snapshots: Dict[str, Any] = {}
    quarantine: Dict[str, Set[str]] = {}
    running_high: Set[str]        = set()
    running_standard: Set[str]    = set()
    candidate_stage_hits: Dict[str, Set[str]] = {}
    candidate_confidence: Dict[str, str]      = {}
    image_cache: Dict[Tuple, Any]       = {}
    ocr_cache: Dict[Tuple, str]         = {}
    preprocess_cache: Dict[Tuple, Any]  = {}
    table_clean_cache: list             = [None]

    # Default values for variables set inside early stages.
    # Overwritten by the normal path (if not _proceed_to_route) or by resume restore.
    _is_image_only: bool   = False
    _rotation_hint: Optional[int] = None
    base_angle: int        = 0
    probe_scores: Dict[int, float] = {}
    probe_texts: Dict[int, Tuple[str, str]] = {}
    _angle_certainty: str  = "UNCERTAIN"

    # ── Resume from a prior timeout ─────────────────────────────────────────
    _proceed_to_route = resume_state is not None
    if _proceed_to_route:
        rs = resume_state
        probe_scores     = rs.get("probe_scores", {})
        probe_texts      = rs.get("probe_texts", {})
        base_angle       = rs.get("base_angle", 0)
        _angle_certainty = rs.get("_angle_certainty", "UNCERTAIN")
        _rotation_hint   = rs.get("_rotation_hint", None)
        _is_image_only   = rs.get("_is_image_only", False)
        running_high.update(rs.get("running_high", []))
        running_standard.update(rs.get("running_standard", []))
        for k, v in rs.get("candidate_stage_hits", {}).items():
            candidate_stage_hits[k] = set(v)
        candidate_confidence.update(rs.get("candidate_confidence", {}))
        all_tried.update(rs.get("all_tried", []))
        for k, v in rs.get("quarantine", {}).items():
            quarantine[k] = set(v)
        # ocr_cache is stored as [[key_list, value], ...] pairs
        for pair in rs.get("ocr_cache", []):
            try:
                ocr_cache[tuple(pair[0])] = pair[1]
            except Exception:
                pass
        timings_saved = rs.get("timings", {})
    else:
        timings_saved = {}

    timings: Dict[str, float] = {
        "filename_ms": 0.0,
        "text_layer_ms": 0.0,
        "ocr_main_ms": 0.0,
        "ocr_strong_ms": 0.0,
        "ocr_context_ms": 0.0,
        "rotation_ms": 0.0,
        "total_active_ms": 0.0,
    }
    timings.update(timings_saved)  # carry forward timings from prior pass on resume

    # ── File stability guard ────────────────────────────────────────────────
    if not file_is_stable(pdf_path):
        log(f"[STABILITY] {name} not yet stable — will retry when file settles.")
        return "NEEDS_REVIEW"

    # ── Zero-page / corrupt-file guard ─────────────────────────────────────
    # Runs before closures are defined, so uses direct log/audit/move calls.
    try:
        _check_doc = fitz.open(pdf_path)
        _check_pc = _check_doc.page_count
        _check_doc.close()
        if _check_pc == 0:
            log(f"[SKIP] {name} has 0 pages — moving to NEEDS_REVIEW")
            safe_move(pdf_path, config.NEEDS_REVIEW_DIR)
            record_hotfolder_needs_review(name, "0-page PDF", hotfolder_secs=0.0)
            audit_event("AWB_HOTFOLDER", file=name, status="NEEDS_REVIEW",
                        route="zero-page", reason="PDF has 0 pages", match_method="No Match")
            return "NEEDS_REVIEW"
    except Exception as exc:
        log(f"[SKIP] {name} could not be opened: {exc} — moving to NEEDS_REVIEW")
        try:
            safe_move(pdf_path, config.NEEDS_REVIEW_DIR)
        except Exception:
            pass  # file may already be gone or unreadable
        record_hotfolder_needs_review(name, f"Corrupt/unreadable PDF: {exc}", hotfolder_secs=0.0)
        audit_event("AWB_HOTFOLDER", file=name, status="NEEDS_REVIEW",
                    route="corrupt", reason=str(exc), match_method="No Match")
        return "NEEDS_REVIEW"

    # =====================================================================
    # INTERNAL HELPERS (closures over process_pdf locals)
    # =====================================================================

    def finalize(status, route, reason, match_method, awb=None):
        timings["total_active_ms"] = round((time.perf_counter() - start_ts) * 1000, 1)
        audit_event(
            "AWB_HOTFOLDER",
            file=name,
            awb=awb,
            status=status,
            route=route,
            match_method=match_method,
            reason=reason,
            timings_ms=timings,
        )
        log(
            f"[TIMING] file={name} method={match_method} route={route} "
            f"filename_ms={timings['filename_ms']} text_layer_ms={timings['text_layer_ms']} "
            f"ocr_main_ms={timings['ocr_main_ms']} ocr_strong_ms={timings['ocr_strong_ms']} "
            f"ocr_context_ms={timings['ocr_context_ms']} rotation_ms={timings['rotation_ms']} "
            f"total_active_ms={timings['total_active_ms']}"
        )

    def awb_extract_secs():
        return round(time.perf_counter() - start_ts, 3)

    def snapshot(stage, candidates):
        if not LOG_STAGE_SNAPSHOTS:
            return
        cset = {c for c in (candidates or set()) if isinstance(c, str) and c}
        stage_snapshots[stage] = {
            "count": len(cset),
            "sample": sorted(cset)[:CANDIDATE_SNAPSHOT_LIMIT],
        }

    def log_snapshots():
        if not LOG_STAGE_SNAPSHOTS or not stage_snapshots:
            return
        for stage in sorted(stage_snapshots):
            snap = stage_snapshots[stage]
            log(f"[SNAPSHOT] {stage}: count={snap['count']} sample={snap['sample']}")

    # ── Candidate merging with quarantine logic ─────────────────────────────
    def merge_stage_candidates(high_set, standard_set, stage_name):
        nonlocal running_high, running_standard
        high_set = {
            c for c in (high_set or set())
            if len(c) == AWB_LEN and c.isdigit() and not _is_disqualified_candidate(c)
        }
        standard_set = {
            c for c in (standard_set or set())
            if len(c) == AWB_LEN and c.isdigit()
            and not _is_disqualified_candidate(c)
            and not _is_likely_date_reference(c)
        }
        # Quarantine: single-hit STANDARD candidates from invert or rotation passes
        # that produced a large number of candidates are treated as noisy.
        # They stay out of running_standard until confirmed by a second stage.
        # Label-backed stages (AirwayLabel, ROI) are exempt.
        _is_noisy_source = any(tag in stage_name for tag in (
            "Invert", "AngFallback", "Rotation-180", "Rotation-270",
        ))
        _is_label_backed = "AirwayLabel" in stage_name or "ROI" in stage_name
        if _is_noisy_source and not _is_label_backed and len(standard_set) > 3:
            for c in standard_set:
                if c not in all_tried:
                    quarantine.setdefault(c, set()).add(stage_name)
            all_tried.update(standard_set)
            # Still add HIGH candidates from these stages unconditionally
            running_high.update(high_set)
            for c in high_set:
                candidate_stage_hits.setdefault(c, set()).add(stage_name)
                candidate_confidence[c] = "HIGH"
            return

        all_tried.update(high_set | standard_set)
        running_high.update(high_set)
        running_standard.update(standard_set)
        running_standard.difference_update(running_high)
        for c in high_set:
            candidate_stage_hits.setdefault(c, set()).add(stage_name)
            candidate_confidence[c] = "HIGH"
        for c in standard_set:
            candidate_stage_hits.setdefault(c, set()).add(stage_name)
            candidate_confidence.setdefault(c, "STANDARD")
        # Promote quarantined candidates that now appear in a second stage
        for c in list(quarantine.keys()):
            if c in running_standard or c in running_high:
                del quarantine[c]  # already in pool
            elif c in (high_set | standard_set):
                running_standard.add(c)
                candidate_stage_hits.setdefault(c, set()).update(quarantine.pop(c))
                candidate_stage_hits[c].add(stage_name)

    def _has_quality_candidates():
        """True when candidates are genuinely promising — not just single-pass OCR noise."""
        if running_high:
            return True
        persistent = {
            c for c in running_standard
            if len(candidate_stage_hits.get(c, set())) >= 2
        }
        return bool(persistent)

    # ── PDF page management ─────────────────────────────────────────────────
    page_doc = None
    page = None

    def close_pdf():
        nonlocal page_doc, page
        if page_doc is not None:
            try:
                page_doc.close()
            except Exception:
                pass
            page_doc = None
            page = None

    def complete_match(awb, method, reason):
        log(f"AWB MATCHED ({method}): {awb} ({name})")
        close_pdf()
        append_to_awb_logs_excel(awb, pdf_path, match_method=method)
        processed_path = move_to_processed_renamed(pdf_path, awb)
        processed_name = os.path.basename(processed_path)
        append_stage_cache_row(name, processed_name, awb, method, awb_extract_secs())
        record_hotfolder_end(name, awb, processed_name, method)
        finalize("MATCHED", "PROCESSED", reason, method, awb=awb)
        # Centralized audit (non-blocking — failure never disrupts pipeline)
        if _ca_write_hotfolder is not None:
            try:
                _ca_write_hotfolder(
                    awb=awb,
                    original_filename=name,
                    processed_filename=processed_name,
                    detection_method=method,
                    hotfolder_secs=round(timings.get("total_active_ms", 0) / 1000, 2),
                    ocr_context_ms=timings.get("ocr_context_ms", 0),
                    result="COMPLETE",
                    notes=reason,
                )
            except Exception:
                pass

    def send_review(reason, method):
        log(f"NO MATCH FOUND -> Needs review: {name}")
        log(f"  Reason: {reason}")
        log(f"  Candidates tried: {sorted(all_tried)}")
        for c in sorted(all_tried):
            stages = sorted(candidate_stage_hits.get(c, set()))
            conf = candidate_confidence.get(c, "STANDARD")
            log(f"  Candidate {c} | conf={conf} | stages={stages}")
        if quarantine:
            qlist = sorted(quarantine.keys())
            log(f"  Quarantined noisy candidates (excluded from matching): {qlist}")
            for c in qlist:
                log(f"  Quarantined {c} | stages={sorted(quarantine[c])}")
        log_snapshots()
        close_pdf()
        safe_move(pdf_path, NEEDS_REVIEW_DIR)
        record_hotfolder_needs_review(name, f"{reason} | cands={sorted(all_tried)}")
        finalize("NEEDS-REVIEW", "NEEDS_REVIEW", reason, method)
        if _ca_write_hotfolder is not None:
            try:
                _ca_write_hotfolder(
                    awb=None,
                    original_filename=name,
                    processed_filename=None,
                    detection_method=method,
                    hotfolder_secs=round(timings.get("total_active_ms", 0) / 1000, 2),
                    ocr_context_ms=timings.get("ocr_context_ms", 0),
                    result="NEEDS_REVIEW",
                    notes=reason,
                )
            except Exception:
                pass

    # ── Priority matchers ───────────────────────────────────────────────────
    def run_exact_priority():
        return prioritize_db_match(
            running_high, running_standard, awb_set, by_prefix, by_suffix,
            include_tolerance=False, candidate_stage_hits=candidate_stage_hits,
        )

    def run_full_priority():
        return prioritize_db_match(
            running_high, running_standard, awb_set, by_prefix, by_suffix,
            include_tolerance=True, candidate_stage_hits=candidate_stage_hits,
        )

    # ── Image / OCR caching layer ──────────────────────────────────────────
    def get_page():
        nonlocal page_doc, page
        if page is None:
            page_doc = fitz.open(pdf_path)
            page = page_doc.load_page(0)
        return page

    def get_image(dpi, rot=0):
        key = (dpi, rot)
        if key in image_cache:
            return image_cache[key]
        base_key = (dpi, 0)
        if base_key not in image_cache:
            image_cache[base_key] = render_page_from_page(get_page(), dpi)
        if rot == 0:
            return image_cache[base_key]
        image_cache[key] = image_cache[base_key].rotate(rot, expand=True)
        return image_cache[key]

    def get_preprocessed(dpi, rot, thr, inv):
        img = get_image(dpi, rot)
        p_key = ((dpi, rot), thr, inv)
        if p_key in preprocess_cache:
            return preprocess_cache[p_key]
        result = preprocess(img, thr=thr, invert=inv)
        preprocess_cache[p_key] = result
        return result

    def get_ocr_digits(dpi, rot, thr, inv, psm):
        img_key = (dpi, rot)
        c_key = (img_key, f"dig_{thr}_{int(inv)}", psm)
        if c_key in ocr_cache:
            return ocr_cache[c_key]
        pre = get_preprocessed(dpi, rot, thr, inv)
        txt = ocr_digits_only(pre, psm=psm)
        ocr_cache[c_key] = txt
        return txt

    def get_ocr_text(dpi, rot, inv, psm):
        img_key = (dpi, rot)
        c_key = (img_key, f"txt_{int(inv)}", psm)
        if c_key in ocr_cache:
            return ocr_cache[c_key]
        img = get_image(dpi, rot)
        pre = preprocess_for_text(img, invert=inv)
        txt = ocr_text_general(pre, psm=psm)
        ocr_cache[c_key] = txt
        return txt

    # ── Clean priority gate ─────────────────────────────────────────────────
    def run_clean_priority_gate(text, stage_name):
        clean = {
            c for c in extract_clean_candidates(text)
            if len(c) == AWB_LEN and c.isdigit()
            and not _is_disqualified_candidate(c)
            and not _is_likely_date_reference(c)
        }
        if clean:
            snapshot(f"{stage_name}-Clean", clean)
            clean_db = clean & awb_set
            merge_stage_candidates(clean_db, clean - clean_db, f"{stage_name}-Clean")
            if len(clean_db) == 1:
                return {"status": "matched", "awb": next(iter(clean_db)), "method": "Clean-Exact"}
            if len(clean_db) > 1:
                return {"status": "tie", "ties": sorted(clean_db), "method": "Clean-Exact"}
        return {"status": "none", "method": "Clean-Exact"}

    # ── Timeout check ───────────────────────────────────────────────────────
    def _check_timeout():
        """Raise _TimeoutDeferred when the long-pass time budget is exceeded.
        Only called at natural angle boundaries — never mid-subpass."""
        if timeout_seconds and (time.perf_counter() - start_ts) > timeout_seconds:
            log(
                f"[TIMEOUT] {name} exceeded {timeout_seconds:.0f}s budget — "
                f"deferring to third-pass with {len(running_high)} high / "
                f"{len(running_standard)} std candidates accumulated"
            )
            raise _TimeoutDeferred()

    # =====================================================================
    # STAGE 0 — FILENAME
    # =====================================================================
    log(f"{'[THIRD-PASS] Resuming' if _proceed_to_route else 'Processing'}: {name}")
    if _proceed_to_route:
        log(
            f"[THIRD-PASS] Restored: base_angle={base_angle}° "
            f"certainty={_angle_certainty} "
            f"high={len(running_high)} std={len(running_standard)} "
            f"ocr_cache_entries={len(ocr_cache)}"
        )
    # Only record a new start row on a fresh (non-resume) pass.
    if not _proceed_to_route:
        record_hotfolder_start(name)

    if not _proceed_to_route:
        fn_start = time.perf_counter()
        awb_from_name = extract_awb_from_filename_strict(name)
        timings["filename_ms"] = round((time.perf_counter() - fn_start) * 1000, 1)
        if awb_from_name:
            complete_match(awb_from_name, "Filename", "Matched by strict filename pattern")
            return "MATCHED"

        # =================================================================
        # STAGE 1 — TEXT LAYER (+ set_rotation fallback + spatial word sort)
        # =================================================================
        tl_start = time.perf_counter()
        txt_layer = get_page().get_text("text") or ""

        # 1a. set_rotation fallback for rotated vector PDFs
        if len(txt_layer.strip()) == 0:
            for _hint in [90, 270, 180]:
                try:
                    get_page().set_rotation(_hint)
                    _t = get_page().get_text("text") or ""
                    if len(_t.strip()) > 20:
                        txt_layer = _t
                        log(f"[TEXT-LAYER] Recovered via set_rotation({_hint})")
                        break
                except Exception:
                    pass
            try:
                get_page().set_rotation(0)
            except Exception:
                pass

        # 1b. Spatial word sort for scrambled multi-column stream
        if len(txt_layer.strip()) > 20:
            _words = get_page().get_text("words") or []
            if _words:
                _sorted_txt = " ".join(
                    w[4] for w in sorted(_words, key=lambda w: (round(w[1] / 10) * 10, w[0]))
                )
                _h_raw, _s_raw = extract_tiered_candidates(txt_layer, awb_set)
                _h_srt, _s_srt = extract_tiered_candidates(_sorted_txt, awb_set)
                if len(_h_srt | _s_srt) > len(_h_raw | _s_raw):
                    txt_layer = _sorted_txt
                    log("[TEXT-LAYER] Using spatially sorted word order")

        timings["text_layer_ms"] = round((time.perf_counter() - tl_start) * 1000, 1)

        # 1c. 400-pattern on text layer (no DB check)
        awb_400 = extract_awb_from_400_pattern(txt_layer)
        if awb_400:
            complete_match(awb_400, "TextLayer-400", "Matched via text-layer 400 pattern")
            return "MATCHED"

        # 1d. Clean gate then full tiered extraction on text layer
        clean_res = run_clean_priority_gate(txt_layer, "Text-Layer")
        if clean_res["status"] == "matched":
            complete_match(
                clean_res["awb"],
                f"Text-Layer-{clean_res['method']}",
                "Matched exact DB candidate from text layer",
            )
            return "MATCHED"
        if clean_res["status"] == "tie":
            send_review(
                f"Ambiguous text-layer clean tie: {clean_res.get('ties', [])[:8]}",
                f"Text-Layer-{clean_res['method']}",
            )
            return "NEEDS_REVIEW"

        high1, std1 = extract_tiered_candidates(txt_layer, awb_set)
        merge_stage_candidates(high1, std1, "Text-Layer")
        snapshot("Text-Layer-HIGH", high1)
        snapshot("Text-Layer-STANDARD", std1)
        # Use wider keyword window for clean text layer
        near_kw = extract_candidates_near_keywords(txt_layer, line_lookahead=5, line_lookback=2)
        near_kw_db = near_kw & awb_set
        if near_kw_db:
            merge_stage_candidates(near_kw_db, set(), "Text-Layer-KW")
        res = run_full_priority()
        if res["status"] == "matched":
            complete_match(
                res["awb"],
                f"Text-Layer-{res['method']}",
                "Matched from text-layer candidates",
            )
            return "MATCHED"
        if res["status"] == "tie":
            send_review(
                f"Ambiguous text-layer priority tie: {res.get('ties', [])[:8]}",
                f"Text-Layer-{res['method']}",
            )
            return "NEEDS_REVIEW"

        # =================================================================
        # PRE-OCR ANGLE DETECTION (0ms checks before any image render)
        # =================================================================
        _is_image_only = len((txt_layer or "").strip()) == 0
        _rotation_hint = None

        # Check 1: PDF metadata rotation
        try:
            _page_meta_rot = get_page().rotation
            if _page_meta_rot in (90, 180, 270):
                _rotation_hint = _page_meta_rot
                log(f"[ANGLE-DETECT] PDF metadata rotation={_page_meta_rot}°")
        except Exception:
            pass

        # Check 2: Page aspect ratio
        if _rotation_hint is None:
            try:
                rect = get_page().rect
                if (rect.width / max(rect.height, 1)) > 1.3:
                    _rotation_hint = 90
                    log(f"[ANGLE-DETECT] Landscape page ratio ({rect.width:.0f}x{rect.height:.0f}) — likely 90°")
            except Exception:
                pass

        # Check 3: Text character spread (only when text layer exists)
        if _rotation_hint is None and len(txt_layer.strip()) > 20:
            try:
                _words_chk = get_page().get_text("words") or []
                if len(_words_chk) > 5:
                    xs = [w[0] for w in _words_chk]
                    ys = [w[1] for w in _words_chk]
                    if (max(ys) - min(ys)) > (max(xs) - min(xs)) * 1.5:
                        _rotation_hint = 90
                        log("[ANGLE-DETECT] Text y-spread >> x-spread — likely 90°/270°")
            except Exception:
                pass

        # Check 4: Pixel row variance (~50ms, only if still unknown and cv2 available)
        if _rotation_hint is None and CV2_AVAILABLE and np is not None:
            try:
                tiny = get_image(60, 0)
                arr = np.array(tiny.convert("L"))
                row_var = float(np.var(arr, axis=1).mean())
                col_var = float(np.var(arr, axis=0).mean())
                if col_var > row_var * 1.4:
                    _rotation_hint = 90
                    log(f"[ANGLE-DETECT] Pixel variance col={col_var:.1f} >> row={row_var:.1f} — likely rotated")
            except Exception:
                pass

        # =================================================================
        # STAGE 2 — OCR MAIN at 0 deg
        # =================================================================
        main_start = time.perf_counter()
        _ocr_angle = 0  # always 0 deg for Stages 2-3; probe runs later

        for _psm in OCR_MAIN_PSMS:
            txt_m = get_ocr_digits(DPI_MAIN, _ocr_angle, 175, False, _psm)
            awb_400_m = extract_awb_from_400_pattern(txt_m)
            if awb_400_m:
                timings["ocr_main_ms"] = round((time.perf_counter() - main_start) * 1000, 1)
                complete_match(awb_400_m, f"OCR-Main-PSM{_psm}-400", "Matched by OCR-main 400 pattern")
                return "MATCHED"
            cr = run_clean_priority_gate(txt_m, f"OCR-Main-PSM{_psm}")
            if cr["status"] == "matched":
                timings["ocr_main_ms"] = round((time.perf_counter() - main_start) * 1000, 1)
                complete_match(
                    cr["awb"],
                    f"OCR-Main-PSM{_psm}-{cr['method']}",
                    "Matched exact clean in OCR-main",
                )
                return "MATCHED"
            if cr["status"] == "tie":
                timings["ocr_main_ms"] = round((time.perf_counter() - main_start) * 1000, 1)
                send_review(
                    f"Ambiguous OCR-main PSM{_psm} clean tie: {cr.get('ties', [])[:8]}",
                    f"OCR-Main-PSM{_psm}-{cr['method']}",
                )
                return "NEEDS_REVIEW"
            hm, sm = extract_tiered_candidates(txt_m, awb_set)
            merge_stage_candidates(hm, sm, f"OCR-Main-PSM{_psm}")
            snapshot(f"OCR-Main-PSM{_psm}", hm | sm)
            res = run_exact_priority()
            if res["status"] == "matched":
                timings["ocr_main_ms"] = round((time.perf_counter() - main_start) * 1000, 1)
                complete_match(
                    res["awb"],
                    f"OCR-Main-PSM{_psm}-{res['method']}",
                    "Matched exact in OCR-main",
                )
                return "MATCHED"
            if res["status"] == "tie":
                timings["ocr_main_ms"] = round((time.perf_counter() - main_start) * 1000, 1)
                send_review(
                    f"Ambiguous OCR-main PSM{_psm} exact tie: {res.get('ties', [])[:8]}",
                    f"OCR-Main-PSM{_psm}-{res['method']}",
                )
                return "NEEDS_REVIEW"
            # Skip PSM11 if PSM6 found nothing and earlier stages have quality candidates
            if _psm == 6 and _has_quality_candidates() and not (hm | sm):
                log("[FAST] Skipping OCR-Main PSM11 — PSM6 empty, quality candidates already present")
                break

        # Soft text pass on OCR-Main
        txt_ms = get_ocr_text(DPI_MAIN, _ocr_angle, False, 11)
        if not _has_quality_candidates() or not (running_high | running_standard):
            awb_400_ms = extract_awb_from_400_pattern(txt_ms)
            if awb_400_ms:
                timings["ocr_main_ms"] = round((time.perf_counter() - main_start) * 1000, 1)
                complete_match(awb_400_ms, "OCR-Main-Soft-400", "Matched by OCR-main soft 400 pattern")
                return "MATCHED"
            hms, sms = extract_tiered_candidates(txt_ms, awb_set)
            merge_stage_candidates(hms, sms, "OCR-Main-Soft")
            snapshot("OCR-Main-Soft", hms | sms)

        res = run_full_priority()
        timings["ocr_main_ms"] = round((time.perf_counter() - main_start) * 1000, 1)
        if res["status"] == "matched":
            complete_match(
                res["awb"],
                f"OCR-Main-{res['method']}",
                "Matched after OCR-main sequence",
            )
            return "MATCHED"
        if res["status"] == "tie":
            send_review(
                f"Ambiguous OCR-main priority tie: {res.get('ties', [])[:8]}",
                f"OCR-Main-{res['method']}",
            )
            return "NEEDS_REVIEW"

        # =================================================================
        # STAGE 3 — OCR STRONG at 0 deg
        # =================================================================
        strong_start = time.perf_counter()
        strong_subpasses = [
            ("OCR-Strong-PSM6",  170, False, 6),
            ("OCR-Strong-PSM11", 170, False, 11),
        ]
        # Only add invert passes if normal passes yielded nothing useful
        _run_strong_invert = not _has_quality_candidates()

        for stage_nm, thr, inv, psm in strong_subpasses:
            txt_s = get_ocr_digits(DPI_STRONG, 0, thr, inv, psm)
            awb_400_s = extract_awb_from_400_pattern(txt_s)
            if awb_400_s:
                timings["ocr_strong_ms"] = round((time.perf_counter() - strong_start) * 1000, 1)
                complete_match(awb_400_s, f"{stage_nm}-400", f"Matched by {stage_nm} 400 pattern")
                return "MATCHED"
            cr = run_clean_priority_gate(txt_s, stage_nm)
            if cr["status"] == "matched":
                timings["ocr_strong_ms"] = round((time.perf_counter() - strong_start) * 1000, 1)
                complete_match(cr["awb"], f"{stage_nm}-{cr['method']}", f"Matched clean exact in {stage_nm}")
                return "MATCHED"
            if cr["status"] == "tie":
                timings["ocr_strong_ms"] = round((time.perf_counter() - strong_start) * 1000, 1)
                send_review(
                    f"Ambiguous {stage_nm} clean tie: {cr.get('ties', [])[:8]}",
                    f"{stage_nm}-{cr['method']}",
                )
                return "NEEDS_REVIEW"
            hs, ss = extract_tiered_candidates(txt_s, awb_set)
            merge_stage_candidates(hs, ss, stage_nm)
            snapshot(stage_nm, hs | ss)
            res = run_exact_priority()
            if res["status"] == "matched":
                timings["ocr_strong_ms"] = round((time.perf_counter() - strong_start) * 1000, 1)
                complete_match(res["awb"], f"{stage_nm}-{res['method']}", f"Matched exact in {stage_nm}")
                return "MATCHED"
            if res["status"] == "tie":
                timings["ocr_strong_ms"] = round((time.perf_counter() - strong_start) * 1000, 1)
                send_review(
                    f"Ambiguous {stage_nm} exact tie: {res.get('ties', [])[:8]}",
                    f"{stage_nm}-{res['method']}",
                )
                return "NEEDS_REVIEW"
            if psm == 6:
                _run_strong_invert = not _has_quality_candidates()

        # Stage 3 — invert passes
        if _run_strong_invert:
            for stage_nm, thr, inv, psm in [
                ("OCR-Strong-Invert-PSM6",  200, True, 6),
                ("OCR-Strong-Invert-PSM11", 200, True, 11),
            ]:
                txt_si = get_ocr_digits(DPI_STRONG, 0, thr, inv, psm)
                awb_400_si = extract_awb_from_400_pattern(txt_si)
                if awb_400_si:
                    timings["ocr_strong_ms"] = round((time.perf_counter() - strong_start) * 1000, 1)
                    complete_match(awb_400_si, f"{stage_nm}-400", f"Matched by {stage_nm} 400 pattern")
                    return "MATCHED"
                cr = run_clean_priority_gate(txt_si, stage_nm)
                if cr["status"] == "matched":
                    timings["ocr_strong_ms"] = round((time.perf_counter() - strong_start) * 1000, 1)
                    complete_match(cr["awb"], f"{stage_nm}-{cr['method']}", f"Matched in {stage_nm}")
                    return "MATCHED"
                if cr["status"] == "tie":
                    timings["ocr_strong_ms"] = round((time.perf_counter() - strong_start) * 1000, 1)
                    send_review(
                        f"Ambiguous {stage_nm} clean tie: {cr.get('ties', [])[:8]}",
                        f"{stage_nm}-{cr['method']}",
                    )
                    return "NEEDS_REVIEW"
                hsi, ssi = extract_tiered_candidates(txt_si, awb_set)
                merge_stage_candidates(hsi, ssi, stage_nm)
                snapshot(stage_nm, hsi | ssi)
                res = run_exact_priority()
                if res["status"] == "matched":
                    timings["ocr_strong_ms"] = round((time.perf_counter() - strong_start) * 1000, 1)
                    complete_match(res["awb"], f"{stage_nm}-{res['method']}", f"Matched exact in {stage_nm}")
                    return "MATCHED"
                if res["status"] == "tie":
                    timings["ocr_strong_ms"] = round((time.perf_counter() - strong_start) * 1000, 1)
                    send_review(
                        f"Ambiguous {stage_nm} exact tie: {res.get('ties', [])[:8]}",
                        f"{stage_nm}-{res['method']}",
                    )
                    return "NEEDS_REVIEW"

        # Strong soft pass
        if not _has_quality_candidates() or len(running_high) < 2:
            txt_3e = get_ocr_text(DPI_STRONG, 0, False, 11)
            h3e, s3e = extract_tiered_candidates(txt_3e, awb_set)
            if not (h3e or s3e):
                txt_3e2 = get_ocr_text(DPI_STRONG, 0, False, 6)
                h3e2, s3e2 = extract_tiered_candidates(txt_3e2, awb_set)
                h3e.update(h3e2)
                s3e.update(s3e2)
            merge_stage_candidates(h3e, s3e, "OCR-Strong-Soft")
            snapshot("OCR-Strong-Soft", h3e | s3e)
            if not (h3e or s3e):
                box_cands = extract_candidates_from_ocr_data(
                    preprocess_for_text(get_image(DPI_STRONG, 0), invert=False)
                )
                if box_cands:
                    merge_stage_candidates(set(), box_cands, "OCR-Strong-Boxes")

        res = run_full_priority()
        timings["ocr_strong_ms"] = round((time.perf_counter() - strong_start) * 1000, 1)
        if res["status"] == "matched":
            complete_match(
                res["awb"],
                f"OCR-Strong-{res['method']}",
                "Matched after OCR-strong sequence",
            )
            return "MATCHED"
        if res["status"] == "tie":
            send_review(
                f"Ambiguous OCR-strong priority tie: {res.get('ties', [])[:8]}",
                f"OCR-Strong-{res['method']}",
            )
            return "NEEDS_REVIEW"

        # =================================================================
        # FAST-LANE EXIT
        # =================================================================
        # Defer immediately after Stage 3 fails — probe, ROI, and all rescue
        # stages run only in long-pass so the fast lane drains the inbox ASAP.
        if not allow_long_pass:
            log(f"[FAST-LANE] Deferred after Stage 3 (no match at 0°): {name}")
            close_pdf()
            return "DEFERRED"

        # =================================================================
        # STAGE 3.1 — ROTATION PROBE
        # =================================================================
        probe_img = get_image(ROTATION_PROBE_DPI, 0)
        # For image-only documents in long-pass where pre-checks strongly
        # indicated rotation, narrow the probe to 0 deg + hint angle only.
        _probe_angles = ALLOWED_ROTATION_ANGLES
        if (
            allow_long_pass
            and _is_image_only
            and _rotation_hint in (90, 180, 270)
        ):
            _probe_angles = (0, _rotation_hint)
            log(f"[ROTATION-PROBE] Narrowed probe to {_probe_angles} (image-only + hint={_rotation_hint}°)")
        base_angle, probe_scores, probe_texts = rotation_probe_best(
            probe_img, return_scores=True, preferred_angles=_probe_angles
        )

        if base_angle not in ALLOWED_ROTATION_ANGLES:
            base_angle = 0

        # Override with pre-angle detection hint if probe was uncertain
        if _rotation_hint is not None and base_angle == 0:
            _best_score = probe_scores.get(base_angle, 0)
            _other_scores = [v for k, v in probe_scores.items() if k != base_angle]
            _second = max(_other_scores) if _other_scores else 0
            if (_best_score - _second) < ROTATION_PROBE_MIN_FLIP_MARGIN:
                base_angle = _rotation_hint
                log(f"[ANGLE-DETECT] Pre-check hint overrides uncertain probe → {base_angle}°")

        score_view = {k: int(v) for k, v in sorted(probe_scores.items())}
        if base_angle:
            log(f"[ROTATION-PROBE] Base angle {base_angle}° selected | scores={score_view}")
        else:
            log(f"[ROTATION-PROBE] No rotation needed (0deg) | scores={score_view}")

    # =====================================================================
    # POST-PROBE: available on both fresh and resume paths
    # =====================================================================

    # Stage 3.2 — Probe text early exit (free check using low-DPI OCR
    # already done during the probe).
    _probe_digit_txt, _probe_general_txt = probe_texts.get(base_angle, ("", ""))
    _probe_combined_txt = "\n".join(
        part for part in (_probe_general_txt, _probe_digit_txt) if part
    )
    if _probe_combined_txt:
        # 400 tight-prefix check
        _probe_awb_400 = (
            extract_awb_from_400_pattern(_probe_digit_txt)
            or extract_awb_from_400_pattern(_probe_general_txt)
        )
        if _probe_awb_400:
            complete_match(_probe_awb_400, "Probe-400", "Matched via probe text 400 pattern")
            return "MATCHED"
        # Exact-high check on combined text
        _ph, _ps = extract_tiered_candidates(_probe_combined_txt, awb_set)
        _ph_db = sorted(_ph & awb_set)
        if len(_ph_db) == 1:
            complete_match(_ph_db[0], "Probe-Exact-High", "Matched via probe combined exact high")
            return "MATCHED"

    # Angle certainty tiers
    _margin = probe_scores.get(base_angle, 0) - max(
        (v for k, v in probe_scores.items() if k != base_angle), default=0
    )
    if _margin >= ROTATION_PROBE_CERTAIN_MARGIN:
        _angle_certainty = "CERTAIN"
    elif _margin >= ROTATION_PROBE_LIKELY_MARGIN:
        _angle_certainty = "LIKELY"
    else:
        _angle_certainty = "UNCERTAIN"
    log(f"[ROTATION-PROBE] certainty={_angle_certainty} margin={_margin}")

    # =====================================================================
    # ROUTING DECISION
    # =====================================================================
    _probe_confident_upright = (base_angle == 0)
    _route = "UPRIGHT" if _probe_confident_upright else "ROTATED"
    log(f"[ROUTE] {_route} (base_angle={base_angle}°, image_only={_is_image_only})")

    # =====================================================================
    # STAGE 3.5 — ROI CROP PASS (both routes)
    # =====================================================================
    def _run_roi_pass(src_img, stage_name):
        w, h = src_img.size
        y1 = max(0, int(h * 0.10))
        y2 = min(h, int(h * 0.62))
        if y2 <= y1 + 40:
            return False
        roi = src_img.crop((0, y1, w, y2))
        roi = _upscale(roi, 2)
        txt_roi = "\n".join([
            ocr_text_general(preprocess_for_text(roi, invert=False), psm=6),
            ocr_text_general(preprocess_for_text(roi, invert=False), psm=11),
            ocr_digits_only(preprocess(roi, thr=170, invert=False), psm=6),
        ])
        # Quick 400 check
        awb_400_roi = extract_awb_from_400_pattern(txt_roi)
        if awb_400_roi:
            complete_match(awb_400_roi, f"{stage_name}-400", "Matched by ROI 400 pattern")
            return True
        cr = run_clean_priority_gate(txt_roi, stage_name)
        if cr["status"] == "matched":
            complete_match(cr["awb"], f"{stage_name}-{cr['method']}", "Matched clean in ROI pass")
            return True
        if cr["status"] == "tie":
            send_review(
                f"Ambiguous ROI clean tie: {cr.get('ties', [])[:8]}",
                f"{stage_name}-{cr['method']}",
            )
            return True
        h_roi, s_roi = extract_tiered_candidates(txt_roi, awb_set)
        box_roi = extract_candidates_from_ocr_data(preprocess_for_text(roi, invert=False))
        if box_roi:
            s_roi.update(box_roi)
            s_roi.difference_update(h_roi)
        merge_stage_candidates(h_roi, s_roi, stage_name)
        snapshot(stage_name, h_roi | s_roi)
        res = run_exact_priority()
        if res["status"] == "matched":
            complete_match(res["awb"], f"{stage_name}-{res['method']}", "Matched exact in ROI pass")
            return True
        if res["status"] == "tie":
            send_review(
                f"Ambiguous ROI exact tie: {res.get('ties', [])[:8]}",
                f"{stage_name}-{res['method']}",
            )
            return True
        roi_unique = _unique_awb_candidate_count(h_roi | s_roi)
        if 0 < roi_unique <= 2:
            res = run_full_priority()
            if res["status"] == "matched":
                complete_match(res["awb"], f"{stage_name}-{res['method']}", "Matched in ROI full priority")
                return True
            if res["status"] == "tie":
                send_review(
                    f"Ambiguous ROI priority tie: {res.get('ties', [])[:8]}",
                    f"{stage_name}-{res['method']}",
                )
                return True
        return False

    roi_start = time.perf_counter()
    try:
        if _run_roi_pass(get_image(DPI_STRONG, base_angle), "OCR-ROI-ShipRow"):
            timings["ocr_context_ms"] += round((time.perf_counter() - roi_start) * 1000, 1)
            return "MATCHED"
        if _is_image_only:
            for _rot_roi in (90, 270):
                if _run_roi_pass(get_image(DPI_STRONG, _rot_roi), f"OCR-ROI-ShipRow-Rot{_rot_roi}"):
                    timings["ocr_context_ms"] += round((time.perf_counter() - roi_start) * 1000, 1)
                    return "MATCHED"
    except Exception as e:
        log(f"[ROI-PASS] Warning: {e}")
    timings["ocr_context_ms"] += round((time.perf_counter() - roi_start) * 1000, 1)

    # =====================================================================
    # ROUTE EXECUTION HELPERS
    # =====================================================================

    # ── Stage 5.5 — Upscale 3x rescue ──────────────────────────────────────
    def _run_upscale_rescue():
        # Only run if there is at most one HIGH-confidence persistent candidate.
        rescue_trigger = [
            c for c in sorted(all_tried)
            if len(candidate_stage_hits.get(c, set())) >= 2
            and not _is_disqualified_candidate(c)
            and not _is_likely_date_reference(c)
            and candidate_confidence.get(c) == "HIGH"
        ]
        if len(rescue_trigger) > 1:
            return False  # multiple HIGH-confidence candidates — genuinely ambiguous
        rsc_start = time.perf_counter()
        try:
            base_src = table_clean_cache[0] if table_clean_cache[0] else get_image(DPI_STRONG, base_angle)
            upscaled = _upscale(base_src, 3)
            txt_rsc = "\n".join([
                ocr_text_general(preprocess_for_text(upscaled, invert=False), psm=6),
                ocr_text_general(preprocess_for_text(upscaled, invert=False), psm=11),
                ocr_digits_only(preprocess(upscaled, thr=170, invert=False), psm=6),
            ])
            awb_400_rsc = extract_awb_from_400_pattern(txt_rsc)
            if awb_400_rsc:
                timings["ocr_context_ms"] += round((time.perf_counter() - rsc_start) * 1000, 1)
                complete_match(awb_400_rsc, "OCR-Rescue-Upscaled-400", "Matched by upscale rescue 400")
                return True
            cr = run_clean_priority_gate(txt_rsc, "OCR-Rescue-Upscaled")
            if cr["status"] == "matched":
                timings["ocr_context_ms"] += round((time.perf_counter() - rsc_start) * 1000, 1)
                complete_match(cr["awb"], f"OCR-Rescue-Upscaled-{cr['method']}", "Matched in upscale rescue")
                return True
            if cr["status"] == "tie":
                timings["ocr_context_ms"] += round((time.perf_counter() - rsc_start) * 1000, 1)
                send_review(
                    f"Upscale rescue tie: {cr.get('ties', [])[:8]}",
                    f"OCR-Rescue-Upscaled-{cr['method']}",
                )
                return True
            h_rsc, s_rsc = extract_tiered_candidates(txt_rsc, awb_set)
            try:
                box_rsc = extract_candidates_from_ocr_data(
                    preprocess_for_text(upscaled, invert=False)
                )
                box_rsc = {
                    c for c in box_rsc
                    if not _is_disqualified_candidate(c) and not _is_likely_date_reference(c)
                }
                if box_rsc:
                    if len(box_rsc) <= 2:
                        h_rsc.update(box_rsc)
                    else:
                        s_rsc.update(box_rsc)
                    s_rsc.difference_update(h_rsc)
            except Exception:
                pass
            merge_stage_candidates(h_rsc, s_rsc, "OCR-Rescue-Upscaled")
            snapshot("OCR-Rescue-Upscaled", h_rsc | s_rsc)
            res = run_full_priority()
            timings["ocr_context_ms"] += round((time.perf_counter() - rsc_start) * 1000, 1)
            if res["status"] == "matched":
                complete_match(res["awb"], f"OCR-Rescue-Upscaled-{res['method']}", "Matched by upscale rescue")
                return True
            if res["status"] == "tie":
                send_review(
                    f"Upscale rescue priority tie: {res.get('ties', [])[:8]}",
                    f"OCR-Rescue-Upscaled-{res['method']}",
                )
                return True
        except Exception as e:
            log(f"[RESCUE-UPSCALED] Warning: {e}")
        return False

    # ── Stage 5 — Table line removal ────────────────────────────────────────
    def _run_table_pass():
        tbl_start = time.perf_counter()
        tbl_img = remove_table_lines_image(get_image(DPI_STRONG, base_angle))
        if tbl_img is None:
            log("[TABLE-PASS] cv2 unavailable — skipping table pass.")
            return False
        table_clean_cache[0] = tbl_img  # store for upscale rescue reuse
        txt_t = ocr_text_general(preprocess_for_text(tbl_img, invert=False), psm=3)
        awb_400_t = extract_awb_from_400_pattern(txt_t)
        if awb_400_t:
            timings["ocr_context_ms"] += round((time.perf_counter() - tbl_start) * 1000, 1)
            complete_match(awb_400_t, "OCR-Table-PSM3-400", "Matched by table pass 400 pattern")
            return True
        cr = run_clean_priority_gate(txt_t, "OCR-Table-PSM3")
        if cr["status"] == "matched":
            timings["ocr_context_ms"] += round((time.perf_counter() - tbl_start) * 1000, 1)
            complete_match(cr["awb"], f"OCR-Table-PSM3-{cr['method']}", "Matched clean in table pass")
            return True
        if cr["status"] == "tie":
            timings["ocr_context_ms"] += round((time.perf_counter() - tbl_start) * 1000, 1)
            send_review(
                f"Table pass clean tie: {cr.get('ties', [])[:8]}",
                f"OCR-Table-PSM3-{cr['method']}",
            )
            return True
        ht, st = extract_tiered_candidates(txt_t, awb_set)
        try:
            box_t = {
                c for c in extract_candidates_from_ocr_data(
                    preprocess_for_text(tbl_img, invert=False)
                )
                if not _is_disqualified_candidate(c) and not _is_likely_date_reference(c)
            }
            if box_t:
                if len(box_t) <= 2:
                    ht.update(box_t)
                else:
                    st.update(box_t)
                st.difference_update(ht)
        except Exception:
            pass
        merge_stage_candidates(ht, st, "OCR-Table-PSM3")
        snapshot("OCR-Table-PSM3", ht | st)
        res = run_full_priority()
        timings["ocr_context_ms"] += round((time.perf_counter() - tbl_start) * 1000, 1)
        if res["status"] == "matched":
            complete_match(res["awb"], f"OCR-Table-{res['method']}", "Matched by table pass")
            return True
        if res["status"] == "tie":
            send_review(
                f"Ambiguous table-pass priority tie: {res.get('ties', [])[:8]}",
                f"OCR-Table-{res['method']}",
            )
            return True
        return False

    # ── Stage 4 — Rotation passes (full) ───────────────────────────────────
    def _run_rotation_passes():
        if not ENABLE_ROTATION_LAST_RESORT:
            return False
        rot_start = time.perf_counter()

        # Build angle order using probe scores
        remaining = [r for r in [90, 180, 270, 0] if r != base_angle]
        if _angle_certainty == "CERTAIN":
            # Certain of base_angle — run it but defer other angles to final fallback.
            # For base_angle == 0 the full OCR was already done in Stages 2-3, so skip.
            if base_angle == 0:
                timings["rotation_ms"] += round((time.perf_counter() - rot_start) * 1000, 1)
                return False
            rotation_order = [base_angle]
        elif _angle_certainty == "LIKELY":
            # Only try probe angle first; others deferred
            rotation_order = [base_angle] if base_angle != 0 else []
        else:
            rotation_order = sorted(
                remaining,
                key=lambda r: probe_scores.get(r, 0),
                reverse=True,
            )
            if base_angle != 0:
                rotation_order = [base_angle] + [r for r in rotation_order if r != base_angle]

        # Pre-angle hint prioritisation
        if (
            _rotation_hint in (90, 180, 270)
            and _rotation_hint in rotation_order
            and _angle_certainty == "UNCERTAIN"
        ):
            rotation_order = [_rotation_hint] + [
                a for a in rotation_order if a != _rotation_hint
            ]
            log(f"[ROTATION] Pre-angle hint {_rotation_hint}° moved to front of rotation order")

        for rot in rotation_order:
            rimg = get_image(DPI_STRONG, rot)
            rot_subpasses = [
                (f"OCR-Rotation-{rot}-PSM6",   170, False, 6),
                (f"OCR-Rotation-{rot}-PSM11",  170, False, 11),
            ]
            _run_rot_invert = not _has_quality_candidates()
            if _run_rot_invert:
                rot_subpasses += [
                    (f"OCR-Rotation-{rot}-Invert-PSM6",  200, True, 6),
                    (f"OCR-Rotation-{rot}-Invert-PSM11", 200, True, 11),
                ]

            for stage_nm, thr, inv, psm in rot_subpasses:
                txt_r = get_ocr_digits(DPI_STRONG, rot, thr, inv, psm)
                awb_400_r = extract_awb_from_400_pattern(txt_r)
                if awb_400_r:
                    timings["rotation_ms"] += round((time.perf_counter() - rot_start) * 1000, 1)
                    complete_match(awb_400_r, f"{stage_nm}-400", f"Matched by {stage_nm} 400 pattern")
                    return True
                cr = run_clean_priority_gate(txt_r, stage_nm)
                if cr["status"] == "matched":
                    timings["rotation_ms"] += round((time.perf_counter() - rot_start) * 1000, 1)
                    complete_match(cr["awb"], f"{stage_nm}-{cr['method']}", f"Matched clean in {stage_nm}")
                    return True
                if cr["status"] == "tie":
                    timings["rotation_ms"] += round((time.perf_counter() - rot_start) * 1000, 1)
                    send_review(
                        f"Ambiguous {stage_nm} clean tie: {cr.get('ties', [])[:8]}",
                        f"{stage_nm}-{cr['method']}",
                    )
                    return True
                hr, sr = extract_tiered_candidates(txt_r, awb_set)
                merge_stage_candidates(hr, sr, stage_nm)
                snapshot(stage_nm, hr | sr)
                res = run_exact_priority()
                if res["status"] == "matched":
                    timings["rotation_ms"] += round((time.perf_counter() - rot_start) * 1000, 1)
                    complete_match(res["awb"], f"{stage_nm}-{res['method']}", f"Matched exact in {stage_nm}")
                    return True
                if res["status"] == "tie":
                    timings["rotation_ms"] += round((time.perf_counter() - rot_start) * 1000, 1)
                    send_review(
                        f"Ambiguous {stage_nm} exact tie: {res.get('ties', [])[:8]}",
                        f"{stage_nm}-{res['method']}",
                    )
                    return True
                if psm == 6:
                    _run_rot_invert = not _has_quality_candidates()

            # Rotation soft pass
            txt_rs = get_ocr_text(DPI_STRONG, rot, False, 11)
            awb_400_rs = extract_awb_from_400_pattern(txt_rs)
            if awb_400_rs:
                timings["rotation_ms"] += round((time.perf_counter() - rot_start) * 1000, 1)
                complete_match(awb_400_rs, f"OCR-Rotation-{rot}-Soft-400", "Matched by rotation soft 400")
                return True
            hrs, srs = extract_tiered_candidates(txt_rs, awb_set)
            merge_stage_candidates(hrs, srs, f"OCR-Rotation-{rot}-Soft")
            snapshot(f"OCR-Rotation-{rot}-Soft", hrs | srs)
            if not (hrs or srs):
                box_r = extract_candidates_from_ocr_data(preprocess_for_text(rimg, invert=False))
                if box_r:
                    merge_stage_candidates(set(), box_r, f"OCR-Rotation-{rot}-Boxes")
            res = run_full_priority()
            if res["status"] == "matched":
                timings["rotation_ms"] += round((time.perf_counter() - rot_start) * 1000, 1)
                complete_match(
                    res["awb"],
                    f"OCR-Rotation-{rot}-{res['method']}",
                    f"Matched after rotation {rot}°",
                )
                return True
            if res["status"] == "tie":
                timings["rotation_ms"] += round((time.perf_counter() - rot_start) * 1000, 1)
                send_review(
                    f"Ambiguous rotation {rot}° priority tie: {res.get('ties', [])[:8]}",
                    f"OCR-Rotation-{rot}-{res['method']}",
                )
                return True

            # Angle complete — check budget before starting next angle
            _check_timeout()

        timings["rotation_ms"] += round((time.perf_counter() - rot_start) * 1000, 1)
        return False

    # =====================================================================
    # ROUTE EXECUTION — wrapped so timeout captures all accumulated state
    # =====================================================================
    try:
        # Execute routes
        if _route == "UPRIGHT":
            # Stage 5.5 -> 5 -> 4 (last resort)
            if ENABLE_UPSCALED_RESCUE_PASS and _run_upscale_rescue():
                return "MATCHED"
            if _run_table_pass():
                return "MATCHED"
            if _run_rotation_passes():
                return "MATCHED"
        else:
            # ROTATED: Stage 4 -> 5 -> 5.5
            if _run_rotation_passes():
                return "MATCHED"
            if _run_table_pass():
                return "MATCHED"
            if ENABLE_UPSCALED_RESCUE_PASS and _run_upscale_rescue():
                return "MATCHED"

        # Final angle fallback for CERTAIN/LIKELY — try deferred angles now
        if _angle_certainty in ("CERTAIN", "LIKELY") and ENABLE_ROTATION_LAST_RESORT:
            _deferred = sorted(
                [r for r in [90, 180, 270, 0] if r != base_angle],
                key=lambda r: probe_scores.get(r, 0),
                reverse=True,
            )
            rot_fb_start = time.perf_counter()
            for rot in _deferred:
                rimg = get_image(DPI_STRONG, rot)
                for stage_nm, thr, inv, psm in [
                    (f"OCR-AngFallback-{rot}-PSM6",   170, False, 6),
                    (f"OCR-AngFallback-{rot}-PSM11",  170, False, 11),
                    (f"OCR-AngFallback-{rot}-Inv6",   200, True,  6),
                    (f"OCR-AngFallback-{rot}-Inv11",  200, True,  11),
                ]:
                    txt_fb = get_ocr_digits(DPI_STRONG, rot, thr, inv, psm)
                    awb_400_fb = extract_awb_from_400_pattern(txt_fb)
                    if awb_400_fb:
                        timings["rotation_ms"] += round((time.perf_counter() - rot_fb_start) * 1000, 1)
                        complete_match(awb_400_fb, f"{stage_nm}-400", "Matched in angle fallback")
                        return "MATCHED"
                    cr = run_clean_priority_gate(txt_fb, stage_nm)
                    if cr["status"] == "matched":
                        timings["rotation_ms"] += round((time.perf_counter() - rot_fb_start) * 1000, 1)
                        complete_match(cr["awb"], f"{stage_nm}-{cr['method']}", "Matched clean in angle fallback")
                        return "MATCHED"
                    if cr["status"] == "tie":
                        timings["rotation_ms"] += round((time.perf_counter() - rot_fb_start) * 1000, 1)
                        send_review(
                            f"Angle fallback tie: {cr.get('ties', [])[:8]}",
                            f"{stage_nm}-{cr['method']}",
                        )
                        return "NEEDS_REVIEW"
                    hfb, sfb = extract_tiered_candidates(txt_fb, awb_set)
                    merge_stage_candidates(hfb, sfb, stage_nm)
                    res = run_exact_priority()
                    if res["status"] == "matched":
                        timings["rotation_ms"] += round((time.perf_counter() - rot_fb_start) * 1000, 1)
                        complete_match(res["awb"], f"{stage_nm}-{res['method']}", "Matched in angle fallback")
                        return "MATCHED"
                    if res["status"] == "tie":
                        timings["rotation_ms"] += round((time.perf_counter() - rot_fb_start) * 1000, 1)
                        send_review(
                            f"Angle fallback exact tie: {res.get('ties', [])[:8]}",
                            f"{stage_nm}-{res['method']}",
                        )
                        return "NEEDS_REVIEW"
            # After all fallback angles, run full priority (incl. tolerance)
            # on the accumulated candidates so near-misses from these angles
            # are not lost.
            res = run_full_priority()
            timings["rotation_ms"] += round((time.perf_counter() - rot_fb_start) * 1000, 1)
            if res["status"] == "matched":
                complete_match(
                    res["awb"],
                    f"OCR-AngFallback-{res['method']}",
                    "Matched after angle fallback (full priority)",
                )
                return "MATCHED"
            if res["status"] == "tie":
                send_review(
                    f"Angle fallback full priority tie: {res.get('ties', [])[:8]}",
                    f"OCR-AngFallback-{res['method']}",
                )
                return "NEEDS_REVIEW"

        # =================================================================
        # STAGE 5.6 — AIRWAY LABEL RESCUE
        # =================================================================
        _run_airway = ENABLE_AIRWAY_LABEL_RESCUE and (
            _is_image_only or (base_angle in (90, 270)) or not _has_quality_candidates()
        )
        if _run_airway:
            label_start = time.perf_counter()
            _MAX_LABEL_RESCUE_MS = int(config.MAX_CONTEXT_RESCUE_MS)
            try:
                rot_order: list = []
                for r in (base_angle, (base_angle + 180) % 360, 0):
                    if r not in rot_order:
                        rot_order.append(r)

                for rot in rot_order:
                    # Time budget guard
                    if (time.perf_counter() - label_start) * 1000 > _MAX_LABEL_RESCUE_MS:
                        log(f"[AIRWAY-LABEL] Time budget ({_MAX_LABEL_RESCUE_MS}ms) reached — stopping rescue")
                        break
                    src = get_image(DPI_STRONG, rot)
                    w_src, h_src = src.size
                    crops = [
                        ("RightMid",   (int(w_src * 0.50), int(h_src * 0.24), w_src, int(h_src * 0.62))),
                        ("UpperRight", (int(w_src * 0.40), int(h_src * 0.05), w_src, int(h_src * 0.45))),
                        ("RightWide",  (int(w_src * 0.32), int(h_src * 0.12), w_src, int(h_src * 0.70))),
                    ]
                    _rot_found_digits = False  # track if any crop at this rot yielded digits
                    for _crop_idx, (crop_name, box) in enumerate(crops):
                        x1, y1, x2, y2 = box
                        if x2 <= x1 + 30 or y2 <= y1 + 30:
                            continue
                        # Early skip: if first crop at this rotation found zero
                        # digits, remaining crops at the same rotation are unlikely
                        # to have the AWB label — skip to save OCR calls.
                        if _crop_idx > 0 and not _rot_found_digits:
                            break
                        crop = src.crop((x1, y1, x2, y2))
                        crop = _upscale(crop, 3)
                        # Two-step OCR: run fast digit pass first; only run the
                        # expensive general-text passes if digits are present.
                        _lbl_dig1 = ocr_digits_only(preprocess(crop, thr=170, invert=False), psm=6)
                        _lbl_dig2 = ocr_digits_only(preprocess(crop, thr=160, invert=False), psm=7)
                        _has_digits = digit_score(_lbl_dig1 + _lbl_dig2) >= 10
                        if _has_digits:
                            _rot_found_digits = True
                            txt_lbl = "\n".join([
                                ocr_text_general(preprocess_for_text(crop, invert=False), psm=6),
                                ocr_text_general(preprocess_for_text(crop, invert=False), psm=11),
                                ocr_text_general(preprocess_for_text(crop, invert=False), psm=7),
                                _lbl_dig1,
                                _lbl_dig2,
                            ])
                        else:
                            txt_lbl = "\n".join([_lbl_dig1, _lbl_dig2])
                        cr = run_clean_priority_gate(txt_lbl, f"OCR-AirwayLabel-Rot{rot}-{crop_name}")
                        if cr["status"] == "matched":
                            timings["ocr_context_ms"] += round((time.perf_counter() - label_start) * 1000, 1)
                            complete_match(
                                cr["awb"],
                                f"OCR-AirwayLabel-Rot{rot}-{crop_name}-{cr['method']}",
                                "Matched clean in airway-label rescue",
                            )
                            return "MATCHED"
                        if cr["status"] == "tie":
                            timings["ocr_context_ms"] += round((time.perf_counter() - label_start) * 1000, 1)
                            send_review(
                                f"Airway-label rescue clean tie: {cr.get('ties', [])[:8]}",
                                f"OCR-AirwayLabel-Rot{rot}-{crop_name}-{cr['method']}",
                            )
                            return "NEEDS_REVIEW"
                        h_l, s_l = extract_tiered_candidates(txt_lbl, awb_set)
                        merge_stage_candidates(h_l, s_l, f"OCR-AirwayLabel-Rot{rot}-{crop_name}")
                        snapshot(f"OCR-AirwayLabel-Rot{rot}-{crop_name}", h_l | s_l)
                        res = run_exact_priority()
                        if res["status"] == "matched":
                            timings["ocr_context_ms"] += round((time.perf_counter() - label_start) * 1000, 1)
                            complete_match(
                                res["awb"],
                                f"OCR-AirwayLabel-Rot{rot}-{crop_name}-{res['method']}",
                                "Matched by airway-label rescue (exact)",
                            )
                            return "MATCHED"
                        if res["status"] == "tie":
                            timings["ocr_context_ms"] += round((time.perf_counter() - label_start) * 1000, 1)
                            send_review(
                                f"Airway-label rescue exact tie: {res.get('ties', [])[:8]}",
                                f"OCR-AirwayLabel-Rot{rot}-{crop_name}-{res['method']}",
                            )
                            return "NEEDS_REVIEW"

                # Guarded full priority if tiny stable label candidate set
                recent_lbl = {
                    c for c in all_tried
                    if any("OCR-AirwayLabel-" in s for s in candidate_stage_hits.get(c, set()))
                }
                if 0 < len(recent_lbl) <= 2:
                    res = run_full_priority()
                    if res["status"] == "matched":
                        timings["ocr_context_ms"] += round((time.perf_counter() - label_start) * 1000, 1)
                        complete_match(
                            res["awb"],
                            f"OCR-AirwayLabel-{res['method']}",
                            "Matched by airway-label rescue (guarded full priority)",
                        )
                        return "MATCHED"
                    if res["status"] == "tie":
                        timings["ocr_context_ms"] += round((time.perf_counter() - label_start) * 1000, 1)
                        send_review(
                            f"Airway-label rescue priority tie: {res.get('ties', [])[:8]}",
                            f"OCR-AirwayLabel-{res['method']}",
                        )
                        return "NEEDS_REVIEW"
            except Exception as e:
                log(f"[AIRWAY-LABEL-RESCUE] Warning: {e}")
            timings["ocr_context_ms"] += round((time.perf_counter() - label_start) * 1000, 1)

        # =================================================================
        # STAGE 6 — EDM PERSISTENCE FALLBACK (dead code — structure stays,
        #            never calls API because edm_awb_exists_fallback is a
        #            no-op stub that always returns None)
        # =================================================================
        persistent = [
            c for c in sorted(all_tried)
            if (
                len(c) == AWB_LEN and c.isdigit()
                and not _is_disqualified_candidate(c)
                and not _is_likely_date_reference(c)
                and len(candidate_stage_hits.get(c, set())) >= 2
                and candidate_confidence.get(c) == "HIGH"  # HIGH only — guards against noise
            )
        ]

        if len(persistent) == 1:
            edm_candidate = persistent[0]
            edm_exists = edm_awb_exists_fallback(edm_candidate)
            if edm_exists:
                complete_match(
                    edm_candidate,
                    "EDM-Exists-Persistent",
                    "Single HIGH-confidence persistent candidate confirmed by EDM",
                )
                return "MATCHED"
            log(f"[EDM-AWB-FALLBACK] Persistent candidate {edm_candidate} not confirmed by EDM.")
        elif len(persistent) > 1:
            send_review(
                f"EDM fallback tie across persistent candidates: {persistent[:8]}",
                "EDM-Persistent-Tie",
            )
            return "NEEDS_REVIEW"

        # =================================================================
        # STAGE 7 — NEEDS REVIEW
        # =================================================================
        send_review("No AWB match after exhausting all stages", "No-Match")
        return "NEEDS_REVIEW"

    except _TimeoutDeferred:
        # Capture all accumulated state so the third-pass can resume without
        # re-running any stage that already completed.
        _captured: Dict[str, Any] = {
            "probe_scores":         dict(probe_scores),
            "probe_texts":          {k: (v[0], v[1]) for k, v in probe_texts.items()},
            "base_angle":           base_angle,
            "_angle_certainty":     _angle_certainty,
            "_rotation_hint":       _rotation_hint,
            "_is_image_only":       _is_image_only,
            "running_high":         list(running_high),
            "running_standard":     list(running_standard),
            "candidate_stage_hits": {k: list(v) for k, v in candidate_stage_hits.items()},
            "candidate_confidence": dict(candidate_confidence),
            "all_tried":            list(all_tried),
            "quarantine":           {k: list(v) for k, v in quarantine.items()},
            # ocr_cache: serialise as [[key_list, value], ...] pairs so that
            # tuple keys round-trip correctly through JSON.
            "ocr_cache":            [
                [list(k), v] for k, v in ocr_cache.items()
                if isinstance(v, str)
            ],
            "timings":              dict(timings),
        }
        if _state_out is not None:
            _state_out.update(_captured)
        close_pdf()
        return "TIMEOUT_DEFERRED"
