# V3/core/ocr_engine.py
# All OCR operations: PDF rendering, image preprocessing, Tesseract wrappers,
# scoring helpers, upscaling, table-line removal, and spatial OCR box analysis.
#
# Extracted from Scripts/awb_hotfolder_V2.py (monolith).
# Every function is a direct, complete port — no logic simplified or removed.

from __future__ import annotations

import re
from typing import Optional, Set, Tuple

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

from PIL import Image, ImageOps
import pytesseract
from pytesseract import Output

from V3 import config

# ── Optional OpenCV ──────────────────────────────────────────────────────────
try:
    import cv2
    import numpy as np

    CV2_AVAILABLE = True
except ImportError:
    cv2 = None  # type: ignore[assignment]
    np = None  # type: ignore[assignment]
    CV2_AVAILABLE = False

# ── Tesseract path from config ───────────────────────────────────────────────
pytesseract.pytesseract.tesseract_cmd = str(config.TESSERACT_PATH)

# =============================================================================
# Tesseract call counter  (instrumentation — zero overhead in production)
# =============================================================================
_tess_call_count: int = 0
_tess_psm_counts: dict = {}   # {psm_key: int}  e.g. {"dig_psm6": 3, "txt_psm11": 1}


def get_call_count() -> int:
    """Return total Tesseract subprocess invocations since last reset_call_count()."""
    return _tess_call_count


def get_psm_counts() -> dict:
    """Return per-mode call breakdown since last reset_call_count()."""
    return dict(_tess_psm_counts)


def reset_call_count() -> None:
    """Reset the per-file Tesseract call counter and PSM breakdown to zero."""
    global _tess_call_count, _tess_psm_counts
    _tess_call_count = 0
    _tess_psm_counts = {}


# =============================================================================
# PDF-to-Image rendering
# =============================================================================

def render_page(pdf_path: str, dpi_value: int) -> Image.Image:
    """Render page 0 of *pdf_path* at *dpi_value* DPI and return a PIL RGB image."""
    doc = fitz.open(pdf_path)
    try:
        page = doc.load_page(0)
        zoom = dpi_value / 72.0
        pix = page.get_pixmap(matrix=fitz.Matrix(zoom, zoom), alpha=False)
        return Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
    finally:
        doc.close()


def render_page_from_page(page, dpi_value: int) -> Image.Image:
    """Render an already-loaded fitz *page* at *dpi_value* DPI -> PIL RGB image."""
    zoom = dpi_value / 72.0
    pix = page.get_pixmap(matrix=fitz.Matrix(zoom, zoom), alpha=False)
    return Image.frombytes("RGB", [pix.width, pix.height], pix.samples)


# =============================================================================
# Image preprocessing
# =============================================================================

def preprocess(img: Image.Image, thr: int = 175, invert: bool = False) -> Image.Image:
    """Convert to grayscale, auto-contrast, optional invert, then threshold."""
    img = img.convert("L")
    img = ImageOps.autocontrast(img)
    if invert:
        img = ImageOps.invert(img)
    return img.point(lambda p: 255 if p > thr else 0)


def preprocess_for_text(img: Image.Image, invert: bool = False) -> Image.Image:
    """Convert to grayscale + auto-contrast (no binarisation) for general OCR."""
    img = img.convert("L")
    img = ImageOps.autocontrast(img)
    if invert:
        img = ImageOps.invert(img)
    return img


# =============================================================================
# Tesseract wrappers
# =============================================================================

def ocr_digits_only(img: Image.Image, psm: int = 6) -> str:
    """Run Tesseract in digits-only whitelist mode."""
    global _tess_call_count, _tess_psm_counts
    _tess_call_count += 1
    _k = f"dig_psm{psm}"
    _tess_psm_counts[_k] = _tess_psm_counts.get(_k, 0) + 1
    cfg = (
        f"--oem 3 --psm {psm} "
        "-c tessedit_char_whitelist=0123456789 "
        "-c preserve_interword_spaces=1 "
    )
    return pytesseract.image_to_string(img, config=cfg)


def ocr_text_general(img: Image.Image, psm: int = 6) -> str:
    """Run Tesseract in general (unrestricted) text mode."""
    global _tess_call_count, _tess_psm_counts
    _tess_call_count += 1
    _k = f"txt_psm{psm}"
    _tess_psm_counts[_k] = _tess_psm_counts.get(_k, 0) + 1
    return pytesseract.image_to_string(img, config=f"--oem 3 --psm {psm}")


# =============================================================================
# Scoring / upscaling helpers
# =============================================================================

def digit_score(s: Optional[str]) -> int:
    """Count the number of digit characters in *s*."""
    if not s:
        return 0
    return sum(1 for ch in s if ch.isdigit())


def _upscale(img: Image.Image, factor: int) -> Image.Image:
    """Upscale a PIL image by an integer *factor* using Lanczos resampling."""
    try:
        rs = Image.Resampling.LANCZOS
    except AttributeError:
        # Pillow < 9 compat
        rs = Image.LANCZOS  # type: ignore[attr-defined]
    return img.resize((img.width * factor, img.height * factor), resample=rs)


# =============================================================================
# Table line removal (cv2)
# =============================================================================

def remove_table_lines_image(img: Image.Image) -> Optional[Image.Image]:
    """Remove horizontal and vertical table lines using morphology.

    Returns a cleaned PIL RGB image, or ``None`` when cv2 is unavailable or
    the operation fails.
    """
    if not CV2_AVAILABLE:
        return None
    try:
        gray = cv2.cvtColor(np.array(img.convert("RGB")), cv2.COLOR_RGB2GRAY)
        bw = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)[1]
        hk = cv2.getStructuringElement(cv2.MORPH_RECT, (60, 1))
        vk = cv2.getStructuringElement(cv2.MORPH_RECT, (1, 60))
        lines = cv2.bitwise_or(
            cv2.morphologyEx(bw, cv2.MORPH_OPEN, hk),
            cv2.morphologyEx(bw, cv2.MORPH_OPEN, vk),
        )
        cleaned = cv2.bitwise_not(cv2.bitwise_and(bw, cv2.bitwise_not(lines)))
        return Image.fromarray(cleaned).convert("RGB")
    except Exception:
        return None


# =============================================================================
# Spatial OCR box analysis
# =============================================================================

def extract_candidates_from_ocr_data(img: Image.Image) -> Set[str]:
    """Use Tesseract ``image_to_data`` to find AWB-keyword-adjacent digit groups.

    This mirrors the V2 implementation exactly: it locates label tokens
    (AWB, AIRWAY, FEDEX, TRACK, etc.) in the spatial OCR output, then scans
    nearby tokens (same-line or just-below) for 12-digit candidate numbers.

    Note: this function deliberately imports candidate-extraction helpers from
    :mod:`V3.core.awb_extractor` at call time to avoid circular imports at
    module level.
    """
    out: Set[str] = set()
    global _tess_call_count
    _tess_call_count += 1
    try:
        data = pytesseract.image_to_data(
            img, output_type=Output.DICT, config="--oem 3 --psm 6"
        )
    except Exception:
        return out

    texts = data.get("text", []) or []
    tops = data.get("top", []) or []
    lefts = data.get("left", []) or []
    img_w = int(getattr(img, "width", 0) or 0)
    img_h = int(getattr(img, "height", 0) or 0)
    y_same_line = max(40, int(img_h * 0.015)) if img_h else 40
    y_below = max(120, int(img_h * 0.06)) if img_h else 120
    x_span = max(1400, int(img_w * 0.98)) if img_w else 1400

    AWB_LEN = config.AWB_LEN

    def _norm(txt: str) -> str:
        return re.sub(r"[^A-Z0-9]+", "", (txt or "").upper())

    def _num_norm(txt: str) -> str:
        return re.sub(r"[^0-9]", "", (txt or ""))

    label_idx: list[int] = []
    for i, raw in enumerate(texts):
        n = _norm(raw)
        if not n:
            continue
        if any(
            kw in n
            for kw in (
                "AWB",
                "AIRWAY",
                "AIRWAYBILL",
                "WAYBILL",
                "TRACK",
                "NUMBER",
                "FEDEX",
                "SHIP",
            )
        ):
            label_idx.append(i)
    if not label_idx:
        return out

    # Lazy imports to avoid circular dependency at module level
    from V3.core.awb_extractor import (
        extract_candidates_from_text,
        extract_candidates_near_keywords,
        _norm_digits_12,
    )

    n_tokens = len(texts)
    for i in label_idx:
        try:
            y0, x0 = int(tops[i]), int(lefts[i])
        except Exception:
            continue
        block_tokens: list[str] = []
        for j in range(n_tokens):
            try:
                y, x = int(tops[j]), int(lefts[j])
            except Exception:
                continue
            if abs(y - y0) <= y_same_line or (0 <= (y - y0) <= y_below):
                if abs(x - x0) <= x_span:
                    t = (texts[j] or "").strip()
                    if t:
                        block_tokens.append(t)
        block = " ".join(block_tokens)
        out.update(extract_candidates_from_text(block))
        out.update(extract_candidates_near_keywords(block))
        for tok in block_tokens:
            d = _num_norm(tok)
            if len(d) == AWB_LEN:
                out.add(d)
            d2 = _norm_digits_12(tok)
            if d2:
                out.add(d2)
    return out
