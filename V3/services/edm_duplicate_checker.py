# V3/services/edm_duplicate_checker.py
# EDM duplicate checker service (standalone worker).
#
# Baseline: edm_duplicate_checker-2.py from legacy pipeline.
# V3 upgrades in this module:
# - Gate 1: incoming all pages vs all EDM pages using exact hash.
# - Gate 2: if no exact-hash hit, run bounded smart probes (pHash/text/OCR).
# - Tier 2 full compare: incoming all pages vs EDM p1-10 with hash/pHash/text/OCR.
# - Text-layer duplicate comparison only when both pages have >= 30 chars
#   (configurable via EDM_TEXT_LAYER_MIN_CHARS).
# - Cargo Control Document (CCD) pages are always exempt from duplicate checks.
#   If all incoming pages are CCD, the document passes clean immediately.
# - Uses V3 config paths, audit logging, and EDM runtime toggle semantics.

from __future__ import annotations

import csv
import hashlib
import io
import logging
import os
import re
import shutil
import sys
import time
import uuid
import zipfile
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Optional

try:
    import fitz  # PyMuPDF
except Exception:
    try:
        import pymupdf as fitz  # type: ignore[no-redef]
    except Exception as exc:  # pragma: no cover
        raise RuntimeError(
            "PyMuPDF import failed. Install PyMuPDF and remove conflicting 'fitz' package."
        ) from exc

try:
    import requests
except Exception:  # pragma: no cover
    requests = None  # type: ignore[assignment]

try:
    from rapidfuzz import fuzz as _rf_fuzz
except Exception:  # pragma: no cover
    _rf_fuzz = None

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from V3 import config
from V3.audit.logger import audit_event
from V3.audit.tracker import write_edm_event
from V3.core.file_ops import append_to_awb_logs_excel, file_is_stable
from V3.services.edm_checker import is_edm_enabled

# -- config aliases -------------------------------------------------------------
PROCESSED_FOLDER = config.PROCESSED_DIR
CLEAN_FOLDER = config.CLEAN_DIR
REJECTED_FOLDER = config.REJECTED_DIR
NEEDS_REVIEW_FOLDER = config.NEEDS_REVIEW_DIR
CSV_PATH = config.CSV_PATH
AWB_LOGS_PATH = config.AWB_LOGS_PATH

TOKEN_FILE = config.TOKEN_FILE
TESSERACT_PATH = str(config.TESSERACT_PATH)
OPERATING_COMPANY = config.EDM_OPERATING_COMPANY
METADATA_URL = config.EDM_METADATA_URL
DOWNLOAD_URL = config.EDM_DOWNLOAD_URL

FILE_SETTLE_SECONDS = config.FILE_SETTLE_SECONDS
TEXT_SIMILARITY_THRESHOLD = config.TEXT_SIMILARITY_THRESHOLD
PHASH_THRESHOLD = config.PHASH_THRESHOLD
PAGE_OCR_LIMIT = config.PAGE_OCR_LIMIT
OCR_COMPARE_LIMIT = config.EDM_OCR_COMPARE_LIMIT
REJECT_IF_DUP_PAGES_OVER = config.EDM_REJECT_IF_DUP_PAGES_OVER
REJECT_IF_DUP_RATIO = config.EDM_REJECT_IF_DUP_RATIO
EARLY_FOCUS_MATCH_THRESHOLD = config.EARLY_FOCUS_MATCH_THRESHOLD

TIER1_INCOMING_PAGES = max(1, int(config.EDM_TIER1_INCOMING_PAGES))
TIER1_EDM_PAGE_LIMIT = max(1, int(config.EDM_TIER1_EDM_PAGE_LIMIT))
TIER2_EDM_PAGE_LIMIT = max(1, int(config.EDM_TIER2_EDM_PAGE_LIMIT))
TEXT_LAYER_MIN_CHARS = max(1, int(config.EDM_TEXT_LAYER_MIN_CHARS))
EDM_OCR_WORKERS = max(1, int(getattr(config, "EDM_OCR_WORKERS", 2)))
EDM_OCR_PARALLEL_MIN_TASKS = max(2, int(getattr(config, "EDM_OCR_PARALLEL_MIN_TASKS", 4)))


# -- logging -------------------------------------------------------------------
def _build_logger() -> logging.Logger:
    logger = logging.getLogger("EDMDuplicateChecker")
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    logger.propagate = False

    config.LOG_DIR.mkdir(parents=True, exist_ok=True)

    fmt = logging.Formatter(
        fmt="%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    fh = logging.FileHandler(config.EDM_LOG, encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    return logger


log = _build_logger()


# -- small shared caches -------------------------------------------------------
_AWB_SESSION_CACHE: dict[str, Any] = {
    "awb": None,
    "doc_ids": None,
    "edm_pdf_list": None,
    "edm_fingerprints": None,
    "edm_ocr_cache": None,
}


def _clear_awb_cache(reason: str = "") -> None:
    prev = _AWB_SESSION_CACHE.get("awb")
    if prev:
        if reason:
            log.info("[CACHE] Clearing AWB cache for %s: %s", prev, reason)
        else:
            log.info("[CACHE] Clearing AWB cache for %s", prev)
    _AWB_SESSION_CACHE["awb"] = None
    _AWB_SESSION_CACHE["doc_ids"] = None
    _AWB_SESSION_CACHE["edm_pdf_list"] = None
    _AWB_SESSION_CACHE["edm_fingerprints"] = None
    _AWB_SESSION_CACHE["edm_ocr_cache"] = None


# -- utility helpers -----------------------------------------------------------
def _ms(start_ts: float) -> float:
    return round((time.perf_counter() - start_ts) * 1000, 1)


def _awb_from_processed_filename(filename: str) -> str | None:
    base = os.path.splitext(filename)[0]
    m = re.match(r"^(\d{12})(?:_\d+)?$", base)
    if m:
        return m.group(1)
    m = re.match(r"^(\d{12})", base)
    return m.group(1) if m else None


def _append_to_csv(filename: str) -> None:
    awb = _awb_from_processed_filename(filename) or ""
    CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    new_file = not CSV_PATH.exists()
    try:
        with open(CSV_PATH, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            if new_file:
                w.writerow(["AWB", "SourceFile", "Timestamp"])
            w.writerow([awb, filename, datetime.now().isoformat(timespec="seconds")])
    except Exception as e:
        log.warning("[CSV] Could not write to awb_list.csv: %s", e)


def _file_md5(path: str) -> str | None:
    h = hashlib.md5()
    try:
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(65536), b""):
                h.update(chunk)
    except Exception:
        return None
    return h.hexdigest()


def _safe_move(src_path: str, dest_folder: Path, filename: str) -> str:
    dest_folder.mkdir(parents=True, exist_ok=True)
    dest_path = dest_folder / filename

    if dest_path.exists():
        src_md5 = _file_md5(src_path)
        dst_md5 = _file_md5(str(dest_path))
        if src_md5 and dst_md5 and src_md5 == dst_md5:
            log.warning("Identical content already at destination; removing source: %s", filename)
            try:
                os.remove(src_path)
            except Exception:
                pass
            return str(dest_path)

        base, ext = os.path.splitext(filename)
        counter = 2
        while dest_path.exists():
            dest_path = dest_folder / f"{base}_{counter}{ext}"
            counter += 1
        log.warning("Destination exists (different content); saving as: %s", dest_path.name)

    shutil.move(src_path, str(dest_path))
    return str(dest_path)


def _route_file(filepath: str, folder: Path, filename: str) -> str:
    return _safe_move(filepath, folder, filename)


def _append_awb_logs(awb: str, filename: str, status: str) -> None:
    try:
        append_to_awb_logs_excel(
            awb=awb,
            source_file=filename,
            match_method="EDM-Check",
            status=status,
        )
    except Exception:
        pass


# -- token + EDM api -----------------------------------------------------------
def _normalize_token(raw: Any) -> str | None:
    if raw is None:
        return None
    token = str(raw).strip().strip('"').strip("'")
    if token.lower().startswith("bearer "):
        token = token[7:].strip()
    return token or None


def _read_token_file() -> str | None:
    if not TOKEN_FILE.exists():
        return None
    try:
        raw = TOKEN_FILE.read_text(encoding="utf-8-sig")
    except Exception:
        return None
    return _normalize_token(raw)


def _get_token() -> str | None:
    file_token = _read_token_file()
    if file_token:
        return file_token
    env_token = _normalize_token(config.EDM_TOKEN)
    if env_token and env_token != "paste_your_token_here":
        return env_token
    return None


def _headers(token: str, accept: str = "application/json, text/plain, */*") -> dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": accept,
        "Origin": config.EDM_PORTAL_URL,
        "Referer": config.EDM_PORTAL_URL + "/",
    }


def get_document_ids(awb: str, token: str) -> Optional[list[str]]:
    """Return doc IDs for AWB.

    Returns:
    - list[str] for successful lookup (possibly empty)
    - None when auth/token/network is inconclusive
    """
    if requests is None:
        log.warning("requests package unavailable; EDM call skipped")
        return None

    payload = {
        "documentClass": "SHIPMENT",
        "group": [{"operatingCompany": OPERATING_COMPANY, "trackingNumber": [awb]}],
        "responseTypes": ["metadata"],
    }
    params = {"pageSize": 25, "continuationToken": "", "archiveSelection": "false"}

    try:
        r = requests.post(
            METADATA_URL,
            headers=_headers(token),
            params=params,
            json=payload,
            timeout=30,
        )
    except requests.exceptions.Timeout:
        log.warning("Timeout querying AWB %s; treating as unchecked", awb)
        return None
    except Exception as e:
        log.warning("Error querying AWB %s: %s", awb, e)
        return None

    if r.status_code in (401, 403):
        log.error("EDM token unauthorized/expired")
        return None
    if r.status_code == 404:
        return []
    if r.status_code != 200:
        log.warning(
            "EDM metadata inconclusive for AWB %s (status=%s); treating as unchecked",
            awb,
            r.status_code,
        )
        return None

    doc_ids: list[str] = []
    try:
        payload_json = r.json()
    except Exception:
        payload_json = {}
    for group in payload_json.get("groups", []):
        if not isinstance(group, dict):
            continue
        for doc in group.get("documents", []):
            if not isinstance(doc, dict):
                continue
            doc_id = doc.get("documentId") or doc.get("id")
            if doc_id:
                doc_ids.append(str(doc_id))
    return doc_ids


def _wrap_pdf_in_zip(pdf_bytes: bytes) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("document.pdf", pdf_bytes)
    buf.seek(0)
    return buf.read()


def _zip_has_supported_docs(zip_bytes: bytes) -> bool:
    try:
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
            for name in z.namelist():
                lower = name.lower()
                if lower.endswith(".pdf") or lower.endswith((".tiff", ".tif")):
                    return True
    except Exception as e:
        log.warning("Error inspecting ZIP: %s", e)
    return False


def extract_pdfs_from_zip(zip_bytes: bytes) -> list[bytes]:
    """Extract PDF payloads from EDM ZIP. TIFF files are converted in-memory."""
    pdfs: list[bytes] = []
    try:
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
            for name in z.namelist():
                lower = name.lower()
                if lower.endswith(".pdf"):
                    pdfs.append(z.read(name))
                    continue

                if lower.endswith((".tiff", ".tif")):
                    try:
                        from PIL import Image as PILImage

                        tiff_bytes = z.read(name)
                        tiff_img = PILImage.open(io.BytesIO(tiff_bytes))
                        frames = []
                        try:
                            while True:
                                frames.append(tiff_img.copy().convert("RGB"))
                                tiff_img.seek(tiff_img.tell() + 1)
                        except EOFError:
                            pass

                        if not frames:
                            continue

                        pdf_doc = fitz.open()
                        for frame in frames:
                            frame_buf = io.BytesIO()
                            frame.save(frame_buf, format="PNG")
                            frame_buf.seek(0)
                            img_doc = fitz.open("png", frame_buf.read())
                            pdf_bytes = img_doc.convert_to_pdf()
                            img_doc.close()
                            page_doc = fitz.open("pdf", pdf_bytes)
                            pdf_doc.insert_pdf(page_doc)
                            page_doc.close()

                        pdf_buf = io.BytesIO()
                        pdf_doc.save(pdf_buf)
                        pdf_doc.close()
                        pdfs.append(pdf_buf.getvalue())
                        log.info("Converted TIFF->PDF: %s (%s frame(s))", name, len(frames))
                    except Exception as e:
                        log.warning("Failed converting TIFF %s: %s", name, e)
                        continue
    except Exception as e:
        log.warning("Error extracting ZIP: %s", e)
    return pdfs


def download_edm_zip(doc_ids: list[str], token: str) -> Optional[bytes]:
    if requests is None:
        return None
    if not doc_ids:
        return None

    params = {"documentClass": "SHIPMENT", "archiveSelection": "false"}
    body = {
        "requestId": str(uuid.uuid4()),
        "smallerSizeDocumentId": ",".join(doc_ids),
    }

    try:
        r = requests.post(
            DOWNLOAD_URL,
            headers=_headers(token, accept="application/zip, */*"),
            params=params,
            json=body,
            timeout=60,
        )
    except Exception as e:
        log.warning("Error downloading EDM ZIP: %s", e)
        return None

    if r.status_code in (401, 403):
        return None
    if r.status_code != 200:
        log.warning("EDM download failed; status %s", r.status_code)
        return None

    content_type = (r.headers.get("Content-Type") or "").lower()
    if "zip" in content_type:
        if _zip_has_supported_docs(r.content):
            return r.content
        log.warning("ZIP had no supported docs; retrying individually")
        return download_edm_individually(doc_ids, token)

    if "pdf" in content_type:
        return _wrap_pdf_in_zip(r.content)

    log.warning("Unexpected EDM download content-type: %s", content_type)
    return None


def download_edm_individually(doc_ids: list[str], token: str) -> Optional[bytes]:
    if requests is None:
        return None

    zip_buffer = io.BytesIO()
    found = 0
    with zipfile.ZipFile(zip_buffer, "w") as z:
        for doc_id in doc_ids:
            params = {"documentClass": "SHIPMENT", "archiveSelection": "false"}
            body = {
                "requestId": str(uuid.uuid4()),
                "smallerSizeDocumentId": doc_id,
            }
            try:
                r = requests.post(
                    DOWNLOAD_URL,
                    headers=_headers(token, accept="application/zip, */*"),
                    params=params,
                    json=body,
                    timeout=60,
                )
                if r.status_code != 200:
                    continue

                ct = (r.headers.get("Content-Type") or "").lower()
                if "pdf" in ct:
                    z.writestr(f"{doc_id}.pdf", r.content)
                    found += 1
                elif "zip" in ct:
                    for j, pdf in enumerate(extract_pdfs_from_zip(r.content)):
                        z.writestr(f"{doc_id}_{j}.pdf", pdf)
                        found += 1
            except Exception as e:
                log.warning("Error downloading doc %s: %s", doc_id, e)

    if found == 0:
        return None

    zip_buffer.seek(0)
    return zip_buffer.read()


def build_edm_fingerprints(
    edm_pdf_list: list[bytes],
    hash_page_limit: Optional[int] = None,
    phash_page_limit: Optional[int] = None,
    text_page_limit: Optional[int] = None,
) -> list[dict[str, Any]]:
    """Precompute EDM features.

    Defaults preserve backward compatibility (windowed precompute). Callers may
    pass `hash_page_limit=-1` for all-page hash precompute and pass 0 for
    phash/text to keep startup light and let getters compute lazily.
    """

    out: list[dict[str, Any]] = []
    for edm_bytes in edm_pdf_list:
        doc = None
        fp: dict[str, Any] = {
            "valid": False,
            "page_count": 0,
            "hashes": [],
            "phashes": [],
            "texts": [],
        }
        try:
            doc = fitz.open(stream=edm_bytes, filetype="pdf")
            page_count = len(doc)
            fp["valid"] = True
            fp["page_count"] = page_count

            # Backward-compatible defaults when limits are omitted.
            max_hash_pages = max(TIER1_EDM_PAGE_LIMIT, TIER2_EDM_PAGE_LIMIT)
            max_phash_pages = min(max_hash_pages, PAGE_OCR_LIMIT)
            max_text_pages = min(max_hash_pages, max(PAGE_OCR_LIMIT, OCR_COMPARE_LIMIT))

            if hash_page_limit is None:
                hash_lim = min(page_count, max_hash_pages)
            elif hash_page_limit < 0:
                hash_lim = page_count
            elif hash_page_limit <= 0:
                hash_lim = 0
            else:
                hash_lim = min(page_count, int(hash_page_limit))

            if phash_page_limit is None:
                phash_lim = min(page_count, max_phash_pages)
            elif phash_page_limit <= 0:
                phash_lim = 0
            else:
                phash_lim = min(page_count, int(phash_page_limit))

            if text_page_limit is None:
                text_lim = min(page_count, max_text_pages)
            elif text_page_limit <= 0:
                text_lim = 0
            else:
                text_lim = min(page_count, int(text_page_limit))

            hashes: list[str] = []
            phashes: list[Any] = []
            texts: list[str] = []

            max_loop = max(hash_lim, phash_lim, text_lim)
            for ei in range(max_loop):
                page = doc[ei]
                if ei < hash_lim:
                    hashes.append(hash_page(page))

                if ei < phash_lim:
                    phashes.append(perceptual_hash_page(page))

                if ei < text_lim:
                    texts.append(extract_embedded_text_only(page, top_percent=100))

            fp["hashes"] = hashes
            fp["phashes"] = phashes
            fp["texts"] = texts
        except Exception as e:
            log.warning("Could not fingerprint EDM doc: %s", e)
        finally:
            if doc is not None:
                try:
                    doc.close()
                except Exception:
                    pass
        out.append(fp)

    return out


# -- page feature helpers ------------------------------------------------------
def hash_page(page: Any) -> str:
    pix = page.get_pixmap(dpi=100)
    return hashlib.md5(pix.tobytes()).hexdigest()


def perceptual_hash_page(page: Any):
    try:
        import imagehash
        from PIL import Image, ImageOps

        pix = page.get_pixmap(dpi=150)
        img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
        img = ImageOps.grayscale(img)
        img = ImageOps.autocontrast(img)
        return imagehash.phash(img)
    except Exception as e:
        log.warning("Error computing pHash: %s", e)
        return None


def _normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip().lower())


def extract_embedded_text_only(page: Any, top_percent: int = 100) -> str:
    try:
        rect = page.rect
        clip = fitz.Rect(rect.x0, rect.y0, rect.x1, rect.y0 + rect.height * top_percent / 100)
        return _normalize_text(page.get_text("text", clip=clip) or "")
    except Exception as e:
        log.warning("Error extracting embedded text: %s", e)
        return ""


def _preprocess_image_for_ocr(img: Any):
    import cv2
    import numpy as np
    from PIL import Image as PILImage

    arr = np.array(img)
    gray = cv2.cvtColor(arr, cv2.COLOR_RGB2GRAY)
    denoised = cv2.fastNlMeansDenoising(gray, h=10)
    clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8))
    enhanced = clahe.apply(denoised)
    thresh = cv2.threshold(enhanced, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)[1]

    coords = np.column_stack(np.where(thresh > 0))
    if len(coords) > 0:
        angle = cv2.minAreaRect(coords)[-1]
        if angle < -45:
            angle = 90 + angle
        if abs(angle) > 0.5:
            h2, w2 = thresh.shape
            center = (w2 // 2, h2 // 2)
            mat = cv2.getRotationMatrix2D(center, angle, 1.0)
            thresh = cv2.warpAffine(
                thresh,
                mat,
                (w2, h2),
                flags=cv2.INTER_CUBIC,
                borderMode=cv2.BORDER_REPLICATE,
            )

    return PILImage.fromarray(thresh)


def extract_ocr_text(page: Any, top_percent: int = 100) -> str:
    try:
        import pytesseract
        from PIL import Image

        pytesseract.pytesseract.tesseract_cmd = TESSERACT_PATH

        pix = page.get_pixmap(dpi=220)
        img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
        crop_h = int(img.height * top_percent / 100)
        cropped = img.crop((0, 0, img.width, crop_h))

        try:
            processed = _preprocess_image_for_ocr(cropped)
        except Exception:
            processed = cropped

        text = pytesseract.image_to_string(processed, config="--psm 6")
        return _normalize_text(text)
    except Exception as e:
        log.warning("Error during OCR: %s", e)
        return ""


def _extract_ocr_from_pdf_path_pages(
    pdf_path: str, page_indices: list[int], top_percent: int = 100
) -> dict[int, str]:
    """OCR selected pages from a file path in one isolated doc-open."""
    out: dict[int, str] = {}
    doc = None
    try:
        doc = fitz.open(pdf_path)
        for idx in page_indices:
            if idx < 0 or idx >= len(doc):
                continue
            out[idx] = extract_ocr_text(doc[idx], top_percent=top_percent)
    except Exception as e:
        log.warning("Error prewarming OCR from file path: %s", e)
    finally:
        if doc is not None:
            try:
                doc.close()
            except Exception:
                pass
    return out


def _extract_ocr_from_pdf_bytes_pages(
    pdf_bytes: bytes, page_indices: list[int], top_percent: int = 100
) -> dict[int, str]:
    """OCR selected pages from in-memory PDF bytes in one isolated doc-open."""
    out: dict[int, str] = {}
    doc = None
    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        for idx in page_indices:
            if idx < 0 or idx >= len(doc):
                continue
            out[idx] = extract_ocr_text(doc[idx], top_percent=top_percent)
    except Exception as e:
        log.warning("Error prewarming OCR from PDF bytes: %s", e)
    finally:
        if doc is not None:
            try:
                doc.close()
            except Exception:
                pass
    return out


def text_similarity(text1: str, text2: str) -> float:
    text1 = _normalize_text(text1)
    text2 = _normalize_text(text2)
    if not text1 or not text2:
        return 0.0

    if _rf_fuzz is not None:
        scores = [
            _rf_fuzz.ratio(text1, text2),
            _rf_fuzz.partial_ratio(text1, text2),
            _rf_fuzz.token_sort_ratio(text1, text2),
            _rf_fuzz.token_set_ratio(text1, text2),
        ]
        return float(max(scores))

    # Fallback when rapidfuzz is unavailable.
    return SequenceMatcher(None, text1, text2).ratio() * 100.0


def page_is_cargo_control_document(page: Any) -> bool:
    """CCD pages are always exempt from duplicate checks.

    A page qualifies when the CCD title marker is present:
    - "CARGO CONTROL DOCUMENT" (or French equivalent)

    The 400-pattern is logged as supplemental signal only.
    """
    try:
        text = page.get_text("text") or ""
        if not text.strip():
            text = extract_ocr_text(page, top_percent=100)

        text_upper = text.upper()
        has_ccd = (
            "CARGO CONTROL DOCUMENT" in text_upper
            or "FEUILLE DE RECAPITULATION" in text_upper
        )
        has_400 = bool(re.search(r"400[\s\-]?\d{10,12}", text_upper))

        # CCD pages are always exempt, even when the 400-line is stamped/unclear.
        if has_ccd:
            return True
        if has_400:
            log.info(
                "    CCD marker missing but 400-pattern present -- has_ccd=%s has_400=%s",
                has_ccd,
                has_400,
            )
        return False
    except Exception as e:
        log.warning("Error checking CCD status on page: %s", e)
        return False


def _rejection_confidence(method_counts: dict[str, int]) -> str:
    h = int(method_counts.get("HASH", 0))
    p = int(method_counts.get("PHASH", 0))
    t = int(method_counts.get("TEXT", 0))
    o = int(method_counts.get("OCR", 0))

    if h >= 1:
        return "HIGH"
    if p >= 2 and (t + o) >= 1:
        return "HIGH"
    if p >= 3:
        return "MEDIUM"
    if (t + o) >= 3 and p >= 1:
        return "MEDIUM"
    return "LOW"


# -- duplicate comparison ------------------------------------------------------
def find_duplicate_pages(
    incoming_path: str,
    edm_pdf_list: list[bytes],
    edm_fingerprints: Optional[list[dict[str, Any]]] = None,
    edm_ocr_cache: Optional[dict[tuple[int, int], str]] = None,
) -> tuple[set[int], dict[str, Any]]:
    """Return duplicate incoming page indexes and comparison metadata."""
    duplicate_pages: set[int] = set()
    page_details: dict[int, dict[str, Any]] = {}
    method_counts: Counter[str] = Counter()

    tier1_hit = False
    tier1_hit_events: list[str] = []

    incoming_doc = None
    edm_docs: list[Any | None] = [None] * len(edm_pdf_list)  # OCR-only lazy open.
    edm_hash_index_all: dict[str, list[tuple[int, int]]] = {}
    edm_hash_index_tier2: dict[str, list[tuple[int, int]]] = {}

    inc_hash: dict[int, str] = {}
    inc_phash: dict[int, Any] = {}
    inc_text: dict[int, str] = {}
    inc_ocr: dict[int, str] = {}
    inc_ccd: dict[int, bool] = {}

    focused_edm_idx: int | None = None
    edm_match_counts: list[int] = [0] * len(edm_pdf_list)
    tier1_candidate_docs: set[int] = set()
    # Gate signal scores: doc_idx → cumulative probe hits (used to prioritise Tier 2).
    # Gate 1 hash hit = 100, Gate 2 pHash = 10, text = 5, OCR = 3.
    gate2_doc_scores: dict[int, int] = {}

    if edm_fingerprints is None:
        edm_fingerprints = build_edm_fingerprints(
            edm_pdf_list,
            hash_page_limit=-1,  # all EDM pages for exact-hash gate
            phash_page_limit=0,
            text_page_limit=0,
        )
    if edm_ocr_cache is None:
        edm_ocr_cache = {}

    def ensure_edm_doc_open(doc_idx: int) -> Any | None:
        if edm_docs[doc_idx] is not None:
            return edm_docs[doc_idx]
        try:
            edm_docs[doc_idx] = fitz.open(stream=edm_pdf_list[doc_idx], filetype="pdf")
        except Exception as e:
            log.warning("Could not open EDM doc %s for OCR: %s", doc_idx + 1, e)
            edm_docs[doc_idx] = None
        return edm_docs[doc_idx]

    def doc_valid(doc_idx: int) -> bool:
        fp = edm_fingerprints[doc_idx] if doc_idx < len(edm_fingerprints) else {}
        return bool(fp.get("valid"))

    def doc_page_count(doc_idx: int) -> int:
        fp = edm_fingerprints[doc_idx] if doc_idx < len(edm_fingerprints) else {}
        try:
            return int(fp.get("page_count", 0))
        except Exception:
            return 0

    def mark_duplicate(ii: int, doc_idx: int, method: str, detail: str) -> None:
        nonlocal focused_edm_idx
        if ii in duplicate_pages:
            return

        duplicate_pages.add(ii)
        page_details[ii + 1] = {
            "method": method,
            "detail": detail,
            "edm_doc": doc_idx + 1,
        }
        method_counts[method] += 1

        edm_match_counts[doc_idx] += 1
        if focused_edm_idx is None and edm_match_counts[doc_idx] >= EARLY_FOCUS_MATCH_THRESHOLD:
            focused_edm_idx = doc_idx
            log.info(
                "    EDM %s: %s page matches reached focus threshold; concentrating checks",
                doc_idx + 1,
                edm_match_counts[doc_idx],
            )

    def should_check_doc(doc_idx: int) -> bool:
        if doc_idx not in tier1_candidate_docs:
            return False
        # Keep stacked-doc safety: once a doc is a candidate, continue checking it.
        return True

    try:
        incoming_doc = fitz.open(incoming_path)
        if len(incoming_doc) == 0:
            return duplicate_pages, {
                "tier1_hit": False,
                "tier1_hit_events": [],
                "all_ccd": False,
                "method_counts": {},
                "page_details": {},
                "decision_trace": "empty_input_doc",
            }

        total_incoming = len(incoming_doc)
        inc_pages = [incoming_doc[p] for p in range(total_incoming)]

        def inc_is_ccd(ii: int) -> bool:
            if ii not in inc_ccd:
                inc_ccd[ii] = page_is_cargo_control_document(inc_pages[ii])
            return inc_ccd[ii]

        def get_inc_hash(ii: int) -> str:
            if ii not in inc_hash:
                inc_hash[ii] = hash_page(inc_pages[ii])
            return inc_hash[ii]

        def get_inc_phash(ii: int):
            if ii not in inc_phash:
                inc_phash[ii] = perceptual_hash_page(inc_pages[ii])
            return inc_phash[ii]

        def get_inc_text(ii: int) -> str:
            if ii not in inc_text:
                inc_text[ii] = extract_embedded_text_only(inc_pages[ii], top_percent=100)
            return inc_text[ii]

        def get_inc_ocr(ii: int) -> str:
            if ii not in inc_ocr:
                inc_ocr[ii] = extract_ocr_text(inc_pages[ii], top_percent=100)
            return inc_ocr[ii]

        def get_edm_hash(doc_idx: int, ei: int) -> str:
            fp = edm_fingerprints[doc_idx] if doc_idx < len(edm_fingerprints) else {}
            hashes = fp.setdefault("hashes", [])
            if ei < len(hashes):
                return str(hashes[ei])
            doc = ensure_edm_doc_open(doc_idx)
            if doc is None or ei >= len(doc):
                return ""
            val = hash_page(doc[ei])
            while len(hashes) <= ei:
                hashes.append("")
            hashes[ei] = val
            return val

        def get_edm_phash(doc_idx: int, ei: int):
            fp = edm_fingerprints[doc_idx] if doc_idx < len(edm_fingerprints) else {}
            phashes = fp.setdefault("phashes", [])
            if ei < len(phashes):
                return phashes[ei]
            doc = ensure_edm_doc_open(doc_idx)
            if doc is None or ei >= len(doc):
                return None
            val = perceptual_hash_page(doc[ei])
            while len(phashes) <= ei:
                phashes.append(None)
            phashes[ei] = val
            return val

        def get_edm_text(doc_idx: int, ei: int) -> str:
            fp = edm_fingerprints[doc_idx] if doc_idx < len(edm_fingerprints) else {}
            texts = fp.setdefault("texts", [])
            if ei < len(texts):
                return str(texts[ei] or "")
            doc = ensure_edm_doc_open(doc_idx)
            if doc is None or ei >= len(doc):
                return ""
            val = extract_embedded_text_only(doc[ei], top_percent=100)
            while len(texts) <= ei:
                texts.append("")
            texts[ei] = val
            return val

        def get_edm_ocr(doc_idx: int, ei: int) -> str:
            key = (doc_idx, ei)
            if key in edm_ocr_cache:
                return edm_ocr_cache[key]
            doc = ensure_edm_doc_open(doc_idx)
            if doc is None or ei >= len(doc):
                edm_ocr_cache[key] = ""
                return ""
            edm_ocr_cache[key] = extract_ocr_text(doc[ei], top_percent=100)
            return edm_ocr_cache[key]

        def prewarm_tier2_ocr_parallel() -> None:
            """Bounded OCR prewarm for Tier2 OCR stage.

            This keeps correctness unchanged: it only fills caches early.
            """
            if EDM_OCR_WORKERS <= 1:
                return

            pending_inc: list[int] = []
            for ii in range(total_incoming):
                if ii in duplicate_pages or inc_is_ccd(ii):
                    continue
                if ii >= OCR_COMPARE_LIMIT:
                    continue
                if len(get_inc_text(ii)) >= TEXT_LAYER_MIN_CHARS:
                    continue
                if ii in inc_ocr:
                    continue
                pending_inc.append(ii)

            pending_edm_by_doc: dict[int, list[int]] = {}
            for doc_idx in range(len(edm_pdf_list)):
                if not doc_valid(doc_idx) or not should_check_doc(doc_idx):
                    continue
                limit = min(doc_page_count(doc_idx), TIER2_EDM_PAGE_LIMIT, OCR_COMPARE_LIMIT)
                for ei in range(limit):
                    key = (doc_idx, ei)
                    if key in edm_ocr_cache:
                        continue
                    if len(get_edm_text(doc_idx, ei)) >= TEXT_LAYER_MIN_CHARS:
                        continue
                    pending_edm_by_doc.setdefault(doc_idx, []).append(ei)

            task_count = (1 if pending_inc else 0) + len(pending_edm_by_doc)
            if task_count < EDM_OCR_PARALLEL_MIN_TASKS:
                return

            max_workers = min(EDM_OCR_WORKERS, task_count)
            if max_workers <= 1:
                return

            futures: dict[Any, tuple[str, Any]] = {}
            with ThreadPoolExecutor(max_workers=max_workers) as ex:
                if pending_inc:
                    futures[
                        ex.submit(
                            _extract_ocr_from_pdf_path_pages,
                            incoming_path,
                            pending_inc,
                            100,
                        )
                    ] = ("inc", None)

                for doc_idx, page_indices in pending_edm_by_doc.items():
                    futures[
                        ex.submit(
                            _extract_ocr_from_pdf_bytes_pages,
                            edm_pdf_list[doc_idx],
                            page_indices,
                            100,
                        )
                    ] = ("edm", doc_idx)

                for fut in as_completed(futures):
                    kind, marker = futures[fut]
                    try:
                        results = fut.result() or {}
                    except Exception:
                        continue
                    if kind == "inc":
                        for ii, txt in results.items():
                            inc_ocr[int(ii)] = str(txt or "")
                    else:
                        doc_idx = int(marker)
                        for ei, txt in results.items():
                            edm_ocr_cache[(doc_idx, int(ei))] = str(txt or "")

        def build_hash_indexes() -> None:
            """Build AWB-local hash indexes for fast exact-match lookups.

            - edm_hash_index_all: all EDM pages (used by Gate 1)
            - edm_hash_index_tier2: EDM p1-10 window (used by Tier2 hash stage)
            """
            if edm_hash_index_all:
                return

            for doc_idx in range(len(edm_pdf_list)):
                if not doc_valid(doc_idx):
                    continue

                page_count = doc_page_count(doc_idx)
                for ei in range(page_count):
                    h = get_edm_hash(doc_idx, ei)
                    if not h:
                        continue

                    edm_hash_index_all.setdefault(h, []).append((doc_idx, ei))
                    if ei < TIER2_EDM_PAGE_LIMIT:
                        edm_hash_index_tier2.setdefault(h, []).append((doc_idx, ei))

        # CCD all-pages bypass: avoids false rejects for stamped CCD docs.
        all_ccd = True
        for ii in range(total_incoming):
            if not inc_is_ccd(ii):
                all_ccd = False
                break
        if all_ccd:
            return duplicate_pages, {
                "tier1_hit": False,
                "tier1_hit_events": ["all_incoming_pages_ccd"],
                "all_ccd": True,
                "method_counts": {},
                "page_details": {},
                "decision_trace": "all_ccd_bypass",
            }

        # Build exact-hash indexes once for this compare call.
        build_hash_indexes()

        # ---------------------------------------------------------------------
        # Gate 1: exact hash across all incoming pages vs all EDM pages.
        # ---------------------------------------------------------------------
        hash_gate_hit = False
        for ii in range(total_incoming):
            if inc_is_ccd(ii):
                continue
            ih = get_inc_hash(ii)
            matches = edm_hash_index_all.get(ih, [])
            if matches:
                hash_gate_hit = True
                tier1_hit = True
                for doc_idx, _ in matches:
                    tier1_candidate_docs.add(doc_idx)
                    gate2_doc_scores[doc_idx] = gate2_doc_scores.get(doc_idx, 0) + 100
                mdoc, mei = matches[0]
                tier1_hit_events.append(f"HASH-GATE inc_p{ii+1} edm{mdoc+1}_p{mei+1}")
            if hash_gate_hit:
                break

        # ---------------------------------------------------------------------
        # Gate 2: bounded smart probes only when exact hash has no hit.
        # ---------------------------------------------------------------------
        probe_ran = False
        probe_indices: list[int] = []
        if not hash_gate_hit:
            # Smart sampling for stacked docs:
            # first 3 pages + 1/3 + middle + 2/3 + last (unique/in-order).
            for idx in range(min(total_incoming, TIER1_INCOMING_PAGES)):
                if idx not in probe_indices:
                    probe_indices.append(idx)
            extra_candidates = [
                total_incoming // 3,
                total_incoming // 2,
                (2 * total_incoming) // 3,
                max(0, total_incoming - 1),
            ]
            for idx in extra_candidates:
                if 0 <= idx < total_incoming and idx not in probe_indices:
                    probe_indices.append(idx)

            # Track which probe pages already have pHash or text signal.
            # Probe C (OCR) is skipped only for pages that already have cheaper signal —
            # not suppressed globally when other probe pages happened to hit.
            gate2_page_hits: set[int] = set()

            for ii in probe_indices:
                if inc_is_ccd(ii):
                    continue
                probe_ran = True

                # Probe A: pHash — run for every probe page, collect per-doc scores.
                # No early-exit: accumulate all signal before deciding on Tier 2 order.
                iph = get_inc_phash(ii) if ii < PAGE_OCR_LIMIT else None
                if iph is not None:
                    for doc_idx in range(len(edm_pdf_list)):
                        if not doc_valid(doc_idx):
                            continue
                        limit = min(doc_page_count(doc_idx), TIER2_EDM_PAGE_LIMIT, PAGE_OCR_LIMIT)
                        for ei in range(limit):
                            eph = get_edm_phash(doc_idx, ei)
                            if eph is None:
                                continue
                            diff = iph - eph
                            if diff <= PHASH_THRESHOLD:
                                tier1_candidate_docs.add(doc_idx)
                                gate2_doc_scores[doc_idx] = gate2_doc_scores.get(doc_idx, 0) + 10
                                gate2_page_hits.add(ii)
                                tier1_hit_events.append(
                                    f"PROBE-PHASH(diff={diff}) inc_p{ii+1} edm{doc_idx+1}_p{ei+1}"
                                )
                                break  # one hit per doc per probe page is enough

                # Probe B: embedded text — run for every probe page, collect per-doc scores.
                in_text = get_inc_text(ii)
                if len(in_text) >= TEXT_LAYER_MIN_CHARS:
                    for doc_idx in range(len(edm_pdf_list)):
                        if not doc_valid(doc_idx):
                            continue
                        limit = min(doc_page_count(doc_idx), TIER2_EDM_PAGE_LIMIT, PAGE_OCR_LIMIT)
                        for ei in range(limit):
                            ed_text = get_edm_text(doc_idx, ei)
                            if len(ed_text) < TEXT_LAYER_MIN_CHARS:
                                continue
                            score = text_similarity(in_text, ed_text)
                            if score >= TEXT_SIMILARITY_THRESHOLD:
                                tier1_candidate_docs.add(doc_idx)
                                gate2_doc_scores[doc_idx] = gate2_doc_scores.get(doc_idx, 0) + 5
                                gate2_page_hits.add(ii)
                                tier1_hit_events.append(
                                    f"PROBE-TEXT(score={score:.1f}) inc_p{ii+1} edm{doc_idx+1}_p{ei+1}"
                                )
                                break  # one hit per doc per probe page is enough

                # Probe C: OCR fallback — skip only for pages that already have pHash/text signal.
                # Runs for scan-only pages even when other probe pages hit, closing the gap
                # where an incoming scan has a text-layer duplicate on EDM.
                if ii < OCR_COMPARE_LIMIT and ii not in gate2_page_hits:
                    inc_has_text_layer = len(in_text) >= TEXT_LAYER_MIN_CHARS
                    in_cmp = in_text if inc_has_text_layer else get_inc_ocr(ii)
                    if in_cmp:
                        for doc_idx in range(len(edm_pdf_list)):
                            if not doc_valid(doc_idx):
                                continue
                            limit = min(
                                doc_page_count(doc_idx),
                                TIER2_EDM_PAGE_LIMIT,
                                OCR_COMPARE_LIMIT,
                            )
                            for ei in range(limit):
                                ed_text = get_edm_text(doc_idx, ei)
                                ed_has_text_layer = len(ed_text) >= TEXT_LAYER_MIN_CHARS
                                if inc_has_text_layer and ed_has_text_layer:
                                    continue
                                ed_cmp = ed_text if ed_has_text_layer else get_edm_ocr(doc_idx, ei)
                                if not ed_cmp:
                                    continue
                                score = text_similarity(in_cmp, ed_cmp)
                                if score >= TEXT_SIMILARITY_THRESHOLD:
                                    tier1_candidate_docs.add(doc_idx)
                                    gate2_doc_scores[doc_idx] = gate2_doc_scores.get(doc_idx, 0) + 3
                                    tier1_hit_events.append(
                                        f"PROBE-OCR(score={score:.1f}) inc_p{ii+1} edm{doc_idx+1}_p{ei+1}"
                                    )
                                    break  # one hit per doc per probe page is enough

            tier1_hit = bool(gate2_doc_scores)

        if not tier1_hit:
            return duplicate_pages, {
                "tier1_hit": False,
                "tier1_hit_events": [],
                "all_ccd": False,
                "method_counts": {},
                "page_details": {},
                "decision_trace": (
                    "hash_gate_no_hit_fast_clean"
                    + (f";probe_indices={[i + 1 for i in probe_indices]}" if probe_indices else "")
                    + (";probe_ran=True" if probe_ran else ";probe_ran=False")
                ),
            }

        # Any gate/probe hit means proceed to full Tier2 checks.
        # Sort docs by descending gate signal (highest score first) so that the
        # most likely duplicate is confirmed earliest, letting subsequent pages
        # short-circuit via the `ii in duplicate_pages` guards.
        _tier2_doc_order: list[int] = sorted(
            (i for i in range(len(edm_pdf_list)) if doc_valid(i)),
            key=lambda d: (-gate2_doc_scores.get(d, 0), d),
        )
        tier1_candidate_docs = set(_tier2_doc_order)

        # ---------------------------------------------------------------------
        # Tier 2: full compare against EDM p1-10 for Tier1 candidate docs only.
        # ---------------------------------------------------------------------
        # 2a) Exact hash
        for ii in range(total_incoming):
            if ii in duplicate_pages:
                continue
            if inc_is_ccd(ii):
                log.info("    Page %s: CCD detected, exempt", ii + 1)
                continue

            ih = get_inc_hash(ii)
            for doc_idx, ei in edm_hash_index_tier2.get(ih, []):
                if not should_check_doc(doc_idx):
                    continue
                log.info(
                    "    EDM %s: DUPLICATE HASH incoming p%s vs EDM p%s",
                    doc_idx + 1,
                    ii + 1,
                    ei + 1,
                )
                mark_duplicate(ii, doc_idx, "HASH", f"edm_p{ei+1}")
                if ii in duplicate_pages:
                    break

        # 2b) pHash
        for ii in range(total_incoming):
            if ii in duplicate_pages or inc_is_ccd(ii):
                continue
            if ii >= PAGE_OCR_LIMIT:
                continue

            iph = get_inc_phash(ii)
            if iph is None:
                continue

            for doc_idx in _tier2_doc_order:
                if not should_check_doc(doc_idx):
                    continue

                limit = min(doc_page_count(doc_idx), TIER2_EDM_PAGE_LIMIT, PAGE_OCR_LIMIT)
                for ei in range(limit):
                    eph = get_edm_phash(doc_idx, ei)
                    if eph is None:
                        continue
                    diff = iph - eph
                    if diff <= PHASH_THRESHOLD:
                        log.info(
                            "    EDM %s: DUPLICATE PHASH incoming p%s vs EDM p%s (diff=%s)",
                            doc_idx + 1,
                            ii + 1,
                            ei + 1,
                            diff,
                        )
                        mark_duplicate(ii, doc_idx, "PHASH", f"diff={diff}")
                        break
                if ii in duplicate_pages:
                    break

        # 2c) Text-layer compare only when both sides have >= TEXT_LAYER_MIN_CHARS
        for ii in range(total_incoming):
            if ii in duplicate_pages or inc_is_ccd(ii):
                continue
            if ii >= PAGE_OCR_LIMIT:
                continue

            in_text = get_inc_text(ii)
            if len(in_text) < TEXT_LAYER_MIN_CHARS:
                continue

            for doc_idx in _tier2_doc_order:
                if not should_check_doc(doc_idx):
                    continue

                limit = min(doc_page_count(doc_idx), TIER2_EDM_PAGE_LIMIT, PAGE_OCR_LIMIT)
                for ei in range(limit):
                    ed_text = get_edm_text(doc_idx, ei)
                    if len(ed_text) < TEXT_LAYER_MIN_CHARS:
                        continue

                    score = text_similarity(in_text, ed_text)
                    if score >= TEXT_SIMILARITY_THRESHOLD:
                        log.info(
                            "    EDM %s: DUPLICATE TEXT incoming p%s vs EDM p%s (score=%.1f)",
                            doc_idx + 1,
                            ii + 1,
                            ei + 1,
                            score,
                        )
                        mark_duplicate(ii, doc_idx, "TEXT", f"score={score:.1f}")
                        break
                if ii in duplicate_pages:
                    break

        # 2d) OCR fallback.
        # Optimization: skip text-vs-text pairs already covered in 2c.
        prewarm_tier2_ocr_parallel()
        for ii in range(total_incoming):
            if ii in duplicate_pages or inc_is_ccd(ii):
                continue
            if ii >= OCR_COMPARE_LIMIT:
                continue

            in_text = get_inc_text(ii)
            inc_has_text_layer = len(in_text) >= TEXT_LAYER_MIN_CHARS
            in_cmp = in_text if inc_has_text_layer else get_inc_ocr(ii)
            if not in_cmp:
                continue

            for doc_idx in _tier2_doc_order:
                if not should_check_doc(doc_idx):
                    continue

                limit = min(doc_page_count(doc_idx), TIER2_EDM_PAGE_LIMIT, OCR_COMPARE_LIMIT)
                for ei in range(limit):
                    ed_text = get_edm_text(doc_idx, ei)
                    ed_has_text_layer = len(ed_text) >= TEXT_LAYER_MIN_CHARS

                    # Already handled in text-layer stage.
                    if inc_has_text_layer and ed_has_text_layer:
                        continue

                    ed_cmp = ed_text if ed_has_text_layer else get_edm_ocr(doc_idx, ei)
                    if not ed_cmp:
                        continue

                    score = text_similarity(in_cmp, ed_cmp)
                    if score >= TEXT_SIMILARITY_THRESHOLD:
                        log.info(
                            "    EDM %s: DUPLICATE OCR incoming p%s vs EDM p%s (score=%.1f)",
                            doc_idx + 1,
                            ii + 1,
                            ei + 1,
                            score,
                        )
                        mark_duplicate(ii, doc_idx, "OCR", f"score={score:.1f}")
                        break
                if ii in duplicate_pages:
                    break

        methods_order = []
        seen = set()
        for page_no in sorted(page_details):
            method = str(page_details[page_no].get("method", ""))
            if method and method not in seen:
                seen.add(method)
                methods_order.append(method)

        trace = (
            f"tier1_hit={tier1_hit};"
            f"tier1_candidate_docs={[d + 1 for d in sorted(tier1_candidate_docs)]};"
            f"tier1_events={tier1_hit_events[:12]};"
            f"focused_edm={focused_edm_idx + 1 if focused_edm_idx is not None else None};"
            f"methods={methods_order};"
            f"method_counts={dict(method_counts)}"
        )

        return duplicate_pages, {
            "tier1_hit": True,
            "tier1_hit_events": tier1_hit_events,
            "all_ccd": False,
            "method_counts": dict(method_counts),
            "page_details": page_details,
            "decision_trace": trace,
        }

    except Exception as e:
        log.warning("Error during duplicate check: %s", e)
        return duplicate_pages, {
            "tier1_hit": tier1_hit,
            "tier1_hit_events": tier1_hit_events,
            "all_ccd": False,
            "method_counts": dict(method_counts),
            "page_details": page_details,
            "decision_trace": f"compare_exception={e}",
        }
    finally:
        try:
            if incoming_doc is not None:
                incoming_doc.close()
        except Exception:
            pass

        for d in edm_docs:
            if d is not None:
                try:
                    d.close()
                except Exception:
                    pass


# -- routing -------------------------------------------------------------------
def _record_outcome(
    awb: str,
    filename: str,
    result: str,
    reason: str,
    match_stats: str,
    total_pages: int | None,
    dup_count: int | None,
    dup_ratio: float | None,
    compare_method: str,
    edm_secs: float,
    cache_state: str,
    route: str,
) -> None:
    try:
        write_edm_event(
            awb=awb,
            filename=filename,
            edm_result=result,
            dup_page_count=dup_count,
            total_pages=total_pages,
            dup_ratio=dup_ratio,
            edm_secs=edm_secs,
            compare_method=compare_method,
            notes=reason,
        )
    except Exception:
        pass

    audit_event(
        "EDM_CHECK",
        file=filename,
        awb=awb,
        status=result,
        route=route,
        reason=reason,
        match_stats=match_stats,
        timings_ms={"total_active": round(edm_secs * 1000.0, 1)},
        cache=cache_state,
    )

    _append_awb_logs(awb=awb, filename=filename, status=result)


def _process_partial_split(
    filepath: str,
    filename: str,
    duplicate_pages: set[int],
    total_pages: int,
) -> tuple[bool, str]:
    clean_pages = [i for i in range(total_pages) if i not in duplicate_pages]

    src_doc = fitz.open(filepath)

    clean_doc = fitz.open()
    for p in clean_pages:
        clean_doc.insert_pdf(src_doc, from_page=p, to_page=p)

    rejected_doc = fitz.open()
    for p in sorted(duplicate_pages):
        rejected_doc.insert_pdf(src_doc, from_page=p, to_page=p)

    src_doc.close()

    tmp_clean = filepath + "_clean.pdf"
    tmp_rejected = filepath + "_rejected.pdf"

    clean_doc.save(tmp_clean)
    rejected_doc.save(tmp_rejected)
    clean_doc.close()
    rejected_doc.close()

    os.remove(filepath)

    _route_file(tmp_clean, CLEAN_FOLDER, filename)
    _route_file(tmp_rejected, REJECTED_FOLDER, filename)

    _append_to_csv(filename)
    reason = (
        "Partial duplicates stripped. Duplicate pages: "
        + ", ".join(str(p + 1) for p in sorted(duplicate_pages))
    )
    return True, reason


# -- main file processor -------------------------------------------------------
def process_file(filepath: str) -> None:
    total_start = time.perf_counter()

    filename = os.path.basename(filepath)
    awb = (_awb_from_processed_filename(filename) or "").strip()

    cache_state = "MISS"

    log.info("=" * 55)
    log.info("File:  %s", filename)
    log.info("AWB:   %s", awb or "<invalid>")

    # Invalid AWB name format: pass through unchecked.
    if not awb:
        _route_file(filepath, CLEAN_FOLDER, filename)
        _append_to_csv(filename)
        edm_secs = round((time.perf_counter() - total_start), 2)
        reason = "Invalid filename format for AWB extraction"
        _record_outcome(
            awb="",
            filename=filename,
            result="CLEAN-UNCHECKED",
            reason=reason,
            match_stats="N/A",
            total_pages=None,
            dup_count=None,
            dup_ratio=None,
            compare_method="INVALID-AWB",
            edm_secs=edm_secs,
            cache_state=cache_state,
            route="CLEAN",
        )
        return

    # Keep cache only for active AWB.
    if _AWB_SESSION_CACHE["awb"] and _AWB_SESSION_CACHE["awb"] != awb:
        _clear_awb_cache("switching to new AWB")

    if not is_edm_enabled():
        log.info("EDM toggle is OFF; bypassing EDM calls")
        _route_file(filepath, CLEAN_FOLDER, filename)
        _append_to_csv(filename)
        edm_secs = round((time.perf_counter() - total_start), 2)
        _record_outcome(
            awb=awb,
            filename=filename,
            result="CLEAN-UNCHECKED",
            reason="EDM toggle OFF (API bypass)",
            match_stats="N/A",
            total_pages=None,
            dup_count=None,
            dup_ratio=None,
            compare_method="BYPASS-TOGGLE-OFF",
            edm_secs=edm_secs,
            cache_state=cache_state,
            route="CLEAN",
        )
        return

    token = _get_token()
    if not token:
        log.warning("No EDM token available; passing through unchecked")
        _route_file(filepath, CLEAN_FOLDER, filename)
        _append_to_csv(filename)
        edm_secs = round((time.perf_counter() - total_start), 2)
        _record_outcome(
            awb=awb,
            filename=filename,
            result="CLEAN-UNCHECKED",
            reason="No EDM token available",
            match_stats="N/A",
            total_pages=None,
            dup_count=None,
            dup_ratio=None,
            compare_method="BYPASS-NO-TOKEN",
            edm_secs=edm_secs,
            cache_state=cache_state,
            route="CLEAN",
        )
        return

    # Cache hit for repeated same-AWB docs in a run.
    cache_ready = (
        _AWB_SESSION_CACHE["awb"] == awb
        and _AWB_SESSION_CACHE["doc_ids"] is not None
        and _AWB_SESSION_CACHE["edm_pdf_list"] is not None
        and _AWB_SESSION_CACHE["edm_fingerprints"] is not None
        and _AWB_SESSION_CACHE["edm_ocr_cache"] is not None
    )

    if cache_ready:
        cache_state = "HIT"
        doc_ids = list(_AWB_SESSION_CACHE["doc_ids"])
        edm_pdf_list = list(_AWB_SESSION_CACHE["edm_pdf_list"])
        edm_fingerprints = list(_AWB_SESSION_CACHE["edm_fingerprints"])
        edm_ocr_cache = dict(_AWB_SESSION_CACHE["edm_ocr_cache"])
        log.info("[CACHE] AWB cache hit for %s", awb)
    else:
        cache_state = "MISS"

        log.info("Querying EDM metadata...")
        doc_ids = get_document_ids(awb, token)
        if doc_ids is None:
            _route_file(filepath, CLEAN_FOLDER, filename)
            _append_to_csv(filename)
            edm_secs = round((time.perf_counter() - total_start), 2)
            _record_outcome(
                awb=awb,
                filename=filename,
                result="CLEAN-UNCHECKED",
                reason="EDM metadata query inconclusive/unauthorized",
                match_stats="N/A",
                total_pages=None,
                dup_count=None,
                dup_ratio=None,
                compare_method="METADATA-INCONCLUSIVE",
                edm_secs=edm_secs,
                cache_state=cache_state,
                route="CLEAN",
            )
            return

        if not doc_ids:
            _AWB_SESSION_CACHE["awb"] = awb
            _AWB_SESSION_CACHE["doc_ids"] = []
            _AWB_SESSION_CACHE["edm_pdf_list"] = []
            _AWB_SESSION_CACHE["edm_fingerprints"] = []
            _AWB_SESSION_CACHE["edm_ocr_cache"] = {}
            edm_pdf_list = []
            edm_fingerprints = []
            edm_ocr_cache = {}
        else:
            log.info("Found %s existing EDM document id(s)", len(doc_ids))
            zip_bytes = download_edm_zip(doc_ids, token)
            if not zip_bytes:
                _route_file(filepath, CLEAN_FOLDER, filename)
                _append_to_csv(filename)
                edm_secs = round((time.perf_counter() - total_start), 2)
                _record_outcome(
                    awb=awb,
                    filename=filename,
                    result="CLEAN-UNCHECKED",
                    reason="EDM download failed",
                    match_stats="N/A",
                    total_pages=None,
                    dup_count=None,
                    dup_ratio=None,
                    compare_method="DOWNLOAD-FAILED",
                    edm_secs=edm_secs,
                    cache_state=cache_state,
                    route="CLEAN",
                )
                return

            edm_pdf_list = extract_pdfs_from_zip(zip_bytes)
            if not edm_pdf_list:
                _route_file(filepath, CLEAN_FOLDER, filename)
                _append_to_csv(filename)
                edm_secs = round((time.perf_counter() - total_start), 2)
                _record_outcome(
                    awb=awb,
                    filename=filename,
                    result="CLEAN-UNCHECKED",
                    reason="EDM ZIP empty or unreadable",
                    match_stats="N/A",
                    total_pages=None,
                    dup_count=None,
                    dup_ratio=None,
                    compare_method="ZIP-EMPTY",
                    edm_secs=edm_secs,
                    cache_state=cache_state,
                    route="CLEAN",
                )
                return

            edm_fingerprints = build_edm_fingerprints(
                edm_pdf_list,
                hash_page_limit=-1,  # all pages for exact-hash gate
                phash_page_limit=0,
                text_page_limit=0,
            )
            edm_ocr_cache = {}
            _AWB_SESSION_CACHE["awb"] = awb
            _AWB_SESSION_CACHE["doc_ids"] = list(doc_ids)
            _AWB_SESSION_CACHE["edm_pdf_list"] = list(edm_pdf_list)
            _AWB_SESSION_CACHE["edm_fingerprints"] = list(edm_fingerprints)
            _AWB_SESSION_CACHE["edm_ocr_cache"] = dict(edm_ocr_cache)
            log.info("[CACHE] Cached EDM snapshot for %s (%s PDFs)", awb, len(edm_pdf_list))

    if not doc_ids:
        _route_file(filepath, CLEAN_FOLDER, filename)
        _append_to_csv(filename)
        edm_secs = round((time.perf_counter() - total_start), 2)
        _record_outcome(
            awb=awb,
            filename=filename,
            result="CLEAN",
            reason="AWB not found in EDM",
            match_stats="N/A",
            total_pages=None,
            dup_count=0,
            dup_ratio=0.0,
            compare_method="AWB-NOT-IN-EDM",
            edm_secs=edm_secs,
            cache_state=cache_state,
            route="CLEAN",
        )
        return

    log.info("Comparing incoming doc against %s EDM PDF(s)...", len(edm_pdf_list))
    duplicate_pages, compare_meta = find_duplicate_pages(
        filepath,
        edm_pdf_list,
        edm_fingerprints=edm_fingerprints,
        edm_ocr_cache=edm_ocr_cache,
    )

    # Persist warmed OCR cache for later same-AWB files in this session.
    _AWB_SESSION_CACHE["edm_fingerprints"] = list(edm_fingerprints)
    _AWB_SESSION_CACHE["edm_ocr_cache"] = dict(edm_ocr_cache)

    try:
        incoming_doc = fitz.open(filepath)
        total_pages = len(incoming_doc)
        incoming_doc.close()
    except Exception:
        total_pages = 0

    methods = dict(compare_meta.get("method_counts", {}))
    reject_conf = _rejection_confidence(methods)

    # Conservative guard:
    # Use HASH/PHASH pages only for automatic reject/split decisions.
    # TEXT/OCR-only similarity is treated as non-destructive (no auto reject).
    strong_methods = {"HASH", "PHASH"}
    page_details = dict(compare_meta.get("page_details", {}))
    strong_duplicate_pages = {
        int(page_no) - 1
        for page_no, info in page_details.items()
        if str((info or {}).get("method", "")).upper() in strong_methods
    }
    has_strong_evidence = len(strong_duplicate_pages) > 0
    effective_duplicate_pages = strong_duplicate_pages if has_strong_evidence else set()

    dup_count_raw = len(duplicate_pages)
    dup_count = len(effective_duplicate_pages)
    dup_ratio = (dup_count / total_pages) if total_pages else 0.0

    decision_trace = str(compare_meta.get("decision_trace", ""))
    match_stats = (
        f"dup_pages_raw={sorted([p + 1 for p in duplicate_pages])} "
        f"dup_pages_effective={sorted([p + 1 for p in effective_duplicate_pages])} "
        f"total_pages={total_pages} edm_docs={len(edm_pdf_list)} "
        f"dup_count_raw={dup_count_raw} dup_count_effective={dup_count} dup_ratio_effective={dup_ratio:.2f} "
        f"has_strong_evidence={has_strong_evidence} "
        f"reject_confidence={reject_conf} trace={decision_trace}"
    )

    # CCD all-doc bypass path.
    if bool(compare_meta.get("all_ccd", False)):
        log.info("All incoming pages classified as CCD; bypassing duplicate checks")
        _route_file(filepath, CLEAN_FOLDER, filename)
        _append_to_csv(filename)
        edm_secs = round((time.perf_counter() - total_start), 2)
        _record_outcome(
            awb=awb,
            filename=filename,
            result="CLEAN",
            reason="All incoming pages are CCD (always exempt)",
            match_stats=match_stats,
            total_pages=total_pages,
            dup_count=0,
            dup_ratio=0.0,
            compare_method="CCD-BYPASS",
            edm_secs=edm_secs,
            cache_state=cache_state,
            route="CLEAN",
        )
        return

    # No gate/probe hit fast clean.
    if not bool(compare_meta.get("tier1_hit", False)):
        log.info("No hash/probe hit; CLEAN fast path")
        _route_file(filepath, CLEAN_FOLDER, filename)
        _append_to_csv(filename)
        edm_secs = round((time.perf_counter() - total_start), 2)
        _record_outcome(
            awb=awb,
            filename=filename,
            result="CLEAN",
            reason="Hash gate + bounded probes found no hit (fast clean)",
            match_stats=match_stats,
            total_pages=total_pages,
            dup_count=0,
            dup_ratio=0.0,
            compare_method="HASH-PROBE-FAST-CLEAN",
            edm_secs=edm_secs,
            cache_state=cache_state,
            route="CLEAN",
        )
        return

    # No strong (HASH/PHASH) evidence: never auto-reject or auto-strip.
    # This protects against similar-but-different invoices where text overlaps.
    if dup_count_raw > 0 and not has_strong_evidence:
        _route_file(filepath, CLEAN_FOLDER, filename)
        _append_to_csv(filename)
        edm_secs = round((time.perf_counter() - total_start), 2)
        _record_outcome(
            awb=awb,
            filename=filename,
            result="CLEAN-UNCHECKED",
            reason="Text/OCR-only similarity found; no HASH/PHASH evidence so auto-reject is bypassed",
            match_stats=match_stats,
            total_pages=total_pages,
            dup_count=dup_count_raw,
            dup_ratio=(dup_count_raw / total_pages) if total_pages else 0.0,
            compare_method="TEXT-OCR-ONLY-BYPASS",
            edm_secs=edm_secs,
            cache_state=cache_state,
            route="CLEAN",
        )
        return

    # No duplicates after full Tier2.
    if dup_count == 0:
        _route_file(filepath, CLEAN_FOLDER, filename)
        _append_to_csv(filename)
        edm_secs = round((time.perf_counter() - total_start), 2)
        _record_outcome(
            awb=awb,
            filename=filename,
            result="CLEAN",
            reason="No matching pages found in Tier2",
            match_stats=match_stats,
            total_pages=total_pages,
            dup_count=0,
            dup_ratio=0.0,
            compare_method="TIER2-NO-DUP",
            edm_secs=edm_secs,
            cache_state=cache_state,
            route="CLEAN",
        )
        return

    # All pages duplicate.
    if total_pages > 0 and dup_count == total_pages:
        edm_secs = round((time.perf_counter() - total_start), 2)
        if reject_conf == "LOW":
            # Conservative protection on weak evidence.
            _route_file(filepath, NEEDS_REVIEW_FOLDER, filename)
            _record_outcome(
                awb=awb,
                filename=filename,
                result="NEEDS-REVIEW",
                reason="All pages matched but confidence LOW",
                match_stats=match_stats,
                total_pages=total_pages,
                dup_count=dup_count,
                dup_ratio=dup_ratio,
                compare_method="ALL-DUP-LOW-CONF",
                edm_secs=edm_secs,
                cache_state=cache_state,
                route="NEEDS_REVIEW",
            )
            return

        _route_file(filepath, REJECTED_FOLDER, filename)
        _record_outcome(
            awb=awb,
            filename=filename,
            result="REJECTED",
            reason=f"All {total_pages} page(s) matched EDM",
            match_stats=match_stats,
            total_pages=total_pages,
            dup_count=dup_count,
            dup_ratio=dup_ratio,
            compare_method="ALL-DUP",
            edm_secs=edm_secs,
            cache_state=cache_state,
            route="REJECTED",
        )
        return

    # Threshold reject.
    if (
        dup_count > REJECT_IF_DUP_PAGES_OVER
        and dup_ratio >= REJECT_IF_DUP_RATIO
        and reject_conf != "LOW"
    ):
        _route_file(filepath, REJECTED_FOLDER, filename)
        edm_secs = round((time.perf_counter() - total_start), 2)
        _record_outcome(
            awb=awb,
            filename=filename,
            result="REJECTED",
            reason=(
                f"Duplicate threshold exceeded (dup_count={dup_count}, "
                f"ratio={dup_ratio:.2f}, confidence={reject_conf})"
            ),
            match_stats=match_stats,
            total_pages=total_pages,
            dup_count=dup_count,
            dup_ratio=dup_ratio,
            compare_method="THRESHOLD-REJECT",
            edm_secs=edm_secs,
            cache_state=cache_state,
            route="REJECTED",
        )
        return

    # Partial duplicate split.
    try:
        ok, partial_reason = _process_partial_split(
            filepath,
            filename,
            effective_duplicate_pages,
            total_pages,
        )
        edm_secs = round((time.perf_counter() - total_start), 2)
        if ok:
            _record_outcome(
                awb=awb,
                filename=filename,
                result="PARTIAL-CLEAN",
                reason=partial_reason,
                match_stats=match_stats,
                total_pages=total_pages,
                dup_count=dup_count,
                dup_ratio=dup_ratio,
                compare_method="PARTIAL-SPLIT",
                edm_secs=edm_secs,
                cache_state=cache_state,
                route="CLEAN+REJECTED",
            )
            return
    except Exception as e:
        log.warning("Error stripping duplicate pages: %s", e)

    # Any strip failure goes to NEEDS_REVIEW.
    _route_file(filepath, NEEDS_REVIEW_FOLDER, filename)
    edm_secs = round((time.perf_counter() - total_start), 2)
    _record_outcome(
        awb=awb,
        filename=filename,
        result="NEEDS-REVIEW",
        reason="Page split/strip failed",
        match_stats=match_stats,
        total_pages=total_pages,
        dup_count=dup_count,
        dup_ratio=dup_ratio,
        compare_method="PARTIAL-SPLIT-FAILED",
        edm_secs=edm_secs,
        cache_state=cache_state,
        route="NEEDS_REVIEW",
    )


# -- watchdog ------------------------------------------------------------------
class ProcessedPDFHandler(FileSystemEventHandler):
    def __init__(self) -> None:
        self._last_seen: dict[str, float] = {}

    def _handle(self, path: str) -> None:
        if not str(path).lower().endswith(".pdf"):
            return

        filepath = str(path)
        filename = os.path.basename(filepath)

        now = time.time()
        # Debounce duplicate FS notifications (create+modify bursts, move events).
        if now - self._last_seen.get(filepath, 0.0) < 0.8:
            return
        self._last_seen[filepath] = now

        log.info("New/updated file detected: %s", filename)

        time.sleep(FILE_SETTLE_SECONDS)
        if not os.path.exists(filepath):
            log.warning("File gone before processing: %s", filename)
            return

        if not file_is_stable(filepath):
            log.warning("File not yet stable; skipping this event: %s", filename)
            return

        try:
            process_file(filepath)
        except Exception as e:
            log.error("Unexpected error on %s: %s", filename, e)

    def on_created(self, event):
        if event.is_directory:
            return
        self._handle(str(event.src_path))

    def on_moved(self, event):
        if event.is_directory:
            return
        self._handle(str(event.dest_path))

    def on_modified(self, event):
        if event.is_directory:
            return
        self._handle(str(event.src_path))


def main() -> None:
    config.ensure_dirs()

    if requests is None:
        log.warning("requests package unavailable; EDM duplicate checker will pass files unchecked")

    log.info("EDM Duplicate Checker V3 started")
    log.info("Watching:  %s", PROCESSED_FOLDER)
    log.info("CLEAN:     %s", CLEAN_FOLDER)
    log.info("REJECTED:  %s", REJECTED_FOLDER)
    log.info("NEEDS_REVIEW: %s", NEEDS_REVIEW_FOLDER)
    log.info(
        "Tier1: incoming p1-%s vs EDM p1-%s (hash+pHash only)",
        TIER1_INCOMING_PAGES,
        TIER1_EDM_PAGE_LIMIT,
    )
    log.info(
        "Tier2: incoming all pages vs EDM p1-%s | text-layer min chars=%s",
        TIER2_EDM_PAGE_LIMIT,
        TEXT_LAYER_MIN_CHARS,
    )

    existing = [f for f in PROCESSED_FOLDER.iterdir() if f.suffix.lower() == ".pdf"]
    if existing:
        log.info("Found %s existing file(s); processing now", len(existing))
        for fp in existing:
            if not file_is_stable(str(fp)):
                log.warning("Skipping unstable startup file: %s", fp.name)
                continue
            try:
                process_file(str(fp))
            except Exception as e:
                log.error("Error on %s: %s", fp.name, e)

    observer = Observer()
    observer.schedule(ProcessedPDFHandler(), str(PROCESSED_FOLDER), recursive=False)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        log.info("Shutting down EDM duplicate checker...")
        observer.stop()

    observer.join()
    log.info("EDM Duplicate Checker V3 stopped")


if __name__ == "__main__":
    main()
