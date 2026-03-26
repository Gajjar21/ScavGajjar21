#!/usr/bin/env python3
"""
EDM duplicate test with simulated printed+scanned PDFs.

Takes real PDFs, renders them to images (removing text layer), then
applies realistic scan disturbance:
  - Gaussian noise (printer/scanner grain)
  - Slight random rotation (±0.5°–1.5°, paper not perfectly straight)
  - Slight brightness/contrast drift (lamp variation)
  - Optional JPEG compression artefacts (low-quality scanner setting)

Then runs find_duplicate_pages() to see whether pHash still catches
the "printed" copy as a duplicate of the original digital file.
"""

import sys
import io
import os
import numpy as np
from PIL import Image, ImageFilter, ImageEnhance
import fitz

sys.path.insert(0, "/Users/gajjar/Desktop/ScavGajjar21")
from V3.services.edm_duplicate_checker import find_duplicate_pages, build_edm_fingerprints

ATTACHED = "/Users/gajjar/Downloads/attached"
P = lambda n: os.path.join(ATTACHED, n)

# ─────────────────────────────────────────────────────────────────────────────
# Simulate scan disturbance
# ─────────────────────────────────────────────────────────────────────────────

def render_page_to_pil(pdf_bytes: bytes, page_idx: int, dpi: int = 150) -> Image.Image:
    """Render one page from PDF bytes to a PIL RGB image."""
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    page = doc[page_idx]
    mat = fitz.Matrix(dpi / 72, dpi / 72)
    pix = page.get_pixmap(matrix=mat, colorspace=fitz.csRGB)
    img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
    doc.close()
    return img


def disturb_image(
    img: Image.Image,
    noise_std: float = 8.0,
    rotation_deg: float = 0.8,
    brightness_factor: float = 1.05,
    contrast_factor: float = 0.97,
    jpeg_quality: int = 75,
) -> Image.Image:
    """
    Simulate print+scan artefacts:
      1. Slight rotation (paper not perfectly aligned on scanner glass)
      2. Gaussian noise (sensor grain, paper texture)
      3. Brightness/contrast shift (scanner lamp variation)
      4. JPEG recompression (scanner saves as JPEG)
    """
    # 1. Slight rotation (white fill for background)
    img = img.rotate(rotation_deg, expand=False, fillcolor=(255, 255, 255))

    # 2. Gaussian noise
    arr = np.array(img, dtype=np.float32)
    noise = np.random.normal(0, noise_std, arr.shape)
    arr = np.clip(arr + noise, 0, 255).astype(np.uint8)
    img = Image.fromarray(arr)

    # 3. Brightness + contrast drift
    img = ImageEnhance.Brightness(img).enhance(brightness_factor)
    img = ImageEnhance.Contrast(img).enhance(contrast_factor)

    # 4. JPEG round-trip (scanner compression)
    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=jpeg_quality)
    buf.seek(0)
    img = Image.open(buf).convert("RGB")

    return img


def make_scanned_pdf(
    source_pdf_bytes: bytes,
    dpi: int = 150,
    noise_std: float = 8.0,
    rotation_deg: float = 0.8,
    jpeg_quality: int = 75,
) -> bytes:
    """
    Convert every page of a PDF into a disturbed image-only (no text layer) PDF.
    Mimics: print → scan with slight noise, tilt, and JPEG compression.
    """
    src_doc = fitz.open(stream=source_pdf_bytes, filetype="pdf")
    out_doc = fitz.open()

    for page_idx in range(src_doc.page_count):
        pil_img = render_page_to_pil(source_pdf_bytes, page_idx, dpi=dpi)
        disturbed = disturb_image(
            pil_img,
            noise_std=noise_std,
            rotation_deg=rotation_deg,
            jpeg_quality=jpeg_quality,
        )

        # Convert PIL image → PDF page
        img_buf = io.BytesIO()
        disturbed.save(img_buf, format="PDF")
        img_buf.seek(0)
        page_doc = fitz.open(stream=img_buf.read(), filetype="pdf")
        out_doc.insert_pdf(page_doc)
        page_doc.close()

    src_doc.close()
    out_buf = io.BytesIO()
    out_doc.save(out_buf)
    out_buf.seek(0)
    return out_buf.read()


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def pdf_bytes(path: str) -> bytes:
    with open(path, "rb") as f:
        return f.read()


def routing_label(dup_pages: set, total: int, page_details: dict) -> str:
    from collections import Counter
    method_counts: Counter = Counter()
    for v in page_details.values():
        method_counts[v.get("method", "?")] += 1

    h = method_counts.get("HASH", 0)
    p = method_counts.get("PHASH", 0)
    t = method_counts.get("TEXT", 0)
    o = method_counts.get("OCR", 0)

    strong = {pg for pg in dup_pages
              if page_details.get(pg + 1, {}).get("method") in ("HASH", "PHASH")}
    dup_count = len(strong)

    if not dup_count and (t + o) == 0:
        return "CLEAN"
    if not dup_count:
        return "CLEAN-UNCHECKED  (text/OCR only)"

    dup_ratio = dup_count / total if total else 0

    if h >= 1:
        conf = "HIGH"
    elif p >= 2 and (t + o) >= 1:
        conf = "HIGH"
    elif p >= 3:
        conf = "MEDIUM"
    elif (t + o) >= 3 and p >= 1:
        conf = "MEDIUM"
    else:
        conf = "LOW"

    if dup_count == total:
        return f"REJECTED  (all {total} pages dup, conf={conf})"

    if dup_count > 5 and dup_ratio >= 0.70 and conf != "LOW":
        return f"REJECTED  (threshold {dup_count}/{total} = {dup_ratio:.0%}, conf={conf})"

    if 0 < dup_count < total:
        return f"SPLIT  ({dup_count} dup / {total - dup_count} clean, conf={conf})"

    return f"CLEAN  (dup_count=0)"


def run_test(name: str, incoming_path: str, edm_bytes_list: list[bytes]) -> None:
    print(f"\n{'='*62}")
    print(f"TEST: {name}")
    inc_doc = fitz.open(incoming_path)
    total = inc_doc.page_count
    inc_doc.close()
    print(f"  incoming pages : {total}")
    print(f"  EDM docs       : {len(edm_bytes_list)}")

    fps = build_edm_fingerprints(edm_bytes_list, hash_page_limit=-1, phash_page_limit=0, text_page_limit=0)
    dup_pages, meta = find_duplicate_pages(incoming_path, edm_bytes_list, edm_fingerprints=fps)

    page_details = meta.get("page_details", {})
    route = routing_label(dup_pages, total, page_details)

    print(f"  result         : {route}")
    print(f"  dup_pages      : {sorted(dup_pages)} (0-indexed)")
    print(f"  methods        : {dict(meta.get('method_counts', {}))}")
    for pg_1, det in sorted(page_details.items()):
        score = det.get("score", det.get("diff", "?"))
        print(f"    page {pg_1}: {det.get('method')}  score/diff={score}")
    print(f"  tier1_hit      : {meta.get('tier1_hit')}")


import tempfile

print("Building simulated scanned PDFs (this renders images — ~20s) ...")

# ── Source files ──────────────────────────────────────────────────────────────
SRC_A = P("473019035113.pdf")   # 2 pages, has text layer
SRC_B = P("888528178007.pdf")   # 7 pages, likely image-heavy

raw_A = pdf_bytes(SRC_A)
raw_B = pdf_bytes(SRC_B)

# ── Build scanned versions: light disturbance ─────────────────────────────────
print("  [1/4] Scanning A — light noise (std=6, rot=0.5°, jpeg=80) ...")
scanned_A_light = make_scanned_pdf(raw_A, noise_std=6, rotation_deg=0.5, jpeg_quality=80)

print("  [2/4] Scanning A — heavy noise (std=18, rot=1.5°, jpeg=55) ...")
scanned_A_heavy = make_scanned_pdf(raw_A, noise_std=18, rotation_deg=1.5, jpeg_quality=55)

print("  [3/4] Scanning B — light noise (std=6, rot=0.5°, jpeg=80) ...")
scanned_B_light = make_scanned_pdf(raw_B, noise_std=6, rotation_deg=0.5, jpeg_quality=80)

print("  [4/4] Scanning B — heavy noise (std=18, rot=1.5°, jpeg=55) ...")
scanned_B_heavy = make_scanned_pdf(raw_B, noise_std=18, rotation_deg=1.5, jpeg_quality=55)

print("Done.\n")

# Save to temp files (find_duplicate_pages needs a filepath)
def to_tmp(data: bytes) -> str:
    f = tempfile.NamedTemporaryFile(suffix=".pdf", delete=False)
    f.write(data)
    f.close()
    return f.name

tmp_A_light = to_tmp(scanned_A_light)
tmp_A_heavy = to_tmp(scanned_A_heavy)
tmp_B_light = to_tmp(scanned_B_light)
tmp_B_heavy = to_tmp(scanned_B_heavy)

# ─────────────────────────────────────────────────────────────────────────────
# TESTS
# ─────────────────────────────────────────────────────────────────────────────

# 1. Light scan of A vs original A → should still match (pHash)
run_test(
    "1. Light scan of 473019035113 vs original\n   (noise=6 std, rot=0.5°, jpeg=80) → expect REJECTED",
    incoming_path=tmp_A_light,
    edm_bytes_list=[raw_A],
)

# 2. Heavy scan of A vs original A → pHash might miss at this disturbance level
run_test(
    "2. Heavy scan of 473019035113 vs original\n   (noise=18 std, rot=1.5°, jpeg=55) → expect REJECTED or NEEDS-REVIEW",
    incoming_path=tmp_A_heavy,
    edm_bytes_list=[raw_A],
)

# 3. Light scan of A vs light scan of A (both printed) → both image-only, no HASH
run_test(
    "3. Light-scanned vs light-scanned (both image-only)\n   → expect REJECTED (pHash match between two scans)",
    incoming_path=tmp_A_light,
    edm_bytes_list=[scanned_A_light],
)

# 4. Heavy scan of A vs light scan of A (different scan sessions)
run_test(
    "4. Heavy scan vs light scan (different scan sessions)\n   → pHash tolerance test",
    incoming_path=tmp_A_heavy,
    edm_bytes_list=[scanned_A_light],
)

# 5. Light scan of B (7 pages) vs original B → expect all pages REJECTED
run_test(
    "5. Light scan of 888528178007 (7p) vs original\n   → expect all 7 pages REJECTED",
    incoming_path=tmp_B_light,
    edm_bytes_list=[raw_B],
)

# 6. Heavy scan of B vs original B
run_test(
    "6. Heavy scan of 888528178007 (7p) vs original\n   (noise=18, rot=1.5°, jpeg=55) → heavier disturbance",
    incoming_path=tmp_B_heavy,
    edm_bytes_list=[raw_B],
)

# 7. Cross-document: scan of A vs original B → should be CLEAN (no match)
run_test(
    "7. Scanned A vs original B (completely different docs) → CLEAN",
    incoming_path=tmp_A_light,
    edm_bytes_list=[raw_B],
)

# 8. Mixed: first page of scanned B + 2 clean pages (from A) vs original B
import fitz as _fitz

def make_mixed_tmp(scanned_pdf_bytes: bytes, clean_pdf_bytes: bytes, n_clean: int) -> str:
    out = _fitz.open()
    sc = _fitz.open(stream=scanned_pdf_bytes, filetype="pdf")
    cl = _fitz.open(stream=clean_pdf_bytes, filetype="pdf")
    out.insert_pdf(sc, from_page=0, to_page=0)  # 1 dup page
    for i in range(n_clean):
        out.insert_pdf(cl, from_page=i, to_page=i)
    sc.close()
    cl.close()
    buf = io.BytesIO()
    out.save(buf)
    buf.seek(0)
    f = tempfile.NamedTemporaryFile(suffix=".pdf", delete=False)
    f.write(buf.read())
    f.close()
    return f.name

tmp_mixed = make_mixed_tmp(scanned_B_light, raw_A, 2)

run_test(
    "8. Mixed: page 1 of scanned B (dup) + 2 pages of A (clean)\n   vs original B → SPLIT expected",
    incoming_path=tmp_mixed,
    edm_bytes_list=[raw_B],
)

# Cleanup
for t in [tmp_A_light, tmp_A_heavy, tmp_B_light, tmp_B_heavy, tmp_mixed]:
    try:
        os.unlink(t)
    except Exception:
        pass

print("\n" + "="*62)
print("All scanned-copy tests complete.")
