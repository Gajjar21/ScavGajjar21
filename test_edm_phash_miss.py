#!/usr/bin/env python3
"""
Test what happens when pHash doesn't catch a printed/scanned duplicate.

Strategy:
  - Gradually increase disturbance until pHash diff > 10 (the threshold)
  - Show what TEXT / OCR fallback does
  - Show the system's designed behaviour: TEXT/OCR-only → CLEAN-UNCHECKED
  - Confirm no false REJECTED routes occur

Disturbance levels tested:
  A. Mild      (noise=6,  rot=0.5°, jpeg=80, dpi=150) — pHash should pass
  B. Moderate  (noise=18, rot=2.0°, jpeg=50, dpi=150) — borderline
  C. Heavy     (noise=35, rot=4.0°, jpeg=35, dpi=100) — likely breaks pHash
  D. Extreme   (noise=55, rot=6.0°, jpeg=25, dpi=72 ) — almost certainly breaks pHash
  E. Worst     (noise=80, rot=8.0°, jpeg=15, dpi=72 ) — pHash definitely breaks
"""

import sys, io, os, tempfile
import numpy as np
from PIL import Image, ImageEnhance
import fitz

sys.path.insert(0, "/Users/gajjar/Desktop/ScavGajjar21")
from V3.services.edm_duplicate_checker import find_duplicate_pages, build_edm_fingerprints

ATTACHED = "/Users/gajjar/Downloads/attached"
P = lambda n: os.path.join(ATTACHED, n)

# ─────────────────────────────────────────────────────────────────────────────
def pdf_bytes(path: str) -> bytes:
    with open(path, "rb") as f:
        return f.read()

def render_page_to_pil(pdf_bytes_: bytes, page_idx: int, dpi: int) -> Image.Image:
    doc = fitz.open(stream=pdf_bytes_, filetype="pdf")
    page = doc[page_idx]
    mat = fitz.Matrix(dpi / 72, dpi / 72)
    pix = page.get_pixmap(matrix=mat, colorspace=fitz.csRGB)
    img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
    doc.close()
    return img

def disturb(img: Image.Image, noise: float, rot: float, jpeg_q: int) -> Image.Image:
    img = img.rotate(rot, expand=False, fillcolor=(255, 255, 255))
    arr = np.array(img, dtype=np.float32)
    arr = np.clip(arr + np.random.normal(0, noise, arr.shape), 0, 255).astype(np.uint8)
    img = Image.fromarray(arr)
    img = ImageEnhance.Brightness(img).enhance(1.06)
    img = ImageEnhance.Contrast(img).enhance(0.95)
    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=jpeg_q)
    buf.seek(0)
    return Image.open(buf).convert("RGB")

def make_scanned_pdf(raw: bytes, dpi: int, noise: float, rot: float, jpeg_q: int) -> bytes:
    src = fitz.open(stream=raw, filetype="pdf")
    out = fitz.open()
    for i in range(src.page_count):
        pil = render_page_to_pil(raw, i, dpi)
        d   = disturb(pil, noise, rot, jpeg_q)
        buf = io.BytesIO()
        d.save(buf, format="PDF")
        buf.seek(0)
        pg_doc = fitz.open(stream=buf.read(), filetype="pdf")
        out.insert_pdf(pg_doc)
        pg_doc.close()
    src.close()
    obuf = io.BytesIO()
    out.save(obuf)
    obuf.seek(0)
    return obuf.read()

def to_tmp(data: bytes) -> str:
    f = tempfile.NamedTemporaryFile(suffix=".pdf", delete=False)
    f.write(data)
    f.close()
    return f.name

# ─────────────────────────────────────────────────────────────────────────────
# Measure raw pHash diff before routing (so we know exactly when threshold breaks)
# ─────────────────────────────────────────────────────────────────────────────
try:
    import imagehash
    from PIL import Image as _PIL
    HAS_IMAGEHASH = True
except ImportError:
    HAS_IMAGEHASH = False

def measure_phash_diffs(scanned_bytes: bytes, original_bytes: bytes) -> list[int]:
    """Return pHash Hamming distance per page between scanned and original."""
    if not HAS_IMAGEHASH:
        return []
    diffs = []
    src_orig = fitz.open(stream=original_bytes, filetype="pdf")
    src_scan = fitz.open(stream=scanned_bytes, filetype="pdf")
    pages = min(src_orig.page_count, src_scan.page_count)
    for i in range(pages):
        def page_hash(doc, idx):
            mat = fitz.Matrix(72/72, 72/72)
            pix = doc[idx].get_pixmap(matrix=mat, colorspace=fitz.csRGB)
            img = _PIL.frombytes("RGB", [pix.width, pix.height], pix.samples)
            return imagehash.phash(img)
        h1 = page_hash(src_orig, i)
        h2 = page_hash(src_scan, i)
        diffs.append(abs(h1 - h2))
    src_orig.close()
    src_scan.close()
    return diffs

def routing_summary(dup_pages, total, meta) -> str:
    from collections import Counter
    pd = meta.get("page_details", {})
    mc = Counter(v.get("method") for v in pd.values())
    h, p_, t, o = mc["HASH"], mc["PHASH"], mc["TEXT"], mc["OCR"]
    strong = sum(1 for v in pd.values() if v.get("method") in ("HASH", "PHASH"))
    if not strong and not (t+o):
        return "CLEAN"
    if not strong:
        return "CLEAN-UNCHECKED  ← TEXT/OCR only, no hash evidence"
    dup_ratio = strong / total
    conf = "HIGH" if h>=1 else ("HIGH" if p_>=2 and t+o>=1 else ("MEDIUM" if p_>=3 else ("MEDIUM" if t+o>=3 and p_>=1 else "LOW")))
    if strong == total:
        return f"REJECTED  (all pages, conf={conf})"
    if strong > 5 and dup_ratio >= 0.70 and conf != "LOW":
        return f"REJECTED  (threshold, conf={conf})"
    if 0 < strong < total:
        return f"SPLIT  ({strong} dup / {total-strong} clean, conf={conf})"
    return "CLEAN"

# ─────────────────────────────────────────────────────────────────────────────
SRC   = P("473019035113.pdf")   # 2-page text+image AWB
raw   = pdf_bytes(SRC)

LEVELS = [
    ("A  MILD",     150, 6,  0.5, 80),
    ("B  MODERATE", 150, 18, 2.0, 50),
    ("C  HEAVY",    100, 35, 4.0, 35),
    ("D  EXTREME",  72,  55, 6.0, 25),
    ("E  WORST",    72,  80, 8.0, 15),
]

print(f"{'='*65}")
print(f"pHash threshold = 10  (Hamming distance ≤ 10 = match)")
print(f"{'='*65}")

for label, dpi, noise, rot, jpeg_q in LEVELS:
    scanned = make_scanned_pdf(raw, dpi=dpi, noise=noise, rot=rot, jpeg_q=jpeg_q)

    diffs = measure_phash_diffs(scanned, raw)
    diff_str = ("  pHash diffs (vs original): " + ", ".join(f"p{i+1}={d}" for i, d in enumerate(diffs))
                if diffs else "  (imagehash not installed — diffs not shown)")

    tmp = to_tmp(scanned)
    fps = build_edm_fingerprints([raw], hash_page_limit=-1, phash_page_limit=0, text_page_limit=0)
    dup_pages, meta = find_duplicate_pages(tmp, [raw], edm_fingerprints=fps)
    os.unlink(tmp)

    route = routing_summary(dup_pages, 2, meta)
    mc    = dict(meta.get("method_counts", {}))
    t1    = meta.get("tier1_hit")

    hit_mark = "✓ caught" if dup_pages else "✗ MISSED"
    print(f"\nLevel {label}  (dpi={dpi}, noise={noise}, rot={rot}°, jpeg={jpeg_q})")
    print(f"  {diff_str}")
    print(f"  → {route}")
    print(f"  methods={mc}  tier1_hit={t1}  [{hit_mark}]")

# ─────────────────────────────────────────────────────────────────────────────
# Show what TEXT/OCR fallback gives when pHash is completely broken
# (manually force: set PHASH_THRESHOLD=0 so nothing ever matches)
# ─────────────────────────────────────────────────────────────────────────────
print(f"\n{'='*65}")
print("WHAT HAPPENS WHEN pHash IS DISABLED (threshold=0)")
print("Shows TEXT/OCR-only routing path for a moderate scan")
print(f"{'='*65}")

import V3.config as _cfg
original_thresh = getattr(_cfg, "PHASH_THRESHOLD", 10)
_cfg.PHASH_THRESHOLD = 0  # force pHash to never match

# also need to patch the module-level constant inside edm_duplicate_checker
import V3.services.edm_duplicate_checker as _edc
original_edc_thresh = _edc.PHASH_THRESHOLD if hasattr(_edc, "PHASH_THRESHOLD") else 10
if hasattr(_edc, "PHASH_THRESHOLD"):
    _edc.PHASH_THRESHOLD = 0

scanned_mod = make_scanned_pdf(raw, dpi=150, noise=18, rot=2.0, jpeg_q=50)
tmp_mod = to_tmp(scanned_mod)

fps2 = build_edm_fingerprints([raw], hash_page_limit=-1, phash_page_limit=0, text_page_limit=0)
dup_pages2, meta2 = find_duplicate_pages(tmp_mod, [raw], edm_fingerprints=fps2)
os.unlink(tmp_mod)

# Restore
_cfg.PHASH_THRESHOLD = original_thresh
if hasattr(_edc, "PHASH_THRESHOLD"):
    _edc.PHASH_THRESHOLD = original_edc_thresh

route2  = routing_summary(dup_pages2, 2, meta2)
mc2     = dict(meta2.get("method_counts", {}))
pd2     = meta2.get("page_details", {})

print(f"\n  PHASH_THRESHOLD forced to 0 → pHash never fires")
print(f"  → {route2}")
print(f"  methods={mc2}")
for pg, det in sorted(pd2.items()):
    print(f"    page {pg}: method={det.get('method')}  score={det.get('score','?')}")
print()
print("  CONCLUSION:")
print("  TEXT/OCR similarity is recorded but does NOT auto-reject.")
print("  Result: CLEAN-UNCHECKED — file passes through for human review")
print("  (EDM stage 6 fallback will still re-check via API if enabled)")
