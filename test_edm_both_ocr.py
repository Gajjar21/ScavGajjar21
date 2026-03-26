#!/usr/bin/env python3
"""
Test: both incoming AND EDM are image-only (no text layer).

Real-world case: the original was submitted as a scanned image PDF,
so there's no text layer in the EDM archive either. A new rescan
arrives — no HASH, pHash might miss on heavy disturbance, and TEXT
(text-layer extraction) returns empty on both sides. Only OCR can compare.

Flow through edm_duplicate_checker stages:
  HASH  → miss (different scan bytes)
  pHash → miss (if disturbance heavy enough, diff > 10)
  TEXT  → skip (both sides return "" — no text layer)
  OCR   → Tesseract runs on BOTH pages, compares via rapidfuzz
           if score ≥ 85 → OCR match recorded
           routing: TEXT/OCR-only → CLEAN-UNCHECKED (never auto-rejected)
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

def disturb(img: Image.Image, noise: float, rot: float, jpeg_q: int, brightness: float = 1.0) -> Image.Image:
    img = img.rotate(rot, expand=False, fillcolor=(255, 255, 255))
    arr = np.array(img, dtype=np.float32)
    arr = np.clip(arr + np.random.normal(0, noise, arr.shape), 0, 255).astype(np.uint8)
    img = Image.fromarray(arr)
    img = ImageEnhance.Brightness(img).enhance(brightness)
    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=jpeg_q)
    buf.seek(0)
    return Image.open(buf).convert("RGB")

def make_scanned_pdf(raw: bytes, dpi: int, noise: float, rot: float, jpeg_q: int, brightness: float = 1.0) -> bytes:
    """Render PDF to images only (no text layer) with disturbance."""
    src = fitz.open(stream=raw, filetype="pdf")
    out = fitz.open()
    for i in range(src.page_count):
        mat = fitz.Matrix(dpi / 72, dpi / 72)
        pix = src[i].get_pixmap(matrix=mat, colorspace=fitz.csRGB)
        pil = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
        d = disturb(pil, noise, rot, jpeg_q, brightness)
        buf = io.BytesIO()
        d.save(buf, format="PDF")
        buf.seek(0)
        pg = fitz.open(stream=buf.read(), filetype="pdf")
        out.insert_pdf(pg)
        pg.close()
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

def routing_full(dup_pages: set, total: int, meta: dict) -> str:
    from collections import Counter
    pd = meta.get("page_details", {})
    mc = Counter(v.get("method") for v in pd.values())
    h, p_, t, o = mc["HASH"], mc["PHASH"], mc["TEXT"], mc["OCR"]
    strong = sum(1 for v in pd.values() if v.get("method") in ("HASH", "PHASH"))
    has_weak = (t + o) > 0
    if not strong and not has_weak:
        return "CLEAN"
    if not strong:
        return "CLEAN-UNCHECKED  ← TEXT/OCR only, no hash evidence"
    dup_ratio = strong / total
    conf = ("HIGH" if h >= 1 else
            "HIGH" if p_ >= 2 and t + o >= 1 else
            "MEDIUM" if p_ >= 3 else
            "MEDIUM" if t + o >= 3 and p_ >= 1 else "LOW")
    if strong == total:
        return f"REJECTED  (all pages, conf={conf})"
    if strong > 5 and dup_ratio >= 0.70 and conf != "LOW":
        return f"REJECTED  (threshold {strong}/{total} = {dup_ratio:.0%}, conf={conf})"
    if 0 < strong < total:
        return f"SPLIT  ({strong} dup / {total - strong} clean, conf={conf})"
    return "CLEAN"

def run(name: str, inc_bytes: bytes, edm_bytes: bytes) -> None:
    print(f"\n{'='*62}")
    print(f"TEST: {name}")

    # Check for text layers
    def has_text(b: bytes, label: str):
        doc = fitz.open(stream=b, filetype="pdf")
        texts = [doc[i].get_text().strip() for i in range(min(doc.page_count, 2))]
        doc.close()
        total_chars = sum(len(t) for t in texts)
        print(f"  {label} text layer chars (p1+p2): {total_chars}  {'← HAS TEXT' if total_chars > 30 else '← IMAGE-ONLY'}")
        return total_chars

    has_text(inc_bytes, "incoming")
    has_text(edm_bytes, "  EDM   ")

    fps = build_edm_fingerprints([edm_bytes], hash_page_limit=-1, phash_page_limit=0, text_page_limit=0)
    tmp = to_tmp(inc_bytes)

    total = fitz.open(stream=inc_bytes, filetype="pdf").page_count

    dup_pages, meta = find_duplicate_pages(tmp, [edm_bytes], edm_fingerprints=fps)
    os.unlink(tmp)

    pd = meta.get("page_details", {})
    route = routing_full(dup_pages, total, meta)
    mc    = dict(meta.get("method_counts", {}))

    print(f"  result   : {route}")
    print(f"  dup_pages: {sorted(dup_pages)} (0-indexed)")
    print(f"  methods  : {mc}  tier1_hit={meta.get('tier1_hit')}")
    for pg, det in sorted(pd.items()):
        print(f"    page {pg}: method={det.get('method')}  score={det.get('score', det.get('diff', '?'))}")

# ─────────────────────────────────────────────────────────────────────────────
# Use 473019035113.pdf — it has a real text layer (AWB with printed text)
raw = pdf_bytes(P("473019035113.pdf"))

print("Preparing scanned variants ...")

# "EDM copy" — first scan, submitted long ago (image-only, light)
edm_scan_A = make_scanned_pdf(raw, dpi=150, noise=5,  rot=0.3, jpeg_q=85)

# "EDM copy" — heavier scan session
edm_scan_B = make_scanned_pdf(raw, dpi=100, noise=15, rot=1.0, jpeg_q=65)

# "Incoming" — new rescan today, different settings
inc_scan_light  = make_scanned_pdf(raw, dpi=150, noise=8,  rot=0.7, jpeg_q=78)
inc_scan_heavy  = make_scanned_pdf(raw, dpi=100, noise=30, rot=3.0, jpeg_q=40)
inc_scan_worst  = make_scanned_pdf(raw, dpi=72,  noise=60, rot=7.0, jpeg_q=20)

print("Done.\n")

# ── Test 1: image-only incoming vs image-only EDM (light vs light) ────────────
run(
    "1. Both image-only: light incoming vs light EDM\n   pHash should catch it",
    inc_bytes=inc_scan_light,
    edm_bytes=edm_scan_A,
)

# ── Test 2: image-only incoming (heavy) vs image-only EDM (light) ─────────────
run(
    "2. Both image-only: heavy incoming vs light EDM\n   pHash tolerance stress test",
    inc_bytes=inc_scan_heavy,
    edm_bytes=edm_scan_A,
)

# ── Test 3: image-only incoming (worst) vs image-only EDM ─────────────────────
run(
    "3. Both image-only: WORST incoming vs light EDM\n   pHash may miss → OCR comparison fires",
    inc_bytes=inc_scan_worst,
    edm_bytes=edm_scan_A,
)

# ── Test 4: worst incoming vs heavier EDM (both degraded) ────────────────────
run(
    "4. Both degraded: worst incoming vs heavy EDM\n   OCR on both sides — can Tesseract still match?",
    inc_bytes=inc_scan_worst,
    edm_bytes=edm_scan_B,
)

# ── Test 5: force pHash off to isolate OCR path only ─────────────────────────
print(f"\n{'='*62}")
print("TEST: 5. pHash disabled (threshold=0) — isolate OCR-only path")
print(f"{'='*62}")

import V3.services.edm_duplicate_checker as _edc
orig = _edc.PHASH_THRESHOLD if hasattr(_edc, "PHASH_THRESHOLD") else 10
_edc.PHASH_THRESHOLD = 0

fps2 = build_edm_fingerprints([edm_scan_A], hash_page_limit=-1, phash_page_limit=0, text_page_limit=0)
tmp5 = to_tmp(inc_scan_light)
dup5, meta5 = find_duplicate_pages(tmp5, [edm_scan_A], edm_fingerprints=fps2)
os.unlink(tmp5)

_edc.PHASH_THRESHOLD = orig

pd5    = meta5.get("page_details", {})
route5 = routing_full(dup5, 2, meta5)
mc5    = dict(meta5.get("method_counts", {}))

print(f"  EDM   : image-only (light scan)")
print(f"  incoming: image-only (light scan, different session)")
print(f"  pHash threshold forced to 0 → pHash always misses")
print(f"  result   : {route5}")
print(f"  methods  : {mc5}  tier1_hit={meta5.get('tier1_hit')}")
for pg, det in sorted(pd5.items()):
    print(f"    page {pg}: method={det.get('method')}  score={det.get('score', '?')}")

print(f"""
{'='*62}
SUMMARY — "both sides image-only" behaviour
{'='*62}
  pHash catches it    → REJECTED (conf=LOW for 2-page, MEDIUM for 7-page)
  pHash misses (heavy disturbance, diff > 10):
    OCR fires on both sides via Tesseract
    If OCR similarity ≥ 85% → method=OCR recorded
    Routing: TEXT/OCR-only → CLEAN-UNCHECKED
             (never auto-rejected without hash evidence)
  If OCR also fails (text too degraded):
    → CLEAN  (treated as unrecognisable — human must review)

Design rationale:
  The system won't reject a document without confident visual hash
  evidence. OCR output from two different scans of the same page is
  rarely character-for-character identical (different noise, micro-
  rotation, JPEG artefacts all affect OCR output). Routing to
  CLEAN-UNCHECKED instead of REJECTED prevents false rejections of
  legitimate originals that happen to look similar in text.
""")
