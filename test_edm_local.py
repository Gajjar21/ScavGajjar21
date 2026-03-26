#!/usr/bin/env python3
"""
Local EDM duplicate detection tests — no EDM API calls.
Uses real PDFs from ~/Downloads/attached/ as both incoming files and "EDM corpus".

Tests:
  1. Exact same file → REJECTED (HASH)
  2. Same AWB, (2) copy → REJECTED or NEEDS-REVIEW depending on scan similarity
  3. Completely different doc → CLEAN
  4. Partial overlap: 2-page doc vs 7-page multi-doc (first 2 pages pasted in)
  5. Threshold reject: 7-page doc vs itself → all dup → REJECTED
  6. Split scenario: mix first 3 pages of one doc + last 4 of another
  7. Text-only fallback: rotated scan variant (if different bytes but same text)
"""

import sys
import os
import io
import hashlib

# Add project root to path
sys.path.insert(0, "/Users/gajjar/Desktop/ScavGajjar21")

import fitz  # PyMuPDF

from V3.services.edm_duplicate_checker import find_duplicate_pages, build_edm_fingerprints

ATTACHED = "/Users/gajjar/Downloads/attached"

# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def pdf_bytes(path: str) -> bytes:
    with open(path, "rb") as f:
        return f.read()

def make_pdf_from_pages(source_paths_and_ranges: list) -> bytes:
    """
    Build a PDF by concatenating specific pages from multiple source files.
    source_paths_and_ranges: list of (path, page_indices) tuples.
      e.g. [("a.pdf", [0, 1]), ("b.pdf", [2, 3, 4])]
    """
    out = fitz.open()
    for path, indices in source_paths_and_ranges:
        src = fitz.open(path)
        for idx in indices:
            out.insert_pdf(src, from_page=idx, to_page=idx)
        src.close()
    buf = io.BytesIO()
    out.save(buf)
    buf.seek(0)
    return buf.read()

def routing_label(dup_pages: set, total: int, page_details: dict) -> str:
    """Replicate the core routing logic for display."""
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
    has_strong = dup_count > 0

    if not has_strong and (t + o) == 0:
        return "CLEAN"
    if not has_strong:
        return "CLEAN-UNCHECKED  (text/OCR only, no hash evidence)"

    dup_ratio = dup_count / total if total else 0

    # Confidence
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

    reject_if_pages = getattr(__import__("V3.config", fromlist=["config"]), "EDM_REJECT_IF_DUP_PAGES_OVER", 5)
    reject_if_ratio = getattr(__import__("V3.config", fromlist=["config"]), "EDM_REJECT_IF_DUP_RATIO", 0.70)

    if dup_count == total:
        if conf == "LOW":
            return "NEEDS-REVIEW"
        return "REJECTED  (all pages dup)"

    if dup_count > reject_if_pages and dup_ratio >= reject_if_ratio and conf != "LOW":
        return f"REJECTED  (threshold: {dup_count}/{total} pages = {dup_ratio:.0%})"

    if 0 < dup_count < total:
        return f"SPLIT  ({dup_count} dup / {total-dup_count} clean)"

    return "CLEAN"

def run_test(name: str, incoming_path: str, edm_bytes_list: list[bytes]) -> None:
    print(f"\n{'='*60}")
    print(f"TEST: {name}")
    print(f"  incoming : {os.path.basename(incoming_path)}")
    print(f"  EDM docs : {len(edm_bytes_list)} doc(s)")

    # Count pages
    inc_doc = fitz.open(incoming_path)
    total = inc_doc.page_count
    inc_doc.close()
    print(f"  pages    : {total} incoming page(s)")

    fps = build_edm_fingerprints(edm_bytes_list, hash_page_limit=-1, phash_page_limit=0, text_page_limit=0)
    dup_pages, meta = find_duplicate_pages(incoming_path, edm_bytes_list, edm_fingerprints=fps)

    page_details = meta.get("page_details", {})
    method_counts = meta.get("method_counts", {})

    route = routing_label(dup_pages, total, page_details)

    print(f"  result   : {route}")
    print(f"  dup_pages: {sorted(dup_pages)} (0-indexed)")
    print(f"  methods  : {dict(method_counts)}")
    if page_details:
        for pg_1based, det in sorted(page_details.items()):
            print(f"    page {pg_1based}: method={det.get('method')} score={det.get('score','?')}")
    print(f"  tier1_hit: {meta.get('tier1_hit')}")

# ─────────────────────────────────────────────────────────────────────────────
# TESTS
# ─────────────────────────────────────────────────────────────────────────────

P = lambda name: os.path.join(ATTACHED, name)

# ── Test 1: Exact duplicate (same file as both incoming and EDM) ─────────────
run_test(
    "1. Exact self-duplicate (473019035113.pdf vs itself)",
    incoming_path=P("473019035113.pdf"),
    edm_bytes_list=[pdf_bytes(P("473019035113.pdf"))],
)

# ── Test 2: (2) copy vs original ─────────────────────────────────────────────
run_test(
    "2. AWB copy vs original (473019035113 (2) vs 473019035113)",
    incoming_path=P("473019035113 (2).pdf"),
    edm_bytes_list=[pdf_bytes(P("473019035113.pdf"))],
)

# ── Test 3: Completely different doc ─────────────────────────────────────────
run_test(
    "3. Completely different doc (888528178007 vs 450204693630)",
    incoming_path=P("888528178007.pdf"),
    edm_bytes_list=[pdf_bytes(P("450204693630.pdf"))],
)

# ── Test 4: 7-page doc vs itself (all pages dup → REJECTED) ──────────────────
run_test(
    "4. 7-page vs itself → all dup REJECTED (888528178007)",
    incoming_path=P("888528178007.pdf"),
    edm_bytes_list=[pdf_bytes(P("888528178007.pdf"))],
)

# ── Test 5: Partial overlap — first 2 pages of 7-pager match, rest are new ───
# Construct incoming = first 2 pages of 888528178007 + 3 pages of unrelated doc
mixed_5pg = make_pdf_from_pages([
    (P("888528178007.pdf"), [0, 1]),          # first 2 pages (dup)
    (P("426048113568.pdf"), [0, 1, 2]),       # 3 completely different pages (clean)
])
# Save to a temp file (find_duplicate_pages needs a filepath)
import tempfile
tmp = tempfile.NamedTemporaryFile(suffix=".pdf", delete=False)
tmp.write(mixed_5pg)
tmp.close()

run_test(
    "5. 5-page mixed: first 2 from 888528178007 (dup) + 3 from 426048113568 (clean)\n   EDM corpus = 888528178007 only → SPLIT expected",
    incoming_path=tmp.name,
    edm_bytes_list=[pdf_bytes(P("888528178007.pdf"))],
)
os.unlink(tmp.name)

# ── Test 6: Threshold reject — 6+ dup pages ──────────────────────────────────
# incoming = all 7 pages of 888528178007 (same as EDM corpus)
# already covered by test 4, so let's do 7 dup + different 2 pages stacked
mixed_9pg = make_pdf_from_pages([
    (P("888528178007.pdf"), list(range(7))),   # 7 dup pages
    (P("426048113568.pdf"), [0, 1]),           # 2 clean pages
])
tmp2 = tempfile.NamedTemporaryFile(suffix=".pdf", delete=False)
tmp2.write(mixed_9pg)
tmp2.close()

run_test(
    "6. 9-page: 7 dup (888528178007) + 2 clean (426048113568)\n   dup_count=7>5 AND ratio=7/9=0.78≥0.70 → REJECTED (threshold)",
    incoming_path=tmp2.name,
    edm_bytes_list=[pdf_bytes(P("888528178007.pdf"))],
)
os.unlink(tmp2.name)

# ── Test 7: 6 dup / 4 clean → ratio 0.60 < 0.70 → SPLIT (not threshold) ─────
mixed_10pg = make_pdf_from_pages([
    (P("888528178007.pdf"), list(range(6))),   # 6 dup pages
    (P("450204693630.pdf"), [0, 1, 2, 3]),     # 4 clean pages
])
tmp3 = tempfile.NamedTemporaryFile(suffix=".pdf", delete=False)
tmp3.write(mixed_10pg)
tmp3.close()

run_test(
    "7. 10-page: 6 dup + 4 clean → ratio=0.60 < 0.70 → SPLIT (not threshold)",
    incoming_path=tmp3.name,
    edm_bytes_list=[pdf_bytes(P("888528178007.pdf"))],
)
os.unlink(tmp3.name)

# ── Test 8: 1-page true original (no match in EDM at all) ────────────────────
# Use one page of 426048113568 as incoming, corpus is 888528178007 (no overlap)
tmp4 = tempfile.NamedTemporaryFile(suffix=".pdf", delete=False)
tmp4.write(make_pdf_from_pages([(P("426048113568.pdf"), [0])]))
tmp4.close()

run_test(
    "8. 1-page true original vs unrelated EDM corpus → CLEAN",
    incoming_path=tmp4.name,
    edm_bytes_list=[pdf_bytes(P("888528178007.pdf"))],
)
os.unlink(tmp4.name)

# ── Test 9: Multiple EDM docs in corpus ──────────────────────────────────────
# incoming = 426048113568 page 0 (matches EDM doc B), EDM = [doc_A, doc_B]
tmp5 = tempfile.NamedTemporaryFile(suffix=".pdf", delete=False)
tmp5.write(make_pdf_from_pages([(P("426048113568.pdf"), [0])]))
tmp5.close()

run_test(
    "9. 1-page vs multi-doc EDM corpus (matches doc 2 of 2) → REJECTED",
    incoming_path=tmp5.name,
    edm_bytes_list=[
        pdf_bytes(P("888528178007.pdf")),   # EDM doc 1: no match
        pdf_bytes(P("426048113568.pdf")),   # EDM doc 2: exact match
    ],
)
os.unlink(tmp5.name)

print("\n" + "="*60)
print("All tests complete.")
