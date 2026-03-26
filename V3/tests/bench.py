#!/usr/bin/env python3
"""
V3/tests/bench.py — AWB Pipeline benchmark script.

Usage:
    python -m V3.tests.bench <pdf_dir> [--long-pass]

Arguments:
    pdf_dir      Directory containing test PDFs (non-recursive, *.pdf).
    --long-pass  Run full pipeline (allow_long_pass=True).
                 Default is fast-lane only (allow_long_pass=False).

IMPORTANT: process_pdf() moves files to CLEAN/NEEDS_REVIEW/etc.
Run against a COPY of your test PDFs, not the originals.

Output:
    Per-file table:  filename | result | total_ms | ocr_main_ms | rotation_ms | tess_calls
    Aggregate:       p50 / p95 / avg of total_active_ms
"""
from __future__ import annotations

import argparse
import statistics
import sys
import time
from pathlib import Path

# Ensure project root is on sys.path regardless of CWD
_HERE = Path(__file__).resolve().parent   # V3/tests/
_ROOT = _HERE.parent.parent               # ScavGajjar21/
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from V3 import config                                          # noqa: E402
from V3.core.file_ops import load_awb_set_from_excel, build_buckets  # noqa: E402
from V3.core import ocr_engine                                 # noqa: E402
from V3.stages.pipeline import process_pdf                     # noqa: E402


# ---------------------------------------------------------------------------
# Column layout
# ---------------------------------------------------------------------------
_CW = (40, 14, 10, 14, 12, 11)
_HEADER = (
    f"{'filename':<{_CW[0]}} "
    f"{'result':<{_CW[1]}} "
    f"{'total_ms':>{_CW[2]}} "
    f"{'ocr_main_ms':>{_CW[3]}} "
    f"{'rotation_ms':>{_CW[4]}} "
    f"{'tess_calls':>{_CW[5]}}"
)
_SEP = "-" * (sum(_CW) + len(_CW))


def _row(fname: str, result: str, total_ms: float,
         ocr_main_ms: float, rotation_ms: float, tess_calls: int) -> str:
    return (
        f"{fname[:_CW[0]]:<{_CW[0]}} "
        f"{result:<{_CW[1]}} "
        f"{total_ms:>{_CW[2]}.0f} "
        f"{ocr_main_ms:>{_CW[3]}.0f} "
        f"{rotation_ms:>{_CW[4]}.0f} "
        f"{tess_calls:>{_CW[5]}}"
    )


def _pct(data: list[float], p: float) -> float:
    if not data:
        return 0.0
    s = sorted(data)
    k = (len(s) - 1) * p / 100
    lo, hi = int(k), min(int(k) + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (k - lo)


# ---------------------------------------------------------------------------
# Log parsing — read the [TIMING] line written by finalize() for this file
# ---------------------------------------------------------------------------

def _parse_timing_line(log_path: Path, byte_offset: int) -> dict[str, float]:
    """Return key→float pairs from the first [TIMING] line after byte_offset."""
    result: dict[str, float] = {}
    if not log_path.exists():
        return result
    try:
        with log_path.open("r", encoding="utf-8", errors="replace") as fh:
            fh.seek(byte_offset)
            for line in fh:
                if "[TIMING]" not in line:
                    continue
                for tok in line.split():
                    if "=" in tok:
                        k, _, v = tok.partition("=")
                        try:
                            result[k] = float(v)
                        except ValueError:
                            pass
                break  # only the first [TIMING] line
    except Exception:
        pass
    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(description="AWB Pipeline benchmark")
    ap.add_argument("pdf_dir", help="Directory of test PDFs")
    ap.add_argument(
        "--long-pass",
        dest="long_pass",
        action="store_true",
        default=False,
        help="Full pipeline (allow_long_pass=True); default is fast-lane only",
    )
    ap.add_argument(
        "--no-edm",
        dest="no_edm",
        action="store_true",
        default=False,
        help="Disable Stage 6 EDM API fallback for this run (patches config at runtime)",
    )
    ap.add_argument(
        "--timeout",
        dest="timeout",
        type=float,
        default=None,
        help="Per-file timeout in seconds for long-pass (mirrors LONG_PASS_TIMEOUT_SECONDS)",
    )
    args = ap.parse_args()

    if args.no_edm:
        config.ENABLE_EDM_FALLBACK = False
        print("[bench] EDM fallback DISABLED for this run")

    pdf_dir = Path(args.pdf_dir).resolve()
    if not pdf_dir.is_dir():
        print(f"[bench] ERROR: {pdf_dir} is not a directory", file=sys.stderr)
        sys.exit(1)

    pdfs = sorted(pdf_dir.glob("*.pdf")) + sorted(pdf_dir.glob("*.PDF"))
    pdfs = list(dict.fromkeys(pdfs))  # deduplicate while preserving sort order
    if not pdfs:
        print(f"[bench] No PDFs found in {pdf_dir}", file=sys.stderr)
        sys.exit(1)

    # Load AWB master set
    excel_path = config.AWB_EXCEL_PATH
    if excel_path.exists():
        awb_set = load_awb_set_from_excel(excel_path)
        by_prefix, by_suffix = build_buckets(awb_set)
        print(f"[bench] Loaded {len(awb_set):,} AWBs from {excel_path}")
    else:
        print(f"[bench] WARNING: AWB Excel not found at {excel_path} — using empty set")
        awb_set = set()
        by_prefix, by_suffix = {}, {}

    mode = "long-pass (full pipeline)" if args.long_pass else "fast-lane only"
    print(f"[bench] Mode       : {mode}")
    print(f"[bench] PDFs       : {len(pdfs)} files in {pdf_dir}")
    print()
    print(_HEADER)
    print(_SEP)

    log_path = config.PIPELINE_LOG
    total_ms_all: list[float] = []
    results: list[tuple] = []

    for pdf_path in pdfs:
        fname = pdf_path.name

        # Record log offset before the call so we read only new bytes
        log_offset = log_path.stat().st_size if log_path.exists() else 0

        ocr_engine.reset_call_count()
        wall_start = time.perf_counter()

        _timeout = args.timeout if args.long_pass else None
        result = process_pdf(
            str(pdf_path),
            awb_set,
            by_prefix,
            by_suffix,
            allow_long_pass=args.long_pass,
            timeout_seconds=_timeout,
        )

        wall_ms = (time.perf_counter() - wall_start) * 1000
        tess_calls = ocr_engine.get_call_count()
        psm_counts = ocr_engine.get_psm_counts()

        # Parse sub-timings from the [TIMING] log line
        timing = _parse_timing_line(log_path, log_offset)
        total_ms    = timing.get("total_active_ms", wall_ms)
        ocr_main_ms = timing.get("ocr_main_ms", 0.0)
        rotation_ms = timing.get("rotation_ms", 0.0)

        total_ms_all.append(total_ms)
        results.append((fname, result, total_ms, ocr_main_ms, rotation_ms, tess_calls, psm_counts))
        print(_row(fname, result, total_ms, ocr_main_ms, rotation_ms, tess_calls))

    # Aggregates
    print()
    if total_ms_all:
        p50 = _pct(total_ms_all, 50)
        p95 = _pct(total_ms_all, 95)
        avg = statistics.mean(total_ms_all)
        total_tess = sum(r[5] for r in results)
        matched    = sum(1 for r in results if r[1] == "MATCHED")
        deferred   = sum(1 for r in results if "DEFERRED" in r[1])
        review     = sum(1 for r in results if r[1] == "NEEDS_REVIEW")

        # Aggregate PSM breakdown across all files
        agg_psm: dict[str, int] = {}
        for r in results:
            for k, v in r[6].items():
                agg_psm[k] = agg_psm.get(k, 0) + v

        print(f"[bench] Files processed : {len(total_ms_all)}")
        print(f"[bench] MATCHED         : {matched}")
        print(f"[bench] NEEDS_REVIEW    : {review}")
        print(f"[bench] DEFERRED*       : {deferred}")
        print(f"[bench] total_ms  p50   : {p50:.0f} ms")
        print(f"[bench] total_ms  p95   : {p95:.0f} ms")
        print(f"[bench] total_ms  avg   : {avg:.0f} ms")
        print(f"[bench] tess_calls total: {total_tess}")
        print(f"[bench] tess_calls avg  : {total_tess / len(total_ms_all):.1f}")
        if agg_psm:
            print(f"[bench] PSM breakdown   :")
            for psm_key in sorted(agg_psm):
                pct = agg_psm[psm_key] / total_tess * 100 if total_tess else 0
                print(f"[bench]   {psm_key:<14}: {agg_psm[psm_key]:>4}  ({pct:.1f}%)")


if __name__ == "__main__":
    main()
