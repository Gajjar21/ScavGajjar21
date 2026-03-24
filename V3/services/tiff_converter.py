# V3/services/tiff_converter.py
# PDF to TIFF converter — IMPROVED version with parallel processing.
#
# Changes vs Scripts/pdf_to_tiff_batch.py:
#   - Parallel conversion via ThreadPoolExecutor (config.TIFF_PARALLEL_WORKERS)
#   - Streaming page approach: one page in memory at a time (low peak memory)
#   - Proper cleanup of temp files (even on error)
#   - Skip-if-exists support
#   - Centralized audit via V3.audit.tracker.write_batch_event()
#
# All paths and settings come from V3.config.

from __future__ import annotations

import sys
import tempfile
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from V3 import config
from V3.audit.logger import audit_event
from V3.audit.tracker import write_batch_event

try:
    import pymupdf as fitz  # PyMuPDF ≥ 1.24 preferred namespace
except ImportError:
    try:
        import fitz  # type: ignore[no-redef]
        fitz.open  # verify it's real PyMuPDF, not the stub package
    except (ImportError, AttributeError) as exc:
        raise RuntimeError(
            "PyMuPDF import failed. Install PyMuPDF and remove conflicting 'fitz' package."
        ) from exc

from PIL import Image

# ── Config aliases ────────────────────────────────────────────────────────────
INPUT_DIR        = config.PENDING_PRINT_DIR
OUTPUT_DIR       = config.PENDING_PRINT_DIR   # output alongside input (same folder)
DPI              = config.TIFF_DPI
TIFF_COMPRESSION = config.TIFF_COMPRESSION
GRAYSCALE        = config.TIFF_GRAYSCALE
SKIP_IF_EXISTS   = config.TIFF_SKIP_IF_EXISTS
PARALLEL_WORKERS = config.TIFF_PARALLEL_WORKERS


def pdf_to_multipage_tiff(pdf_path: Path, tiff_path: Path) -> int:
    """Convert *pdf_path* to a multi-page TIFF at *tiff_path*.

    Streaming approach: each page is rendered and written to a temporary
    single-page TIFF, then assembled into a multi-page TIFF via Pillow
    append mode.  Peak memory = one rendered page, not the full document.

    Returns the number of pages converted.
    """
    doc = fitz.open(str(pdf_path))
    if doc.page_count == 0:
        doc.close()
        raise RuntimeError("PDF has 0 pages")

    zoom    = DPI / 72.0
    mat     = fitz.Matrix(zoom, zoom)
    tmp_dir = Path(tempfile.mkdtemp())

    # Initialise all tracked state before the try block so the finally
    # can always reference them without a NameError.
    tmp_files:  list[Path]  = []
    first_img:  Image.Image | None = None
    rest_imgs:  list[Image.Image]  = []
    page_count = doc.page_count

    try:
        for i in range(page_count):
            page = doc.load_page(i)
            pix  = page.get_pixmap(matrix=mat, alpha=False)
            img  = Image.frombytes("RGB", (pix.width, pix.height), pix.samples)
            if GRAYSCALE:
                img = img.convert("L")

            tmp_path = tmp_dir / f"page_{i:04d}.tif"
            save_kw: dict = {}
            if TIFF_COMPRESSION:
                save_kw["compression"] = TIFF_COMPRESSION
            img.save(str(tmp_path), **save_kw)
            img.close()
            tmp_files.append(tmp_path)

        if not tmp_files:
            raise RuntimeError("No pages rendered")

        first_img = Image.open(str(tmp_files[0]))
        rest_imgs = [Image.open(str(p)) for p in tmp_files[1:]]

        save_kw = {"save_all": True, "append_images": rest_imgs}
        if TIFF_COMPRESSION:
            save_kw["compression"] = TIFF_COMPRESSION
        first_img.save(str(tiff_path), **save_kw)

        return page_count

    except Exception:
        # Remove partial output TIFF so it can't be mistaken for a complete file
        try:
            if tiff_path.exists():
                tiff_path.unlink()
        except Exception:
            pass
        raise

    finally:
        # Close PDF
        try:
            doc.close()
        except Exception:
            pass
        # Close Pillow handles BEFORE unlinking temp files — critical on Windows
        # where an open handle prevents deletion.
        if first_img is not None:
            try:
                first_img.close()
            except Exception:
                pass
        for img in rest_imgs:
            try:
                img.close()
            except Exception:
                pass
        # Delete temp single-page TIFFs and the temp directory
        for p in tmp_files:
            try:
                p.unlink()
            except Exception:
                pass
        try:
            tmp_dir.rmdir()
        except Exception:
            pass


def _convert_one(pdf_path: Path) -> tuple[str, str, int]:
    """Convert a single PDF to TIFF.  Returns ``(name, status, pages)``."""
    tiff_path = OUTPUT_DIR / (pdf_path.stem + ".tiff")

    if SKIP_IF_EXISTS and tiff_path.exists():
        if tiff_path.stat().st_size > 1024:
            return (pdf_path.name, "SKIP", 0)
        print(f"  [WARN] Existing TIFF {tiff_path.name} appears corrupt "
              f"(size={tiff_path.stat().st_size}), re-converting")
        tiff_path.unlink()

    try:
        pages = pdf_to_multipage_tiff(pdf_path, tiff_path)
        audit_event("TIFF_CONVERT", file=pdf_path.name, status="OK", pages=pages)
        try:
            write_batch_event(
                event_type="TIFF_CONVERTED",
                filename=tiff_path.name,
                page_count=pages,
                output_path=str(tiff_path),
            )
        except Exception:
            pass
        return (pdf_path.name, "OK", pages)
    except Exception as e:
        audit_event("TIFF_CONVERT", file=pdf_path.name, status="FAIL", reason=str(e))
        try:
            write_batch_event(
                event_type="TIFF_FAILED",
                filename=pdf_path.name,
                notes=str(e),
            )
        except Exception:
            pass
        return (pdf_path.name, f"FAIL: {e}", 0)


def main() -> None:
    config.ensure_dirs()

    if not INPUT_DIR.is_dir():
        print(f"ERROR: Folder not found: {INPUT_DIR}")
        sys.exit(1)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    pdf_files = sorted(
        f for f in INPUT_DIR.iterdir() if f.suffix.lower() == ".pdf"
    )
    if not pdf_files:
        print(f"No PDFs found in: {INPUT_DIR}")
        return

    workers = max(1, PARALLEL_WORKERS)
    print(f"Found {len(pdf_files)} PDF(s). Converting to TIFF...")
    print(f"  DPI:         {DPI}")
    print(f"  Compression: {TIFF_COMPRESSION or 'none'}")
    print(f"  Grayscale:   {GRAYSCALE}")
    print(f"  Workers:     {workers}")
    print()

    converted = skipped = failed = 0

    if workers == 1:
        # Sequential path — simpler, avoids thread overhead for small jobs
        for pdf_path in pdf_files:
            name, status, pages = _convert_one(pdf_path)
            if status == "SKIP":
                print(f"SKIP (exists): {name}")
                skipped += 1
            elif status == "OK":
                print(f"OK:   {name} ({pages} pages)")
                converted += 1
            else:
                print(f"FAIL: {name} | {status}")
                failed += 1
    else:
        # Parallel path — ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=workers) as executor:
            future_to_name = {
                executor.submit(_convert_one, pdf_path): pdf_path.name
                for pdf_path in pdf_files
            }
            for future in as_completed(future_to_name):
                try:
                    name, status, pages = future.result()
                except Exception as e:
                    name = future_to_name[future]
                    status = f"FAIL: {e}"
                    pages = 0

                if status == "SKIP":
                    print(f"SKIP (exists): {name}")
                    skipped += 1
                elif status == "OK":
                    print(f"OK:   {name} ({pages} pages)")
                    converted += 1
                else:
                    print(f"FAIL: {name} | {status}")
                    failed += 1

    print("\nDone.")
    print(f"Converted: {converted}")
    print(f"Skipped:   {skipped}")
    print(f"Failed:    {failed}")

    audit_event(
        "TIFF_BATCH_SUMMARY",
        converted=converted,
        skipped=skipped,
        failed=failed,
        total=len(pdf_files),
        workers=workers,
    )


if __name__ == "__main__":
    main()
