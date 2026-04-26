# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**Document Processing Pipeline V3** — An automated FedEx logistics document-processing system. It is a cross-platform (Mac/Windows) Tkinter desktop app that watches an inbox folder, runs a multi-stage OCR pipeline to extract 12-digit Air Waybill (AWB) numbers from PDFs, matches them against a master database, screens duplicates, and routes documents into workflow folders (`CLEAN`, `REJECTED`, `NEEDS_REVIEW`, `PENDING_PRINT`).

## Common Commands

```bash
# Run the application
make run
# or directly:
python -m V3.app

# Run the branded launcher
python -m V3.launcher

# Verify config and paths
make config
# or:
python -m V3.config

# Lint
make lint           # ruff on V3/

# Tests
make test           # pytest
make check          # lint + compile check + tests

# Setup
make setup          # create venv + install requirements.txt
make setup-dev      # setup with dev dependencies
```

## Architecture

### Entry Points
- `V3/launcher.py` — GJ21-branded splash screen launcher (user-facing)
- `V3/app.py` — Main UI entry point
- `V3/config.py` — Central config: reads `.env`, resolves cross-platform paths, auto-detects Tesseract

### Pipeline Flow

```
INBOX  →  Hotfolder (watchdog)
             ├─ Fast lane: Stages 0–3 + ProbeLite
             └─ Long lane: Full pipeline (45s timeout, state-captured for resume)
          ↓
       PROCESSED
          ↓
       EDM Duplicate Checker (hash → phash → text → OCR)
          ↓
       CLEAN / REJECTED / CLEAN-UNCHECKED
          ↓
       Batch Builder (cover pages + print stacks)
          ↓
       TIFF Converter
          ↓
       PENDING_PRINT
```

### Pipeline Stages (`V3/stages/pipeline.py`)
| Stage | Method |
|-------|--------|
| 0 | Filename regex |
| 1 | Text layer + keyword proximity |
| 2 | OCR Main (320 DPI) |
| 3 | OCR Strong (420 DPI) |
| 3.1 | Rotation Probe (140 DPI) — result captured for timeout resume |
| 3.2 | Probe-text early exit (zero-cost reuse of probe OCR output) |
| 3.5 | ROI Crop |
| 4 | Rotation Fallback (probe pre-check at each angle before DPI_STRONG OCR) |
| 5 | Table/Context Rescue (PSM3) |
| 5.5 | Upscale Rescue (3×) |
| 5.6 | Airway Label Rescue |
| 6 | EDM API Persistence Fallback |
| 7 | Needs Review (terminal) |

### Route Execution Order (after Stage 3.5)
| Route | Order |
|-------|-------|
| UPRIGHT | upscale → table → rotation (rotation skipped if stable non-DB HIGH pool) |
| ROTATED CERTAIN | upscale → table → rotation |
| ROTATED LIKELY | rotation → table → upscale |
| **ROTATED UNCERTAIN** | **table → rotation → upscale** |

ROTATED UNCERTAIN puts table (PSM3) first because the probe margin is low and table-layout docs (AWB buried in table structure) are found in 2-8s vs 44s+ of rotation. `_table_pass_ran` one-shot guard prevents double-running table.

### Key Modules
| Path | Role |
|------|------|
| `V3/core/awb_extractor.py` | Regex patterns, tiered candidate extraction, keyword-adjacent mining |
| `V3/core/awb_matcher.py` | Hamming distance tolerance matching (1–2 digit), priority logic |
| `V3/core/ocr_engine.py` | PDF rendering, image preprocessing, Tesseract wrappers, spatial OCR |
| `V3/core/file_ops.py` | File I/O, Excel/CSV loaders and writers |
| `V3/services/hotfolder.py` | Watchdog inbox monitor, two-pass scheduler, processing loop |
| `V3/services/batch_builder.py` | Assembles print stacks with ReportLab cover pages |
| `V3/services/tiff_converter.py` | PDF → TIFF with DPI/compression config |
| `V3/services/edm_checker.py` | FedEx EDM API fallback (Stage 6) |
| `V3/services/edm_duplicate_checker.py` | Full duplicate detection service (73KB) |
| `V3/ui/app_window.py` | Tkinter UI (~4,400 lines): controls, live folder counts, log viewer |
| `V3/ui/theme.py` | Centralised colours, fonts, platform adjustments |
| `V3/audit/logger.py` | JSONL audit logger with 50 MB rotation |

### Data & Folders (runtime, not in git)
- `data/AWB_dB.xlsx` — Master AWB database (required)
- `data/token.txt` — FedEx EDM token (takes priority over `.env`)
- `pdf_organizer/INBOX/` → `PROCESSED/` → `CLEAN/` → `PENDING_PRINT/`
- `logs/pipeline.log`, `logs/pipeline_audit.jsonl`

## Configuration

Copy `.env.example` to `.env`. Key variables:

| Variable | Default | Purpose |
|----------|---------|---------|
| `PIPELINE_BASE_DIR` | project root when omitted | Root folder containing `pdf_organizer/` and `data/` |
| `TESSERACT_PATH` | auto-detected on Mac | Path to Tesseract binary |
| `EDM_TOKEN` | — | FedEx EDM API token |
| `ENABLE_EDM_FALLBACK` | false | Default Stage-6 EDM lookup state when runtime toggle is absent |
| `OCR_DPI_MAIN` / `OCR_DPI_STRONG` | 320 / 420 | Rendering DPI for OCR passes |
| `ENABLE_INBOX_TWO_PASS` | true | Fast+long two-pass scheduling |
| `LONG_PASS_TIMEOUT_SECONDS` | 45 | Combined fast+long budget; long-pass gets `max(45 - fast_proc, 10s)` |
| `THIRD_PASS_TIMEOUT_SECONDS` | 195 | Max budget for third-pass (capped by global timeout) |
| `GLOBAL_DOC_TIMEOUT_SECONDS` | 150 | Hard cap on total processing time per file across all passes |
| `LARGE_FILE_THRESHOLD_BYTES` | 1000000 | Files over 1 MB sorted to end of long-pass queue |
| `FASTLANE_IMAGE_ONLY_BUDGET_SECONDS` | 16.0 | Fast-lane OCR budget for image-only docs |
| `ROTATION_PROBE_CERTAIN_MARGIN` | 300 | Probe score margin to call rotation CERTAIN vs LIKELY/UNCERTAIN |
| `AUTO_INTERVAL_SEC` | 10 | Auto-mode polling interval |
| `MAX_PAGES_PER_BATCH` | 48 | Pages per print batch |
| `TIFF_DPI` / `TIFF_COMPRESSION` | 200 / tiff_lzw | TIFF output settings |

## Important Patterns

- **File stability**: hotfolder waits for file to stop growing before processing (guards against partial writes).
- **Timeout/resume**: long-pass files capture full pipeline state (probe scores, candidates, OCR cache, rotation angle) at timeout and resume in third-pass. `_build_timeout_state()` is the single helper used by both the exception path and the post-Stage-3.1 explicit gate.
- **Global proc-time tracking**: `_file_proc_seconds[path]` accumulates actual `process_pdf()` wall time only — queue wait is excluded. Third-pass budget = `min(THIRD_PASS_TIMEOUT_SECONDS, GLOBAL_DOC_TIMEOUT - proc_so_far)`.
- **Matching tiers**: `awb_matcher.py` applies Exact-High → Exact-Standard → Tolerance-High (1-digit) → Tolerance-Standard (2-digit); first tier that yields a confident match wins.
- **`_check_timeout()` fires at angle boundaries only**: it runs after all subpasses for a rotation angle complete, never mid-angle. Budget overruns of up to one full angle (~8-22s depending on file speed) are expected.
- **Cross-stage consensus**: `_stable_high_pool_no_db_match()` skips rotation for UPRIGHT docs where the same HIGH candidates (3+ stage hits) keep appearing but none are in the DB. Stage 5/5.5 still run.
- **Probe pre-check**: Stage 4 reuses `probe_texts[rot]` (already computed at 140 DPI) for 400-pattern, clean, and exact matching before spending on DPI_STRONG OCR at each angle.
- **EDM duplicate gate**: `edm_duplicate_checker.py` applies four strategies in order (exact hash, perceptual hash, text similarity, OCR comparison). A `CLEAN-UNCHECKED` bucket exists for safe fallback when the EDM service is unavailable.
- **Cross-platform paths**: `config.py` normalises Windows backslash paths read from `.env`; always use `Path` objects in new code.
- **Log rotation**: `pipeline.log` rotates at 50 MB (2 backups); `edm_checker.log` at 20 MB (1 backup).
- **Probe skip for upright image-only docs** (commit `aa1cffe`): `_run_fastlane_quick_rotated_psm6` returns immediately when `rot is None` (no angle-detection hint), skipping `rotation_probe_best()`. `_run_early_rotation_route_probe_strict` also returns immediately when `_rotation_hint is None`. `_run_micro_probe_exact_only` only adds the 90° secondary pass when `_rotation_hint is not None`. Saves 7–19s per upright image-only doc. Rotated docs missed by angle detection fall to Stage 3.1 + Stage 4 in the long-pass.

## Performance Benchmarks (2026-03-26, 100-doc batch, commit `aa1cffe`)

### Fast-lane upright image-only docs (OCR-Main Stage 2 match)
| Before avg total | After avg total | Saved |
|-----------------|-----------------|-------|
| ~13,000ms | ~3,200ms | **~9,800ms (~10s per file)** |

Representative files:
| File | Before | After | Saved |
|------|--------|-------|-------|
| 20260317155404720.pdf | 18,720ms | 3,085ms | -15,635ms |
| 20260317160223997.pdf | 18,110ms | 4,542ms | -13,568ms |
| 20260317155736239.pdf | 22,361ms | 9,976ms | -12,385ms |
| 20260317155845241.pdf | 14,184ms | 3,086ms | -11,098ms |
| 20260317155113552.pdf | 14,534ms | 3,246ms | -11,288ms |
| 10A.pdf | 13,949ms | 2,994ms | -10,955ms |

### ProbeMicro early-lane docs
| File | Before | After | Saved |
|------|--------|-------|-------|
| 20260317155001448.pdf | 12,059ms | 1,458ms | -10,601ms |
| z.pdf / q.pdf / 5A.pdf / g.pdf | ~11,000ms | ~1,200ms | ~-9,800ms |

### Known regressions (rotated docs where angle detection missed — still match correctly)
| File | Before | After | Method change |
|------|--------|-------|--------------|
| 6.pdf | 6,801ms | 24,364ms | FastQuickRot-PreStage2 → FastCertainRotRescue |
| 20260317160052993.pdf | 7,759ms | 21,004ms | FastQuickRot-PreStage2 → FastCertainRotRescue |
| 20260318114358685.pdf | 11,475ms | 36,856ms | FastQuickRot-PreStage2 → Budget path |

**Net across batch**: ~16 files × -10s = -160s saved vs 3 regressions × +19s = +56s lost → **~104s net saved per 100-doc batch.**

### Timing field reference (`[TIMING]` log line)
| Field | Meaning |
|-------|---------|
| `filename_ms` | Stage 0 filename regex |
| `text_layer_ms` | Stage 1 text-layer extraction |
| `pre_stage2_ms` | Wall time from pipeline start to Stage 2 entry (captures all pre-Stage 2 probe overhead) |
| `ocr_main_ms` | Stage 2 OCR at DPI_MAIN (320) |
| `ocr_strong_ms` | Stage 3 OCR at DPI_STRONG (420) |
| `ocr_context_ms` | Stage 5 table/context rescue |
| `rotation_ms` | Stage 4 rotation fallback |
| `total_active_ms` | Total wall time in `process_pdf()` |

Pre_stage2_ms baseline after fix: **~700ms** (text-layer docs, probes skip instantly) · **~1,500–2,700ms** (upright image-only, no rotation hint) · **~9,000–10,000ms** (image-only with angle-detection rotation hint, probes fire correctly).
