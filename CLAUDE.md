# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**AWB Pipeline V3** — An automated Air Waybill (AWB) extraction system for FedEx logistics documents. It is a cross-platform (Mac/Windows) Tkinter desktop app that watches an inbox folder, runs a multi-stage OCR pipeline to extract 12-digit AWB numbers from PDFs, matches them against a master database, and routes documents into workflow folders (CLEAN, REJECTED, NEEDS_REVIEW, PENDING_PRINT).

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
| `PIPELINE_BASE_DIR` | (required) | Root folder containing `pdf_organizer/` and `data/` |
| `TESSERACT_PATH` | auto-detected on Mac | Path to Tesseract binary |
| `EDM_TOKEN` | — | FedEx EDM API token |
| `ENABLE_EDM_FALLBACK` | true | Enable Stage-6 EDM lookup |
| `OCR_DPI_MAIN` / `OCR_DPI_STRONG` | 320 / 420 | Rendering DPI for OCR passes |
| `ENABLE_INBOX_TWO_PASS` | true | Fast+long two-pass scheduling |
| `LONG_PASS_TIMEOUT_SECONDS` | 65 | Combined fast+long budget; long-pass gets `max(65 - fast_proc, 10s)` |
| `THIRD_PASS_TIMEOUT_SECONDS` | 195 | Max budget for third-pass (capped by global timeout) |
| `GLOBAL_DOC_TIMEOUT_SECONDS` | 150 | Hard cap on total processing time per file across all passes |
| `LARGE_FILE_THRESHOLD_BYTES` | 1000000 | Files over 1 MB sorted to end of long-pass queue |
| `FASTLANE_IMAGE_ONLY_BUDGET_SECONDS` | 16.0 | Fast-lane OCR budget for image-only docs |
| `ROTATION_PROBE_CERTAIN_MARGIN` | 240 | Probe score margin to call rotation CERTAIN vs LIKELY/UNCERTAIN |
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
