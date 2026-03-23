# AWB Pipeline V3

Automated Air Waybill (AWB) extraction and document processing pipeline with a cross-platform Tkinter control centre.

## Quick Start

```bash
# Mac
./Install_Mac.command

# Windows
Install_Windows.bat
```

Or manually:

```bash
python3 -m venv .venv
source .venv/bin/activate          # Mac/Linux
# .venv\\Scripts\\activate           # Windows

pip install -r V3/requirements.txt
python -m V3.config                # verify paths
python -m V3.app                   # launch UI
```

## End-to-End Flow

PDFs are dropped into **INBOX** and flow through an event-driven, watchdog-backed pipeline:

```text
INBOX
  |  Hotfolder Scheduler (two-pass)
  |    Fast lane   : Stages 0-3 + post-Stage-3 ProbeLite strict check
  |    Long pass   : full pipeline on deferred files (45s timeout budget)
  |    Third pass  : resume timeout-deferred files with captured state (no timeout)
  v
PROCESSED  (<AWB>.pdf with collision handling)
  |  EDM duplicate checker service
  |    -> CLEAN
  |    -> REJECTED
  |    -> CLEAN-UNCHECKED (safe bypass paths)
  v
CLEAN
  |  Batch Builder (cover pages + print stacks)
  v
data/OUT/PRINT_STACK_BATCH_*.pdf
  |  Copy to PENDING_PRINT + TIFF conversion
  v
PENDING_PRINT/*.pdf + *.tiff
```

Files that cannot be matched are moved to **NEEDS_REVIEW**.

## Pipeline Stages (AWB Extraction Engine)

| Stage | Name | Summary |
|---|---|---|
| 0 | Filename | Strict filename regex extraction + DB confirmation |
| 1 | Text Layer | Embedded-text extraction, rotation metadata fallback, keyword/context extraction |
| 2 | OCR Main (320 DPI) | Digits/general OCR passes with tiered candidate extraction |
| 3 | OCR Strong (420 DPI) | High-resolution OCR passes at selected base angle |
| Fast-lane post-check | ProbeLite | Strict low-cost check after Stage 3 fail (`400` or unique exact-high) before defer |
| 3.1 | Rotation Probe | Low-DPI (140) rotation scoring across 0/90/180/270 |
| 3.2 | Probe Exit | Zero-cost reuse of probe OCR text for early match exit |
| 3.5 | ROI Crop | Targeted region crop/upscale OCR pass |
| 4 | Rotation Fallback | Deferred-angle OCR sequence + full-priority fallback sweep |
| 5 | Table/Context Rescue | Table-line cleanup + context rescue passes |
| 5.5 | Upscale Rescue | 3x upscale rescue pass |
| 5.6 | Airway Label Rescue | Targeted right-side crops with two-step OCR gating |
| 6 | EDM Persistence Fallback | Runtime-gated EDM AWB existence confirmation for persistent HIGH candidate |
| 7 | Needs Review | Terminal no-match path with diagnostics |

**Match priority (high-level):** `Exact-High > Exact-Standard > Tolerance-High > Tolerance-Standard`

## Project Structure

```text
AWB_PIPELINE/
├── V3/
│   ├── app.py
│   ├── config.py
│   ├── core/
│   │   ├── awb_extractor.py
│   │   ├── awb_matcher.py
│   │   ├── file_ops.py
│   │   └── ocr_engine.py
│   ├── stages/
│   │   └── pipeline.py
│   ├── services/
│   │   ├── hotfolder.py
│   │   ├── edm_checker.py
│   │   ├── edm_duplicate_checker.py
│   │   ├── batch_builder.py
│   │   └── tiff_converter.py
│   ├── audit/
│   │   ├── logger.py
│   │   └── tracker.py
│   └── ui/
│       ├── app_window.py
│       └── theme.py
├── docs/OPERATIONS.md
├── awb_pipeline_fedex-2.html
└── pdf_organizer/
    ├── INBOX/
    ├── PROCESSED/
    ├── CLEAN/
    ├── REJECTED/
    ├── NEEDS_REVIEW/
    └── PENDING_PRINT/
```

## Configuration Highlights (`.env`)

### Core

| Variable | Default | Purpose |
|---|---:|---|
| `LONG_PASS_TIMEOUT_SECONDS` | `45.0` | Long-pass per-file timeout before third-pass defer |
| `ENABLE_INBOX_TWO_PASS` | `true` | Enable fast-lane/long-pass scheduling |
| `ROTATION_PROBE_DPI` | `140` | Low-cost rotation probe DPI |
| `OCR_DPI_MAIN` | `320` | Main OCR DPI |
| `OCR_DPI_STRONG` | `420` | Strong OCR DPI |

### EDM Fallback / Duplicate Screening

| Variable | Default | Purpose |
|---|---:|---|
| `ENABLE_EDM_FALLBACK` | `false` | Default EDM fallback state when no runtime override exists |
| `EDM_TIER1_INCOMING_PAGES` | `3` | Tier-1 probe seed pages |
| `EDM_TIER1_EDM_PAGE_LIMIT` | `5` | Tier-1 pages per EDM doc |
| `EDM_TIER2_EDM_PAGE_LIMIT` | `10` | Tier-2 full-compare pages per EDM doc |
| `EDM_TEXT_LAYER_MIN_CHARS` | `30` | Minimum embedded text length for text-layer compare |
| `EDM_OCR_WORKERS` | `2` | OCR prewarm worker count |
| `EDM_OCR_PARALLEL_MIN_TASKS` | `4` | Minimum pending OCR tasks before parallel prewarm |

### Batch / TIFF

| Variable | Default | Purpose |
|---|---:|---|
| `MAX_PAGES_PER_BATCH` | `48` | Max pages per print stack |
| `MIN_CLEAN_BATCHES_FOR_AUTO` | `2` | AUTO MODE minimum estimated batches |
| `TIFF_DPI` | `200` | Output TIFF DPI |
| `TIFF_COMPRESSION` | `tiff_lzw` | TIFF compression |

Run `python -m V3.config` to verify configuration and paths.

## UI Controls

### Primary
- `Start AWB`: start/stop hotfolder service.
- `AUTO MODE`: continuous unattended orchestration.
- `Full Cycle`: one complete controlled cycle.
- `Upload Files`: copy selected PDFs into INBOX.

### Secondary
- `Prepare Batch`: build print stacks from CLEAN.
- `Convert TIFF`: convert stack PDFs to TIFF.
- `Retry Failed`: move NEEDS_REVIEW PDFs back to INBOX.
- `EDM: ON/OFF`: runtime EDM fallback toggle (`data/edm_toggle.json`).
- `Clear All`: clear INBOX/OUT work files and restart AWB flow safely.

## Data and Logs

| File | Purpose |
|---|---|
| `data/AWB_dB.xlsx` | Master AWB DB |
| `data/AWB_Logs.xlsx` | AWB log workbook |
| `data/stage_cache.csv` | Stage/method cache for downstream logic |
| `data/edm_awb_exists_cache.json` | Stage-6 EDM existence cache |
| `logs/pipeline.log` | Unified runtime log |
| `logs/edm_checker.log` | EDM-specific service logging |
| `logs/pipeline_audit.jsonl` | Structured audit stream |

## Dependencies

Core runtime dependencies are managed via `V3/requirements.txt`.

System dependency: **Tesseract OCR** (installed by setup scripts or package manager).

## Legacy Compatibility

`main.py` remains a compatibility shim forwarding to `V3.app`.

## Documentation

- `docs/OPERATIONS.md`: operator runbook and incident handling
- `awb_pipeline_fedex-2.html`: visual architecture/process walkthrough
- `CHANGELOG.md`: release history
