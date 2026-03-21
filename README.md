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
# .venv\Scripts\activate           # Windows

pip install -r V3/requirements.txt
python -m V3.config                # verify paths
python -m V3.app                   # launch UI
```

## How It Works

PDFs are dropped into **INBOX** and flow through an automated pipeline:

```
INBOX
  |  AWB Hotfolder (watchdog-based, event-driven)
  |    Two-pass scheduling:
  |      Fast lane  -> Stages 0-3 (filename, text layer, OCR main/strong)
  |      Long lane  -> Full pipeline (rotation, ROI, table, upscale rescue)
  |      Third pass -> Resume timeout-deferred files with cached state
  v
PROCESSED  (renamed to <AWB>.pdf)
  |  V3 move step (EDM bypass)
  v
CLEAN
  |  Batch Builder (cover pages + merge into print stacks)
  v
data/OUT/PRINT_STACK_BATCH_*.pdf
  |  Copy to PENDING_PRINT
  v
PENDING_PRINT
  |  TIFF Converter (multi-page TIFF for print systems)
  v
Done
```

Files that cannot be matched are moved to **NEEDS_REVIEW** for manual handling.
Files that fail validation are moved to **REJECTED**.

## Pipeline Stages (per PDF)

| Stage | Name | What it does |
|-------|------|-------------|
| 0 | Filename | Strict regex match on the PDF filename |
| 1 | Text Layer | Extract embedded text, spatial word sort, 400-pattern + tiered extraction |
| 2 | OCR Main (320 DPI) | Digit-only and general OCR at standard resolution |
| 3 | OCR Strong (420 DPI) | Higher-DPI OCR with invert passes |
| 3.1 | Rotation Probe | Low-DPI keyword-scored rotation detection (0/90/180/270) |
| 3.5 | ROI Crop | Crop top 10-62% of page, upscale 2x, OCR |
| 4 | Rotation Passes | Full OCR at detected angle + fallback angles |
| 5 | Table Line Removal | Morphological line removal (cv2), then OCR |
| 5.5 | Upscale Rescue | 3x upscale of best image, OCR |
| 5.6 | Airway Label Rescue | Targeted crop regions (right-mid, upper-right), 3x upscale |
| 6 | EDM Fallback | Persistence check (disabled in V3 — stub only) |
| 7 | Needs Review | No match found after all stages |

**Matching priority cascade:** Exact-High > Exact-Standard > Tolerance-High > Tolerance-Standard

## Project Structure

```
AWB_PIPELINE/
├── V3/
│   ├── app.py                      # Entry point (python -m V3.app)
│   ├── config.py                   # Centralised config from .env
│   ├── requirements.txt            # Python dependencies
│   ├── core/
│   │   ├── awb_extractor.py        # AWB candidate extraction (regex, keywords, patterns)
│   │   ├── awb_matcher.py          # Hamming distance matching, priority cascade
│   │   ├── file_ops.py             # Logging, file moves, Excel AWB loading
│   │   └── ocr_engine.py           # Tesseract OCR, PDF rendering, preprocessing
│   ├── stages/
│   │   └── pipeline.py             # Multi-stage pipeline orchestrator (1700 lines)
│   ├── services/
│   │   ├── hotfolder.py            # Watchdog inbox monitor, two-pass scheduler
│   │   ├── batch_builder.py        # CLEAN -> print stack PDFs with barcode covers
│   │   └── tiff_converter.py       # PDF -> multi-page TIFF conversion
│   ├── audit/
│   │   ├── logger.py               # JSONL audit event logger
│   │   └── tracker.py              # Excel audit workbook (4-sheet dashboard)
│   └── ui/
│       ├── app_window.py           # Tkinter control centre (1330 lines)
│       └── theme.py                # Colours, fonts, log tags
├── .env                            # Local config (not committed)
├── .env.example                    # Template for .env
├── Install_Mac.command             # One-click Mac installer
├── Install_Windows.bat             # One-click Windows installer
├── Run_V3_Mac.command              # Alternative Mac launcher (bypasses pyenv)
├── main.py                         # Legacy compatibility shim -> V3.app
└── pdf_organizer/                  # Runtime folders (auto-created)
    ├── INBOX/
    ├── PROCESSED/
    ├── CLEAN/
    ├── REJECTED/
    ├── NEEDS_REVIEW/
    └── PENDING_PRINT/
```

## Configuration

All settings live in `.env` at the project root. Copy `.env.example` to get started.

### Required

| Variable | Description |
|----------|-------------|
| `PIPELINE_BASE_DIR` | Absolute path to the project root |
| `TESSERACT_PATH` | Path to the Tesseract OCR binary |

Both are set automatically by the installer scripts.

### OCR / Matching

| Variable | Default | Description |
|----------|---------|-------------|
| `OCR_DPI_MAIN` | 320 | DPI for standard OCR passes |
| `OCR_DPI_STRONG` | 420 | DPI for high-resolution OCR passes |
| `ENABLE_ROTATION_LAST_RESORT` | true | Try all 4 rotation angles as last resort |
| `LONG_PASS_TIMEOUT_SECONDS` | 45 | Per-file timeout for long-pass before third-pass defer |
| `ALLOW_1_DIGIT_TOLERANCE` | true | Allow 1-digit Hamming distance tolerance matching |
| `POLL_SECONDS` | 2 | Inbox polling interval |
| `EXCEL_REFRESH_SECONDS` | 30 | AWB database reload interval |

### Batch Builder

| Variable | Default | Description |
|----------|---------|-------------|
| `MAX_PAGES_PER_BATCH` | 48 | Max pages per print stack PDF |
| `COVER_PAGE_SIZE` | LETTER | Cover page paper size |
| `ENABLE_TIER_BATCHING` | false | Separate batches by confidence tier |

### TIFF Converter

| Variable | Default | Description |
|----------|---------|-------------|
| `TIFF_DPI` | 200 | Output TIFF resolution |
| `TIFF_COMPRESSION` | tiff_lzw | TIFF compression codec |
| `TIFF_GRAYSCALE` | true | Convert to grayscale |

### Auto Mode

| Variable | Default | Description |
|----------|---------|-------------|
| `AUTO_INTERVAL_SEC` | 10 | Seconds between auto-mode cycles |
| `MIN_CLEAN_BATCHES_FOR_AUTO` | 2 | Minimum estimated batches before auto-build triggers |
| `INBOX_EMPTY_STABLE_SECONDS` | 8 | How long inbox must stay empty before proceeding |

Run `python -m V3.config` to verify all settings.

## UI Controls

### Pipeline Group
- **Start AWB** — Start/stop the hotfolder watcher
- **Full Cycle Once** — One complete cycle: AWB -> move -> batch -> TIFF
- **AUTO MODE** — Continuous unattended loop with readiness checks

### Actions Group
- **Prepare Batch** — Build print stacks from CLEAN folder
- **Retry NEEDS_REVIEW** — Move review files back to INBOX for reprocessing
- **Upload Files** — Copy selected PDFs into INBOX
- **EDM: DISABLED** — EDM duplicate checker (disabled in V3)

### Maintenance Group
- **Clear All** — Stop scripts, clear INBOX + OUT working files (protected files untouched)
- **Clear Log** — Clear the log viewer

### Other
- **Refresh DB** — Signal the hotfolder to reload the AWB database immediately
- Folder buttons open INBOX, CLEAN, REJECTED, NEEDS_REVIEW, OUT, PENDING_PRINT

## Data Files

| File | Location | Purpose |
|------|----------|---------|
| `AWB_dB.xlsx` | `data/` | Master AWB database (protected, never deleted) |
| `AWB_Logs.xlsx` | `data/` | Legacy AWB match log (protected) |
| `pipeline_audit.xlsx` | `data/` | Unified 4-sheet audit workbook |
| `stage_cache.csv` | `data/` | Per-file detection method cache |
| `session.json` | `data/` | Employee login session |
| `token.txt` | `data/` | FedEx EDM API token |
| `pipeline.log` | `logs/` | Full pipeline log |
| `pipeline_audit.jsonl` | `logs/` | Structured audit events |

## Dependencies

```
PyMuPDF>=1.24.0          # PDF rendering and manipulation
Pillow>=10.0.0           # Image processing
pytesseract>=0.3.10      # Tesseract OCR wrapper
openpyxl>=3.1.0          # Excel read/write
python-dotenv>=1.0.0     # .env loading
requests>=2.31.0         # HTTP (EDM API, currently unused)
watchdog>=4.0.0          # Filesystem event monitoring
reportlab>=4.0.0         # Barcode cover page generation
opencv-python-headless>=4.8.0  # Table line removal, preprocessing
numpy>=1.24.0            # Array operations
ImageHash>=4.3.0         # Perceptual image hashing
```

System dependency: **Tesseract OCR** (installed by the installer scripts or via `brew install tesseract`).

## Cross-Platform Notes

- **Config auto-detection**: If `.env` contains a Windows path on Mac (or vice versa), `config.py` auto-detects and falls back to the project root. Tesseract is auto-located on Mac via Homebrew paths.
- **macOS Tk 8.5**: The legacy system Tk on macOS has dark-mode rendering issues. The UI detects Tk < 8.6 and uses fallback dialogs automatically.
- **Folder open**: Uses `open` (Mac), `os.startfile` (Windows), or `xdg-open` (Linux).

## Legacy Compatibility

`main.py` is a thin shim that forwards to `V3.app`. Older launchers calling `python main.py` continue to work.

## Development Workflow

```bash
# one-time
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt -r requirements-dev.txt

# health checks
make check
```

## Repo Management Files

- `CONTRIBUTING.md` — contribution workflow and PR checklist
- `SECURITY.md` — private vulnerability reporting policy
- `CODE_OF_CONDUCT.md` — collaboration standards
- `CHANGELOG.md` — release history
- `.github/workflows/ci.yml` — CI validation on push/PR
- `.pre-commit-config.yaml` — optional local quality hooks
