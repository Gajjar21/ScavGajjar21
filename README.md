# Document Processing Pipeline V3

Automated FedEx document-processing workflow for extracting Air Waybill (AWB) numbers, screening duplicates, building print-ready batches, and producing scanner-ready TIFF output from a local desktop control centre.

The system is built around a watched `INBOX` folder. PDFs can arrive from multiple sources, then move through AWB extraction, EDM-aware duplicate screening, batch assembly, TIFF conversion, and audit logging.

## Start Here: Visual Walkthrough

**First-time repo viewers should start with the visual process walkthrough:**

[Open the rendered HTML walkthrough](https://htmlpreview.github.io/?https://github.com/Gajjar21/ScavGajjar21/blob/main/project-manifest.html)

This page is the fastest way to understand the project end to end. It explains the old manual workflow, the automated document-processing flow, AWB extraction stages, EDM duplicate screening, batch/TIFF output, runtime controls, architecture, risk handling, and current process snapshot.

Repository copy:

- [`project-manifest.html`](project-manifest.html) — source file in this repo
- Best local view: open `project-manifest.html` directly in a browser

## What It Does

- Watches `pdf_organizer/INBOX` for incoming PDFs.
- Extracts and validates 12-digit AWB numbers using filename patterns, embedded text, OCR, rotation handling, and rescue passes.
- Routes matched files to `PROCESSED` with collision-safe naming.
- Uses EDM runtime checks for persistent AWB fallback and downstream duplicate screening.
- Routes downstream documents to `CLEAN`, `REJECTED`, or safe bypass paths.
- Builds barcode cover pages and print-stack PDFs from `CLEAN`.
- Converts batch PDFs to multi-page TIFFs in `PENDING_PRINT`.
- Records operational history in logs, CSV/Excel outputs, and structured audit files.

## Quick Start

```bash
# Mac
./Install_Mac.command

# Windows
Install_Windows.bat

# Windows direct launcher
Run_GJ21_Launcher_Windows.bat
```

Windows install notes:

1. Install Python 3.11+ first and tick **Add Python to PATH**.
2. Double-click `Install_Windows.bat` from the project folder.
3. Let the installer create `.venv`, install `requirements.txt`, check/install Tesseract OCR, and write `.env`.
4. Add your EDM token to `.env` only when you are ready to enable EDM checks.
5. Launch with the Desktop `AWB Pipeline` shortcut or `Run_GJ21_Launcher_Windows.bat`.

Manual setup:

```bash
python3 -m venv .venv
source .venv/bin/activate          # Mac/Linux
# .venv\Scripts\activate           # Windows

pip install -r V3/requirements.txt
cp .env.example .env               # then edit local paths/tokens
# copy .env.example .env           # Windows equivalent
python -m V3.config                # verify paths
python -m V3.launcher              # branded launcher + UI
```

System dependency: **Tesseract OCR**.

## End-to-End Flow

```text
PDFs from multiple sources
  v
pdf_organizer/INBOX
  |  Hotfolder Scheduler
  |    Fast lane   : Stages 0-3 + strict post-Stage-3 ProbeLite
  |    Long pass   : full pipeline on deferred files with timeout budget
  |    Third pass  : resume timeout-deferred files with captured state
  v
pdf_organizer/PROCESSED
  |  EDM duplicate checker
  |    -> CLEAN
  |    -> REJECTED
  |    -> CLEAN-UNCHECKED / safe bypass behavior
  v
pdf_organizer/CLEAN
  |  Batch Builder
  v
data/OUT/PRINT_STACK_BATCH_*.pdf
  |  Copy to PENDING_PRINT + TIFF conversion
  v
pdf_organizer/PENDING_PRINT/*.pdf + *.tiff
```

Files that cannot be matched or safely processed are moved to `pdf_organizer/NEEDS_REVIEW`.

## Pipeline Stages

| Stage | Name | Summary |
|---|---|---|
| 0 | Filename | Strict filename regex extraction plus AWB DB confirmation |
| 1 | Text Layer | Embedded-text extraction, metadata rotation fallback, keyword/context extraction |
| 2 | OCR Main | 320 DPI OCR with digit/general passes and tiered candidate extraction |
| 3 | OCR Strong | 420 DPI OCR at selected base angle |
| Fast-lane post-check | ProbeLite | Strict low-cost check after Stage 3 fail before defer |
| 3.1 | Rotation Probe | 140 DPI rotation scoring across 0/90/180/270 |
| 3.2 | Probe Exit | Reuses probe OCR text for zero-cost early match exit |
| 3.5 | ROI Crop | Targeted region crop/upscale OCR pass |
| 4 | Rotation Fallback | Deferred-angle OCR sequence and priority fallback sweep |
| 5 | Table/Context Rescue | Table-line cleanup and context rescue passes |
| 5.5 | Upscale Rescue | 3x upscale rescue pass |
| 5.6 | Airway Label Rescue | Targeted right-side crops with two-step OCR gating |
| 6 | EDM Persistence Fallback | Runtime-gated EDM AWB existence confirmation for a persistent HIGH candidate |
| 7 | Needs Review | Terminal no-match path with diagnostics |

Match priority: `Exact-High > Exact-Standard > Tolerance-High > Tolerance-Standard`.

## Desktop Control Centre

Primary controls:

- `Start AWB`: start/stop the hotfolder service.
- `EDM: ON/OFF`: runtime EDM fallback and duplicate-check toggle.
- `AUTO MODE`: unattended cycle from inbox drain to batch/TIFF output.
- `Full Cycle`: one supervised end-to-end run.
- `Upload Files`: copy selected PDFs into `INBOX`.

Operational controls:

- `Prepare Batch`: build print stacks from `CLEAN`.
- `Convert TIFF`: convert stack PDFs to TIFF.
- `Retry Failed`: move `NEEDS_REVIEW` PDFs back to `INBOX`.
- `Refresh DB`: force an AWB Excel reload trigger.
- `Clear All`: safely clear working files while protecting configured data.

## Project Structure

```text
ScavGajjar21/
├── V3/
│   ├── app.py
│   ├── config.py
│   ├── core/
│   ├── stages/
│   ├── services/
│   ├── audit/
│   └── ui/
├── data/
│   └── OUT/
├── docs/
│   └── OPERATIONS.md
├── logs/
├── pdf_organizer/
│   ├── INBOX/
│   ├── PROCESSED/
│   ├── CLEAN/
│   ├── REJECTED/
│   ├── NEEDS_REVIEW/
│   └── PENDING_PRINT/
├── project-manifest.html
├── README.md
├── CHANGELOG.md
└── .env.example
```

## Configuration

Create `.env` from `.env.example` and set local machine paths before running.

Core defaults:

| Variable | Default | Purpose |
|---|---:|---|
| `ENABLE_INBOX_TWO_PASS` | `true` | Enable fast-lane/long-pass scheduling |
| `LONG_PASS_TIMEOUT_SECONDS` | `45.0` | Long-pass timeout before third-pass defer |
| `OCR_DPI_MAIN` | `320` | Main OCR DPI |
| `OCR_DPI_STRONG` | `420` | Strong OCR DPI |
| `ROTATION_PROBE_DPI` | `140` | Low-cost rotation probe DPI |

EDM and duplicate-screening defaults:

| Variable | Default | Purpose |
|---|---:|---|
| `ENABLE_EDM_FALLBACK` | `false` | Default EDM state when runtime toggle is absent |
| `EDM_TIER1_INCOMING_PAGES` | `3` | Tier-1 incoming probe pages |
| `EDM_TIER1_EDM_PAGE_LIMIT` | `5` | Tier-1 pages per EDM document |
| `EDM_TIER2_EDM_PAGE_LIMIT` | `10` | Tier-2 full-compare page limit |
| `TEXT_SIMILARITY_THRESHOLD` | `60` | Track text/OCR duplicate similarity |
| `TEXT_STRONG_THRESHOLD` | `85` | Promote text/OCR similarity to strong evidence |

Batch and TIFF defaults:

| Variable | Default | Purpose |
|---|---:|---|
| `MAX_PAGES_PER_BATCH` | `48` | Max pages per print stack |
| `MIN_CLEAN_BATCHES_FOR_AUTO` | `2` | AUTO MODE minimum estimated batches |
| `TIFF_DPI` | `200` | Output TIFF DPI |
| `TIFF_COMPRESSION` | `tiff_lzw` | TIFF compression |

Run:

```bash
python -m V3.config
```

## Data And Logs

| Path | Purpose |
|---|---|
| `data/AWB_dB.xlsx` | Local AWB database workbook |
| `data/AWB_Logs.xlsx` | AWB detection log workbook |
| `data/stage_cache.csv` | Stage/method cache for downstream tiering |
| `data/edm_awb_exists_cache.json` | Stage-6 EDM existence cache |
| `logs/pipeline.log` | Unified runtime pipeline log |
| `logs/edm_checker.log` | EDM duplicate checker log |
| `logs/pipeline_audit.jsonl` | Structured audit event stream |

Runtime data, tokens, local `.env` files, and generated outputs should not be committed.

## Documentation

- `docs/OPERATIONS.md`: operator runbook and incident handling.
- `project-manifest.html`: visual architecture and process walkthrough.
- `CHANGELOG.md`: release history.
- `CONTRIBUTING.md`: local setup and PR checklist.
- `SECURITY.md`: private vulnerability reporting guidance.

## Legacy Compatibility

`main.py` remains a compatibility shim forwarding to the V3 application.

Launcher helpers are kept for operator convenience on Mac and Windows. They are intentionally separate from the pipeline logic.
