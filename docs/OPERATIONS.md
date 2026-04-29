# Operations Runbook

This runbook is for operators running Document Processing Pipeline V3 from the desktop control centre.

## Daily Startup

1. Launch the app:
   - Preferred launcher: `python -m V3.launcher`
   - Direct app entry: `python -m V3.app`
2. Confirm `EDM: ON/OFF` is set to the intended runtime mode.
3. Start the hotfolder with `Start AWB` if it is not already running.
4. Verify the log shows:
   - AWB DB load count
   - two-pass scheduler active
   - long-pass timeout budget, normally `45s`
   - EDM status and token availability, when EDM is ON
5. Confirm folder baselines:
   - `pdf_organizer/INBOX`
   - `pdf_organizer/PROCESSED`
   - `pdf_organizer/CLEAN`
   - `pdf_organizer/REJECTED`
   - `pdf_organizer/NEEDS_REVIEW`
   - `pdf_organizer/PENDING_PRINT`

## Runtime Controls

| Control | Purpose |
|---|---|
| `Start AWB` | Start or stop the hotfolder scheduler. |
| `EDM: ON/OFF` | Runtime gate for EDM fallback and duplicate-screening calls. |
| `AUTO MODE` | Continuous unattended loop: INBOX drain, PROCESSED drain, batch, TIFF. |
| `Full Cycle` | One supervised end-to-end cycle. |
| `Upload Files` | Copy selected PDFs into `INBOX`. |
| `Prepare Batch` | Build print-stack PDFs from `CLEAN`. |
| `Convert TIFF` | Generate TIFF output from stack PDFs. |
| `Retry Failed` | Requeue `NEEDS_REVIEW` PDFs into `INBOX`. |
| `Refresh DB` | Force an AWB Excel reload trigger. |
| `Clear All` | Stop active work and clear operational working outputs safely. |

## Normal Processing Expectations

- Filename and text-layer matches usually complete in sub-second to low-second time.
- Hard files may defer from the fast lane and resolve in long pass or third pass.
- Long-pass timeout is expected on some complex rotated or image-only scans.
- Timeout-deferred files resume in third pass with captured OCR/candidate state.
- Files that still cannot be matched route to `NEEDS_REVIEW` with diagnostics.

## EDM Behavior

### Stage 6 AWB Fallback

- Entry requires one persistent HIGH candidate seen across at least two stages.
- Runtime is gated by `EDM: ON/OFF`, token availability, and endpoint response.
- A positive EDM existence check confirms the AWB as `EDM-Exists-Persistent`.
- OFF, missing-token, auth, or network uncertainty bypasses safely rather than hard failing.

### Downstream Duplicate Checker

- Input: `pdf_organizer/PROCESSED`
- Outputs:
  - `CLEAN`
  - `REJECTED`
  - `CLEAN-UNCHECKED` or safe bypass behavior
- Current gate/layer strategy:
  - Gate 1: all-page exact hash
  - Gate 2: bounded probe checks
  - Tier 2: full HASH/PHASH/TEXT/OCR checks with conservative reject rules
- CCD pages are exempt from duplicate checks.
- Text/OCR-only similarity is tracked carefully; strong automatic decisions require stronger evidence.

## AUTO MODE Cycle

1. Wait for `INBOX` to remain empty for the configured stable period.
2. Wait for `PROCESSED` to drain through the EDM duplicate checker.
3. Estimate batch readiness from `CLEAN`.
4. Build batches when the minimum batch threshold is met, or when old `CLEAN` files reach the force-batch age.
5. Copy batch PDFs into `PENDING_PRINT`.
6. Convert batch PDFs to TIFF.
7. Return to idle and repeat on the configured interval.

## Daily Health Checks

1. `python -m V3.config` passes with no path errors.
2. `data/AWB_dB.xlsx` is present and current.
3. `TESSERACT_PATH` resolves to a real binary.
4. `logs/pipeline.log` is advancing while the hotfolder is running.
5. If EDM is ON, token is present and valid in `data/token.txt` or `.env`.
6. `pdf_organizer/PENDING_PRINT` contains expected PDF/TIFF pairs after batch output.

## Incident Playbooks

### INBOX Not Draining

- Verify `Start AWB` is running.
- Check logs for file stability failures, corrupt PDFs, or Tesseract errors.
- Confirm the file extension is `.pdf`.
- Requeue stuck review files with `Retry Failed` if needed.

### Excess `TIMEOUT_DEFERRED`

- Expect some defers with large, rotated, scanned, or image-only documents.
- Validate third-pass resume messages are appearing.
- Tune only if backlog affects operations:
  - `LONG_PASS_TIMEOUT_SECONDS`
  - `GLOBAL_DOC_TIMEOUT_SECONDS`
  - upstream scan quality or orientation

### EDM Warnings Or Bypassed Checks

- If intentional, leave EDM OFF.
- If unintentional, verify token, endpoint URL, and network reachability.
- Treat `CLEAN-UNCHECKED` as a safe fallback path, not a hard failure.
- Refresh or replace `data/token.txt` when auth expires.

### Batch Or TIFF Not Produced

- Confirm `CLEAN` has eligible AWB-named PDF files.
- Run `Prepare Batch` manually.
- Run `Convert TIFF` manually.
- Verify outputs in:
  - `data/OUT`
  - `pdf_organizer/PENDING_PRINT`

### NEEDS_REVIEW Backlog

- Confirm AWB DB freshness with `Refresh DB`.
- Inspect `logs/pipeline.log` for candidate diagnostics.
- Improve upstream scan quality if repeated failures are image-quality related.
- Requeue after fixes with `Retry Failed`.

## End Of Day

1. Stop `AUTO MODE` first.
2. Stop `Start AWB`.
3. Confirm no long-running subprocesses remain.
4. Confirm logs and audit files are flushed.
5. Review `NEEDS_REVIEW`, `REJECTED`, and `PENDING_PRINT` for outstanding manual work.

## Key Paths

| Path | Purpose |
|---|---|
| `pdf_organizer/*` | Hotfolder runtime and routing folders |
| `data/*` | AWB DB, caches, audit workbooks, output lists |
| `data/OUT/*` | Generated print-stack PDFs and sequence files |
| `logs/*` | Runtime and EDM logs |
| `project-manifest.html` | Visual architecture/process walkthrough |
