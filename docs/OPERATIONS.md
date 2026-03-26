# Operations Runbook

## Daily Startup

1. Launch app: `python -m V3.app`.
2. Confirm `EDM: ON/OFF` is set to intended runtime mode.
3. Start AWB hotfolder (`Start AWB`) if not already running.
4. Verify log shows:
- AWB DB load count
- two-pass scheduler active
- long-pass timeout budget (`45s`)
5. Confirm folder baselines:
- `pdf_organizer/INBOX`
- `pdf_organizer/PROCESSED`
- `pdf_organizer/CLEAN`
- `pdf_organizer/REJECTED`
- `pdf_organizer/NEEDS_REVIEW`

## Runtime Controls

- `Start AWB`: toggles hotfolder and scheduler.
- `AUTO MODE`: unattended loop (`INBOX drain -> PROCESSED drain -> batch -> TIFF`).
- `Full Cycle`: one supervised end-to-end cycle.
- `Prepare Batch`: build print stacks from CLEAN.
- `Convert TIFF`: generate TIFF outputs from stack PDFs.
- `Retry Failed`: requeue NEEDS_REVIEW PDFs.
- `Refresh DB`: drops reload trigger for hotfolder AWB DB refresh.

## Normal Processing Expectations

- Fast matches complete in sub-second to low-seconds.
- Hard files may defer from fast lane and resolve in long/third pass.
- Long pass timeout is expected on some complex rotated/image-only scans.
- Timeout-deferred files resume in third pass with captured OCR/candidate state.

## EDM Operational Behavior

### Stage 6 (pipeline fallback)
- Entry requires one persistent HIGH candidate seen across 2+ stages.
- Runtime gated by `EDM: ON/OFF` and token availability.
- `True` confirms AWB (`EDM-Exists-Persistent`), else flow continues safely.

### Downstream duplicate checker
- Input: `PROCESSED`.
- Output: `CLEAN`, `REJECTED`, or `CLEAN-UNCHECKED`.
- Uses gate/layer strategy:
- Gate 1: all-page exact hash
- Gate 2: bounded probe checks
- Tier 2: full checks (HASH/PHASH/TEXT/OCR) with conservative reject rules

## Daily Health Checks

1. `python -m V3.config` passes with no path errors.
2. `data/AWB_dB.xlsx` is present and current.
3. `TESSERACT_PATH` resolves to a real binary.
4. `logs/pipeline.log` is advancing.
5. If EDM is ON: token present and not expired (`data/token.txt`).

## Incident Playbooks

### 1) INBOX not draining
- Verify AWB service is running.
- Check for file stability failures or corrupt PDFs in log.
- Requeue stuck files via `Retry Failed` if needed.

### 2) Excess `TIMEOUT_DEFERRED`
- Expect some with large rotated scans.
- Validate third-pass resumes are occurring.
- Tune only if persistent backlog impacts SLA:
- `LONG_PASS_TIMEOUT_SECONDS`
- input quality/scan settings upstream

### 3) EDM warnings or bypassed checks
- If intentional, leave EDM OFF.
- If unintentional, verify token and endpoint reachability.
- `CLEAN-UNCHECKED` is safe fallback behavior, not a hard failure.

### 4) Batch/TIFF not produced
- Confirm CLEAN has eligible files.
- Run `Prepare Batch` then `Convert TIFF` manually.
- Verify outputs in `data/OUT` and `pdf_organizer/PENDING_PRINT`.

## End-of-Day / Controlled Stop

1. Stop AUTO MODE first.
2. Stop AWB.
3. Ensure no long-running subprocesses remain.
4. Confirm logs and audit files are flushed.

## Key Paths

- Hotfolder runtime: `pdf_organizer/*`
- Data: `data/*`
- Logs: `logs/*`
- Visual doc: `awb_pipeline_fedex-2.html`
