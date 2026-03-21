# Operations Runbook

## Daily Startup

1. Launch app: `python -m V3.app`
2. Confirm hotfolder starts without config errors.
3. Verify AWB load count in log.

## Key Controls

- `Start AWB`: starts watcher.
- `Full Cycle Once`: one full batch cycle.
- `AUTO MODE`: unattended loop.
- `Retry NEEDS_REVIEW`: re-queue review files.

## Common Checks

- `python -m V3.config`
- Confirm `data/AWB_dB.xlsx` exists and is current.
- Confirm `TESSERACT_PATH` points to a real binary.

## Troubleshooting

- Slow long-pass: lower `LONG_PASS_TIMEOUT_SECONDS`.
- Empty UI fields on macOS: ensure Python uses Tk >= 8.6 runtime.
- EDM warnings: expected when token is missing in V3 (fallback disabled path).
