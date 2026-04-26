# Contributing

Thanks for contributing to Document Processing Pipeline V3.

## Local Setup

1. Create and activate a virtual environment.
2. Install dependencies:
   - `pip install -r V3/requirements.txt -r requirements-dev.txt`
3. Copy `.env.example` to `.env` and set:
   - `PIPELINE_BASE_DIR`
   - `TESSERACT_PATH`
4. Run config check:
   - `python -m V3.config`
5. Run focused tests when changing behavior:
   - `pytest`

## Branching

- `main` is always releasable.
- Use short feature branches: `feat/<name>`, `fix/<name>`, `chore/<name>`.

## Commit Style

Use clear messages, for example:

- `feat(ui): add retry needs-review control`
- `fix(pipeline): reduce long-pass timeout default`
- `docs: update v3 standalone setup`

## Pull Request Checklist

- [ ] Config check passes (`python -m V3.config`)
- [ ] Tests/checks pass for the touched area
- [ ] README/docs updated for behavior changes
- [ ] No secrets committed (`.env`, tokens)
- [ ] Generated runtime files are not committed (`logs`, workbook outputs, PDFs/TIFFs)

## Documentation-Only Changes

For README, runbook, changelog, or visual-process updates:

- Keep wording aligned with the current V3 flow.
- Do not edit launcher scripts or pipeline scripts unless the change requires code behavior updates.
- Use `Document Processing Pipeline` for the project name and reserve `AWB` for the air waybill number/domain concept.
