# Contributing

Thanks for contributing to AWB Pipeline V3.

## Local Setup

1. Create and activate a virtual environment.
2. Install dependencies:
   - `pip install -r requirements.txt -r requirements-dev.txt`
3. Copy `.env.example` to `.env` and set:
   - `PIPELINE_BASE_DIR`
   - `TESSERACT_PATH`
4. Run config check:
   - `python -m V3.config`

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
- [ ] Lint/checks pass (`make check`)
- [ ] README/docs updated for behavior changes
- [ ] No secrets committed (`.env`, tokens)
