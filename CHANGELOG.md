# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Documentation
- Rebranded GitHub-facing documentation from AWB Pipeline to Document Processing Pipeline while keeping AWB terminology for air waybill extraction.
- Updated `README.md` to align with live V3 behavior:
  - fast-lane ProbeLite after Stage 3 fail
  - two-pass scheduler and third-pass resume semantics
  - EDM Stage-6 fallback and downstream duplicate checker outputs
  - current config keys, thresholds, and operational flow
- Expanded `docs/OPERATIONS.md` into a full runbook:
  - startup checks
  - control mapping
  - normal processing expectations
  - AUTO MODE behavior
  - incident playbooks
  - controlled shutdown
- Added `.env.example` for safe local configuration onboarding.
- Added `.gitignore` coverage for local secrets, virtual environments, runtime state, logs, and generated PDFs/TIFFs.
- Updated `CONTRIBUTING.md` for current dependency setup and documentation-only change guidance.
- Updated `awb_pipeline_fedex-2.html` visual documentation for current process wording and phone-friendly layout.

### UI Reliability
- Hardened UI shutdown in `V3/ui/app_window.py`:
  - prevents late async log callbacks during close
  - ensures EDM duplicate checker is stopped on app exit even when AWB is already stopped

## [1.0.0] - 2026-03-21

### Added
- Standalone V3 project structure with runtime folder skeleton.
- Two-pass scheduler with configurable long-pass timeout.
- Cross-platform launchers and installer scripts.
- Project governance and repo management files.
