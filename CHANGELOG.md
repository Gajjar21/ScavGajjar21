# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Documentation
- Updated `README.md` to align with live V3 behavior:
- fast-lane ProbeLite after Stage 3 fail
- two-pass scheduler and third-pass resume semantics
- EDM Stage-6 fallback and downstream duplicate checker outputs (`CLEAN`, `REJECTED`, `CLEAN-UNCHECKED`)
- current config keys and operational flow
- Expanded `docs/OPERATIONS.md` into a full runbook:
- startup checks
- control mapping
- daily health checks
- incident playbooks
- controlled shutdown

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
