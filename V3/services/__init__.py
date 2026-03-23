# V3/services/__init__.py
# Service modules for AWB Pipeline V3.
#
# Submodules:
#   hotfolder      - Watchdog-based inbox monitor (main entry point)
#   batch_builder  - Batch PDF builder
#   tiff_converter - PDF to TIFF converter
#   edm_checker    - EDM AWB existence fallback + runtime toggle helpers
#   edm_duplicate_checker - EDM duplicate-page checker service (PROCESSED -> CLEAN/REJECTED)
