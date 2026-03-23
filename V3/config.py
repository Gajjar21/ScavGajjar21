# V3/config.py
# Central configuration for AWB Pipeline V3.
# All modules import from here — no hardcoded paths anywhere else.
#
# On first run:  create/edit .env with your local values, then:
#   python -m V3.config        ← verifies all paths are valid

import os
import platform
import sys
from pathlib import Path
from dotenv import load_dotenv

# ── Locate and load .env ─────────────────────────────────────────────────────
# Walk up from this file to find .env (supports running from any CWD).
_HERE = Path(__file__).resolve().parent
_PROJECT_ROOT = _HERE.parent          # AWB_PIPELINE/
_ENV_PATH = _PROJECT_ROOT / ".env"

if not _ENV_PATH.exists():
    print(
        f"\n[config] ERROR: .env not found at {_ENV_PATH}\n"
        "  Create a .env file in the project root with your local values.\n"
    )
    sys.exit(1)

load_dotenv(_ENV_PATH, override=True)

_IS_MAC = platform.system() == "Darwin"
_IS_WIN = platform.system() == "Windows"


# ── Helper parsers ───────────────────────────────────────────────────────────

def _require(key: str) -> str:
    """Return env var or exit with a clear error."""
    val = os.getenv(key, "").strip()
    if not val:
        print(f"[config] ERROR: {key} is not set in .env")
        sys.exit(1)
    return val


def _bool(key: str, default: bool) -> bool:
    return os.getenv(key, str(default)).strip().lower() in ("1", "true", "yes", "on")


def _int(key: str, default: int) -> int:
    try:
        return int(os.getenv(key, str(default)).strip())
    except ValueError:
        return default


def _float(key: str, default: float) -> float:
    try:
        return float(os.getenv(key, str(default)).strip())
    except ValueError:
        return default


# ── Cross-platform path helper ────────────────────────────────────────────────
def _is_foreign_path(p: Path) -> bool:
    """Return True when *p* looks like a path from a different OS."""
    s = str(p)
    if _IS_MAC and len(s) >= 3 and s[1] == ":" and s[2] == "\\":
        return True       # Windows drive letter on Mac/Linux
    if _IS_WIN and s.startswith("/"):
        return True       # Unix absolute path on Windows
    return False


# ── Base directory ───────────────────────────────────────────────────────────
_base_raw = Path(_require("PIPELINE_BASE_DIR"))
if _is_foreign_path(_base_raw):
    BASE_DIR = _PROJECT_ROOT
    print(f"[config] NOTE: PIPELINE_BASE_DIR={_base_raw} is for another OS, using {BASE_DIR}")
elif _base_raw.is_absolute() and _base_raw.exists():
    BASE_DIR = _base_raw
else:
    BASE_DIR = _PROJECT_ROOT
    print(f"[config] NOTE: PIPELINE_BASE_DIR={_base_raw} not found, using {BASE_DIR}")

# ── Runtime folders ──────────────────────────────────────────────────────────
ORGANIZER_DIR     = BASE_DIR / "pdf_organizer"
INBOX_DIR         = ORGANIZER_DIR / "INBOX"
PROCESSED_DIR     = ORGANIZER_DIR / "PROCESSED"
CLEAN_DIR         = ORGANIZER_DIR / "CLEAN"
REJECTED_DIR      = ORGANIZER_DIR / "REJECTED"
NEEDS_REVIEW_DIR  = ORGANIZER_DIR / "NEEDS_REVIEW"
PENDING_PRINT_DIR = ORGANIZER_DIR / "PENDING_PRINT"

# ── Data files ───────────────────────────────────────────────────────────────
DATA_DIR              = BASE_DIR / "data"
OUT_DIR               = DATA_DIR / "OUT"
AWB_EXCEL_PATH        = DATA_DIR / "AWB_dB.xlsx"
AWB_LOGS_PATH         = DATA_DIR / "AWB_Logs.xlsx"
CSV_PATH              = OUT_DIR  / "awb_list.csv"
SEQUENCE_XLSX         = OUT_DIR  / "awb_sequence.xlsx"
TOKEN_FILE            = DATA_DIR / "token.txt"
STAGE_CACHE_CSV       = DATA_DIR / "stage_cache.csv"
PIPELINE_SUMMARY_CSV  = DATA_DIR / "pipeline_summary.csv"
EDM_AWB_EXISTS_CACHE  = DATA_DIR / "edm_awb_exists_cache.json"
EDM_TOGGLE_FILE       = DATA_DIR / "edm_toggle.json"
AWB_RELOAD_TRIGGER    = DATA_DIR / "reload_awb.trigger"

# ── Logs ─────────────────────────────────────────────────────────────────────
LOG_DIR      = BASE_DIR / "logs"
PIPELINE_LOG = LOG_DIR  / "pipeline.log"
EDM_LOG      = LOG_DIR  / "edm_checker.log"
AUDIT_LOG    = LOG_DIR  / "pipeline_audit.jsonl"

# ── Tesseract ────────────────────────────────────────────────────────────────
_tess_raw = Path(_require("TESSERACT_PATH"))
if not _is_foreign_path(_tess_raw) and _tess_raw.exists():
    TESSERACT_PATH = _tess_raw
elif _IS_MAC:
    # Auto-detect Tesseract on Mac when .env has a Windows path
    _mac_tess_candidates = [
        Path("/opt/homebrew/bin/tesseract"),   # Apple Silicon
        Path("/usr/local/bin/tesseract"),       # Intel Mac / Homebrew
    ]
    TESSERACT_PATH = _tess_raw  # default (will fail in require_tesseract)
    for _candidate in _mac_tess_candidates:
        if _candidate.exists():
            TESSERACT_PATH = _candidate
            print(f"[config] NOTE: TESSERACT_PATH={_tess_raw} not found, using {TESSERACT_PATH}")
            break
else:
    TESSERACT_PATH = _tess_raw

# ── EDM API ────────────────────────────────────────────────────────────────────
EDM_TOKEN             = os.getenv("EDM_TOKEN", "").strip() or None
EDM_OPERATING_COMPANY = os.getenv("EDM_OPERATING_COMPANY", "FXE").strip()
EDM_BASE_URL          = os.getenv(
    "EDM_BASE_URL",
    "https://shipment-portal-service-g.prod.cloud.fedex.com",
).strip()
EDM_METADATA_URL = EDM_BASE_URL + "/edm/protocol/retrieve/groups/metadata"
EDM_DOWNLOAD_URL = EDM_BASE_URL + "/edm/protocol/downloadDocuments"
ENABLE_EDM_FALLBACK   = _bool("ENABLE_EDM_FALLBACK", False)

# ── OCR / Matching ───────────────────────────────────────────────────────────
AWB_LEN                     = 12
OCR_DPI_MAIN                = _int("OCR_DPI_MAIN",   320)
OCR_DPI_STRONG              = _int("OCR_DPI_STRONG", 420)
ROTATION_PROBE_DPI          = _int("ROTATION_PROBE_DPI", 140)
ROTATION_FAST_DPI           = _int("ROTATION_FAST_DPI", 200)
ENABLE_ROTATION_LAST_RESORT = _bool("ENABLE_ROTATION_LAST_RESORT", True)

ALLOW_1_DIGIT_TOLERANCE       = _bool("ALLOW_1_DIGIT_TOLERANCE",     True)
STRICT_AMBIGUOUS              = _bool("STRICT_AMBIGUOUS",            True)
STOP_EARLY_IF_MANY_12DIGITS   = _bool("STOP_EARLY_IF_MANY_12DIGITS", True)
MANY_12DIGITS_THRESHOLD       = _int("MANY_12DIGITS_THRESHOLD",       6)

EXCEL_REFRESH_SECONDS = _int("EXCEL_REFRESH_SECONDS", 30)
POLL_SECONDS          = _int("POLL_SECONDS", 2)
HEARTBEAT_SECONDS     = _int("HEARTBEAT_SECONDS", 10)

CONTEXT_WINDOW_CHARS  = _int("CONTEXT_WINDOW_CHARS", 40)

# OCR PSM modes
OCR_MAIN_PSMS   = (6, 11)
OCR_STRONG_PSMS = (6, 11)

# ── Tolerance matching thresholds ────────────────────────────────────────────
ALLOW_STANDARD_TOLERANCE             = _bool("ALLOW_STANDARD_TOLERANCE", True)
TOLERANCE_HIGH_MAX_DISTANCE          = _int("TOLERANCE_HIGH_MAX_DISTANCE", 2)
TOLERANCE_STANDARD_MAX_DISTANCE      = _int("TOLERANCE_STANDARD_MAX_DISTANCE", 1)
MIN_STAGE_HITS_HIGH_TOL1             = _int("MIN_STAGE_HITS_HIGH_TOL1", 1)
MIN_STAGE_HITS_HIGH_TOL2             = _int("MIN_STAGE_HITS_HIGH_TOL2", 2)
MIN_STAGE_HITS_STANDARD_TOL          = _int("MIN_STAGE_HITS_STANDARD_TOL", 2)
REQUIRE_SINGLE_STANDARD_CANDIDATE_FOR_TOL = _bool("REQUIRE_SINGLE_STANDARD_CANDIDATE_FOR_TOL", True)

# ── Rotation probe thresholds ────────────────────────────────────────────────
ROTATION_PROBE_MIN_FLIP_MARGIN    = _int("ROTATION_PROBE_MIN_FLIP_MARGIN", 80)
ROTATION_PROBE_DIGIT_CLEAR_MARGIN = _int("ROTATION_PROBE_DIGIT_CLEAR_MARGIN", 24)
ROTATION_PROBE_CERTAIN_MARGIN     = _int("ROTATION_PROBE_CERTAIN_MARGIN", 300)
ROTATION_PROBE_LIKELY_MARGIN      = _int("ROTATION_PROBE_LIKELY_MARGIN", 120)

# ── Pipeline feature flags ───────────────────────────────────────────────────
ENABLE_UPSCALED_RESCUE_PASS       = _bool("ENABLE_UPSCALED_RESCUE_PASS", True)
ENABLE_AIRWAY_LABEL_RESCUE        = _bool("ENABLE_AIRWAY_LABEL_RESCUE", True)
MAX_CONTEXT_RESCUE_MS             = _int("MAX_CONTEXT_RESCUE_MS", 60000)
ENABLE_INBOX_TWO_PASS             = _bool("ENABLE_INBOX_TWO_PASS", True)
LONG_PASS_TIMEOUT_SECONDS         = _float("LONG_PASS_TIMEOUT_SECONDS", 45.0)
FASTLANE_MICRO_PROBE_ENABLED      = _bool("FASTLANE_MICRO_PROBE_ENABLED", True)
FASTLANE_IMAGE_ONLY_BUDGET_SECONDS = _float("FASTLANE_IMAGE_ONLY_BUDGET_SECONDS", 10.0)
FASTLANE_IMAGE_ONLY_HARD_DOC_BUDGET_SECONDS = _float("FASTLANE_IMAGE_ONLY_HARD_DOC_BUDGET_SECONDS", 14.0)
FASTLANE_SINGLE_ROTATED_PASS_ENABLED = _bool("FASTLANE_SINGLE_ROTATED_PASS_ENABLED", True)
FASTLANE_EARLY_HINT_ROTATED_PROBE_ENABLED = _bool("FASTLANE_EARLY_HINT_ROTATED_PROBE_ENABLED", True)
FASTLANE_STAGE3_EARLY_ROTATED_PASS_ENABLED = _bool("FASTLANE_STAGE3_EARLY_ROTATED_PASS_ENABLED", True)
FASTLANE_EARLY_ROTATION_ROUTE_ENABLED = _bool("FASTLANE_EARLY_ROTATION_ROUTE_ENABLED", True)
FASTLANE_QUICK_ROTATED_PSM6_ENABLED = _bool("FASTLANE_QUICK_ROTATED_PSM6_ENABLED", True)
FASTLANE_ROTATED_TOL1_ENABLED = _bool("FASTLANE_ROTATED_TOL1_ENABLED", True)
FASTLANE_CERTAIN_ROTATION_RESCUE_LITE_ENABLED = _bool("FASTLANE_CERTAIN_ROTATION_RESCUE_LITE_ENABLED", True)
LOG_STAGE_SNAPSHOTS               = _bool("LOG_STAGE_SNAPSHOTS", True)
CANDIDATE_SNAPSHOT_LIMIT           = _int("CANDIDATE_SNAPSHOT_LIMIT", 20)

# ── EDM duplicate-check tuning ───────────────────────────────────────────────
TEXT_SIMILARITY_THRESHOLD    = _int("TEXT_SIMILARITY_THRESHOLD", 85)
PAGE_OCR_LIMIT               = _int("PAGE_OCR_LIMIT", 8)
PHASH_THRESHOLD              = _int("PHASH_THRESHOLD", 10)
MIN_EMBEDDED_TEXT_LENGTH     = _int("MIN_EMBEDDED_TEXT_LENGTH", 25)
EARLY_FOCUS_MATCH_THRESHOLD  = _int("EARLY_FOCUS_MATCH_THRESHOLD", 3)
FILE_SETTLE_SECONDS          = _int("FILE_SETTLE_SECONDS", 3)
EDM_OCR_COMPARE_LIMIT        = _int("EDM_OCR_COMPARE_LIMIT", 10)
EDM_REJECT_IF_DUP_PAGES_OVER = _int("EDM_REJECT_IF_DUP_PAGES_OVER", 5)
EDM_REJECT_IF_DUP_RATIO      = _float("EDM_REJECT_IF_DUP_RATIO", 0.70)
EDM_TIER1_INCOMING_PAGES     = _int("EDM_TIER1_INCOMING_PAGES", 3)
EDM_TIER1_EDM_PAGE_LIMIT     = _int("EDM_TIER1_EDM_PAGE_LIMIT", 5)
EDM_TIER2_EDM_PAGE_LIMIT     = _int("EDM_TIER2_EDM_PAGE_LIMIT", 10)
EDM_TEXT_LAYER_MIN_CHARS     = _int("EDM_TEXT_LAYER_MIN_CHARS", 30)
EDM_OCR_WORKERS              = _int("EDM_OCR_WORKERS", 2)
EDM_OCR_PARALLEL_MIN_TASKS   = _int("EDM_OCR_PARALLEL_MIN_TASKS", 4)

# ── Batch builder ────────────────────────────────────────────────────────────
MAX_PAGES_PER_BATCH  = _int("MAX_PAGES_PER_BATCH", 48)
COVER_PAGE_SIZE      = os.getenv("COVER_PAGE_SIZE", "LETTER").strip().upper()
PRINT_STACK_BASENAME = "PRINT_STACK_BATCH"
ENABLE_TIER_BATCHING = _bool("ENABLE_TIER_BATCHING", False)

# ── TIFF converter ───────────────────────────────────────────────────────────
TIFF_DPI            = _int("TIFF_DPI", 200)
TIFF_COMPRESSION    = os.getenv("TIFF_COMPRESSION", "tiff_lzw").strip() or None
TIFF_GRAYSCALE      = _bool("TIFF_GRAYSCALE", True)
TIFF_SKIP_IF_EXISTS = _bool("TIFF_SKIP_IF_EXISTS", True)
TIFF_PARALLEL_WORKERS = _int("TIFF_PARALLEL_WORKERS", 4)

# ── UI / Auto mode ───────────────────────────────────────────────────────────
AUTO_INTERVAL_SEC              = _int("AUTO_INTERVAL_SEC", 10)
AUTO_WAIT_FOR_INBOX_EMPTY      = _bool("AUTO_WAIT_FOR_INBOX_EMPTY", True)
INBOX_EMPTY_STABLE_SECONDS     = _int("INBOX_EMPTY_STABLE_SECONDS", 8)
INBOX_EMPTY_MAX_WAIT           = _int("INBOX_EMPTY_MAX_WAIT", 1800)
PROCESSED_EMPTY_STABLE_SECONDS = _int("PROCESSED_EMPTY_STABLE_SECONDS", 5)
PROCESSED_EMPTY_MAX_WAIT       = _int("PROCESSED_EMPTY_MAX_WAIT", 600)
MIN_CLEAN_BATCHES_FOR_AUTO     = _int("MIN_CLEAN_BATCHES_FOR_AUTO", 2)

# ── Audit ────────────────────────────────────────────────────────────────────
AUDIT_XLSX_PATH      = DATA_DIR / "pipeline_audit.xlsx"
WRITE_LEGACY_TRACKER = _bool("WRITE_LEGACY_TRACKER", False)  # V3: legacy off by default

# ── AWB context keywords (ordered, deduplicated) ─────────────────────────────
AWB_CONTEXT_KEYWORDS = (
    "AWB", "AIR WAYBILL", "AIRWAY BILL", "AIRWAYBILL",
    "AIRWAY BILL NUMBER", "AIRWAYBILL NUMBER", "WAYBILL",
    "TRACKING", "TRACKING NUMBER", "SHIPMENT", "MASTER",
    "MAWB", "HAWB", "BILL NO", "BOL",
    "COMMERCIAL INVOICE", "C/I", "CI NO", "CI NUMBER",
    "AWB NO", "AWB NUMBER", "AWB#", "TRACKING #", "TRACKING NUM",
    "FEDEX TRACKING", "FED EX TRACKING", "AIR WAY BILL",
    "AIR WAY BILL NUMBER", "AIR WAYBILL NUMBER", "WAY BILL",
    "ACI", "ACI NO", "ACI NUMBER", "CARGO CONTROL NUMBER",
    "CARGO CONTROL NO", "CCN", "CONSIGNMENT", "CONSIGNMENT NO",
    "CONSIGNMENT NUMBER", "FDX", "FDE", "FDXE", "FEDEX", "FED-EX",
    "FDX TRACKING", "FDXE TRACKING", "SHIP",
    "TRK", "TRK#", "TRK NO", "TRK NUMBER", "TRACKING NO",
    "B/L", "B/L NO", "B/L NUMBER", "BL NO", "BL NUMBER",
)

# ── Protected files (never deleted by Clear All) ─────────────────────────────
PROTECTED_FILES = {AWB_EXCEL_PATH, AWB_LOGS_PATH, AUDIT_XLSX_PATH}

# ── Folders to create on startup ─────────────────────────────────────────────
RUNTIME_DIRS = [
    INBOX_DIR, PROCESSED_DIR, CLEAN_DIR, REJECTED_DIR,
    NEEDS_REVIEW_DIR, PENDING_PRINT_DIR,
    DATA_DIR, OUT_DIR, LOG_DIR,
]


def ensure_dirs():
    """Create all runtime directories if they don't exist."""
    for d in RUNTIME_DIRS:
        d.mkdir(parents=True, exist_ok=True)


# ── Self-check ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("\n=== AWB Pipeline V3 — Config Check ===\n")
    ok = True
    checks = {
        "BASE_DIR":       (BASE_DIR, True),
        "TESSERACT_PATH": (TESSERACT_PATH, True),
        "AWB_EXCEL_PATH": (AWB_EXCEL_PATH, False),
        "TOKEN_FILE":     (TOKEN_FILE, False),
    }
    for label, (path, required) in checks.items():
        exists = path.exists()
        if required:
            status = "OK" if exists else "MISSING"
        else:
            status = "OK" if exists else "not found (optional)"
        print(f"  {label:<20} {status}  ({path})")
        if not exists and required:
            ok = False

    token_ok = bool(EDM_TOKEN and EDM_TOKEN != "paste_your_token_here")
    print(f"  {'EDM_TOKEN':<20} {'present' if token_ok else 'not set (EDM check skipped)'}")
    print(f"  {'ENABLE_EDM_FALLBACK':<20} {ENABLE_EDM_FALLBACK}")
    print(f"\n  OCR_DPI_MAIN={OCR_DPI_MAIN}  OCR_DPI_STRONG={OCR_DPI_STRONG}")
    print(f"  TIFF_DPI={TIFF_DPI}  MAX_PAGES_PER_BATCH={MAX_PAGES_PER_BATCH}")
    print(f"  TWO_PASS={ENABLE_INBOX_TWO_PASS}  ROTATION={ENABLE_ROTATION_LAST_RESORT}")
    print()
    if ok:
        print("All required checks passed.\n")
    else:
        print("Fix the issues above in your .env file, then re-run.\n")
        sys.exit(1)
