# V3/ui/theme.py
# Central theme/style constants for AWB Pipeline V3 UI.
# All colours, fonts, thresholds, and log-tag definitions live here.

import platform

_IS_WIN = platform.system() == "Windows"
_IS_MAC = platform.system() == "Darwin"

# ── Colours ──────────────────────────────────────────────────────────────────
HEADER_BG    = "#1a2744"
HEADER_FG    = "white"
APP_BG       = "#f4f7fb"
PANEL_BG     = "#ffffff"
STRIP_BG     = "#edf2fa"
STRIP_CELL   = "#ffffff"
TEXT_FG      = "#1f2633"
TEXT_SEC     = "#5a6b85"
TEXT_MUTED   = "#7d8aa0"
OK           = "#3cb043" if _IS_MAC else "#1f7a1f"
WARN         = "#e6a817" if _IS_MAC else "#b57b00"
CRIT         = "#d97a1f" if _IS_MAC else "#b54708"
INFO         = "#4ea8e0" if _IS_MAC else "#0c6db0"
REVIEW       = "#e08a30" if _IS_MAC else "#b54708"
ACCENT       = "#2b5797"
BTN_BG       = "#e8effb"
BTN_HOVER    = "#d7e4fa"
BTN_FG       = TEXT_FG
FEDEX_PURPLE = "#4d148c"
FEDEX_ORANGE = "#ff6600"
STRIP_IDLE   = "#8da0bd"

# ── Surface/text defaults ────────────────────────────────────────────────────
FRAME_LABEL_FG = "#333333"
DIALOG_BG = "#f6f8fc"
DIALOG_FG = "#1a1a1a"

# ── Fonts ────────────────────────────────────────────────────────────────────
if _IS_WIN:
    FONT_HEADER = ("Segoe UI", 16, "bold")
    FONT_TITLE  = ("Segoe UI", 9, "bold")
    FONT_LABEL  = ("Segoe UI", 11)
    FONT_SMALL  = ("Segoe UI", 10)
    FONT_MONO   = ("Consolas", 10)
    FONT_BTN    = ("Segoe UI", 9)
    FONT_COUNT  = ("Segoe UI", 24, "bold")
else:
    FONT_HEADER = ("Helvetica", 17, "bold")
    FONT_TITLE  = ("Helvetica", 10, "bold")
    FONT_LABEL  = ("Helvetica", 12)
    FONT_SMALL  = ("Helvetica", 10)
    FONT_MONO   = ("Menlo", 10)
    FONT_BTN    = ("Helvetica", 9, "bold")
    FONT_COUNT  = ("Helvetica", 24, "bold")

# ── Folder count colour thresholds  (orange_at, red_at) ─────────────────────
THRESHOLDS = {
    "inbox":    (10, 25),
    "review":   (1,  5),
    "rejected": (1,  10),
    "pending":  (20, 50),
}

# ── Log tag definitions  (tag_name, (fg, bg), [keywords]) ───────────────────
LOG_TAGS = [
    ("error",    (CRIT,      None), ["ERROR", "FAIL", "FAILED", "EXCEPTION"]),
    ("warn",     (WARN,      None), ["WARN", "WARNING"]),
    ("review",   (REVIEW,    None), ["NEEDS_REVIEW", "NEEDS-REVIEW"]),
    ("success",  (OK,        None), ["COMPLETE", " OK:", "OK ", "CLEAN", "MATCHED"]),
    ("rejected", (CRIT,      None), ["REJECTED"]),
    ("token",    ("#9933cc", None), ["TOKEN EXPIRED"]),
    ("skip",     ("#888888", None), ["SKIP", "SKIPPED"]),
    ("stage",    ("#2266cc", None), ["[Stage", "[STAGE", "[AUTO]", "[BATCH]", "[CYCLE]"]),
    ("info",     (INFO,      None), ["===", "---"]),
]

# ── Log cap ──────────────────────────────────────────────────────────────────
LOG_MAX_LINES = 2000
