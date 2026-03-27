# V3/ui/app_window.py
# ScavGajjar V3 — Main Application Window
#
# Fully self-contained Tkinter UI with:
#   - Employee login dialog
#   - Start/Stop Scan, EDM toggle, Prepare Batch, Full Cycle, Auto Mode
#   - Folder shortcuts, live status strip, folder counts, stats panel
#   - Colour-coded log viewer with line cap
#   - Animated progress indicator
#   - Clear All, Upload Files, Retry NEEDS_REVIEW, Refresh DB

import json
import os
import re
import shutil
import subprocess
import sys
import threading
import time
import tkinter as tk
from collections import deque
from pathlib import Path
from tkinter import filedialog, messagebox, scrolledtext

from PIL import Image, ImageDraw, ImageTk

from V3 import config
from V3.services.edm_checker import is_edm_enabled, set_edm_enabled
from V3.ui.theme import (
    ACCENT,
    APP_BG,
    BTN_BG,
    BTN_FG,
    BTN_HOVER,
    CRIT,
    FEDEX_ORANGE,
    FEDEX_PURPLE,
    FONT_BTN,
    FONT_COUNT,
    FONT_HEADER,
    FONT_LABEL,
    FONT_MONO,
    FONT_SMALL,
    FONT_TITLE,
    FRAME_LABEL_FG,
    HEADER_BG,
    HEADER_FG,
    INFO,
    LOG_MAX_LINES,
    LOG_TAGS,
    OK,
    PANEL_BG,
    REVIEW,
    STRIP_BG,
    STRIP_CELL,
    STRIP_IDLE,
    TEXT_FG,
    TEXT_MUTED,
    TEXT_SEC,
    THRESHOLDS,
    WARN,
)

# ── Paths / constants ────────────────────────────────────────────────────────
_ROOT        = Path(__file__).resolve().parent.parent.parent   # AWB_PIPELINE/
STATE_FILE   = config.BASE_DIR / "_run_state.json"
SESSION_FILE = config.DATA_DIR / "session.json"
LOGO_FILE    = _ROOT / "V3" / "ui" / "assets" / "gj21_logo.png"

PROTECTED = {p.resolve() for p in config.PROTECTED_FILES}

WORKING_PATTERNS      = ["*.pdf", "*.png", "*.jpg", "*.jpeg", "*.tif", "*.tiff",
                          "*.txt", "*.csv", "*.xlsx"]
OUTPUT_FILES_TO_CLEAR = [config.CSV_PATH]

# Auto-mode config pulled from config.py
AUTO_INTERVAL_SEC              = config.AUTO_INTERVAL_SEC
AUTO_WAIT_FOR_INBOX_EMPTY      = config.AUTO_WAIT_FOR_INBOX_EMPTY
INBOX_EMPTY_STABLE_SECONDS     = config.INBOX_EMPTY_STABLE_SECONDS
INBOX_EMPTY_MAX_WAIT           = config.INBOX_EMPTY_MAX_WAIT
PROCESSED_EMPTY_STABLE_SECONDS = config.PROCESSED_EMPTY_STABLE_SECONDS
PROCESSED_EMPTY_MAX_WAIT       = config.PROCESSED_EMPTY_MAX_WAIT
MIN_CLEAN_BATCHES_FOR_AUTO     = config.MIN_CLEAN_BATCHES_FOR_AUTO
AUTO_FORCE_BATCH_AGE_SECONDS   = config.AUTO_FORCE_BATCH_AGE_SECONDS


# ═════════════════════════════════════════════════════════════════════════════
# HELPERS
# ═════════════════════════════════════════════════════════════════════════════

def save_state(state: dict) -> None:
    try:
        STATE_FILE.write_text(json.dumps(state, indent=2), encoding="utf-8")
    except Exception:
        pass


def now_run_id() -> str:
    return time.strftime("%Y%m%d_%H%M%S")


def safe_delete_file(fp: Path) -> bool:
    if fp.resolve() in PROTECTED:
        return False
    if fp.exists():
        try:
            fp.unlink()
            return True
        except Exception:
            return False
    return False


def delete_matching(folder: Path, patterns: list) -> int:
    deleted = 0
    for pat in patterns:
        for fp in folder.glob(pat):
            if fp.resolve() in PROTECTED:
                continue
            try:
                fp.unlink()
                deleted += 1
            except Exception:
                pass
    return deleted


def _next_available_path(folder: Path, filename: str) -> Path:
    dst = folder / filename
    if not dst.exists():
        return dst
    stem, sfx = dst.stem, dst.suffix
    k = 2
    while True:
        candidate = folder / f"{stem}_{k}{sfx}"
        if not candidate.exists():
            return candidate
        k += 1


def _count_pdfs(folder: Path) -> int:
    try:
        return len(list(folder.glob("*.pdf")))
    except Exception:
        return 0


def inbox_pdf_count() -> int:
    return _count_pdfs(config.INBOX_DIR)


def clean_pdf_count() -> int:
    return _count_pdfs(config.CLEAN_DIR)


def processed_pdf_count() -> int:
    return _count_pdfs(config.PROCESSED_DIR)


def clean_plus_rejected_count() -> int:
    return _count_pdfs(config.CLEAN_DIR) + _count_pdfs(config.REJECTED_DIR)


def _oldest_clean_file_age_seconds() -> float | None:
    """Return the age in seconds of the oldest PDF in CLEAN, or None if empty."""
    try:
        files = list(config.CLEAN_DIR.glob("*.pdf"))
        if not files:
            return None
        return time.time() - min(f.stat().st_mtime for f in files)
    except Exception:
        return None


def wait_until_inbox_empty(log_fn, stable_seconds=8, max_wait=1800, stop_event=None) -> bool:
    start = time.time()
    empty_since = None
    while True:
        if stop_event is not None and stop_event.is_set():
            return False
        n = inbox_pdf_count()
        if n == 0:
            if empty_since is None:
                empty_since = time.time()
                log_fn(f"[AUTO] Inbox empty — confirming stable for {stable_seconds}s...")
            if (time.time() - empty_since) >= stable_seconds:
                return True
        else:
            empty_since = None
            log_fn(f"[AUTO] Waiting INBOX empty | remaining: {n}")
        if (time.time() - start) >= max_wait:
            log_fn(f"[AUTO] Timeout after {max_wait}s.")
            return False
        for _ in range(4):
            if stop_event is not None and stop_event.is_set():
                return False
            time.sleep(0.5)


def wait_until_processed_empty(log_fn, stable_seconds=5, max_wait=600, stop_event=None) -> bool:
    start = time.time()
    empty_since = None
    while True:
        if stop_event is not None and stop_event.is_set():
            return False
        n = processed_pdf_count()
        if n == 0:
            if empty_since is None:
                empty_since = time.time()
                log_fn(f"[AUTO] PROCESSED drain — confirming stable for {stable_seconds}s...")
            if (time.time() - empty_since) >= stable_seconds:
                return True
        else:
            empty_since = None
            log_fn(f"[AUTO] Waiting PROCESSED drain | remaining: {n}")
        if (time.time() - start) >= max_wait:
            log_fn(f"[AUTO] PROCESSED timeout after {max_wait}s.")
            return False
        for _ in range(4):
            if stop_event is not None and stop_event.is_set():
                return False
            time.sleep(0.5)


def _estimate_batch_count() -> int:
    """Call V3.services.batch_builder --estimate-batches, return int count."""
    try:
        result = subprocess.run(
            [sys.executable, "-u", "-m", "V3.services.batch_builder", "--estimate-batches"],
            capture_output=True, text=True, timeout=30,
            encoding="utf-8", errors="replace",
            env={**os.environ, "PYTHONUTF8": "1", "PYTHONIOENCODING": "utf-8"},
            cwd=str(_ROOT),
        )
        return int(result.stdout.strip())
    except Exception:
        return 0


def _load_session() -> dict:
    try:
        if SESSION_FILE.exists():
            return json.loads(SESSION_FILE.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {}


def _save_session(data: dict) -> None:
    try:
        config.DATA_DIR.mkdir(parents=True, exist_ok=True)
        SESSION_FILE.write_text(json.dumps(data, indent=2), encoding="utf-8")
    except Exception:
        pass


# ═════════════════════════════════════════════════════════════════════════════
# LABEL-BUTTON  (color-stable on macOS — tk.Button loses bg on focus change)
# ═════════════════════════════════════════════════════════════════════════════

class _LabelBtn(tk.Frame):
    """Flat button built from Frame + Label so bg never greys out on macOS."""

    def __init__(self, parent, text, cmd, bg, fg, hover_bg,
                 font, padx, pady, width=0, image=None, compound="left"):
        super().__init__(parent, bg=bg, cursor="right_ptr")
        self._bg       = bg
        self._fg       = fg
        self._hover_bg = hover_bg
        self._cmd      = cmd
        self._disabled = False
        self._image    = image
        self._lbl = tk.Label(
            self, text=text, font=font, bg=bg, fg=fg,
            padx=padx, pady=pady, anchor="center",
            image=image, compound=compound,
        )
        if width:
            self._lbl.config(width=width)
        self._lbl.pack(fill="both", expand=True)
        for w in (self, self._lbl):
            w.bind("<Button-1>", self._on_click)
            w.bind("<Enter>",    self._on_enter)
            w.bind("<Leave>",    self._on_leave)

    def _on_click(self, _e=None):
        if not self._disabled:
            self._cmd()

    def _on_enter(self, _e=None):
        if not self._disabled:
            super().configure(bg=self._hover_bg)
            self._lbl.configure(bg=self._hover_bg)

    def _on_leave(self, _e=None):
        super().configure(bg=self._bg)
        self._lbl.configure(bg=self._bg)

    def config(self, **kw):
        if "text" in kw:
            self._lbl.configure(text=kw.pop("text"))
        if "image" in kw:
            self._image = kw.pop("image")
            self._lbl.configure(image=self._image)
        if "compound" in kw:
            self._lbl.configure(compound=kw.pop("compound"))
        bg_new = kw.pop("bg", kw.pop("background", None))
        if bg_new is not None:
            self._bg = bg_new
            super().configure(bg=bg_new)
            if not self._disabled:
                self._lbl.configure(bg=bg_new)
        fg_new = kw.pop("fg", kw.pop("foreground", None))
        if fg_new is not None:
            self._fg = fg_new
            if not self._disabled:
                self._lbl.configure(fg=fg_new)
        hover = kw.pop("activebackground", None)
        if hover is not None:
            self._hover_bg = hover
        kw.pop("activeforeground",    None)
        kw.pop("highlightbackground", None)
        kw.pop("highlightthickness",  None)
        state = kw.pop("state", None)
        if state is not None:
            self._disabled = (state == "disabled")
            alpha = "#aaaaaa"
            self._lbl.configure(fg=alpha if self._disabled else self._fg)
            super().configure(cursor="arrow" if self._disabled else "right_ptr")
        if kw:
            try:
                super().configure(**kw)
            except Exception:
                pass

    configure = config   # alias so both spellings work


# ═════════════════════════════════════════════════════════════════════════════
# APP
# ═════════════════════════════════════════════════════════════════════════════

class App(tk.Tk):
    """ScavGajjar V3 — Control Centre."""

    def __init__(self):
        super().__init__()
        self.title("ScavGajjar V3 \u2014 Control Centre")
        self.geometry("1440x900")
        self.minsize(1100, 700)
        self.configure(bg=APP_BG)
        self._header_logo_img = None
        self._window_icon_img = None
        self._load_branding_assets()
        config.ensure_dirs()
        self._tk_patchlevel = self._read_tk_patchlevel()
        self._legacy_tk_on_mac = (sys.platform == "darwin" and self._tk_patchlevel < (8, 6))
        self._session_start = time.time()
        self._awb_start_time: float | None = None

        # ── Session state ────────────────────────────────────────────────────
        self.employee_id             = ""
        self.awb_proc                = None
        self.edm_proc                = None   # compatibility placeholder
        self.edm_enabled             = is_edm_enabled()
        _session = _load_session()
        if not config.EDM_TOGGLE_FILE.exists() and "edm_enabled" in _session:
            self.edm_enabled = bool(_session.get("edm_enabled"))
        try:
            set_edm_enabled(self.edm_enabled)
        except Exception:
            pass
        self.batch_running           = False
        self.full_cycle_running      = False
        self.full_cycle_stop_event   = threading.Event()
        self.auto_phase              = "Idle"
        self.auto_running            = False
        self.auto_stop_event         = threading.Event()
        self.auto_thread             = None
        self._stats_inflight         = False
        self._perf_last_good_stats: dict | None = None   # guard against all-zero glitch reads
        self._is_closing             = False
        self._audit_offset           = 0
        self._audit_inode            = None
        self._audit_recent           = deque(maxlen=140)
        self._prev_counts: dict      = {}   # folder key → last count for delta display
        self._expected_edm_stops: set[int] = set()
        self._snapshot_icon_cache: dict = {}
        self._toolbar_icon_cache: dict = {}
        self._toolbar_icon_state_key   = None
        self._refresh_tick_counter      = 0
        self._count_refresh_job         = None
        self._last_count_scan_ts        = 0.0
        self._last_focus_refresh_ts     = 0.0
        self._enable_summary_animations = False
        self._last_match_signature   = None
        self._match_event_counter    = 0
        self._last_edm_frontend_state = None
        self._last_activity_stage = None
        self._batch_candidate_counts = {"strong": 0, "mix": 0, "weak": 0}
        self._batch_tier_totals = {"strong": 0, "mix": 0, "weak": 0}
        self._batch_candidate_reset_job = None
        self._edm_off_move_job = None
        self._perf_extra_complete = 0
        self._perf_extra_batches = 0
        self._summary_last_event_ts = {"match": 0.0, "edm": 0.0, "batch": 0.0}
        self._last_edm_event_count = 0
        self._last_batch_stat_signature = None
        self._session_stats_baseline = {
            "batches_built": 0,
            "tiffs_converted": 0,
            "batch_tier_strong": 0,
            "batch_tier_mix": 0,
            "batch_tier_weak": 0,
        }

        self._build_ui()
        self._setup_log_tags()
        self._initialize_session_audit_tail()
        self._initialize_session_stats_baseline()

        self.log_append("  ScavGajjar V3  |  INBOX -> [AWB] -> PROCESSED -> CLEAN/REJECTED -> [Batch] -> OUT")
        self.log_append(f"  Base: {config.BASE_DIR}")
        self.log_append(f"  Protected: {config.AWB_EXCEL_PATH.name}  |  {config.AWB_LOGS_PATH.name}")
        self.log_append(
            f"  EDM fallback: {'ON (API calls allowed)' if self.edm_enabled else 'OFF (API calls bypassed)'}"
        )
        self.log_append("  Ready.")

        self._refresh_live_status()
        self._request_count_refresh(0)
        self._start_count_refresh()
        # Pre-settle all grid/pack geometry before the window is first painted.
        # Without this, the right panel snaps to its final size after first render.
        self.update_idletasks()
        self.bind("<FocusIn>", self._on_app_focus_in)
        self.protocol("WM_DELETE_WINDOW", self.on_close)

        # Employee login after UI is drawn.
        # macOS + Tk 8.5 has known dialog rendering issues, so use fallback.
        if self._legacy_tk_on_mac:
            self._bootstrap_employee_id()
            tk_ver = ".".join(str(x) for x in self._tk_patchlevel)
            self.log_append(f"[UI WARN] Detected Tk {tk_ver} on macOS. Login prompt disabled (fallback employee ID in use).")
        else:
            self.after(100, self._prompt_employee_number)

        self.after(200, self._run_startup_checks)

    # ─────────────────────────────────────────────────────────────────────────
    # STARTUP CHECKS
    # ─────────────────────────────────────────────────────────────────────────
    def _run_startup_checks(self):
        """Show early warnings for missing Tesseract or AWB database."""
        if not config.TESSERACT_PATH.exists():
            messagebox.showerror(
                "Startup Error",
                f"Tesseract not found:\n{config.TESSERACT_PATH}\n\nCheck TESSERACT_PATH in .env",
            )
            self.destroy()
            sys.exit(1)
        if not config.AWB_EXCEL_PATH.exists():
            messagebox.showwarning(
                "AWB Database Missing",
                f"AWB database not found:\n{config.AWB_EXCEL_PATH}\n\n"
                "Matching will fail until it is placed there.",
            )

    def _trim_log_rows(self):
        """No-op — log is now a fixed-widget panel; no rows to trim."""

    # ─────────────────────────────────────────────────────────────────────────
    # UI CONSTRUCTION
    # ─────────────────────────────────────────────────────────────────────────
    def _load_branding_assets(self):
        """Load GJ21 brand logo for header and window icon."""
        try:
            if not LOGO_FILE.exists():
                return
            src = Image.open(LOGO_FILE).convert("RGBA")
            self._header_logo_img = ImageTk.PhotoImage(src.resize((42, 42), Image.Resampling.LANCZOS))
            self._window_icon_img = ImageTk.PhotoImage(src.resize((96, 96), Image.Resampling.LANCZOS))
            self.iconphoto(True, self._window_icon_img)
        except Exception:
            self._header_logo_img = None
            self._window_icon_img = None

    def _read_tk_patchlevel(self):
        """Return Tk patchlevel as tuple, e.g. (8, 6, 14)."""
        try:
            raw = str(self.tk.call("info", "patchlevel"))
            parts = []
            for p in raw.split("."):
                try:
                    parts.append(int(p))
                except ValueError:
                    break
            return tuple(parts) if parts else (0,)
        except Exception:
            return (0,)

    def _on_app_focus_in(self, _event=None):
        """Throttle focus-triggered refresh so child-focus changes don't spam rescans."""
        try:
            if self.state() == "iconic":
                return
        except Exception:
            return
        if self.focus_displayof() is None:
            return
        now = time.monotonic()
        if (now - self._last_focus_refresh_ts) < 1.5:
            return
        self._last_focus_refresh_ts = now
        self._request_count_refresh(80)

    def _bootstrap_employee_id(self):
        """Set employee ID from session (or fallback) without opening a dialog."""
        session = _load_session()
        val = (session.get("employee_id", "") or "").strip() or "UNKNOWN"
        self.employee_id = val
        os.environ["PIPELINE_EMPLOYEE_ID"] = val
        self.lbl_employee.config(text=f"Employee: {val}")
        if hasattr(self, "lbl_session"):
            self.lbl_session.config(text=f"Session: {val}")
        _save_session({**session, "employee_id": val})

    def _persist_edm_toggle(self):
        try:
            set_edm_enabled(self.edm_enabled)
        except Exception as e:
            self.log_append(f"[EDM] Warning: could not persist EDM toggle: {e}")
        session = _load_session()
        _save_session({**session, "edm_enabled": bool(self.edm_enabled)})

    def _apply_edm_button_state(self):
        if self.edm_enabled:
            self.btn_edm.config(
                text="EDM: ON",
                bg="#eaf2ff",
                fg=INFO,
                activebackground="#eaf2ff",
                activeforeground=INFO,
                highlightbackground="#eaf2ff",
            )
        else:
            self.btn_edm.config(
                text="EDM: OFF",
                bg="#edf1f5",
                fg=TEXT_SEC,
                activebackground="#edf1f5",
                activeforeground=TEXT_SEC,
                highlightbackground="#edf1f5",
            )
        self._update_menu_labels()
        if hasattr(self, "btn_edm"):
            self._apply_toolbar_button_icons()

    def _build_ui(self):
        self._default_fg = TEXT_FG
        _card_border  = "#d8dee7"
        _card_hdr_bg  = "#f3eefc"
        _rail_bg      = "#171c24"

        # ── Reusable widget factories ─────────────────────────────────────────

        def _btn(parent, text, cmd, width=14, bg=BTN_BG, fg=BTN_FG,
                 height=1, padx=10, pady=4, font=None):
            f = font or FONT_BTN
            def _lighten(hex_col, amount=22):
                try:
                    h = hex_col.lstrip("#")
                    r, g, b2 = int(h[0:2],16), int(h[2:4],16), int(h[4:6],16)
                    return f"#{min(255,r+amount):02x}{min(255,g+amount):02x}{min(255,b2+amount):02x}"
                except Exception:
                    return BTN_HOVER
            return _LabelBtn(
                parent, text=text, cmd=cmd, bg=bg, fg=fg,
                hover_bg=_lighten(bg), font=f,
                padx=padx, pady=pady, width=width,
            )

        def _brighten(hex_col: str, amount: int = 18) -> str:
            """Return a brighter variant of hex_col (for hover on coloured chips)."""
            try:
                h = hex_col.lstrip("#")
                r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
                r = min(255, r + amount)
                g = min(255, g + amount)
                b = min(255, b + amount)
                return f"#{r:02x}{g:02x}{b:02x}"
            except Exception:
                return hex_col

        def _card(parent, **kw):
            return tk.Frame(
                parent, bg=PANEL_BG, bd=0,
                highlightthickness=1, highlightbackground=_card_border, **kw
            )

        def _card_header(card, title):
            hf = tk.Frame(card, bg=_card_hdr_bg, bd=0,
                          highlightthickness=0)
            hf.pack(fill="x")
            tk.Frame(hf, bg=FEDEX_PURPLE, width=3).pack(side="left", fill="y")
            tk.Label(hf, text=title, font=FONT_TITLE,
                     fg=TEXT_SEC, bg=_card_hdr_bg,
                     padx=10, pady=6).pack(side="left")
            return hf

        # ── Global actions menu  (overflow items not already available on main UI)
        self._global_actions_menu = tk.Menu(self, tearoff=0)
        self._global_actions_menu.add_command(label="Open LOGS",             command=lambda: self.open_folder(config.LOG_DIR))
        self._global_actions_menu.add_command(label="Open Audit Log (JSONL)",command=lambda: self._open_file(config.AUDIT_LOG))
        self._global_actions_menu.add_command(label="Open Audit Workbook",   command=lambda: self._open_file(config.AUDIT_XLSX_PATH))
        self._global_actions_menu.add_command(label="Open EDM Log",          command=lambda: self._open_file(config.EDM_LOG))
        self._global_actions_menu.add_command(label="Open Sequence Workbook",command=lambda: self._open_file(config.SEQUENCE_XLSX))
        self._global_actions_menu.add_command(label="Export Activity Feed",   command=self._export_log)
        self._native_menubar = None
        self._menu_pipeline  = None
        self._menu_maint     = None

        # ═══════════════════════════════════════════════════════════════════════
        # HEADER  —  deep purple-black, orange accent bar on left
        # ═══════════════════════════════════════════════════════════════════════
        hdr = tk.Frame(self, bg=HEADER_BG, height=76)
        hdr.pack(fill="x")
        hdr.pack_propagate(False)

        # FedEx-orange left accent stripe
        tk.Frame(hdr, bg=FEDEX_ORANGE, width=6).pack(side="left", fill="y")
        tk.Frame(hdr, bg=HEADER_BG,    width=10).pack(side="left", fill="y")

        logo_col = tk.Frame(hdr, bg=HEADER_BG)
        logo_col.pack(side="left", fill="y", pady=10, padx=(2, 10))
        if self._header_logo_img is not None:
            tk.Label(logo_col, image=self._header_logo_img, bg=HEADER_BG).pack(anchor="w")
        else:
            tk.Label(
                logo_col,
                text="GJ21",
                font=(FONT_HEADER[0], max(FONT_HEADER[1] - 3, 12), "bold"),
                bg=HEADER_BG,
                fg=HEADER_FG,
            ).pack(anchor="w")

        # Title block
        title_col = tk.Frame(hdr, bg=HEADER_BG)
        title_col.pack(side="left", fill="y", pady=10)
        tk.Label(
            title_col, text="ScavGajjar V3",
            font=FONT_HEADER, bg=HEADER_BG, fg=HEADER_FG,
        ).pack(anchor="w")
        sub_row = tk.Frame(title_col, bg=HEADER_BG)
        sub_row.pack(anchor="w", pady=(1, 0))
        tk.Frame(sub_row, bg=FEDEX_PURPLE, width=3, height=12).pack(
            side="left", anchor="center", padx=(0, 7))
        tk.Label(
            sub_row,
            text="Operations Control Centre  ·  ⌨ Ctrl+W AWB  Ctrl+E EDM  Ctrl+B Batch  Ctrl+F Search  Ctrl+U Upload",
            font=(FONT_SMALL[0], FONT_SMALL[1]),
            bg=HEADER_BG, fg="#8f98a6",
        ).pack(side="left", anchor="center")

        # Right: employee + clock
        right_hdr = tk.Frame(hdr, bg=HEADER_BG)
        right_hdr.pack(side="right", padx=(12, 26), fill="y", pady=12)
        self.lbl_employee = tk.Label(
            right_hdr, text="Employee: —",
            font=(FONT_LABEL[0], FONT_LABEL[1], "bold"),
            bg=HEADER_BG, fg="#cfd6df",
        )
        self.lbl_employee.pack(anchor="e")
        self.lbl_clock = tk.Label(
            right_hdr, text="",
            font=(FONT_MONO[0], 14, "bold"),
            bg=HEADER_BG, fg="#e6ebf2",
        )
        self.lbl_clock.pack(anchor="e", pady=(3, 0))
        self.lbl_uptime = tk.Label(
            right_hdr, text="Up: 0m 00s",
            font=(FONT_MONO[0], 8),
            bg=HEADER_BG, fg="#8f98a6",
        )
        self.lbl_uptime.pack(anchor="e", pady=(2, 0))

        # ── Live status indicator dots (right side of header) ─────────────────
        dots_row = tk.Frame(right_hdr, bg=HEADER_BG)
        dots_row.pack(anchor="e", pady=(5, 0))

        def _dot(parent, label):
            lbl = tk.Label(
                parent, text=f"○ {label}",
                font=(FONT_SMALL[0], 8, "bold"),
                bg=HEADER_BG, fg=STRIP_IDLE,
                padx=6,
            )
            lbl.pack(side="left")
            return lbl

        self._dot_awb   = _dot(dots_row, "AWB")
        self._dot_edm   = _dot(dots_row, "EDM")
        self._dot_batch = _dot(dots_row, "BATCH")
        self._dot_auto  = _dot(dots_row, "AUTO")

        self._tick_clock()

        # NOTE: _apply_edm_button_state() is called after toolbar (needs btn_edm)

        # ═══════════════════════════════════════════════════════════════════════
        # TOOLBAR  —  primary row + secondary row
        # ═══════════════════════════════════════════════════════════════════════
        toolbar_wrap = tk.Frame(self, bg=APP_BG,
                                highlightthickness=1,
                                highlightbackground="#d8dee7")
        toolbar_wrap.pack(fill="x")

        # ── Primary row (big action buttons) ─────────────────────────────────
        row1 = tk.Frame(toolbar_wrap, bg=APP_BG)
        row1.pack(fill="x", padx=10, pady=(8, 4))
        row1_actions = tk.Frame(row1, bg=APP_BG)
        row1_actions.pack(side="left")

        self.btn_get_awb = _btn(
            row1_actions, "Start Scan", self.on_toggle_get_awb,
            width=0, bg="#4a33a2", fg="white", height=2, padx=14, pady=8, font=FONT_BTN,
        )
        self.btn_auto = _btn(
            row1_actions, "AUTO MODE", self.on_toggle_auto_mode,
            width=0, bg="#4a33a2", fg="white", height=2, padx=14, pady=8, font=FONT_BTN,
        )
        self.btn_full_cycle = _btn(
            row1_actions, "Full Cycle", self.on_run_full_cycle_once,
            width=0, bg="#4a33a2", fg="white", height=2, padx=14, pady=8, font=FONT_BTN,
        )
        self.btn_upload = _btn(
            row1_actions, "Upload Files", self.on_upload_files,
            width=0, bg="#4a33a2", fg="white", height=2, padx=14, pady=8, font=FONT_BTN,
        )
        self.btn_get_awb.pack(side="left", padx=(0, 5))
        self.btn_auto.pack(side="left",    padx=(0, 5))
        self.btn_full_cycle.pack(side="left", padx=(0, 5))
        self.btn_upload.pack(side="left",  padx=(0, 5))

        # Breadcrumb on the right of row 1
        self.lbl_breadcrumb = tk.Label(
            row1, text="Home  /  Operations  /  Idle",
            font=FONT_SMALL, fg=TEXT_MUTED, bg=APP_BG,
        )
        self.lbl_breadcrumb.pack(side="right", padx=(0, 6), pady=(2, 0))

        # ── Secondary row (smaller utility buttons) ───────────────────────────
        row2 = tk.Frame(toolbar_wrap, bg="#f1f4f8",
                        highlightthickness=1,
                        highlightbackground="#d8dee7")
        row2.pack(fill="x", padx=0, pady=0)

        row2_actions = tk.Frame(row2, bg="#f1f4f8")
        row2_actions.pack(side="left", padx=10, pady=6)
        row2_right = tk.Frame(row2, bg="#f1f4f8")
        row2_right.pack(side="right", padx=10, pady=6)

        # Create all row-2 buttons (no pack yet — order matters for pack geometry)
        self.btn_batch = _btn(
            row2_actions, "Prepare Batch", self.on_prepare_batch,
            width=0, bg=PANEL_BG, fg=TEXT_FG, padx=12, pady=4,
        )
        self.btn_tiff = _btn(
            row2_actions, "Convert TIFF", self.on_convert_tiff,
            width=0, bg=PANEL_BG, fg=TEXT_FG, padx=12, pady=4,
        )
        self.btn_retry_review = _btn(
            row2_actions, "Retry Failed", self.on_retry_needs_review,
            width=0, bg=PANEL_BG, fg=TEXT_FG, padx=12, pady=4,
        )
        self.btn_edm_runner = _btn(
            row2_actions, "Start EDM Check", self.on_toggle_edm_duplicate_checker,
            width=0, bg="#eef3fb", fg=INFO, padx=12, pady=4,
        )
        self.btn_edm = _btn(
            row2_actions, "EDM: OFF", self.on_toggle_edm_checker,
            width=0, bg="#edf1f5", fg=TEXT_SEC, padx=12, pady=4,
        )
        self.btn_clear_all = _btn(
            row2_actions, "Clear All", self.on_clear_all,
            width=0, bg="#f2f4f7", fg=TEXT_SEC, padx=12, pady=4,
        )
        self.btn_global_actions = _btn(
            row2_right, "More", self._open_global_actions_menu,
            width=0, bg="#e4e9ef", fg=TEXT_FG, padx=12, pady=4,
        )

        # Pack left-side items in display order (left → right)
        self.btn_batch.pack(side="left",        padx=(0, 4))
        self.btn_tiff.pack(side="left",         padx=(0, 4))
        self.btn_retry_review.pack(side="left", padx=(0, 4))
        self.btn_edm_runner.pack(side="left",   padx=(0, 4))
        self.btn_edm.pack(side="left",          padx=(0, 4))
        self.btn_clear_all.pack(side="left",    padx=(0, 4))
        self.btn_global_actions.pack(side="right")

        # btn_edm now exists — apply initial EDM button appearance
        self._apply_edm_button_state()
        self._apply_toolbar_button_icons()

        # ── Keyboard shortcuts ────────────────────────────────────────────────
        self.bind("<Control-w>",       lambda _e: self.on_toggle_get_awb())
        self.bind("<Control-W>",       lambda _e: self.on_toggle_get_awb())
        self.bind("<Control-b>",       lambda _e: self.on_prepare_batch())
        self.bind("<Control-B>",       lambda _e: self.on_prepare_batch())
        self.bind("<Control-t>",       lambda _e: self.on_convert_tiff())
        self.bind("<Control-T>",       lambda _e: self.on_convert_tiff())
        self.bind("<Control-r>",       lambda _e: self._refresh_counts())
        self.bind("<Control-R>",       lambda _e: self._refresh_counts())
        self.bind("<Control-l>",       lambda _e: self.clear_log())
        self.bind("<Control-L>",       lambda _e: self.clear_log())
        self.bind("<Control-f>",       lambda _e: self._focus_log_search())
        self.bind("<Control-F>",       lambda _e: self._focus_log_search())
        self.bind("<Control-u>",       lambda _e: self.on_upload_files())
        self.bind("<Control-U>",       lambda _e: self.on_upload_files())
        self.bind("<Control-e>",       lambda _e: self.on_toggle_edm_duplicate_checker())
        self.bind("<Control-E>",       lambda _e: self.on_toggle_edm_duplicate_checker())

        # Hidden compat references (clear_log, open_audit, etc. — not visible)
        self._hidden_btn_bank = tk.Frame(self, bg=APP_BG)
        self.btn_clear_log    = _btn(self._hidden_btn_bank, "Clear Log",       self.clear_log,                              width=10)
        self.btn_open_audit   = _btn(self._hidden_btn_bank, "📋 Open Audit",   lambda: self._open_file(config.AUDIT_LOG),   width=16)
        self.btn_open_sequence= _btn(self._hidden_btn_bank, "📊 Sequence",     lambda: self._open_file(config.SEQUENCE_XLSX),width=16)
        self.btn_export_log   = _btn(self._hidden_btn_bank, "↓ Export Log",    self._export_log,                            width=12)

        # ═══════════════════════════════════════════════════════════════════════
        # MAIN CONTENT  —  side rail | left panel | right panel
        # ═══════════════════════════════════════════════════════════════════════
        main = tk.Frame(self, bg=APP_BG)
        main.pack(fill="both", expand=True, padx=10, pady=(6, 6))
        self.main_area = main
        main.grid_columnconfigure(0, weight=2)   # left panel
        main.grid_columnconfigure(1, weight=3)   # right panel
        main.grid_rowconfigure(0, weight=1)

        # ── Left panel ────────────────────────────────────────────────────────
        left = tk.Frame(main, bg=APP_BG)
        left.grid(row=0, column=0, sticky="nsew", padx=(0, 8))
        left.grid_columnconfigure(0, weight=1)
        left.grid_rowconfigure(1, weight=1)   # Live Activity expands

        # ── Operations Snapshot (folder count tiles) ──────────────────────────
        snap = _card(left)
        snap.grid(row=0, column=0, sticky="ew", pady=(0, 6))
        _card_header(snap, "OPERATIONS SNAPSHOT")

        snap_status_row = tk.Frame(snap, bg="#f7f9fc", height=26)
        snap_status_row.pack(fill="x", padx=10, pady=(5, 1))
        snap_status_row.pack_propagate(False)
        self.lbl_status_strip = tk.Label(
            snap_status_row,
            text="System stable  ·  Queue idle  ·  No review",
            font=FONT_SMALL,
            bg="#f7f9fc",
            fg=TEXT_SEC,
            anchor="w",
            padx=4,
            pady=4,
        )
        self.lbl_status_strip.pack(side="left", fill="x")

        # Separator colour shows through the 1px inter-tile gap
        tiles_frame = tk.Frame(snap, bg="#bfcfe0")
        tiles_frame.pack(fill="x", padx=6, pady=(4, 4))
        for _ci in range(7):
            tiles_frame.columnconfigure(_ci, weight=1, uniform="snapshot")
        tiles_frame.rowconfigure(0, weight=1, minsize=130)

        def _count_tile(parent, row, col, label, click_cmd=None):
            """Return the count label for a metric tile."""
            _ACC_IDLE  = "#b8d0ea"
            _ACC_HOVER = "#3d7fcb"
            _HOVER_BG  = "#e6f0ff"
            _PRESS_BG  = "#d2e4f8"

            # 1px right gap (except last col) exposes tiles_frame bg as separator
            tile = tk.Frame(parent, bg=PANEL_BG, cursor="arrow")
            tile.grid(row=row, column=col,
                      padx=(0, 1 if col < 6 else 0), pady=0, sticky="nsew")
            tile.grid_propagate(False)

            # 3px left accent stripe — changes colour on hover for a crisp visual cue
            accent = tk.Frame(tile, bg=_ACC_IDLE, width=3)
            accent.pack(side="left", fill="y")

            inner = tk.Frame(tile, bg=PANEL_BG, padx=6, pady=7)
            inner.pack(fill="both", expand=True)
            icon_color = "#90a3bf"

            icon_wrap = tk.Frame(inner, bg=PANEL_BG)
            icon_wrap.pack(fill="x")
            icon_img = self._snapshot_icon_image(label, icon_color, 21)
            icon_badge = tk.Label(
                icon_wrap, image=icon_img, bg=PANEL_BG,
                bd=0, highlightthickness=0,
            )
            icon_badge.image = icon_img
            icon_badge.pack(anchor="center")

            title_lbl = tk.Label(
                inner, text=label, font=(FONT_SMALL[0], 8, "bold"),
                fg=TEXT_MUTED, bg=PANEL_BG,
            )
            title_lbl.pack(anchor="center", pady=(5, 0))

            cnt = tk.Label(
                inner, text="0", font=(FONT_COUNT[0], 20, "bold"),
                fg=TEXT_FG, bg=PANEL_BG,
                anchor="center",
            )
            cnt.pack(anchor="center", pady=(6, 0))

            delta_lbl = tk.Label(
                inner, text=" ",
                font=(FONT_SMALL[0], FONT_SMALL[1]),
                fg=TEXT_MUTED, bg=PANEL_BG,
                anchor="center",
            )
            delta_lbl.pack(anchor="center", pady=(2, 0))

            cnt._delta_lbl = delta_lbl
            cnt._icon_badge = icon_badge
            cnt._icon_color = icon_color

            # Hover + click + press feedback
            if click_cmd:
                _all_widgets = (tile, inner, icon_wrap, cnt, title_lbl, delta_lbl, icon_badge)

                def _apply(bg, acc, a=accent, ws=_all_widgets):
                    try:
                        a.config(bg=acc)
                        for w in ws:
                            w.config(bg=bg)
                    except Exception:
                        pass

                def _on_enter(_, cmd=click_cmd):
                    _apply(_HOVER_BG, _ACC_HOVER)
                    try: tile.config(cursor="right_ptr")
                    except Exception: pass

                def _on_leave(_):
                    _apply(PANEL_BG, _ACC_IDLE)
                    try: tile.config(cursor="arrow")
                    except Exception: pass

                def _on_press(_):
                    _apply(_PRESS_BG, _ACC_HOVER)

                def _on_release(_, cmd=click_cmd):
                    _apply(_HOVER_BG, _ACC_HOVER)
                    try: cmd()
                    except Exception: pass

                for w in _all_widgets + (accent,):
                    w.bind("<Enter>",           _on_enter)
                    w.bind("<Leave>",           _on_leave)
                    w.bind("<ButtonPress-1>",   _on_press)
                    w.bind("<ButtonRelease-1>", _on_release)

            # store accent frame reference on cnt for _refresh_counts
            cnt._accent = accent
            return cnt

        self.lbl_inbox     = _count_tile(tiles_frame, 0, 0, "INBOX",
                                         lambda: self.open_folder(config.INBOX_DIR))
        self.lbl_processed = _count_tile(tiles_frame, 0, 1, "PROCESSED",
                                         lambda: self.open_folder(config.PROCESSED_DIR))
        self.lbl_clean     = _count_tile(tiles_frame, 0, 2, "CLEAN",
                                         lambda: self.open_folder(config.CLEAN_DIR))
        self.lbl_rejected  = _count_tile(tiles_frame, 0, 3, "REJECTED",
                                         lambda: self.open_folder(config.REJECTED_DIR))
        self.lbl_review  = _count_tile(tiles_frame, 0, 4, "REVIEW",
                                         lambda: self.open_folder(config.NEEDS_REVIEW_DIR))
        self.lbl_out     = _count_tile(tiles_frame, 0, 5, "OUT",
                                         lambda: self.open_folder(config.OUT_DIR))
        self.lbl_pending = _count_tile(tiles_frame, 0, 6, "PENDING",
                                         lambda: self.open_folder(config.PENDING_PRINT_DIR))


        # ── Right panel ───────────────────────────────────────────────────────
        right = tk.Frame(main, bg=APP_BG)
        right.grid(row=0, column=1, sticky="nsew")
        right.grid_columnconfigure(0, weight=1)
        right.grid_rowconfigure(0, weight=0)
        right.grid_rowconfigure(1, weight=1)

        # ── Run Overview ──────────────────────────────────────────────────────
        ov_card = _card(right)
        ov_card.grid(row=0, column=0, sticky="ew", pady=(0, 6))
        _card_header(ov_card, "RUN OVERVIEW")

        ov_body = tk.Frame(ov_card, bg=PANEL_BG)
        ov_body.pack(fill="x", padx=14, pady=10)

        self.lbl_run_mode = tk.Label(
            ov_body, text="Mode: IDLE",
            font=(FONT_LABEL[0], 10, "bold"), fg=TEXT_FG, bg=PANEL_BG, anchor="w",
        )
        self.lbl_run_mode.pack(fill="x")
        self.lbl_active_jobs = tk.Label(
            ov_body, text="Active jobs: none",
            font=FONT_SMALL, fg=TEXT_SEC, bg=PANEL_BG, anchor="w",
        )
        self.lbl_active_jobs.pack(fill="x", pady=(4, 0))
        self.lbl_run_status = tk.Label(
            ov_body, text="Status: Ready.",
            font=FONT_SMALL, fg=TEXT_SEC, bg=PANEL_BG, anchor="w",
        )
        self.lbl_run_status.pack(fill="x", pady=(2, 0))
        self.step_icon_row = tk.Frame(ov_body, bg=PANEL_BG, height=24)
        self.step_icon_row.pack(fill="x", pady=(7, 0))
        self.step_icon_row.pack_propagate(False)
        self._step_icons: dict[str, tuple[tk.Label, str]] = {}
        for _step_name, _active_color in [
            ("INBOX", "#2a5ca8"),
            ("AWB", FEDEX_PURPLE),
            ("EDM", "#9b55e0"),
            ("BATCH", INFO),
            ("OUT", OK),
        ]:
            _lbl = tk.Label(
                self.step_icon_row,
                text=f"○ {_step_name}",
                font=(FONT_SMALL[0], 8, "bold"),
                fg=TEXT_MUTED,
                bg="#eaf0f9",
                padx=6,
                pady=2,
            )
            _lbl.pack(side="left", padx=(0, 4))
            self._step_icons[_step_name] = (_lbl, _active_color)

        tk.Frame(ov_body, bg=_card_border, height=1).pack(fill="x", pady=(8, 6))

        self._default_run_hint = "Tip: use  ⋯ More  for folders, maintenance, and advanced operations."
        self.lbl_run_hint = tk.Label(
            ov_body, text=self._default_run_hint,
            font=FONT_SMALL, fg=TEXT_MUTED, bg=PANEL_BG, anchor="w",
            height=2, justify="left",
        )
        self.lbl_run_hint.pack(fill="x", pady=(2, 0))

        # ── Performance (right panel, row 1) ──────────────────────────────────
        perf_card = _card(right)
        perf_card.grid(row=1, column=0, sticky="nsew")
        perf_card.grid_columnconfigure(0, weight=1)
        _card_header(perf_card, "PERFORMANCE")

        perf_body = tk.Frame(perf_card, bg=PANEL_BG)
        perf_body.pack(fill="both", expand=True, padx=14, pady=12)

        # ── Today's date + avg processing time ───────────────────────────────
        perf_meta_row = tk.Frame(perf_body, bg=PANEL_BG)
        perf_meta_row.pack(fill="x", pady=(0, 8))
        self._perf_date_lbl = tk.Label(
            perf_meta_row,
            text=f"Today  ·  {time.strftime('%A, %d %b %Y')}",
            font=(FONT_SMALL[0], 8), fg=TEXT_MUTED, bg=PANEL_BG, anchor="w",
        )
        self._perf_date_lbl.pack(side="left")
        self._perf_avg_lbl = tk.Label(
            perf_meta_row, text="Avg: —",
            font=(FONT_SMALL[0], 8), fg=TEXT_SEC, bg=PANEL_BG, anchor="e",
        )
        self._perf_avg_lbl.pack(side="right")

        # ── Big metric tiles (Processed / Complete / Review / Failed) ─────────
        metric_row = tk.Frame(perf_body, bg=PANEL_BG)
        metric_row.pack(fill="x", pady=(0, 12))
        for _ci in range(3):
            metric_row.columnconfigure(_ci, weight=1)

        self._stat_labels = {}
        metric_row.rowconfigure(0, minsize=70)

        def _perf_tile(col, label, key, fg_color, tile_bg):
            tile = tk.Frame(metric_row, bg=tile_bg, bd=0,
                            highlightthickness=1, highlightbackground="#ccd7e8")
            tile.grid(row=0, column=col, padx=(0 if col == 0 else 5, 0), sticky="nsew")
            tile.grid_propagate(False)
            tk.Label(tile, text=label, font=(FONT_SMALL[0], 7, "bold"),
                     fg=TEXT_MUTED, bg=tile_bg).pack(anchor="center", pady=(10, 0))
            val = tk.Label(tile, text="0", font=(FONT_COUNT[0], 22, "bold"),
                           fg=fg_color, bg=tile_bg)
            val.pack(anchor="center", pady=(3, 10))
            self._stat_labels[key] = val

        _perf_tile(0, "PROCESSED", "hot_total",    TEXT_FG,  "#f0f5fc")
        _perf_tile(1, "MATCH",     "hot_complete", OK,       "#edfaf2")
        _perf_tile(2, "REVIEW",    "hot_review",   REVIEW,   "#fff8ec")

        # ── Success rate bar ──────────────────────────────────────────────────
        rate_frame = tk.Frame(perf_body, bg=PANEL_BG)
        rate_frame.pack(fill="x", pady=(0, 12))

        rate_hdr = tk.Frame(rate_frame, bg=PANEL_BG)
        rate_hdr.pack(fill="x")
        tk.Label(rate_hdr, text="SUCCESS RATE", font=(FONT_SMALL[0], 8, "bold"),
                 fg=TEXT_MUTED, bg=PANEL_BG).pack(side="left")
        self._perf_rate_lbl = tk.Label(rate_hdr, text="—",
                                       font=(FONT_SMALL[0], 8), fg=TEXT_SEC, bg=PANEL_BG)
        self._perf_rate_lbl.pack(side="right")

        bar_bg_frame = tk.Frame(rate_frame, bg="#dce5f0", height=8)
        bar_bg_frame.pack(fill="x", pady=(5, 0))
        bar_bg_frame.pack_propagate(False)
        self._perf_bar_fill = tk.Frame(bar_bg_frame, bg=OK, height=8)
        self._perf_bar_fill.place(relx=0, rely=0, relwidth=0.0, relheight=1.0)

        # ── EDM + Batch sub-panels ────────────────────────────────────────────
        sub_row = tk.Frame(perf_body, bg=PANEL_BG)
        sub_row.pack(fill="x")
        sub_row.columnconfigure(0, weight=1)
        sub_row.columnconfigure(1, weight=1)

        # EDM / Duplicates
        edm_sub = tk.Frame(sub_row, bg="#f7f9fc", bd=0,
                           highlightthickness=1, highlightbackground="#ccd7e8")
        edm_sub.grid(row=0, column=0, sticky="nsew", padx=(0, 5))
        tk.Label(edm_sub, text="EDM",
                 font=(FONT_SMALL[0], 7, "bold"), fg=TEXT_MUTED, bg="#f7f9fc").pack(
                     anchor="w", padx=10, pady=(9, 2))
        edm_top = tk.Frame(edm_sub, bg="#f7f9fc")
        edm_top.pack(fill="x", padx=10)
        edm_top.columnconfigure(0, weight=1)
        edm_clean_n = tk.Label(edm_top, text="0",
                               font=(FONT_COUNT[0], 20, "bold"), fg=OK, bg="#f7f9fc")
        edm_clean_n.grid(row=0, column=0, sticky="w")
        tk.Label(edm_top, text="clean", font=(FONT_SMALL[0], 8),
                 fg=TEXT_MUTED, bg="#f7f9fc").grid(row=0, column=1, sticky="sw",
                                                    padx=(3, 0), pady=(0, 3))
        self._stat_labels["edm_clean"] = edm_clean_n
        edm_detail = tk.Frame(edm_sub, bg="#f7f9fc")
        edm_detail.pack(fill="x", padx=10, pady=(4, 9))
        edm_partial_lbl = tk.Label(edm_detail, text="Partial-clean: 0",
                                   font=FONT_SMALL, fg=TEXT_SEC, bg="#f7f9fc", anchor="w")
        edm_partial_lbl.pack(fill="x")
        edm_rej_lbl = tk.Label(edm_detail, text="Rejected: 0",
                               font=FONT_SMALL, fg=self._default_fg, bg="#f7f9fc", anchor="w")
        edm_rej_lbl.pack(fill="x")
        tk.Frame(edm_detail, bg="#dde5f0", height=1).pack(fill="x", pady=(4, 2))
        edm_checked_lbl = tk.Label(edm_detail, text="Checked today: 0",
                                   font=(FONT_SMALL[0], FONT_SMALL[1]),
                                   fg=TEXT_MUTED, bg="#f7f9fc", anchor="w")
        edm_checked_lbl.pack(fill="x")
        self._stat_labels["edm_rejected"] = edm_rej_lbl
        self._perf_edm_partial_lbl = edm_partial_lbl
        self._perf_edm_total_lbl = edm_checked_lbl

        # Batch / Output
        batch_sub = tk.Frame(sub_row, bg="#f7f9fc", bd=0,
                             highlightthickness=1, highlightbackground="#ccd7e8")
        batch_sub.grid(row=0, column=1, sticky="nsew", padx=(5, 0))
        tk.Label(batch_sub, text="BATCH / OUTPUT",
                 font=(FONT_SMALL[0], 7, "bold"), fg=TEXT_MUTED, bg="#f7f9fc").pack(
                     anchor="w", padx=10, pady=(9, 2))
        batch_top = tk.Frame(batch_sub, bg="#f7f9fc")
        batch_top.pack(fill="x", padx=10)
        batch_top.columnconfigure(0, weight=1)
        batch_n = tk.Label(batch_top, text="0",
                           font=(FONT_COUNT[0], 20, "bold"), fg=INFO, bg="#f7f9fc")
        batch_n.grid(row=0, column=0, sticky="w")
        tk.Label(batch_top, text="batches", font=(FONT_SMALL[0], 8),
                 fg=TEXT_MUTED, bg="#f7f9fc").grid(row=0, column=1, sticky="sw",
                                                    padx=(3, 0), pady=(0, 3))
        self._stat_labels["batches_built"] = batch_n
        batch_tiff_lbl = tk.Label(batch_sub, text="TIFFs: 0",
                                  font=FONT_SMALL, fg=INFO, bg="#f7f9fc", anchor="w")
        batch_tiff_lbl.pack(fill="x", padx=10, pady=(4, 2))
        self._stat_labels["tiffs"] = batch_tiff_lbl
        batch_tier_lbl = tk.Label(batch_sub, text="Tier  S: 0  ·  M: 0  ·  W: 0",
                                  font=FONT_SMALL, fg=TEXT_SEC, bg="#f7f9fc", anchor="w")
        batch_tier_lbl.pack(fill="x", padx=10, pady=(0, 9))
        self._stat_labels["batch_tiers"] = batch_tier_lbl

        # ── All-time summary strip ─────────────────────────────────────────────
        alltime_strip = tk.Frame(perf_body, bg="#eef1f7", bd=0,
                                 highlightthickness=1, highlightbackground="#ccd7e8")
        alltime_strip.pack(fill="x", pady=(10, 0))
        tk.Label(alltime_strip, text="ALL TIME",
                 font=(FONT_SMALL[0], 7, "bold"), fg=TEXT_MUTED, bg="#eef1f7",
                 padx=10, pady=6).pack(side="left")
        self._alltime_summary_lbl = tk.Label(
            alltime_strip,
            text="—  matched  ·  —  review  ·  —  failed",
            font=(FONT_SMALL[0], 8), fg=TEXT_SEC, bg="#eef1f7",
        )
        self._alltime_summary_lbl.pack(side="right", padx=10)

        # ── Live Activity (left panel, row 1) ─────────────────────────────────
        tl_card = _card(left)
        tl_card.grid(row=1, column=0, sticky="nsew")
        tl_card.grid_rowconfigure(1, weight=1)
        tl_card.grid_columnconfigure(0, weight=1)

        tl_hdr = tk.Frame(tl_card, bg=_card_hdr_bg, bd=0, highlightthickness=0)
        tl_hdr.grid(row=0, column=0, sticky="ew")
        tk.Frame(tl_hdr, bg=FEDEX_PURPLE, width=3).pack(side="left", fill="y")
        tk.Label(
            tl_hdr, text="LIVE ACTIVITY",
            font=FONT_TITLE, fg=TEXT_SEC, bg=_card_hdr_bg,
            padx=10, pady=6,
        ).pack(side="left")
        self.btn_clear_log_inline = _btn(
            tl_hdr, "Clear", self.clear_log,
            width=9, bg="#fff5f5", fg="#d14a4a", padx=10, pady=4,
        )
        self.btn_clear_log_inline.pack(side="left", padx=(0, 10), pady=4)

        # ── Fixed-widget activity body (no dynamic widget creation at runtime) ─
        activity_body = tk.Frame(tl_card, bg=PANEL_BG)
        activity_body.grid(row=1, column=0, sticky="nsew", padx=14, pady=12)
        activity_body.grid_columnconfigure(0, weight=1)
        activity_body.grid_columnconfigure(1, weight=1)

        # Stage status pills — 1×3 row, only .config() updates during runtime
        _IDLE_DOT = "#c8d0da"
        self._stage_pills: dict = {}  # key → (frame, dot_canvas, text_label, active_color, dim_color)
        _pill_defs = [
            ("AWB",   "Searching for AWBs",  "#4a33a2", "#2d1f63", 0, 0),
            ("EDM",   "Checking duplicates", "#1f78d1", "#134a83", 0, 1),
            ("BATCH", "Building batches",    "#2f9d57", "#1b5e34", 0, 2),
        ]
        activity_body.grid_columnconfigure(2, weight=1)
        for _sk, _pill_txt, _acol, _dcol, _pr, _pc in _pill_defs:
            _pf = tk.Frame(activity_body, bg=PANEL_BG, bd=0, highlightthickness=0)
            _pf.grid(row=_pr, column=_pc, sticky="ew", padx=(0, 8), pady=(0, 8))
            _dot = tk.Canvas(_pf, width=9, height=9, bg=PANEL_BG,
                             highlightthickness=0, bd=0)
            _dot.create_oval(1, 1, 8, 8, fill=_IDLE_DOT, outline=_IDLE_DOT, tags="dot")
            _dot.pack(side="left", padx=(0, 6))
            _lbl = tk.Label(_pf, text=_pill_txt, font=FONT_SMALL,
                            fg=TEXT_MUTED, bg=PANEL_BG, anchor="w")
            _lbl.pack(side="left", fill="x")
            self._stage_pills[_sk] = (_pf, _dot, _lbl, _acol, _dcol)
        self._stage_active_jobs: dict = {}
        self._blink_dot_phase: dict = {}  # stage_key → bool (True = bright phase)
        self.after(600, self._blink_tick)  # stage_key → after() job id

        # Separator
        tk.Frame(activity_body, bg="#e0e6f0", height=1).grid(
            row=2, column=0, columnspan=3, sticky="ew", pady=(2, 10))

        # Recent Matches header
        tk.Label(
            activity_body, text="RECENT MATCHES",
            font=(FONT_SMALL[0], 8, "bold"), fg=TEXT_MUTED, bg=PANEL_BG, anchor="w",
        ).grid(row=3, column=0, columnspan=3, sticky="ew", pady=(0, 4))

        # 3 fixed recent-match rows — alternating bg + 1px separators between rows
        _MATCH_ROW_BG = [PANEL_BG, "#f0f5fb", PANEL_BG]
        self._recent_awb_lbls: list = []  # list of (dot_canvas, awb_label, time_label)
        for _i in range(3):
            if _i > 0:
                # 1px separator between rows
                tk.Frame(activity_body, bg="#d8e4f4", height=1).grid(
                    row=4 + (_i * 2) - 1, column=0, columnspan=3, sticky="ew")
            _row_bg = _MATCH_ROW_BG[_i]
            _rf = tk.Frame(activity_body, bg=_row_bg, bd=0, highlightthickness=0)
            _rf.grid(row=4 + (_i * 2), column=0, columnspan=3, sticky="ew", pady=(2, 2))
            _d2 = tk.Canvas(_rf, width=9, height=9, bg=_row_bg,
                            highlightthickness=0, bd=0)
            _d2.create_oval(1, 1, 8, 8, fill=_IDLE_DOT, outline=_IDLE_DOT, tags="dot")
            _d2.pack(side="left", padx=(2, 6))
            _l2 = tk.Label(_rf, text="—", font=FONT_SMALL,
                           fg=TEXT_MUTED, bg=_row_bg, anchor="w")
            _l2.pack(side="left", fill="x", expand=True)
            _t2 = tk.Label(_rf, text="", font=(FONT_MONO[0], FONT_SMALL[1] - 1),
                           fg=TEXT_MUTED, bg=_row_bg, anchor="e")
            _t2.pack(side="right", padx=(4, 4))
            self._recent_awb_lbls.append((_d2, _l2, _t2))

        # Separator (shifted down to row 9 to make room for inter-row separators)
        tk.Frame(activity_body, bg="#e0e6f0", height=1).grid(
            row=9, column=0, columnspan=3, sticky="ew", pady=(10, 6))

        # Status line — left: status text; right: time + current file being processed
        _status_row_frame = tk.Frame(activity_body, bg=PANEL_BG)
        _status_row_frame.grid(row=10, column=0, columnspan=3, sticky="ew")
        self._live_status_lbl = tk.Label(
            _status_row_frame, text="Ready",
            font=FONT_SMALL, fg=TEXT_MUTED, bg=PANEL_BG, anchor="w",
        )
        self._live_status_lbl.pack(side="left", fill="x", expand=True)
        self._current_file_lbl = tk.Label(
            _status_row_frame, text="",
            font=(FONT_MONO[0], FONT_SMALL[1] - 1), fg=TEXT_MUTED, bg=PANEL_BG, anchor="e",
        )
        self._current_file_lbl.pack(side="right", padx=(6, 0))

        # Back-compat stubs (export / match-card code still references these)
        self._match_cards = deque(maxlen=5)
        self._match_card_labels = []
        self._search_var = tk.StringVar(value="")
        self._severity_var = tk.StringVar(value="All")
        self._autoscroll = tk.BooleanVar(value=True)
        self._wrap_log = tk.BooleanVar(value=True)
        self._log_rows = []
        self._log_search_job = None
        self._log_filter_refresh_job = None
        self._ui_log_max_rows = 250
        self._last_activity_stage = None

        # Hidden plain-text mirror (for log export)
        self.log_widget = scrolledtext.ScrolledText(
            self, wrap=tk.WORD, height=1, font=FONT_MONO,
        )
        self.log_widget.configure(
            state="disabled", bg="#fafcff", fg=TEXT_FG,
            insertbackground=TEXT_FG,
        )
        self._log_lines = []
        self._log_export_lines = []

        # ═══════════════════════════════════════════════════════════════════════
        # STATUS BAR + BOTTOM BAR
        # ═══════════════════════════════════════════════════════════════════════
        self.status_var = tk.StringVar(value="Ready.")
        tk.Label(
            self, textvariable=self.status_var, anchor="w",
            font=FONT_SMALL, fg=TEXT_MUTED, bg=APP_BG,
        ).pack(fill="x", padx=10, pady=(4, 0))

        bottom_bar = tk.Frame(
            self, bg="#e6ebf1", bd=0,
            highlightthickness=1, highlightbackground="#d7dde6",
        )
        bottom_bar.pack(side="bottom", fill="x")

        bb_inner = tk.Frame(bottom_bar, bg="#e6ebf1")
        bb_inner.pack(fill="x", padx=12, pady=5)

        bb_left = tk.Frame(bb_inner, bg="#e6ebf1")
        bb_left.pack(side="left", fill="x", expand=True)
        bb_right = tk.Frame(bb_inner, bg="#e6ebf1")
        bb_right.pack(side="right")

        tk.Label(
            bb_left,
            text=f"ScavGajjar V3  ·  Base: {config.BASE_DIR.name}",
            font=FONT_SMALL, fg=TEXT_SEC, bg="#e6ebf1",
        ).pack(side="left")

        self.lbl_health_summary = tk.Label(
            bb_left, text="● Healthy",
            font=(FONT_SMALL[0], FONT_SMALL[1], "bold"),
            fg=OK, bg="#e6ebf1",
            padx=8,
        )
        self.lbl_health_summary.pack(side="left", padx=(16, 0))

        self.lbl_last_refresh = tk.Label(
            bb_left, text="Refreshed: —",
            font=FONT_SMALL, fg=TEXT_MUTED, bg="#e6ebf1",
        )
        self.lbl_last_refresh.pack(side="left", padx=(10, 0))

        self.lbl_session = tk.Label(
            bb_left, text="Session: —",
            font=FONT_SMALL, fg=TEXT_SEC, bg="#e6ebf1", cursor="arrow",
        )
        self.lbl_session.pack(side="left", padx=18)
        self.lbl_session.bind("<Button-1>", lambda _e: self._prompt_employee_number())

        self.lbl_quick_check = tk.Label(
            bb_left, text="Quick check: waiting for EDM activity",
            font=FONT_SMALL, fg=TEXT_MUTED, bg="#e6ebf1", anchor="w",
        )
        self.lbl_quick_check.pack(side="left", padx=(4, 0))

        tk.Button(
            bb_right, text="Config", font=FONT_SMALL, command=lambda: self._open_file(config.BASE_DIR / ".env"),
            relief="flat", padx=8, pady=2,
            bg="#dbe2ea", fg=TEXT_FG,
            activebackground="#cfd7e1",
            highlightbackground="#dbe2ea",
            cursor="right_ptr",
        ).pack(side="right", padx=(4, 0))
        tk.Button(
            bb_right, text="Open DB", font=FONT_SMALL, command=lambda: self._open_file(config.AWB_EXCEL_PATH),
            relief="flat", padx=8, pady=2,
            bg="#dbe2ea", fg=TEXT_FG,
            activebackground="#cfd7e1",
            highlightbackground="#dbe2ea",
            cursor="right_ptr",
        ).pack(side="right", padx=(4, 0))
        tk.Button(
            bb_right, text="Refresh DB", font=FONT_SMALL, command=self.on_refresh_db,
            relief="flat", padx=8, pady=2,
            bg="#dbe2ea", fg=TEXT_FG,
            activebackground="#cfd7e1",
            highlightbackground="#dbe2ea",
            cursor="right_ptr",
        ).pack(side="right", padx=(4, 0))

    # ─────────────────────────────────────────────────────────────────────────
    # LOG TAG SETUP
    # ─────────────────────────────────────────────────────────────────────────
    def _setup_log_tags(self):
        self._log_tag_styles = {}
        for tag_name, (fg, bg), _ in LOG_TAGS:
            self._log_tag_styles[tag_name] = (fg, bg)
            kw = {}
            if fg:
                kw["foreground"] = fg
            if bg:
                kw["background"] = bg
            self.log_widget.tag_configure(tag_name, **kw)
        self.log_widget.tag_configure("search_highlight", background="#fff3b0")

    def _update_menu_labels(self):
        pass  # menu items that had dynamic labels were removed (no longer duplicated in toolbar)

    def _open_global_actions_menu(self):
        if not hasattr(self, "btn_global_actions"):
            return
        try:
            x = self.btn_global_actions.winfo_rootx()
            y = self.btn_global_actions.winfo_rooty() + self.btn_global_actions.winfo_height()
            self._global_actions_menu.tk_popup(x, y)
        finally:
            try:
                self._global_actions_menu.grab_release()
            except Exception:
                pass

    def _toggle_side_rail(self):
        self._rail_expanded = not self._rail_expanded
        for btn, icon, label in getattr(self, "_rail_buttons", []):
            btn.config(text=f"{icon}  {label}" if self._rail_expanded else icon, width=18 if self._rail_expanded else 4)
        if self._rail_expanded:
            self.btn_toggle_rail.config(text="☰ Rail: Expanded")
            try:
                self.side_rail.config(width=170)
                self.main_area.grid_columnconfigure(0, minsize=170)
            except Exception:
                pass
        else:
            self.btn_toggle_rail.config(text="☰ Rail")
            try:
                self.side_rail.config(width=52)
                self.main_area.grid_columnconfigure(0, minsize=52)
            except Exception:
                pass

    def _compact_text(self, text: str, max_len: int = 120) -> str:
        clean = " ".join(str(text).split())
        if len(clean) <= max_len:
            return clean
        return clean[: max_len - 3].rstrip() + "..."

    def _extract_timing_ms(self, timings) -> float | None:
        if not isinstance(timings, dict):
            return None
        for key in ("total_active_ms", "total_active", "total_ms", "elapsed_ms"):
            val = timings.get(key)
            try:
                if val is None:
                    continue
                return float(val)
            except Exception:
                continue
        return None

    def _format_timing_ms(self, value) -> str:
        try:
            ms = float(value)
        except Exception:
            return "—"
        if ms < 1000:
            return f"{ms:.0f}ms"
        return f"{(ms / 1000.0):.2f}s"

    def _format_seconds_only(self, value) -> str:
        try:
            ms = float(value)
        except Exception:
            return ""
        return f"{(ms / 1000.0):.2f}s"

    def _extract_total_active_ms_from_timing_line(self, text: str) -> float | None:
        src = str(text or "")
        m = re.search(r"total_active_ms\s*=\s*([0-9]+(?:\.[0-9]+)?)", src, flags=re.IGNORECASE)
        if not m:
            return None
        try:
            return float(m.group(1))
        except Exception:
            return None

    def _hex_to_rgb(self, value: str) -> tuple[int, int, int]:
        value = value.lstrip("#")
        return tuple(int(value[i:i + 2], 16) for i in (0, 2, 4))

    def _rgb_to_hex(self, rgb: tuple[int, int, int]) -> str:
        return "#{:02x}{:02x}{:02x}".format(*rgb)

    def _mix_hex(self, start: str, end: str, ratio: float) -> str:
        sr, sg, sb = self._hex_to_rgb(start)
        er, eg, eb = self._hex_to_rgb(end)
        out = (
            int(sr + (er - sr) * ratio),
            int(sg + (eg - sg) * ratio),
            int(sb + (eb - sb) * ratio),
        )
        return self._rgb_to_hex(out)

    def _animate_label_color(self, label, start: str, end: str, steps: int = 7, delay_ms: int = 34):
        if not self._enable_summary_animations:
            try:
                label.config(fg=end)
            except Exception:
                pass
            return
        try:
            prev_job = getattr(label, "_color_anim_job", None)
            if prev_job:
                self.after_cancel(prev_job)
        except Exception:
            pass

        def _tick(idx: int = 0):
            ratio = min(1.0, idx / max(1, steps - 1))
            try:
                label.config(fg=self._mix_hex(start, end, ratio))
            except Exception:
                return
            if idx < steps - 1:
                try:
                    label._color_anim_job = self.after(delay_ms, lambda: _tick(idx + 1))
                except Exception:
                    pass
            else:
                try:
                    label._color_anim_job = None
                except Exception:
                    pass

        _tick(0)

    def _animate_widget_bg(self, widget, start: str, end: str, steps: int = 8, delay_ms: int = 30):
        if not self._enable_summary_animations:
            try:
                widget.config(bg=end)
            except Exception:
                pass
            return
        try:
            prev_job = getattr(widget, "_bg_anim_job", None)
            if prev_job:
                self.after_cancel(prev_job)
        except Exception:
            pass

        def _tick(idx: int = 0):
            ratio = min(1.0, idx / max(1, steps - 1))
            try:
                widget.config(bg=self._mix_hex(start, end, ratio))
            except Exception:
                return
            if idx < steps - 1:
                try:
                    widget._bg_anim_job = self.after(delay_ms, lambda: _tick(idx + 1))
                except Exception:
                    pass
            else:
                try:
                    widget._bg_anim_job = None
                except Exception:
                    pass

        _tick(0)

    def _split_summary_primary(self, text: str) -> tuple[str, str, str]:
        clean = " ".join(str(text or "").split())
        m = re.match(r"^(AWB)\s+(\d{8,})(.*)$", clean, flags=re.IGNORECASE)
        if m:
            prefix = f"{m.group(1).upper()} "
            value = m.group(2)
            suffix = m.group(3).strip()
            suffix = f" {suffix}" if suffix else ""
            return prefix, value, suffix
        m = re.match(r"^(\d+)\s+(.+)$", clean)
        if m:
            return "", m.group(1), f" {m.group(2)}"
        return "", "", clean

    def _set_summary_primary(self, label, text: str, fg: str):
        changed = label.cget("text") != text
        target_fg = fg if fg in {OK, WARN, INFO} else TEXT_FG
        label.config(text=text)
        if changed and target_fg == OK:
            self._animate_label_color(label, "#d6eddc", "#51b96f")
        elif changed and target_fg in {WARN, CRIT, REVIEW}:
            self._animate_label_color(label, "#ead7d7", TEXT_FG)
        else:
            label.config(fg=target_fg if target_fg != WARN else TEXT_FG)

    def _set_summary_primary_parts(self, prefix_lbl, value_lbl, suffix_lbl, text: str, fg: str):
        prefix, value, suffix = self._split_summary_primary(text)
        old_sig = (
            prefix_lbl.cget("text"),
            value_lbl.cget("text"),
            suffix_lbl.cget("text"),
        )
        new_sig = (prefix, value, suffix)
        prefix_lbl.config(text=prefix, fg=TEXT_FG)
        suffix_lbl.config(text=suffix if (value or suffix) else "", fg=TEXT_FG)
        value_lbl.config(text=value if value else (suffix if not prefix else ""))
        if value:
            if fg == OK:
                value_lbl.config(fg="#51b96f")
            elif fg in {WARN, CRIT, REVIEW}:
                value_lbl.config(fg=TEXT_FG)
            elif fg == INFO:
                value_lbl.config(fg="#5e86bd")
            else:
                value_lbl.config(fg=TEXT_FG)
        else:
            value_lbl.config(fg=TEXT_FG)
            if suffix and not prefix:
                suffix_lbl.config(text="")
        if old_sig != new_sig:
            if fg == OK and value_lbl.cget("text"):
                self._animate_label_color(value_lbl, "#d6eddc", "#51b96f")
            elif fg in {WARN, CRIT, REVIEW}:
                self._animate_label_color(value_lbl, "#efe1e1", TEXT_FG)

    def _flash_summary_panel(self, state_lbl, widgets, tone: str):
        body = state_lbl.master
        body_end = "#f7f9fc"
        state_end = "#e8edf5"
        if tone == "positive":
            body_start = "#edf8f1"
            state_start = "#e1f1e6"
        elif tone == "negative":
            body_start = "#fff1f1"
            state_start = "#f4e1e1"
        else:
            body_start = "#eef4fb"
            state_start = "#e5edf8"
        self._animate_widget_bg(body, body_start, body_end)
        self._animate_widget_bg(state_lbl, state_start, state_end)
        for widget in widgets:
            self._animate_widget_bg(widget, body_start, body_end)

    def _next_match_badge(self, signature) -> str:
        if signature != self._last_match_signature:
            self._last_match_signature = signature
            self._match_event_counter += 1
        return f"{self._match_event_counter:02d}" if self._match_event_counter else "00"

    def _flash_tile_delta(self, label, text: str = "", fg: str = TEXT_MUTED, duration_ms: int = 1400):
        delta_lbl = getattr(label, "_delta_lbl", None)
        if delta_lbl is None:
            return
        try:
            prev_job = getattr(delta_lbl, "_clear_job", None)
            if prev_job:
                self.after_cancel(prev_job)
        except Exception:
            pass
        delta_lbl.config(text=text, fg=fg)
        if text:
            try:
                delta_lbl._clear_job = self.after(duration_ms, lambda: delta_lbl.config(text=" "))
            except Exception:
                pass

    def _summarize_resource(self, raw: str) -> str:
        text = str(raw or "").strip()
        if not text:
            return "file"
        name = Path(text).name if ("/" in text or "\\" in text) else text
        upper = name.upper()
        if upper.startswith("PRINT_STACK_BATCH"):
            return "print stack"
        if upper == "AWB_SEQUENCE.XLSX":
            return "sequence sheet"
        if upper == "PIPELINE_AUDIT.JSONL":
            return "audit log"
        suffix = Path(name).suffix.lower()
        if suffix == ".xlsx":
            return "Excel sheet"
        if suffix == ".pdf":
            if re.fullmatch(r"\d{12}(?:_\d+)?\.pdf", name, flags=re.IGNORECASE):
                m = re.match(r"(\d{12})(?:_\d+)?\.pdf", name, flags=re.IGNORECASE)
                if m:
                    return f"AWB {m.group(1)}"
                return "AWB"
            return "PDF"
        return "file"

    def _humanize_activity_text(self, text: str) -> str:
        compact = " ".join(str(text or "").split())
        if not compact:
            return " "

        # Strip front-end noise prefixes (timestamps, levels, bracket tags).
        compact = re.sub(r"^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}(?:[.,]\d+)?\s*", "", compact)
        compact = re.sub(r"^(INFO|WARNING|WARN|ERROR|DEBUG)\s+", "", compact, flags=re.IGNORECASE)
        compact = re.sub(r"^\[[A-Z0-9_\-]+\]\s*", "", compact)

        generic_replacements = [
            ("Base:", "Workspace:"),
            ("Protected:", "Protected files:"),
            ("EDM fallback: OFF (API calls bypassed)", ""),
            ("EDM fallback: ON (API calls allowed)", ""),
            ("Ready.", "System ready"),
            ("Launch hotfolder service", ""),
            ("Launch EDM checker service", ""),
            ("Launch batch service", ""),
            ("AWB hotfolder stopping", ""),
            ("Starting EDM duplicate checker...", ""),
            ("Stopping EDM duplicate checker...", ""),
        ]
        for src, dst in generic_replacements:
            compact = compact.replace(src, dst)

        compact = re.sub(r"\[[0-9:\- ]+\]\s*", "", compact)
        compact = re.sub(r"/Users/[^ ]+", lambda m: self._summarize_resource(m.group(0)), compact)
        compact = re.sub(r"[A-Za-z]:\\[^ ]+", lambda m: self._summarize_resource(m.group(0)), compact)
        compact = re.sub(r"PRINT_STACK_BATCH_\d+(?:_v\d+)?\.pdf", "print stack", compact, flags=re.IGNORECASE)
        compact = re.sub(r"awb_sequence\.xlsx", "sequence sheet", compact, flags=re.IGNORECASE)
        compact = re.sub(
            r"\b(\d{12})(?:_\d+)?\.pdf\b",
            lambda m: f"AWB {m.group(1)}",
            compact,
            flags=re.IGNORECASE,
        )

        front_end_rules = [
            (r"OK\s+\[CLEAN\]\s+Deleted:\s+.+", "Clean queue -1"),
            (r"\[PENDING_PRINT\]\s+Copied:\s+.+", "Print stack copied"),
            (r"INFO\s+PENDING_PRINT updated:\s+(\d+)\s+file\(s\)\s+copied\.", r"Print queue +\1"),
            (r"INFO\s+Batch PDF:\s+.+", "Print stack ready"),
            (r"INFO\s+Excel sequence:\s+.+", "Sequence sheet ready"),
            (r"INFO\s+Launch batch service", "Batch service started"),
            (r"INFO\s+DONE", "Batch complete"),
            (r"OK\s+Cleaned\s+(\d+)\s+file\(s\)\s+from CLEAN\.", r"Clean queue cleared · \1"),
            (r"^OK Preparing batch$", "Preparing batch"),
            (r"OK\s+===\s+\[BATCH\]\s+Prepare Batch\s+\((\d+)\s+file\(s\)\s+in CLEAN\)\s+===", r"Preparing batch from \1 clean files"),
            (r"INFO\s+Stopping EDM duplicate checker\.\.\.", ""),
            (r"INFO\s+EDM bypass active", ""),
            (r"INFO\s+EDM checks.*", ""),
            (r"INFO\s+Workspace:\s+.+", "Workspace ready"),
            (r"INFO\s+Protected files:\s+.+", "Protected files loaded"),
            (r"ERR\s+Scheduling:\s+two-pass.*", ""),
            (r"INFO\s+Long-pass timeout budget per file:\s*\d+s", ""),
            (r"INFO\s+Mode:\s+watchdog event-driven.*", ""),
            (r"INFO\s+Loaded AWBs:\s*\d+.*", "AWB list loaded"),
            (r"INFO\s+Watching INBOX.*", "Watching inbox"),
            (r"INFO\s+INBOX:\s*file", "New file detected"),
            (r"INFO\s+Processing:\s+.+", "Checking next document"),
            (r"INFO\s+\[FAST-LANE\].*", ""),
            (r"INFO\s+\[LONG-PASS\].*", ""),
            (r"INFO\s+\[THIRD-PASS\].*", ""),
            (r"INFO\s+\[TIMEOUT.*", ""),
            (r"INFO\s+\[GLOBAL-TIMEOUT\].*", ""),
            (r"INFO\s+\[ROTATION-PROBE\].*", ""),
            (r"INFO\s+\[STAGE.*", ""),
            (r"INFO\s+\[HEARTBEAT\].*", ""),
            (r"INFO\s+\[RELOAD\].*", ""),
            (r".*file gone before processing.*", ""),
            (r"INFO\s+EXCEL:\s*Excel sheet", "Database sheet ready"),
            (r"INFO\s+LOGS:\s*Excel sheet", "Log sheet ready"),
            (r"\[TIMING\].*", ""),
            # Raw hotfolder match line — extract just the 12-digit AWB number.
            (r"AWB MATCHED \([^)]+\):\s*(\d{12}).*", r"\1"),
            # EDM plain-text result line emitted by _record_outcome().
            (r"^EDM-DONE (\d{12}) (Clean|Mixed|Duplicate)$", r"AWB \1 — \2"),
            # EDM verbose lines — all suppressed.
            (r"^={3,}$", ""),
            (r"^File:\s+.+", ""),
            (r"^AWB:\s+.+", ""),
            (r"^Querying EDM metadata.*", ""),
            (r"^Found \d+ existing EDM.*", ""),
            (r"^Comparing incoming doc.*", ""),
            (r"^New/updated file detected.*", ""),
            (r"^No hash/probe hit.*", ""),
            (r"^EDM Duplicate Checker.*", ""),
            (r".*EDM toggle is OFF; bypassing EDM calls.*", ""),
            (r".*EDM toggle is ON; EDM calls enabled.*", ""),
            (r".*EDM fallback set to OFF.*", ""),
            (r".*EDM fallback set to ON.*", ""),
            (r".*unexpected error on .* no such file or directory.*", ""),
        ]
        for pattern, replacement in front_end_rules:
            compact = re.sub(pattern, replacement, compact, flags=re.IGNORECASE)

        compact = compact.replace("-> PROCESSED", " to processed")
        compact = compact.replace("-> CLEAN", " to clean")
        compact = compact.replace("-> OUT", " to out")
        compact = compact.replace("->", " to ")
        compact = compact.replace("  ", " ")
        compact = compact.replace(" | ", " · ")
        compact = re.sub(r"^(INFO|OK|WARN|ERR|STEP|EDM|SKIP|REVIEW|REJECT)\s+", "", compact, flags=re.IGNORECASE)
        return self._compact_text(compact.strip(), 82)

    def _frontend_visible_message(self, pretty_message: str) -> str | None:
        compact = " ".join(str(pretty_message or "").split())
        if not compact:
            return None
        # Allowlist: only the two key user-facing events reach the feed.
        # Everything else is captured in the audit log or surfaced via the
        # hard-error bypass in log_append().
        show_patterns = (
            r"^\d{12}$",                                     # AWB match — just the number
            r"^AWB \d{12} — (Clean|Mixed|Duplicate)$",      # EDM result
            r"^Preparing batch$",                            # Manual batch trigger
        )
        for pattern in show_patterns:
            if re.search(pattern, compact, flags=re.IGNORECASE):
                return compact
        return None

    def _is_hard_error_event(self, raw_message: str, tag_name: str) -> bool:
        """Return True only for genuinely hard failures that deserve red/orange emphasis."""
        m = str(raw_message or "").lower()
        tag = str(tag_name or "").lower()
        hard_patterns = (
            "traceback",
            "fatal",
            "panic",
            "permission denied",
            "disk full",
            "out of memory",
            "segmentation fault",
            "failed to start",
            "cannot start",
            "crash",
            "unhandled exception",
            "module not found",
            "jsondecodeerror",
            "keyerror",
            "typeerror",
            "valueerror",
            "runtimeerror",
            "connection refused",
            "timed out",
            "ssl",
            "token invalid",
            "edm audit unavailable",
        )
        if any(p in m for p in hard_patterns):
            return True
        # Treat generic WARN/ERROR tags as non-hard unless message indicates hard failure.
        if tag in {"warn", "error", "rejected"}:
            return False
        return False

    def _classify_activity_stage(self, message: str, payload, pretty_message: str) -> str:
        """Return stage key used to visually group live activity rows."""
        if isinstance(payload, dict):
            stage = str(payload.get("stage", "")).upper()
            if stage == "AWB_HOTFOLDER":
                return "AWB"
            if stage == "EDM_CHECK":
                return "EDM"
            if stage == "BATCH":
                return "BATCH"
        txt = " ".join([str(message or ""), str(pretty_message or "")]).upper()
        if "AWB" in txt:
            return "AWB"
        if "EDM" in txt:
            return "EDM"
        if "BATCH" in txt or "TIFF" in txt or "PRINT" in txt:
            return "BATCH"
        return "SYSTEM"

    def _ui_line_icon_image(self, icon_name: str, stroke: str = "#90a3bf", size: int = 22, cache_name: str = "snapshot"):
        cache = self._snapshot_icon_cache if cache_name == "snapshot" else self._toolbar_icon_cache
        key = (icon_name, stroke, size)
        cached = cache.get(key)
        if cached is not None:
            return cached

        is_toolbar = cache_name == "toolbar"
        oversample = 8 if is_toolbar else 10
        s = max(48, size * oversample)
        img = Image.new("RGBA", (s, s), (0, 0, 0, 0))
        draw = ImageDraw.Draw(img)
        unit = s / 24.0
        width = max(1 if is_toolbar else 2, int((0.085 if is_toolbar else 0.11) * s))

        def pt(x: float, y: float) -> tuple[int, int]:
            return (int(x * unit), int(y * unit))

        def line(points):
            flat = [pt(x, y) for x, y in points]
            draw.line(flat, fill=stroke, width=width, joint="curve")

        def rect(x1, y1, x2, y2, radius: float = 2.2):
            draw.rounded_rectangle(
                [pt(x1, y1), pt(x2, y2)],
                radius=max(1, int(radius * unit)),
                outline=stroke,
                width=width,
            )

        def arc(x1, y1, x2, y2, start, end):
            draw.arc([pt(x1, y1), pt(x2, y2)], start=start, end=end, fill=stroke, width=width)

        def circle(x1, y1, x2, y2):
            draw.ellipse([pt(x1, y1), pt(x2, y2)], outline=stroke, width=width)

        def doc_outline():
            line([(6, 4), (14, 4), (18, 8)])
            line([(18, 8), (18, 20), (6, 20), (6, 4)])
            line([(14, 4), (14, 8), (18, 8)])

        name = str(icon_name or "").strip().lower()
        if name == "inbox":
            line([(6, 16), (6, 20), (18, 20), (18, 16)])
            line([(6, 16), (10, 16), (12, 18), (14, 16), (18, 16)])
            line([(12, 4), (12, 13)])
            line([(9, 10), (12, 13), (15, 10)])
        elif name == "processed":
            doc_outline()
            line([(9, 12), (15, 12)])
            line([(9, 16), (15, 16)])
        elif name == "clean":
            doc_outline()
            line([(9, 14), (11.5, 16.5), (15.8, 10.8)])
        elif name == "rejected":
            doc_outline()
            line([(9, 10), (15.5, 16.5)])
            line([(15.5, 10), (9, 16.5)])
        elif name == "review":
            arc(6, 6, 15, 15, 0, 359)
            line([(14, 14), (18, 18)])
        elif name == "out":
            rect(6, 9, 13, 16)
            rect(10, 6, 17, 13)
        elif name == "pending":
            rect(6, 7, 18, 12)
            rect(8, 13, 16, 18)
            line([(8, 10), (16, 10)])
            line([(9, 20), (15, 20)])
        elif name == "start":
            line([(9, 6), (17, 12), (9, 18), (9, 6)])
        elif name == "stop":
            rect(8, 8, 16, 16, radius=1.6)
        elif name == "auto":
            # Cleaner clockwise ring + arrow head for AUTO MODE
            arc(5.5, 5.5, 18.5, 18.5, 40, 325)
            line([(15.3, 6.7), (18.4, 6.7), (18.4, 9.7)])
        elif name == "cycle":
            arc(6, 6, 18, 18, 36, 330)
            line([(15.5, 6.8), (18.2, 6.8), (18.2, 9.4)])
        elif name == "upload":
            line([(12, 5), (12, 15)])
            line([(9, 8), (12, 5), (15, 8)])
            line([(6, 17), (6, 20), (18, 20), (18, 17)])
        elif name == "batch":
            # Clear "stacked docs/tray" glyph for Prepare Batch
            rect(6, 9, 18, 13, radius=1.8)
            line([(8, 15), (16, 15)])
            line([(9, 17.5), (15, 17.5)])
        elif name == "tiff":
            doc_outline()
            line([(9, 11), (15, 11)])
            line([(9, 14), (15, 14)])
            line([(9, 17), (15, 17)])
        elif name == "retry":
            # Counter-clockwise return arrow for Retry Failed
            arc(5.5, 5.5, 18.5, 18.5, 220, 500)
            line([(7.3, 15.7), (5.2, 13.7), (7.8, 12.8)])
        elif name == "clear":
            circle(5.5, 5.5, 18.5, 18.5)
            line([(8.5, 8.5), (15.5, 15.5)])
            line([(15.5, 8.5), (8.5, 15.5)])
        elif name == "more":
            for cx in (7.5, 12, 16.5):
                draw.ellipse([pt(cx - 1.1, 11), pt(cx + 1.1, 13.2)], fill=stroke, outline=stroke)
        elif name == "edm":
            rect(5, 9, 19, 15, radius=3.0)
            circle(6.5, 10.3, 10.8, 14.6)
        else:
            rect(6, 6, 18, 18)

        final_img = img.resize((size, size), Image.Resampling.LANCZOS)
        icon = ImageTk.PhotoImage(final_img)
        # FIFO eviction: keep each icon cache bounded to 64 entries
        while len(cache) >= 64:
            cache.pop(next(iter(cache)))
        cache[key] = icon
        return icon

    def _snapshot_icon_image(self, tile_label: str, stroke: str = "#90a3bf", size: int = 22):
        return self._ui_line_icon_image(str(tile_label or "").lower(), stroke=stroke, size=size, cache_name="snapshot")

    def _toolbar_icon_image(self, icon_name: str, stroke: str = "#90a3bf", size: int = 15):
        return self._ui_line_icon_image(icon_name, stroke=stroke, size=size, cache_name="toolbar")

    def _apply_toolbar_button_icons(self):
        if not hasattr(self, "btn_get_awb"):
            return

        def assign(btn, icon_name: str, stroke: str, size: int = 15):
            try:
                icon = self._toolbar_icon_image(icon_name, stroke=stroke, size=size)
                btn.config(image=icon, compound="left")
                btn._toolbar_icon = icon
            except Exception:
                pass

        awb_running = self.is_awb_running()
        edm_dup_running = self.is_edm_duplicate_running()
        state_key = (awb_running, bool(self.auto_running), bool(self.edm_enabled), edm_dup_running)
        if self._toolbar_icon_state_key == state_key:
            return
        icon_size = 14
        assign(self.btn_get_awb, "stop" if awb_running else "start", "white", icon_size)
        assign(self.btn_auto, "stop" if self.auto_running else "auto", "white", icon_size)
        assign(self.btn_full_cycle, "cycle", "white", icon_size)
        assign(self.btn_upload, "upload", "white", icon_size)
        assign(self.btn_batch, "batch", "#71809a", icon_size)
        assign(self.btn_tiff, "tiff", "#71809a", icon_size)
        assign(self.btn_retry_review, "retry", "#71809a", icon_size)
        if hasattr(self, "btn_edm_runner"):
            assign(self.btn_edm_runner, "stop" if edm_dup_running else "start", INFO if edm_dup_running else "#6f7f97", icon_size)
        assign(self.btn_edm, "edm", INFO if self.edm_enabled else "#7b8597", icon_size)
        assign(self.btn_clear_all, "clear", "#7b8597", icon_size)
        assign(self.btn_global_actions, "more", "#5b687c", icon_size)
        self._toolbar_icon_state_key = state_key

    def _summary_palette(self, fg: str) -> tuple[str, str, str]:
        if fg == OK:
            return ("#f7f9fc", "#e8edf5", "#5e8a68")
        if fg == WARN:
            return ("#f7f9fc", "#e8edf5", "#b38a49")
        if fg == INFO:
            return ("#f7f9fc", "#e8edf5", "#6a84a8")
        if fg == CRIT or fg == REVIEW:
            return ("#f7f9fc", "#e8edf5", "#b17777")
        return ("#f7f9fc", "#e8edf5", TEXT_SEC)

    def _style_summary_card(self, state_lbl, primary_lbl, detail_labels, fg: str):
        body_bg, pill_bg, pill_fg = self._summary_palette(fg)
        try:
            body = state_lbl.master
            body.config(bg=body_bg)
        except Exception:
            body_bg = PANEL_BG
        try:
            state_lbl.config(fg=pill_fg, bg=pill_bg)
        except Exception:
            pass
        try:
            primary_lbl.config(bg=body_bg, fg=TEXT_FG)
        except Exception:
            pass
        for lbl in detail_labels:
            try:
                lbl.config(bg=body_bg, fg=TEXT_SEC)
            except Exception:
                pass

    def _update_match_summary(self, state: str = "WAITING", primary: str = "No active match",
                              line1: str = "Type: —", line2: str = "Confidence: —", line3: str = "Route: —",
                              timing_text: str | None = None, fg: str = TEXT_SEC):
        return  # panel removed

    def _update_edm_duplicate_summary(self, state: str = "IDLE", primary: str = "No EDM result",
                                      line1: str = "Full clean: 0", line2: str = "Partial clean: 0",
                                      line3: str = "Clean pages: 0  ·  Duplicate pages: 0",
                                      timing_text: str | None = None, fg: str = TEXT_SEC):
        return  # panel removed

    def _update_batch_prep_summary(self, state: str = "IDLE", primary: str = "No batch output",
                                   line1: str = "PDF stacks: 0", line2: str = "TIFF prepared: 0",
                                   line3: str = "Pending print: 0", fg: str = TEXT_SEC):
        return  # panel removed

    def _infer_match_confidence(self, method: str) -> str:
        m = str(method or "").upper()
        if m.startswith("FILENAME"):
            return "HIGH"
        if "TEXT-LAYER" in m or m.startswith("TEXTLAYER"):
            return "HIGH"
        if "TOLERANCE" in m:
            return "MEDIUM"
        if "OCR-TABLE" in m or "UPSCAL" in m or "EDM" in m:
            return "MEDIUM"
        return "STRONG"

    def _initialize_session_audit_tail(self):
        """Start this UI session from current audit EOF so 3 summary tabs are run-scoped."""
        try:
            audit_path = config.AUDIT_LOG
            if audit_path.exists():
                st = audit_path.stat()
                self._audit_inode = getattr(st, "st_ino", None)
                self._audit_offset = st.st_size
            else:
                self._audit_inode = None
                self._audit_offset = 0
            self._audit_recent.clear()
            self._last_edm_event_count = 0
        except Exception:
            pass

    def _initialize_session_stats_baseline(self):
        """Capture dashboard baseline so summary tabs can be run-scoped."""
        try:
            from V3.audit.tracker import read_dashboard_stats
            s = read_dashboard_stats() or {}
            for k in self._session_stats_baseline.keys():
                self._session_stats_baseline[k] = int(s.get(k, 0) or 0)
        except Exception:
            pass

    def _mark_summary_event(self, key: str):
        if key in self._summary_last_event_ts:
            self._summary_last_event_ts[key] = time.monotonic()

    def _apply_summary_idle_decay(self):
        """After brief inactivity, reduce 3 summary cards to placeholders."""
        now = time.monotonic()
        idle_after_s = 10.0
        if now - self._summary_last_event_ts.get("match", 0.0) > idle_after_s:
            self._update_match_summary(
                state="WAITING",
                primary="--",
                line1="--",
                line2="--",
                line3="--",
                timing_text="--",
                fg=TEXT_SEC,
            )
        if now - self._summary_last_event_ts.get("edm", 0.0) > idle_after_s:
            self._update_edm_duplicate_summary(
                state="IDLE",
                primary="--",
                line1="--",
                line2="--",
                line3="--",
                timing_text="--",
                fg=TEXT_SEC,
            )
        if now - self._summary_last_event_ts.get("batch", 0.0) > idle_after_s:
            self._update_batch_prep_summary(
                state="IDLE",
                primary="--",
                line1="--",
                line2="--",
                line3="--",
                fg=TEXT_SEC,
            )

    def _candidate_bucket(self, method: str) -> str:
        m = str(method or "").upper()
        # S — definitive: filename regex or embedded text layer
        if m.startswith("FILENAME") or m.startswith("TEXTLAYER") or m.startswith("TEXT-LAYER"):
            return "strong"
        # W — tolerance / fuzzy match (1-2 digit off)
        if "TOLERANCE" in m:
            return "weak"
        # S — clean exact match at Stage 2 (OCR-Main first pass, reliable)
        if m.startswith("OCR-MAIN") and "EXACT" in m:
            return "strong"
        # M — exact/400-pattern via Stage 3+, probes, rotation, or rescue
        if "EXACT" in m or "-400" in m or "400" in m:
            return "mix"
        return "weak"

    def _schedule_batch_candidate_reset(self):
        try:
            if self._batch_candidate_reset_job is not None:
                self.after_cancel(self._batch_candidate_reset_job)
        except Exception:
            pass
        self._batch_candidate_reset_job = self.after(5000, self._reset_batch_candidate_counts)

    def _reset_batch_candidate_counts(self):
        self._batch_candidate_reset_job = None
        moved = (
            int(self._batch_candidate_counts.get("strong", 0))
            + int(self._batch_candidate_counts.get("mix", 0))
            + int(self._batch_candidate_counts.get("weak", 0))
        )
        if moved > 0:
            self._perf_extra_complete += moved
            self._perf_extra_batches += moved
        self._batch_candidate_counts = {"strong": 0, "mix": 0, "weak": 0}
        self._refresh_batch_candidate_summary(use_placeholder=True)
        self._refresh_stats()

    def _refresh_batch_candidate_summary(self, use_placeholder: bool = False):
        return  # panel removed
        if not hasattr(self, "lbl_batchprep_state"):
            return
        state = self.lbl_batchprep_state.cget("text")
        primary = (
            f"{self.lbl_batchprep_primary_prefix.cget('text')}"
            f"{self.lbl_batchprep_primary_value.cget('text')}"
            f"{self.lbl_batchprep_primary_suffix.cget('text')}"
        ).strip()
        line1 = (
            f"Tier mix: Strong {self._batch_tier_totals['strong']}  ·  "
            f"Mix {self._batch_tier_totals['mix']}  ·  "
            f"Weak {self._batch_tier_totals['weak']}"
        )
        line2 = "Latest: --" if use_placeholder else (
            f"Latest: S {self._batch_candidate_counts['strong']}  ·  "
            f"M {self._batch_candidate_counts['mix']}  ·  "
            f"W {self._batch_candidate_counts['weak']}"
        )
        self._update_batch_prep_summary(
            state=state or "IDLE",
            primary=primary or "No batch output",
            line1=line1,
            line2=line2,
            line3="--",
            fg=OK if str(state).upper() == "READY" else TEXT_SEC,
        )

    def _update_run_overview(self):
        if not hasattr(self, "lbl_run_mode"):
            return
        awb_on = self.is_awb_running()
        edm_dup_on = self.is_edm_duplicate_running()
        jobs = []

        if self.auto_running:
            mode_text = f"Mode: AUTO ({self.auto_phase})"
        elif self.full_cycle_running:
            mode_text = "Mode: FULL CYCLE ONCE"
        elif awb_on:
            mode_text = "Mode: AWB LIVE"
        else:
            mode_text = "Mode: MANUAL / IDLE"

        if awb_on:
            jobs.append("AWB hotfolder")
        if edm_dup_on:
            jobs.append("EDM duplicate checker")
        if self.batch_running:
            jobs.append("Batch builder")
        if self.full_cycle_running:
            jobs.append("Full cycle runner")
        if not jobs:
            jobs_text = "Active jobs: none"
        else:
            jobs_text = "Active jobs: " + ", ".join(jobs)

        status_text = f"Status: {self._compact_text(self.status_var.get(), 100)}"
        self.lbl_run_mode.config(text=mode_text)
        self.lbl_active_jobs.config(text=jobs_text)
        self.lbl_run_status.config(text=status_text)
        self._update_step_icons(awb_on=awb_on, edm_dup_on=edm_dup_on)
        if hasattr(self, "lbl_breadcrumb"):
            if self.auto_running:
                crumb = f"Home  /  AUTO MODE  /  {self.auto_phase}"
            elif self.full_cycle_running:
                crumb = "Home  /  Full Cycle  /  Running"
            elif awb_on:
                crumb = "Home  /  Hotfolder  /  Live"
            else:
                crumb = "Home  /  Operations  /  Idle"
            self.lbl_breadcrumb.config(text=crumb)

    def _update_step_icons(self, awb_on: bool, edm_dup_on: bool):
        if not hasattr(self, "_step_icons"):
            return
        try:
            inbox_n = _count_pdfs(config.INBOX_DIR)
            out_n = len(list(config.OUT_DIR.glob(f"{config.PRINT_STACK_BASENAME}_*.pdf")))
        except Exception:
            inbox_n = 0
            out_n = 0
        states = {
            "INBOX": inbox_n > 0,
            "AWB": awb_on,
            "EDM": self.edm_enabled and edm_dup_on,
            "BATCH": self.batch_running,
            "OUT": out_n > 0,
        }
        for name, (lbl, active_color) in self._step_icons.items():
            is_on = bool(states.get(name, False))
            if is_on:
                lbl.config(text=f"● {name}", fg=active_color, bg="#e8effa")
            else:
                lbl.config(text=f"○ {name}", fg=TEXT_MUTED, bg="#eef3fb")

    def _update_stage_status_panel(self):
        try:
            inbox_n = _count_pdfs(config.INBOX_DIR)
            processed_n = _count_pdfs(config.PROCESSED_DIR)
            clean_n = _count_pdfs(config.CLEAN_DIR)
            review_n = _count_pdfs(config.NEEDS_REVIEW_DIR)
            rejected_n = _count_pdfs(config.REJECTED_DIR)
            pending_n = _count_pdfs(config.PENDING_PRINT_DIR)
            out_n = len(list(config.OUT_DIR.glob(f"{config.PRINT_STACK_BASENAME}_*.pdf")))
        except Exception:
            inbox_n = processed_n = clean_n = review_n = rejected_n = pending_n = out_n = 0

        awb_on = self.is_awb_running()
        latest_match = None
        for event in reversed(list(getattr(self, "_audit_recent", []))):
            if str(event.get("stage", "")).upper() == "AWB_HOTFOLDER" and str(event.get("status", "")).upper() == "MATCHED":
                latest_match = event
                break

        if latest_match:
            method = str(latest_match.get("match_method", "Matched"))
            route = str(latest_match.get("route", "PROCESSED"))
            awb = str(latest_match.get("awb", "—"))
            signature = (
                latest_match.get("ts"),
                latest_match.get("file"),
                awb,
                method,
                route,
            )
            hit_no = self._next_match_badge(signature)
            self._update_match_summary(
                state=f"AWB MATCHED · {hit_no}",
                primary=f"AWB {awb}",
                line1=f"Type: {self._short_reason(method, 30)}",
                line2=f"Confidence: {self._infer_match_confidence(method)}",
                line3=f"Route: {route}",
                fg=OK,
            )
            return

        if review_n or rejected_n:
            self._update_match_summary(
                state="ATTENTION",
                primary="Review queue",
                line1=f"Review queue: {review_n}",
                line2=f"Rejected docs: {rejected_n}",
                line3=f"Processed waiting: {processed_n}",
                fg=WARN,
            )
        elif out_n or pending_n:
            self._update_match_summary(
                state="READY",
                primary=f"{max(out_n, pending_n)} output ready",
                line1=f"Out batches: {out_n}",
                line2=f"Pending print: {pending_n}",
                line3=f"Clean docs remaining: {clean_n}",
                fg=OK,
            )
        elif awb_on:
            self._update_match_summary(
                state="LIVE",
                primary="Matching now",
                line1=f"Inbox queued: {inbox_n}",
                line2=f"Processed: {processed_n}",
                line3=f"EDM mode: {'ON' if self.edm_enabled else 'OFF'}",
                fg=INFO,
            )
        else:
            self._update_match_summary(
                state="WAITING",
                primary="No active match",
                line1=f"Inbox queued: {inbox_n}",
                line2=f"Clean docs: {clean_n}",
                line3=f"Review docs: {review_n}",
                fg=TEXT_SEC,
            )

    def _short_reason(self, reason: str, max_len: int = 60) -> str:
        out = " ".join(str(reason or "").split())
        replacements = [
            ("Matched by strict filename pattern", "Filename hit"),
            ("Matched exact DB candidate from text layer", "Text-layer hit"),
            ("Matched exact clean in OCR-main", "OCR hit"),
            ("EDM toggle OFF (API bypass)", "EDM OFF"),
            ("No EDM token available", "No EDM token"),
            ("EDM metadata query inconclusive/unauthorized", "Metadata inconclusive"),
            ("Partial duplicates stripped.", "Partial dup removed."),
        ]
        for src, dst in replacements:
            out = out.replace(src, dst)
        return self._compact_text(out, max_len)

    def _format_timeline_message(self, message: str, tag_name: str) -> str:
        text = " ".join(str(message).split())
        if not text:
            return " "
        if text.startswith("{") and text.endswith("}"):
            try:
                payload = json.loads(text)
            except Exception:
                payload = None
            if isinstance(payload, dict):
                stage = str(payload.get("stage", "")).upper()
                status = str(payload.get("status", "")).upper()
                awb = str(payload.get("awb", "")).strip()
                if stage == "AWB_HOTFOLDER":
                    if status == "MATCHED":
                        # Just the AWB number — green dot comes from the success tag.
                        return self._compact_text(awb or "—", 82)
                    # All other AWB events suppressed from feed.
                    return self._compact_text(f"AWB {awb or '—'} updated", 82)
                if stage == "EDM_CHECK":
                    if status == "CLEAN":
                        return self._compact_text(f"AWB {awb or '—'} — Clean", 82)
                    if status == "PARTIAL-CLEAN":
                        return self._compact_text(f"AWB {awb or '—'} — Mixed", 82)
                    if status == "REJECTED":
                        return self._compact_text(f"AWB {awb or '—'} — Duplicate", 82)
                    # CLEAN-UNCHECKED and all other EDM events suppressed from feed.
                    return self._compact_text(f"EDM {awb or '—'} unchecked", 82)
                if stage == "BATCH":
                    action = self._short_reason(payload.get("action", payload.get("status", "event")), 40)
                    count = payload.get("output_count", payload.get("awb_count", ""))
                    if "build_print_stacks" in str(payload.get("action", "")).lower():
                        return self._compact_text(f"Batch prepared · {count or 0} output", 82)
                    suffix = f" · {count}" if str(count) else ""
                    return self._compact_text(f"Batch {action}{suffix}", 82)
                if stage:
                    reason = self._short_reason(payload.get("reason", payload.get("action", "")), 48)
                    return self._compact_text(f"{stage} updated · {reason}", 82)

        replacements = [
            ("=== AWB Hotfolder started ===", "AWB hotfolder started"),
            ("Starting EDM duplicate checker...", "EDM duplicate checker started"),
            ("Stopping AWB Hotfolder...", "AWB hotfolder stopping"),
            ("Running: -m V3.services.hotfolder", "Launch hotfolder service"),
            ("Running: -m V3.services.edm_duplicate_checker", "Launch EDM checker service"),
            ("Running: -m V3.services.batch_builder", "Launch batch service"),
        ]
        compact = text
        for src, dst in replacements:
            compact = compact.replace(src, dst)
        compact = self._short_reason(compact, 120)
        return self._humanize_activity_text(compact)

    def _is_key_match_event(self, raw_message: str, pretty_message: str) -> bool:
        raw_up = str(raw_message).upper()
        pretty_up = str(pretty_message).upper()
        keys = (
            "MATCHED",
            "DUPLICATE",
            "CLEAN-UNCHECKED",
            "OCR",
            "TEXT-LAYER",
            "FILENAME",
            "EDM_CHECK",
            "AWB_HOTFOLDER",
            "PARTIAL",
        )
        return any(k in raw_up for k in keys) or any(k in pretty_up for k in ("AWB ", "EDM ", "BATCH ", "REJECT"))

    def _push_match_card(self, pretty_message: str):
        if not hasattr(self, "_match_cards"):
            return
        friendly = self._humanize_activity_text(pretty_message.strip())
        trimmed = self._compact_text(friendly, 84)
        self._match_cards.appendleft(trimmed)
        for idx, lbl in enumerate(self._match_card_labels):
            if idx < len(self._match_cards):
                lbl.config(text=f"• {self._match_cards[idx]}", fg="#31486a")
            else:
                lbl.config(text="• waiting for match events...", fg=TEXT_MUTED)

    # ─────────────────────────────────────────────────────────────────────────
    # LIVE ACTIVITY PANEL — zero widget-creation helpers
    # ─────────────────────────────────────────────────────────────────────────
    def _pulse_stage(self, stage_key: str):
        """Light up a stage pill (blink ticker will handle animation)."""
        if not hasattr(self, "_stage_pills"):
            return
        pill_info = self._stage_pills.get(stage_key)
        if pill_info is None:
            return
        _pf, dot, lbl, active_col, _dim_col = pill_info
        try:
            dot.itemconfig("dot", fill=active_col, outline=active_col)
            lbl.config(fg=TEXT_FG)
        except Exception:
            pass

    def _blink_tick(self):
        """Blink only the dot (not the text) for active stage pills.
        Inactive dots are never touched — they stay at _IDLE_DOT set by _refresh_live_status."""
        if getattr(self, "_is_closing", False):
            return
        if hasattr(self, "_stage_pills"):
            awb_on    = self.awb_proc is not None
            try:
                edm_on = self.is_edm_duplicate_running()
            except Exception:
                edm_on = False
            try:
                batch_on = self.is_batch_builder_running()
            except Exception:
                batch_on = False
            running_map = {"AWB": awb_on, "EDM": edm_on, "BATCH": batch_on}
            for _sk, _info in self._stage_pills.items():
                _, dot, _lbl, active_col, dim_col = _info
                if running_map.get(_sk):
                    phase = self._blink_dot_phase.get(_sk, True)
                    self._blink_dot_phase[_sk] = not phase
                    # Alternate between full active color and a darker shade of the same hue.
                    # Never touch _IDLE_DOT so inactive dots stay visually still.
                    col = active_col if phase else dim_col
                    try:
                        dot.itemconfig("dot", fill=col, outline=col)
                    except Exception:
                        pass
        self.after(600, self._blink_tick)

    def _push_activity_line(self, text: str, dot_color: str, text_color: str, timing_str: str):
        """Slide all 3 activity rows down and insert a new entry at the top. Config-only."""
        rows = getattr(self, "_recent_awb_lbls", [])
        if not rows:
            return
        for i in range(len(rows) - 1, 0, -1):
            _d_prev, _l_prev, _t_prev = rows[i - 1]
            _d_cur, _l_cur, _t_cur = rows[i]
            try:
                _prev_fill = _d_prev.itemcget("dot", "fill")
                _d_cur.itemconfig("dot", fill=_prev_fill, outline=_prev_fill)
                _l_cur.config(text=_l_prev.cget("text"), fg=_l_prev.cget("fg"))
                _t_cur.config(text=_t_prev.cget("text"))
            except Exception:
                pass
        _d0, _l0, _t0 = rows[0]
        try:
            _d0.itemconfig("dot", fill=dot_color, outline=dot_color)
            _l0.config(text=text, fg=text_color)
            _t0.config(text=timing_str)
        except Exception:
            pass

    def _push_recent_awb(self, awb: str, _method: str, _conf: str, timing_str: str):
        """Push a matched AWB to the top activity row."""
        self._push_activity_line(f"AWB {awb}", OK, OK, timing_str)

    def _update_status_badges(self):
        if not hasattr(self, "lbl_status_strip"):
            return
        try:
            inbox_n = _count_pdfs(config.INBOX_DIR)
            review_n = _count_pdfs(config.NEEDS_REVIEW_DIR)
            rejected_n = _count_pdfs(config.REJECTED_DIR)
        except Exception:
            return

        status_word = "System active" if (review_n or rejected_n or inbox_n) else "System stable"
        pending_text = f"Inbox {inbox_n}" if inbox_n > 0 else "Queue idle"
        error_text = f"Review {review_n}" if review_n > 0 else "No review"
        self.lbl_status_strip.config(text=f"{status_word}  ·  {pending_text}  ·  {error_text}")

        # Bottom bar health summary
        try:
            if review_n > 0 and rejected_n > 0:
                self.lbl_health_summary.config(
                    text=f"{review_n} review · {rejected_n} rejected", fg=CRIT)
            elif review_n > 0:
                self.lbl_health_summary.config(
                    text=f"{review_n} need review", fg=WARN)
            elif rejected_n > 0:
                self.lbl_health_summary.config(
                    text=f"{rejected_n} rejected", fg=WARN)
            else:
                self.lbl_health_summary.config(text="● Healthy", fg=OK)
        except Exception:
            pass

    def _show_toast(self, text: str, level: str = "info", duration_ms: int = 2200):
        if not hasattr(self, "_toast_host"):
            self._toast_host = tk.Frame(self, bg=APP_BG)
            self._toast_host.place(relx=1.0, rely=0.0, x=-14, y=14, anchor="ne")
        palette = {
            "success": ("#e6f8ed", "#1f7a3a", "OK"),
            "warn": ("#fff4df", "#9a5e00", "WARN"),
            "error": ("#ffe9e9", "#a52c2c", "ERR"),
            "info": ("#e8f1ff", "#2d5f9a", "INFO"),
        }
        bg, fg, icon = palette.get(level, palette["info"])
        box = tk.Label(
            self._toast_host,
            text=f"{icon}  {text}",
            font=FONT_SMALL,
            bg=bg,
            fg=fg,
            padx=10,
            pady=6,
            bd=1,
            relief="flat",
            highlightthickness=1,
            highlightbackground="#cfd8e8",
        )
        box.pack(anchor="e", pady=3)

        def _cleanup_toast(target=box):
            try:
                target.destroy()
            except Exception:
                pass
            try:
                if hasattr(self, "_toast_host") and not self._toast_host.winfo_children():
                    self._toast_host.destroy()
                    delattr(self, "_toast_host")
            except Exception:
                pass

        self.after(duration_ms, _cleanup_toast)

    def _focus_log_search(self):
        try:
            self.entry_log_search.focus_set()
            self.entry_log_search.selection_range(0, tk.END)
        except Exception:
            pass

    def _copy_to_clipboard(self, text: str):
        try:
            self.clipboard_clear()
            self.clipboard_append(text)
            self._show_toast("Copied to clipboard", "success", 1200)
        except Exception:
            pass

    def _show_log_row_menu(self, event, message: str, ts: str):
        menu = tk.Menu(self, tearoff=0)
        menu.add_command(label="Copy line",
                         command=lambda: self._copy_to_clipboard(message))
        menu.add_command(label="Copy with timestamp",
                         command=lambda: self._copy_to_clipboard(f"[{ts}] {message}"))
        menu.add_separator()
        menu.add_command(label='Search for this',
                         command=lambda: self._search_log_for(message[:40]))
        try:
            menu.tk_popup(event.x_root, event.y_root)
        finally:
            try:
                menu.grab_release()
            except Exception:
                pass

    def _search_log_for(self, text: str):
        try:
            self._search_var.set(text)
            self._apply_search_highlight()
            self.entry_log_search.focus_set()
        except Exception:
            pass

    def _build_export_line(self, badge_text: str, pretty_message: str) -> str:
        badge_clean = " ".join(str(badge_text).split())
        msg_clean = " ".join(str(pretty_message).split())
        return f"{badge_clean} | {msg_clean}"

    def _toggle_wrap_log(self):
        """No-op — log is now a fixed-widget panel."""

    def _bind_log_wheel_events(self):
        """No-op — no scrollable canvas in the new live activity panel."""

    def _on_log_mousewheel(self, event):
        """No-op."""

    def _on_log_canvas_configure(self, event):
        """No-op."""

    # ─────────────────────────────────────────────────────────────────────────
    # CLOCK
    # ─────────────────────────────────────────────────────────────────────────
    def _tick_clock(self):
        self.lbl_clock.config(text=time.strftime("%H:%M:%S"))
        self._tick_uptime()
        self.after(1000, self._tick_clock)

    def _tick_uptime(self):
        elapsed = int(time.time() - self._session_start)
        h, rem = divmod(elapsed, 3600)
        m, s   = divmod(rem, 60)
        if h:
            uptime_txt = f"Up: {h}h {m:02d}m {s:02d}s"
        else:
            uptime_txt = f"Up: {m}m {s:02d}s"
        # AWB run-time annotation
        if self._awb_start_time is not None:
            awb_elapsed = int(time.time() - self._awb_start_time)
            aw_h, aw_rem = divmod(awb_elapsed, 3600)
            aw_m, aw_s   = divmod(aw_rem, 60)
            if aw_h:
                awb_txt = f"  ·  AWB {aw_h}h{aw_m:02d}m"
            else:
                awb_txt = f"  ·  AWB {aw_m}m{aw_s:02d}s"
            uptime_txt += awb_txt
        try:
            self.lbl_uptime.config(text=uptime_txt)
        except Exception:
            pass

    # ─────────────────────────────────────────────────────────────────────────
    # EMPLOYEE LOGIN
    # ─────────────────────────────────────────────────────────────────────────
    def _prompt_employee_number(self):
        session = _load_session()
        prev = (session.get("employee_id", "") or "").strip()
        fallback = prev or "UNKNOWN"

        # Compute position before creating the Toplevel so update_idletasks()
        # doesn't trigger a geometry recalculation while the dialog is mid-init.
        width = 430
        height = 210
        self.update_idletasks()
        x = self.winfo_rootx() + max(0, (self.winfo_width() - width) // 2)
        y = self.winfo_rooty() + max(0, (self.winfo_height() - height) // 2)

        dialog = tk.Toplevel(self)
        dialog.title("Employee Login — ScavGajjar V3")
        dialog.configure(bg=APP_BG)
        dialog.resizable(False, False)
        dialog.transient(self)
        dialog.grab_set()
        dialog.geometry(f"{width}x{height}+{x}+{y}")

        hdr = tk.Frame(dialog, bg=FEDEX_PURPLE, height=40)
        hdr.pack(fill="x")
        hdr.pack_propagate(False)
        tk.Label(
            hdr,
            text="Employee Login — ScavGajjar V3",
            font=(FONT_LABEL[0], FONT_LABEL[1], "bold"),
            fg="white",
            bg=FEDEX_PURPLE,
        ).pack(side="left", padx=12, pady=10)

        body = tk.Frame(dialog, bg=APP_BG)
        body.pack(fill="both", expand=True, padx=14, pady=10)
        tk.Label(body, text="Employee ID", font=FONT_LABEL, bg=APP_BG, fg=self._default_fg).pack(anchor="w")
        emp_var = tk.StringVar(value=prev)
        entry = tk.Entry(body, textvariable=emp_var, font=FONT_LABEL, width=32)
        entry.pack(anchor="w", pady=(4, 4))

        err_var = tk.StringVar(value="")
        tk.Label(body, textvariable=err_var, font=FONT_SMALL, fg=CRIT, bg=APP_BG).pack(anchor="w")

        btn_row = tk.Frame(body, bg=APP_BG)
        btn_row.pack(anchor="e", fill="x", pady=(10, 0))

        result = {"value": None}

        def _apply_employee_id(value: str):
            self.employee_id = value
            os.environ["PIPELINE_EMPLOYEE_ID"] = value
            self.lbl_employee.config(text=f"Employee: {value}")
            if hasattr(self, "lbl_session"):
                self.lbl_session.config(text=f"Session: {value}")
            _save_session({**session, "employee_id": value})

        def _submit():
            val = (emp_var.get() or "").strip()
            if not val:
                err_var.set("Employee ID is required.")
                return
            result["value"] = val
            dialog.destroy()

        def _cancel():
            result["value"] = fallback
            dialog.destroy()

        tk.Button(
            btn_row,
            text="Cancel",
            width=10,
            font=FONT_SMALL,
            command=_cancel,
            bg=BTN_BG,
            fg=TEXT_FG,
            activebackground=BTN_HOVER,
            highlightbackground=BTN_BG,
            cursor="right_ptr",
        ).pack(side="right", padx=(8, 0))
        tk.Button(
            btn_row,
            text="Login",
            width=10,
            font=FONT_SMALL,
            command=_submit,
            bg=FEDEX_PURPLE,
            fg="white",
            activebackground=FEDEX_PURPLE,
            activeforeground="white",
            highlightbackground=FEDEX_PURPLE,
            cursor="right_ptr",
        ).pack(side="right")

        entry.focus_set()
        entry.selection_range(0, tk.END)
        dialog.bind("<Return>", lambda _e: _submit())
        dialog.protocol("WM_DELETE_WINDOW", _cancel)
        self.wait_window(dialog)

        chosen = (result.get("value") or "").strip() or fallback
        if not chosen:
            chosen = "UNKNOWN"
        _apply_employee_id(chosen)

    # ─────────────────────────────────────────────────────────────────────────
    # FOLDER COUNT REFRESH
    # ─────────────────────────────────────────────────────────────────────────
    def _start_count_refresh(self):
        self._refresh_tick_counter += 1
        is_active = (
            self.is_awb_running()
            or self.is_edm_duplicate_running()
            or self.batch_running
            or self.auto_running
            or self.full_cycle_running
        )
        # Folder counts run every tick on a background thread — no blocking.
        self._trigger_bg_count_refresh()
        # Heavier I/O (stats xlsx + audit jsonl) every tick when active, every
        # other tick when idle (they use their own inflight guards anyway).
        if is_active or (self._refresh_tick_counter % 3 == 0):
            self._refresh_stats()
            self._refresh_audit_health()
        self._update_stage_status_panel()
        self._apply_summary_idle_decay()
        self._refresh_live_status()
        next_ms = 1000 if is_active else 4000
        self.after(next_ms, self._start_count_refresh)

    def _safety_count_resync(self, is_active: bool):
        """No-op — folder counts now run every tick via _trigger_bg_count_refresh."""

    def _request_count_refresh(self, delay_ms: int = 100):
        if self._count_refresh_job is not None:
            try:
                self.after_cancel(self._count_refresh_job)
            except Exception:
                pass
        self._count_refresh_job = self.after(delay_ms, self._run_count_refresh_now)

    def _run_count_refresh_now(self):
        self._count_refresh_job = None
        self._refresh_counts()
        self._last_count_scan_ts = time.monotonic()

    def _refresh_audit_health(self):
        if not hasattr(self, "lbl_quick_check"):
            return
        audit_path = config.AUDIT_LOG
        if not audit_path.exists():
            self.lbl_quick_check.config(text="Quick check: waiting for EDM activity", fg="#7b8597")
            self.lbl_run_hint.config(text=self._default_run_hint, fg="#7b8597")
            self._update_edm_duplicate_summary(timing_text="")
            return
        try:
            stat = audit_path.stat()
            inode = getattr(stat, "st_ino", None)
            if self._audit_inode is None:
                self._audit_inode = inode
            if inode != self._audit_inode or stat.st_size < self._audit_offset:
                self._audit_inode = inode
                self._audit_offset = 0
                self._audit_recent.clear()
            _new_matched: list = []
            with audit_path.open("r", encoding="utf-8", errors="replace") as fh:
                fh.seek(self._audit_offset)
                for raw in fh:
                    line = raw.strip()
                    if not line:
                        continue
                    try:
                        row = json.loads(line)
                    except Exception:
                        continue
                    if isinstance(row, dict):
                        self._audit_recent.append(row)
                        if (str(row.get("stage", "")).upper() == "AWB_HOTFOLDER"
                                and str(row.get("status", "")).upper() == "MATCHED"):
                            _new_matched.append(row)
                self._audit_offset = fh.tell()
            # Push newly-seen matches to the fixed recent-AWBs panel.
            for _ev in _new_matched:
                _awb    = str(_ev.get("awb", "—"))
                _method = str(_ev.get("match_method", "Matched"))
                _conf   = self._infer_match_confidence(_method)
                _ms     = self._extract_timing_ms(_ev.get("timings_ms"))
                _timing = self._format_seconds_only(_ms) if _ms is not None else ""
                self._pulse_stage("AWB")
                self._push_recent_awb(_awb, _method, _conf, _timing)
        except Exception:
            self.lbl_quick_check.config(text="Quick check: EDM audit unavailable", fg=WARN)
            self._update_edm_duplicate_summary(state="ERROR", primary="Audit log unavailable", timing_text="", fg=WARN)
            return

        edm_events = [e for e in self._audit_recent if str(e.get("stage", "")).upper() == "EDM_CHECK"]
        if not edm_events:
            self.lbl_quick_check.config(text="Quick check: waiting for EDM events", fg="#7b8597")
            self.lbl_run_hint.config(text=self._default_run_hint, fg="#7b8597")
            self._update_edm_duplicate_summary(timing_text="")
            return

        window = edm_events[-40:]
        if len(edm_events) > self._last_edm_event_count:
            self._mark_summary_event("edm")
            self._last_edm_event_count = len(edm_events)
        total = len(window)
        unchecked = 0
        bypass_off = 0
        no_token = 0
        other_unchecked = 0
        full_clean = 0
        partial_clean = 0
        latest_dup_pages = 0
        latest_clean_pages = 0
        latest_check_ms = None
        for e in window:
            status = str(e.get("status", "")).upper()
            reason = str(e.get("reason", "")).lower()
            match_stats = str(e.get("match_stats", ""))
            event_ms = self._extract_timing_ms(e.get("timings_ms"))
            if event_ms is not None:
                latest_check_ms = event_ms
            dup_match = re.search(r"dup_count_effective=(\d+)", match_stats)
            total_match = re.search(r"total_pages=(\d+)", match_stats)
            if dup_match and total_match:
                latest_dup_pages = int(dup_match.group(1))
                total_pages = int(total_match.group(1))
                latest_clean_pages = max(0, total_pages - latest_dup_pages)
            if status == "CLEAN":
                full_clean += 1
            elif status == "PARTIAL-CLEAN":
                partial_clean += 1
            if status == "CLEAN-UNCHECKED":
                unchecked += 1
                if "toggle off" in reason:
                    bypass_off += 1
                elif "no edm token" in reason:
                    no_token += 1
                else:
                    other_unchecked += 1

        checked = total - unchecked
        if unchecked == 0:
            edm_state = "FULL CLEAN" if partial_clean == 0 and latest_dup_pages == 0 else "PARTIAL CLEAN"
            edm_primary = f"{checked} checked"
            edm_fg = OK
        else:
            edm_state = "BYPASS" if bypass_off else "WATCH"
            edm_primary = f"{unchecked} unchecked"
            edm_fg = WARN
        self._update_edm_duplicate_summary(
            state=edm_state,
            primary=edm_primary,
            line1=f"Full clean: {full_clean}",
            line2=f"Partial clean: {partial_clean}",
            line3=f"Clean pages: {latest_clean_pages}  ·  Duplicate pages: {latest_dup_pages}",
            timing_text=(self._format_seconds_only(latest_check_ms) if latest_check_ms is not None else ""),
            fg=edm_fg,
        )
        if unchecked == 0:
            self.lbl_quick_check.config(text=f"Quick check: EDM verified · {checked}/{total} checked", fg="#7b8597")
            self.lbl_run_hint.config(text=self._default_run_hint, fg="#7b8597")
            return

        detail_parts = []
        if bypass_off:
            detail_parts.append(f"toggle OFF {bypass_off}")
        if no_token:
            detail_parts.append(f"no token {no_token}")
        if other_unchecked:
            detail_parts.append(f"other {other_unchecked}")
        detail_text = ", ".join(detail_parts) if detail_parts else "reason unavailable"
        self.lbl_quick_check.config(text=f"Quick check: {detail_text} · {unchecked} unchecked", fg="#7b8597")

        if bypass_off > 0:
            self.lbl_run_hint.config(
                text="Tip: EDM is OFF, so duplicates are bypassed (CLEAN-UNCHECKED). Toggle EDM ON to enforce EDM checks.",
                fg="#7b8597",
            )
        elif no_token > 0:
            self.lbl_run_hint.config(
                text="Tip: add/refresh EDM token to enable real EDM duplicate validation.",
                fg="#7b8597",
            )
        else:
            self.lbl_run_hint.config(text=self._default_run_hint, fg="#7b8597")

    def _threshold_color(self, key: str, n) -> str:
        if n is None:
            return self._default_fg
        warn_at, crit_at = THRESHOLDS.get(key, (9999, 9999))
        if n >= crit_at:
            return CRIT
        if n >= warn_at:
            return WARN
        return self._default_fg

    def _trigger_bg_count_refresh(self):
        """Scan folder counts on a background thread; apply to UI on the main thread.

        Uses an inflight flag so overlapping scans are skipped — the previous
        scan's result will arrive shortly via after(0, ...) anyway.
        """
        if getattr(self, "_bg_count_inflight", False):
            return
        self._bg_count_inflight = True

        def _scan():
            try:
                counts = {
                    "inbox":     _count_pdfs(config.INBOX_DIR),
                    "processed": _count_pdfs(config.PROCESSED_DIR),
                    "clean":     _count_pdfs(config.CLEAN_DIR),
                    "rejected":  _count_pdfs(config.REJECTED_DIR),
                    "review":    _count_pdfs(config.NEEDS_REVIEW_DIR),
                    "pending":   _count_pdfs(config.PENDING_PRINT_DIR),
                    "out":       len(list(config.OUT_DIR.glob(
                                     f"{config.PRINT_STACK_BASENAME}_*.pdf"))),
                }
            except Exception:
                counts = None
            finally:
                self._bg_count_inflight = False
            if counts is not None:
                try:
                    self.after(0, lambda c=counts: self._refresh_counts(c))
                except Exception:
                    pass

        threading.Thread(target=_scan, daemon=True).start()

    def _refresh_counts(self, precomputed: dict | None = None):
        if precomputed is not None:
            inbox_n     = precomputed["inbox"]
            processed_n = precomputed["processed"]
            clean_n     = precomputed["clean"]
            rejected_n  = precomputed["rejected"]
            review_n    = precomputed["review"]
            pending_n   = precomputed["pending"]
            out_n       = precomputed["out"]
        else:
            # Synchronous fallback for immediate on-demand refreshes.
            def _count_batches():
                try:
                    return len(list(config.OUT_DIR.glob(f"{config.PRINT_STACK_BASENAME}_*.pdf")))
                except Exception:
                    return None

            inbox_n     = _count_pdfs(config.INBOX_DIR)
            processed_n = _count_pdfs(config.PROCESSED_DIR)
            clean_n     = _count_pdfs(config.CLEAN_DIR)
            rejected_n  = _count_pdfs(config.REJECTED_DIR)
            review_n    = _count_pdfs(config.NEEDS_REVIEW_DIR)
            pending_n   = _count_pdfs(config.PENDING_PRINT_DIR)
            out_n       = _count_batches()

        def _fmt(n):
            return str(n) if n is not None else "?"

        # Accent bar colours: neutral slate -> blue -> amber -> muted grey
        _ACCENT_NEUTRAL  = "#dde3ea"
        _ACCENT_GOOD     = "#dce8fb"
        _ACCENT_WARN     = "#efe6d6"
        _ACCENT_CRIT     = "#e6e9ee"

        def _accent_for(fg_color):
            if fg_color == OK:    return _ACCENT_GOOD
            if fg_color == WARN:  return _ACCENT_WARN
            if fg_color == CRIT:  return _ACCENT_CRIT
            return _ACCENT_NEUTRAL

        def _apply_tile(lbl, text, fg, key=None):
            lbl.config(text=text, fg=fg)
            # Update the accent bar if the tile carries the reference
            try:
                lbl._accent.config(bg=_accent_for(fg))
            except AttributeError:
                pass
            # Delta trend label
            try:
                if key is not None:
                    try:
                        current = int(text)
                    except ValueError:
                        current = None
                    if current is not None:
                        prev = self._prev_counts.get(key)
                        delta_txt, delta_fg = "", TEXT_MUTED
                        if prev is not None:
                            if current > prev:
                                delta_txt = f"+{current - prev}"
                                delta_fg  = OK if fg != CRIT else WARN
                            elif current < prev:
                                delta_txt = f"-{prev - current}"
                                delta_fg  = TEXT_MUTED
                        self._prev_counts[key] = current
                        self._flash_tile_delta(lbl, delta_txt, delta_fg)
            except AttributeError:
                pass

        # Bottom bar: last refresh + health summary
        try:
            self.lbl_last_refresh.config(text=f"Refreshed: {time.strftime('%H:%M:%S')}")
        except Exception:
            pass

        inbox_fg = self._threshold_color("inbox", inbox_n)
        _apply_tile(self.lbl_inbox,     _fmt(inbox_n),     inbox_fg,  key="inbox")
        # PROCESSED: blue-tint when non-zero (items in flight)
        proc_fg = INFO if processed_n else self._default_fg
        _apply_tile(self.lbl_processed, _fmt(processed_n), proc_fg,   key="processed")
        clean_fg = OK if clean_n else self._default_fg
        _apply_tile(self.lbl_clean,     _fmt(clean_n),     clean_fg,  key="clean")
        rej_fg = self._threshold_color("rejected", rejected_n)
        _apply_tile(self.lbl_rejected,  _fmt(rejected_n),  rej_fg,    key="rejected")
        rev_fg = self._threshold_color("review", review_n)
        _apply_tile(self.lbl_review,    _fmt(review_n),    rev_fg,    key="review")
        out_fg = OK if out_n else self._default_fg
        _apply_tile(self.lbl_out,       _fmt(out_n),       out_fg,    key="out")
        pend_fg = self._threshold_color("pending", pending_n)
        _apply_tile(self.lbl_pending,   _fmt(pending_n),   pend_fg,   key="pending")

    def _refresh_stats(self):
        """Update today's stats panel from V3.audit.tracker (non-blocking)."""
        def _pull():
            try:
                from V3.audit.tracker import read_dashboard_stats, read_alltime_stats
                return read_dashboard_stats(), read_alltime_stats()
            except Exception:
                return None, None

        def _apply(result):
            stats, alltime = result if result else (None, None)
            if not stats:
                return
            complete = int(stats["hot_complete"] or 0)
            review   = int(stats["hot_review"]   or 0)
            failed   = int(stats["hot_failed"]   or 0)
            # PROCESSED = resolved files only (excludes IN-PROGRESS rows)
            resolved = complete + review + failed

            # ── Stale-read guard ──────────────────────────────────────────────
            # read_dashboard_stats() reads without a write lock, so it can catch
            # the xlsx mid-save and return all-zeros.  If the new resolved count
            # is zero but we previously had non-zero data, treat this as a bad
            # read and keep the last-known-good values on screen.
            _last = self._perf_last_good_stats
            if _last is not None:
                _last_resolved = (
                    int(_last.get("hot_complete", 0))
                    + int(_last.get("hot_review", 0))
                    + int(_last.get("hot_failed", 0))
                )
                if resolved == 0 and _last_resolved > 0:
                    return  # skip — looks like a corrupt/empty read
            self._perf_last_good_stats = stats
            # ─────────────────────────────────────────────────────────────────

            self._stat_labels["hot_total"].config(text=str(resolved))
            self._stat_labels["hot_complete"].config(text=str(complete))
            self._stat_labels["hot_review"].config(
                text=str(review),
                fg=CRIT if review > 0 else REVIEW)
            # Progress bar — rate = matched / resolved
            rate = complete / max(1, resolved)
            try:
                self._perf_bar_fill.place(relwidth=rate)
                self._perf_rate_lbl.config(
                    text=f"{rate * 100:.1f}%  ({complete} matched / {resolved} resolved)")
                bar_color = OK if rate >= 0.9 else (REVIEW if rate >= 0.7 else CRIT)
                self._perf_bar_fill.config(bg=bar_color)
            except Exception:
                pass
            edm_clean_v   = int(stats["edm_clean"]    or 0)
            edm_partial_v = int(stats["edm_partial"]  or 0)
            edm_rej_v     = int(stats["edm_rejected"]  or 0)
            edm_checked   = edm_clean_v + edm_partial_v + edm_rej_v
            self._stat_labels["edm_clean"].config(text=str(edm_clean_v))
            self._stat_labels["edm_rejected"].config(
                text=f"Rejected: {edm_rej_v}",
                fg=CRIT if edm_rej_v > 0 else self._default_fg)
            try:
                self._perf_edm_partial_lbl.config(
                    text=f"Partial-clean: {edm_partial_v}",
                    fg=REVIEW if edm_partial_v > 0 else TEXT_SEC)
                self._perf_edm_total_lbl.config(
                    text=f"Checked today: {edm_checked}")
                avg = str(stats.get("avg_secs", "N/A") or "N/A")
                self._perf_avg_lbl.config(text=f"Avg: {avg}")
            except Exception:
                pass
            self._stat_labels["batches_built"].config(
                text=str(int(stats["batches_built"] or 0)))
            self._stat_labels["tiffs"].config(
                text=f"TIFFs: {int(stats['tiffs_converted'] or 0)}")
            ts = int(stats.get("batch_tier_strong", 0) or 0)
            tm = int(stats.get("batch_tier_mix",    0) or 0)
            tw = int(stats.get("batch_tier_weak",   0) or 0)
            self._stat_labels["batch_tiers"].config(
                text=f"Tier  S: {ts}  ·  M: {tm}  ·  W: {tw}")
            # All-time strip
            if alltime:
                try:
                    ac = int(alltime["all_complete"] or 0)
                    ar = int(alltime["all_review"]   or 0)
                    af = int(alltime["all_failed"]   or 0)
                    self._alltime_summary_lbl.config(
                        text=f"{ac:,}  matched  ·  {ar}  review  ·  {af}  failed")
                except Exception:
                    pass

        if self._stats_inflight:
            return
        self._stats_inflight = True

        def _thread():
            try:
                s = _pull()
                self.after(0, lambda: _apply(s))
            finally:
                self._stats_inflight = False

        threading.Thread(target=_thread, daemon=True).start()

    def _refresh_live_status(self):
        awb_on   = self.is_awb_running()
        edm_dup_on = self.is_edm_duplicate_running()
        batch_on = self.batch_running

        # ── Live status dots in header ────────────────────────────────────────
        if awb_on and self._awb_start_time is not None:
            awb_elapsed = int(time.time() - self._awb_start_time)
            aw_h, aw_rem = divmod(awb_elapsed, 3600)
            aw_m, aw_s   = divmod(aw_rem, 60)
            awb_dur = f" {aw_h}h{aw_m:02d}m" if aw_h else f" {aw_m}m{aw_s:02d}s"
        else:
            awb_dur = ""
        try:
            self._dot_awb.config(
                text=f"● AWB{awb_dur}" if awb_on else "○ AWB",
                fg=OK if awb_on else STRIP_IDLE,
            )
            edm_on = self.edm_enabled
            self._dot_edm.config(
                text="● EDM" if edm_on else "○ EDM",
                fg=INFO if edm_on else STRIP_IDLE,
            )
            self._dot_batch.config(
                text="● BATCH" if batch_on else "○ BATCH",
                fg=INFO if batch_on else STRIP_IDLE,
            )
            self._dot_auto.config(
                text=f"● AUTO" if self.auto_running else "○ AUTO",
                fg=INFO if self.auto_running else STRIP_IDLE,
            )
        except Exception:
            pass

        edm_on = self.edm_enabled

        # ── Sync Live Activity stage pills with actual service state ──────────
        if hasattr(self, "_stage_pills"):
            _IDLE_DOT = "#c8d0da"
            for _sk, _running in (("AWB", awb_on), ("EDM", edm_dup_on), ("BATCH", batch_on)):
                _info = self._stage_pills.get(_sk)
                if _info is None:
                    continue
                _, _dot, _lbl, _acol, _dcol = _info
                try:
                    if _running:
                        # Dot is handled by _blink_tick; only sync the text colour.
                        _lbl.config(fg=TEXT_FG)
                    else:
                        _dot.itemconfig("dot", fill=_IDLE_DOT, outline=_IDLE_DOT)
                        _lbl.config(fg=TEXT_MUTED)
                except Exception:
                    pass
        # ─────────────────────────────────────────────────────────────────────

        # Primary control button colours: keep top row visually unified with Full Cycle.
        top_row_bg = "#4a33a2"
        awb_idle_bg = top_row_bg
        top_row_hover_bg = "#5a42b8"
        self.btn_get_awb.config(
            text="Stop Scan" if awb_on else "Start Scan",
            bg=top_row_bg if awb_on else awb_idle_bg,
            fg="white",
            activebackground=top_row_hover_bg,
            activeforeground="white",
            highlightbackground=top_row_bg if awb_on else awb_idle_bg,
        )
        self.btn_auto.config(
            text="Stop AUTO" if self.auto_running else "AUTO MODE",
            bg=top_row_bg,
            fg="white",
            activebackground=top_row_hover_bg,
            activeforeground="white",
            highlightbackground=top_row_bg,
        )
        if hasattr(self, "btn_edm_runner"):
            if edm_dup_on:
                self.btn_edm_runner.config(
                    text="Stop EDM Check",
                    bg="#e9f1ff",
                    fg=INFO,
                    activebackground="#e9f1ff",
                    activeforeground=INFO,
                    highlightbackground="#e9f1ff",
                )
            else:
                idle_fg = INFO if self.edm_enabled else TEXT_SEC
                self.btn_edm_runner.config(
                    text="Start EDM Check",
                    bg="#eef3fb",
                    fg=idle_fg,
                    activebackground="#eef3fb",
                    activeforeground=idle_fg,
                    highlightbackground="#eef3fb",
                )
        self._apply_toolbar_button_icons()

        # Button state management — clear-all always available (guarded in handler)
        self.btn_clear_all.config(state="normal")
        self._update_menu_labels()
        self._update_run_overview()
        self._update_status_badges()
        self._update_stage_status_panel()

    def _set_auto_phase(self, phase: str):
        self.auto_phase = phase
        self.after(0, self._refresh_live_status)

    def _set_batch_running(self, running: bool):
        self.batch_running = running
        self.after(0, lambda: self.btn_batch.config(
            state="disabled" if running else "normal"))
        if hasattr(self, "btn_tiff"):
            self.after(0, lambda: self.btn_tiff.config(
                state="disabled" if running else "normal"))
        self.after(0, self._refresh_live_status)
        if running:
            self._update_batch_prep_summary(
                state="BUILDING",
                primary="Building batch output",
                line1="PDF stacks: in progress",
                line2="TIFF prepared: pending",
                line3="Pending print: updating...",
                fg=INFO,
            )
        try:
            self._dot_batch.config(text="● BATCH" if running else "○ BATCH", fg=INFO if running else STRIP_IDLE)
        except Exception:
            pass

    # ─────────────────────────────────────────────────────────────────────────
    # LOG / FILE HELPERS
    # ─────────────────────────────────────────────────────────────────────────
    def _open_file(self, path: Path):
        path = Path(path)
        if not path.exists():
            self.log_append(f"[OPEN ERROR] File not found: {path}")
            return
        try:
            if os.name == "nt":
                os.startfile(str(path))
            elif sys.platform == "darwin":
                # macOS: some extensions (e.g., .jsonl) may have no default app.
                # Prefer text-mode open first, then explicit TextEdit fallback.
                suffix = path.suffix.lower()
                text_like = {".jsonl", ".log", ".txt", ".csv", ".env", ".md", ".py"}
                if suffix in text_like:
                    candidates = [
                        ["open", "-t", str(path)],
                        ["open", "-a", "TextEdit", str(path)],
                        ["open", str(path)],
                    ]
                else:
                    candidates = [["open", str(path)]]

                opened = False
                for cmd in candidates:
                    proc = subprocess.run(cmd, check=False, capture_output=True, text=True)
                    if proc.returncode == 0:
                        opened = True
                        break
                if not opened:
                    raise RuntimeError(f"macOS could not open file: {path.name}")
            else:
                subprocess.run(["xdg-open", str(path)], check=False)
            self.log_append(f"[OPEN] {path.name}")
        except Exception as e:
            self.log_append(f"[OPEN ERROR] {e}")

    def _export_log(self):
        ts = time.strftime("%Y%m%d_%H%M%S")
        default_name = f"pipeline_log_{ts}.txt"
        out_path = filedialog.asksaveasfilename(
            title="Export UI Log",
            defaultextension=".txt",
            initialfile=default_name,
            initialdir=str(config.DATA_DIR),
            filetypes=[("Text Files", "*.txt"), ("All Files", "*.*")],
        )
        if not out_path:
            return
        try:
            content = "\n".join(self._log_export_lines)
            Path(out_path).write_text(content, encoding="utf-8")
            self.log_append(f"[LOG] Exported: {Path(out_path).name}")
            self.set_status("Log exported.")
        except Exception as e:
            self.log_append(f"[LOG ERROR] {e}")

    def _on_search_log(self, _event=None):
        """No-op — search removed in fixed-widget live activity panel."""

    def _run_debounced_log_search(self):
        """No-op."""

    def _schedule_search_refresh(self, delay_ms: int = 100):
        """No-op."""

    def _run_scheduled_search_refresh(self):
        """No-op."""

    def _apply_search_highlight(self):
        """No-op — no scrollable log rows in the new panel."""

    # ─────────────────────────────────────────────────────────────────────────
    # FOLDER OPEN
    # ─────────────────────────────────────────────────────────────────────────
    def open_folder(self, folder: Path):
        folder = Path(folder)
        folder.mkdir(parents=True, exist_ok=True)
        try:
            if os.name == "nt":
                os.startfile(str(folder))
            elif sys.platform == "darwin":
                subprocess.Popen(["open", str(folder)])
            else:
                subprocess.Popen(["xdg-open", str(folder)])
            self.log_append(f"[OPEN] {folder.name}")
        except Exception as e:
            self.log_append(f"[OPEN ERROR] {e}")

    # ─────────────────────────────────────────────────────────────────────────
    # UPLOAD FILES
    # ─────────────────────────────────────────────────────────────────────────
    def on_upload_files(self):
        files = filedialog.askopenfilenames(
            title="Select files to upload to INBOX",
            initialdir=str(config.BASE_DIR),
            filetypes=[("PDF files", "*.pdf"), ("All files", "*.*")],
        )
        if not files:
            return

        def _copy():
            config.INBOX_DIR.mkdir(parents=True, exist_ok=True)
            copied = 0
            for src in files:
                src_path = Path(src)
                dst = _next_available_path(config.INBOX_DIR, src_path.name)
                try:
                    shutil.copy2(str(src_path), str(dst))
                    self.log_append(f"[UPLOAD] {src_path.name}  ->  INBOX/{dst.name}")
                    copied += 1
                except Exception as e:
                    self.log_append(f"[UPLOAD ERROR] {src_path.name}: {e}")
            self.set_status(f"Uploaded {copied} file(s) to INBOX.")
            self.after(0, lambda: self._request_count_refresh(80))

        self.run_in_thread(_copy)

    # ─────────────────────────────────────────────────────────────────────────
    # REFRESH DB
    # ─────────────────────────────────────────────────────────────────────────
    def on_refresh_db(self):
        """Drop a trigger file so the AWB hotfolder reloads its DB on next loop tick."""
        try:
            config.DATA_DIR.mkdir(parents=True, exist_ok=True)
            config.AWB_RELOAD_TRIGGER.touch()
            self.log_append("[DB] Refresh signal sent — AWB hotfolder will reload on next cycle.")
            self.set_status("DB refresh triggered.")
            self._show_toast("DB refresh signal sent", "success")
        except Exception as e:
            self.log_append(f"[DB] Failed to signal refresh: {e}")
            self._show_toast("DB refresh failed", "error")

    # ─────────────────────────────────────────────────────────────────────────
    # UI HELPERS
    # ─────────────────────────────────────────────────────────────────────────
    def clear_log(self):
        for _attr in ("_count_refresh_job", "_log_search_job", "_log_filter_refresh_job"):
            _job = getattr(self, _attr, None)
            if _job is not None:
                try:
                    self.after_cancel(_job)
                except Exception:
                    pass
                setattr(self, _attr, None)
        # Cancel all stage-pill pulse jobs
        for _job2 in list(getattr(self, "_stage_active_jobs", {}).values()):
            try:
                self.after_cancel(_job2)
            except Exception:
                pass
        if hasattr(self, "_stage_active_jobs"):
            self._stage_active_jobs.clear()
        self._log_rows = []
        self._log_lines = []
        self._log_export_lines = []
        self.log_widget.configure(state="normal")
        self.log_widget.delete("1.0", tk.END)
        self.log_widget.configure(state="disabled")
        # Reset stage pills to idle
        _IDLE_DOT = "#c8d0da"
        for _sk, (_pf, _dot, _lbl, _acol, _dcol) in getattr(self, "_stage_pills", {}).items():
            try:
                _dot.itemconfig("dot", fill=_IDLE_DOT, outline=_IDLE_DOT)
                _lbl.config(fg=TEXT_MUTED)
            except Exception:
                pass
        # Reset recent AWB rows
        for (_d2, _l2, _t2) in getattr(self, "_recent_awb_lbls", []):
            try:
                _d2.itemconfig("dot", fill=_IDLE_DOT, outline=_IDLE_DOT)
                _l2.config(text="—", fg=TEXT_MUTED)
                _t2.config(text="")
            except Exception:
                pass
        # Reset status line
        try:
            self._live_status_lbl.config(text="Ready", fg=TEXT_MUTED)
        except Exception:
            pass
        if hasattr(self, "_match_cards"):
            self._match_cards.clear()
        self._update_match_summary(timing_text="")
        self._update_edm_duplicate_summary(timing_text="")
        self._update_batch_prep_summary()
        self._update_stage_status_panel()

    def set_status(self, msg: str):
        self.status_var.set(msg)
        self._update_run_overview()

    def log_append(self, msg: str):
        """Append a log message. No widgets are created — only pre-built labels are updated."""
        if getattr(self, "_is_closing", False):
            return

        def _do():
            message = str(msg).rstrip("\n")
            if message == "":
                message = " "
            payload = None
            if message.startswith("{") and message.endswith("}"):
                try:
                    payload = json.loads(message)
                except Exception:
                    payload = None

            # Hidden plain-text mirror for log export.
            self.log_widget.configure(state="normal")
            line_start = self.log_widget.index(tk.END)
            self.log_widget.insert(tk.END, message + "\n")
            msg_upper = message.upper()
            tag_name = "info"
            for cand, _colors, keywords in LOG_TAGS:
                if any(kw.upper() in msg_upper for kw in keywords):
                    tag_name = cand
                    row_idx = int(line_start.split(".")[0])
                    self.log_widget.tag_add(cand, f"{row_idx}.0", f"{row_idx}.end")
                    break
            self.log_widget.configure(state="disabled")

            ts = time.strftime("%H:%M:%S")
            pretty_message = self._format_timeline_message(message, tag_name)
            hard_error = self._is_hard_error_event(message, tag_name)
            visible_message = pretty_message if hard_error else self._frontend_visible_message(pretty_message)
            badge_text = {
                "error": "ERR", "warn": "WARN", "success": "OK",
                "review": "REVIEW", "rejected": "REJECT",
                "stage": "STEP", "token": "EDM", "skip": "SKIP", "info": "INFO",
            }.get(tag_name, "INFO")

            # Export log (plain text only — no widget list).
            self._log_lines.append(message)
            self._log_export_lines.append(self._build_export_line(badge_text, pretty_message))
            if len(self._log_lines) > LOG_MAX_LINES:
                self._log_lines = self._log_lines[-LOG_MAX_LINES:]
            if len(self._log_export_lines) > LOG_MAX_LINES:
                self._log_export_lines = self._log_export_lines[-LOG_MAX_LINES:]
            total_lines = int(self.log_widget.index("end-1c").split(".")[0])
            if total_lines > LOG_MAX_LINES:
                excess = total_lines - LOG_MAX_LINES
                self.log_widget.configure(state="normal")
                self.log_widget.delete("1.0", f"{excess + 1}.0")
                self.log_widget.configure(state="disabled")

            # Pulse the matching stage pill (config-only — no widget creation).
            stage_key = self._classify_activity_stage(message, payload, pretty_message)
            self._pulse_stage(stage_key)

            # Update the status line for user-visible messages.
            if visible_message:
                try:
                    self._live_status_lbl.config(
                        text=f"{ts}  {visible_message[:90]}",
                        fg=TEXT_SEC if not hard_error else CRIT,
                    )
                except Exception:
                    pass

            # Track current file being processed — shown in right side of status line.
            if message.startswith("Processing: ") or "[THIRD-PASS] Resuming: " in message:
                _colon = message.find(": ")
                if _colon >= 0:
                    _fname = message[_colon + 2:].strip()
                    try:
                        self._current_file_lbl.config(
                            text=f"{ts}  ·  {_fname}", fg=TEXT_MUTED)
                    except Exception:
                        pass

            # Deferred / skipped files → push to activity rows with orange dot.
            if "[DEFER]" in message:
                _defer_text = message.split("[DEFER]", 1)[-1].strip()
                self._push_activity_line(_defer_text, REVIEW, REVIEW, ts)

            # Timing fallback: keep existing match card timing fresh.
            text_timing_ms = self._extract_total_active_ms_from_timing_line(message)
            if text_timing_ms is not None:
                try:
                    if hasattr(self, "lbl_match_state") and str(self.lbl_match_state.cget("text")).upper().startswith("AWB MATCHED"):
                        self._update_match_summary(
                            state=self.lbl_match_state.cget("text"),
                            primary=(
                                f"{self.lbl_match_primary_prefix.cget('text')}"
                                f"{self.lbl_match_primary_value.cget('text')}"
                                f"{self.lbl_match_primary_suffix.cget('text')}"
                            ).strip() or "No active match",
                            line1=self.lbl_match_line1.cget("text"),
                            line2=self.lbl_match_line2.cget("text"),
                            line3=self.lbl_match_line3.cget("text"),
                            timing_text=self._format_seconds_only(text_timing_ms),
                            fg=OK,
                        )
                except Exception:
                    pass

            if self._should_refresh_counts_from_event(message, payload):
                self._request_count_refresh(120)
                if (not self.edm_enabled
                        and not self.is_edm_duplicate_running()
                        and isinstance(payload, dict)
                        and str(payload.get("stage", "")).upper() == "AWB_HOTFOLDER"):
                    self._schedule_edm_off_clean_move()

            if (isinstance(payload, dict)
                    and str(payload.get("stage", "")).upper() == "AWB_HOTFOLDER"
                    and str(payload.get("status", "")).upper() == "MATCHED"):
                method = str(payload.get("match_method", "Matched"))
                route = str(payload.get("route", "PROCESSED"))
                awb = str(payload.get("awb", "—"))
                match_ms = self._extract_timing_ms(payload.get("timings_ms"))
                self._mark_summary_event("match")
                bucket = self._candidate_bucket(method)
                if bucket in self._batch_candidate_counts:
                    self._batch_candidate_counts[bucket] += 1
                    self._batch_tier_totals[bucket] += 1
                    self._refresh_batch_candidate_summary()
                    self._schedule_batch_candidate_reset()
                signature = (payload.get("ts"), payload.get("file"), awb, method, route)
                hit_no = self._next_match_badge(signature)
                conf = self._infer_match_confidence(method)
                timing_str = self._format_seconds_only(match_ms) if match_ms is not None else ""
                self._update_match_summary(
                    state=f"AWB MATCHED · {hit_no}",
                    primary=f"AWB {awb}",
                    line1=f"Type: {self._short_reason(method, 30)}",
                    line2=f"Confidence: {conf}",
                    line3=f"Route: {route}",
                    timing_text=timing_str,
                    fg=OK,
                )
                # Push to the fixed recent-AWBs panel (config-only).
                self._push_recent_awb(awb, method, conf, timing_str)

        try:
            self.after(0, _do)
        except Exception:
            pass

    def _should_refresh_counts_from_event(self, message: str, payload) -> bool:
        if isinstance(payload, dict):
            stage = str(payload.get("stage", "")).upper()
            status = str(payload.get("status", "")).upper()
            action = str(payload.get("action", "")).lower()
            route = str(payload.get("route", "")).upper()
            if stage in {"AWB_HOTFOLDER", "EDM_CHECK", "BATCH"}:
                return True
            if route in {"PROCESSED", "CLEAN", "REJECTED", "NEEDS_REVIEW", "OUT"}:
                return True
            if action in {"copy_to_pending_print", "build_print_stacks"}:
                return True
            if status in {"MATCHED", "CLEAN", "PARTIAL-CLEAN", "REJECTED", "DONE", "OK"}:
                return True
        m = str(message or "").strip().upper()
        return m.startswith("[UPLOAD]") or m.startswith("[RETRY]") or m.startswith("[CLEAR]")

    def run_in_thread(self, fn):
        def wrapper():
            try:
                fn()
            except Exception as e:
                self.after(0, lambda e=e: messagebox.showerror("Error", str(e)))
                self.log_append(f"[ERROR] {e}")
                self.set_status("Ready.")
        threading.Thread(target=wrapper, daemon=True).start()

    def _make_env(self) -> dict:
        """Build subprocess environment with employee ID and UTF-8 flags."""
        env = os.environ.copy()
        env["PYTHONUTF8"]           = "1"
        env["PYTHONIOENCODING"]     = "utf-8"
        env["PIPELINE_EMPLOYEE_ID"] = self.employee_id
        env["PIPELINE_EDM_ENABLED"] = "1" if self.edm_enabled else "0"
        return env

    def _popen_utf8(self, cmd_args: list):
        """Launch a subprocess with UTF-8 encoding and live stdout piping."""
        self.log_append(f"Running: {' '.join(str(a) for a in cmd_args[-2:])}")
        popen_kwargs = dict(
            args=cmd_args,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1, universal_newlines=True,
            env=self._make_env(),
            cwd=str(_ROOT),
        )
        # Windows: keep child console windows hidden so all output stays in-app.
        if os.name == "nt":
            try:
                popen_kwargs["creationflags"] = getattr(subprocess, "CREATE_NO_WINDOW", 0)
                si = subprocess.STARTUPINFO()
                si.dwFlags |= subprocess.STARTF_USESHOWWINDOW
                si.wShowWindow = 0
                popen_kwargs["startupinfo"] = si
            except Exception:
                pass
        return subprocess.Popen(**popen_kwargs)

    def run_script_blocking_live(self, cmd_args: list):
        """Run a subprocess to completion, streaming stdout to the log."""
        p = self._popen_utf8(cmd_args)
        for line in p.stdout:
            self.log_append(line.rstrip("\n"))
        rc = p.wait()
        if rc != 0:
            raise RuntimeError(f"Script failed (exit {rc}). See log above.")

    # ─────────────────────────────────────────────────────────────────────────
    # AWB HOTFOLDER
    # ─────────────────────────────────────────────────────────────────────────
    def is_awb_running(self) -> bool:
        return self.awb_proc is not None and self.awb_proc.poll() is None

    def is_edm_duplicate_running(self) -> bool:
        return self.edm_proc is not None and self.edm_proc.poll() is None

    def _start_edm_duplicate_checker(self):
        if self.is_edm_duplicate_running():
            return
        self.log_append("Starting EDM duplicate checker...")
        self.log_append(f"[EDM] Log file: {config.EDM_LOG}")
        cmd = [sys.executable, "-u", "-m", "V3.services.edm_duplicate_checker"]
        proc = self._popen_utf8(cmd)
        self.edm_proc = proc
        self._expected_edm_stops.discard(id(proc))
        self._refresh_live_status()

        def reader():
            try:
                for line in proc.stdout:
                    self.log_append(line.rstrip("\n"))
            except Exception as e:
                self.log_append(f"[EDM DUP ERROR] {e}")
            rc = proc.wait()
            expected_stop = id(proc) in self._expected_edm_stops
            self._expected_edm_stops.discard(id(proc))
            if self.edm_proc is proc:
                self.edm_proc = None
            self.after(0, self._refresh_live_status)
            if self.is_awb_running() and not expected_stop:
                self.log_append(
                    "[EDM] Duplicate checker stopped while AWB is running. "
                    "Duplicate-routing protection is now reduced."
                )
            if rc != 0 and not expected_stop:
                self.log_append(f"[EDM] Duplicate checker exited with code {rc}.")

        threading.Thread(target=reader, daemon=True).start()

    def _stop_edm_duplicate_checker(self):
        proc = self.edm_proc
        if proc is None or proc.poll() is not None:
            self.edm_proc = None
            self._refresh_live_status()
            return
        if id(proc) in self._expected_edm_stops:
            return
        self._expected_edm_stops.add(id(proc))
        self.log_append("Stopping EDM duplicate checker...")
        try:
            proc.terminate()
            time.sleep(1)
            if proc.poll() is None:
                proc.kill()
        except Exception:
            pass
        finally:
            if self.edm_proc is proc and proc.poll() is not None:
                self.edm_proc = None
            self._refresh_live_status()

    def start_awb(self):
        if self.is_awb_running():
            return
        save_state({"last_run_id": now_run_id()})
        self._awb_start_time = time.time()
        self.set_status("AWB Hotfolder running...")
        self.log_append("\n=== AWB Hotfolder started ===")
        cmd = [sys.executable, "-u", "-m", "V3.services.hotfolder"]
        self.awb_proc = self._popen_utf8(cmd)
        self.btn_get_awb.config(text="Stop Scan")
        self._refresh_live_status()

        def reader():
            try:
                for line in self.awb_proc.stdout:
                    self.log_append(line.rstrip("\n"))
            except Exception as e:
                self.log_append(f"[AWB ERROR] {e}")
            rc = self.awb_proc.wait()
            self.awb_proc = None
            self._awb_start_time = None
            self.after(0, lambda: self.btn_get_awb.config(text="Start Scan"))
            self.after(0, self._refresh_live_status)
            self.set_status("AWB search stopped.")
            if rc not in (0, -15) and not self._is_closing:
                self.log_append(
                    f"[HOTFOLDER CRASH] Process exited with code {rc} — click Start Scan to restart"
                )

        threading.Thread(target=reader, daemon=True).start()

        # In manual AWB mode, also launch EDM duplicate checker when EDM is enabled.
        # This keeps duplicate routing/logging active without requiring AUTO/FULL CYCLE.
        if self.edm_enabled and not self.is_edm_duplicate_running():
            self._start_edm_duplicate_checker()

    def on_toggle_edm_duplicate_checker(self):
        if self.is_edm_duplicate_running():
            self._stop_edm_duplicate_checker()
            self.set_status("EDM duplicate checker stopped.")
            self._show_toast("EDM check stopped", "warn")
            return

        if not self.edm_enabled:
            self.log_append(
                "[EDM] EDM fallback is OFF. Manual checker will run in CLEAN-UNCHECKED mode."
            )
        self._start_edm_duplicate_checker()
        self.set_status("EDM duplicate checker started.")
        self._show_toast("EDM check started", "success")

    def stop_awb(self, stop_edm_checker: bool = True):
        awb_running = self.is_awb_running()
        edm_running = self.is_edm_duplicate_running()
        if not awb_running and (not edm_running or not stop_edm_checker):
            self.awb_proc = None
            if stop_edm_checker:
                self.edm_proc = None
            self.btn_get_awb.config(text="Start Scan")
            self._refresh_live_status()
            return
        if awb_running:
            self.log_append("Stopping AWB Hotfolder...")
            try:
                self.awb_proc.terminate()
                time.sleep(1)
                if self.awb_proc.poll() is None:
                    self.awb_proc.kill()
            except Exception:
                pass
        if stop_edm_checker:
            self._stop_edm_duplicate_checker()
        self.btn_get_awb.config(text="Start Scan")
        self._refresh_live_status()

    def on_toggle_get_awb(self):
        if self.is_awb_running():
            self.stop_awb()
            self._show_toast("AWB hotfolder stopped", "warn")
        else:
            self.start_awb()
            self._show_toast("AWB hotfolder started", "success")

    # ─────────────────────────────────────────────────────────────────────────
    # EDM TOGGLE
    # ─────────────────────────────────────────────────────────────────────────
    def on_toggle_edm_checker(self):
        self.edm_enabled = not self.edm_enabled
        self._persist_edm_toggle()
        self._apply_edm_button_state()
        self._refresh_live_status()

        mode = "ON" if self.edm_enabled else "OFF"
        self.log_append(
            f"[EDM] EDM fallback set to {mode} "
            f"({'API calls enabled' if self.edm_enabled else 'API calls bypassed'})."
        )
        if self.is_awb_running():
            self.log_append("[EDM] Change applies immediately to running hotfolder checks.")
        if self.is_edm_duplicate_running() and not self.edm_enabled:
            self.log_append(
                "[EDM] Duplicate checker remains active and will route files as CLEAN-UNCHECKED while EDM is OFF."
            )
        self.set_status(f"EDM fallback is now {mode}.")
        self._show_toast(f"EDM fallback {mode}", "info" if self.edm_enabled else "warn")

    # ─────────────────────────────────────────────────────────────────────────
    # PREPARE BATCH
    # ─────────────────────────────────────────────────────────────────────────
    def _run_batch_once(self, tag: str = "[BATCH]", min_batches: int = 1) -> bool:
        if self.batch_running:
            self.log_append(f"{tag} Batch already running — skipping.")
            return False

        n = clean_pdf_count()
        if n == 0:
            self.log_append(f"{tag} CLEAN folder is empty — nothing to batch.")
            self.set_status("CLEAN is empty.")
            return False

        if min_batches > 1:
            estimated = _estimate_batch_count()
            self.log_append(f"{tag} Estimated batches: {estimated} (minimum required: {min_batches})")
            if estimated < min_batches:
                self.log_append(f"{tag} Not enough files for {min_batches} batches yet — waiting.")
                return False

        self._set_batch_running(True)
        try:
            self.set_status(f"Building batch from {n} CLEAN file(s)...")
            self.log_append(f"\n=== {tag} Prepare Batch ({n} file(s) in CLEAN) ===")
            cmd = [sys.executable, "-u", "-m", "V3.services.batch_builder"]
            self.run_script_blocking_live(cmd)
            self.log_append(f"{tag} Batch complete.")
            self.set_status("Batch complete.")
            return True
        finally:
            self._set_batch_running(False)

    def on_prepare_batch(self):
        if self.batch_running:
            self.log_append("[BATCH] Batch already running.")
            return
        self.log_append("OK Preparing batch")
        self._show_toast("Batch build started", "info")
        self.run_in_thread(lambda: self._run_batch_once(tag="[BATCH]", min_batches=1))

    def _run_tiff_once(self, tag: str = "[TIFF]") -> bool:
        out_pdfs = sorted(config.OUT_DIR.glob(f"{config.PRINT_STACK_BASENAME}_*.pdf"))
        if not out_pdfs:
            self.log_append(f"{tag} No print stacks found in OUT.")
            self.set_status("No print stacks ready for TIFF.")
            return False

        self.set_status(f"Converting {len(out_pdfs)} print stack(s) to TIFF...")
        self.log_append(f"\n=== {tag} Convert to TIFF ({len(out_pdfs)} print stack(s)) ===")
        cmd = [sys.executable, "-u", "-m", "V3.services.tiff_converter"]
        self.run_script_blocking_live(cmd)
        self.log_append(f"{tag} TIFF conversion complete.")
        self.set_status("TIFF conversion complete.")
        return True

    def on_convert_tiff(self):
        if self.batch_running:
            self.log_append("[TIFF] Batch build is running. Wait for it to finish first.")
            self.set_status("Batch is running.")
            return
        self._show_toast("TIFF conversion started", "info")
        self.run_in_thread(lambda: self._run_tiff_once(tag="[TIFF]"))

    # ─────────────────────────────────────────────────────────────────────────
    # FULL CYCLE ONCE
    # ─────────────────────────────────────────────────────────────────────────
    def _set_full_cycle_running(self, running: bool):
        self.full_cycle_running = running
        self.after(0, lambda: self.btn_full_cycle.config(
            state="disabled" if running else "normal"))

    def on_run_full_cycle_once(self):
        if self.full_cycle_running:
            self.log_append("[CYCLE] Full cycle is already running.")
            return
        if self.batch_running:
            self.log_append("[CYCLE] Batch build is currently running. Try again after it completes.")
            self.set_status("Batch is running.")
            return
        if self.auto_running:
            if not messagebox.askyesno(
                "AUTO MODE Running",
                "AUTO MODE is currently running.\n\n"
                "Run Full Cycle Once requires AUTO MODE to stop first.\n\nContinue?",
            ):
                return
            self.stop_auto_mode()
        self._show_toast("Full cycle started", "info")

        def _job():
            started_awb = False
            self._set_full_cycle_running(True)
            self.full_cycle_stop_event.clear()
            try:
                self.log_append("\n=== [CYCLE] Full Cycle Once started ===")
                self.set_status("Full cycle: preparing services...")

                # Start Scan if not running
                if not self.is_awb_running():
                    started_awb = True
                    self.start_awb()
                    time.sleep(0.5)
                if self.edm_enabled and not self.is_edm_duplicate_running():
                    self._start_edm_duplicate_checker()

                # Wait for INBOX to drain
                self.set_status("Full cycle: waiting INBOX empty...")
                ok_inbox = wait_until_inbox_empty(
                    self.log_append,
                    INBOX_EMPTY_STABLE_SECONDS,
                    INBOX_EMPTY_MAX_WAIT,
                    stop_event=self.full_cycle_stop_event,
                )
                if not ok_inbox:
                    self.log_append("[CYCLE] INBOX wait cancelled or timed out. Aborting cycle.")
                    return

                # Let EDM duplicate checker drain PROCESSED when available.
                if self.is_edm_duplicate_running():
                    self.set_status("Full cycle: waiting EDM checker to drain PROCESSED...")
                elif self.edm_enabled:
                    self.set_status("Full cycle: waiting for EDM checker (toggle ON)...")
                    self.log_append(
                        "[CYCLE] EDM toggle ON but checker not running — skipping fallback move."
                    )
                else:
                    self.set_status("Full cycle: EDM checker OFF; legacy PROCESSED->CLEAN move...")
                    self.log_append(
                        "[CYCLE] Warning: EDM checker OFF — falling back to direct PROCESSED->CLEAN move."
                    )
                    self._move_processed_to_clean(tag="[CYCLE]")

                # Wait for PROCESSED to be empty (should already be after move)
                self.set_status("Full cycle: confirming PROCESSED drained...")
                ok_processed = wait_until_processed_empty(
                    self.log_append,
                    PROCESSED_EMPTY_STABLE_SECONDS,
                    PROCESSED_EMPTY_MAX_WAIT,
                    stop_event=self.full_cycle_stop_event,
                )
                if not ok_processed:
                    self.log_append("[CYCLE] PROCESSED wait cancelled or timed out. Aborting cycle.")
                    return

                # Batch build
                self.log_append("[CYCLE] Intake drained. Running batch build...")
                did_batch = self._run_batch_once(tag="[CYCLE]", min_batches=1)
                if did_batch:
                    # TIFF conversion
                    self.log_append("[CYCLE] Running TIFF conversion...")
                    self.set_status("Full cycle: TIFF conversion...")
                    tiff_cmd = [sys.executable, "-u", "-m", "V3.services.tiff_converter"]
                    self.run_script_blocking_live(tiff_cmd)
                    self.log_append("[CYCLE] TIFF conversion complete.")
                else:
                    self.log_append("[CYCLE] Batch skipped (CLEAN empty or below threshold).")

                self.log_append("=== [CYCLE] Full Cycle Once complete ===")
                self.set_status("Full cycle complete.")
            finally:
                if started_awb and self.is_awb_running():
                    self.stop_awb()
                self._set_full_cycle_running(False)
                if self.status_var.get().startswith("Full cycle"):
                    self.set_status("Ready.")

        self.run_in_thread(_job)

    # ─────────────────────────────────────────────────────────────────────────
    # MOVE PROCESSED -> CLEAN
    # ─────────────────────────────────────────────────────────────────────────
    def _schedule_edm_off_clean_move(self):
        """Debounce PROCESSED->CLEAN auto-move when EDM toggle is OFF.

        Multiple files can finish in quick succession; the 600ms debounce
        ensures they are all swept in a single pass rather than one move per file.
        """
        if self._edm_off_move_job is not None:
            try:
                self.after_cancel(self._edm_off_move_job)
            except Exception:
                pass
        self._edm_off_move_job = self.after(600, self._do_edm_off_clean_move)

    def _do_edm_off_clean_move(self):
        self._edm_off_move_job = None
        if self.edm_enabled or self.is_edm_duplicate_running():
            return
        moved = self._move_processed_to_clean(tag="[EDM-OFF]")
        if moved:
            self._request_count_refresh(0)

    def _move_processed_to_clean(self, tag: str = "[AUTO]"):
        """Move all PDFs from PROCESSED to CLEAN for batch prep."""
        config.CLEAN_DIR.mkdir(parents=True, exist_ok=True)
        moved = 0
        for src in sorted(config.PROCESSED_DIR.glob("*.pdf")):
            if not src.exists():
                continue
            dst = _next_available_path(config.CLEAN_DIR, src.name)
            try:
                shutil.move(str(src), str(dst))
                self.log_append(f"{tag} {src.name} -> CLEAN/{dst.name}")
                moved += 1
            except Exception as e:
                self.log_append(f"{tag} ERROR moving {src.name}: {e}")
        if moved:
            self.log_append(f"{tag} Moved {moved} file(s) from PROCESSED to CLEAN.")
        return moved

    # ─────────────────────────────────────────────────────────────────────────
    # RETRY NEEDS_REVIEW
    # ─────────────────────────────────────────────────────────────────────────
    def on_retry_needs_review(self):
        review_files = sorted(config.NEEDS_REVIEW_DIR.glob("*.pdf"))
        if not review_files:
            self.log_append("[RETRY] NEEDS_REVIEW has no PDF files.")
            self.set_status("No review files to retry.")
            return

        if not messagebox.askyesno(
            "Retry NEEDS_REVIEW",
            f"Move {len(review_files)} PDF file(s) from NEEDS_REVIEW to INBOX for reprocessing?",
        ):
            return

        def _job():
            config.INBOX_DIR.mkdir(parents=True, exist_ok=True)
            moved = 0
            failed = 0
            for src in review_files:
                if not src.exists():
                    continue
                dst = _next_available_path(config.INBOX_DIR, src.name)
                try:
                    shutil.move(str(src), str(dst))
                    self.log_append(f"[RETRY] {src.name}  ->  INBOX/{dst.name}")
                    moved += 1
                except Exception as e:
                    self.log_append(f"[RETRY ERROR] {src.name}: {e}")
                    failed += 1
            self.set_status(f"Retry complete. Moved={moved}, Failed={failed}.")
            self.after(0, lambda: self._request_count_refresh(80))

        self.run_in_thread(_job)

    # ─────────────────────────────────────────────────────────────────────────
    # CLEAR ALL
    # ─────────────────────────────────────────────────────────────────────────
    def on_clear_all(self):
        if self.full_cycle_running:
            if not messagebox.askyesno(
                "Full Cycle Running",
                "Full Cycle Once is currently running.\n\n"
                "Clear All will cancel the full cycle first.\n\nContinue?",
            ):
                return
            self.full_cycle_stop_event.set()
            time.sleep(0.3)

        if self.is_awb_running() or self.is_edm_duplicate_running() or self.batch_running:
            if not messagebox.askyesno(
                "Processes Running",
                "Scripts are currently running.\n\n"
                "Clear All will stop them first.\n\nContinue?",
            ):
                return

        if not messagebox.askyesno(
            "Confirm Clear All",
            "This will stop all scripts and clear INBOX + OUT working files.\n"
            "PROCESSED, CLEAN, REJECTED, NEEDS_REVIEW and protected files\n"
            "are NOT affected.\n\nContinue?",
        ):
            return

        def job():
            if self.is_awb_running() or self.is_edm_duplicate_running():
                self.stop_awb()
                time.sleep(0.5)

            self.set_status("Clearing...")
            self.log_append("\n=== Clear All ===")
            for fp in OUTPUT_FILES_TO_CLEAR:
                if safe_delete_file(fp):
                    self.log_append(f"Deleted: {fp.name}")
            self.log_append(f"INBOX cleared:  {delete_matching(config.INBOX_DIR, WORKING_PATTERNS)} file(s)")
            self.log_append(f"OUT cleared:    {delete_matching(config.OUT_DIR, WORKING_PATTERNS)} file(s)")
            self.log_append("Protected files untouched.")
            save_state({"last_run_id": None})
            self.set_status("Clear complete. Restarting AWB...")
            if not self.is_awb_running():
                self.start_awb()
            self.after(0, lambda: self._request_count_refresh(120))

        self.run_in_thread(job)

    # ─────────────────────────────────────────────────────────────────────────
    # AUTO MODE
    # ─────────────────────────────────────────────────────────────────────────
    def on_toggle_auto_mode(self):
        if not self.auto_running and self.full_cycle_running:
            self.log_append("[AUTO] Cannot start AUTO while Full Cycle Once is running.")
            self.set_status("Stop Full Cycle Once before starting AUTO.")
            return
        if self.auto_running:
            self.stop_auto_mode()
        else:
            self.start_auto_mode()

    def start_auto_mode(self):
        if self.auto_running:
            return
        self.auto_running = True
        self.auto_stop_event.clear()
        self.btn_auto.config(text="Stop AUTO")
        self._set_auto_phase("Starting")
        self.set_status("AUTO MODE running...")
        self.log_append(f"\n=== AUTO MODE STARTED (employee: {self.employee_id or chr(8212)}) ===")
        self.log_append(
            "  V3 Flow: INBOX empty -> AWB"
            f"{' + EDM fallback' if self.edm_enabled else ''}"
            f" -> PROCESSED -> CLEAN -> batch (min {MIN_CLEAN_BATCHES_FOR_AUTO}) -> TIFF"
        )
        self._show_toast("AUTO MODE started", "success")
        self._refresh_live_status()

        # Start Scan if not running
        if not self.is_awb_running():
            self.start_awb()
        if self.edm_enabled and not self.is_edm_duplicate_running():
            self._start_edm_duplicate_checker()

        def loop():
            while not self.auto_stop_event.is_set():
                try:
                    # Snapshot CLEAN+REJECTED before waiting so we can detect growth
                    baseline = clean_plus_rejected_count()

                    # Step 1: Wait for INBOX to empty
                    if AUTO_WAIT_FOR_INBOX_EMPTY:
                        self._set_auto_phase("Waiting INBOX empty")
                        ok = wait_until_inbox_empty(
                            self.log_append,
                            INBOX_EMPTY_STABLE_SECONDS,
                            INBOX_EMPTY_MAX_WAIT,
                            stop_event=self.auto_stop_event,
                        )
                        if not ok:
                            self._set_auto_phase("Idle")
                            self._sleep_interval()
                            continue

                    # Step 2: Stop Scan so no new files arrive during batch
                    self._set_auto_phase("Stopping AWB")
                    if self.is_awb_running():
                        self.stop_awb(stop_edm_checker=False)
                        time.sleep(0.5)

                    # Step 3: Let EDM duplicate checker drain PROCESSED when available.
                    if self.is_edm_duplicate_running():
                        self._set_auto_phase("Waiting EDM checker drain")
                    elif self.edm_enabled:
                        self._set_auto_phase("Waiting EDM checker")
                        self.log_append(
                            "[AUTO] EDM toggle ON but checker not running — skipping fallback move."
                        )
                    else:
                        self._set_auto_phase("Moving PROCESSED -> CLEAN")
                        self.log_append(
                            "[AUTO] Warning: EDM checker OFF — falling back to direct PROCESSED->CLEAN move."
                        )
                        self._move_processed_to_clean(tag="[AUTO]")

                    # Step 4: Confirm PROCESSED is drained
                    self._set_auto_phase("Confirming PROCESSED drain")
                    done = wait_until_processed_empty(
                        self.log_append,
                        PROCESSED_EMPTY_STABLE_SECONDS,
                        PROCESSED_EMPTY_MAX_WAIT,
                        stop_event=self.auto_stop_event,
                    )
                    if not done:
                        self._set_auto_phase("Idle")
                        # Restart AWB before sleeping
                        if not self.is_awb_running():
                            self.start_awb()
                        self._sleep_interval()
                        continue

                    # Check that pipeline actually processed something (CLEAN+REJECTED grew)
                    current = clean_plus_rejected_count()
                    if current <= baseline:
                        self.log_append(
                            f"[AUTO] CLEAN+REJECTED unchanged ({current}) — "
                            "no new files routed yet. Waiting."
                        )
                        self._set_auto_phase("Idle")
                        # Restart AWB before sleeping
                        if not self.is_awb_running():
                            self.start_awb()
                        self._sleep_interval()
                        continue

                    self.log_append(
                        f"[AUTO] CLEAN+REJECTED grew from {baseline} -> {current}. "
                        f"Checking batch readiness (min {MIN_CLEAN_BATCHES_FOR_AUTO} batches)..."
                    )

                    # Step 5: Batch build
                    # Log queue depth so the operator can see how full the queue is.
                    _clean_n   = clean_pdf_count()
                    _est_batch = _estimate_batch_count()
                    self.log_append(
                        f"[AUTO] CLEAN: {_clean_n} file(s)  |  "
                        f"Est. batches: {_est_batch}  |  "
                        f"Min required: {MIN_CLEAN_BATCHES_FOR_AUTO}"
                    )
                    # Force-batch if the oldest CLEAN file has been waiting too long,
                    # even if the estimated count is below the minimum threshold.
                    _oldest_age = _oldest_clean_file_age_seconds()
                    _force_batch = (
                        _oldest_age is not None
                        and _oldest_age >= AUTO_FORCE_BATCH_AGE_SECONDS
                    )
                    if _force_batch:
                        self.log_append(
                            f"[AUTO] Force-batch: oldest CLEAN file has been waiting "
                            f"{int(_oldest_age) // 60}m — overriding min-batch threshold."
                        )
                    _min_b = 1 if _force_batch else MIN_CLEAN_BATCHES_FOR_AUTO
                    self._set_auto_phase("Batching")
                    did_batch = self._run_batch_once(tag="[AUTO]", min_batches=_min_b)

                    # Step 6: TIFF conversion (only if batch was built)
                    if did_batch:
                        self._set_auto_phase("TIFF conversion")
                        self.log_append("[AUTO] Running TIFF conversion...")
                        try:
                            tiff_cmd = [sys.executable, "-u", "-m", "V3.services.tiff_converter"]
                            self.run_script_blocking_live(tiff_cmd)
                            self.log_append("[AUTO] TIFF conversion complete.")
                        except Exception as e:
                            self.log_append(f"[AUTO] TIFF conversion failed: {e}")

                    # Step 7: Restart AWB for next cycle
                    self._set_auto_phase("Restarting AWB")
                    if not self.is_awb_running():
                        self.start_awb()

                    self._set_auto_phase("Idle")
                    self.log_append("[AUTO] Cycle complete. Idle.")

                except Exception as e:
                    self.log_append(f"[AUTO ERROR] {e}")
                    self._set_auto_phase("Idle")
                    # Make sure AWB is running for recovery
                    if not self.is_awb_running():
                        try:
                            self.start_awb()
                        except Exception:
                            pass

                self._sleep_interval()

            self.log_append("\n=== AUTO MODE STOPPED ===")
            self.set_status("Ready.")
            self._set_auto_phase("Idle")

        self.auto_thread = threading.Thread(target=loop, daemon=True)
        self.auto_thread.start()

    def _sleep_interval(self):
        for _ in range(AUTO_INTERVAL_SEC):
            if self.auto_stop_event.is_set():
                break
            time.sleep(1)

    def stop_auto_mode(self):
        if not self.auto_running:
            return
        self.auto_running = False
        self.auto_stop_event.set()
        self._stop_edm_duplicate_checker()
        self.btn_auto.config(text="AUTO MODE")
        self._set_auto_phase("Idle")
        self._refresh_live_status()
        self.log_append("\nStopping AUTO MODE...")
        self.set_status("Stopping...")
        self._show_toast("AUTO MODE stopped", "warn")

    # ─────────────────────────────────────────────────────────────────────────
    # CLOSE
    # ─────────────────────────────────────────────────────────────────────────
    def on_close(self):
        self._is_closing = True
        if self.full_cycle_running:
            self.full_cycle_stop_event.set()
        if self.auto_running:
            self.stop_auto_mode()
            time.sleep(0.3)
        if self.is_awb_running():
            self.stop_awb()
            time.sleep(0.3)
        elif self.is_edm_duplicate_running():
            # Ensure EDM checker is not left running when AWB is already stopped.
            self._stop_edm_duplicate_checker()
            time.sleep(0.2)
        self.destroy()
