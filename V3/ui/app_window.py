# V3/ui/app_window.py
# AWB Pipeline V3 — Main Application Window
#
# Fully self-contained Tkinter UI with:
#   - Employee login dialog
#   - Start/Stop AWB, EDM (disabled stub), Prepare Batch, Full Cycle, Auto Mode
#   - Folder shortcuts, live status strip, folder counts, stats panel
#   - Colour-coded log viewer with line cap
#   - Animated progress indicator
#   - Clear All, Upload Files, Retry NEEDS_REVIEW, Refresh DB

import json
import os
import shutil
import subprocess
import sys
import threading
import time
import tkinter as tk
from pathlib import Path
from tkinter import filedialog, messagebox, scrolledtext, simpledialog

from V3 import config
from V3.ui.theme import (
    APP_BG,
    BTN_BG,
    BTN_HOVER,
    CRIT,
    FONT_BTN,
    FONT_HEADER,
    FONT_LABEL,
    FONT_MONO,
    FONT_SMALL,
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
    TEXT_FG,
    THRESHOLDS,
    WARN,
)

# ── Paths / constants ────────────────────────────────────────────────────────
_ROOT        = Path(__file__).resolve().parent.parent.parent   # AWB_PIPELINE/
STATE_FILE   = config.BASE_DIR / "_run_state.json"
SESSION_FILE = config.DATA_DIR / "session.json"

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
# APP
# ═════════════════════════════════════════════════════════════════════════════

class App(tk.Tk):
    """AWB Pipeline V3 — Control Centre."""

    def __init__(self):
        super().__init__()
        self.title("AWB Pipeline V3 \u2014 Control Centre")
        self.geometry("1440x900")
        self.minsize(1100, 700)
        self.configure(bg=APP_BG)
        config.ensure_dirs()
        self._tk_patchlevel = self._read_tk_patchlevel()
        self._legacy_tk_on_mac = (sys.platform == "darwin" and self._tk_patchlevel < (8, 6))

        # ── Session state ────────────────────────────────────────────────────
        self.employee_id             = ""
        self.awb_proc                = None
        self.edm_proc                = None   # kept for state compat — always None in V3
        self.batch_running           = False
        self.full_cycle_running      = False
        self.full_cycle_stop_event   = threading.Event()
        self.auto_phase              = "Idle"
        self.auto_running            = False
        self.auto_stop_event         = threading.Event()
        self.auto_thread             = None
        self._indicator_step         = 0
        self._indicator_job          = None
        self._indicator_label        = None
        self._indicator_prefix       = ""
        self._stats_inflight         = False

        self._build_ui()
        self._setup_log_tags()

        self.log_append("  AWB Pipeline V3  |  INBOX -> [AWB] -> PROCESSED -> CLEAN/REJECTED -> [Batch] -> OUT")
        self.log_append(f"  Base: {config.BASE_DIR}")
        self.log_append(f"  Protected: {config.AWB_EXCEL_PATH.name}  |  {config.AWB_LOGS_PATH.name}")
        self.log_append("  EDM duplicate checker is DISABLED in V3 (no API calls).")
        self.log_append("  Ready.")

        self._refresh_live_status()
        self._start_count_refresh()
        self.protocol("WM_DELETE_WINDOW", self.on_close)

        # Employee login after UI is drawn.
        # macOS + Tk 8.5 has known dialog rendering issues, so use fallback.
        if self._legacy_tk_on_mac:
            self._bootstrap_employee_id()
            tk_ver = ".".join(str(x) for x in self._tk_patchlevel)
            self.log_append(f"[UI WARN] Detected Tk {tk_ver} on macOS. Login prompt disabled (fallback employee ID in use).")
        else:
            self.after(100, self._prompt_employee_number)

    # ─────────────────────────────────────────────────────────────────────────
    # UI CONSTRUCTION
    # ─────────────────────────────────────────────────────────────────────────
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

    def _bootstrap_employee_id(self):
        """Set employee ID from session (or fallback) without opening a dialog."""
        session = _load_session()
        val = (session.get("employee_id", "") or "").strip() or "UNKNOWN"
        self.employee_id = val
        os.environ["PIPELINE_EMPLOYEE_ID"] = val
        self.lbl_employee.config(text=f"Employee: {val}")
        _save_session({**session, "employee_id": val})

    def _build_ui(self):
        # ── Header bar ───────────────────────────────────────────────────────
        hdr = tk.Frame(self, bg=HEADER_BG, height=48)
        hdr.pack(fill="x")
        hdr.pack_propagate(False)

        tk.Label(
            hdr, text="AWB PIPELINE V3", font=FONT_HEADER,
            bg=HEADER_BG, fg=HEADER_FG,
        ).pack(side="left", padx=14, pady=10)

        self.lbl_employee = tk.Label(
            hdr, text="Employee: \u2014", font=FONT_LABEL,
            bg=HEADER_BG, fg="#aad4ff",
        )
        self.lbl_employee.pack(side="right", padx=14)

        self.lbl_clock = tk.Label(
            hdr, text="", font=FONT_LABEL,
            bg=HEADER_BG, fg="#aad4ff",
        )
        self.lbl_clock.pack(side="right", padx=6)
        self._tick_clock()

        # ── Action buttons (grouped) ─────────────────────────────────────────
        btn_row = tk.Frame(self, pady=4, bg=APP_BG)
        btn_row.pack(fill="x", padx=10)

        def _btn(parent, text, cmd, width=15, bg_color=BTN_BG, **kw):
            b = tk.Button(
                parent, text=text, width=width, command=cmd,
                font=FONT_BTN, relief="raised", bd=2,
                bg=bg_color,
                fg=TEXT_FG,
                activebackground=BTN_HOVER,
                activeforeground=TEXT_FG,
                highlightbackground=bg_color,
                **kw,
            )
            return b

        # --- Pipeline group ---
        grp_pipeline = tk.LabelFrame(
            btn_row, text="Pipeline", font=FONT_SMALL, padx=6, pady=4,
            fg=FRAME_LABEL_FG, bg=PANEL_BG,
        )
        grp_pipeline.pack(side="left", padx=(0, 8), fill="y")

        self.btn_get_awb    = _btn(grp_pipeline, "▶  Start AWB",       self.on_toggle_get_awb,      width=14)
        self.btn_full_cycle = _btn(grp_pipeline, "⟳  Full Cycle Once", self.on_run_full_cycle_once, width=16, bg_color="#eef7ff")
        self.btn_auto       = _btn(grp_pipeline, "⚡  AUTO MODE",       self.on_toggle_auto_mode,    width=14, bg_color="#eefce8")

        self.btn_get_awb.grid(row=0, column=0, padx=3, pady=2)
        self.btn_full_cycle.grid(row=0, column=1, padx=3, pady=2)
        self.btn_auto.grid(row=0, column=2, padx=3, pady=2)

        # --- Actions group ---
        grp_actions = tk.LabelFrame(
            btn_row, text="Actions", font=FONT_SMALL, padx=6, pady=4,
            fg=FRAME_LABEL_FG, bg=PANEL_BG,
        )
        grp_actions.pack(side="left", padx=(0, 8), fill="y")

        self.btn_batch        = _btn(grp_actions, "⚙  Prepare Batch", self.on_prepare_batch,      width=16)
        self.btn_retry_review = _btn(grp_actions, "↩  Retry NEEDS_REVIEW", self.on_retry_needs_review, width=20, bg_color="#fff8e6")
        self.btn_upload       = _btn(grp_actions, "⬆  Upload Files",  self.on_upload_files,       width=14, bg_color="#deeeff")
        self.btn_edm          = _btn(grp_actions, "EDM: DISABLED",    self.on_toggle_edm_checker, width=14, bg_color="#e8e8e8")

        self.btn_batch.grid(row=0, column=0, padx=3, pady=2)
        self.btn_retry_review.grid(row=0, column=1, padx=3, pady=2)
        self.btn_upload.grid(row=0, column=2, padx=3, pady=2)
        self.btn_edm.grid(row=0, column=3, padx=3, pady=2)

        # --- Maintenance group ---
        grp_maint = tk.LabelFrame(
            btn_row, text="Maintenance", font=FONT_SMALL, padx=6, pady=4,
            fg=FRAME_LABEL_FG, bg=PANEL_BG,
        )
        grp_maint.pack(side="left", fill="y")

        self.btn_clear_all = _btn(grp_maint, "🗑  Clear All", self.on_clear_all, width=12, bg_color="#fff0f0")
        self.btn_clear_log = _btn(grp_maint, "Clear Log",     self.clear_log,    width=10)

        self.btn_clear_all.grid(row=0, column=0, padx=3, pady=2)
        self.btn_clear_log.grid(row=0, column=1, padx=3, pady=2)

        # ── Open-folder row ──────────────────────────────────────────────────
        grp_folders = tk.LabelFrame(
            self, text="Folders", font=FONT_SMALL, padx=6, pady=2,
            fg=FRAME_LABEL_FG, bg=PANEL_BG,
        )
        grp_folders.pack(fill="x", padx=10, pady=(2, 0))

        folder_btns = [
            ("INBOX",         config.INBOX_DIR),
            ("CLEAN",         config.CLEAN_DIR),
            ("REJECTED",      config.REJECTED_DIR),
            ("NEEDS REVIEW",  config.NEEDS_REVIEW_DIR),
            ("OUT",           config.OUT_DIR),
            ("PENDING PRINT", config.PENDING_PRINT_DIR),
        ]
        for col, (label, path) in enumerate(folder_btns):
            tk.Button(
                grp_folders, text=label, width=16, font=FONT_SMALL,
                command=lambda p=path: self.open_folder(p),
                bg="#e8effb", fg=TEXT_FG, activebackground=BTN_HOVER,
                highlightbackground="#e8effb",
            ).grid(row=0, column=col, padx=3, pady=2)

        # ── Separator ────────────────────────────────────────────────────────
        tk.Frame(self, height=1, bg="#d4dbe8").pack(fill="x", padx=10, pady=(4, 0))

        # ── Default foreground (macOS dark mode needs light text) ────────────
        self._default_fg = TEXT_FG

        # ── Live status strip ────────────────────────────────────────────────
        live_frame = tk.Frame(self, bd=1, relief="groove", bg=STRIP_BG)
        live_frame.pack(fill="x", padx=10, pady=(4, 0))

        self.lbl_live_awb   = tk.Label(live_frame, text="AWB: OFF",       width=20, anchor="w", font=(*FONT_LABEL[:2], "bold"), fg=self._default_fg, bg=STRIP_BG)
        self.lbl_live_edm   = tk.Label(live_frame, text="EDM: DISABLED",  width=20, anchor="w", font=(*FONT_LABEL[:2], "bold"), fg="#888888", bg=STRIP_BG)
        self.lbl_live_batch = tk.Label(live_frame, text="BATCH: IDLE",    width=20, anchor="w", font=(*FONT_LABEL[:2], "bold"), fg=self._default_fg, bg=STRIP_BG)
        self.lbl_live_auto  = tk.Label(live_frame, text="AUTO: OFF",      width=28, anchor="w", font=(*FONT_LABEL[:2], "bold"), fg=self._default_fg, bg=STRIP_BG)

        for i, lbl in enumerate([self.lbl_live_awb, self.lbl_live_edm,
                                 self.lbl_live_batch, self.lbl_live_auto]):
            lbl.grid(row=0, column=i, padx=10, pady=3)

        # ── Folder counts bar ────────────────────────────────────────────────
        counts_frame = tk.Frame(self, bd=1, relief="sunken", bg=STRIP_BG)
        counts_frame.pack(fill="x", padx=10, pady=(2, 0))

        self.lbl_inbox     = tk.Label(counts_frame, text="INBOX: 0",        width=13, anchor="w", font=FONT_SMALL, fg=self._default_fg, bg=STRIP_BG)
        self.lbl_processed = tk.Label(counts_frame, text="PROCESSED: 0",    width=15, anchor="w", font=FONT_SMALL, fg=self._default_fg, bg=STRIP_BG)
        self.lbl_clean     = tk.Label(counts_frame, text="CLEAN: 0",        width=13, anchor="w", font=FONT_SMALL, fg=self._default_fg, bg=STRIP_BG)
        self.lbl_rejected  = tk.Label(counts_frame, text="REJECTED: 0",     width=14, anchor="w", font=FONT_SMALL, fg=self._default_fg, bg=STRIP_BG)
        self.lbl_review    = tk.Label(counts_frame, text="NEEDS_REVIEW: 0", width=18, anchor="w", font=FONT_SMALL, fg=self._default_fg, bg=STRIP_BG)
        self.lbl_out       = tk.Label(counts_frame, text="OUT batches: 0",  width=16, anchor="w", font=FONT_SMALL, fg=self._default_fg, bg=STRIP_BG)
        self.lbl_pending   = tk.Label(counts_frame, text="PENDING: 0",      width=12, anchor="w", font=FONT_SMALL, fg=self._default_fg, bg=STRIP_BG)

        for i, lbl in enumerate([self.lbl_inbox, self.lbl_processed, self.lbl_clean,
                                 self.lbl_rejected, self.lbl_review,
                                 self.lbl_out, self.lbl_pending]):
            lbl.grid(row=0, column=i, padx=6, pady=2)

        # ── Stats mini-panel ─────────────────────────────────────────────────
        stats_frame = tk.Frame(self, bd=1, relief="groove", pady=3, bg=STRIP_BG)
        stats_frame.pack(fill="x", padx=10, pady=(2, 0))

        tk.Label(stats_frame, text="TODAY:", font=(*FONT_SMALL[:2], "bold"),
                 width=6, fg=self._default_fg, bg=STRIP_BG).grid(row=0, column=0, padx=4)

        self._stat_labels = {}
        stat_defs = [
            ("hot_total",     "Processed: 0", self._default_fg),
            ("hot_complete",  "Complete: 0",   OK),
            ("hot_review",    "Review: 0",     REVIEW),
            ("hot_failed",    "Failed: 0",     CRIT),
            ("edm_clean",     "EDM Clean: 0",  OK),
            ("edm_rejected",  "EDM Rej: 0",    CRIT),
            ("batches_built", "Batches: 0",    INFO),
            ("tiffs",         "TIFFs: 0",      INFO),
        ]
        for col, (key, text, color) in enumerate(stat_defs, start=1):
            kw = {"font": FONT_SMALL, "padx": 6, "width": 12, "anchor": "w", "bg": STRIP_BG}
            if color:
                kw["fg"] = color
            lbl = tk.Label(stats_frame, text=text, **kw)
            lbl.grid(row=0, column=col)
            self._stat_labels[key] = lbl

        # ── Status bar ───────────────────────────────────────────────────────
        self.status_var = tk.StringVar(value="Ready.")
        tk.Label(
            self, textvariable=self.status_var, anchor="w", font=FONT_SMALL,
            fg=self._default_fg, bg=APP_BG,
        ).pack(fill="x", padx=10, pady=(2, 0))

        # ── Log viewer ───────────────────────────────────────────────────────
        self.log_widget = scrolledtext.ScrolledText(
            self, wrap=tk.WORD, height=28, font=FONT_MONO,
        )
        self.log_widget.pack(fill="both", expand=True, padx=10, pady=(4, 0))
        self.log_widget.configure(
            state="disabled",
            bg="#ffffff",
            fg=TEXT_FG,
            insertbackground=TEXT_FG,
        )

        # ── Bottom bar ───────────────────────────────────────────────────────
        bottom_bar = tk.Frame(self, bg=APP_BG)
        bottom_bar.pack(fill="x", padx=10, pady=(2, 6))

        tk.Button(
            bottom_bar, text="↻ Refresh DB", font=FONT_SMALL,
            command=self.on_refresh_db, relief="groove", padx=6, pady=1,
            bg="#e8effb", fg=TEXT_FG, activebackground=BTN_HOVER,
            highlightbackground="#e8effb",
        ).pack(side="right")

        tk.Label(
            bottom_bar, text="V3", font=FONT_SMALL, fg="#7e8798", bg=APP_BG,
        ).pack(side="left", padx=4)

    # ─────────────────────────────────────────────────────────────────────────
    # LOG TAG SETUP
    # ─────────────────────────────────────────────────────────────────────────
    def _setup_log_tags(self):
        for tag_name, (fg, bg), _ in LOG_TAGS:
            kw = {}
            if fg:
                kw["foreground"] = fg
            if bg:
                kw["background"] = bg
            self.log_widget.tag_configure(tag_name, **kw)

    # ─────────────────────────────────────────────────────────────────────────
    # CLOCK
    # ─────────────────────────────────────────────────────────────────────────
    def _tick_clock(self):
        self.lbl_clock.config(text=time.strftime("%Y-%m-%d  %H:%M:%S"))
        self.after(1000, self._tick_clock)

    # ─────────────────────────────────────────────────────────────────────────
    # EMPLOYEE LOGIN
    # ─────────────────────────────────────────────────────────────────────────
    def _prompt_employee_number(self):
        session = _load_session()
        prev = (session.get("employee_id", "") or "").strip()

        # Use native dialog on macOS to avoid custom Toplevel rendering glitches.
        val = simpledialog.askstring(
            "Employee Login",
            "Enter your Employee Number to begin:",
            parent=self,
            initialvalue=prev,
        )
        val = (val or "").strip()

        if not val:
            val = prev or "UNKNOWN"
            self.log_append("[LOGIN] Empty employee ID; using fallback value.")

        self.employee_id = val
        os.environ["PIPELINE_EMPLOYEE_ID"] = val
        self.lbl_employee.config(text=f"Employee: {val}")
        _save_session({**session, "employee_id": val})

    # ─────────────────────────────────────────────────────────────────────────
    # FOLDER COUNT REFRESH
    # ─────────────────────────────────────────────────────────────────────────
    def _start_count_refresh(self):
        self._refresh_counts()
        self._refresh_stats()
        self._refresh_live_status()
        self.after(3000, self._start_count_refresh)

    def _threshold_color(self, key: str, n) -> str:
        if n is None:
            return self._default_fg
        warn_at, crit_at = THRESHOLDS.get(key, (9999, 9999))
        if n >= crit_at:
            return CRIT
        if n >= warn_at:
            return WARN
        return self._default_fg

    def _refresh_counts(self):
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

        self.lbl_inbox.config(
            text=f"INBOX: {_fmt(inbox_n)}",
            fg=self._threshold_color("inbox", inbox_n),
        )
        self.lbl_processed.config(
            text=f"PROCESSED: {_fmt(processed_n)}",
            fg=self._default_fg,
        )
        self.lbl_clean.config(
            text=f"CLEAN: {_fmt(clean_n)}",
            fg=OK if clean_n else self._default_fg,
        )
        self.lbl_rejected.config(
            text=f"REJECTED: {_fmt(rejected_n)}",
            fg=self._threshold_color("rejected", rejected_n),
        )
        self.lbl_review.config(
            text=f"NEEDS_REVIEW: {_fmt(review_n)}",
            fg=self._threshold_color("review", review_n),
        )
        self.lbl_out.config(
            text=f"OUT batches: {_fmt(out_n)}",
            fg=self._default_fg,
        )
        self.lbl_pending.config(
            text=f"PENDING: {_fmt(pending_n)}",
            fg=self._threshold_color("pending", pending_n),
        )

    def _refresh_stats(self):
        """Update today's stats panel from V3.audit.tracker (non-blocking)."""
        def _pull():
            try:
                from V3.audit.tracker import read_dashboard_stats
                return read_dashboard_stats()
            except Exception:
                return None

        def _apply(stats):
            if not stats:
                return
            self._stat_labels["hot_total"].config(
                text=f"Processed: {stats['hot_total']}")
            self._stat_labels["hot_complete"].config(
                text=f"Complete: {stats['hot_complete']}")
            self._stat_labels["hot_review"].config(
                text=f"Review: {stats['hot_review']}",
                fg=CRIT if stats["hot_review"] > 0 else REVIEW)
            self._stat_labels["hot_failed"].config(
                text=f"Failed: {stats['hot_failed']}",
                fg=CRIT if stats["hot_failed"] > 0 else self._default_fg)
            self._stat_labels["edm_clean"].config(
                text=f"EDM Clean: {stats['edm_clean'] + stats['edm_partial']}")
            self._stat_labels["edm_rejected"].config(
                text=f"EDM Rej: {stats['edm_rejected']}",
                fg=CRIT if stats["edm_rejected"] > 0 else self._default_fg)
            self._stat_labels["batches_built"].config(
                text=f"Batches: {stats['batches_built']}")
            self._stat_labels["tiffs"].config(
                text=f"TIFFs: {stats['tiffs_converted']}")

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
        batch_on = self.batch_running

        self.lbl_live_awb.config(
            text=f"AWB: {'RUNNING' if awb_on else 'OFF'}",
            fg=OK if awb_on else CRIT,
        )
        # EDM is always disabled in V3
        self.lbl_live_edm.config(text="EDM: DISABLED", fg="#888888")

        self.lbl_live_batch.config(
            text=f"BATCH: {'RUNNING' if batch_on else 'IDLE'}",
            fg=INFO if batch_on else self._default_fg,
        )
        auto_text = f"AUTO: {'ON' if self.auto_running else 'OFF'}  |  {self.auto_phase}"
        self.lbl_live_auto.config(
            text=auto_text,
            fg=OK if self.auto_running else self._default_fg,
        )

        # Button state management — clear-all always available (guarded in handler)
        self.btn_clear_all.config(state="normal")

    def _set_auto_phase(self, phase: str):
        self.auto_phase = phase
        self.after(0, self._refresh_live_status)

    def _set_batch_running(self, running: bool):
        self.batch_running = running
        self.after(0, lambda: self.btn_batch.config(
            state="disabled" if running else "normal"))
        self.after(0, self._refresh_live_status)
        if running:
            self._start_indicator(self.lbl_live_batch, "BATCH")
        else:
            self._stop_indicator()
            self.lbl_live_batch.config(text="BATCH: IDLE", fg=self._default_fg)

    # ─────────────────────────────────────────────────────────────────────────
    # ANIMATED INDICATOR
    # ─────────────────────────────────────────────────────────────────────────
    _DOTS = ["", ".", "..", "..."]

    def _start_indicator(self, label, prefix):
        self._stop_indicator()
        self._indicator_label  = label
        self._indicator_prefix = prefix
        self._indicator_step   = 0
        self._animate_indicator()

    def _animate_indicator(self):
        dots = self._DOTS[self._indicator_step % len(self._DOTS)]
        try:
            self._indicator_label.config(text=f"{self._indicator_prefix}: RUNNING{dots}")
        except Exception:
            pass
        self._indicator_step += 1
        self._indicator_job = self.after(500, self._animate_indicator)

    def _stop_indicator(self):
        if self._indicator_job:
            self.after_cancel(self._indicator_job)
            self._indicator_job = None

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
        except Exception as e:
            self.log_append(f"[DB] Failed to signal refresh: {e}")

    # ─────────────────────────────────────────────────────────────────────────
    # UI HELPERS
    # ─────────────────────────────────────────────────────────────────────────
    def clear_log(self):
        self.log_widget.configure(state="normal")
        self.log_widget.delete("1.0", tk.END)
        self.log_widget.configure(state="disabled")

    def set_status(self, msg: str):
        self.status_var.set(msg)
        self.update_idletasks()

    def log_append(self, msg: str):
        def _do():
            self.log_widget.configure(state="normal")
            line_start = self.log_widget.index(tk.END)
            self.log_widget.insert(tk.END, msg + "\n")
            # Apply colour tags
            msg_upper = msg.upper()
            for tag_name, _colors, keywords in LOG_TAGS:
                if any(kw.upper() in msg_upper for kw in keywords):
                    row = int(line_start.split(".")[0])
                    self.log_widget.tag_add(tag_name, f"{row}.0", f"{row}.end")
                    break
            # Cap log length
            total_lines = int(self.log_widget.index("end-1c").split(".")[0])
            if total_lines > LOG_MAX_LINES:
                excess = total_lines - LOG_MAX_LINES
                self.log_widget.delete("1.0", f"{excess + 1}.0")
            self.log_widget.see(tk.END)
            self.log_widget.configure(state="disabled")
        self.after(0, _do)

    def run_in_thread(self, fn):
        def wrapper():
            try:
                fn()
            except Exception as e:
                self.after(0, lambda: messagebox.showerror("Error", str(e)))
                self.log_append(f"[ERROR] {e}")
                self.set_status("Ready.")
        threading.Thread(target=wrapper, daemon=True).start()

    def _make_env(self) -> dict:
        """Build subprocess environment with employee ID and UTF-8 flags."""
        env = os.environ.copy()
        env["PYTHONUTF8"]           = "1"
        env["PYTHONIOENCODING"]     = "utf-8"
        env["PIPELINE_EMPLOYEE_ID"] = self.employee_id
        return env

    def _popen_utf8(self, cmd_args: list):
        """Launch a subprocess with UTF-8 encoding and live stdout piping."""
        self.log_append(f"Running: {' '.join(str(a) for a in cmd_args[-2:])}")
        return subprocess.Popen(
            cmd_args,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, encoding="utf-8", errors="replace",
            bufsize=1, universal_newlines=True,
            env=self._make_env(),
            cwd=str(_ROOT),
        )

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

    def start_awb(self):
        if self.is_awb_running():
            return
        save_state({"last_run_id": now_run_id()})
        self.set_status("AWB Hotfolder running...")
        self.log_append("\n=== AWB Hotfolder started ===")
        cmd = [sys.executable, "-u", "-m", "V3.services.hotfolder"]
        self.awb_proc = self._popen_utf8(cmd)
        self.btn_get_awb.config(text="■  Stop AWB")
        self._refresh_live_status()

        def reader():
            try:
                for line in self.awb_proc.stdout:
                    self.log_append(line.rstrip("\n"))
            except Exception as e:
                self.log_append(f"[AWB ERROR] {e}")
            rc = self.awb_proc.wait()
            self.awb_proc = None
            self.after(0, lambda: self.btn_get_awb.config(text="▶  Start AWB"))
            self.after(0, self._refresh_live_status)
            self.set_status("AWB stopped." if rc == 0 else "AWB ended with errors.")

        threading.Thread(target=reader, daemon=True).start()

    def stop_awb(self):
        if not self.is_awb_running():
            self.awb_proc = None
            self.btn_get_awb.config(text="▶  Start AWB")
            self._refresh_live_status()
            return
        self.log_append("Stopping AWB Hotfolder...")
        try:
            self.awb_proc.terminate()
            time.sleep(1)
            if self.awb_proc.poll() is None:
                self.awb_proc.kill()
        except Exception:
            pass

    def on_toggle_get_awb(self):
        if self.is_awb_running():
            self.stop_awb()
        else:
            self.start_awb()

    # ─────────────────────────────────────────────────────────────────────────
    # EDM CHECKER (DISABLED STUB)
    # ─────────────────────────────────────────────────────────────────────────
    def on_toggle_edm_checker(self):
        """EDM is disabled in V3 — no API calls are made."""
        self.log_append("[EDM] EDM checker is disabled in V3 (no API calls).")
        self.set_status("EDM checker is disabled in V3.")

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
        self.run_in_thread(lambda: self._run_batch_once(tag="[BATCH]", min_batches=1))

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

        def _job():
            started_awb = False
            self._set_full_cycle_running(True)
            self.full_cycle_stop_event.clear()
            try:
                self.log_append("\n=== [CYCLE] Full Cycle Once started ===")
                self.set_status("Full cycle: preparing services...")

                # Start AWB if not running
                if not self.is_awb_running():
                    started_awb = True
                    self.start_awb()
                    time.sleep(0.5)

                # (EDM disabled in V3 — skip)

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

                # V3: No EDM step — move PROCESSED directly to CLEAN
                self.set_status("Full cycle: moving PROCESSED to CLEAN...")
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
    # MOVE PROCESSED -> CLEAN  (V3: no EDM, so files skip the check)
    # ─────────────────────────────────────────────────────────────────────────
    def _move_processed_to_clean(self, tag: str = "[AUTO]"):
        """Move all PDFs from PROCESSED directly to CLEAN (EDM is disabled in V3)."""
        config.CLEAN_DIR.mkdir(parents=True, exist_ok=True)
        moved = 0
        for src in sorted(config.PROCESSED_DIR.glob("*.pdf")):
            if not src.exists():
                continue
            dst = _next_available_path(config.CLEAN_DIR, src.name)
            try:
                shutil.move(str(src), str(dst))
                self.log_append(f"{tag} {src.name} -> CLEAN/{dst.name} (EDM bypassed)")
                moved += 1
            except Exception as e:
                self.log_append(f"{tag} ERROR moving {src.name}: {e}")
        if moved:
            self.log_append(f"{tag} Moved {moved} file(s) from PROCESSED to CLEAN (EDM disabled).")
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

        if self.is_awb_running() or self.batch_running:
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
            if self.is_awb_running():
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
        self.btn_auto.config(text="■  Stop AUTO")
        self._set_auto_phase("Starting")
        self.set_status("AUTO MODE running...")
        self.log_append(f"\n=== AUTO MODE STARTED (employee: {self.employee_id or chr(8212)}) ===")
        self.log_append(f"  V3 Flow: INBOX empty -> AWB -> PROCESSED -> CLEAN (EDM bypassed) -> batch (min {MIN_CLEAN_BATCHES_FOR_AUTO}) -> TIFF")
        self._refresh_live_status()

        # Start AWB if not running
        if not self.is_awb_running():
            self.start_awb()

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

                    # Step 2: Stop AWB so no new files arrive during batch
                    self._set_auto_phase("Stopping AWB")
                    if self.is_awb_running():
                        self.stop_awb()
                        time.sleep(0.5)

                    # Step 3: (EDM bypassed) — Move PROCESSED directly to CLEAN
                    self._set_auto_phase("Moving PROCESSED -> CLEAN")
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
                    self._set_auto_phase("Batching")
                    did_batch = self._run_batch_once(tag="[AUTO]", min_batches=MIN_CLEAN_BATCHES_FOR_AUTO)

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
        self.btn_auto.config(text="⚡  AUTO MODE")
        self._set_auto_phase("Idle")
        self._refresh_live_status()
        self.log_append("\nStopping AUTO MODE...")
        self.set_status("Stopping...")

    # ─────────────────────────────────────────────────────────────────────────
    # CLOSE
    # ─────────────────────────────────────────────────────────────────────────
    def on_close(self):
        if self.full_cycle_running:
            self.full_cycle_stop_event.set()
        if self.auto_running:
            self.stop_auto_mode()
            time.sleep(0.3)
        if self.is_awb_running():
            self.stop_awb()
            time.sleep(0.3)
        self._stop_indicator()
        self.destroy()
