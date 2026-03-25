# V3/services/hotfolder.py
# Watchdog-based inbox monitor and main loop.
#
# Entry point for the AWB hotfolder pipeline:
#   - Sets up a watchdog observer on INBOX_DIR
#   - Implements two-pass scheduling (fast lane / long lane / third pass)
#   - Calls process_pdf from V3.stages.pipeline
#   - Manages AWB Excel reload, heartbeat, safety rescan
#
# FLOW (two-pass scheduling):
#   Fast lane:   Stages 0-3 only.  Defer if no match.
#   Long lane:   Full pipeline on deferred docs when fast queue is empty.
#   Third pass:  Resume timeout-deferred files (no timeout, cached state).
#
# All paths come from V3.config — no hardcoded values.

from __future__ import annotations

import json
import os
import threading
import time
from datetime import datetime
from pathlib import Path
from queue import Queue, Empty

from V3 import config
from V3.stages.pipeline import process_pdf
from V3.services.edm_checker import is_edm_enabled
from V3.core.awb_extractor import extract_awb_from_filename_strict
from V3.core.file_ops import (
    log,
    require_tesseract,
    load_awb_set_from_excel,
    build_buckets,
    flush_awb_logs_buffer,
    safe_move,
)
from V3.audit.logger import audit_event
from V3.audit.tracker import rebuild_dashboard_now

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# ── Config aliases ────────────────────────────────────────────────────────────
INBOX_DIR                  = config.INBOX_DIR
AWB_EXCEL_PATH             = config.AWB_EXCEL_PATH
AWB_LOGS_PATH              = config.AWB_LOGS_PATH
POLL_SECONDS               = config.POLL_SECONDS
HEARTBEAT_SECONDS          = config.HEARTBEAT_SECONDS
EXCEL_REFRESH_SECONDS      = config.EXCEL_REFRESH_SECONDS
ENABLE_INBOX_TWO_PASS      = config.ENABLE_INBOX_TWO_PASS
LONG_PASS_TIMEOUT_SECONDS  = config.LONG_PASS_TIMEOUT_SECONDS
THIRD_PASS_TIMEOUT_SECONDS = config.THIRD_PASS_TIMEOUT_SECONDS
GLOBAL_DOC_TIMEOUT_SECONDS = config.GLOBAL_DOC_TIMEOUT_SECONDS
LARGE_FILE_THRESHOLD_BYTES = config.LARGE_FILE_THRESHOLD_BYTES
EDM_EXISTS_CACHE_PATH      = config.EDM_AWB_EXISTS_CACHE


# ── File-size helper ──────────────────────────────────────────────────────────

def _fsize(path: str) -> int:
    """Return file size in bytes; 0 if inaccessible."""
    try:
        return os.path.getsize(path)
    except Exception:
        return 0


# ── EDM exists cache reset ────────────────────────────────────────────────────

def _reset_edm_exists_cache() -> None:
    """Clear the shared EDM existence cache file at the start of each session."""
    try:
        EDM_EXISTS_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        tmp = EDM_EXISTS_CACHE_PATH.with_name(EDM_EXISTS_CACHE_PATH.name + ".tmp")
        tmp.write_text(json.dumps({}, indent=2, sort_keys=True), encoding="utf-8")
        tmp.replace(EDM_EXISTS_CACHE_PATH)
        log("[EDM-AWB-FALLBACK] Reset shared EDM existence cache for this hotfolder session")
    except Exception as e:
        log(f"[EDM-AWB-FALLBACK] Warning: could not reset cache file: {e}")


# ── Token helpers ─────────────────────────────────────────────────────────────

def _normalize_token(raw) -> str | None:
    if not raw:
        return None
    token = str(raw).strip().strip('"').strip("'")
    if token.lower().startswith("bearer "):
        token = token[7:].strip()
    return token or None


def _read_token_file() -> str | None:
    if not config.TOKEN_FILE.exists():
        return None
    try:
        raw = config.TOKEN_FILE.read_text(encoding="utf-8-sig")
    except Exception:
        return None
    return _normalize_token(raw)


def _get_edm_token() -> str | None:
    file_token = _read_token_file()
    if file_token:
        return file_token
    env_token = _normalize_token(config.EDM_TOKEN)
    if env_token and env_token != "paste_your_token_here":
        return env_token
    return None


# ── Watchdog handler ──────────────────────────────────────────────────────────

class InboxPDFHandler(FileSystemEventHandler):
    """Enqueue PDF files that appear in INBOX_DIR."""

    def __init__(self, q: Queue):
        self.q = q
        self._last_seen: dict[str, float] = {}
        self._lock = threading.Lock()

    def _enqueue(self, path: str) -> None:
        p = str(path)
        if not p.lower().endswith(".pdf"):
            return
        with self._lock:
            now = time.time()
            # De-bounce: skip if we saw this path less than 0.8 s ago
            if now - self._last_seen.get(p, 0) < 0.8:
                return
            self._last_seen[p] = now
        self.q.put(p)

    def on_created(self, event):
        if not event.is_directory:
            self._enqueue(event.src_path)

    def on_moved(self, event):
        if not event.is_directory:
            self._enqueue(event.dest_path)

    def on_modified(self, event):
        if not event.is_directory:
            self._enqueue(event.src_path)


# ── AWB reload trigger ────────────────────────────────────────────────────────

def _check_reload_trigger() -> bool:
    """Return ``True`` (and delete the trigger file) if the UI dropped a reload marker."""
    trigger = config.AWB_RELOAD_TRIGGER
    if trigger.exists():
        try:
            trigger.unlink()
        except Exception:
            pass
        return True
    return False


# ═════════════════════════════════════════════════════════════════════════════
# MAIN LOOP
# ═════════════════════════════════════════════════════════════════════════════

def main() -> None:
    config.ensure_dirs()
    require_tesseract()
    _reset_edm_exists_cache()
    rebuild_dashboard_now()  # full dashboard rebuild once at startup

    edm_on = is_edm_enabled()
    startup_token = _get_edm_token()
    if not edm_on:
        log("[EDM] EDM fallback is OFF (UI/config toggle). API calls are bypassed.")
    elif not startup_token:
        log("[WARNING] EDM is ON but no EDM token found; fallback stage will be skipped.")
    else:
        log("[EDM] Token present at startup (expiry checked on first fallback call).")

    awb_set: set[str] = set()
    by_prefix: dict[str, list[str]] = {}
    by_suffix: dict[str, list[str]] = {}
    last_excel_mtime: float = 0
    last_excel_load:  float = 0
    last_heartbeat:   float = 0
    last_rescan:      float = 0

    # Two-pass state
    deferred_long_pass:     list[str]       = []   # paths deferred by fast lane
    timeout_deferred_state: dict[str, dict] = {}   # path -> captured state dict
    # Accumulated PROCESSING seconds per file (queue wait time does NOT count).
    # This is the right metric for the global 2.5-min cap — a file waiting in
    # the deferred queue while other docs run should not have that wait charged
    # against its own budget.
    _file_proc_seconds:     dict[str, float] = {}  # path -> total seconds in process_pdf()

    file_queue: Queue[str] = Queue()
    handler  = InboxPDFHandler(file_queue)
    observer = Observer()
    observer.schedule(handler, str(INBOX_DIR), recursive=False)
    observer.start()

    log("=== AWB Hot Folder Pipeline V3 started ===")
    log(f"INBOX:  {INBOX_DIR}")
    log(f"EXCEL:  {AWB_EXCEL_PATH}")
    log(f"LOGS:   {AWB_LOGS_PATH}")
    log(
        "Scheduling: two-pass (fast lane = Stages 0-3 only, defer after Stage 3 fail; "
        "long-pass = full pipeline on deferred docs when fast queue empty)"
        if ENABLE_INBOX_TWO_PASS
        else "Scheduling: single-pass full pipeline"
    )
    if ENABLE_INBOX_TWO_PASS:
        log(f"Long-pass timeout budget per file: {LONG_PASS_TIMEOUT_SECONDS:.0f}s")
    log("Mode: watchdog event-driven with periodic safety rescan")

    # Seed the queue with any PDFs already sitting in INBOX
    try:
        for fn in INBOX_DIR.iterdir():
            if fn.suffix.lower() == ".pdf":
                handler._enqueue(str(fn))
    except Exception as e:
        log(f"Startup scan warning: {e}")

    try:
        while True:
            loop_sleep = POLL_SECONDS
            try:
                now = time.time()

                # ── AWB reload trigger from UI ───────────────────────────
                if _check_reload_trigger():
                    log("[RELOAD] AWB reload trigger detected — forcing Excel refresh")
                    last_excel_mtime = 0  # force reload on next check

                # ── Refresh Excel ────────────────────────────────────────
                if now - last_excel_load >= EXCEL_REFRESH_SECONDS:
                    try:
                        mtime = AWB_EXCEL_PATH.stat().st_mtime
                    except FileNotFoundError:
                        mtime = -1   # sentinel: distinguishes missing from mtime=0
                        if last_excel_mtime != -1:
                            log(f"[WARNING] AWB Excel not found: {AWB_EXCEL_PATH} — running with 0 AWBs")
                    except Exception:
                        mtime = 0
                    if mtime != last_excel_mtime and mtime >= 0:
                        awb_set = load_awb_set_from_excel(AWB_EXCEL_PATH)
                        by_prefix, by_suffix = build_buckets(awb_set)
                        last_excel_mtime = mtime
                        log(f"Loaded AWBs: {len(awb_set)} (Excel refreshed)")
                    elif mtime == -1:
                        last_excel_mtime = mtime
                    last_excel_load = now

                # ── Heartbeat ────────────────────────────────────────────
                if now - last_heartbeat >= HEARTBEAT_SECONDS:
                    try:
                        fc = len([
                            x for x in INBOX_DIR.iterdir()
                            if x.suffix.lower() == ".pdf"
                        ])
                    except Exception:
                        fc = -1
                    log(
                        f"Watching INBOX | PDF Files: {fc} | AWBs loaded: {len(awb_set)} | "
                        f"deferred-long-pass: {len(deferred_long_pass)} | "
                        f"timeout-deferred: {len(timeout_deferred_state)}"
                    )
                    last_heartbeat = now

                # ── Safety rescan (every 30s — watchdog handles real-time) ─
                if now - last_rescan >= 30:
                    try:
                        # Build set of files already in any queue — never re-enqueue
                        # them (would cause fast→long→timeout→fast loops).
                        _already_queued = (
                            set(deferred_long_pass) | set(timeout_deferred_state.keys())
                        )
                        for fn in INBOX_DIR.iterdir():
                            if fn.suffix.lower() == ".pdf":
                                if str(fn) not in _already_queued:
                                    handler._enqueue(str(fn))
                    except Exception as e:
                        log(f"Rescan warning: {e}")
                    last_rescan = now

                processed_any = False

                # ── Drain fast-lane queue ────────────────────────────────
                # Collect all pending items then sort: files whose name
                # contains a 12-digit AWB go first (Stage 0 matches them in
                # <1ms — no OCR needed).  Everything else follows in FIFO order.
                _pending: list[str] = []
                while True:
                    try:
                        _pending.append(file_queue.get_nowait())
                    except Empty:
                        break
                _pending.sort(
                    key=lambda p: 0 if extract_awb_from_filename_strict(
                        os.path.basename(p)
                    ) is not None else 1
                )
                for path in _pending:
                    if not os.path.exists(path) or not path.lower().endswith(".pdf"):
                        _file_proc_seconds.pop(path, None)
                        continue
                    # Skip files already sitting in a deferred queue — fast lane
                    # must not process a doc that is already scheduled for long/third pass.
                    if path in timeout_deferred_state or path in deferred_long_pass:
                        continue
                    # Global 2.5-min hard cap on accumulated PROCESSING time.
                    # Queue wait time is NOT charged — only actual process_pdf() wall time.
                    _proc_so_far = _file_proc_seconds.get(path, 0.0)
                    if _proc_so_far >= GLOBAL_DOC_TIMEOUT_SECONDS:
                        log(
                            f"[GLOBAL-TIMEOUT] {os.path.basename(path)} exceeded "
                            f"{GLOBAL_DOC_TIMEOUT_SECONDS:.0f}s processing ({_proc_so_far:.0f}s) — NEEDS_REVIEW"
                        )
                        try:
                            safe_move(path, config.NEEDS_REVIEW_DIR)
                        except Exception as _e:
                            log(f"[GLOBAL-TIMEOUT] Could not move {os.path.basename(path)}: {_e}")
                        _file_proc_seconds.pop(path, None)
                        processed_any = True
                        continue
                    if ENABLE_INBOX_TWO_PASS:
                        _t0 = time.perf_counter()
                        result = process_pdf(
                            str(path), awb_set, by_prefix, by_suffix,
                            allow_long_pass=False,
                        )
                        _file_proc_seconds[path] = _proc_so_far + (time.perf_counter() - _t0)
                        if result in ("DEFERRED", "DEFERRED_URGENT"):
                            # Don't re-add files already queued for third-pass
                            if path not in timeout_deferred_state and path not in deferred_long_pass:
                                deferred_long_pass.append(path)
                        elif not os.path.exists(path):
                            _file_proc_seconds.pop(path, None)
                    else:
                        _t0 = time.perf_counter()
                        process_pdf(
                            str(path), awb_set, by_prefix, by_suffix,
                            allow_long_pass=True,
                        )
                        _file_proc_seconds[path] = _proc_so_far + (time.perf_counter() - _t0)
                        if not os.path.exists(path):
                            _file_proc_seconds.pop(path, None)
                    processed_any = True

                # ── Long-pass: process deferred when fast queue empty ────
                _DEFERRED_BATCH = 5  # process up to N deferred files per cycle
                if (
                    ENABLE_INBOX_TWO_PASS
                    and file_queue.empty()
                    and deferred_long_pass
                ):
                    # Sort: small files first; files over threshold go to end.
                    # This ensures large (slow) docs don't block smaller ones.
                    deferred_long_pass.sort(key=lambda p: (
                        1 if _fsize(p) > LARGE_FILE_THRESHOLD_BYTES else 0,
                        _fsize(p),
                    ))
                    _batch_count = 0
                    while deferred_long_pass and _batch_count < _DEFERRED_BATCH:
                        path = deferred_long_pass.pop(0)
                        if not os.path.exists(path):
                            _file_proc_seconds.pop(path, None)
                            continue
                        # Global cap check on accumulated processing time only
                        _proc_so_far = _file_proc_seconds.get(path, 0.0)
                        _remaining = GLOBAL_DOC_TIMEOUT_SECONDS - _proc_so_far
                        if _remaining <= 0:
                            log(
                                f"[GLOBAL-TIMEOUT] {os.path.basename(path)} exceeded "
                                f"{GLOBAL_DOC_TIMEOUT_SECONDS:.0f}s processing ({_proc_so_far:.0f}s) — NEEDS_REVIEW"
                            )
                            try:
                                safe_move(path, config.NEEDS_REVIEW_DIR)
                            except Exception as _e:
                                log(f"[GLOBAL-TIMEOUT] Could not move {os.path.basename(path)}: {_e}")
                            _file_proc_seconds.pop(path, None)
                            processed_any = True
                            _batch_count += 1
                            if not file_queue.empty():
                                break
                            continue
                        # Dynamic budget: LONG_PASS_TIMEOUT_SECONDS is the combined
                        # fast-lane + long-pass budget. Fast-lane processing time is
                        # charged against it so slow-OCR files don't get a free extra
                        # full slot. Never drop below 10s so we always attempt something.
                        _lp_remaining = max(LONG_PASS_TIMEOUT_SECONDS - _proc_so_far, 10.0)
                        _budget = min(_lp_remaining, _remaining)
                        log(
                            f"[LONG-PASS] Processing deferred: {os.path.basename(path)} "
                            f"(proc={_proc_so_far:.0f}s, budget={_budget:.0f}s)"
                        )
                        state_out: dict = {}
                        _t0 = time.perf_counter()
                        result = process_pdf(
                            str(path), awb_set, by_prefix, by_suffix,
                            allow_long_pass=True,
                            timeout_seconds=_budget,
                            _state_out=state_out,
                        )
                        _proc_so_far += time.perf_counter() - _t0
                        _file_proc_seconds[path] = _proc_so_far
                        if result == "TIMEOUT_DEFERRED":
                            state_out["_enqueued_ts"] = time.time()
                            state_out["_tp_attempts"] = 0
                            state_out["_proc_seconds"] = _proc_so_far  # carry forward
                            timeout_deferred_state[path] = state_out
                            log(
                                f"[TIMEOUT-DEFERRED] {os.path.basename(path)} "
                                f"queued for third-pass "
                                f"(total queued: {len(timeout_deferred_state)})"
                            )
                        elif not os.path.exists(path):
                            _file_proc_seconds.pop(path, None)
                        processed_any = True
                        _batch_count += 1
                        # Break early if new fast files arrived
                        if not file_queue.empty():
                            break

                # ── Third-pass: resume timeout-deferred files ────────────
                _THIRD_BATCH = 3  # process up to N third-pass files per cycle
                if (
                    ENABLE_INBOX_TWO_PASS
                    and file_queue.empty()
                    and not deferred_long_pass
                    and timeout_deferred_state
                ):
                    _tp_count = 0
                    # Evict entries older than 24 hours (stale / file disappeared)
                    _stale = [p for p, v in timeout_deferred_state.items()
                              if time.time() - v.get("_enqueued_ts", 0) > 86400]
                    for _sp in _stale:
                        log(f"[DEFERRED-EVICT] Evicting stale entry (>24h): {os.path.basename(_sp)}")
                        timeout_deferred_state.pop(_sp)
                        _file_proc_seconds.pop(_sp, None)
                    while timeout_deferred_state and _tp_count < _THIRD_BATCH:
                        path, saved_state = next(iter(timeout_deferred_state.items()))
                        del timeout_deferred_state[path]
                        if not os.path.exists(path):
                            _file_proc_seconds.pop(path, None)
                            continue
                        tp_attempts = saved_state.get("_tp_attempts", 0)
                        # Accumulated processing seconds — prefer value carried from long-pass
                        _proc_so_far = saved_state.get(
                            "_proc_seconds", _file_proc_seconds.get(path, 0.0)
                        )
                        _remaining = GLOBAL_DOC_TIMEOUT_SECONDS - _proc_so_far
                        if _remaining <= 0 or tp_attempts >= 1:
                            _reason = (
                                f"exceeded {GLOBAL_DOC_TIMEOUT_SECONDS:.0f}s processing ({_proc_so_far:.0f}s)"
                                if _remaining <= 0
                                else f"exhausted third-pass retries ({tp_attempts})"
                            )
                            log(
                                f"[THIRD-PASS-LIMIT] {os.path.basename(path)} {_reason} — routing to NEEDS_REVIEW"
                            )
                            try:
                                safe_move(path, config.NEEDS_REVIEW_DIR)
                            except Exception as _e:
                                log(f"[THIRD-PASS-LIMIT] Could not move {os.path.basename(path)}: {_e}")
                            _file_proc_seconds.pop(path, None)
                            processed_any = True
                            _tp_count += 1
                            continue
                        # Dynamic budget: cap to remaining global allowance
                        _budget = min(THIRD_PASS_TIMEOUT_SECONDS, _remaining)
                        log(
                            f"[THIRD-PASS] Resuming: {os.path.basename(path)} "
                            f"(attempt {tp_attempts + 1}, proc={_proc_so_far:.0f}s, "
                            f"budget={_budget:.0f}s, remaining in queue: {len(timeout_deferred_state)})"
                        )
                        saved_state["_tp_attempts"] = tp_attempts + 1
                        _t0 = time.perf_counter()
                        process_pdf(
                            str(path), awb_set, by_prefix, by_suffix,
                            allow_long_pass=True,
                            timeout_seconds=_budget,
                            resume_state=saved_state,
                        )
                        _proc_so_far += time.perf_counter() - _t0
                        _file_proc_seconds[path] = _proc_so_far
                        # If file still in INBOX after third-pass, re-queue with updated
                        # counter so next cycle enforces the retry limit.
                        if os.path.exists(path):
                            saved_state["_enqueued_ts"] = time.time()
                            saved_state["_proc_seconds"] = _proc_so_far  # carry forward
                            timeout_deferred_state[path] = saved_state
                            log(f"[THIRD-PASS] {os.path.basename(path)} still present after third-pass — re-queued (will force NEEDS_REVIEW next cycle)")
                        else:
                            _file_proc_seconds.pop(path, None)
                        processed_any = True
                        _tp_count += 1
                        # Break early if new fast files arrived
                        if not file_queue.empty():
                            break

                if processed_any:
                    loop_sleep = 0.2

            except Exception as e:
                log(f"LOOP ERROR: {e}")

            time.sleep(loop_sleep)

    except KeyboardInterrupt:
        log("Shutting down hotfolder watcher...")
    finally:
        observer.stop()
        observer.join()
        # Flush any buffered AWB log rows to Excel before exit
        try:
            flush_awb_logs_buffer()
        except Exception:
            pass


if __name__ == "__main__":
    main()
