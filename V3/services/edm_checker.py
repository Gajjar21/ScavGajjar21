# V3/services/edm_checker.py
# EDM AWB existence fallback service.
#
# This module is intentionally lightweight:
# - Provides a runtime ON/OFF toggle for EDM calls.
# - Provides edm_awb_exists_fallback(awb) used by pipeline Stage 6.
# - Uses an on-disk cache so repeated AWB checks avoid repeated API calls.

from __future__ import annotations

import json
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

try:
    import requests
except Exception:  # pragma: no cover - handled gracefully at runtime
    requests = None  # type: ignore[assignment]

from V3 import config
from V3.core.file_ops import log

_TRUE_STRINGS = {"1", "true", "yes", "on", "enabled"}
_FALSE_STRINGS = {"0", "false", "no", "off", "disabled"}

_cache_mem: dict[str, bool] | None = None
_warned_disabled = False
_warned_no_token = False
_warned_no_requests = False
_warned_auth = False


def _edm_log(message: str) -> None:
    """Write EDM messages to pipeline log and dedicated EDM log."""
    log(message)
    try:
        config.LOG_DIR.mkdir(parents=True, exist_ok=True)
        with open(config.EDM_LOG, "a", encoding="utf-8") as fh:
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            fh.write(f"[{ts}] {message}\n")
    except Exception:
        pass


def _parse_boolish(raw: Any) -> Optional[bool]:
    if isinstance(raw, bool):
        return raw
    if raw is None:
        return None
    text = str(raw).strip().lower()
    if text in _TRUE_STRINGS:
        return True
    if text in _FALSE_STRINGS:
        return False
    return None


def _normalize_token(raw: Any) -> str | None:
    if raw is None:
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


def set_edm_enabled(enabled: bool) -> None:
    """Persist runtime EDM ON/OFF toggle used by hotfolder/pipeline."""
    payload = {
        "enabled": bool(enabled),
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    config.DATA_DIR.mkdir(parents=True, exist_ok=True)
    tmp = Path(str(config.EDM_TOGGLE_FILE) + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(config.EDM_TOGGLE_FILE)


def is_edm_enabled() -> bool:
    """Return runtime EDM enabled state.

    Priority:
    1) Runtime toggle file (updated by UI button; supports live changes)
    2) PIPELINE_EDM_ENABLED env override (useful for one-off process runs)
    3) ENABLE_EDM_FALLBACK config default from .env
    """
    try:
        if config.EDM_TOGGLE_FILE.exists():
            raw = json.loads(config.EDM_TOGGLE_FILE.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                parsed = _parse_boolish(raw.get("enabled"))
                if parsed is not None:
                    return parsed
            else:
                parsed = _parse_boolish(raw)
                if parsed is not None:
                    return parsed
    except Exception:
        pass

    env_override = _parse_boolish(os.getenv("PIPELINE_EDM_ENABLED"))
    if env_override is not None:
        return env_override

    return bool(config.ENABLE_EDM_FALLBACK)


def _load_cache() -> dict[str, bool]:
    if not config.EDM_AWB_EXISTS_CACHE.exists():
        return {}
    try:
        raw = json.loads(config.EDM_AWB_EXISTS_CACHE.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(raw, dict):
        return {}

    out: dict[str, bool] = {}
    for k, v in raw.items():
        key = str(k).strip()
        if not (key.isdigit() and len(key) == config.AWB_LEN):
            continue
        if isinstance(v, dict):
            parsed = _parse_boolish(v.get("exists"))
        else:
            parsed = _parse_boolish(v)
        if parsed is not None:
            out[key] = parsed
    return out


def _cache_get(awb: str) -> Optional[bool]:
    global _cache_mem
    if _cache_mem is None:
        _cache_mem = _load_cache()
    return _cache_mem.get(awb)


def _cache_put(awb: str, exists: bool) -> None:
    global _cache_mem
    if _cache_mem is None:
        _cache_mem = _load_cache()
    _cache_mem[awb] = bool(exists)
    try:
        config.EDM_AWB_EXISTS_CACHE.parent.mkdir(parents=True, exist_ok=True)
        tmp = Path(str(config.EDM_AWB_EXISTS_CACHE) + ".tmp")
        tmp.write_text(json.dumps(_cache_mem, indent=2, sort_keys=True), encoding="utf-8")
        tmp.replace(config.EDM_AWB_EXISTS_CACHE)
    except Exception:
        pass


def _payload_contains_awb(payload: Any, awb: str) -> bool:
    if payload is None:
        return False
    if isinstance(payload, dict):
        for k, v in payload.items():
            if _payload_contains_awb(k, awb) or _payload_contains_awb(v, awb):
                return True
        return False
    if isinstance(payload, (list, tuple, set)):
        return any(_payload_contains_awb(x, awb) for x in payload)

    text = str(payload)
    if awb in text:
        return True
    digits = re.sub(r"\D", "", text)
    return awb in digits


def _payload_explicitly_empty(payload: Any) -> bool:
    if isinstance(payload, list):
        return len(payload) == 0

    if not isinstance(payload, dict):
        return False

    count_keys = {"count", "total", "totalcount", "resultcount", "documentscount", "hits"}
    list_keys = {"documents", "results", "items", "records", "groups", "matches"}

    for k, v in payload.items():
        key = str(k).strip().lower()
        if key in count_keys:
            try:
                if int(v) == 0:
                    return True
            except Exception:
                pass
        if key in list_keys and isinstance(v, list) and not v:
            return True

    if len(payload) == 1:
        only = next(iter(payload.values()))
        return _payload_explicitly_empty(only)

    return False


def _check_edm_api_for_awb(awb: str, token: str) -> Optional[bool]:
    if requests is None:
        return None

    # V1/V2-compatible EDM metadata request (known-good legacy shape).
    # Keep this as the primary call path so behavior matches prior pipeline.
    legacy_headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json, text/plain, */*",
        "Origin": config.EDM_PORTAL_URL,
        "Referer": config.EDM_PORTAL_URL + "/",
    }
    legacy_payload = {
        "documentClass": "SHIPMENT",
        "group": [
            {
                "operatingCompany": config.EDM_OPERATING_COMPANY,
                "trackingNumber": [awb],
            }
        ],
        "responseTypes": ["metadata"],
    }
    legacy_params = {
        "pageSize": 25,
        "continuationToken": "",
        "archiveSelection": "false",
    }
    try:
        response = requests.post(
            config.EDM_METADATA_URL,
            headers=legacy_headers,
            params=legacy_params,
            json=legacy_payload,
            timeout=15,
        )
        if response.status_code in (401, 403):
            return None
        if response.status_code == 404:
            return False
        if response.status_code == 200:
            try:
                payload = response.json()
            except Exception:
                payload = None

            doc_ids: list[str] = []
            if isinstance(payload, dict):
                for group in payload.get("groups", []):
                    if not isinstance(group, dict):
                        continue
                    for doc in group.get("documents", []):
                        if not isinstance(doc, dict):
                            continue
                        doc_id = doc.get("documentId") or doc.get("id")
                        if doc_id:
                            doc_ids.append(str(doc_id))
            if doc_ids:
                return True
            return False
    except Exception:
        pass

    auth_headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }

    # Secondary fallback probes for endpoint shape drift.
    payload_variants = [
        {
            "awbNumber": awb,
            "airWaybillNumber": awb,
            "operatingCompany": config.EDM_OPERATING_COMPANY,
        },
        {
            "query": {
                "awbNumber": awb,
                "operatingCompany": config.EDM_OPERATING_COMPANY,
            }
        },
    ]

    for body in payload_variants:
        try:
            response = requests.post(
                config.EDM_METADATA_URL,
                headers={**auth_headers, "Content-Type": "application/json"},
                json=body,
                timeout=(5, 20),
            )
        except Exception:
            continue

        if response.status_code in (401, 403):
            return None
        if response.status_code == 404:
            return False

        text = response.text or ""
        if awb in text:
            return True

        try:
            payload = response.json()
        except Exception:
            payload = None

        if _payload_contains_awb(payload, awb):
            return True
        if _payload_explicitly_empty(payload):
            return False

    # Fallback to query-string style in case endpoint accepts GET semantics.
    try:
        response = requests.get(
            config.EDM_METADATA_URL,
            headers=auth_headers,
            params={
                "awbNumber": awb,
                "airWaybillNumber": awb,
                "operatingCompany": config.EDM_OPERATING_COMPANY,
            },
            timeout=(5, 20),
        )
    except Exception:
        return None

    if response.status_code in (401, 403):
        return None
    if response.status_code == 404:
        return False

    text = response.text or ""
    if awb in text:
        return True

    try:
        payload = response.json()
    except Exception:
        return None

    if _payload_contains_awb(payload, awb):
        return True
    if _payload_explicitly_empty(payload):
        return False
    return None


def edm_awb_exists_fallback(awb: str) -> Optional[bool]:
    """Return True/False when EDM can confirm AWB existence, else None.

    None is used for bypassed/unchecked paths (toggle OFF, missing token, auth
    issues, endpoint shape mismatch, transient network errors).
    """
    global _warned_disabled, _warned_no_token, _warned_no_requests, _warned_auth

    awb = (awb or "").strip()
    if not (awb.isdigit() and len(awb) == config.AWB_LEN):
        return None

    if not is_edm_enabled():
        if not _warned_disabled:
            _edm_log("[EDM-AWB-FALLBACK] EDM is OFF. API calls are bypassed.")
            _warned_disabled = True
        return None

    cached = _cache_get(awb)
    if cached is not None:
        return cached

    if requests is None:
        if not _warned_no_requests:
            _edm_log("[EDM-AWB-FALLBACK] requests package not available; EDM check skipped.")
            _warned_no_requests = True
        return None

    token = _get_edm_token()
    if not token:
        if not _warned_no_token:
            _edm_log("[EDM-AWB-FALLBACK] No EDM token found; check skipped.")
            _warned_no_token = True
        return None

    exists = _check_edm_api_for_awb(awb, token)
    if exists is None:
        if not _warned_auth:
            _edm_log(
                "[EDM-AWB-FALLBACK] EDM response was inconclusive or unauthorized; "
                "falling back without EDM confirmation."
            )
            _warned_auth = True
        return None

    _cache_put(awb, exists)
    _edm_log(
        f"[EDM-AWB-FALLBACK] AWB {awb} "
        f"{'confirmed in EDM' if exists else 'not found in EDM'}."
    )
    return exists
