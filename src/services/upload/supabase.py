from __future__ import annotations

import os
import json
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Iterable
import httpx

logger = logging.getLogger(__name__)

# ---------------- helpers ----------------
def _ensure_list(v: Any):
    """
    Normalize value into a Python list.
    - If v is JSON list string (e.g., '["A","B"]') -> parse to list.
    - If v is plain string -> wrap as [v].
    - If v is already list -> return as-is.
    - If v is None -> return None (so caller can decide to keep/drop).
    """
    if v is None:
        return None
    if isinstance(v, list):
        return v
    if isinstance(v, str):
        s = v.strip()
        # Try JSON parse first (common for '["X","Y"]')
        try:
            parsed = json.loads(s)
            if isinstance(parsed, list):
                return parsed
        except Exception:
            pass
        # Fallback: treat string as single item
        return [s]
    try:
        return list(v)
    except Exception:
        return [str(v)]

def _drop_none_keys(d: Dict[str, Any], keep_none: Iterable[str] = ()) -> Dict[str, Any]:
    """
    Drop keys whose value is None, EXCEPT those listed in keep_none.
    This lets us explicitly insert NULL for some columns.
    """
    keep = set(keep_none or ())
    out: Dict[str, Any] = {}
    for k, v in d.items():
        if v is None and k not in keep:
            continue
        out[k] = v
    return out

def _sanitize_row_for_insert(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    - Ensure array-ish fields (tickers/tags/sub_sector) are lists.
    - Force dimension/votes/score to NULL (None) so DB stores NULL.
    - Do NOT null-out tickers (DB expects text[]).
    """
    r = dict(row)

    # Arrays
    if "tickers" in r:
        r["tickers"] = _ensure_list(r.get("tickers"))
    if "tags" in r:
        r["tags"] = _ensure_list(r.get("tags"))
    if "sub_sector" in r:
        r["sub_sector"] = _ensure_list(r.get("sub_sector"))

    # Force these columns to NULL (per requirement)
    for k in ("dimension", "votes", "score"):
        if k in r:
            r[k] = None

    return r

def _debug_field_types(r: Dict[str, Any], fields=("symbol","tags","tickers","sub_sector","price_transaction")) -> None:
    for k in fields:
        v = r.get(k, None)
        logger.debug("FIELD %s -> type=%s value=%r", k, type(v).__name__, v)

def _filter_allowed(row: Dict[str, Any], allowed: Optional[Iterable[str]]) -> Dict[str, Any]:
    if not allowed:
        return row
    allow = set(allowed)
    return {k: v for k, v in row.items() if k in allow}

# ---------------- result ----------------
@dataclass
class UploadResult:
    inserted: int = 0
    failed_rows: List[Dict[str, Any]] = field(default_factory=list)
    errors: List[Any] = field(default_factory=list)

# ---------------- main uploader ----------------
class SupabaseUploader:
    def __init__(self,
                 url: Optional[str] = None,
                 key: Optional[str] = None,
                 table: Optional[str] = None) -> None:
        # BACA ENV DI RUNTIME (setelah .env diload)
        effective_url = (url or os.getenv("SUPABASE_URL") or "").rstrip("/")
        effective_key = (key or os.getenv("SUPABASE_KEY") or "")
        effective_table = table or os.getenv("SUPABASE_TABLE", "idx_filings")

        self.url = effective_url
        self.key = effective_key
        self.default_table = effective_table

        if not self.url or not self.key:
            masked = (self.key[:4] + "…" + self.key[-4:]) if self.key else "None"
            logger.error("SupabaseUploader missing URL/KEY. url=%r key=%s", self.url or None, masked)
            raise RuntimeError(
                "Supabase not configured. Provide SUPABASE_URL and SUPABASE_KEY via .env/ENV "
                "or pass --supabase-url / --supabase-key flags."
            )

        self._client = httpx.Client(
            base_url=self.url,
            headers={
                "apikey": str(self.key),
                "Authorization": f"Bearer {self.key}",
                "Content-Type": "application/json",
                "Prefer": "return=representation",  # return inserted row(s)
            },
            timeout=30.0,
        )

    def _post_one(self, table: str, payload: Dict[str, Any]) -> httpx.Response:
        endpoint = f"/rest/v1/{table}"
        resp = self._client.post(endpoint, json=payload)
        logger.info('HTTP Request: POST %s "%s %s"', self.url + endpoint, resp.http_version, resp.status_code)
        return resp

    def upload_records(self,
                       table: Optional[str],
                       rows: List[Dict[str, Any]],
                       allowed_columns: Optional[Iterable[str]] = None,
                       normalize_keys: bool = False,
                       stop_on_first_error: bool = False) -> UploadResult:
        """
        allowed_columns: whitelist kolom yang boleh dikirim (mis. dari header CSV Supabase)
        stop_on_first_error: berhenti di error pertama (kalau True), default False (lanjutkan upload lainnya)
        """
        tbl = table or self.default_table
        res = UploadResult()
        for row in rows:
            r = _sanitize_row_for_insert(row)
            r = _filter_allowed(r, allowed_columns)

            # Keep explicit NULL for these columns
            r = _drop_none_keys(r, keep_none=("dimension", "votes", "score"))

            _debug_field_types(r)
            try:
                resp = self._post_one(tbl, r)
                if resp.status_code >= 400:
                    res.failed_rows.append(r)
                    try:
                        res.errors.append(resp.json())
                    except Exception:
                        res.errors.append(resp.text)
                    if stop_on_first_error:
                        break
                else:
                    res.inserted += 1
            except Exception as e:
                res.failed_rows.append(r)
                res.errors.append(repr(e))
                if stop_on_first_error:
                    break
        return res

    def debug_probe(self, table: str, sample_row: Dict[str, Any]) -> str:
        # Probe with the same sanitation + keep_none policy
        r = _drop_none_keys(_sanitize_row_for_insert(sample_row), keep_none=("dimension","votes","score"))
        _debug_field_types(r)
        resp = self._post_one(table or self.default_table, r)
        try:
            body = resp.json()
        except Exception:
            body = resp.text
        return f'status={resp.status_code} body={body!r}'
