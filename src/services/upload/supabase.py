from __future__ import annotations

import os
import json
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Iterable
import httpx

logger = logging.getLogger(__name__)

def _ensure_list(v: Any):
    if v is None: return None
    if isinstance(v, list): return v
    if isinstance(v, str):
        s = v.strip()
        try:
            parsed = json.loads(s)
            if isinstance(parsed, list): return parsed
        except Exception:
            pass
        if s.startswith("{") and s.endswith("}"):
            inner = s[1:-1]
            if not inner: return []
            parts, buf, quote = [], [], False
            i = 0
            while i < len(inner):
                ch = inner[i]
                if ch == '"': quote = not quote; i += 1; continue
                if ch == ',' and not quote:
                    parts.append(''.join(buf).strip()); buf = []; i += 1; continue
                buf.append(ch); i += 1
            if buf: parts.append(''.join(buf).strip())
            return [p.strip('"') for p in parts]
        return [v]
    try:
        return list(v)
    except Exception:
        return [str(v)]

def _drop_none_keys(d: Dict[str, Any]) -> Dict[str, Any]:
    return {k: v for k, v in d.items() if v is not None}

def _sanitize_row_for_insert(row: Dict[str, Any]) -> Dict[str, Any]:
    r = dict(row)
    if "tickers" in r: r["tickers"] = None
    if "tags" in r: r["tags"] = _ensure_list(r.get("tags"))
    return r

def _debug_field_types(r: Dict[str, Any], fields=("symbol","tags","tickers","price_transaction")) -> None:
    for k in fields:
        v = r.get(k, None)
        logger.debug("FIELD %s -> type=%s value=%r", k, type(v).__name__, v)

def _filter_allowed(row: Dict[str, Any], allowed: Optional[Iterable[str]]) -> Dict[str, Any]:
    if not allowed: return row
    allow = set(allowed)
    return {k: v for k, v in row.items() if k in allow}

@dataclass
class UploadResult:
    inserted: int = 0
    failed_rows: List[Dict[str, Any]] = field(default_factory=list)
    errors: List[Any] = field(default_factory=list)

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
                "Prefer": "return=representation",
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
        tbl = table or self.default_table
        res = UploadResult()
        for row in rows:
            r = _sanitize_row_for_insert(row)
            r = _filter_allowed(r, allowed_columns)
            r = _drop_none_keys(r)
            _debug_field_types(r)
            try:
                resp = self._post_one(tbl, r)
                if resp.status_code >= 400:
                    res.failed_rows.append(r)
                    try: res.errors.append(resp.json())
                    except Exception: res.errors.append(resp.text)
                    if stop_on_first_error: break
                else:
                    res.inserted += 1
            except Exception as e:
                res.failed_rows.append(r)
                res.errors.append(repr(e))
                if stop_on_first_error: break
        return res

    def debug_probe(self, table: str, sample_row: Dict[str, Any]) -> str:
        r = _drop_none_keys(_sanitize_row_for_insert(sample_row))
        _debug_field_types(r)
        resp = self._post_one(table or self.default_table, r)
        try: body = resp.json()
        except Exception: body = resp.text
        return f'status={resp.status_code} body={body!r}'
