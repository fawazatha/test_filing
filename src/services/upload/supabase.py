from __future__ import annotations

import os
import json
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Iterable
import httpx

# Import the new core type
from src.core.types import FilingRecord

logger = logging.getLogger(__name__)

# ---------------- helpers ----------------
def _debug_field_types(r: Dict[str, Any], fields=("symbol","tags","sub_sector","price_transaction")) -> None:
    """Helper to log types of key fields before upload."""
    for k in fields:
        v = r.get(k, None)
        logger.debug("FIELD %s -> type=%s value=%r", k, type(v).__name__, v)

# ---------------- result ----------------
@dataclass
class UploadResult:
    """Dataclass to hold the result of an upload batch."""
    inserted: int = 0
    failed_rows: List[Dict[str, Any]] = field(default_factory=list)
    errors: List[Any] = field(default_factory=list)

# ---------------- main uploader ----------------
class SupabaseUploader:
    def __init__(self,
                 url: Optional[str] = None,
                 key: Optional[str] = None,
                 table: Optional[str] = None) -> None:
        # Read ENV vars at runtime
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
                "Supabase not configured. Provide SUPABASE_URL and SUPABASE_KEY."
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
        """Sends a single row to Supabase."""
        endpoint = f"/rest/v1/{table}"
        resp = self._client.post(endpoint, json=payload)
        logger.debug('HTTP Request: POST %s "%s %s"', self.url + endpoint, resp.http_version, resp.status_code)
        return resp

    def upload_records(self,
                       table: Optional[str],
                       rows: List[Dict[str, Any]],
                       allowed_columns: Optional[Iterable[str]] = None,
                       stop_on_first_error: bool = False,
                       **kwargs) -> UploadResult: # --- PERBAIKAN: Menambahkan **kwargs ---
        """
        Uploads a list of pre-cleaned DICTIONARIES to Supabase.
        
        This method assumes data is already clean and standardized.
        It no longer performs any sanitation logic.
        
        **kwargs is added to accept and ignore legacy arguments
        like 'normalize_keys' for backward compatibility.
        """
        
        # --- PERBAIKAN: Memberi log jika ada argumen yang tidak terpakai ---
        if 'normalize_keys' in kwargs:
            logger.debug("Ignoring legacy argument 'normalize_keys' in upload_records.")
        # --- AKHIR PERBAIKAN ---

        tbl = table or self.default_table
        res = UploadResult()
        
        allow_set = set(allowed_columns) if allowed_columns else None

        for row in rows:
            # 1. Filter by allowed_columns if provided
            payload = {k: v for k, v in row.items() if k in allow_set} if allow_set else row
            
            # 2. Drop None keys (still good practice)
            payload = {k: v for k, v in payload.items() if v is not None}

            _debug_field_types(payload)
            try:
                resp = self._post_one(tbl, payload)
                if resp.status_code >= 400:
                    res.failed_rows.append(row) # Log original row
                    try:
                        res.errors.append(resp.json())
                    except Exception:
                        res.errors.append(resp.text)
                    if stop_on_first_error:
                        break
                else:
                    res.inserted += 1
            except Exception as e:
                res.failed_rows.append(row)
                res.errors.append(repr(e))
                if stop_on_first_error:
                    break
        return res

    def upload_filing_records(self,
                       table: Optional[str],
                       records: List[FilingRecord],
                       allowed_columns: Optional[Iterable[str]] = None,
                       stop_on_first_error: bool = False) -> UploadResult:
        """
        New convenience method to upload FilingRecord objects directly.
        This should be the new standard method for uploads.
        """
        # Convert dataclasses to the clean DB dict format
        rows_to_upload = [rec.to_db_dict() for rec in records]
        
        # Use the list of columns from the record's converter if not provided
        if not allowed_columns and rows_to_upload:
            allowed_columns = list(rows_to_upload[0].keys())
        
        return self.upload_records(
            table=table,
            rows=rows_to_upload,
            allowed_columns=allowed_columns,
            stop_on_first_error=stop_on_first_error
        )

    def debug_probe(self, table: str, sample_row: Dict[str, Any]) -> str:
        """
        Sends one sample row to Supabase to debug insertion errors.
        Assumes sample_row is a clean dict.
        """
        # Simple cleanup, mirroring the upload loop
        r = {k: v for k, v in sample_row.items() if v is not None}
        _debug_field_types(r)
        
        resp = self._post_one(table or self.default_table, r)
        try:
            body = resp.json()
        except Exception:
            body = resp.text
        return f'status={resp.status_code} body={body!r}'

