from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional
from datetime import datetime

try:
    import zoneinfo
    JKT = zoneinfo.ZoneInfo("Asia/Jakarta")
except Exception:
    JKT = None

from .provider import get_tags, get_company_info, get_latest_price


# --- Helpers for WIB parsing/formatting ---

def _parse_dt_wib(dtstr: Optional[str]) -> Optional[datetime]:
    """
    Parse a variety of timestamp formats and attach Asia/Jakarta tz if naive.
    """
    if not dtstr:
        return None
    # common formats we’ve seen across ingestion/download meta
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y%m%d-%H%M%S"):
        try:
            dt = datetime.strptime(dtstr, fmt)
            return dt.replace(tzinfo=JKT) if (JKT and dt.tzinfo is None) else dt
        except Exception:
            pass
    # last resort: ISO parser
    try:
        dt = datetime.fromisoformat(dtstr)
        return dt.replace(tzinfo=JKT) if (JKT and dt.tzinfo is None) else dt
    except Exception:
        return None


def _iso_wib(dt: Optional[datetime]) -> Optional[str]:
    """
    Return ISO8601 with explicit WIB offset (+07:00), no microseconds.
    """
    if dt is None:
        return None
    if JKT:
        dt = dt.astimezone(JKT)
    return dt.replace(microsecond=0).isoformat()


def _extract_announcement_published_at(
    raw: Dict[str, Any],
    downloads_meta_map: Optional[Dict[str, Any]],
) -> Optional[str]:
    """
    Pull the announcement published timestamp (WIB) from downloads/ingestion metadata.

    Priority:
      1) downloads_meta_map[one_of_keys].published_at_wib
      2) downloads_meta_map[one_of_keys].announcement_published_at
      3) downloads_meta_map[one_of_keys].published_at
      4) raw['published_at'] (fallback)

    Keys tried (in order): id, source_id, pdf_id, source_key, url
    """
    if downloads_meta_map:
        key_candidates = [
            raw.get("id"),
            raw.get("source_id"),
            raw.get("pdf_id"),
            raw.get("source_key"),
            raw.get("url"),
        ]
        meta = None
        for k in key_candidates:
            if k and k in downloads_meta_map:
                meta = downloads_meta_map.get(k)
                break
        if isinstance(meta, dict):
            wib_str = meta.get("published_at_wib") or meta.get("announcement_published_at") or meta.get("published_at")
            return _iso_wib(_parse_dt_wib(wib_str))

    # Fallback if no meta
    return _iso_wib(_parse_dt_wib(raw.get("published_at")))


# --------------------------------------------------------------------------------------
# NOTE:
# - We intentionally DO NOT modify your existing build_row(..) implementation.
# - We only append `announcement_published_at` after build_row returns a row.
# - Your prior logic for tags/prices/etc. stays untouched.
# --------------------------------------------------------------------------------------

def process_all(parsed_lists: List[List[Dict[str, Any]]], downloads_meta_map: Dict[str, Any] | None) -> List[Dict[str, Any]]:
    """Flatten + transform all parsed chunks into final rows, then inject `announcement_published_at`."""
    out: List[Dict[str, Any]] = []
    for chunk in parsed_lists:
        if not chunk:
            continue
        for raw in chunk:
            try:
                # Your original row builder (kept intact)
                row = build_row(raw, downloads_meta_map)  # type: ignore[name-defined]
                # NEW: inject ISO8601 WIB published date
                row["announcement_published_at"] = _extract_announcement_published_at(raw, downloads_meta_map)
                out.append(row)
            except Exception:
                # swallow & continue like original
                continue
    return out
