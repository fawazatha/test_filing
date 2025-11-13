# src/services/alerts/ingestion_context.py
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional, List
import json

# Only the fields we actually need to echo back into alert context
ANN_KEYS = [
    "date",          # "2025-10-27T15:30:12"
    "title",
    "title_slug",
    "company_name",  # e.g., "BCIP"
    "main_link",
    "filename",      # normalized main filename (from URL)
    "attachments",   # list[{filename, url}]
]

def _trim_announcement(a: Dict[str, Any]) -> Dict[str, Any]:
    """
    Keep a stable subset and normalize attachments -> [{filename, url}].
    Missing keys become None.
    """
    out = {k: a.get(k) for k in ANN_KEYS}
    # normalize attachments
    atts: List[Dict[str, Optional[str]]] = []
    for it in (a.get("attachments") or []):
        atts.append({
            "filename": (it or {}).get("filename"),
            "url":      (it or {}).get("url"),
        })
    out["attachments"] = atts
    return out

def _safe_read_json(path: Path) -> List[Dict[str, Any]]:
    """
    Read a JSON array file safely. Returns [] if missing/empty.
    """
    if not path.exists():
        return []
    txt = path.read_text(encoding="utf-8").strip()
    if not txt:
        return []
    try:
        data = json.loads(txt)
        return data if isinstance(data, list) else []
    except Exception:
        # tolerate a line-delimited emergency format (not typical for ingestion)
        rows: List[Dict[str, Any]] = []
        for line in txt.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                if isinstance(obj, dict):
                    rows.append(obj)
            except Exception:
                # ignore bad lines
                pass
        return rows

def build_ingestion_index(ingestion_json_path: str) -> Dict[str, Dict[str, Any]]:
    """
    Build a fast lookup:
        by_filename_lower[<filename>] -> trimmed announcement
    The index includes:
      - main 'filename'
      - each attachment 'filename'
    """
    p = Path(ingestion_json_path)
    rows = _safe_read_json(p)
    index: Dict[str, Dict[str, Any]] = {}

    for row in rows:
        ann = _trim_announcement(row)

        # index main filename
        fn = (ann.get("filename") or "").strip()
        if fn:
            index[fn.lower()] = ann

        # index attachment filenames
        for att in (ann.get("attachments") or []):
            afn = (att.get("filename") or "").strip()
            if afn:
                index[afn.lower()] = ann

    return index

def resolve_doc_context_from_announcement(
    ann: Optional[Dict[str, Any]],
    filename: Optional[str],
) -> Dict[str, Any]:
    """
    Build a compact doc context to embed into alerts:
      {
        "filename": ...,
        "url": ...,                 # best-effort: use attachment url that matches filename, else main_link
        "title": ...,
        "company_name": ...,
        "published_at": ...,        # same as 'date' in ingestion
      }
    """
    ctx: Dict[str, Any] = {
        "filename": filename,
        "url": None,
        "title": None,
        "company_name": None,
        "published_at": None,
    }
    if not ann:
        return ctx

    ctx["title"] = ann.get("title")
    ctx["company_name"] = ann.get("company_name")
    ctx["published_at"] = ann.get("date")

    # try to resolve the exact attachment url by filename
    fn_l = (filename or "").strip().lower()
    url: Optional[str] = None
    for att in (ann.get("attachments") or []):
        afn = (att.get("filename") or "").strip().lower()
        if afn and afn == fn_l:
            url = att.get("url")
            break

    # fallback to main_link
    if not url:
        url = ann.get("main_link")

    ctx["url"] = url
    return ctx
