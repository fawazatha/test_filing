# src/services/alerts/ingestion_context.py
from __future__ import annotations
from pathlib import Path
from typing import Any, Dict, List, Optional

ANN_KEYS = [
    "date", "title", "title_slug", "company_name",
    "main_link", "filename", "attachments"
]

def _trim_announcement(a: Dict[str, Any]) -> Dict[str, Any]:
    out = {k: a.get(k) for k in ANN_KEYS}
    # pastikan attachments minimal [{filename,url}]
    atts = []
    for it in (a.get("attachments") or []):
        atts.append({
            "filename": (it or {}).get("filename"),
            "url": (it or {}).get("url"),
        })
    out["attachments"] = atts
    return out

def build_ingestion_index(ingestion_json_path: str) -> Dict[str, Dict[str, Any]]:
    """
    Return dict:
      by_filename[<filename>] -> trimmed announcement
    Index mencakup main 'filename' dan setiap attachment 'filename'.
    """
    p = Path(ingestion_json_path)
    if not p.exists():
        return {}
    data = p.read_text(encoding="utf-8")
    import json
    rows = json.loads(data) if data.strip() else []
    index: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        ann = _trim_announcement(row)
        # index main filename
        fn = (ann.get("filename") or "").strip()
        if fn:
            index[fn] = ann
        # index attachments filename
        for att in ann.get("attachments") or []:
            afn = (att.get("filename") or "").strip()
            if afn:
                index[afn] = ann
    return index

# src/services/alerts/ingestion_context.py
from __future__ import annotations
from pathlib import Path
from typing import Any, Dict, List, Optional

ANN_KEYS = [
    "date", "title", "title_slug", "company_name",
    "main_link", "filename", "attachments"
]

def _trim_announcement(a: Dict[str, Any]) -> Dict[str, Any]:
    out = {k: a.get(k) for k in ANN_KEYS}
    # pastikan attachments minimal [{filename,url}]
    atts = []
    for it in (a.get("attachments") or []):
        atts.append({
            "filename": (it or {}).get("filename"),
            "url": (it or {}).get("url"),
        })
    out["attachments"] = atts
    return out

def build_ingestion_index(ingestion_json_path: str) -> Dict[str, Dict[str, Any]]:
    """
    Return dict:
      by_filename[<filename>] -> trimmed announcement
    Index mencakup main 'filename' dan setiap attachment 'filename'.
    """
    p = Path(ingestion_json_path)
    if not p.exists():
        return {}
    data = p.read_text(encoding="utf-8")
    import json
    rows = json.loads(data) if data.strip() else []
    index: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        ann = _trim_announcement(row)
        # index main filename
        fn = (ann.get("filename") or "").strip()
        if fn:
            index[fn] = ann
        # index attachments filename
        for att in ann.get("attachments") or []:
            afn = (att.get("filename") or "").strip()
            if afn:
                index[afn] = ann
    return index
