from __future__ import annotations
from pathlib import Path
from typing import Dict, Any, List
import json

def load_downloads_map(path: Path) -> Dict[str, Dict[str, Any]]:
    """
    Map: { filename.pdf: {"url":..., "timestamp":..., "ticker":..., "title":...}, ... }
    """
    if not path.exists():
        return {}
    data: List[Dict[str, Any]] = json.loads(path.read_text())
    out: Dict[str, Dict[str, Any]] = {}
    for row in data:
        fn = (row.get("filename") or "").strip()
        if not fn:
            continue
        out[fn] = {
            "url": row.get("url"),
            "timestamp": row.get("timestamp"),
            "ticker": (row.get("ticker") or "").strip().upper(),
            "title": row.get("title"),
        }
    return out
