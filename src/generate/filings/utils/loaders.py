from __future__ import annotations
import json
from pathlib import Path
from typing import Any, Dict, List

def load_json(path: str | Path) -> Any:
    p = Path(path)
    if not p.exists():
        return None
    return json.loads(p.read_text(encoding="utf-8"))

def load_parsed_files(paths: List[str | Path]) -> List[List[dict]]:
    out: List[List[dict]] = []
    for p in paths:
        data = load_json(p)
        if not data:
            out.append([])
            continue
        # support array or {"rows":[...]}
        if isinstance(data, list):
            out.append(data)
        else:
            out.append(data.get("rows", []))
    return out

def build_downloads_meta_map(downloads_file: str | Path) -> Dict[str, Any]:
    data = load_json(downloads_file) or []
    items = data if isinstance(data, list) else data.get("items", [])
    m: Dict[str, Any] = {}
    for it in items:
        k = it.get("pdf_url") or it.get("source") or it.get("filename")
        if not k:
            continue
        m[k] = it
        # also map by filename for convenience
        try:
            from pathlib import Path as _P
            m[_P(k).name] = it
        except Exception:
            pass
    return m
