from __future__ import annotations
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Tuple

log = logging.getLogger("filings.loaders")

def load_json(path: str | Path) -> Any:
    """Loads a JSON file, returning None on error or if missing."""
    p = Path(path)
    if not p.exists():
        log.info("[LOAD] missing: %s", p)
        return None
    try:
        txt = p.read_text(encoding="utf-8")
        if not txt.strip():
            log.info("[LOAD] empty file: %s", p)
            return None
        return json.loads(txt)
    except Exception as e:
        log.warning("[LOAD] invalid json %s: %s", p, e)
        return None

def _coerce_list(data: Any) -> List[dict]:
    """
    Finds the list of records within a loaded JSON structure.
    (e.g., handles {"items": [...]}).
    """
    if data is None:
        return []
    if isinstance(data, list):
        return [d for d in data if isinstance(d, dict)]
    if isinstance(data, dict):
        items = data.get("items") or data.get("data") or data.get("rows") or []
        if isinstance(items, list):
            return [d for d in items if isinstance(d, dict)]
    return []

def load_parsed_files(paths: List[str | Path]) -> List[List[dict]]:
    """
    Return list of chunks; each element = list[dict] for one parsed file.
    """
    out: List[List[dict]] = []
    for p in paths:
        lst = _coerce_list(load_json(p))
        log.info("[LOAD] parsed file %-40s → %d rows", str(p), len(lst))
        out.append(lst)
    return out

def build_downloads_meta_map(path: str | Path) -> Dict[str, Any]:
    """
    Build index metadata from downloads (optional).
    Indexed by: pdf_url / source / filename
    """
    data = load_json(path)
    if not data:
        return {}
    items = data if isinstance(data, list) else data.get("items", [])
    m: Dict[str, Any] = {}
    for it in items:
        if not isinstance(it, dict):
            continue
        k = it.get("pdf_url") or it.get("source") or it.get("filename")
        if not k:
            continue
        m[k] = it
        try:
            from pathlib import Path as _P
            m[_P(k).name] = it
        except Exception:
            pass
    log.info("[LOAD] downloads meta: %d keys", len(m))
    return m

def build_ingestion_map(path: str | Path) -> Dict[str, Dict[str, Any]]:
    """
    Loads the ingestion file (e.g., ingestion.json) and creates
    a lookup map of {filename: {date: "...", "main_link": "..."}}.
    """
    log.info(f"Building ingestion map from: {path}")
    data = load_json(path)
    items = _coerce_list(data)
    
    # Mengubah dari Dict[str, str] menjadi Dict[str, Dict[str, Any]]
    ingestion_map: Dict[str, Dict[str, Any]] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        filename = item.get("filename")
        if filename:
            # Menyimpan seluruh item untuk fleksibilitas
            ingestion_map[str(filename)] = item
            
    log.info(f"Built ingestion map with {len(ingestion_map)} entries.")
    return ingestion_map