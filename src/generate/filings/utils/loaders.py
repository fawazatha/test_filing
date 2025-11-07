# src/generate/filings/utils/loaders.py
from __future__ import annotations
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Tuple

log = logging.getLogger("filings.loaders")

def _basename(p: str) -> str:
    try:
        return Path(p).name
    except Exception:
        return p

def _stem(p: str) -> str:
    try:
        return Path(p).stem
    except Exception:
        return p


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
    Indexed by: pdf_url / source / filename + their basename/stem aliases.
    """
    data = load_json(path)
    if not data:
        return {}
    items = data if isinstance(data, list) else data.get("items", [])
    m: Dict[str, Any] = {}
    for it in items:
        if not isinstance(it, dict):
            continue

        # normalize a canonical URL field for downstream usage
        main_link = (
            it.get("main_link") or
            it.get("link") or
            it.get("url") or
            it.get("pdf_url") or
            it.get("original_url") or
            it.get("public_url")
        )
        if main_link:
            it["main_link"] = main_link  # ensure present

        # primary candidate keys
        keys = []
        for k in (it.get("pdf_url"), it.get("source"), it.get("filename")):
            if k:
                keys.extend([k, _basename(k), _stem(k)])

        # Make sure we have at least one key; if none, skip
        if not keys:
            continue

        for k in keys:
            m[str(k)] = it

    log.info("[LOAD] downloads meta: %d keys", len(m))
    return m


def build_ingestion_map(path: str | Path) -> Dict[str, Dict[str, Any]]:
    """
    Loads the ingestion file (e.g., ingestion.json) and creates a robust lookup map:
      keys: filename / source / pdf_url (and their basename/stem aliases)
      values: full item dict, with at least { "date": "...", "main_link": "..." } if available.
    """
    log.info(f"Building ingestion map from: {path}")
    data = load_json(path)
    items = _coerce_list(data)

    ingestion_map: Dict[str, Dict[str, Any]] = {}
    for item in items:
        if not isinstance(item, dict):
            continue

        # normalize main_link for downstream consumers
        item["main_link"] = (
            item.get("main_link") or
            item.get("link") or
            item.get("url") or
            item.get("pdf_url") or
            item.get("original_url") or
            item.get("public_url")
        )

        # keep whichever date-like field is present (no hard requirement)
        item["date"] = item.get("date") or item.get("timestamp") or item.get("announcement_published_at")

        # candidate keys (we’ll index each + basename + stem)
        candidate_keys: List[str] = []
        for k in (item.get("filename"), item.get("source"), item.get("pdf_url")):
            if k:
                candidate_keys.extend([k, _basename(k), _stem(k)])

        # if still nothing, try to salvage from explicit filename-like fields
        if not candidate_keys and item.get("file"):
            candidate_keys.extend([item["file"], _basename(item["file"]), _stem(item["file"])])

        # if we really have nothing to index by, skip
        if not candidate_keys:
            continue

        for k in candidate_keys:
            ingestion_map[str(k)] = item

    log.info(f"Built ingestion map with {len(ingestion_map)} entries.")
    return ingestion_map
