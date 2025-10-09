from __future__ import annotations
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Tuple

log = logging.getLogger("filings.loaders")

def load_json(path: str | Path) -> Any:
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
    # Parser kita menyimpan LIST of dict untuk parsed_*_output.json
    # Namun jaga-jaga kalau ada wrapper {"items": [...]}.
    if data is None:
        return []
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        items = data.get("items") or data.get("data") or data.get("rows") or []
        if isinstance(items, list):
            return items
    # fallback: tidak dikenal
    return []

def load_parsed_files(paths: List[str | Path]) -> List[List[dict]]:
    """
    Return list of chunks; setiap elemen = list[dict] untuk satu file parsed.
    Selain mengembalikan data, fungsi ini juga melakukan logging jumlah
    per file agar terlihat mana yang kosong/terbaca.
    """
    out: List[List[dict]] = []
    for p in paths:
        lst = _coerce_list(load_json(p))
        log.info("[LOAD] parsed file %-40s → %d rows", str(p), len(lst))
        out.append(lst)
    return out

def build_downloads_meta_map(path: str | Path) -> Dict[str, Any]:
    """
    Build index metadata dari downloads (opsional). Kami index-kan berdasarkan:
    - pdf_url / source / filename
    - basename(filename) sebagai convenience key
    """
    data = load_json(path)
    if not data:
        return {}
    items = data if isinstance(data, list) else data.get("items", [])
    m: Dict[str, Any] = {}
    for it in items:
        k = it.get("pdf_url") or it.get("source") or it.get("filename")
        if not k:
            continue
        m[k] = it
        # juga map by basename
        try:
            from pathlib import Path as _P
            m[_P(k).name] = it
        except Exception:
            pass
    log.info("[LOAD] downloads meta: %d keys", len(m))
    return m
