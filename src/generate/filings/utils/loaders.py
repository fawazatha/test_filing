from __future__ import annotations
import os, json, logging
from typing import Any, Dict, List
from ..types import DownloadMeta

logger = logging.getLogger(__name__)

def load_json(path: str) -> Any:
    if not os.path.exists(path):
        logger.warning("Missing file: %s", path);  return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def save_json(path: str, obj: Any):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)

def ensure_dir(path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)

def build_download_map(downloads_json: str) -> Dict[str, DownloadMeta]:
    raw = load_json(downloads_json) or []
    out: Dict[str, DownloadMeta] = {}
    for rec in raw:
        fn = os.path.basename(rec.get("filename","") or "")
        out[fn] = DownloadMeta(
            filename=fn,
            url=rec.get("url"),
            timestamp=rec.get("timestamp"),  
            title=rec.get("title"),
        )
    logging.getLogger(__name__).info("Download map loaded: %d entries", len(out))
    return out

def load_parsed_items(paths: List[str]) -> List[dict]:
    items: List[dict] = []
    for p in paths:
        arr = load_json(p)
        if isinstance(arr, list):
            items.extend(arr)
            logging.getLogger(__name__).info("Loaded %s: %d items", p, len(arr))
        elif arr is None:
            logging.getLogger(__name__).info("Skip missing: %s", p)
        else:
            logging.getLogger(__name__).warning("Unexpected shape (not list) in %s", p)
    logging.getLogger(__name__).info("Total parsed items: %d", len(items))
    return items
