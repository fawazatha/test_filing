# utils/uploader.py
from __future__ import annotations
import os
import json
from typing import Any, Dict, List, Optional, Iterable, Tuple, Union
from datetime import datetime
import pathlib

from services.upload.supabase import SupabaseUploader
from .io_utils import get_logger

log = get_logger(__name__)

# -------- helpers --------
def _ensure_list(v: Any) -> Optional[List[Any]]:
    if v is None:
        return None
    if isinstance(v, list):
        return v
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None
        # coba parse JSON list
        try:
            parsed = json.loads(s)
            if isinstance(parsed, list):
                return parsed
        except Exception:
            # fallback: treat as single item list
            return [s]
    return [v]

def _ensure_str_list(v: Any) -> Optional[List[str]]:
    lst = _ensure_list(v)
    if lst is None:
        return None
    out: List[str] = []
    for x in lst:
        if x is None:
            continue
        out.append(str(x).strip())
    return out or None

def _first_str(seq: Optional[Iterable[Any]]) -> Optional[str]:
    if not seq:
        return None
    for x in seq:
        if x is None:
            continue
        s = str(x).strip()
        if s:
            return s
    return None

def _coerce_iso(ts: Optional[str]) -> Optional[str]:
    if not ts:
        return None
    s = str(ts).strip()
    if not s:
        return None
    # terima bentuk "YYYY-MM-DD HH:MM:SS" atau ISO
    try:
        if "T" in s:
            try:
                datetime.fromisoformat(s.replace("Z", "+00:00"))
                return s
            except Exception:
                pass
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%Y/%m/%d %H:%M:%S"):
            try:
                dt = datetime.strptime(s, fmt)
                return dt.isoformat()
            except Exception:
                continue
        return s
    except Exception:
        return s


_ALLOWED_COLS = {
    "title", "body", "source", "timestamp",
    "company_name", "symbol", "sector", "sub_sector",
    "tags", "dimension", "votes", "score",
    "tickers", 
}

def _normalize_article_row(row: Dict[str, Any], prefer_symbol: bool = True) -> Dict[str, Any]:
    r = dict(row) 

    sym = (r.get("symbol") or "").strip()
    tickers: Optional[List[str]] = _ensure_str_list(r.get("tickers"))

    if prefer_symbol:
        if not tickers and sym:
            tickers = [sym]
    else:
        if not sym:
            sym = _first_str(tickers) or ""

    if not sym:
        sym = _first_str(tickers) or ""

    r["symbol"] = sym or None
    r["tickers"] = tickers 

    r["sub_sector"] = _ensure_str_list(r.get("sub_sector"))
    r["tags"] = _ensure_str_list(r.get("tags"))

    r["timestamp"] = _coerce_iso(r.get("timestamp"))

    if r.get("sentiment") is not None:
        try:
            r["sentiment"] = str(r["sentiment"]).strip().lower()
        except Exception:
            pass

    r["dimension"] = None
    r["votes"] = None
    r["score"] = None

    keep: Dict[str, Any] = {}
    for k, v in r.items():
        if k in _ALLOWED_COLS:
            keep[k] = v
    return keep

def _read_json_or_jsonl(path: Union[str, pathlib.Path]) -> List[Dict[str, Any]]:
    p = pathlib.Path(path)
    if not p.exists():
        raise FileNotFoundError(str(p))
    if p.suffix.lower() == ".jsonl":
        rows: List[Dict[str, Any]] = []
        with p.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                rows.append(json.loads(line))
        return rows
    elif p.suffix.lower() == ".json":
        data = json.loads(p.read_text(encoding="utf-8"))
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            return [data]
        raise ValueError("Unsupported JSON structure: expected list or dict")
    else:
        raise ValueError("Only .json or .jsonl is supported")

def upload_news_file_cli(
    input_path: str,
    table: str = "idx_news",
    dry_run: bool = False,
    timeout: int = 30,
    prefer_symbol: bool = True,
    supabase_url: Optional[str] = None,
    supabase_key: Optional[str] = None,
) -> Tuple[int, int]:
    """
    Upload file artikel (.json/.jsonl) ke tabel `table`.
    Return: (success_count, fail_count)
    """
    rows = _read_json_or_jsonl(input_path)
    normed = [_normalize_article_row(r, prefer_symbol=prefer_symbol) for r in rows]

    log.info("Loaded %d rows from %s", len(normed), input_path)

    if dry_run:
        preview = normed[0] if normed else {}
        log.info("[DRY-RUN] sample row after normalization: %s", json.dumps(preview, ensure_ascii=False))
        return (0, 0)

    uploader = SupabaseUploader(
        url=supabase_url or os.getenv("SUPABASE_URL", ""),
        key=supabase_key or os.getenv("SUPABASE_KEY", ""),
        default_table=table,
        timeout=timeout,
    )

    res = uploader.insert_many(normed, table=table, stop_on_first_error=False)
    ok = sum(1 for r in res if r.ok)
    bad = sum(1 for r in res if not r.ok)
    if bad:
        log.warning("Some rows failed to insert: %d failed / %d total", bad, len(normed))
        for i, r in enumerate(res):
            if not r.ok:
                log.error("Row %d insert failed: status=%s body=%s", i, r.status_code, r.body)
    else:
        log.info("All rows inserted OK: %d", ok)
    return (ok, bad)
