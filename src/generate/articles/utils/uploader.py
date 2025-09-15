from __future__ import annotations
import os, json, time
from typing import Any, Dict, List, Optional
from .io_utils import read_json, read_jsonl
from .io_utils import get_logger

log = get_logger(__name__)

ALLOWED_NEWS_COLS = {
    "title", "body", "source", "timestamp",
    "sector", "sub_sector", "tags", "tickers",
    "dimension", "votes", "score",
}

def _ensure_list(x):
    if x is None:
        return []
    return x if isinstance(x, list) else [x]

def _parse_json_or(val, default=None):
    if val is None:
        return default
    if isinstance(val, (dict, list)):
        return val
    if isinstance(val, str):
        try:
            return json.loads(val)
        except Exception:
            return default
    return default

def _get_supabase_client():
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY") or os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_ANON_KEY")
    if not url or not key:
        raise RuntimeError("SUPABASE_URL / SUPABASE_KEY env tidak tersedia.")

    # Prefer library if installed, else fallback HTTP
    try:
        from supabase import create_client  # type: ignore
        return ("lib", create_client(url, key))
    except Exception:
        import requests  # type: ignore
        session = requests.Session()
        session.headers.update({
            "apikey": key,
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
            "Prefer": "return=representation",
        })
        return ("http", (url, session))

def _http_insert(url_base: str, session, table: str, row: Dict[str, Any], timeout: int = 30):
    import requests
    endpoint = f"{url_base}/rest/v1/{table}"
    r = session.post(endpoint, data=json.dumps(row), timeout=timeout)
    if r.status_code not in (200, 201):
        raise RuntimeError(f"HTTP {r.status_code}: {r.text[:300]}")
    return r.json() if r.text else {}

def _read_news_items(input_path: str) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    try_json = True
    if input_path.lower().endswith(".jsonl"):
        try_json = False
    if try_json:
        try:
            obj = read_json(input_path)
            if isinstance(obj, list):
                items = obj
            elif isinstance(obj, dict):
                items = [obj]
            else:
                log.error("JSON tidak valid (bukan list/dict).")
        except Exception:
            # fallback ke JSONL
            items = read_jsonl(input_path)
    else:
        items = read_jsonl(input_path)
    return items

def _sanitize_row(item: Dict[str, Any]) -> Dict[str, Any]:
    row_full = {
        "title": item.get("title"),
        "body": item.get("body"),
        "source": item.get("source"),
        "timestamp": item.get("timestamp"),
        "sector": item.get("sector"),
        "sub_sector": _ensure_list(item.get("sub_sector")),
        "tags": _ensure_list(item.get("tags")),
        "tickers": _ensure_list(item.get("tickers")),
        "dimension": _parse_json_or(item.get("dimension"), None),
        "votes": _parse_json_or(item.get("votes"), None),
        "score": item.get("score", None),
    }
    row = {k: v for k, v in row_full.items() if k in ALLOWED_NEWS_COLS}
    return row

def upload_news_items(items: List[Dict[str, Any]], table: str = "idx_news",
                      dry_run: bool = False, timeout: int = 30) -> int:
    if not items:
        log.warning("Tidak ada items untuk diupload.")
        return 0

    if dry_run:
        for it in items[:3]:
            log.info(f"[DRY-RUN] {json.dumps(_sanitize_row(it), ensure_ascii=False)}")
        if len(items) > 3:
            log.info(f"[DRY-RUN] ... {len(items)-3} rows lainnya")
        return 0

    mode, client = _get_supabase_client()
    inserted = 0

    if mode == "lib":
        sb = client
        for it in items:
            row = _sanitize_row(it)
            try:
                resp = sb.table(table).insert(row).execute()
                log.info(f"[INSERTED NEWS] {row.get('title')} → {str(resp)[:120]}")
                inserted += 1
            except Exception as e:
                log.error(f"[ERROR] Insert failed for title={row.get('title')}: {e}")
    else:
        url_base, session = client
        for it in items:
            row = _sanitize_row(it)
            try:
                resp = _http_insert(url_base, session, table, row, timeout=timeout)
                log.info(f"[INSERTED NEWS] {row.get('title')} → ok")
                inserted += 1
            except Exception as e:
                log.error(f"[ERROR] Insert failed for title={row.get('title')}: {e}")

    log.info(f"[DONE] Inserted {inserted} rows into {table}.")
    return inserted

def upload_news_file_cli(input_path: str, table: str = "idx_news",
                         dry_run: bool = False, timeout: int = 30):
    if not os.path.exists(input_path):
        log.error(f"{input_path} not found.")
        return
    items = _read_news_items(input_path)
    upload_news_items(items, table=table, dry_run=dry_run, timeout=timeout)
