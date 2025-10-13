from __future__ import annotations
import os
import sys
import json
import urllib.parse
import pathlib
from datetime import datetime, timezone
from typing import Dict, Any, Optional, Tuple, List

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

import requests

# --------------------------
# Konfigurasi I/O
# --------------------------
OUT_JSON   = pathlib.Path("data/company/company_map.json")
META_JSON  = pathlib.Path("data/company/company_map.meta.json")

# --------------------------
# Supabase env
# --------------------------
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = (
    os.getenv("SUPABASE_KEY")
    or os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    or os.getenv("SUPABASE_ANON_KEY")
)
SCHEMA_NAME  = os.getenv("COMPANY_SCHEMA", "public")

# Satu-satunya tabel sumber
REPORT_TABLE = os.getenv("COMPANY_REPORT_TABLE", "idx_company_report")
# Kolom yang diambil dari report
REPORT_SELECT = "symbol,company_name,sector,sub_sector,last_close_price,latest_close_date"

# Flags/opsi
ALLOW_OFFLINE = (os.getenv("COMPANY_MAP_ALLOW_OFFLINE", "1").lower() in ("1", "true", "yes", "y"))
FORCE_REFRESH = (os.getenv("COMPANY_MAP_FORCE_REFRESH", "0").lower() in ("1", "true", "yes", "y"))

# --------------------------
# Logging & helpers
# --------------------------
def log(msg: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{ts} | {msg}", file=sys.stderr)

def _headers() -> Dict[str, str]:
    if not SUPABASE_URL or not SUPABASE_KEY:
        return {}
    h = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Prefer": "count=exact",
    }
    if SCHEMA_NAME and SCHEMA_NAME != "public":
        h["Accept-Profile"] = SCHEMA_NAME
    return h

def _build_url(table: str, select: str, extra_params: Optional[Dict[str, str]] = None) -> str:
    base = f"{SUPABASE_URL}/rest/v1/{table}"
    params: Dict[str, str] = {"select": select}
    if extra_params:
        params.update(extra_params)
    return f"{base}?{urllib.parse.urlencode(params)}"

def _normalize_full(sym: str) -> str:
    s = (sym or "").upper().strip()
    return s if s.endswith(".JK") else f"{s}.JK"

def _checksum(d: Dict[str, Dict[str, Any]]) -> str:
    parts: List[str] = []
    for k in sorted(d.keys()):
        v = d[k] or {}
        parts.append(
            f"{k}|{v.get('company_name','')}|{v.get('sector','')}|{v.get('sub_sector','')}|"
            f"{v.get('last_close_price','')}|{v.get('latest_close_date','')}"
        )
    import hashlib as _hl  # local import
    return "sha256:" + _hl.sha256("\n".join(parts).encode("utf-8")).hexdigest()

# --------------------------
# Normalizer sektor/subsektor
# --------------------------
def _extract_str(v: Any) -> Optional[str]:
    """Ambil string bermakna dari dict/slug/JSON-ish/primitive."""
    if v is None:
        return None
    if isinstance(v, dict):
        for key in ("name", "title", "label", "text", "slug"):
            val = v.get(key)
            if isinstance(val, str) and val.strip():
                return val.strip()
        return None
    if isinstance(v, str):
        raw = v.strip()
        if not raw:
            return None
        # rescue JSON-ish {"name":"X"} (best-effort)
        if raw.startswith("{") and raw.endswith("}"):
            for tag in ['"name"', '"title"', '"slug"', "'name'", "'title'", "'slug'"]:
                idx = raw.find(tag)
                if idx != -1:
                    colon = raw.find(":", idx)
                    if colon != -1:
                        val = raw[colon + 1:].strip().strip(",} ").strip('"').strip("'").strip()
                        if val:
                            raw = val
                            break
        return raw or None
    try:
        s = str(v).strip()
        return s or None
    except Exception:
        return None

def _titlecase_preserve(s: Optional[str]) -> Optional[str]:
    """Title Case ringan, menjaga akronim pendek (PT, Tbk, JK)."""
    if not s:
        return s
    s = " ".join(s.split())
    words = []
    for w in s.split(" "):
        if w.isupper() and len(w) <= 3:
            words.append(w)
        else:
            words.append(w.capitalize())
    return " ".join(words)

def _normalize_sector(ss: Any) -> str:
    s = _extract_str(ss)
    return _titlecase_preserve(s) or ""

# --------------------------
# Local cache IO
# --------------------------
def load_local() -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Any]]:
    mapping: Dict[str, Dict[str, Any]] = {}
    meta: Dict[str, Any] = {}
    try:
        if OUT_JSON.exists():
            raw = json.loads(OUT_JSON.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                for sym, row in raw.items():
                    if not isinstance(row, dict):
                        continue
                    symu = (sym or "").upper().strip()
                    if not symu:
                        continue
                    entry = {
                        "company_name": (row.get("company_name") or "").strip(),
                        "sector": _normalize_sector(row.get("sector")),
                        "sub_sector": _normalize_sector(row.get("sub_sector")),
                        "last_close_price": row.get("last_close_price"),
                        "latest_close_date": row.get("latest_close_date"),
                    }
                    mapping[_normalize_full(symu)] = entry
            elif isinstance(raw, list):
                # dukung format lama berbasis list of rows
                for row in raw:
                    if not isinstance(row, dict):
                        continue
                    symu = (row.get("symbol") or "").upper().strip()
                    if not symu:
                        continue
                    entry = {
                        "company_name": (row.get("company_name") or "").strip(),
                        "sector": _normalize_sector(row.get("sector")),
                        "sub_sector": _normalize_sector(row.get("sub_sector")),
                        "last_close_price": row.get("last_close_price"),
                        "latest_close_date": row.get("latest_close_date"),
                    }
                    mapping[_normalize_full(symu)] = entry
    except Exception as e:
        log(f"warn: failed reading {OUT_JSON}: {e}")
    try:
        if META_JSON.exists():
            meta = json.loads(META_JSON.read_text(encoding="utf-8"))
    except Exception as e:
        log(f"warn: failed reading {META_JSON}: {e}")
    return mapping, meta

def save_local(mapping: Dict[str, Dict[str, Any]], rows_meta: Dict[str, Any]):
    # pastikan semua entry normalized sebelum simpan
    normed: Dict[str, Dict[str, Any]] = {}
    for sym, row in mapping.items():
        out = {
            "company_name": (row.get("company_name") or "").strip(),
            "sector": _normalize_sector(row.get("sector")),
            "sub_sector": _normalize_sector(row.get("sub_sector")),
            "last_close_price": row.get("last_close_price"),
            "latest_close_date": row.get("latest_close_date"),
        }
        normed[_normalize_full(sym)] = out
    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(normed, ensure_ascii=False, indent=2), encoding="utf-8")
    META_JSON.write_text(json.dumps(rows_meta, ensure_ascii=False, indent=2), encoding="utf-8")

# --------------------------
# Remote helpers
# --------------------------
def remote_row_count(table: str) -> Optional[int]:
    """Ambil total row via Content-Range + Prefer: count=exact dengan payload minimum."""
    if not SUPABASE_URL or not SUPABASE_KEY:
        return None
    try:
        url = _build_url(table, "symbol", {"limit": "1"})
        headers = _headers()
        headers["Range"] = "0-0"
        r = requests.get(url, headers=headers, timeout=30)
        if r.status_code not in (200, 206):
            log(f"warn: count({table}) status {r.status_code}: {r.text[:200]}")
            return None
        cr = r.headers.get("Content-Range", "")
        if "/" in cr:
            try:
                return int(cr.split("/")[-1])
            except Exception:
                return None
        return None
    except Exception as e:
        log(f"warn: count({table}) failed: {e}")
        return None

def fetch_report_all() -> Optional[List[Dict[str, Any]]]:
    """Ambil SEMUA kolom yang dibutuhkan dari idx_company_report."""
    if not SUPABASE_URL or not SUPABASE_KEY:
        log("error: SUPABASE_URL/KEY missing; cannot fetch report.")
        return None
    url = _build_url(REPORT_TABLE, REPORT_SELECT, {"limit": "20000"})
    r = requests.get(url, headers=_headers(), timeout=180)
    if r.status_code != 200:
        log(f"error: fetch {REPORT_TABLE} status {r.status_code}: {r.text[:300]}")
        return None
    return r.json() or []

# --------------------------
# Build map (single source)
# --------------------------
def build_map_from_report() -> Optional[Tuple[Dict[str, Dict[str, Any]], Dict[str, Any]]]:
    rows = fetch_report_all()
    if rows is None:
        return None

    mapping: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        raw_sym = (row.get("symbol") or "").upper().strip()
        if not raw_sym:
            continue
        sym = _normalize_full(raw_sym)
        entry = {
            "company_name": (row.get("company_name") or "").strip(),
            "sector": _normalize_sector(row.get("sector")),
            "sub_sector": _normalize_sector(row.get("sub_sector")),
            "last_close_price": _safe_float(row.get("last_close_price")),
            "latest_close_date": row.get("latest_close_date"),
        }
        mapping[sym] = entry

    meta: Dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "report_table": REPORT_TABLE,
        "row_count": len(mapping),
        "row_count_report": None,  # akan diisi di get_company_map()
        "checksum": _checksum(mapping),
        "source": "idx_company_report",
        "columns": REPORT_SELECT,
    }
    return mapping, meta

def _safe_float(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        if isinstance(v, (int, float)):
            return float(v)
        s = str(v).strip()
        if not s:
            return None
        return float(s.replace(",", ""))  # antisipasi format "1,234.56"
    except Exception:
        return None

# --------------------------
# Main logic (single-source invalidation)
# --------------------------
def get_company_map(force: bool = False) -> Dict[str, Dict[str, Any]]:
    local_map, meta = load_local()

    # Jika tidak ada Supabase env: pakai cache lokal bila ada
    if not (SUPABASE_URL and SUPABASE_KEY):
        if local_map:
            log("No Supabase env; using local company_map cache.")
            return local_map
        log("No Supabase env and no local cache.")
        return {}

    # Force?
    if force or FORCE_REFRESH:
        log("FORCE refresh requested for company_map.")
        fresh = build_map_from_report()
        if fresh:
            mapping, new_meta = fresh
            rc_r = remote_row_count(REPORT_TABLE)
            new_meta["row_count_report"] = rc_r
            save_local(mapping, new_meta)
            return mapping
        if ALLOW_OFFLINE and local_map:
            log("Using cached company_map (force refresh failed).")
            return local_map
        log("No company_map available.")
        return {}

    # Invalidasi ringan: bandingkan jumlah baris
    rc_report  = remote_row_count(REPORT_TABLE)
    prev_rc_report  = meta.get("row_count_report")

    if local_map and rc_report is not None and prev_rc_report is not None and int(rc_report) == int(prev_rc_report):
        return local_map

    fresh = build_map_from_report()
    if fresh:
        mapping, new_meta = fresh
        new_meta["row_count_report"]  = rc_report
        save_local(mapping, new_meta)
        return mapping

    if ALLOW_OFFLINE and local_map:
        log("Using cached company_map (refresh failed).")
        return local_map

    log("No company_map available.")
    return {}

# --------------------------
# Utilities & CLI
# --------------------------
def _safe_unlink(path: pathlib.Path):
    try:
        if path.exists():
            path.unlink()
            log(f"deleted: {path}")
    except Exception as e:
        log(f"warn: failed delete {path}: {e}")

def reset_all():
    """Bersihkan cache lokal."""
    _safe_unlink(OUT_JSON)
    _safe_unlink(META_JSON)
    log("reset done.")

def _cmd_get():
    m = get_company_map(force=False)
    log(f"company_map loaded. Rows: {len(m)}")

def _cmd_refresh():
    m = get_company_map(force=True)
    print(f"Refreshed company_map.json. Rows: {len(m)}")

def _cmd_print():
    if not OUT_JSON.exists():
        print("{}")
        return
    print(OUT_JSON.read_text(encoding="utf-8"))

def _cmd_status():
    mapping, meta = load_local()
    payload = {
        "local_rows": len(mapping),
        "local_meta": meta,
        "env_present": bool(SUPABASE_URL) and bool(SUPABASE_KEY),
        "report_table": REPORT_TABLE,
        "out_paths": {
            "company_map": str(OUT_JSON),
            "company_meta": str(META_JSON),
        },
        "present": {
            "company_map": OUT_JSON.exists(),
            "company_meta": META_JSON.exists(),
        }
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))

def _cmd_reset():
    reset_all()

def _build_argparser():
    import argparse
    ap = argparse.ArgumentParser(description="Company map (single-source: idx_company_report).")
    sub = ap.add_subparsers(dest="cmd")

    sub.add_parser("get")
    sub.add_parser("print")
    sub.add_parser("status")
    sub.add_parser("refresh")
    sub.add_parser("reset")

    return ap

if __name__ == "__main__":
    ap = _build_argparser()
    args = ap.parse_args()
    cmd = (args.cmd or "get").lower()

    if cmd == "get":
        _cmd_get()
    elif cmd == "refresh":
        _cmd_refresh()
    elif cmd == "reset":
        _cmd_reset()
    elif cmd == "status":
        _cmd_status()
    elif cmd == "print":
        _cmd_print()
    else:
        print("Commands: get | refresh | reset | status | print", file=sys.stderr)
        sys.exit(2)
