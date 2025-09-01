#!/usr/bin/env python3
# src/scripts/company_map_hybrid.py
# Build a local cache: symbol -> { company_name, sector, sub_sector }
from __future__ import annotations
import os, sys, json, urllib.parse, pathlib, hashlib
from datetime import datetime, timezone
from typing import Dict, Any, Optional, Tuple, List

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

import requests  # pip install requests

OUT_JSON  = pathlib.Path("data/company/company_map.json")
META_JSON = pathlib.Path("data/company/company_map.meta.json")

SUPABASE_URL  = os.getenv("SUPABASE_URL")
SUPABASE_KEY  = os.getenv("SUPABASE_KEY") or os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_ANON_KEY")
SCHEMA_NAME   = os.getenv("COMPANY_SCHEMA", "public")

# Sumber data:
PROFILE_TABLE = os.getenv("COMPANY_PROFILE_TABLE", "idx_company_profile")  # basis: symbol + company_name
REPORT_TABLE  = os.getenv("COMPANY_REPORT_TABLE",  "idx_company_report")   # overlay: sector + sub_sector

# Flags/opsi
ALLOW_OFFLINE = (os.getenv("COMPANY_MAP_ALLOW_OFFLINE", "1").lower() in ("1","true","yes","y"))
FORCE_REFRESH = (os.getenv("COMPANY_MAP_FORCE_REFRESH", "0").lower() in ("1","true","yes","y"))

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

def _build_url(table: str, select: str, limit: int = 20000) -> str:
    base = f"{SUPABASE_URL}/rest/v1/{table}"
    params = {"select": select, "limit": str(limit)}
    return f"{base}?{urllib.parse.urlencode(params)}"

def _checksum(d: Dict[str, Dict[str, str]]) -> str:
    parts: List[str] = []
    for k in sorted(d.keys()):
        v = d[k] or {}
        parts.append(f"{k}|{v.get('company_name','')}|{v.get('sector','')}|{v.get('sub_sector','')}")
    s = "\n".join(parts)
    import hashlib as _hl
    return "sha256:" + _hl.sha256(s.encode("utf-8")).hexdigest()

# ---------- local cache IO ----------
def load_local() -> Tuple[Dict[str, Dict[str, str]], Dict[str, Any]]:
    mapping: Dict[str, Dict[str, str]] = {}
    meta: Dict[str, Any] = {}
    try:
        if OUT_JSON.exists():
            raw = json.loads(OUT_JSON.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                for sym, row in raw.items():
                    if isinstance(row, dict):
                        mapping[sym.upper()] = {
                            "company_name": row.get("company_name", ""),
                            "sector": row.get("sector", ""),
                            "sub_sector": row.get("sub_sector", ""),
                        }
            elif isinstance(raw, list):
                for row in raw:
                    if isinstance(row, dict):
                        sym = (row.get("symbol") or "").upper()
                        if sym:
                            mapping[sym] = {
                                "company_name": row.get("company_name", ""),
                                "sector": row.get("sector", ""),
                                "sub_sector": row.get("sub_sector", ""),
                            }
    except Exception as e:
        log(f"warn: failed reading {OUT_JSON}: {e}")
    try:
        if META_JSON.exists():
            meta = json.loads(META_JSON.read_text(encoding="utf-8"))
    except Exception as e:
        log(f"warn: failed reading {META_JSON}: {e}")
    return mapping, meta

def save_local(mapping: Dict[str, Dict[str, str]], rows_meta: Dict[str, Any]):
    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(mapping, ensure_ascii=False, indent=2), encoding="utf-8")
    META_JSON.write_text(json.dumps(rows_meta, ensure_ascii=False, indent=2), encoding="utf-8")

# ---------- remote row count (tanpa updated_on/at) ----------
def remote_row_count(table: str) -> Optional[int]:
    """Ambil total row via Content-Range + Prefer: count=exact dengan payload minimum."""
    if not SUPABASE_URL or not SUPABASE_KEY:
        return None
    try:
        url = _build_url(table, "symbol", limit=1)  # limit kecil supaya ringan
        headers = _headers()
        headers["Range"] = "0-0"  # minta 1 row saja; total ada di Content-Range
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

# ---------- fetchers (data penuh) ----------
def fetch_profile() -> Optional[Dict[str, Dict[str, str]]]:
    if not SUPABASE_URL or not SUPABASE_KEY:
        log("error: SUPABASE_URL/KEY missing; cannot fetch profile.")
        return None
    url = _build_url(PROFILE_TABLE, "symbol,company_name")
    r = requests.get(url, headers=_headers(), timeout=120)
    if r.status_code != 200:
        log(f"error: fetch {PROFILE_TABLE} status {r.status_code}: {r.text[:300]}")
        return None
    rows = r.json()
    out: Dict[str, Dict[str, str]] = {}
    for row in rows:
        sym = (row.get("symbol") or "").upper().strip()
        if not sym:
            continue
        out[sym] = {
            "company_name": (row.get("company_name") or "").strip(),
            "sector": "",
            "sub_sector": "",
        }
    return out

def fetch_report() -> Optional[Dict[str, Dict[str, str]]]:
    if not SUPABASE_URL or not SUPABASE_KEY:
        log("error: SUPABASE_URL/KEY missing; cannot fetch report.")
        return None
    url = _build_url(REPORT_TABLE, "symbol,sector,sub_sector")
    r = requests.get(url, headers=_headers(), timeout=120)
    if r.status_code != 200:
        log(f"error: fetch {REPORT_TABLE} status {r.status_code}: {r.text[:300]}")
        return None
    rows = r.json()
    out: Dict[str, Dict[str, str]] = {}
    for row in rows:
        sym = (row.get("symbol") or "").upper().strip()
        if not sym:
            continue
        out[sym] = {
            "sector": (row.get("sector") or "").strip(),
            "sub_sector": (row.get("sub_sector") or "").strip(),
        }
    return out

# ---------- build map: PROFILE base + REPORT overlay sectors ----------
def build_map_from_remote() -> Optional[Tuple[Dict[str, Dict[str, str]], Dict[str, Any]]]:
    base = fetch_profile()
    if base is None:
        return None
    overlay = fetch_report() or {}

    for sym, sec in overlay.items():
        if sym in base:
            if sec.get("sector"):
                base[sym]["sector"] = sec["sector"]
            if sec.get("sub_sector"):
                base[sym]["sub_sector"] = sec["sub_sector"]
        # jika ada di report tapi tidak di profile → abaikan (basis = profile)

    meta: Dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "profile_table": PROFILE_TABLE,
        "report_table": REPORT_TABLE,
        "row_count": len(base),             # total symbol di cache (dari profile)
        "row_count_profile": None,          # akan diisi di luar
        "row_count_report": None,           # akan diisi di luar
        "checksum": _checksum(base),
    }
    return base, meta

# ---------- main hybrid logic: pakai row_count untuk invalidasi ----------
def get_company_map(force: bool = False) -> Dict[str, Dict[str, str]]:
    local_map, meta = load_local()

    # jika tidak ada env Supabase → pakai cache lokal kalau ada
    if not (SUPABASE_URL and SUPABASE_KEY):
        if local_map:
            log("No Supabase env; using local cache.")
            return local_map
        log("No Supabase env and no local cache.")
        return {}

    if force or FORCE_REFRESH:
        log("FORCE refresh requested.")
        fresh = build_map_from_remote()
        if fresh:
            mapping, new_meta = fresh
            # ambil row_count terbaru dari server untuk disimpan
            rc_p = remote_row_count(PROFILE_TABLE)
            rc_r = remote_row_count(REPORT_TABLE)
            new_meta["row_count_profile"] = rc_p
            new_meta["row_count_report"] = rc_r
            save_local(mapping, new_meta)
            return mapping
        if ALLOW_OFFLINE and local_map:
            log("Using cached map (force refresh failed).")
            return local_map
        log("No map available.")
        return {}

    # --- non-force path: cek row_count dulu ---
    rc_profile = remote_row_count(PROFILE_TABLE)
    rc_report  = remote_row_count(REPORT_TABLE)

    prev_rc_profile = meta.get("row_count_profile")
    prev_rc_report  = meta.get("row_count_report")

    # Jika row_count tidak berubah dan cache ada → pakai cache
    if (
        local_map
        and rc_profile is not None
        and rc_report is not None
        and prev_rc_profile is not None
        and prev_rc_report is not None
        and int(rc_profile) == int(prev_rc_profile)
        and int(rc_report) == int(prev_rc_report)
    ):
        return local_map

    # Else, fetch ulang
    fresh = build_map_from_remote()
    if fresh:
        mapping, new_meta = fresh
        new_meta["row_count_profile"] = rc_profile
        new_meta["row_count_report"]  = rc_report
        save_local(mapping, new_meta)
        return mapping

    # Kalau fetch gagal tapi boleh offline → pakai cache lama jika ada
    if ALLOW_OFFLINE and local_map:
        log("Using cached map (refresh failed).")
        return local_map

    log("No map available.")
    return {}

# ------------- CLI -------------
def _cmd_get():
    m = get_company_map(force=False)
    # print(json.dumps(m, ensure_ascii=False, indent=2))

def _cmd_refresh():
    m = get_company_map(force=True)
    print(f"Refreshed. Rows: {len(m)}")

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
        "profile_table": PROFILE_TABLE,
        "report_table": REPORT_TABLE,
        "out_paths": {"json": str(OUT_JSON), "meta": str(META_JSON)},
    }
    # print(json.dumps(payload, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    cmd = (sys.argv[1] if len(sys.argv) > 1 else "get").lower()
    if cmd == "get":
        _cmd_get()
    elif cmd == "refresh":
        _cmd_refresh()
    elif cmd == "status":
        _cmd_status()
    elif cmd == "print":
        _cmd_print()
    else:
        print("Commands: get | refresh | status | print", file=sys.stderr)
        sys.exit(2)
