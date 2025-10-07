# src/utils/company_map_hybrid.py
# -*- coding: utf-8 -*-

"""
Build & refresh local caches:
 - company_map.json        : symbol -> { company_name, sector, sub_sector }
 - latest_prices.json      : { "prices": { SYMBOL.JK: { "close": float, "date": "YYYY-MM-DD" } } }
Bonus:
 - company_map.hydrated.json (opsional) menggabungkan map + harga untuk inspeksi.

Perbaikan:
 - Normalisasi sector/sub_sector menjadi string konsisten (title-case), baik saat load cache,
   fetch dari remote, maupun saat merge—mencegah format dict/JSON atau slug nyasar ke downstream.

Tidak butuh perubahan schema Supabase.
"""

from __future__ import annotations
import os
import sys
import json
import urllib.parse
import pathlib
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional, Tuple, List, Iterable

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

import requests

# Outputs
OUT_JSON            = pathlib.Path("data/company/company_map.json")
META_JSON           = pathlib.Path("data/company/company_map.meta.json")
LATEST_PRICES_JSON  = pathlib.Path("data/company/latest_prices.json")
HYDRATED_JSON       = pathlib.Path("data/company/company_map.hydrated.json")

# Supabase env
SUPABASE_URL  = os.getenv("SUPABASE_URL")
SUPABASE_KEY  = os.getenv("SUPABASE_KEY") or os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_ANON_KEY")
SCHEMA_NAME   = os.getenv("COMPANY_SCHEMA", "public")

# Tables
PROFILE_TABLE = os.getenv("COMPANY_PROFILE_TABLE", "idx_company_report")   # symbol, company_name
REPORT_TABLE  = os.getenv("COMPANY_REPORT_TABLE",  "idx_company_report")    # symbol, sector, sub_sector
PRICES_TABLE  = os.getenv("PRICES_TABLE",          "idx_daily_data")        # symbol, date, close, updated_on

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

def _checksum(d: Dict[str, Dict[str, str]]) -> str:
    parts: List[str] = []
    for k in sorted(d.keys()):
        v = d[k] or {}
        parts.append(f"{k}|{v.get('company_name','')}|{v.get('sector','')}|{v.get('sub_sector','')}")
    import hashlib as _hl  # local import
    return "sha256:" + _hl.sha256("\n".join(parts).encode("utf-8")).hexdigest()

def _normalize_full(sym: str) -> str:
    s = (sym or "").upper().strip()
    return s if s.endswith(".JK") else f"{s}.JK"


# --------------------------
# Sector/sub_sector normalizers
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
        # best-effort: kalau kelihatan JSON-ish {"name":"X"} tapi gak valid, coba culik value-nya
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
    # fallback ke str() untuk tipe lain
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

def coalesce_sector_values(sector: Any, sub_sector: Any) -> Tuple[Optional[str], Optional[str]]:
    s = _extract_str(sector)
    ss = _extract_str(sub_sector)
    s = _titlecase_preserve(s) if s else None
    ss = _titlecase_preserve(ss) if ss else None
    return s, ss

def normalize_sector_entry(entry: Dict[str, Any]) -> Dict[str, Any]:
    """Mutasi + kembalikan dict dengan sector/sub_sector yang sudah normal."""
    if not isinstance(entry, dict):
        return entry
    s, ss = coalesce_sector_values(entry.get("sector"), entry.get("sub_sector"))
    entry["sector"] = s or ""
    entry["sub_sector"] = ss or ""
    return entry


# --------------------------
# Local cache IO (dengan normalisasi)
# --------------------------

def load_local() -> Tuple[Dict[str, Dict[str, str]], Dict[str, Any]]:
    mapping: Dict[str, Dict[str, str]] = {}
    meta: Dict[str, Any] = {}
    try:
        if OUT_JSON.exists():
            raw = json.loads(OUT_JSON.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                for sym, row in raw.items():
                    if isinstance(row, dict):
                        symu = (sym or "").upper().strip()
                        if not symu:
                            continue
                        entry = {
                            "company_name": (row.get("company_name") or "").strip(),
                            "sector": row.get("sector"),
                            "sub_sector": row.get("sub_sector"),
                        }
                        entry = normalize_sector_entry(entry)
                        mapping[_normalize_full(symu)] = entry
            elif isinstance(raw, list):
                for row in raw:
                    if isinstance(row, dict):
                        symu = (row.get("symbol") or "").upper().strip()
                        if not symu:
                            continue
                        entry = {
                            "company_name": (row.get("company_name") or "").strip(),
                            "sector": row.get("sector"),
                            "sub_sector": row.get("sub_sector"),
                        }
                        entry = normalize_sector_entry(entry)
                        mapping[_normalize_full(symu)] = entry
    except Exception as e:
        log(f"warn: failed reading {OUT_JSON}: {e}")
    try:
        if META_JSON.exists():
            meta = json.loads(META_JSON.read_text(encoding="utf-8"))
    except Exception as e:
        log(f"warn: failed reading {META_JSON}: {e}")
    return mapping, meta

def save_local(mapping: Dict[str, Dict[str, str]], rows_meta: Dict[str, Any]):
    # pastikan semua entry ter-normalisasi sebelum simpan
    normed: Dict[str, Dict[str, str]] = {}
    for sym, row in mapping.items():
        out = {
            "company_name": (row.get("company_name") or "").strip(),
            "sector": row.get("sector"),
            "sub_sector": row.get("sub_sector"),
        }
        normed[sym] = normalize_sector_entry(out)
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

def fetch_profile() -> Optional[Dict[str, Dict[str, str]]]:
    if not SUPABASE_URL or not SUPABASE_KEY:
        log("error: SUPABASE_URL/KEY missing; cannot fetch profile.")
        return None
    url = _build_url(PROFILE_TABLE, "symbol,company_name", {"limit": "20000"})
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
        out[_normalize_full(sym)] = {
            "company_name": (row.get("company_name") or "").strip(),
            "sector": "",
            "sub_sector": "",
        }
    return out

def fetch_report() -> Optional[Dict[str, Dict[str, str]]]:
    if not SUPABASE_URL or not SUPABASE_KEY:
        log("error: SUPABASE_URL/KEY missing; cannot fetch report.")
        return None
    url = _build_url(REPORT_TABLE, "symbol,sector,sub_sector", {"limit": "20000"})
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
        entry = {
            "sector": row.get("sector"),
            "sub_sector": row.get("sub_sector"),
        }
        entry = normalize_sector_entry(entry)  # <-- NORMALISASI DI SINI
        out[_normalize_full(sym)] = entry
    return out


# --------------------------
# Build map: PROFILE base + REPORT overlay sectors
# --------------------------

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
            # pastikan hasil akhir normalized (idempotent, harmless)
            base[sym] = normalize_sector_entry(base[sym])

    # juga normalize semua row yang mungkin belum terisi sector/sub_sector
    for sym in list(base.keys()):
        base[sym] = normalize_sector_entry(base[sym])

    meta: Dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "profile_table": PROFILE_TABLE,
        "report_table": REPORT_TABLE,
        "row_count": len(base),
        "row_count_profile": None,
        "row_count_report": None,
        "checksum": _checksum(base),
    }
    return base, meta


# --------------------------
# Prices
# --------------------------

def fetch_latest_price_for_symbol(sym_full: str) -> Optional[Dict[str, Any]]:
    if not SUPABASE_URL or not SUPABASE_KEY:
        return None
    url = _build_url(
        PRICES_TABLE,
        "date,close,updated_on",
        {"symbol": f"eq.{sym_full}", "order": "date.desc,updated_on.desc", "limit": "1"}
    )
    r = requests.get(url, headers=_headers(), timeout=30)
    if r.status_code != 200:
        return None
    rows = r.json()
    if not rows:
        return None
    row = rows[0]
    try:
        return {"close": float(row["close"]), "date": row.get("date")}
    except Exception:
        return None

def _fetch_recent_rows_all(start_date: str, page_size: int = 20000) -> List[dict]:
    """Tarik semua baris date >= start_date (paged). Order: symbol ASC, date DESC, updated_on DESC."""
    if not SUPABASE_URL or not SUPABASE_KEY:
        return []
    url = _build_url(
        PRICES_TABLE,
        "symbol,date,close,updated_on",
        {"date": f"gte.{start_date}", "order": "symbol.asc,date.desc,updated_on.desc"}
    )
    headers = _headers()
    out: List[dict] = []
    offset = 0
    while True:
        headers["Range"] = f"{offset}-{offset + page_size - 1}"
        r = requests.get(url, headers=headers, timeout=90)
        if r.status_code not in (200, 206):
            log(f"warn: fetch recent rows status {r.status_code}: {r.text[:200]}")
            break
        rows = r.json() or []
        if not rows:
            break
        out.extend(rows)
        if len(rows) < page_size:
            break
        offset += page_size
    return out

def _latest_per_symbol_from_rows(rows: Iterable[dict]) -> Dict[str, Dict[str, Any]]:
    """Ambil kemunculan pertama per symbol → terbaru (rows sudah terurut global)."""
    latest: Dict[str, Dict[str, Any]] = {}
    last_sym = None
    for row in rows:
        sym = (row.get("symbol") or "").upper().strip()
        if not sym:
            continue
        if sym != last_sym:
            if sym not in latest:
                try:
                    latest[sym] = {"close": float(row.get("close") or 0), "date": row.get("date")}
                except Exception:
                    pass
            last_sym = sym
    return latest

def refresh_latest_prices_program_only(symbols: List[str] | None = None,
                                      lookback_days: int = 7,
                                      use_fallback: bool = True) -> Dict[str, Dict[str, Any]]:
    """Bulk + group; lalu fallback per-simbol untuk yang miss."""
    start_date = (datetime.utcnow().date() - timedelta(days=lookback_days)).isoformat()
    rows = _fetch_recent_rows_all(start_date)
    latest_all = _latest_per_symbol_from_rows(rows)

    if symbols:
        targets = {_normalize_full(s) for s in symbols}
        found: Dict[str, Dict[str, Any] | None] = {}
        for s in targets:
            found[s] = latest_all.get(s) or latest_all.get(s.upper()) \
                       or latest_all.get(s.replace(".JK", "").upper() + ".JK")
        missing = [s for s, rec in found.items() if not rec]

        if use_fallback and missing:
            for m in missing:
                rec = fetch_latest_price_for_symbol(m)
                if rec:
                    found[m] = rec

        prices = {k: v for k, v in found.items() if v}
    else:
        prices = latest_all

    payload = {
        "updated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "prices": prices
    }
    LATEST_PRICES_JSON.parent.mkdir(parents=True, exist_ok=True)
    LATEST_PRICES_JSON.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )
    log(f"Saved latest prices (program-only bulk): {LATEST_PRICES_JSON} ({len(prices)} symbols)")
    return prices


# --------------------------
# Main hybrid logic: invalidasi map pakai row_count
# --------------------------

def get_company_map(force: bool = False) -> Dict[str, Dict[str, str]]:
    local_map, meta = load_local()

    if not (SUPABASE_URL and SUPABASE_KEY):
        if local_map:
            log("No Supabase env; using local company_map cache.")
            return local_map
        log("No Supabase env and no local cache.")
        return {}

    if force or FORCE_REFRESH:
        log("FORCE refresh requested for company_map.")
        fresh = build_map_from_remote()
        if fresh:
            mapping, new_meta = fresh
            rc_p = remote_row_count(PROFILE_TABLE)
            rc_r = remote_row_count(REPORT_TABLE)
            new_meta["row_count_profile"] = rc_p
            new_meta["row_count_report"] = rc_r
            save_local(mapping, new_meta)
            return mapping
        if ALLOW_OFFLINE and local_map:
            log("Using cached company_map (force refresh failed).")
            return local_map
        log("No company_map available.")
        return {}

    rc_profile = remote_row_count(PROFILE_TABLE)
    rc_report  = remote_row_count(REPORT_TABLE)
    prev_rc_profile = meta.get("row_count_profile")
    prev_rc_report  = meta.get("row_count_report")

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

    fresh = build_map_from_remote()
    if fresh:
        mapping, new_meta = fresh
        new_meta["row_count_profile"] = rc_profile
        new_meta["row_count_report"]  = rc_report
        save_local(mapping, new_meta)
        return mapping

    if ALLOW_OFFLINE and local_map:
        log("Using cached company_map (refresh failed).")
        return local_map

    log("No company_map available.")
    return {}


# --------------------------
# Utilities: hydrate, bootstrap, reset, CLI
# --------------------------

def hydrate_company_map_with_prices():
    """Gabungkan company_map + latest_prices ke satu file untuk inspeksi."""
    mapping, _ = load_local()
    prices_obj: Dict[str, Any] = {}
    if LATEST_PRICES_JSON.exists():
        try:
            p = json.loads(LATEST_PRICES_JSON.read_text(encoding="utf-8"))
            prices_obj = p.get("prices", {}) if isinstance(p, dict) else {}
        except Exception:
            prices_obj = {}

    hydrated: Dict[str, Any] = {}
    for sym, info in mapping.items():
        pr = prices_obj.get(sym) or prices_obj.get(sym.upper())
        hydrated[sym] = {
            **info,
            "latest_price": pr or None,
        }

    HYDRATED_JSON.parent.mkdir(parents=True, exist_ok=True)
    HYDRATED_JSON.write_text(json.dumps(hydrated, ensure_ascii=False, indent=2), encoding="utf-8")
    log(f"Wrote hydrated map: {HYDRATED_JSON} ({len(hydrated)} symbols)")
    return hydrated

def bootstrap_all(lookback_days: int = 7, use_fallback: bool = True):
    """
    First run helper:
      1) build company_map.json (force)
      2) extract symbols
      3) refresh latest_prices.json for those symbols
      4) (optional) produce hydrated map
    """
    mapping = get_company_map(force=True)
    symbols = sorted(mapping.keys())
    refresh_latest_prices_program_only(symbols=symbols, lookback_days=lookback_days, use_fallback=use_fallback)
    hydrate_company_map_with_prices()

def _safe_unlink(path: pathlib.Path):
    try:
        if path.exists():
            path.unlink()
            log(f"deleted: {path}")
    except Exception as e:
        log(f"warn: failed delete {path}: {e}")

def reset_all(no_bootstrap: bool = False, lookback_days: int = 7, use_fallback: bool = True):
    """
    Bersihkan semua cache local & (default) rebuild dari nol.
    """
    _safe_unlink(OUT_JSON)
    _safe_unlink(META_JSON)
    _safe_unlink(LATEST_PRICES_JSON)
    _safe_unlink(HYDRATED_JSON)

    if no_bootstrap:
        log("reset done (no bootstrap).")
        return

    bootstrap_all(lookback_days=lookback_days, use_fallback=use_fallback)

# CLI
def _cmd_get():
    _ = get_company_map(force=False)

def _cmd_refresh():
    m = get_company_map(force=True)
    print(f"Refreshed company_map.json. Rows: {len(m)}")

def _cmd_refresh_prices(args):
    mapping, _ = load_local()
    if not mapping:
        log("company_map.json empty. Refreshing map first...")
        mapping = get_company_map(force=True)
    symbols = sorted(mapping.keys())
    refresh_latest_prices_program_only(
        symbols=symbols,
        lookback_days=args.prices_lookback_days,
        use_fallback=not args.no_fallback
    )

def _cmd_refresh_all(args):
    _cmd_refresh()
    _cmd_refresh_prices(args)

def _cmd_hydrate():
    hydrate_company_map_with_prices()

def _cmd_bootstrap(args):
    bootstrap_all(lookback_days=args.prices_lookback_days, use_fallback=not args.no_fallback)

def _cmd_reset(args):
    reset_all(
        no_bootstrap=args.no_bootstrap,
        lookback_days=args.prices_lookback_days,
        use_fallback=not args.no_fallback
    )

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
        "prices_table": PRICES_TABLE,
        "out_paths": {
            "company_map": str(OUT_JSON),
            "company_meta": str(META_JSON),
            "latest_prices": str(LATEST_PRICES_JSON),
            "hydrated": str(HYDRATED_JSON),
        },
        "latest_prices_present": LATEST_PRICES_JSON.exists(),
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))

def _build_argparser():
    import argparse
    ap = argparse.ArgumentParser(description="Hybrid company map & latest prices refresher (program-only).")
    sub = ap.add_subparsers(dest="cmd")

    sub.add_parser("get")
    sub.add_parser("print")
    sub.add_parser("status")
    sub.add_parser("refresh")
    sub.add_parser("hydrate")

    p_prices = sub.add_parser("refresh_prices")
    p_prices.add_argument("--prices-lookback-days", type=int, default=int(os.getenv("PRICES_LOOKBACK_DAYS", "7")),
                          help="Look back this many days for latest prices (default 7).")
    p_prices.add_argument("--no-fallback", action="store_true",
                          help="Disable per-symbol fallback for symbols missing in lookback window.")

    p_all = sub.add_parser("refresh_all")
    p_all.add_argument("--prices-lookback-days", type=int, default=int(os.getenv("PRICES_LOOKBACK_DAYS", "7")))
    p_all.add_argument("--no-fallback", action="store_true")

    p_boot = sub.add_parser("bootstrap")
    p_boot.add_argument("--prices-lookback-days", type=int, default=int(os.getenv("PRICES_LOOKBACK_DAYS", "7")))
    p_boot.add_argument("--no-fallback", action="store_true")

    p_reset = sub.add_parser("reset")
    p_reset.add_argument("--no-bootstrap", action="store_true", help="Hanya bersihkan cache tanpa rebuild.")
    p_reset.add_argument("--prices-lookback-days", type=int, default=int(os.getenv("PRICES_LOOKBACK_DAYS", "7")))
    p_reset.add_argument("--no-fallback", action="store_true")

    return ap

if __name__ == "__main__":
    ap = _build_argparser()
    args = ap.parse_args()
    cmd = (args.cmd or "get").lower()

    if cmd == "get":
        _cmd_get()
    elif cmd == "refresh":
        _cmd_refresh()
    elif cmd == "refresh_prices":
        _cmd_refresh_prices(args)
    elif cmd == "refresh_all":
        _cmd_refresh_all(args)
    elif cmd == "hydrate":
        _cmd_hydrate()
    elif cmd == "bootstrap":
        _cmd_bootstrap(args)
    elif cmd == "reset":
        _cmd_reset(args)
    elif cmd == "status":
        _cmd_status()
    elif cmd == "print":
        _cmd_print()
    else:
        print("Commands: get | refresh | refresh_prices | refresh_all | hydrate | bootstrap | reset | status | print", file=sys.stderr)
        sys.exit(2)
