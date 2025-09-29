from __future__ import annotations

import os
import sys
import csv
import json
import logging
import urllib.parse
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

try:
    from dotenv import load_dotenv  
    load_dotenv()
except Exception:
    pass

import requests

# Config 
@dataclass
class Cfg:
    url: str = os.getenv("SUPABASE_URL", "").rstrip("/")
    key: str = (
        os.getenv("SUPABASE_KEY")
        or os.getenv("SUPABASE_SERVICE_ROLE_KEY")
        or os.getenv("SUPABASE_ANON_KEY")
        or ""
    )
    schema: str = os.getenv("COMPANY_SCHEMA", "public")
    table: str = "idx_company_report"  
    col_symbol: str = "symbol"
    col_company_name: str = "company_name"
    col_sector: str = "sector"
    col_sub_sector: str = "sub_sector"
    col_tags: str = "tags"
    col_listing_board: str = "listing_board"

    col_report_date: str = ""  
    col_updated_at: str = ""  

    def require(self):
        if not self.url or not self.key:
            raise RuntimeError("SUPABASE_URL/KEY missing; export cannot proceed")

# HTTP helpers 
def _headers(cfg: Cfg) -> Dict[str, str]:
    h = {
        "apikey": cfg.key,
        "Authorization": f"Bearer {cfg.key}",
        "Prefer": "count=exact",
    }
    if cfg.schema and cfg.schema != "public":
        h["Accept-Profile"] = cfg.schema
    return h

def _build_url(cfg: Cfg, table: str, select: str, extra: Optional[Dict[str, str]] = None) -> str:
    base = f"{cfg.url}/rest/v1/{table}"
    params: Dict[str, str] = {"select": select}
    if extra:
        params.update(extra)
    return f"{base}?{urllib.parse.urlencode(params)}"

def _paged_get(cfg: Cfg, url: str, page_size: int = 10000, timeout: int = 60) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    headers = _headers(cfg)
    offset = 0
    while True:
        headers["Range"] = f"{offset}-{offset + page_size - 1}"
        r = requests.get(url, headers=headers, timeout=timeout)
        if r.status_code not in (200, 206):
            raise RuntimeError(f"GET {url} -> {r.status_code}: {r.text[:300]}")
        rows = r.json() or []
        out.extend(rows)
        if len(rows) < page_size:
            break
        offset += page_size
    return out

def export_company_report(
    cfg: Cfg,
    *,
    select_cols: Optional[List[str]] = None,
    symbol_list: Optional[List[str]] = None,
    tags_any: Optional[List[str]] = None,
    since_updated: Optional[str] = None,
    until_updated: Optional[str] = None,
    since_report: Optional[str] = None,
    until_report: Optional[str] = None,
    limit: Optional[int] = None,
    order: Optional[str] = None,
) -> List[Dict[str, Any]]:
    cfg.require()

    selects = select_cols or ["*"]

    filters: Dict[str, str] = {}
    if since_updated and cfg.col_updated_at:
        filters[cfg.col_updated_at] = f"gte.{since_updated}"
    elif since_updated:
        logging.warning("--since-updated diabaikan (kolom 'updated_at' tidak ada di idx_company_report)")

    if until_updated and cfg.col_updated_at:
        filters[cfg.col_updated_at] = f"lte.{until_updated}"
    elif until_updated:
        logging.warning("--until-updated diabaikan (kolom 'updated_at' tidak ada di idx_company_report)")

    if order:
        filters["order"] = order
    if limit:
        filters["limit"] = str(limit)

    url = _build_url(cfg, cfg.table, ",".join(selects), filters)
    logging.debug("export URL=%s", url)
    rows = _paged_get(cfg, url)

    # Client-side filters
    if symbol_list:
        syms_norm = {s.upper().strip() for s in symbol_list if s}
        rows = [r for r in rows if str(r.get(cfg.col_symbol, "")).upper().strip() in syms_norm]

    if tags_any:
        keys = [t.strip().lower() for t in tags_any if t.strip()]
        if keys:
            rows = [r for r in rows if any(k in str(r.get(cfg.col_tags, "")).lower() for k in keys)]

    # Report-date range 
    if (since_report or until_report) and cfg.col_report_date:
        def _in_range(val: Optional[str], lo: Optional[str], hi: Optional[str]) -> bool:
            if not val: return False
            if lo and val < lo: return False
            if hi and val > hi: return False
            return True
        rows = [r for r in rows if _in_range(str(r.get(cfg.col_report_date) or ""), since_report, until_report)]
    elif (since_report or until_report):
        logging.warning("--since-report/--until-report diabaikan (kolom 'report_date' tidak ada di idx_company_report)")

    logging.info("exported rows: %d", len(rows))
    return rows


def write_json(path: str, rows: List[Dict[str, Any]]):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)

def write_csv(path: str, rows: List[Dict[str, Any]]):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    if not rows:
        with open(path, "w", newline="", encoding="utf-8") as f:
            f.write("")
        return
    fieldnames = list(rows[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _build_argparser():
    import argparse
    ap = argparse.ArgumentParser(description="Export idx_company_report via Supabase REST (no ENV column mapping)")
    ap.add_argument("--select", default=None, help="Comma-separated columns (default: '*')")
    ap.add_argument("--all", action="store_true", help="Same as --select '*'")
    ap.add_argument("--symbol", default=None, help="Comma-separated symbols (e.g., BBRI.JK,BBCA.JK)")
    ap.add_argument("--tags", default=None, help="Comma-separated tag substrings (OR filter)")
    ap.add_argument("--since-updated", dest="since_updated", default=None, help="Ignored (no updated_at column)")
    ap.add_argument("--until-updated", dest="until_updated", default=None, help="Ignored (no updated_at column)")
    ap.add_argument("--since-report", dest="since_report", default=None, help="Ignored (no report_date column)")
    ap.add_argument("--until-report", dest="until_report", default=None, help="Ignored (no report_date column)")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--order", default=None, help="e.g., symbol.asc")
    ap.add_argument("--out-json", default=None)
    ap.add_argument("--out-csv", default=None)
    ap.add_argument("--log-level", default="INFO")
    return ap

def main(argv: Optional[List[str]] = None) -> int:
    argv = argv or sys.argv[1:]
    ap = _build_argparser()
    args = ap.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, (args.log_level or "INFO").upper(), logging.INFO),
        format="%(asctime)s | %(levelname)-7s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    cfg = Cfg()

    # select default: '*' 
    if args.all or not args.select:
        select_cols = ["*"]
    else:
        select_cols = [c.strip() for c in args.select.split(",") if c.strip()]

    symbols = [s.strip() for s in args.symbol.split(",")] if args.symbol else None
    tags = [t.strip() for t in args.tags.split(",")] if args.tags else None

    try:
        rows = export_company_report(
            cfg,
            select_cols=select_cols,
            symbol_list=symbols,
            tags_any=tags,
            since_updated=args.since_updated,
            until_updated=args.until_updated,
            since_report=args.since_report,
            until_report=args.until_report,
            limit=args.limit,
            order=args.order,
        )
    except Exception as e:
        logging.exception("export failed: %s", e)
        print(f"error: {e}", file=sys.stderr)
        return 3

    if args.out_json:
        write_json(args.out_json, rows)
        logging.info("wrote JSON: %s", args.out_json)
    if args.out_csv:
        write_csv(args.out_csv, rows)
        logging.info("wrote CSV : %s", args.out_csv)

    if not args.out_json and not args.out_csv:
        print(json.dumps(rows, ensure_ascii=False))

    return 0

if __name__ == "__main__":
    raise SystemExit(main())
