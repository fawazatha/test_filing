#!/usr/bin/env python3
# src/scripts/fetch_filings.py
from __future__ import annotations

"""
Fetch rows from Supabase `idx_filings` for a flexible time window and write them to JSON.

Features
- Time window:
  * --from / --to (ISO). If timezone missing, assumed Asia/Jakarta (+07:00).
  * --use-checkpoint: use last saved end as the next start; first run falls back to (now-24h, now).
  * --working-days-only: skip Sat/Sun.
  * Half-open window (gt start, lt end) to avoid duplicates.
- Column/time-kind:
  * --ts-col <column> (default: inserted_at)  e.g., created_at, inserted_at (timestamptz) OR timestamp (no tz).
  * --ts-kind timestamptz|timestamp (default: timestamptz).
- Symbols filter:
  * --symbols "AAA.JK,BBB.JK"
  * --company-report-json path/to/company_report.json (expects array of objects with "symbol")
- Output:
  * --out path/to/output.json

Env
- SUPABASE_URL, SUPABASE_KEY (loaded via dotenv if present)
"""

import argparse
import asyncio
import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

import httpx
from dotenv import load_dotenv

# --------- Setup ---------
load_dotenv()
logging.basicConfig(
    level=os.environ.get("LOGLEVEL", "INFO"),
    format="%(asctime)s | %(levelname)-7s | %(message)s",
)
logger = logging.getLogger("fetch_idx_filings")

JKT = timezone(timedelta(hours=7))


# =========================
# Supabase REST (standalone)
# =========================
def _sb_base() -> str:
    url = os.getenv("SUPABASE_URL")
    if not url:
        raise RuntimeError("SUPABASE_URL is not set")
    return url.rstrip("/")


def _sb_headers() -> Dict[str, str]:
    key = os.getenv("SUPABASE_KEY")
    if not key:
        raise RuntimeError("SUPABASE_KEY is not set")
    # Prefer count=exact gives Content-Range totals (optional)
    return {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Accept": "application/json",
        "Prefer": "count=exact",
    }


# NOTE: Return a LIST OF (key, value) to allow duplicated keys (e.g., timestamp=gt... & timestamp=lt...)
def _build_query_params(
    *,
    select: str = "*",
    eq: Optional[Dict[str, Any]] = None,
    gte: Optional[Dict[str, Any]] = None,
    lte: Optional[Dict[str, Any]] = None,
    gt: Optional[Dict[str, Any]] = None,
    lt: Optional[Dict[str, Any]] = None,
    ilike: Optional[Dict[str, str]] = None,
    in_: Optional[Dict[str, Iterable[Any]]] = None,
    order: Optional[str] = None,
) -> List[Tuple[str, str]]:
    qs: List[Tuple[str, str]] = [("select", select)]

    def _add(op: str, d: Optional[Dict[str, Any]]):
        if not d:
            return
        for k, v in d.items():
            qs.append((k, f"{op}.{v}"))

    _add("eq", eq)
    _add("gte", gte)
    _add("lte", lte)
    _add("gt", gt)
    _add("lt", lt)

    if ilike:
        for k, v in ilike.items():
            qs.append((k, f"ilike.{v}"))

    if in_:
        for k, vals in in_.items():
            items: List[str] = []
            for v in vals or []:
                s = str(v)
                if ("," in s) or (" " in s):
                    s = f'"{s}"'
                items.append(s)
            qs.append((k, f"in.({','.join(items)})"))

    if order:
        qs.append(("order", order))

    return qs


async def _rest_get(
    table: str,
    *,
    select: str = "*",
    eq: Optional[Dict[str, Any]] = None,
    gte: Optional[Dict[str, Any]] = None,
    lte: Optional[Dict[str, Any]] = None,
    gt: Optional[Dict[str, Any]] = None,
    lt: Optional[Dict[str, Any]] = None,
    ilike: Optional[Dict[str, str]] = None,
    in_: Optional[Dict[str, Iterable[Any]]] = None,
    order: Optional[str] = None,
    range_: Optional[Tuple[int, int]] = None,  # inclusive
    timeout: float = 60.0,
) -> Tuple[List[Dict[str, Any]], Dict[str, str]]:
    base = _sb_base()
    qs = _build_query_params(
        select=select, eq=eq, gte=gte, lte=lte, gt=gt, lt=lt, ilike=ilike, in_=in_, order=order
    )
    url = f"{base}/rest/v1/{table}?{httpx.QueryParams(qs)}"
    headers = _sb_headers()
    if range_:
        start, end = range_
        headers["Range-Unit"] = "items"
        headers["Range"] = f"{start}-{end}"

    async with httpx.AsyncClient(timeout=timeout) as client:
        r = await client.get(url, headers=headers)
        r.raise_for_status()
        return r.json(), {k.lower(): v for k, v in r.headers.items()}


async def _rest_get_all(
    table: str,
    *,
    select: str = "*",
    eq: Optional[Dict[str, Any]] = None,
    gte: Optional[Dict[str, Any]] = None,
    lte: Optional[Dict[str, Any]] = None,
    gt: Optional[Dict[str, Any]] = None,
    lt: Optional[Dict[str, Any]] = None,
    ilike: Optional[Dict[str, str]] = None,
    in_: Optional[Dict[str, Iterable[Any]]] = None,
    order: Optional[str] = None,
    page_size: int = 1000,
    timeout: float = 60.0,
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    start = 0
    while True:
        batch, headers = await _rest_get(
            table,
            select=select,
            eq=eq,
            gte=gte,
            lte=lte,
            gt=gt,
            lt=lt,
            ilike=ilike,
            in_=in_,
            order=order,
            range_=(start, start + page_size - 1),
            timeout=timeout,
        )
        out.extend(batch)
        cr = headers.get("content-range")  # e.g. "0-999/2345"
        if not cr:
            if len(batch) < page_size:
                break
            start += len(batch)
            continue
        try:
            total = int(cr.split("/")[-1])
        except Exception:
            total = None
        start += len(batch)
        if total is None or start >= total or len(batch) == 0:
            break
    return out


# =========================
# High-level fetch (ts-col aware)
# =========================
def _parse_dt_iso(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=JKT)  # assume JKT if naive
    return dt


def _now_jkt() -> datetime:
    return datetime.now(JKT).replace(microsecond=0)


def _to_utc_z(dt: datetime) -> str:
    """UTC ISO 8601 ending with Z (no microseconds)."""
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _fmt_for_ts_kind(dt: datetime, ts_kind: str) -> str:
    """
    If column is timestamptz: send UTC Z.
    If column is timestamp (no tz): send local JKT 'YYYY-MM-DD HH:MM:SS'.
    """
    if ts_kind == "timestamptz":
        return _to_utc_z(dt)
    return dt.astimezone(JKT).strftime("%Y-%m-%d %H:%M:%S")


async def get_idx_filings_between(
    ts_col: str,
    ts_kind: str,
    start_dt: datetime,
    end_dt: datetime,
    *,
    symbols: Optional[Iterable[str]] = None,
    table: str = "idx_filings",
    select: str = "*",  # keep simple; change if you want a fixed column list
) -> List[Dict[str, Any]]:
    """
    Fetch rows from `table` where ts_col is (gt start_dt) and (lt end_dt), ordered by ts_col then id.
    """
    start_val = _fmt_for_ts_kind(start_dt, ts_kind)
    end_val = _fmt_for_ts_kind(end_dt, ts_kind)
    in_filter = {"symbol": [str(s).strip().upper() for s in symbols]} if symbols else None

    return await _rest_get_all(
        table,
        select=select,
        gt={ts_col: start_val},   # half-open window
        lt={ts_col: end_val},
        in_=in_filter,
        order=f"{ts_col}.asc,id.asc",
    )


# =========================
# Local helpers (checkpoint, IO)
# =========================
def _ensure_parent(path: str) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)


def _load_checkpoint(path: str) -> Optional[datetime]:
    p = Path(path)
    if not p.exists():
        return None
    try:
        obj = json.loads(p.read_text(encoding="utf-8"))
        s = obj.get("last_window_end")
        if not s:
            return None
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=JKT)
        return dt
    except Exception:
        return None


def _save_checkpoint(path: str, window_end: datetime) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    payload = {"last_window_end": window_end.isoformat()}
    Path(path).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _merge_symbols(symbols_arg: Optional[str], company_report_json: Optional[str]) -> List[str]:
    symset: Set[str] = set()

    if symbols_arg:
        for s in symbols_arg.split(","):
            t = s.strip().upper()
            if t:
                symset.add(t)

    if company_report_json:
        try:
            data = json.loads(Path(company_report_json).read_text(encoding="utf-8"))
            if isinstance(data, list):
                for row in data:
                    sym = str(row.get("symbol", "")).strip().upper()
                    if sym:
                        symset.add(sym)
            else:
                logger.warning("company_report_json is not a JSON array; ignoring.")
        except Exception as e:
            logger.error("Failed reading company_report_json: %s", e)

    return sorted(symset)


# =========================
# CLI & window resolution
# =========================
def build_argparser():
    p = argparse.ArgumentParser(description="Fetch idx_filings for a time window and write to JSON.")

    # Window options
    p.add_argument("--from", dest="from_iso", help="Start datetime (ISO). If omitted, uses checkpoint or (now-24h).")
    p.add_argument("--to", dest="to_iso", help="End datetime (ISO). If omitted, uses 'now' (JKT).")
    p.add_argument("--use-checkpoint", action="store_true", help="Use checkpoint as 'from' if --from not given.")
    p.add_argument("--checkpoint", default="data/reports/.filings_checkpoint.json", help="Checkpoint path.")
    p.add_argument("--working-days-only", action="store_true", help="Skip Sat/Sun.")

    # Time column config
    p.add_argument("--ts-col", default="inserted_at",
                   help="Time column to filter: e.g. inserted_at|created_at (timestamptz) or timestamp (no tz).")
    p.add_argument("--ts-kind", default="timestamptz", choices=["timestamptz", "timestamp"],
                   help="Column type: 'timestamptz' (UTC Z) or 'timestamp' (no timezone).")

    # Symbol universe
    p.add_argument("--symbols", help="Comma-separated symbols, e.g. 'BBRI.JK,TLKM.JK'.")
    p.add_argument("--company-report-json", help="Path to company_report_YYYY-MM.json to derive symbols.")

    # Output
    p.add_argument("--out", default="data/reports/today_filings.json", help="Output JSON path.")
    return p


@dataclass
class Window:
    start: datetime
    end: datetime


def resolve_window(
    from_iso: Optional[str],
    to_iso: Optional[str],
    use_checkpoint: bool,
    checkpoint_path: str,
) -> Window:
    """
    Priority:
      1) If --from provided -> use [--from, --to or now]
      2) Else if --use-checkpoint -> [checkpoint or (now-24h), now]
      3) Else -> [now-24h, now]
    """
    now = _now_jkt()
    ws = _parse_dt_iso(from_iso)
    we = _parse_dt_iso(to_iso) or now

    if ws is not None:
        if ws >= we:
            raise ValueError("--from must be earlier than --to/now")
        return Window(ws, we)

    if use_checkpoint:
        last = _load_checkpoint(checkpoint_path)
        ws = last if last else (we - timedelta(days=1))
        if ws >= we:
            ws = we - timedelta(days=1)
        return Window(ws, we)

    return Window(we - timedelta(days=1), we)


# =========================
# Runner
# =========================
async def main():
    args = build_argparser().parse_args()

    # Weekend guard
    if args.working_days_only:
        dow = _now_jkt().weekday()  # 0=Mon..6=Sun
        if dow >= 5:
            logger.info("Weekend detected; skipping run.")
            return

    # Resolve window
    window = resolve_window(args.from_iso, args.to_iso, args.use_checkpoint, args.checkpoint)
    logger.info("Window: %s -> %s (JKT)", window.start.isoformat(), window.end.isoformat())

    # Symbols (optional)
    symbols = _merge_symbols(args.symbols, args.company_report_json)
    if symbols:
        logger.info("Filter symbols: %d symbols", len(symbols))
    else:
        logger.info("No symbol filter; querying ALL symbols in idx_filings")

    # Query Supabase using chosen time column/kind
    filings = await get_idx_filings_between(
        ts_col=args.ts_col,
        ts_kind=args.ts_kind,
        start_dt=window.start,
        end_dt=window.end,
        symbols=symbols or None,
        table="idx_filings",
        select="*",  # change to a comma-list if you want fewer columns
    )

    # Output
    _ensure_parent(args.out)
    Path(args.out).write_text(json.dumps(filings, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("Wrote %d rows to %s", len(filings), args.out)

    # Save checkpoint
    if args.use_checkpoint and args.checkpoint:
        _save_checkpoint(args.checkpoint, window.end)
        logger.info("Saved checkpoint at %s", args.checkpoint)


if __name__ == "__main__":
    asyncio.run(main())
