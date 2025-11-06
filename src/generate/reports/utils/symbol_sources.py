# src/generate/reports/utils/symbol_sources.py
from __future__ import annotations
from typing import Iterable, List, Optional, Dict, Any
import json
from pathlib import Path

from ..core import fetch_company_report_symbols, load_companies_from_json
from ....common import sb as sbapi  # keep import if used elsewhere

def parse_symbols_arg(arg: Optional[str]) -> List[str]:
    out: List[str] = []
    if not arg:
        return out
    for s in arg.split(","):
        t = s.strip().upper()
        if t:
            out.append(t)
    return sorted(set(out))

def _normalize_tags(v: Any) -> List[str]:
    # tags bisa list atau string; jadikan list of str lower
    if v is None:
        return []
    if isinstance(v, list):
        return [str(x).strip().lower() for x in v]
    if isinstance(v, str):
        # coba parse json list, kalau gagal treat as single
        try:
            arr = json.loads(v)
            if isinstance(arr, list):
                return [str(x).strip().lower() for x in arr]
        except Exception:
            pass
        return [v.strip().lower()]
    return [str(v).strip().lower()]

def _load_rows(path: str) -> List[Dict[str, Any]]:
    p = Path(path)
    if not p.exists():
        return []
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        if isinstance(data, list):
            return data
    except Exception:
        pass
    return []

async def fetch_watchlist_symbols(
    *,
    companies_json_in: Optional[str] = None,
    symbol_col: str = "symbol",
) -> List[str]:
    """
    Ambil simbol watchlist.
    - Jika companies_json_in ada: filter client-side (listing_board == 'watchlist').
    - Jika tidak ada: server-side via fetch_company_report_symbols(listing_board='watchlist').
    """
    if companies_json_in:
        rows = load_companies_from_json(companies_json_in)
        out: List[str] = []
        for r in rows:
            board = str(r.get("listing_board") or "").strip().lower()
            sym = str(r.get(symbol_col) or "").strip().upper()
            if board == "watchlist" and sym:
                out.append(sym)
        return sorted(set(out))
    # server-side
    return await fetch_company_report_symbols(listing_board="watchlist", symbol_col=symbol_col)

async def fetch_insider_tagged_symbols(
    *,
    companies_json_in: Optional[str] = None,
    symbol_col: str = "symbol",
) -> List[str]:
    """
    Fallback: simbol yang bertag 'insider'.
    """
    if companies_json_in:
        rows = load_companies_from_json(companies_json_in)
        out: List[str] = []
        for r in rows:
            sym = str(r.get(symbol_col) or "").strip().upper()
            if not sym:
                continue
            tags = _normalize_tags(r.get("tags"))
            if any("insider" in t for t in tags):
                out.append(sym)
        return sorted(set(out))
    # server-side (via core)
    return await fetch_company_report_symbols(min_tag_substring="insider", symbol_col=symbol_col)

async def resolve_symbols_priority(
    *,
    symbols_arg: Optional[str] = None,
    use_company_report_watchlist: bool = True,
    companies_json_in: Optional[str] = None,
    symbol_col: str = "symbol",
) -> List[str]:
    """
    Prioritas:
    1) --symbols jika ada.
    2) Watchlist (companies_json_in kalau ada; jika tidak, server-side).
    3) (Terakhir) fallback insider-tagged.
    """
    # 1) explicit --symbols
    syms = parse_symbols_arg(symbols_arg)
    if syms:
        return syms

    # 2) watchlist
    if use_company_report_watchlist:
        wl = await fetch_watchlist_symbols(companies_json_in=companies_json_in, symbol_col=symbol_col)
        if wl:
            return wl

    # 3) fallback insider-tagged
    return await fetch_insider_tagged_symbols(companies_json_in=companies_json_in, symbol_col=symbol_col)
