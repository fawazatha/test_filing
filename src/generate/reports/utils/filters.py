# src/generate/reports/utils/filters.py
from __future__ import annotations
from typing import Iterable, List, Optional, Dict, Any
import json
from pathlib import Path

from ..core import fetch_company_report_symbols, load_companies_from_json
from . import sb as sbapi  # keep import if used elsewhere


# -----------------------------
# Parsing & normalization
# -----------------------------
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
    """tags bisa list atau string; jadikan list[str] lowercase"""
    if v is None:
        return []
    if isinstance(v, list):
        return [str(x).strip().lower() for x in v]
    if isinstance(v, str):
        # coba parse json list, kalau gagal treat as single string
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


# -----------------------------
# Symbol resolvers (watchlist-first)
# -----------------------------
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


# -----------------------------
# Client-side sweeps / filters
# -----------------------------
def _extract_symbol_from_obj(obj: Any) -> str:
    """
    Ambil symbol dari:
    - dict: obj["symbol"]
    - Filing dataclass: obj.symbol atau obj.raw["symbol"]
    """
    # dict case
    if isinstance(obj, dict):
        return (str(obj.get("symbol") or "").strip().upper())

    # dataclass Filing (atau objek lain yang punya attribute)
    sym = getattr(obj, "symbol", None)
    if sym:
        return (str(sym).strip().upper())

    raw = getattr(obj, "raw", None)
    if isinstance(raw, dict):
        return (str(raw.get("symbol") or "").strip().upper())

    return ""


def filter_filings_by_symbols(filings: List[Any], symbols: List[str]) -> List[Any]:
    """
    Sapu akhir client-side—hanya pertahankan filings dengan symbol ∈ symbols.
    Mendukung elemen bertipe dict atau Filing dataclass.
    """
    if not filings or not symbols:
        return filings
    allowed = set(s.upper() for s in symbols)
    out: List[Any] = []
    for f in filings:
        s = _extract_symbol_from_obj(f)
        if s in allowed:
            out.append(f)
    return out


def filter_company_rows_by_board(company_rows: List[Dict[str, Any]], board: str) -> List[Dict[str, Any]]:
    if not company_rows or not board:
        return company_rows
    lb = board.strip().lower()
    return [r for r in company_rows if str(r.get("listing_board") or "").strip().lower() == lb]
