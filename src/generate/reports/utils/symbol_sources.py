# src/generate/reports/utils/symbol_sources.py
from __future__ import annotations
from typing import Iterable, List, Optional, Dict, Any
import json
from pathlib import Path

from ..core import fetch_company_report_symbols, load_companies_from_json
from . import sb as sbapi  # Supabase REST helper kamu

def parse_symbols_arg(arg: Optional[str]) -> List[str]:
    out: List[str] = []
    if not arg: return out
    for s in arg.split(","):
        t = s.strip().upper()
        if t: out.append(t)
    return sorted(set(out))

async def fetch_symbols_from_company_report_watchlist(
    *, select: str = "symbol,listing_board", table: str = "idx_company_report"
) -> List[str]:
    """Ambil simbol dari idx_company_report di mana listing_board == 'watchlist'."""
    rows = await sbapi.fetch_all(
        table=table,
        select=select,
        eq={"listing_board": "watchlist"},
        order="symbol.asc",
        page_size=5000,
        timeout=60.0,
    )
    syms = []
    for r in rows:
        sym = str(r.get("symbol", "")).strip().upper()
        if sym: syms.append(sym)
    return sorted(set(syms))

async def fetch_symbols_from_user_watchlist(
    user_email: str,
    *,
    table: str = "watchlist",
    symbol_col: str = "symbol",
    owner_col: str = "owner_email",
) -> List[str]:
    """Ambil simbol dari tabel watchlist milik user tertentu (opsional jika sudah ada)."""
    rows = await sbapi.fetch_all(
        table=table,
        select=f"{symbol_col},{owner_col}",
        eq={owner_col: user_email},
        order=f"{symbol_col}.asc",
        page_size=5000,
        timeout=60.0,
    )
    syms = []
    for r in rows:
        sym = str(r.get(symbol_col, "")).strip().upper()
        if sym: syms.append(sym)
    return sorted(set(syms))

async def resolve_symbols_priority(
    *,
    symbols_arg: Optional[str] = None,
    user_email: Optional[str] = None,
    use_company_report_watchlist: bool = True,
    companies_json_in: Optional[str] = None,
) -> List[str]:
    """
    Prioritas simbol:
    1) --symbols (jika diisi)
    2) watchlist user (jika user_email diberikan dan tabelnya tersedia)
    3) watchlist dari idx_company_report (listing_board='watchlist') bila diizinkan
    4) insider-tagged (fallback lama)
    """
    # 1) explicit
    syms = parse_symbols_arg(symbols_arg)
    if syms:
        return syms

    # 2) user watchlist
    if user_email:
        try:
            syms = await fetch_symbols_from_user_watchlist(user_email=user_email)
            if syms: return syms
        except Exception:
            # tabel belum ada -> skip
            pass

    # 3) company_report watchlist
    if use_company_report_watchlist:
        try:
            syms = await fetch_symbols_from_company_report_watchlist()
            if syms: return syms
        except Exception:
            pass

    # 4) fallback insider-tagged (pakai JSON offline jika diberi, kalau tidak pakai Supabase)
    if companies_json_in:
        rows = load_companies_from_json(companies_json_in)
        out: List[str] = []
        for r in rows:
            sym = str(r.get("symbol") or "").strip().upper()
            if not sym: continue
            tags = r.get("tags") or []
            tag_str = " ".join(map(str, tags)).lower()
            if "insider" in tag_str:
                out.append(sym)
        return sorted(set(out))
    else:
        return await fetch_company_report_symbols(min_tag_substring="insider")
