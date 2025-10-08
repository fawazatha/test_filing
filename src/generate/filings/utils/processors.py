from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional
from datetime import datetime

try:
    import zoneinfo
    JKT = zoneinfo.ZoneInfo("Asia/Jakarta")
except Exception:
    JKT = None

from .provider import get_tags, get_company_info, get_latest_price

# ---- helpers ----

def _parse_dt_wib(dtstr: str | None) -> Optional[datetime]:
    if not dtstr:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y%m%d-%H%M%S"):
        try:
            dt = datetime.strptime(dtstr, fmt)
            return dt.replace(tzinfo=JKT) if JKT and dt.tzinfo is None else dt
        except Exception:
            continue
    # last resort: fromisoformat variants
    try:
        dt = datetime.fromisoformat(dtstr.replace(" ", "T"))
        return dt if not JKT else dt.astimezone(JKT)
    except Exception:
        return None


def _fmt_published_date(row: Dict[str, Any], downloads_meta_map: Dict[str, Any] | None) -> Optional[str]:
    src = (row.get("source") or "").strip()
    pub = None
    if downloads_meta_map and src:
        # match by full url or filename
        meta = downloads_meta_map.get(src) or downloads_meta_map.get(Path(src).name)
        if isinstance(meta, dict):
            pub = meta.get("published_at") or meta.get("timestamp") or meta.get("time")

    if not pub:
        pub = row.get("announcement_published_at") or row.get("timestamp")

    dt = _parse_dt_wib(pub) if isinstance(pub, str) else None
    if not dt:
        return None
    # '7 October 2025'
    if os.name == "nt":
        return dt.strftime("%#d %B %Y")
    return dt.strftime("%-d %B %Y")


def _fmt_int(val: Any) -> Any:
    try:
        return f"{int(float(val)):,}"
    except Exception:
        return val


def _compose_body(row: Dict[str, Any], downloads_meta_map: Dict[str, Any] | None) -> str:
    pub_date = _fmt_published_date(row, downloads_meta_map)
    lead = f"According to the published announcement on {pub_date}" if pub_date else "According to the published announcement"

    holder = row.get("holder_name") or "the shareholder"
    company = row.get("company_name") or row.get("symbol") or "the company"
    sym = f" ({row['symbol']})" if row.get("symbol") else ""
    tx = (row.get("transaction_type") or "").lower()
    verb = {"buy": "bought", "sell": "sold", "transfer": "transferred"}.get(tx, "transacted")

    amt = _fmt_int(row.get("amount_transaction"))
    price_clause = ""
    p = row.get("price")
    try:
        if p not in (None, "", 0, "0"):
            price_clause = f" at ≈ IDR {float(p):,.0f} per share"
    except Exception:
        pass

    before = _fmt_int(row.get("holding_before"))
    after = _fmt_int(row.get("holding_after"))

    return (
        f"{lead}, {holder} {verb} {amt} shares of {company}{sym}{price_clause}, "
        f"changing its holding from {before} to {after}."
    )


def _build_price_transaction(row: Dict[str, Any]) -> Dict[str, Any] | None:
    txs = row.get("transactions")
    if not isinstance(txs, list):
        return None
    types, prices, amts = [], [], []
    for t in txs:
        amt = t.get("amount")
        if not amt:
            continue
        types.append(t.get("type"))
        prices.append(t.get("price"))
        amts.append(amt)
    if not amts:
        return None
    return {"type": types, "prices": prices, "amount_transacted": amts}


def _sanity_checks(row: Dict[str, Any]) -> None:
    """Attach needs_review/skip_reason when simple sanity rules fail."""
    # 0 ≤ share% ≤ 100
    for key in ("share_percentage_before", "share_percentage_after"):
        v = row.get(key)
        try:
            if v is None:
                continue
            f = float(v)
            if f < 0 or f > 100:
                row["needs_review"] = True
                row.setdefault("parse_warnings", []).append(f"{key}_out_of_range")
        except Exception:
            row["needs_review"] = True
            row.setdefault("parse_warnings", []).append(f"{key}_non_numeric")

    # price presence (optional gating hook)
    # You can add staleness check vs published date here if you join OHLC by date.


def build_row(raw: Dict[str, Any], downloads_meta_map: Dict[str, Any] | None) -> Dict[str, Any]:
    """Transform a parsed filing (IDX/Non-IDX) into standardized row."""
    row = dict(raw)  # shallow copy

    # company enrichment (sector normalization handled later in normalizers)
    sym_full = row.get("symbol")
    comp = get_company_info(sym_full) if sym_full else None
    if comp:
        row.setdefault("company_name", comp.company_name)
        row.setdefault("sector", comp.sector)
        row.setdefault("sub_sector", comp.sub_sector)

    # ensure proper body
    row["body"] = _compose_body(row, downloads_meta_map)

    # tags (list, whitelist)
    row["tags"] = get_tags(
        row.get("transaction_type"),
        row.get("share_percentage_before"),
        row.get("share_percentage_after"),
        body=row.get("body"),
    )

    # price_transaction as object
    pt = _build_price_transaction(row)
    if pt:
        row["price_transaction"] = pt

    # optional: latest price (join already handled elsewhere)
    # lp = get_latest_price(sym_full) if sym_full else None
    # if lp is not None and not row.get("price"):
        # row["price"] = lp

    # sanity hooks
    _sanity_checks(row)

    return row


def process_all(parsed_lists: List[List[Dict[str, Any]]], downloads_meta_map: Dict[str, Any] | None) -> List[Dict[str, Any]]:
    """Flatten + transform all parsed chunks into final rows."""
    out: List[Dict[str, Any]] = []
    for chunk in parsed_lists:
        if not chunk:
            continue
        for raw in chunk:
            try:
                out.append(build_row(raw, downloads_meta_map))
            except Exception as e:
                # skip but could log elsewhere
                continue
    return out
