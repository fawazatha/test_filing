from __future__ import annotations
from typing import Any, Dict, List, Optional, Set
import json
import re

# Ambil info sektor dari company_map via provider
try:
    from generate.filings.provider import get_company_info  # type: ignore
except Exception:
    # fallback relative
    from provider import get_company_info  # type: ignore

# Whitelist 9 tag
WHITELIST: Set[str] = {
    "bullish","bearish","takeover","investment","divestment",
    "free-float-requirement","MESOP","inheritance","share-transfer",
}

_S1 = re.compile(r"([a-z0-9])([A-Z])")
_S2 = re.compile(r"[^a-zA-Z0-9]+")

def _titlecase_like(s: str | None) -> str | None:
    if not s:
        return s
    parts = []
    for token in str(s).split("-"):
        parts.append(token.strip().title())
    return "-".join(p for p in parts if p)

def _kebab(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    ss = re.sub(r"[^A-Za-z0-9]+", "-", str(s)).strip("-").lower()
    return ss or None

def _to_int(v: Any) -> int | None:
    if v is None or v == "": return None
    try:
        return int(str(v).replace(",", "").strip())
    except Exception:
        return None

def _to_float(v: Any) -> float | None:
    if v is None or v == "": return None
    try:
        return float(str(v).replace(",", "").strip())
    except Exception:
        return None

def _ensure_list(v: Any) -> Optional[List[Any]]:
    if v is None: return None
    if isinstance(v, list): return v
    if isinstance(v, str):
        s = v.strip()
        try:
            arr = json.loads(s)
            if isinstance(arr, list):
                return arr
        except Exception:
            pass
        if s.startswith("{") and s.endswith("}"):  # postgres array
            inner = s[1:-1]
            if not inner: return []
            out, buf, quote = [], [], False
            for ch in inner:
                if ch == '"':
                    quote = not quote
                    continue
                if ch == ',' and not quote:
                    out.append(''.join(buf).strip())
                    buf = []
                    continue
                buf.append(ch)
            if buf: out.append(''.join(buf).strip())
            return [p.strip('"') for p in out]
        return [v]
    try:
        return list(v)
    except Exception:
        return [str(v)]

def normalize_row(row: Dict) -> Dict:
    # tags whitelist (lower)
    tags = row.get("tags")
    if tags is not None:
        if isinstance(tags, str):
            try:
                tags = json.loads(tags)
            except Exception:
                tags = [tags]
        if isinstance(tags, list):
            tags = [str(t).strip().lower() for t in tags if str(t).strip().lower() in WHITELIST]
        row["tags"] = tags

    # tickers → None (pakai field single `symbol`)
    row["tickers"] = None

    # ints
    for k in ("holding_before", "holding_after", "amount_transaction"):
        if k in row:
            row[k] = _to_int(row.get(k))

    # floats
    for k in ("price","transaction_value","share_percentage_before","share_percentage_after","share_percentage_transaction"):
        if k in row:
            row[k] = _to_float(row.get(k))

    # price_transaction json
    pt = row.get("price_transaction")
    if isinstance(pt, str):
        try:
            row["price_transaction"] = json.loads(pt)
        except Exception:
            pass

    # ==== sector / sub_sector ====
    # 1) Ambil dari provider kalau kosong/null
    sym = row.get("symbol")
    if not row.get("sector") or row.get("sector") in ([], ""):
        ci = get_company_info(sym)
        if ci and ci.sector:
            row["sector"] = ci.sector
    if not row.get("sub_sector") or row.get("sub_sector") in ([], ""):
        ci = ci if 'ci' in locals() else get_company_info(sym)
        if ci and ci.sub_sector:
            row["sub_sector"] = ci.sub_sector

    # 2) Pastikan kebab-case & string (bukan list)
    if isinstance(row.get("sector"), list):
        row["sector"] = _kebab(" ".join([str(x) for x in row["sector"] if x is not None]))
    else:
        row["sector"] = _kebab(row.get("sector"))

    if isinstance(row.get("sub_sector"), list):
        row["sub_sector"] = _kebab(" ".join([str(x) for x in row["sub_sector"] if x is not None]))
    else:
        row["sub_sector"] = _kebab(row.get("sub_sector"))

    # Ensure announcement_published_at exists (mirror from timestamp if missing)
    if not row.get("announcement_published_at"):
        row["announcement_published_at"] = row.get("timestamp")

    return row

def normalize_all(rows: List[Dict]) -> List[Dict]:
    return [normalize_row(r) for r in rows]
