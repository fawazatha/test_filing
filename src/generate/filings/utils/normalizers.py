from __future__ import annotations
from typing import Any, Dict, List, Optional, Set
import json
import re

try:
    from generate.filings.utils.provider import get_company_info  # type: ignore
except Exception:
    from provider import get_company_info  

WHITELIST: Set[str] = {
    "bullish", "bearish", "takeover", "investment", "divestment",
    "free-float-requirement", "mesop", "inheritance", "share-transfer",
}

_S1 = re.compile(r"([a-z0-9])([A-Z])")
_S2 = re.compile(r"[^a-zA-Z0-9]+")


def _kebab(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    ss = re.sub(r"[^A-Za-z0-9]+", "-", str(s)).strip("-").lower()
    return ss or None


def _to_int(v: Any) -> int | None:
    if v is None or v == "":
        return None
    try:
        return int(str(v).replace(",", "").strip())
    except Exception:
        return None


def _to_float(v: Any) -> float | None:
    if v is None or v == "":
        return None
    try:
        return float(str(v).replace(",", "").strip())
    except Exception:
        return None


def _ensure_list(v: Any) -> Optional[List[Any]]:
    if v is None:
        return None
    if isinstance(v, list):
        return v
    if isinstance(v, str):
        s = v.strip()
        # Coba parse JSON array
        try:
            arr = json.loads(s)
            if isinstance(arr, list):
                return arr
        except Exception:
            pass
        # Postgres array-like: {"a","b"}
        if s.startswith("{") and s.endswith("}"):
            inner = s[1:-1]
            if not inner:
                return []
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
            if buf:
                out.append(''.join(buf).strip())
            return [p.strip('"') for p in out]
        return [v]
    try:
        return list(v)
    except Exception:
        return [str(v)]


def _normalize_symbol(sym: Any) -> Optional[str]:
    """Selalu kembalikan UPPER + .JK (kalau ada nilai)."""
    if sym is None:
        return None
    s = str(sym).strip().upper()
    if not s:
        return None
    return s if s.endswith(".JK") else f"{s}.JK"


def normalize_row(row: Dict) -> Dict:
    # ---------- tags whitelist (case-insensitive) ----------
    tags = row.get("tags")
    if tags is not None:
        if isinstance(tags, str):
            # coba parse JSON string → list
            try:
                tags = json.loads(tags)
            except Exception:
                tags = [tags]
        if isinstance(tags, list):
            cleaned = []
            for t in tags:
                tt = str(t).strip()
                if not tt:
                    continue
                low = tt.lower()
                if low in WHITELIST:
                    cleaned.append(low)
            tags = cleaned
        row["tags"] = tags

    # ---------- symbol ----------
    row["symbol"] = _normalize_symbol(row.get("symbol"))

    # ---------- tickers → None (pakai field single `symbol`) ----------
    row["tickers"] = None

    # ---------- ints ----------
    for k in ("holding_before", "holding_after", "amount_transaction"):
        if k in row:
            row[k] = _to_int(row.get(k))

    # ---------- floats ----------
    for k in (
        "price",
        "transaction_value",
        "share_percentage_before",
        "share_percentage_after",
        "share_percentage_transaction",
    ):
        if k in row:
            row[k] = _to_float(row.get(k))

    # ---------- price_transaction (JSON string → dict) ----------
    pt = row.get("price_transaction")
    if isinstance(pt, str):
        try:
            parsed = json.loads(pt)
            if isinstance(parsed, (dict, list)):
                row["price_transaction"] = parsed
        except Exception:
            # biarkan apa adanya jika gagal parse
            pass

    # ---------- sector / sub_sector ----------
    # 1) Isi dari company_map kalau kosong/null
    sym_norm = row.get("symbol")  # sudah dinormalisasi .JK
    if (not row.get("sector")) or row.get("sector") in ([], ""):
        try:
            ci = get_company_info(sym_norm)
        except Exception:
            ci = None
        if ci and getattr(ci, "sector", None):
            row["sector"] = ci.sector

    if (not row.get("sub_sector")) or row.get("sub_sector") in ([], ""):
        try:
            # pakai ci sebelumnya kalau ada; kalau tidak, panggil lagi
            ci = ci if "ci" in locals() and ci else get_company_info(sym_norm)
        except Exception:
            ci = None
        if ci and getattr(ci, "sub_sector", None):
            row["sub_sector"] = ci.sub_sector

    # 2) Paksa kebab-case & string (bukan list)
    if isinstance(row.get("sector"), list):
        row["sector"] = _kebab(" ".join([str(x) for x in row["sector"] if x is not None]))
    else:
        row["sector"] = _kebab(row.get("sector"))

    if isinstance(row.get("sub_sector"), list):
        row["sub_sector"] = _kebab(" ".join([str(x) for x in row["sub_sector"] if x is not None]))
    else:
        row["sub_sector"] = _kebab(row.get("sub_sector"))

    # ---------- announcement_published_at fallback ----------
    if not row.get("announcement_published_at"):
        row["announcement_published_at"] = row.get("timestamp")

    return row


def normalize_all(rows: List[Dict]) -> List[Dict]:
    return [normalize_row(r) for r in rows]
