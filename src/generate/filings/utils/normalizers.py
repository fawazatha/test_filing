# src/generate/filings/normalizers.py
from __future__ import annotations
from typing import Any, Dict, List, Optional, Set
import json
import re

# Whitelist tags (lowercase for comparison)
WHITELIST: Set[str] = {
    "bullish","bearish","takeover","investment","divestment",
    "free-float-requirement","mesop","inheritance","share-transfer",
}

_non_alnum = re.compile(r"[^a-z0-9]+", flags=re.IGNORECASE)

def _kebab(s: Optional[str]) -> Optional[str]:
    """Ubah string ke kebab-case: 'Oil, Gas & Coal' -> 'oil-gas-coal'."""
    if not s:
        return None
    s = str(s).strip().lower()
    s = _non_alnum.sub("-", s)      # ganti non-alnum jadi '-'
    s = re.sub(r"-{2,}", "-", s)    # rapikan '---' -> '-'
    s = s.strip("-")
    return s or None

def _first_str(x: Any) -> Optional[str]:
    """Ambil string pertama bermakna dari value (string/list/None/other)."""
    if x is None:
        return None
    if isinstance(x, list):
        for v in x:
            if isinstance(v, str) and v.strip():
                return v.strip()
        return None
    if isinstance(x, str):
        s = x.strip()
        return s if s else None
    try:
        s = str(x).strip()
        return s if s else None
    except Exception:
        return None

def _normalize_symbol(sym: Optional[str]) -> Optional[str]:
    if not sym:
        return None
    s = str(sym).strip().upper()
    if not s:
        return None
    return s if s.endswith(".JK") else f"{s}.JK"

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
        # coba parse JSON list
        try:
            arr = json.loads(s)
            if isinstance(arr, list):
                return arr
        except Exception:
            pass
        # postgres array-style {"a","b"}
        if s.startswith("{") and s.endswith("}"):
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
        return [v] if s else []
    try:
        return list(v)
    except Exception:
        return [str(v)]

def normalize_row(row: Dict) -> Dict:
    # --- tags: parse, lower, whitelist ---
    tags = row.get("tags")
    if tags is not None:
        if isinstance(tags, str):
            # coba JSON
            try:
                parsed = json.loads(tags)
                tags = parsed if isinstance(parsed, list) else [tags]
            except Exception:
                tags = [tags]
        if isinstance(tags, list):
            cleaned = []
            for t in tags:
                tt = str(t).strip()
                if not tt:
                    continue
                if tt.lower() in WHITELIST:
                    cleaned.append(tt.lower())
            tags = cleaned
        else:
            tags = []
        row["tags"] = tags

    # --- symbol: isi dari tickers jika kosong ---
    sym = _first_str(row.get("symbol"))
    if not sym:
        tickers = _ensure_list(row.get("tickers")) or []
        for t in tickers:
            if isinstance(t, str) and t.strip():
                sym = t.strip()
                break
    row["symbol"] = _normalize_symbol(sym) or None

    # bersihkan tickers dari payload upload (tidak dibutuhkan di tabel filings)
    if "tickers" in row:
        del row["tickers"]

    # --- ints ---
    for k in ("holding_before", "holding_after", "amount_transaction"):
        if k in row:
            row[k] = _to_int(row.get(k))

    # --- floats ---
    for k in ("price","transaction_value","share_percentage_before","share_percentage_after","share_percentage_transaction"):
        if k in row:
            row[k] = _to_float(row.get(k))

    # --- price_transaction: parse JSON string jika ada ---
    pt = row.get("price_transaction")
    if isinstance(pt, str) and pt.strip():
        try:
            row["price_transaction"] = json.loads(pt)
        except Exception:
            # biarkan apa adanya jika bukan JSON valid
            pass

    # --- sector & sub_sector: pastikan single string dan kebab-case ---
    sector_raw = _first_str(row.get("sector"))
    subsec_raw = _first_str(row.get("sub_sector") or row.get("subsector"))
    row["sector"] = _kebab(sector_raw)
    row["sub_sector"] = _kebab(subsec_raw)

    # --- announcement_published_at fallback ---
    if not row.get("announcement_published_at"):
        row["announcement_published_at"] = row.get("timestamp")

    return row

def normalize_all(rows: List[Dict]) -> List[Dict]:
    return [normalize_row(r) for r in rows]
