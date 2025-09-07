from __future__ import annotations
import json, logging, re
from typing import Any, Dict, Iterable, List, Optional

logger = logging.getLogger(__name__)

# === Kolom mengikuti CSV Supabase ===
ALLOWED_COLUMNS: set[str] = {
    "title",
    "body",
    "source",
    "timestamp",
    "sector",
    "sub_sector",
    "tags",
    "tickers",                   # akan dipaksa None
    "transaction_type",
    "holding_before",
    "holding_after",
    "amount_transaction",
    "holder_type",
    "holder_name",
    "price",
    "transaction_value",
    "price_transaction",
    "share_percentage_before",
    "share_percentage_after",
    "share_percentage_transaction",
    "uid",                       # dari "UID" -> snake_case
    "symbol",
}

# Wajib ada (menyesuaikan CSV kamu—tidak ada `transaction_date` di CSV)
REQUIRED_COLUMNS: set[str] = {"title", "symbol", "sector", "timestamp"}

# --- helpers ---
_S1 = re.compile(r"([a-z0-9])([A-Z])")
_S2 = re.compile(r"[^a-zA-Z0-9]+")

def _to_snake(s: str) -> str:
    s = _S1.sub(r"\1_\2", s)
    s = _S2.sub("_", s)
    return s.strip("_").lower()

def _snake_shallow(d: Dict[str, Any]) -> Dict[str, Any]:
    return {_to_snake(k): v for k, v in d.items()}

def _ensure_list(v: Any) -> Optional[List[Any]]:
    if v is None: return None
    if isinstance(v, list): return v
    if isinstance(v, str):
        s = v.strip()
        # coba JSON array: '["a","b"]'
        try:
            parsed = json.loads(s)
            if isinstance(parsed, list):
                return parsed
        except Exception:
            pass
        # coba literal Postgres: {"a","b"}
        if s.startswith("{") and s.endswith("}"):
            inner = s[1:-1]
            if not inner: return []
            out, buf, quote = [], [], False
            for ch in inner:
                if ch == '"': quote = not quote; continue
                if ch == ',' and not quote:
                    out.append(''.join(buf).strip()); buf = []; continue
                buf.append(ch)
            if buf: out.append(''.join(buf).strip())
            return [p.strip('"') for p in out]
        return [v]
    try:
        return list(v)
    except Exception:
        return [str(v)]

def _to_int(v: Any) -> Optional[int]:
    if v is None or v == "": return None
    try: return int(str(v).replace(",", "").strip())
    except Exception: return None

def _to_float(v: Any) -> Optional[float]:
    if v is None or v == "": return None
    try: return float(str(v).replace(",", "").strip())
    except Exception: return None

def _filter_allowed(row: Dict[str, Any], allowed: Optional[Iterable[str]]) -> Dict[str, Any]:
    if not allowed: return row
    allow = set(allowed)
    return {k: v for k, v in row.items() if k in allow}

# --- main cleaners ---
def clean_row(row: Dict[str, Any]) -> Dict[str, Any]:
    r = _snake_shallow(row)

    # tags -> list
    if "tags" in r:
        r["tags"] = _ensure_list(r.get("tags"))

    # price_transaction -> dict/list (jsonb)
    if isinstance(r.get("price_transaction"), str):
        s = r["price_transaction"].strip()
        try:
            r["price_transaction"] = json.loads(s)
        except Exception:
            pass  # biarkan string; PostgREST akan menolak kalau tidak valid JSON

    # angka: int
    for k in ("holding_before", "holding_after", "amount_transaction"):
        r[k] = _to_int(r.get(k)) if k in r else None

    # angka: float
    for k in ("price", "transaction_value",
              "share_percentage_before", "share_percentage_after", "share_percentage_transaction"):
        r[k] = _to_float(r.get(k)) if k in r else None

    # tickers -> None (sesuai permintaan)
    r["tickers"] = None

    # pastikan required fields tidak kosong (biar cepat ketahuan)
    for k in REQUIRED_COLUMNS:
        if not r.get(k):
            logger.debug("Missing required field %s on row with title=%r", k, r.get("title"))

    # keep only allowed columns
    r = _filter_allowed(r, ALLOWED_COLUMNS)
    return r

def clean_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for i, row in enumerate(rows):
        try:
            out.append(clean_row(row))
        except Exception as e:
            logger.error("clean_rows: failed on row %d: %s", i, e)
    return out
