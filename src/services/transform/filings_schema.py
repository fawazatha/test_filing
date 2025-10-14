# src/services/transform/filings_schema.py
from __future__ import annotations
from typing import Any, Dict, List, Iterable, Optional
from datetime import datetime, date
import json
import re

# =====================================================================================
# Table contract: must match Supabase table `idx_filings`
# =====================================================================================

ALLOWED_COLUMNS: List[str] = [
    # server-managed
    "id",               # ignored on insert
    "created_at",       # ignored on insert

    # article-ish
    "title",
    "body",
    "source",
    "timestamp",

    # classification
    "sector",           # text
    "sub_sector",       # text

    # arrays
    "tags",             # text[]
    "tickers",          # text[]

    # transaction facts
    "transaction_type",             # text
    "holding_before",               # int8
    "holding_after",                # int8
    "amount_transaction",           # int8
    "holder_type",                  # text
    "holder_name",                  # text
    "price",                        # numeric
    "transaction_value",            # numeric
    "price_transaction",            # jsonb {"prices":[...], "amount_transacted":[...]}

    # percentages
    "share_percentage_before",      # float8
    "share_percentage_after",       # float8
    "share_percentage_transaction", # float8

    # misc
    "UID",                          # text
    "symbol",                       # text
]

REQUIRED_COLUMNS: List[str] = [
    "symbol",
    "timestamp",
    "transaction_type",
    "holding_before",
    "holding_after",
    "amount_transaction",
    "price",
    "transaction_value",
    "share_percentage_before",
    "share_percentage_after",
    "share_percentage_transaction",
]

# =====================================================================================
# Coercers
# =====================================================================================

def _to_float(x: Any) -> Optional[float]:
    if x is None or x == "":
        return None
    try:
        return float(str(x).replace(",", ""))
    except Exception:
        return None

def _to_int(x: Any) -> Optional[int]:
    if x is None or x == "":
        return None
    try:
        return int(x)
    except Exception:
        try:
            return int(float(x))
        except Exception:
            return None

def _to_str(x: Any) -> Optional[str]:
    if x is None:
        return None
    return str(x)

def _to_datetime_iso(x: Any) -> Optional[str]:
    if x is None or x == "":
        return None
    if isinstance(x, datetime):
        return x.isoformat()
    if isinstance(x, date):
        return datetime(x.year, x.month, x.day).isoformat()
    return str(x)

def _parse_json_list_or_csv(x: Any) -> Optional[List[str]]:
    if x is None:
        return None
    if isinstance(x, list):
        return [str(t).strip() for t in x if str(t).strip()]
    if isinstance(x, str):
        s = x.strip()
        if s.startswith("[") and s.endswith("]"):
            try:
                arr = json.loads(s)
                if isinstance(arr, list):
                    return [str(t).strip() for t in arr if str(t).strip()]
            except Exception:
                pass
        return [t.strip() for t in s.split(",") if t.strip()]
    return None

def _parse_json_obj(x: Any) -> Optional[Dict[str, Any]]:
    if x is None:
        return None
    if isinstance(x, dict):
        return x
    if isinstance(x, str) and x.strip().startswith("{") and x.strip().endswith("}"):
        try:
            obj = json.loads(x)
            if isinstance(obj, dict):
                return obj
        except Exception:
            return None
    return None

# kebab helper for sector/sub_sector
_NONALNUM = re.compile(r"[^0-9A-Za-z]+")
def _kebab(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    return _NONALNUM.sub("-", str(s).strip()).strip("-").lower() or None

def _first_scalar(x: Any) -> Optional[str]:
    if x is None:
        return None
    if isinstance(x, list):
        for it in x:
            if it not in (None, "", []):
                return str(it)
        return None
    return str(x)

def _normalize_symbol(sym: Optional[str]) -> Optional[str]:
    s = (sym or "").strip().upper()
    if not s:
        return None
    return s if s.endswith(".JK") else f"{s}.JK"

# =====================================================================================
# Core normalizer (one row)
# =====================================================================================

def _clean_one(row: Dict[str, Any]) -> Dict[str, Any]:
    src = row or {}
    out: Dict[str, Any] = {}

    # --- direct copies (strings) ---
    for k in ["title","body","source","holder_type","holder_name","UID"]:
        if k in src and src[k] is not None:
            out[k] = _to_str(src[k])

    # --- symbol ---
    out["symbol"] = _normalize_symbol(src.get("symbol") or src.get("issuer_code"))

    # --- timestamp (ISO string) ---
    if src.get("timestamp"):
        out["timestamp"] = _to_datetime_iso(src["timestamp"])

    # --- sector / sub_sector (force text, kebab; flatten if list) ---
    sector = src.get("sector")
    if isinstance(sector, list):
        sector = " ".join(str(x) for x in sector if x is not None)
    out["sector"] = _kebab(_first_scalar(sector)) if sector else None

    sub_sector = src.get("sub_sector")
    if isinstance(sub_sector, list):
        sub_sector = " ".join(str(x) for x in sub_sector if x is not None)
    out["sub_sector"] = _kebab(_first_scalar(sub_sector)) if sub_sector else None

    # --- tags / tickers (text[]) ---
    tags = _parse_json_list_or_csv(src.get("tags"))
    if tags is not None:
        out["tags"] = sorted({t.lower() for t in tags if t})

    tickers = _parse_json_list_or_csv(src.get("tickers"))
    if tickers is not None:
        out["tickers"] = [t.upper() for t in tickers if t] or []

    # --- transaction_type ---
    t = (src.get("transaction_type") or src.get("type") or "").strip().lower()
    if t not in {"buy","sell","transfer","other"}:
        # derive from holdings if possible
        hb = _to_float(src.get("holding_before"))
        ha = _to_float(src.get("holding_after"))
        if isinstance(hb, (int,float)) and isinstance(ha, (int,float)):
            if ha > hb: t = "buy"
            elif ha < hb: t = "sell"
    if t not in {"buy","sell","transfer","other"}:
        t = "other"
    out["transaction_type"] = t

    # --- holdings / amount_transaction (map legacy amount_transacted) ---
    if src.get("holding_before") is not None:
        out["holding_before"] = _to_int(src.get("holding_before"))
    if src.get("holding_after") is not None:
        out["holding_after"] = _to_int(src.get("holding_after"))

    amt = src.get("amount_transaction")
    if amt is None:
        amt = src.get("amount_transacted")  # legacy from parser
    if amt is None:
        amt = src.get("amount")
    out["amount_transaction"] = _to_int(amt)

    # --- numbers ---
    out["price"] = _to_float(src.get("price"))
    out["transaction_value"] = _to_float(src.get("transaction_value"))

    # percentages
    out["share_percentage_before"] = _to_float(src.get("share_percentage_before"))
    out["share_percentage_after"] = _to_float(src.get("share_percentage_after"))
    out["share_percentage_transaction"] = _to_float(src.get("share_percentage_transaction"))

    # --- price_transaction (jsonb) ---
    pt = _parse_json_obj(src.get("price_transaction"))
    if pt is None:
        pt = {"prices": [], "amount_transacted": []}
    out["price_transaction"] = pt

    # --- fallback compute value if missing and price/amount exist ---
    if out.get("transaction_value") is None and out.get("price") is not None and out.get("amount_transaction") is not None:
        try:
            out["transaction_value"] = float(out["price"]) * int(out["amount_transaction"])
        except Exception:
            pass

    # --- final strip: keep only allowed & non-None ---
    out = {k: v for k, v in out.items() if (k in ALLOWED_COLUMNS and v is not None)}
    return out

# =====================================================================================
# Public API
# =====================================================================================

def clean_rows(rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    cleaned: List[Dict[str, Any]] = []
    for r in rows or []:
        if not isinstance(r, dict):
            continue
        cr = _clean_one(r)
        if cr:
            cleaned.append(cr)
    return cleaned

__all__ = ["ALLOWED_COLUMNS", "REQUIRED_COLUMNS", "clean_rows"]
