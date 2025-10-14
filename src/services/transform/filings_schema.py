# src/services/transform/filings_schema.py
from __future__ import annotations
from typing import Any, Dict, List, Iterable, Optional
from datetime import datetime, date
import json
import re
import ast
from pathlib import PurePath

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
    "sector",           # text (kebab)
    "sub_sector",       # text (kebab)

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

def _basename(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    try:
        p = str(s).strip()
        if not p:
            return None
        return PurePath(p).name
    except Exception:
        return None

# =====================================================================================
# Title/Body formatting helpers
# =====================================================================================

def _tt(s: Any) -> str:
    """Title-case with graceful fallback."""
    if not s:
        return ""
    try:
        return str(s).strip().title()
    except Exception:
        return str(s)

def _a_an(noun: str) -> str:
    if not noun:
        return "a"
    return "an" if str(noun).strip()[:1].lower() in "aeiou" else "a"

def _fmt_commas(x: Any) -> Optional[str]:
    xi = _to_int(x)
    return f"{xi:,}" if xi is not None else None

def _compose_title_body(out: Dict[str, Any], src: Dict[str, Any]) -> None:
    """
    Title:
      "<HolderName> Buy/Sell/Transfer Transaction of <CompanyName|Symbol>"

    Body:
      "<HolderName>, an <holder_type>, bought/sold/transferred <amount> shares of <CompanyName|Symbol>,
       increasing/decreasing its holdings from <before> to <after>. The transaction occurred in the
       <sector> sector, specifically <sub_sector>. The purpose of the transaction is <purpose|undisclosed>."
    """
    typ   = (out.get("transaction_type") or "").lower()
    holder = out.get("holder_name") or src.get("holder_name_raw") or "Shareholder"
    htype  = out.get("holder_type") or src.get("holder_type") or ""
    symbol = out.get("symbol") or ""
    company = src.get("company_name") or src.get("company_name_raw") or symbol

    amt = _fmt_commas(out.get("amount_transaction"))
    hb  = _fmt_commas(out.get("holding_before"))
    ha  = _fmt_commas(out.get("holding_after"))

    sector  = out.get("sector")
    subsec  = out.get("sub_sector")
    purpose = (src.get("purpose") or "").strip() or "undisclosed"

    action_title = {"buy":"Buy", "sell":"Sell", "transfer":"Transfer"}.get(typ, "Transaction")
    verb = {"buy":"bought", "sell":"sold", "transfer":"transferred"}.get(typ, "executed")

    # Title
    if not out.get("title"):
        out["title"] = f"{_tt(holder)} {action_title} Transaction of {_tt(company)}"

    # Body
    if not out.get("body"):
        who = _tt(holder)
        type_phrase = ""
        if htype:
            type_phrase = f", {_a_an(htype)} {htype.lower()}"
        amount_phrase = f"{amt} shares of {_tt(company)}" if amt else f"shares of {_tt(company)}"

        # holdings delta phrase
        delta = ""
        if hb and ha:
            try:
                before = int(out.get("holding_before")) if out.get("holding_before") is not None else None
                after  = int(out.get("holding_after"))  if out.get("holding_after")  is not None else None
            except Exception:
                before = after = None
            if before is not None and after is not None:
                if after > before:
                    delta = f", increasing its holdings from {hb} to {ha}"
                elif after < before:
                    delta = f", decreasing its holdings from {hb} to {ha}"
                else:
                    delta = f", resulting in holdings of {ha}"

        # sector sentence
        sector_bits = []
        if sector:
            sector_bits.append(f"the {sector} sector")
        if subsec:
            sector_bits.append(f"specifically {subsec}")
        sector_sentence = ""
        if sector_bits:
            sector_sentence = " The transaction occurred in " + ", ".join(sector_bits) + "."

        purpose_sentence = f" The purpose of the transaction is {purpose}."

        if verb == "executed":
            lead = f"{who}{type_phrase} executed a transaction of {amount_phrase}"
        else:
            lead = f"{who}{type_phrase} {verb} {amount_phrase}"
        out["body"] = f"{lead}{delta}.{sector_sentence}{purpose_sentence}".strip()

# =====================================================================================
# Sub-sector + source normalizers
# =====================================================================================

def _normalize_sub_sector(val: Any) -> Optional[str]:
    if val is None or val == "":
        return None

    # If it's a string that *looks* like a list, try to parse it.
    if isinstance(val, str):
        s = val.strip()
        if (s.startswith("[") and s.endswith("]")) or (s.startswith("(") and s.endswith(")")):
            # try JSON first (double quotes)
            try:
                parsed = json.loads(s)
                if isinstance(parsed, list) and parsed:
                    val = parsed[0]
                else:
                    # fall through to literal_eval
                    raise ValueError
            except Exception:
                # handle single-quoted list strings safely
                try:
                    parsed = ast.literal_eval(s)
                    if isinstance(parsed, (list, tuple)) and parsed:
                        val = parsed[0]
                except Exception:
                    # keep original string if parsing fails
                    pass

    # Real list/tuple -> take first non-empty
    if isinstance(val, (list, tuple)):
        for x in val:
            if x not in (None, "", []):
                val = x
                break

    return _kebab(_first_scalar(val)) if val else None



def _pick_url_from(obj: Dict[str, Any]) -> Optional[str]:
    """
    Try to pull a URL from common keys.
    """
    for k in ("pdf_url", "url", "attachment_url", "source_url"):
        v = obj.get(k)
        if isinstance(v, str) and "://" in v and v.lower().endswith(".pdf"):
            return v
    return None


def _normalize_source(src_row: Dict[str, Any]) -> Optional[str]:
    """
    Prefer the full URL to the PDF.
    Logic:
      1) If 'source' is already a URL → keep it.
      2) Else treat 'source' as a filename; try to find a matching URL in:
         - top-level keys: pdf_url/url/attachment_url/source_url
         - nested 'announcement' or 'announcement_meta'
         - any field whose value is a URL string that endswith the filename
      3) If nothing found, return original 'source'.
    """
    raw = src_row.get("source")
    if isinstance(raw, str) and "://" in raw:
        return raw  # already a URL

    filename = _basename(raw) if isinstance(raw, str) else None

    # direct candidates
    for key in ("pdf_url", "url", "attachment_url", "source_url"):
        v = src_row.get(key)
        if isinstance(v, str) and "://" in v:
            if not filename:
                return v
            # match filename tail
            if _basename(v) == filename:
                return v

    # nested announcement blocks
    for nest_key in ("announcement", "announcement_meta"):
        blk = src_row.get(nest_key)
        if isinstance(blk, dict):
            cand = _pick_url_from(blk)
            if cand:
                if not filename or _basename(cand) == filename:
                    return cand

    # brute scan: any url string value that ends with our filename
    if filename:
        for v in src_row.values():
            if isinstance(v, str) and "://" in v and v.lower().endswith(".pdf"):
                if _basename(v) == filename:
                    return v

    # fallback: keep whatever we had
    return raw if isinstance(raw, str) else None

# =====================================================================================
# Core normalizer (one row)
# =====================================================================================

def _clean_one(row: Dict[str, Any]) -> Dict[str, Any]:
    src = row or {}
    out: Dict[str, Any] = {}

    # --- source (upgrade filename -> URL when possible) ---
    norm_source = _normalize_source(src)
    if norm_source:
        out["source"] = norm_source

    # --- direct copies (strings) ---
    for k in ["title","body","holder_type","holder_name","UID"]:
        if k in src and src[k] is not None:
            out[k] = _to_str(src[k])

    # --- symbol ---
    out["symbol"] = _normalize_symbol(src.get("symbol") or src.get("issuer_code"))

    # --- timestamp (ISO string) ---
    if src.get("timestamp"):
        out["timestamp"] = _to_datetime_iso(src["timestamp"])

    # --- sector / sub_sector ---
    sector = src.get("sector")
    if isinstance(sector, list):
        sector = " ".join(str(x) for x in sector if x is not None)
    out["sector"] = _kebab(_first_scalar(sector)) if sector else None

    out["sub_sector"] = _normalize_sub_sector(src.get("sub_sector"))
    if isinstance(out.get("sub_sector"), (list, tuple)) or (
        isinstance(out.get("sub_sector"), str) and out["sub_sector"].strip().startswith("[")
    ):
        out["sub_sector"] = _normalize_sub_sector(out["sub_sector"])

    # --- tags / tickers (text[]) ---
    tags = _parse_json_list_or_csv(src.get("tags"))
    if tags is not None:
        out["tags"] = sorted({t.lower() for t in tags if t})

    tickers = _parse_json_list_or_csv(src.get("tickers"))
    if tickers is not None:
        out["tickers"] = [t.upper() for t in tickers if t] or []
    # If you prefer always-present tickers array, uncomment:
    # elif "tickers" not in out:
    #     out["tickers"] = []

    # --- transaction_type ---
    t = (src.get("transaction_type") or src.get("type") or "").strip().lower()
    if t not in {"buy","sell","transfer","other"}:
        hb = _to_float(src.get("holding_before"))
        ha = _to_float(src.get("holding_after"))
        if isinstance(hb, (int,float)) and isinstance(ha, (int,float)):
            if ha > hb: t = "buy"
            elif ha < hb: t = "sell"
    if t not in {"buy","sell","transfer","other"}:
        t = "other"
    out["transaction_type"] = t

    # --- holdings / amount_transaction (map legacy amount_transacted/amount) ---
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

    # === Compose title/body per requested template ===
    _compose_title_body(out, src)

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
