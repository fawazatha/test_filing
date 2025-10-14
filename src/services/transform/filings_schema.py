# src/services/filings_schema.py
from __future__ import annotations
from typing import Any, Dict, List, Optional
from datetime import datetime, date
import json
import re

# =====================================================================================
# Allowed & Required Columns
# =====================================================================================

REQUIRED_COLUMNS = [
    # inti identitas filing (ikuti kontrak producer)
    "symbol",
    "holder_name",
    "transaction_date",  # ISO or YYYY-MM-DD
    "type",              # buy|sell|transfer|other
    "amount",
]

ALLOWED_COLUMNS = [
    # --- identitas & metadata dasar ---
    "symbol",
    "issuer_code",
    "company_name",
    "holder_name",
    "holder_type",
    "classification_of_shareholder",
    "citizenship",
    "transaction_date",
    "type",  # buy|sell|transfer|other
    "currency",
    "notes",

    # --- dari parser ---
    "title",
    "body",
    "source",
    "timestamp",  # timestamp untuk row; akan dipakai sebagai mirror ke announcement_published_at jika kosong

    # --- nilai dari PDF / parser (existing) ---
    "price",
    "amount",
    "value",
    "holding_before",
    "holding_after",
    "share_percentage_before",
    "share_percentage_after",
    "share_percentage_transaction",

    # --- transaksi rinci (array) ---
    "transactions",  # list of {type, date?, price, amount, value, reasons?}

    # --- konteks dokumen & pasar ---
    "document_median_price",   # float
    "market_reference",        # dict: {ref_price, ref_type, asof_date, n_days, freshness_days}

    # --- audit kepemilikan model vs PDF ---
    "total_shares_model",
    "delta_pp_model",
    "pp_after_model",
    "percent_discrepancy",
    "discrepancy_pp",

    # --- flags & reasons ---
    "suspicious_price_level",
    "needs_review",
    "skip_reason",
    "reasons",
    "announcement",

    # --- klasifikasi & atribut tambahan untuk DB idx_filings ---
    "tags",            # text[]
    "tickers",         # text[]
    "sector",          # text (kebab-case)
    "sub_sector",      # text (kebab-case)
    "price_transaction",  # jsonb

    # --- lain-lain/legacy ---
    "announcement_published_at",
    "source_type",
    "doc_id",
    "downloaded_pdf_path",
]

# Map tipe sederhana untuk normalisasi
COLUMN_TYPES: Dict[str, str] = {
    "symbol": "str",
    "issuer_code": "str",
    "company_name": "str",
    "holder_name": "str",
    "holder_type": "str",
    "classification_of_shareholder": "str",
    "citizenship": "str",
    "transaction_date": "date",
    "type": "str",
    "currency": "str",
    "notes": "str",

    "title": "str",
    "body": "str",
    "source": "str",
    "timestamp": "datetime",

    "price": "float",
    "amount": "float",
    "value": "float",
    "holding_before": "float",
    "holding_after": "float",
    "share_percentage_before": "float",
    "share_percentage_after": "float",
    "share_percentage_transaction": "float",

    "transactions": "list",

    "document_median_price": "float",
    "market_reference": "dict",

    "total_shares_model": "float",
    "delta_pp_model": "float",
    "pp_after_model": "float",
    "percent_discrepancy": "bool",
    "discrepancy_pp": "float",

    "suspicious_price_level": "bool",
    "needs_review": "bool",
    "skip_reason": "str",
    "reasons": "list",
    "announcement": "dict",

    "tags": "list",
    "tickers": "list",
    "sector": "str",
    "sub_sector": "str",
    "price_transaction": "dict",

    "announcement_published_at": "datetime",
    "source_type": "str",
    "doc_id": "str",
    "downloaded_pdf_path": "str",
}

# =====================================================================================
# Helpers: coercion
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
            fx = float(str(x).replace(",", ""))
            return int(fx)
        except Exception:
            return None

def _to_bool(x: Any) -> Optional[bool]:
    if isinstance(x, bool):
        return x
    if x in (None, "",):
        return None
    s = str(x).strip().lower()
    if s in ("1", "true", "yes", "y", "t"):
        return True
    if s in ("0", "false", "no", "n", "f"):
        return False
    return None

def _to_str(x: Any) -> Optional[str]:
    if x is None:
        return None
    return str(x)

def _to_list(x: Any) -> Optional[List[Any]]:
    if x is None:
        return None
    return x if isinstance(x, list) else None

def _to_dict(x: Any) -> Optional[Dict[str, Any]]:
    if x is None:
        return None
    return x if isinstance(x, dict) else None

def _to_date_iso(x: Any) -> Optional[str]:
    """
    Normalisasi ke 'YYYY-MM-DD' bila memungkinkan.
    """
    if x is None or x == "":
        return None
    try:
        if isinstance(x, str):
            # sudah ISO YYYY-MM-DD / YYYY-MM-DDTHH:MM:SS
            return x[:10] if len(x) >= 10 else x
        if isinstance(x, (datetime, date)):
            return x.strftime("%Y-%m-%d")
    except Exception:
        pass
    return _to_str(x)

def _to_datetime_iso(x: Any) -> Optional[str]:
    """
    Normalisasi ke ISO 'YYYY-MM-DDTHH:MM:SS' bila memungkinkan.
    """
    if x is None or x == "":
        return None
    if isinstance(x, datetime):
        return x.isoformat()
    if isinstance(x, str):
        # anggap sudah iso-ish, kembalikan apa adanya
        return x
    try:
        if isinstance(x, date):
            return datetime(x.year, x.month, x.day).isoformat()
    except Exception:
        pass
    return _to_str(x)

def _coerce_value(col: str, val: Any) -> Any:
    t = COLUMN_TYPES.get(col)
    if t == "float":
        return _to_float(val)
    if t == "int":
        return _to_int(val)
    if t == "bool":
        return _to_bool(val)
    if t == "str":
        return _to_str(val)
    if t == "list":
        return _to_list(val)
    if t == "dict":
        return _to_dict(val)
    if t == "date":
        return _to_date_iso(val)
    if t == "datetime":
        return _to_datetime_iso(val)
    return val

# =====================================================================================
# Helpers: domain normalizers (kebab, array/string parsing, etc.)
# =====================================================================================

_KNONALNUM = re.compile(r"[^0-9A-Za-z]+")  # untuk kebab

def _kebab(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    t = _KNONALNUM.sub("-", str(s).strip()).strip("-").lower()
    return t or None

def _first_scalar(x: Any) -> Optional[str]:
    """
    Ambil elemen pertama bila list-like, kalau string/angka → jadikan string,
    kalau kosong → None.
    """
    if x is None:
        return None
    if isinstance(x, list):
        for it in x:
            if it not in (None, "", []):
                return str(it)
        return None
    return str(x)

def _parse_json_list_or_csv(x: Any) -> Optional[List[str]]:
    """
    Untuk tags/tickers: terima list, string JSON array, atau CSV.
    """
    if x is None:
        return None
    if isinstance(x, list):
        return [str(t).strip() for t in x if str(t).strip()]
    if isinstance(x, str):
        s = x.strip()
        # coba JSON array
        if s.startswith("[") and s.endswith("]"):
            try:
                arr = json.loads(s)
                if isinstance(arr, list):
                    return [str(t).strip() for t in arr if str(t).strip()]
            except Exception:
                pass
        # fallback CSV
        return [t.strip() for t in s.split(",") if t.strip()]
    return None

def _parse_json_dict(x: Any) -> Optional[Dict[str, Any]]:
    """
    Untuk price_transaction: terima dict atau string JSON object.
    """
    if x is None:
        return None
    if isinstance(x, dict):
        return x
    if isinstance(x, str):
        s = x.strip()
        if s.startswith("{") and s.endswith("}"):
            try:
                obj = json.loads(s)
                if isinstance(obj, dict):
                    return obj
            except Exception:
                return None
    return None

# =====================================================================================
# Backfills & business rules (required fields, price/value)
# =====================================================================================

def _ensure_symbol_jk(sym: Optional[str]) -> Optional[str]:
    if sym is None:
        return None
    s = str(sym).strip().upper()
    if not s:
        return None
    return s if s.endswith(".JK") else f"{s}.JK"

def _backfill_transaction_date(r: Dict[str, Any]) -> Optional[str]:
    """
    Urutan fallback: transaction_date → date → announcement_published_at[:10] → timestamp[:10]
    """
    for k in ("transaction_date", "date", "announcement_published_at", "timestamp"):
        v = r.get(k)
        if v:
            iso = _to_date_iso(v)
            if iso:
                return iso[:10]
    return None

def _derive_type(r: Dict[str, Any]) -> Optional[str]:
    """
    type → transaction_type → derive from holding_after vs holding_before
    """
    t = str(r.get("type") or r.get("transaction_type") or "").strip().lower()
    if t in {"buy", "sell", "transfer", "other"}:
        return t if t != "" else None
    hb = _to_float(r.get("holding_before"))
    ha = _to_float(r.get("holding_after"))
    if isinstance(hb, (int, float)) and isinstance(ha, (int, float)):
        if ha > hb:
            return "buy"
        if ha < hb:
            return "sell"
    return None

def _derive_amount(r: Dict[str, Any]) -> Optional[float]:
    """
    amount → amount_transaction → abs(holding_after - holding_before)
    """
    amt = r.get("amount")
    if amt not in (None, ""):
        return _to_float(amt)
    amt2 = r.get("amount_transaction")
    if amt2 not in (None, ""):
        return _to_float(amt2)
    hb = _to_float(r.get("holding_before"))
    ha = _to_float(r.get("holding_after"))
    if isinstance(hb, (int, float)) and isinstance(ha, (int, float)):
        try:
            return float(abs(ha - hb))
        except Exception:
            return None
    return None

def _backfill_price_value(r: Dict[str, Any]) -> None:
    """
    Isi price & transaction_value dari market_reference.ref_price bila null.
    Tidak menyentuh price_transaction.{...}
    """
    mr = r.get("market_reference") or {}
    refp = _to_float(mr.get("ref_price"))
    # price
    if r.get("price") in (None, "", 0, 0.0):
        if refp is not None:
            r["price"] = refp
    # transaction_value
    if r.get("transaction_value") in (None, "", 0, 0.0):
        amt = _to_float(r.get("amount") if "amount" in r else r.get("amount_transaction"))
        prc = _to_float(r.get("price"))
        if amt is not None and prc is not None:
            r["transaction_value"] = prc * amt

def _flatten_sub_sector(r: Dict[str, Any]) -> None:
    """
    Jika sub_sector list → gabung jadi string. Sesudah itu sector/sub_sector → kebab-case.
    """
    if isinstance(r.get("sub_sector"), list):
        r["sub_sector"] = ", ".join(str(x) for x in r["sub_sector"] if x is not None) or None
    # kebab-case konsisten
    r["sector"] = _kebab(_first_scalar(r.get("sector")))
    r["sub_sector"] = _kebab(_first_scalar(r.get("sub_sector")))

# =====================================================================================
# Public API
# =====================================================================================

def clean_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    - Hanya pertahankan kolom di ALLOWED_COLUMNS
    - Coerce tipe sesuai COLUMN_TYPES
    - Backfill business rules (symbol .JK, required fields, price/value)
    - Post-normalization:
        * sector/sub_sector: flatten list → kebab-case string
        * tags: lower, unique, list[str]
        * tickers: upper list atau None
        * price_transaction: dict atau None (parse string JSON bila perlu)
        * announcement_published_at: jika kosong → mirror dari timestamp
    """
    out: Dict[str, Any] = {}
    src = row or {}

    # 1) salin & coerce basic sesuai ALLOWED/COLUMN_TYPES
    for k in ALLOWED_COLUMNS:
        if k in src:
            out[k] = _coerce_value(k, src.get(k))

    # 2) symbol: double safety .JK
    out["symbol"] = _ensure_symbol_jk(out.get("symbol") or src.get("symbol") or src.get("issuer_code") or src.get("ticker"))

    # 3) Required fields backfill
    out["transaction_date"] = _backfill_transaction_date({**src, **out})
    out["type"] = _derive_type({**src, **out})
    out["amount"] = _derive_amount({**src, **out})

    # 4) price/value backfill (row-level)
    #    menjaga rule: price_transaction tetap tidak diubah
    #    gunakan union sumber agar akses holding/amount dsb tetap lengkap
    tmp = {**src, **out}
    _backfill_price_value(tmp)
    # sinkronkan hasilnya ke out
    if "price" in tmp and out.get("price") in (None, "", 0, 0.0):
        out["price"] = tmp.get("price")
    if "transaction_value" in tmp and out.get("transaction_value") in (None, "", 0, 0.0):
        out["transaction_value"] = tmp.get("transaction_value")

    # 5) sector/sub_sector flatten & kebab
    #    (gunakan out lebih dulu; fallback ke src)
    if "sector" not in out and "sector" in src:
        out["sector"] = src["sector"]
    if "sub_sector" not in out and "sub_sector" in src:
        out["sub_sector"] = src["sub_sector"]
    _flatten_sub_sector(out)

    # 6) tags (list[str] lowercase)
    tags = _parse_json_list_or_csv(src.get("tags") if "tags" in src else out.get("tags"))
    if tags is not None:
        out["tags"] = sorted({t.lower() for t in tags if t})

    # 7) tickers (list[str] uppercase) — kosongkan jika hasil akhirnya empty
    tickers = _parse_json_list_or_csv(src.get("tickers") if "tickers" in src else out.get("tickers"))
    if tickers is not None:
        tickers = [t.upper() for t in tickers if t]
        out["tickers"] = tickers or None

    # 8) price_transaction (jsonb/dict)
    pt = _parse_json_dict(src.get("price_transaction") if "price_transaction" in src else out.get("price_transaction"))
    out["price_transaction"] = pt

    # 9) fallback: announcement_published_at ← timestamp (jika kosong)
    if not out.get("announcement_published_at") and out.get("timestamp"):
        out["announcement_published_at"] = out.get("timestamp")

    return out


def clean_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [clean_row(r) for r in (rows or [])]


__all__ = [
    "ALLOWED_COLUMNS",
    "REQUIRED_COLUMNS",
    "clean_row",
    "clean_rows",
]
