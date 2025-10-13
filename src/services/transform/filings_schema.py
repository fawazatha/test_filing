# src/services/filings_schema.py
from __future__ import annotations
from typing import Any, Dict, List, Optional
from datetime import datetime, date

# =====================================================================================
# Allowed & Required Columns
# =====================================================================================
# NOTE:
# - Required columns mempertahankan skema lama (sesuaikan kalau sebelumnya berbeda).
# - Kolom audit/alerts BARU ditambahkan ke ALLOWED_COLUMNS supaya tidak dibuang saat clean_rows().
# - Tipe disederhanakan agar robust (mis. float untuk angka, dict/list untuk nested).
# =====================================================================================

REQUIRED_COLUMNS = [
    # inti identitas filing
    "symbol",
    "holder_name",
    "transaction_date",  # ISO or YYYY-MM-DD
    "type",              # buy|sell|transfer|other
    # angka basic (tetap toleran bila ada yang None, pengecekan ada di level lain)
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

    # --- konteks dokumen & pasar (BARU) ---
    "document_median_price",   # float
    "market_reference",        # dict: {ref_price, ref_type, asof_date, n_days, freshness_days}

    # --- audit kepemilikan model vs PDF (BARU, P0-5) ---
    "total_shares_model",      # float
    "delta_pp_model",          # float (pp)
    "pp_after_model",          # float (pp)
    "percent_discrepancy",     # bool
    "discrepancy_pp",          # float (pp selisih)

    # --- flags & reasons (BARU, P0-4/P0-A/P1-6) ---
    "suspicious_price_level",  # bool
    "needs_review",            # bool
    "skip_reason",             # str (e.g., suspicious_price_level, percent_discrepancy, stale_price, ...)
    "reasons",                 # list of reason objects
    "announcement",            # dict {id,title,url,pdf_url,source_type}

    # --- tags / klasifikasi (existing) ---
    "tags",

    # --- lain-lain yang umum muncul di pipeline lama ---
    "announcement_published_at",
    "source_type",
    "doc_id",
    "downloaded_pdf_path",
]

# Map tipe sederhana untuk normalisasi
# 'float' → angka float; 'int' → int; 'bool' → bool; 'str' → string; 'list' → list; 'dict' → dict; 'date'/'datetime' → ISO str
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
        return float(x)
    except Exception:
        return None

def _to_int(x: Any) -> Optional[int]:
    if x is None or x == "":
        return None
    try:
        return int(x)
    except Exception:
        try:
            fx = float(x)
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
    s = str(x)
    return s

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
        # sudah ISO YYYY-MM-DD
        if isinstance(x, str) and len(x) >= 10:
            return x[:10]
        if isinstance(x, (datetime, date)):
            return x.strftime("%Y-%m-%d")
    except Exception:
        pass
    # fallback: biarkan apa adanya (biar tidak buang info)
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
        # jika sudah ISO atau mirip, kembalikan apa adanya
        return x
    try:
        # kalau date → jam 00:00
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
    # default: jangan buang
    return val

# =====================================================================================
# Public API
# =====================================================================================

def clean_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    - Hanya pertahankan kolom di ALLOWED_COLUMNS
    - Coerce tipe sesuai COLUMN_TYPES
    - Jangan buang nested object (reasons/announcement/market_reference/transactions)
    """
    out: Dict[str, Any] = {}
    for k in ALLOWED_COLUMNS:
        if k in row:
            out[k] = _coerce_value(k, row.get(k))
    return out

def clean_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [clean_row(r) for r in (rows or [])]

__all__ = [
    "ALLOWED_COLUMNS",
    "REQUIRED_COLUMNS",
    "clean_row",
    "clean_rows",
]
