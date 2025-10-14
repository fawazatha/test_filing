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
    return str(x)

def _to_list(x: Any) -> Optional[List[Any]]:
    if x is None:
        return None
    return x if isinstance(x, list) else None

def _to_dict(x: Any) -> Optional[Dict[str, Any]]:
    if x is None:
        return None
    return x if isinstance(x, dict) else None

# --- date/datetime coercer ---
def _to_date_iso(x: Any) -> Optional[str]:
    """
    Normalisasi ke 'YYYY-MM-DD' bila memungkinkan.
    """
    if x is None or x == "":
        return None

    # cepat: string ISO-like → potong 10
    if isinstance(x, str):
        s = x.strip()
        if len(s) >= 10 and re.match(r"\d{4}-\d{2}-\d{2}", s):
            return s[:10]

    # coba beberapa format umum (ID/EN)
    candidates = [
        "%Y-%m-%d",
        "%d-%m-%Y",
        "%d/%m/%Y",
        "%d %B %Y",
        "%d %b %Y",
        "%d %B %y",
        "%d %b %y",
        "%B %d, %Y",
        "%b %d, %Y",
    ]
    if isinstance(x, str):
        for fmt in candidates:
            try:
                dt = datetime.strptime(x.strip(), fmt)
                return dt.strftime("%Y-%m-%d")
            except Exception:
                pass
    if isinstance(x, (datetime, date)):
        return x.strftime("%Y-%m-%d")

    # last resort: string apa adanya
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
# Extra helpers for required fields
# =====================================================================================

def _normalize_symbol(sym: Optional[str], issuer_code: Optional[str] = None) -> Optional[str]:
    s = (sym or issuer_code or "")
    s = str(s).strip().upper()
    if not s:
        return None
    return s if s.endswith(".JK") else f"{s}.JK"

def _choose_type(src: Dict[str, Any]) -> Optional[str]:
    t = (src.get("type") or src.get("transaction_type") or "").strip().lower()
    if t in {"buy", "sell", "transfer", "other"}:
        return t
    # fallback dari transactions: ambil mayoritas/pertama
    txs = src.get("transactions") or []
    if isinstance(txs, list) and txs:
        types = [str((tx.get("type") or "")).strip().lower() for tx in txs if isinstance(tx, dict)]
        types = [x for x in types if x]
        if types:
            # jika campur, pakai yang pertama; kalau tidak ada di set, pakai 'other'
            t0 = types[0]
            return t0 if t0 in {"buy", "sell", "transfer", "other"} else "other"
    return None

def _parse_date_any(s: str) -> Optional[str]:
    if not s:
        return None
    # sudah ISO?
    if re.match(r"^\d{4}-\d{2}-\d{2}", s):
        return s[:10]
    # Coba berbagai format umum
    fmts = [
        "%d %B %Y", "%d %b %Y",
        "%B %d, %Y", "%b %d, %Y",
        "%d-%m-%Y", "%d/%m/%Y",
        "%Y-%m-%d",
    ]
    for fmt in fmts:
        try:
            dt = datetime.strptime(s.strip(), fmt)
            return dt.strftime("%Y-%m-%d")
        except Exception:
            continue
    return None

def _choose_tx_date(src: Dict[str, Any]) -> Optional[str]:
    # 1) kalau sudah ada transaction_date → normalisasi
    td = src.get("transaction_date")
    td = _to_date_iso(td) if td else None
    if td:
        return td

    # 2) dari transactions: ambil tanggal maksimum (paling baru)
    txs = src.get("transactions") or []
    dates: List[str] = []
    if isinstance(txs, list):
        for tx in txs:
            if not isinstance(tx, dict):
                continue
            d = tx.get("date") or tx.get("transaction_date")
            if isinstance(d, str):
                iso = _parse_date_any(d)
                if iso:
                    dates.append(iso)
    if dates:
        return max(dates)

    # 3) fallback: dari timestamp (mirror di normalisasi)
    ts = src.get("timestamp")
    if ts:
        iso = _to_datetime_iso(ts)
        if isinstance(iso, str) and len(iso) >= 10:
            return iso[:10]

    return None

def _choose_amount(src: Dict[str, Any]) -> Optional[float]:
    # prioritas: amount (top-level), amount_transaction, amount_transacted
    amt = src.get("amount")
    amt = _to_float(amt) if amt is not None else None
    if amt is not None:
        return float(amt)

    for k in ("amount_transaction", "amount_transacted"):
        v = _to_float(src.get(k))
        if v is not None:
            return float(v)

    # fallback: sum dari transactions.amount
    txs = src.get("transactions") or []
    if isinstance(txs, list) and txs:
        total = 0.0
        found = False
        for tx in txs:
            if not isinstance(tx, dict):
                continue
            a = _to_float(tx.get("amount"))
            if a is not None:
                total += float(a)
                found = True
        if found:
            return total

    return None

# =====================================================================================
# Public API
# =====================================================================================

def clean_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    - Hanya pertahankan kolom di ALLOWED_COLUMNS
    - Coerce tipe sesuai COLUMN_TYPES
    - Post-normalization & autofill:
        * symbol: uppercase + suffix .JK jika belum ada (fallback issuer_code)
        * holder_name: fallback ke holder_name_raw
        * type: fallback dari transaction_type atau transactions
        * amount: fallback dari amount_transaction/amount_transacted/sum(transactions.amount)
        * transaction_date: fallback dari max(transactions.date) → timestamp[:10]
        * price: bila None dan market_reference.ref_price ada → pakai ref_price
        * value/transaction_value: bila None dan price*amount tersedia → hitung
        * sector/sub_sector: first-scalar → kebab-case
        * tags: lower, unique, list[str]
        * tickers: upper, list[str] atau None
        * price_transaction: dict atau None (parse string JSON bila perlu)
        * announcement_published_at: jika kosong → mirror dari timestamp
    """
    out: Dict[str, Any] = {}
    src = row or {}

    # 1) salin & coerce basic sesuai ALLOWED/COLUMN_TYPES
    for k in ALLOWED_COLUMNS:
        if k in src:
            out[k] = _coerce_value(k, src.get(k))

    # 2) symbol normalisasi + issuer_code fallback
    out["symbol"] = _normalize_symbol(out.get("symbol"), out.get("issuer_code"))

    # 3) holder_name fallback
    if not out.get("holder_name"):
        hn = src.get("holder_name_raw")
        out["holder_name"] = str(hn) if hn else out.get("holder_name")

    # 4) type fallback
    if not out.get("type"):
        t = _choose_type(src)
        out["type"] = t or None

    # 5) amount fallback
    if out.get("amount") is None:
        out["amount"] = _choose_amount(src)

    # 6) transaction_date fallback
    if not out.get("transaction_date"):
        out["transaction_date"] = _choose_tx_date(src)

    # 7) sector/sub_sector (pastikan STRING & kebab)
    sec_raw = _first_scalar(src.get("sector") if "sector" in src else out.get("sector"))
    ssec_raw = _first_scalar(src.get("sub_sector") if "sub_sector" in src else out.get("sub_sector"))
    out["sector"] = _kebab(sec_raw) if sec_raw else (out.get("sector") and _kebab(out.get("sector")))
    out["sub_sector"] = _kebab(ssec_raw) if ssec_raw else (out.get("sub_sector") and _kebab(out.get("sub_sector")))

    # 8) tags (list[str] lowercase)
    tags = _parse_json_list_or_csv(src.get("tags") if "tags" in src else out.get("tags"))
    if tags is not None:
        out["tags"] = sorted({t.lower() for t in tags if t})

    # 9) tickers (list[str] uppercase) — kosongkan jika hasil akhirnya empty
    tickers = _parse_json_list_or_csv(src.get("tickers") if "tickers" in src else out.get("tickers"))
    if tickers is not None:
        tickers = [t.upper() for t in tickers if t]
        out["tickers"] = tickers or None

    # 10) price_transaction (jsonb/dict)
    pt = _parse_json_dict(src.get("price_transaction") if "price_transaction" in src else out.get("price_transaction"))
    out["price_transaction"] = pt

    # 11) Fallback price dari market_reference.ref_price
    price = _to_float(out.get("price"))
    if price is None:
        mr = out.get("market_reference") if isinstance(out.get("market_reference"), dict) else None
        ref = _to_float((mr or {}).get("ref_price")) if mr else None
        if ref is not None:
            price = ref
            out["price"] = price

    # 12) Hitung value/transaction_value bila kosong dan price*amount ada
    val = _to_float(out.get("value"))
    tval = _to_float(out.get("transaction_value"))
    amt = _to_float(out.get("amount"))
    if (val is None or tval is None) and (price is not None and amt is not None):
        computed = float(price) * float(amt)
        if val is None:
            out["value"] = computed
        if tval is None:
            out["transaction_value"] = computed

    # 13) fallback: announcement_published_at ← timestamp (jika kosong)
    if not out.get("announcement_published_at") and out.get("timestamp"):
        out["announcement_published_at"] = out.get("timestamp")

    # 14) normalisasi akhir tipe kolom (pastikan bentuk sesuai COLUMN_TYPES)
    for col, typ in COLUMN_TYPES.items():
        if col in out and out[col] is not None:
            out[col] = _coerce_value(col, out[col])

    return out


def clean_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [clean_row(r) for r in (rows or [])]


__all__ = [
    "ALLOWED_COLUMNS",
    "REQUIRED_COLUMNS",
    "clean_row",
    "clean_rows",
]
