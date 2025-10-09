from __future__ import annotations
import json, logging, re
from typing import Any, Dict, Iterable, List, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

# --- timezone support for WIB (+07:00) ---
try:
    import zoneinfo
    JKT = zoneinfo.ZoneInfo("Asia/Jakarta")
except Exception:
    JKT = None  # fallback: keep strings as-is if tz module unavailable

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
    # --- NEW: published date in WIB stored to DB ---
    "announcement_published_at",
}

# Wajib ada (menyesuaikan CSV kamu—tidak ada `transaction_date` di CSV)
REQUIRED_COLUMNS: set[str] = {"title", "symbol", "sector", "timestamp"}

# --- helpers: snake case ---
_S1 = re.compile(r"([a-z0-9])([A-Z])")
_S2 = re.compile(r"[^a-zA-Z0-9]+")

def _to_snake(s: str) -> str:
    s = _S1.sub(r"\1_\2", s)
    s = _S2.sub("_", s)
    return s.strip("_").lower()

def _snake_shallow(d: Dict[str, Any]) -> Dict[str, Any]:
    return {_to_snake(k): v for k, v in d.items()}

# --- helpers: collection/number coercion ---
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

def _to_int(v: Any) -> Optional[int]:
    if v is None or v == "": return None
    try:
        return int(str(v).replace(",", "").strip())
    except Exception:
        return None

def _to_float(v: Any) -> Optional[float]:
    if v is None or v == "": return None
    try:
        return float(str(v).replace(",", "").strip())
    except Exception:
        return None

def _filter_allowed(row: Dict[str, Any], allowed: Optional[Iterable[str]]) -> Dict[str, Any]:
    if not allowed: return row
    allow = set(allowed)
    return {k: v for k, v in row.items() if k in allow}

# --- helpers: datetime normalization to ISO8601 WIB ---
def _parse_dt_wib(dtstr: Optional[str]) -> Optional[datetime]:
    """Parse common formats; attach Asia/Jakarta tz if naive."""
    if not dtstr:
        return None
    # try common explicit formats
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y%m%d-%H%M%S"):
        try:
            dt = datetime.strptime(dtstr, fmt)
            if JKT and dt.tzinfo is None:
                dt = dt.replace(tzinfo=JKT)
            return dt
        except Exception:
            pass
    # try ISO8601
    try:
        dt = datetime.fromisoformat(dtstr)
        if JKT and dt.tzinfo is None:
            dt = dt.replace(tzinfo=JKT)
        return dt
    except Exception:
        return None

def _iso_wib(dt: Optional[datetime]) -> Optional[str]:
    """Return ISO8601 with +07:00 offset, drop microseconds."""
    if dt is None:
        return None
    if JKT:
        dt = dt.astimezone(JKT)
    return dt.replace(microsecond=0).isoformat()

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
            # biarkan string; PostgREST akan menolak kalau tidak valid JSON
            pass

    # angka: int
    for k in ("holding_before", "holding_after", "amount_transaction"):
        r[k] = _to_int(r.get(k)) if k in r else None

    # angka: float
    for k in ("price", "transaction_value",
              "share_percentage_before", "share_percentage_after", "share_percentage_transaction"):
        r[k] = _to_float(r.get(k)) if k in r else None

    # tickers -> None (sesuai permintaan)
    r["tickers"] = None

    # Normalize timestamp (publish time) to ISO8601 WIB string (keep original if unparsable)
    if "timestamp" in r and isinstance(r.get("timestamp"), str):
        ts_parsed = _parse_dt_wib(r["timestamp"])
        r["timestamp"] = _iso_wib(ts_parsed) or r["timestamp"]

    # NEW: normalize announcement_published_at to ISO8601 WIB (+07:00)
    if "announcement_published_at" in r:
        v = r.get("announcement_published_at")
        if v is None or v == "":
            r["announcement_published_at"] = None
        elif isinstance(v, str):
            r["announcement_published_at"] = _iso_wib(_parse_dt_wib(v)) or v  # keep original string if parsing fails
        elif isinstance(v, datetime):
            r["announcement_published_at"] = _iso_wib(v)
        else:
            # last resort: string cast
            try:
                r["announcement_published_at"] = _iso_wib(_parse_dt_wib(str(v))) or str(v)
            except Exception:
                r["announcement_published_at"] = str(v)

    # If announcement_published_at missing, mirror from timestamp (per your directive)
    if not r.get("announcement_published_at"):
        ts = r.get("timestamp")
        if isinstance(ts, str):
            r["announcement_published_at"] = _iso_wib(_parse_dt_wib(ts)) or ts

    # pastikan required fields tidak kosong (biar cepat ketahuan)
    for k in REQUIRED_COLUMNS:
        if not r.get(k):
            logger.debug("Missing required field %s on row with title=%r", k, r.get("title"))
            logger.warning("DROP row: missing %s (title=%r, symbol=%r, sector=%r, timestamp=%r)", k, r.get("title"), r.get("symbol"), r.get("sector"), r.get("timestamp"))

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
