from __future__ import annotations

import os
import json
import logging
from typing import Any, Dict, List, Optional
from datetime import datetime

try:
    import zoneinfo
    JKT = zoneinfo.ZoneInfo("Asia/Jakarta")
except Exception:
    JKT = None

from .provider import get_company_info

log = logging.getLogger("filings.processors")

# ----------------------
# Time helpers (WIB)
# ----------------------
def _parse_dt_wib(dtstr: Optional[str]) -> Optional[datetime]:
    if not dtstr:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y%m%d-%H%M%S"):
        try:
            dt = datetime.strptime(str(dtstr), fmt)
            return dt.replace(tzinfo=JKT) if (JKT and dt.tzinfo is None) else dt
        except Exception:
            pass
    try:
        dt = datetime.fromisoformat(str(dtstr).replace("Z", "+00:00"))
        return dt.replace(tzinfo=JKT) if (JKT and dt.tzinfo is None) else dt
    except Exception:
        return None

def _iso_wib(dt: Optional[datetime]) -> Optional[str]:
    if dt is None:
        return None
    if JKT:
        dt = dt.astimezone(JKT)
    return dt.replace(microsecond=0).isoformat()

# ----------------------
# CompanyInfo access helper (dict OR object)
# ----------------------
def _ci_get(info: Any, key: str, default: Any = None) -> Any:
    """
    Safely read company info field from either a dict or an object/dataclass.
    Tries: exact attr -> lowercased attr -> dict.get
    """
    if info is None:
        return default
    if isinstance(info, dict):
        return info.get(key, default)
    # object/dataclass
    if hasattr(info, key):
        return getattr(info, key)
    lk = key.lower()
    if hasattr(info, lk):
        return getattr(info, lk)
    # some dataclasses store sub_sector as list, sector as str, etc.
    # last resort: to dict via __dict__ if available
    d = getattr(info, "__dict__", None)
    if isinstance(d, dict):
        return d.get(key, d.get(lk, default))
    return default

# ----------------------
# Schema robustness helpers (IDX vs Non-IDX)
# ----------------------
def _derive_symbol(raw: Dict[str, Any]) -> Optional[str]:
    # Direct
    s = raw.get("symbol") or raw.get("ticker") or raw.get("stock_code")
    if isinstance(s, str) and s.strip():
        s = s.strip().upper()
        if not s.endswith(".JK") and len(s) <= 6:
            s = f"{s}.JK"
        return s

    # IDX: issuer_code
    ic = raw.get("issuer_code") or raw.get("issuer") or raw.get("kode_emiten")
    if isinstance(ic, str) and ic.strip():
        ic = ic.strip().upper()
        if not ic.endswith(".JK"):
            ic = f"{ic}.JK"
        return ic

    # tickers array
    t = raw.get("tickers") or raw.get("symbols")
    if isinstance(t, list) and t:
        v = next((str(x).strip().upper() for x in t if x), None)
        if v and not v.endswith(".JK") and len(v) <= 6:
            v = f"{v}.JK"
        if v:
            return v

    # fallback: map from company name via provider (handle object return)
    cname = raw.get("company_name") or raw.get("company_name_raw")
    if cname:
        info = get_company_info(None, company_name=str(cname))
        if info:
            ss = _ci_get(info, "symbol")
            if isinstance(ss, str) and ss.strip():
                return ss.strip().upper()
    return None

def _guess_source(raw: Dict[str, Any]) -> str:
    # prefer explicit URLs
    for k in ("pdf_url", "source", "url", "pdf_link", "link"):
        v = raw.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    # else filename-ish
    for k in ("filename", "file", "pdf_file", "pdf_path", "download_name"):
        fn = raw.get(k)
        if isinstance(fn, str) and fn.strip():
            if "/" in fn or "\\" in fn or fn.lower().endswith(".pdf"):
                return fn
            return f"downloads/idx-format/{fn}"
    return ""

def _pick_timestamp(raw: Dict[str, Any], downloads_meta_map: Optional[Dict[str, Any]], source: str) -> Optional[str]:
    for k in (
        "timestamp", "published_at", "published_at_wib",
        "announcement_published_at", "announcement_time", "publish_time",
    ):
        v = raw.get(k)
        if v:
            return _iso_wib(_parse_dt_wib(v)) or str(v)
    if downloads_meta_map:
        key = source or raw.get("filename") or raw.get("pdf_file")
        if key:
            meta = downloads_meta_map.get(key) or downloads_meta_map.get(os.path.basename(str(key)))
            if meta and isinstance(meta, dict):
                v = meta.get("timestamp") or meta.get("published_at")
                if v:
                    return _iso_wib(_parse_dt_wib(v)) or str(v)
    return None

def _enrich_company(row: Dict[str, Any]) -> None:
    sym = row.get("symbol")
    if not sym:
        return
    info = get_company_info(sym)
    if not info:
        return
    # use _ci_get to support dict OR CompanyInfo object
    row.setdefault("company_name", _ci_get(info, "company_name") or "")
    row.setdefault("sector", _ci_get(info, "sector") or "")
    ss = _ci_get(info, "sub_sector")
    if row.get("sub_sector") is None:
        if isinstance(ss, list):
            row["sub_sector"] = ss
        elif isinstance(ss, str) and ss.strip():
            row["sub_sector"] = [ss]
        else:
            row["sub_sector"] = []

# ----------------------
# Main builder
# ----------------------
def build_row(raw: Dict[str, Any], downloads_meta_map: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Coerce a single parsed object into a normalized *pre-clean* filing row.
    Tidak drop row; validasi final di normalizer.
    Tahan schema beda untuk IDX (issuer_code/published_at/pdf_link/filename).
    """
    row: Dict[str, Any] = {}

    # surface fields
    row["title"] = raw.get("title") or raw.get("subject") or raw.get("headline") or ""
    row["source"] = _guess_source(raw)
    row["holder_type"] = raw.get("holder_type")
    row["holder_name"] = raw.get("holder_name")
    row["transaction_type"] = raw.get("transaction_type") or raw.get("type")

    # numeric-ish
    row["holding_before"] = raw.get("holding_before") or raw.get("holdings_before") or raw.get("previous_holding")
    row["holding_after"]  = raw.get("holding_after")  or raw.get("holdings_after")  or raw.get("new_holding")
    row["amount_transaction"] = raw.get("amount_transaction") or raw.get("amount") or raw.get("volume")
    row["price"] = raw.get("price")
    row["transaction_value"] = raw.get("transaction_value")
    row["price_transaction"] = raw.get("price_transaction") or raw.get("prices")  # str/dict ok

    # percentages
    row["share_percentage_before"] = raw.get("share_percentage_before")
    row["share_percentage_after"] = raw.get("share_percentage_after")
    row["share_percentage_transaction"] = raw.get("share_percentage_transaction")

    # UID
    row["uid"] = raw.get("uid") or raw.get("UID") or raw.get("id")

    # symbol & company
    row["symbol"] = raw.get("symbol") or _derive_symbol(raw)
    row["company_name"] = raw.get("company_name") or raw.get("company_name_raw")

    # sector / sub_sector (may be enriched later)
    row["sector"] = raw.get("sector")
    row["sub_sector"] = raw.get("sub_sector")

    # timestamp (publish time) → ISO WIB
    ts_iso = _pick_timestamp(raw, downloads_meta_map, row["source"])
    row["timestamp"] = ts_iso or raw.get("timestamp")  # keep raw if unparsable

    # announcement_published_at mirrors timestamp (WIB)
    row["announcement_published_at"] = row["timestamp"]

    # enrich (now safe for dict OR CompanyInfo)
    _enrich_company(row)

    return row

def process_all(parsed_lists: List[List[Dict[str, Any]]], downloads_meta_map: Dict[str, Any] | None) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    failed = 0
    for ci, chunk in enumerate(parsed_lists or []):
        if not chunk:
            continue
        for ri, raw in enumerate(chunk):
            try:
                row = build_row(raw, downloads_meta_map)
                out.append(row)
            except Exception as e:
                failed += 1
                t = raw.get("title") or raw.get("subject") or ""
                s = raw.get("symbol") or raw.get("issuer_code") or ""
                log.warning("[PROCESS] drop (exc) chunk=%d idx=%d title=%r symbol=%r err=%s", ci, ri, t, s, e)
                continue
    if failed:
        log.warning("[PROCESS] total failed builds: %d", failed)
    return out
