# generate/filings/provider.py
from __future__ import annotations

import json
import logging
import os
import threading
from dataclasses import asdict, is_dataclass
from pathlib import Path
from statistics import median
from typing import Any, Dict, List, Optional, Tuple, Union

# Local imports (same package)
try:
    from .types import CompanyInfo, DownloadMeta
    from .config import (
        COMPANY_MAP_PATH,
        LATEST_PRICES_PATH,
        MARKET_REF_N_DAYS,
        SUGGEST_PRICE_RATIO,
    )
except Exception:  # pragma: no cover
    # Fallback for flat layout during testing
    from types import CompanyInfo, DownloadMeta  # type: ignore
    from config import (  # type: ignore
        COMPANY_MAP_PATH,
        LATEST_PRICES_PATH,
        MARKET_REF_N_DAYS,
        SUGGEST_PRICE_RATIO,
    )

logger = logging.getLogger(__name__)

# Optional: classifier (tetap robust kalau tak ada)
try:
    from parser.utils.transaction_classifier import TransactionClassifier as _TC  # type: ignore
except Exception:  # pragma: no cover
    _TC = None

# =============================================================================
# In-memory caches (thread-safe)
# =============================================================================
_lock = threading.RLock()

_company_map_raw: Optional[Dict[str, Any]] = None
_company_map_mtime: Optional[float] = None

# Fast indexes
_sym_index: Dict[str, Dict[str, Any]] = {}
_name_index: Dict[str, Dict[str, Any]] = {}

_prices_cache: Optional[Dict[str, Any]] = None
_prices_mtime: Optional[float] = None

# Allow multiple candidate paths (beberapa repo simpan beda lokasi)
COMPANY_MAP_PATHS: Tuple[str, ...] = (
    os.getenv("FILINGS_COMPANY_MAP", COMPANY_MAP_PATH if "COMPANY_MAP_PATH" in globals() else "data/company/company_map.json"),
    "data/company/company_map.hydrated.json",  # optional (hasil hydrate)
)

# Pastikan latest prices tidak menunjuk company_map.json
LATEST_PRICE_PATHS: Tuple[str, ...] = (
    os.getenv("FILINGS_LATEST_PRICES", LATEST_PRICES_PATH if "LATEST_PRICES_PATH" in globals() else "data/company/latest_prices.json"),
)

# =============================================================================
# Helpers
# =============================================================================
def _load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)

def _normalize_symbol(sym: Optional[str]) -> Optional[str]:
    if not sym:
        return None
    s = str(sym).strip().upper()
    if not s:
        return None
    return s if s.endswith(".JK") else f"{s}.JK"

def _sym_key_variants(k: str) -> List[str]:
    """Bangun beberapa varian key untuk index (dengan/tanpa .JK)."""
    s = str(k).strip().upper()
    out = set()
    if s:
        out.add(s)
        if s.endswith(".JK"):
            out.add(s[:-3])  # tanpa suffix
        else:
            out.add(f"{s}.JK")
    return list(out)

def _first_scalar(x: Any) -> Optional[str]:
    """
    Ambil elemen pertama bila list-like; kalau string/angka → string; selain itu → None.
    """
    if x is None:
        return None
    if isinstance(x, list):
        for it in x:
            if it not in (None, "", []):
                return str(it)
        return None
    if isinstance(x, (str, int, float)):
        s = str(x).strip()
        return s if s else None
    return None

def _kebab(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    import re
    t = re.sub(r"[^0-9A-Za-z]+", "-", s.strip()).strip("-").lower()
    return t or None

def _ensure_company_map_loaded() -> None:
    """Load company_map + build fast indexes (by symbol & name)."""
    global _company_map_raw, _company_map_mtime, _sym_index, _name_index
    with _lock:
        found: Optional[Path] = None
        for candidate in COMPANY_MAP_PATHS:
            if not candidate:
                continue
            p = Path(candidate)
            if p.exists():
                found = p
                break
        if not found:
            logger.debug("company_map not found in %s", COMPANY_MAP_PATHS)
            _company_map_raw = {}
            _sym_index = {}
            _name_index = {}
            _company_map_mtime = None
            return

        mtime = found.stat().st_mtime
        if _company_map_raw is not None and _company_map_mtime == mtime:
            return

        try:
            data = _load_json(found)
            # Terima bentuk {"map": {...}} atau dict langsung
            cmap = data.get("map") if isinstance(data, dict) else None
            if isinstance(cmap, dict):
                _company_map_raw = cmap
            elif isinstance(data, dict):
                _company_map_raw = data
            else:
                _company_map_raw = {}

            # rebuild indexes
            sym_idx: Dict[str, Dict[str, Any]] = {}
            name_idx: Dict[str, Dict[str, Any]] = {}

            for k, v in (_company_map_raw or {}).items():
                if not isinstance(v, dict):
                    continue
                # index by multiple symbol variants
                for key in _sym_key_variants(k):
                    sym_idx[key] = v
                # index by company_name (upper, trimmed)
                name = (v.get("company_name") or v.get("name") or "").strip().upper()
                if name:
                    name_idx[name] = v

            _sym_index = sym_idx
            _name_index = name_idx
            _company_map_mtime = mtime
            logger.info(
                "Loaded company map from %s (symbols=%d names=%d)",
                found, len(_sym_index), len(_name_index)
            )
        except Exception as e:  # pragma: no cover
            logger.warning("Failed loading company map from %s: %s", found, e)
            _company_map_raw = {}
            _sym_index = {}
            _name_index = {}
            _company_map_mtime = None

def _ensure_prices_loaded() -> None:
    global _prices_cache, _prices_mtime
    with _lock:
        found: Optional[Path] = None
        for candidate in LATEST_PRICE_PATHS:
            if not candidate:
                continue
            p = Path(candidate)
            if p.exists():
                found = p
                break
        if not found:
            logger.debug("latest_prices not found in %s", LATEST_PRICE_PATHS)
            _prices_cache = {}
            _prices_mtime = None
            return

        mtime = found.stat().st_mtime
        if _prices_cache is not None and _prices_mtime == mtime:
            return

        try:
            data = _load_json(found)
            # Terima {"prices": {...}} atau dict langsung
            if isinstance(data, dict) and "prices" in data and isinstance(data["prices"], dict):
                _prices_cache = data["prices"]
            elif isinstance(data, dict):
                _prices_cache = data
            else:
                _prices_cache = {}
            _prices_mtime = mtime
            logger.info("Loaded latest prices from %s (%d symbols)", found, len(_prices_cache or {}))
        except Exception as e:  # pragma: no cover
            logger.warning("Failed loading latest prices: %s", e)
            _prices_cache = {}
            _prices_mtime = None

def _today_iso() -> str:
    from datetime import date
    return date.today().isoformat()

def _days_between(d1: Optional[str], d2: Optional[str]) -> Optional[int]:
    if not d1 or not d2:
        return None
    from datetime import datetime
    try:
        a = datetime.fromisoformat(d1[:10])
        b = datetime.fromisoformat(d2[:10])
        return abs((a - b).days)
    except Exception:
        return None

# =============================================================================
# Public API
# =============================================================================
def get_company_info(symbol: Optional[str]) -> CompanyInfo:
    """
    Lookup company information untuk simbol. Robust pada kunci dengan/ tanpa '.JK'.
    Selalu kebab-case sector/sub_sector. Prefer isi dari map.
    """
    _ensure_company_map_loaded()

    info: Dict[str, Any] = {}
    sym_norm = _normalize_symbol(symbol)
    if sym_norm and sym_norm in _sym_index:
        info = _sym_index[sym_norm]
    elif symbol:
        # coba raw symbol (bila map disimpan tanpa .JK)
        raw = str(symbol).strip().upper()
        if raw in _sym_index:
            info = _sym_index[raw]

    # ambil variasi key & first scalar
    company_name = _first_scalar(info.get("company_name") or info.get("name"))
    sector_raw = (
        info.get("sector") or info.get("Sector")
    )
    subsec_raw = (
        info.get("sub_sector")
        or info.get("subsector")
        or info.get("Sub-Sector")
        or info.get("SubSector")
    )

    sector = _kebab(_first_scalar(sector_raw))
    sub_sector = _kebab(_first_scalar(subsec_raw))

    return CompanyInfo(
        company_name=company_name or "",
        sector=sector,
        sub_sector=sub_sector,
    )

def get_latest_price(symbol: Optional[str]) -> Optional[Dict[str, Any]]:
    """
    Return latest price info from local cache:
    { "close": float, "vwap": float?, "date": "YYYY-MM-DD" }
    """
    _ensure_prices_loaded()
    sym = _normalize_symbol(symbol)
    if not sym:
        return None
    entry = (_prices_cache or {}).get(sym) or (_prices_cache or {}).get(sym[:-3])  # allow no suffix
    if not isinstance(entry, dict):
        return None
    # Normalize keys
    close = entry.get("close") or entry.get("last") or entry.get("price") or entry.get("last_close_price")
    vwap = entry.get("vwap") or entry.get("VWAP")
    date_str = entry.get("date") or entry.get("asof") or entry.get("as_of") or entry.get("updated_on") or entry.get("latest_close_date")
    out: Dict[str, Any] = {}
    if close is not None:
        try:
            out["close"] = float(str(close).replace(",", ""))
        except Exception:
            pass
    if vwap is not None:
        try:
            out["vwap"] = float(str(vwap).replace(",", ""))
        except Exception:
            pass
    if date_str:
        out["date"] = str(date_str)[:10]
    return out or None

def get_market_reference(symbol: Optional[str], n_days: int = None) -> Optional[Dict[str, Any]]:
    """
    Compute market reference price untuk sanity checks:
      - Prefer N-day VWAP/median-close kalau ada series
      - Fallback ke latest close
    """
    _ensure_prices_loaded()
    if n_days is None:
        n_days = MARKET_REF_N_DAYS
    sym = _normalize_symbol(symbol)
    if not sym:
        return None
    entry = (_prices_cache or {}).get(sym) or (_prices_cache or {}).get(sym[:-3])
    if not isinstance(entry, dict):
        return None

    # Jika historical series tersedia: [{"date":..,"close":..,"vwap":..}, ...]
    series = entry.get("series")
    if isinstance(series, list) and series:
        last_n = series[-n_days:] if n_days > 0 else series[:]
        # prefer VWAP jika mayoritas tersedia
        v_list = [float(x["vwap"]) for x in last_n if isinstance(x, dict) and x.get("vwap") is not None]
        if len(v_list) >= max(1, len(last_n) // 2):
            ref = sum(v_list) / len(v_list)
            asof = str(last_n[-1].get("date"))[:10] if last_n else None
            fres = _days_between(asof, _today_iso())
            return {
                "ref_price": float(ref),
                "ref_type": f"vwap_{len(last_n)}",
                "asof_date": asof,
                "n_days": len(last_n),
                "freshness_days": fres,
            }
        # else median close
        c_list = [float(x["close"]) for x in last_n if isinstance(x, dict) and x.get("close") is not None]
        if c_list:
            ref = median(c_list)
            asof = str(last_n[-1].get("date"))[:10]
            fres = _days_between(asof, _today_iso())
            return {
                "ref_price": float(ref),
                "ref_type": f"median_close_{len(c_list)}",
                "asof_date": asof,
                "n_days": len(c_list),
                "freshness_days": fres,
            }

    # Fallback ke single latest close
    latest = get_latest_price(sym) or {}
    if "close" in latest:
        asof = latest.get("date")
        fres = _days_between(asof, _today_iso())
        return {
            "ref_price": float(latest["close"]),
            "ref_type": "close",
            "asof_date": asof,
            "n_days": 1,
            "freshness_days": fres,
        }
    return None

def compute_document_median_price(prices: List[Union[int, float]]) -> Optional[float]:
    vals = [float(x) for x in prices if isinstance(x, (int, float))]
    if not vals:
        return None
    return float(median(vals))

def suggest_price_range(ref_price: Optional[float]) -> Optional[Dict[str, float]]:
    if ref_price is None:
        return None
    try:
        ref = float(ref_price)
        delta = ref * float(SUGGEST_PRICE_RATIO)
        return {"min": max(0.0, ref - delta), "max": ref + delta}
    except Exception:
        return None

def build_announcement_block(meta: Optional[Union[DownloadMeta, Dict[str, Any]]]) -> Optional[Dict[str, Any]]:
    if meta is None:
        return None
    if is_dataclass(meta):
        m = asdict(meta)  # type: ignore
    elif isinstance(meta, dict):
        m = dict(meta)
    else:
        return None

    url = m.get("url") or m.get("link") or m.get("announcement_url")
    pdf_url = m.get("pdf_url") or m.get("pdf") or m.get("attachment_url")
    # Best-effort id
    doc_id = (
        m.get("id")
        or m.get("doc_id")
        or m.get("uuid")
        or (Path(m.get("filename")).stem if m.get("filename") else None)
    )
    title = m.get("title") or m.get("subject") or m.get("heading")
    src = None
    u = (url or pdf_url or "").lower()
    if "idx.co.id" in u or "idx" in (m.get("source") or "").lower():
        src = "idx"
    elif u:
        src = "non-idx"

    return {
        "id": str(doc_id) if doc_id is not None else None,
        "title": title,
        "url": url,
        "pdf_url": pdf_url,
        "source_type": src,
    }

# =============================================================================
# Optional: tag computation passthrough
# =============================================================================
_TAG_WHITELIST = {
    "bullish", "bearish", "takeover", "investment", "divestment",
    "free-float-requirement", "mesop", "inheritance", "share-transfer",
}

def get_tags(
    tx_type: Optional[str] = None,
    before_pct: Optional[float] = None,
    after_pct: Optional[float] = None,
    body: Optional[str] = None,
) -> List[str]:
    if _TC is not None:
        tx = (tx_type or "").strip().lower()
        txns: List[Dict[str, Any]] = []
        if tx in {"buy", "sell", "transfer"}:
            txns = [{"type": tx, "amount": 1}]
        flags: Dict[str, bool] = _TC.detect_flags_from_text(body or "") if body else {}
        tags = _TC.compute_filings_tags(
            txns=txns,
            share_percentage_before=before_pct,
            share_percentage_after=after_pct,
            flags=flags,
        )
        tags = [t.lower() for t in tags if isinstance(t, str)]
        return [t for t in tags if t in _TAG_WHITELIST]
    return []

__all__ = [
    "get_company_info",
    "get_latest_price",
    "get_market_reference",
    "compute_document_median_price",
    "suggest_price_range",
    "build_announcement_block",
    "get_tags",
]
