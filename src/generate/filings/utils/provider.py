# src/generate/filings/provider.py
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
except Exception:
    # Fallback for flat layout during testing
    from types import CompanyInfo, DownloadMeta
    from config import (
        COMPANY_MAP_PATH,
        LATEST_PRICES_PATH,
        MARKET_REF_N_DAYS,
        SUGGEST_PRICE_RATIO,
    )

logger = logging.getLogger(__name__)

# Optional: classifier for tags, keep robust if missing
try:
    from parser.utils.transaction_classifier import TransactionClassifier as _TC  # type: ignore
except Exception:  # pragma: no cover
    _TC = None

# -----------------------------------------------------------------------------
# In-memory caches with thread-safety
# -----------------------------------------------------------------------------
_lock = threading.RLock()
_company_map_cache: Optional[Dict[str, Dict[str, Any]]] = None
_company_map_mtime: Optional[float] = None

_prices_cache: Optional[Dict[str, Any]] = None
_prices_mtime: Optional[float] = None

# Allow multiple candidate paths (some repos keep legacy files)
COMPANY_MAP_PATHS: Tuple[str, ...] = (
    os.getenv(
        "FILINGS_COMPANY_MAP",
        COMPANY_MAP_PATH if "COMPANY_MAP_PATH" in globals() else "data/company/company_map.json",
    ),
    "data/company/company_map.hydrated.json",  # optional
)

# IMPORTANT: default to latest_prices.json (NOT company_map.json)
LATEST_PRICE_PATHS: Tuple[str, ...] = (
    os.getenv(
        "FILINGS_LATEST_PRICES",
        LATEST_PRICES_PATH if "LATEST_PRICES_PATH" in globals() else "data/company/latest_prices.json",
    ),
)

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
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

def _as_str_or_none(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    return s if s else None

def _ensure_company_map_loaded() -> None:
    """
    Load company map from:
      - dict { "TICKER.JK": {...} }
      - dict { "map": { "TICKER.JK": {...} } }
      - list [ { "symbol": "...", "company_name": "...", ... }, ... ]
    """
    global _company_map_cache, _company_map_mtime
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
            _company_map_cache = {}
            _company_map_mtime = None
            return

        mtime = found.stat().st_mtime
        if _company_map_cache is not None and _company_map_mtime == mtime:
            return

        try:
            raw = _load_json(found)
            cmap: Dict[str, Dict[str, Any]] = {}

            if isinstance(raw, dict) and isinstance(raw.get("map"), dict):
                # hydrated format
                cmap = raw["map"]
            elif isinstance(raw, dict):
                # plain dict keyed by symbol
                cmap = {k.upper().strip(): v for k, v in raw.items() if isinstance(v, dict)}
            elif isinstance(raw, list):
                # list of rows with "symbol"
                for row in raw:
                    if not isinstance(row, dict):
                        continue
                    sym = _normalize_symbol(row.get("symbol"))
                    if not sym:
                        continue
                    cmap[sym] = {
                        "company_name": _as_str_or_none(row.get("company_name")) or "",
                        "sector": _as_str_or_none(row.get("sector")),
                        "sub_sector": _as_str_or_none(row.get("sub_sector")) or _as_str_or_none(row.get("subsector")),
                        "last_close_price": row.get("last_close_price"),
                        "latest_close_date": _as_str_or_none(row.get("latest_close_date")),
                    }
            else:
                cmap = {}

            _company_map_cache = cmap
            _company_map_mtime = mtime
            logger.info("Loaded company map from %s (%d symbols)", found, len(_company_map_cache or {}))
        except Exception as e:  # pragma: no cover
            logger.warning("Failed loading company map from %s: %s", found, e)
            _company_map_cache = {}
            _company_map_mtime = None

def _ensure_prices_loaded() -> None:
    """
    Load latest prices from:
      - dict { "TICKER.JK": { "close":..., "date":... } }
      - dict { "prices": { ... } }
      - list [ { "symbol": "TICKER.JK", "close":..., "date":... }, ... ]
    Also tolerates when the file is accidentally a company_map (fallback to last_close_price/latest_close_date).
    """
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
            raw = _load_json(found)
            prices: Dict[str, Any] = {}

            # Case 1: {"prices": {...}}
            if isinstance(raw, dict) and isinstance(raw.get("prices"), dict):
                prices = raw["prices"]

            # Case 2: dict keyed by symbol
            elif isinstance(raw, dict):
                # Detect if this is actually a company_map-style dict and adapt
                sample_val = next(iter(raw.values())) if raw else {}
                looks_like_company_map = isinstance(sample_val, dict) and (
                    "company_name" in sample_val or "last_close_price" in sample_val
                )
                if looks_like_company_map:
                    for k, v in raw.items():
                        sym = _normalize_symbol(k)
                        if not sym or not isinstance(v, dict):
                            continue
                        close = v.get("close")
                        if close is None:
                            close = v.get("last_close_price") or v.get("price")
                        date = v.get("date") or v.get("latest_close_date") or v.get("updated_on")
                        prices[sym] = {"close": close, "date": _as_str_or_none(date)}
                else:
                    prices = { (k.upper().strip() if isinstance(k, str) else k): v for k, v in raw.items() }

            # Case 3: list of rows
            elif isinstance(raw, list):
                for row in raw:
                    if not isinstance(row, dict):
                        continue
                    sym = _normalize_symbol(row.get("symbol"))
                    if not sym:
                        continue
                    close = row.get("close") or row.get("last") or row.get("price") or row.get("last_close_price")
                    date = row.get("date") or row.get("asof") or row.get("as_of") or row.get("updated_on") or row.get("latest_close_date")
                    entry: Dict[str, Any] = {}
                    if close is not None:
                        try:
                            entry["close"] = float(close)
                        except Exception:
                            pass
                    if date is not None:
                        entry["date"] = _as_str_or_none(date)
                    if entry:
                        prices[sym] = entry

            else:
                prices = {}

            _prices_cache = prices
            _prices_mtime = mtime
            logger.info("Loaded latest prices from %s (%d symbols)", found, len(_prices_cache or {}))
        except Exception as e:  # pragma: no cover
            logger.warning("Failed loading latest prices from %s: %s", found, e)
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
        a = datetime.fromisoformat(str(d1)[:10])
        b = datetime.fromisoformat(str(d2)[:10])
        return abs((a - b).days)
    except Exception:
        return None

# -----------------------------------------------------------------------------
# Public API
# -----------------------------------------------------------------------------
def get_company_info(symbol: Optional[str]) -> CompanyInfo:
    """
    Lookup company information for a symbol. Returns empty CompanyInfo if missing.
    """
    _ensure_company_map_loaded()
    sym = _normalize_symbol(symbol) or ""
    info = (_company_map_cache or {}).get(sym, {})
    if not isinstance(info, dict):
        info = {}
    return CompanyInfo(
        company_name=(_as_str_or_none(info.get("company_name")) or _as_str_or_none(info.get("name")) or "") ,
        sector=_as_str_or_none(info.get("sector")),
        sub_sector=_as_str_or_none(info.get("sub_sector")) or _as_str_or_none(info.get("subsector")),
    )

def get_latest_price(symbol: Optional[str]) -> Optional[Dict[str, Any]]:
    """
    Return latest price info from local cache:
      { "close": float, "vwap": float?, "date": "YYYY-MM-DD" }
    Accepts a few schema variants and also tolerates when the cache
    is accidentally a company_map (uses last_close_price/latest_close_date).
    """
    _ensure_prices_loaded()
    sym = _normalize_symbol(symbol)
    if not sym:
        return None
    entry = (_prices_cache or {}).get(sym)

    # scalar number → treat as close
    if isinstance(entry, (int, float)):
        return {"close": float(entry)}

    if not isinstance(entry, dict):
        return None

    # Normalize keys
    close = (
        entry.get("close")
        or entry.get("last")
        or entry.get("price")
        or entry.get("last_close_price")  # company_map-style
    )
    vwap = entry.get("vwap") or entry.get("VWAP")
    date_str = (
        entry.get("date")
        or entry.get("asof")
        or entry.get("as_of")
        or entry.get("updated_on")
        or entry.get("latest_close_date")  # company_map-style
    )

    out: Dict[str, Any] = {}
    if close is not None:
        try:
            out["close"] = float(close)
        except Exception:
            pass
    if vwap is not None:
        try:
            out["vwap"] = float(vwap)
        except Exception:
            pass
    if date_str:
        out["date"] = str(date_str)[:10]

    return out or None

def get_market_reference(symbol: Optional[str], n_days: int = None) -> Optional[Dict[str, Any]]:
    """
    Compute a market reference price used for sanity checks:
      - Prefer N-day VWAP/median-close if historical series available in cache
      - Fallback to latest close in cache

    Returns:
      {
        "ref_price": float,
        "ref_type": "vwap_n" | "median_close_n" | "close",
        "asof_date": "YYYY-MM-DD",
        "n_days": int,
        "freshness_days": int?  # distance between asof_date and today
      }
    """
    _ensure_prices_loaded()
    if n_days is None:
        n_days = MARKET_REF_N_DAYS
    sym = _normalize_symbol(symbol)
    if not sym:
        return None
    entry = (_prices_cache or {}).get(sym)
    if not isinstance(entry, dict):
        # if scalar price, treat as latest close
        try:
            if entry is not None:
                ref = float(entry)
                return {
                    "ref_price": ref,
                    "ref_type": "close",
                    "asof_date": None,
                    "n_days": 1,
                    "freshness_days": None,
                }
        except Exception:
            return None
        return None

    # If historical series exists under "series": [{"date":..,"close":..,"vwap":..}, ...]
    series = entry.get("series")
    if isinstance(series, list) and series:
        last_n = series[-n_days:] if n_days > 0 else series[:]
        # prefer VWAP if present for majority of points
        v_list = [float(x["vwap"]) for x in last_n if isinstance(x, dict) and x.get("vwap") is not None]
        if len(v_list) >= max(1, len(last_n) // 2):
            ref = sum(v_list) / len(v_list)
            asof = _as_str_or_none(last_n[-1].get("date")) if last_n else None
            fres = _days_between(asof, _today_iso())
            return {
                "ref_price": float(ref),
                "ref_type": f"vwap_{len(last_n)}",
                "asof_date": asof[:10] if asof else None,
                "n_days": len(last_n),
                "freshness_days": fres,
            }
        # else compute median of closes
        c_list = [float(x["close"]) for x in last_n if isinstance(x, dict) and x.get("close") is not None]
        if c_list:
            ref = median(c_list)
            asof = _as_str_or_none(last_n[-1].get("date"))
            fres = _days_between(asof, _today_iso())
            return {
                "ref_price": float(ref),
                "ref_type": f"median_close_{len(c_list)}",
                "asof_date": asof[:10] if asof else None,
                "n_days": len(c_list),
                "freshness_days": fres,
            }

    # Fallback to single latest close
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
    """Utility used by processors to compute doc-median price robustly."""
    vals = [float(x) for x in prices if isinstance(x, (int, float))]
    if not vals:
        return None
    return float(median(vals))

def suggest_price_range(ref_price: Optional[float]) -> Optional[Dict[str, float]]:
    """Return low/high suggestion window around a reference price for alerts."""
    if ref_price is None:
        return None
    try:
        ref = float(ref_price)
        delta = ref * float(SUGGEST_PRICE_RATIO)
        return {"min": max(0.0, ref - delta), "max": ref + delta}
    except Exception:
        return None

def build_announcement_block(meta: Optional[Union[DownloadMeta, Dict[str, Any]]]) -> Optional[Dict[str, Any]]:
    """
    Normalize document/announcement metadata for Alerts v2:
    Returns:
      {
        "id": str|None,
        "title": str|None,
        "url": str|None,
        "pdf_url": str|None,
        "source_type": "idx" | "non-idx" | None
      }
    """
    if meta is None:
        return None

    if is_dataclass(meta):
        m = asdict(meta)  # type: ignore
    elif isinstance(meta, dict):
        m = dict(meta)
    else:
        # unknown type
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

# -----------------------------------------------------------------------------
# Optional: tag computation passthrough (kept for backward-compat)
# -----------------------------------------------------------------------------
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
    """
    Backward-compatible tag generator.
    If external classifier exists, we delegate; otherwise we return [] or a minimal mapping.
    """
    # If external classifier is available, use it
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
        # enforce whitelist & lowercase
        tags = [t.lower() for t in tags if isinstance(t, str)]
        return [t for t in tags if t in _TAG_WHITELIST]

    # Fallback minimalist: no inference, just return []
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
