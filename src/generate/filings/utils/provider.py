from __future__ import annotations

import json
import logging
import os
import threading
from pathlib import Path
from typing import Any
from ..types import CompanyInfo

logger = logging.getLogger(__name__)

LATEST_PRICE_PATHS: list[str] = [os.getenv("LATEST_PRICE_PATH") or "data/company/latest_prices.json"]
COMPANY_MAP_PATH: str = os.getenv("COMPANY_MAP_PATH") or "data/company/company_map.json"

_lock = threading.RLock()
_prices_cache: dict[str, Any] | None = None
_prices_mtime: float | None = None
_map_cache: dict[str, Any] | None = None
_map_mtime: float | None = None


def _load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _ensure_prices_loaded() -> None:
    global _prices_cache, _prices_mtime
    with _lock:
        found: Path | None = None
        for candidate in LATEST_PRICE_PATHS:
            if not candidate:
                continue
            p = Path(candidate)
            if p.exists():
                found = p
                break

        if not found:
            if _prices_cache is None:
                logger.warning("File latest_price(s).json tidak ditemukan di %s", LATEST_PRICE_PATHS)
                _prices_cache = {}
            return

        mtime = found.stat().st_mtime
        if _prices_cache is None or _prices_mtime != mtime:
            try:
                data = _load_json(found) or {}
                _prices_cache = data
                _prices_mtime = mtime
                logger.info("Loaded latest prices from %s", found)
            except Exception as e:
                logger.warning("Gagal load prices dari %s: %s", found, e)
                _prices_cache, _prices_mtime = {}, None


def _ensure_map_loaded() -> None:
    global _map_cache, _map_mtime
    with _lock:
        p = Path(COMPANY_MAP_PATH)
        if not p.exists():
            if _map_cache is None:
                logger.warning("File company_map.json tidak ditemukan di %s", p)
                _map_cache = {}
            return

        mtime = p.stat().st_mtime
        if _map_cache is None or _map_mtime != mtime:
            try:
                _map_cache = _load_json(p) or {}
                _map_mtime = mtime
                logger.info("Loaded company map from %s", p)
            except Exception as e:
                logger.warning("Gagal load company map dari %s: %s", p, e)
                _map_cache, _map_mtime = {}, None


def _lookup_price(symbol_full: str) -> float | None:
    _ensure_prices_loaded()
    data = _prices_cache or {}
    prices = data.get("prices", data)

    entry = prices.get(symbol_full)
    if entry is None:
        entry = prices.get(symbol_full.upper())
        if entry is None:
            return None

    if isinstance(entry, dict):
        val = entry.get("close")
        try:
            return float(val) if val is not None else None
        except (TypeError, ValueError):
            return None

    try:
        return float(entry)
    except (TypeError, ValueError):
        return None


def _lookup_company(symbol_full: str) -> CompanyInfo | None:
    _ensure_map_loaded()
    mapping = _map_cache or {}
    rec = mapping.get(symbol_full) or mapping.get(symbol_full.upper())
    if not isinstance(rec, dict):
        return None

    name = (rec.get("company_name") or rec.get("name") or "").strip()
    sector = rec.get("sector")
    sub_sector = rec.get("sub_sector") or rec.get("subsector")

    return CompanyInfo(company_name=name, sector=sector, sub_sector=sub_sector)


def get_latest_price(symbol_full: str) -> float | None:
    return _lookup_price(symbol_full)


def get_company_info(symbol_full: str) -> CompanyInfo | None:
    return _lookup_company(symbol_full)


def get_tags(
    tx_type: str,
    before_pct: float | None,
    after_pct: float | None,
    body: str | None = None,   # optional, if you want to detect MESOP/free-float later
) -> list[str]:
    """
    Canonical tags for idx_filings. Enforces the 9-tag whitelist:
    ['bullish','bearish','takeover','investment','divestment',
     'free-float-requirement','MESOP','inheritance','share-transfer']
    """
    tx = (tx_type or "").strip().lower()
    tags: set[str] = set()

    # primary tags from tx type
    if tx == "buy":
        tags.update(["investment", "bullish"])
    elif tx == "sell":
        tags.update(["divestment", "bearish"])
    elif tx == "transfer":
        tags.add("share-transfer")
    # (neutral/other → no bullish/bearish/invest/divest)

    # takeover only on crossings of 50%
    try:
        b = float(before_pct) if before_pct is not None else None
        a = float(after_pct)  if after_pct  is not None else None
        if b is not None and a is not None:
            if (b < 50 <= a) or (b >= 50 > a):
                tags.add("takeover")
    except Exception:
        pass

    # (Optional) detect MESOP/free-float/inheritance from body text if you pass it in.
    # Keep commented unless you plan to pass `body=` from processors:
    # tl = (body or "").lower()
    # if any(k in tl for k in ["mesop","msop","esop","program opsi","employee stock option"]):
    #     tags.add("MESOP")
    # if any(k in tl for k in ["free float","free-float","freefloat","porsi publik"]):
    #     tags.add("free-float-requirement")
    # if any(k in tl for k in ["waris","inheritance","hibah","grant","bequest"]):
    #     tags.update(["inheritance","share-transfer"])

    # normalize & enforce whitelist
    whitelist = {
        "bullish","bearish","takeover","investment","divestment",
        "free-float-requirement","MESOP","inheritance","share-transfer",
    }
    out = sorted(t for t in {t.strip().lower() for t in tags} if t in whitelist)
    return out


def configure_paths(latest_price_path: str | None = None, company_map_path: str | None = None) -> None:
    global LATEST_PRICE_PATHS, COMPANY_MAP_PATH, _prices_mtime, _map_mtime
    if latest_price_path:
        LATEST_PRICE_PATHS = [latest_price_path]
    if company_map_path:
        COMPANY_MAP_PATH = company_map_path

    _prices_mtime = None
    _map_mtime = None