from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Dict, Any, Tuple

def _normalize_company_name(s: str) -> str:
    # Normalisasi longgar agar 'PT ABC Tbk' ≈ 'ABC'.
    # - huruf kecil
    # - hapus titik/koma/apostrof/hyphen
    # - ganti '&' -> 'and'
    # - buang kata umum: pt, tbk, indonesia, limited, tbk., corp, corporate, co, ltd
    # - kompres spasi
    
    if not s:
        return ""
    t = s.lower()
    t = t.replace("&", " and ")
    t = re.sub(r"[.,'\"()-/]", " ", t)
    t = re.sub(r"\b(pt|tbk|tbk\.|indonesia|limited|corporation|corporate|corp|co|ltd|tbk,)\b", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t

def load_company_map(path: str) -> Tuple[Dict[str, Any], Dict[str, str]]:
    """
    Return:
      - raw_map: dict file (ticker -> {...})
      - by_name_norm: map nama_norm -> ticker
    """
    p = Path(path)
    data = json.loads(p.read_text(encoding="utf-8"))
    by_name_norm: Dict[str, str] = {}
    for ticker, meta in data.items():
        name = str(meta.get("company_name") or "").strip()
        if not name:
            continue
        by_name_norm[_normalize_company_name(name)] = ticker
    return data, by_name_norm

def annotate_holder_tickers(filings: list[dict], by_name_norm: Dict[str, str]) -> int:
    filled = 0
    for f in filings:
        if f.holder_ticker:
            continue
        name = str(f.get("holder_name") or "")
        key = _normalize_company_name(name)
        ticker = by_name_norm.get(key)
        if ticker:
            f["holder_ticker"] = ticker
            filled += 1
    return filled
