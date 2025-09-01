from __future__ import annotations

from pathlib import Path
from typing import Dict, Any, Optional
import json

from .generator import FilingGenerator
from .utils.writer import write_filings_json


def _load_company_map(path: Optional[Path]) -> Dict[str, str]:
    """Return {SYMBOL: COMPANY_NAME}. Tolerant to several JSON shapes."""
    if not path or not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}

    out: Dict[str, str] = {}

    def _add(sym: Any, name: Any) -> None:
        s = str(sym or "").upper().strip()
        n = str(name or "").strip()
        if s:
            out[s] = n

    if isinstance(data, dict):
        if all(isinstance(v, str) for v in data.values()):
            for k, v in data.items():
                _add(k, v)
            return out
        for key in ("symbol_to_company", "companies", "data"):
            block = data.get(key)
            if isinstance(block, dict) and all(isinstance(v, str) for v in block.values()):
                for k, v in block.items():
                    _add(k, v)
                return out
            if isinstance(block, list):
                for row in block:
                    if isinstance(row, dict):
                        sym = row.get("symbol") or row.get("ticker")
                        name = row.get("company_name") or row.get("emiten") or row.get("name")
                        if sym and name:
                            _add(sym, name)
                return out

    if isinstance(data, list):
        for row in data:
            if not isinstance(row, dict):
                continue
            sym = row.get("symbol") or row.get("ticker")
            name = row.get("company_name") or row.get("emiten") or row.get("name")
            if sym and name:
                _add(sym, name)
        return out

    return out


def _load_downloads_map(path: Optional[Path]) -> Dict[str, Dict[str, Any]]:
    """
    Build {filename: {ticker, url, title, timestamp, ...}} from data/downloaded_pdfs.json.
    """
    if not path or not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    out: Dict[str, Dict[str, Any]] = {}
    for row in data if isinstance(data, list) else []:
        if not isinstance(row, dict):
            continue
        fn = (row.get("filename") or "").strip()
        if not fn:
            continue
        row = dict(row)
        if "ticker" in row and row["ticker"]:
            row["ticker"] = str(row["ticker"]).upper().strip()
        out[fn] = row
    return out


def main(
    *,
    idx_path: Optional[Path],
    non_idx_path: Optional[Path],
    downloads_path: Optional[Path],
    company_map_path: Optional[Path],
    out_path: Path,
    verbose: bool = False,
) -> Path:
    """
    Called by cli.py with keyword args.
    """
    company_lookup = _load_company_map(company_map_path)
    downloads_map = _load_downloads_map(downloads_path)

    gen = FilingGenerator(verbose=verbose, company_lookup=company_lookup, uid_window=3)
    filings = gen.run(idx_path=idx_path, non_idx_path=non_idx_path, downloads_map=downloads_map)
    return write_filings_json(filings, out_path)
