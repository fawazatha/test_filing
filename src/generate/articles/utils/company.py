from __future__ import annotations
import json, pathlib
from typing import Dict, Any, Optional
from .io_utils import get_logger

log = get_logger(__name__)

class CompanyCache:
    def __init__(self, company_map_path: str, latest_prices_path: str):
        self.company_map_path = pathlib.Path(company_map_path)
        self.latest_prices_path = pathlib.Path(latest_prices_path)
        self._map: Dict[str, Dict[str, str]] = {}
        self._prices: Dict[str, Any] = {}
        self._load()

    def _load(self):
        if self.company_map_path.exists():
            try:
                raw = json.loads(self.company_map_path.read_text(encoding="utf-8"))
                if isinstance(raw, dict):
                    for sym, row in raw.items():
                        self._map[sym.upper()] = {
                            "company_name": (row.get("company_name") or "").strip(),
                            "sector": (row.get("sector") or "").strip(),
                            "sub_sector": (row.get("sub_sector") or "").strip(),
                        }
                elif isinstance(raw, list):
                    for row in raw:
                        sym = (row.get("symbol") or "").upper()
                        if not sym:
                            continue
                        self._map[sym] = {
                            "company_name": (row.get("company_name") or "").strip(),
                            "sector": (row.get("sector") or "").strip(),
                            "sub_sector": (row.get("sub_sector") or "").strip(),
                        }
            except Exception as e:
                log.warning(f"Failed reading company_map: {e}")
        else:
            log.warning(f"Company map not found: {self.company_map_path}")

        if self.latest_prices_path.exists():
            try:
                raw = json.loads(self.latest_prices_path.read_text(encoding="utf-8"))
                if isinstance(raw, dict):
                    self._prices = raw.get("prices", {})
            except Exception as e:
                log.warning(f"Failed reading latest prices: {e}")

    def get(self, symbol: str | None) -> Optional[Dict[str, str]]:
        if not symbol:
            return None
        s = symbol.upper()
        if not s.endswith(".JK"):
            s = s + ".JK"
        return self._map.get(s) or self._map.get(symbol.upper())
