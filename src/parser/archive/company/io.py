import os
import json
from pathlib import Path
from typing import Dict, Optional

from src.common.log import get_logger

"""
This module contains only I/O logic for loading company data.
"""

logger = get_logger(__name__)
DEFAULT_MAP_PATH = Path(os.getenv("COMPANY_MAP_FILE", "data/company/company_map.json"))


def load_symbol_to_name_from_file(path: Path = DEFAULT_MAP_PATH) -> Optional[Dict[str, str]]:
    """
    Load company map from JSON. Accepts either:
      { "ABCD": "PT Alpha Beta Tbk", ... }
      or      { "ABCD": {"company_name": "...", ...}, ... }
    Adds both BASE and BASE.JK aliases to maximize resolvability.
    """
    try:
        if not path.exists():
            logger.warning(f"Company map not found: {path}")
            return None

        raw = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            logger.error("company_map must be a dict: symbol -> {company_name,...} or string")
            return None

        out: Dict[str, str] = {}

        def _add(sym: str, nm: str) -> None:
            s = (sym or "").strip().upper()
            n = (nm or "").strip()
            if not s or not n:
                return
            if s.endswith(".JK"):
                base = s[:-3]
                out[base] = n
                out[s] = n
            else:
                out[s] = n
                out[f"{s}.JK"] = n

        for sym, val in raw.items():
            if isinstance(val, dict):
                name = val.get("company_name") or val.get("name") or val.get("legal_name") or ""
            else:
                name = str(val or "")
            _add(sym, name)

        logger.info(f"Loaded {len(out)} symbol entries (with .JK aliases) from {path}")
        return out

    except Exception as e:
        logger.warning(f"Failed reading company_map file {path}: {e}")
        return None