from __future__ import annotations
from typing import Dict, List, Optional, Tuple
import os
import re

from src.common.log import get_logger
from .io import load_symbol_to_name_from_file
from .resolver import (
    build_reverse_map,
    resolve_symbol_from_emiten,
    resolve_symbol_and_name as resolver_facade
)
from .format import normalize_company_name, canonical_name_for_symbol

logger = get_logger(__name__)

class CompanyService:
    """
    Facade for company-map resolution.
    Loads data on init and provides simple methods for parsers.
    """
    def __init__(self) -> None:
        # 1. Load data
        self.symbol_to_name: Dict[str, str] = load_symbol_to_name_from_file() or {}
        
        # 2. Transform data
        self.rev_map: Dict[str, List[str]] = build_reverse_map(self.symbol_to_name)
        
        logger.info(
            "[company_map] file=%s symbols=%d reverse_keys=%d",
            os.getenv("COMPANY_MAP_FILE", "data/company/company_map.json"),
            len(self.symbol_to_name),
            len(self.rev_map),
        )

    def resolve_symbol(self, emiten_name: Optional[str], full_text: str, min_score_env: str = "88") -> Optional[str]:
        """
        Resolve by emiten name first; if not found, scan for tickers in text.
        Returns BASE ticker (no '.JK').
        """
        if emiten_name:
            min_score = int(os.getenv("NONIDX_RESOLVE_MIN_SCORE", min_score_env))
            # 3. Use resolution logic
            sym, _, _ = resolve_symbol_from_emiten(
                emiten_name,
                symbol_to_name=self.symbol_to_name,
                rev_map=self.rev_map,
                fuzzy=True,
                min_score=min_score,
            )
            if sym:
                return sym[:-3] if sym.endswith(".JK") else sym

            # Fallback: try a PT ... Tbk capture in the text
            m = re.search(r'(PT\s+.+?Tbk\.?)', full_text or "", flags=re.I)
            if m:
                alt = m.group(1)
                sym2, _, _ = resolve_symbol_from_emiten(
                    alt,
                    symbol_to_name=self.symbol_to_name,
                    rev_map=self.rev_map,
                    fuzzy=True,
                    min_score=min_score,
                )
                if sym2:
                    return sym2[:-3] if sym2.endswith(".JK") else sym2

        # Final fallback: scan text for tokens
        return self._scan_tokens(full_text)

    def resolve_symbol_and_name(
        self,
        emiten_raw: str,
        fuzzy: bool = True,
        min_score: int = 85
    ) -> Tuple[Optional[str], str, str, List[str]]:
        """Passthrough to the main resolver facade."""
        return resolver_facade(
            emiten_raw,
            self.symbol_to_name,
            self.rev_map,
            fuzzy=fuzzy,
            min_score=min_score
        )
    
    def get_canonical_name(self, symbol: str) -> Optional[str]:
        """Gets the canonical name for a symbol."""
        return canonical_name_for_symbol(self.symbol_to_name, symbol)

    def normalized_key(self, name: Optional[str]) -> str:
        """Gets the normalized key for a name."""
        return normalize_company_name(name or "")

    # Internals
    def _scan_tokens(self, text: str) -> Optional[str]:
        """Scan text for 3-4 char uppercase tokens that match the symbol map."""
        candidates = set(re.findall(r'\b([A-Z]{3,4})\b', text or ""))
        for cand in candidates:
            if cand in self.symbol_to_name or f"{cand}.JK" in self.symbol_to_name:
                return cand
        return None
