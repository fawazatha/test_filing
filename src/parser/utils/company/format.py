import re
from typing import Optional

from src.common.strings import (
    COMMON_UPPER,
    strip_diacritics,
    normalize_company_key,
    normalize_company_key_lower,
)

"""
This module contains only string formatting and normalization logic
related to company names. It does not perform I/O or resolution.
"""

# Normalization Aliases
def normalize_company_name(s: str) -> str:
    """Backward-compatible alias that delegates to src.common.strings.normalize_company_key."""
    return normalize_company_key(s)

def _normalize_name_lower(s: str) -> str:
    """Lower-case normalization for quick exact lookups (delegates to common helper)."""
    return normalize_company_key_lower(s)


# Display Helpers
def pretty_company_name(raw: str) -> str:
    """
    Human-friendly formatter for company names:
    - Standardize 'PT' variants and 'Tbk'
    - Keep common acronyms uppercase (PT, LLC, INC, etc.)
    - Title-case the rest (hyphen-aware)
    - Preserve spacing & punctuation (& , ( ) . -)
    """
    if not raw:
        return ""
    s = strip_diacritics(raw).strip()

    # Standardize PT variants and TBK spelling
    s = re.sub(r"\bP\.?\s*T\.?\b", "PT", s, flags=re.I)
    s = re.sub(r"\bTBK\.?\b", "Tbk", s, flags=re.I)

    # Split but keep separators
    parts = re.split(r"(\s+|[&(),.-])", s)

    def fmt(tok: str) -> str:
        if not tok or tok.isspace() or tok in "&(),.-":
            return tok
        up = tok.upper()
        if up in COMMON_UPPER:
            return up
        if up == "TBK":
            return "Tbk"
        # Hyphenated: "multi-purpose" -> "Multi-Purpose"
        if "-" in tok:
            return "-".join(p[:1].upper() + p[1:].lower() if p else p for p in tok.split("-"))
        return tok[:1].upper() + tok[1:].lower()

    out = "".join(fmt(t) for t in parts)
    out = re.sub(r"\s+", " ", out).strip(" ,.;-")
    out = out.replace(" ,", ",").replace(" .", ".")
    return out

def canonical_name_for_symbol(symbol_to_name: dict[str, str], symbol: str) -> Optional[str]:
    """Return canonical company name for a given symbol (handles BASE and BASE.JK)."""
    s = (symbol or "").strip().upper()
    if not s:
        return None
    if s in symbol_to_name:
        return symbol_to_name[s]
    if s.endswith(".JK"):
        return symbol_to_name.get(s[:-3])
    return symbol_to_name.get(f"{s}.JK")
