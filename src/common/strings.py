from __future__ import annotations

import re
import unicodedata
from typing import Any, Optional

"""
String utilities shared across the project.
"""

# Corporate tokens / acronyms
COMMON_UPPER = {
    "PT", "CV", "UD", "LLC", "LLP", "INC", "NV", "BV", "GMBH", "BHD", "PLC", "RI",
    "OJK", "KPK", "BPK", "BPKP",
}

# Tokens to drop when building normalized company keys
CORP_STOPWORDS = {
    "PT", "P.T", "PERSEROAN", "TERBATAS",
    "TBK", "TBK.", "TBK,", "TBK)", "(TBK",
    "PERSERO", "(PERSERO)",
}


# Tokenization regexes
_TOKEN_SPLIT_UP = re.compile(r"[^A-Z0-9]+", re.UNICODE)
_TOKEN_SPLIT_LO = re.compile(r"[^a-z0-9]+", re.UNICODE)
SPLIT_RE = re.compile(r"[^A-Za-z0-9]+", re.UNICODE) 


# Slug helpers
_slug_non_alnum = re.compile(r"[^A-Za-z0-9]+")
_ws = re.compile(r"\s+")


# Core helpers
def strip_diacritics(s: str) -> str:
    """Remove diacritics/accents: 'é' -> 'e' (ASCII)."""
    return unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode("ascii")


# Company-name normalization
def normalize_company_key(s: str) -> str:
    """
    Uppercased normalization key for company matching.
    """
    s = strip_diacritics(s).upper().replace("&", " AND ")
    tokens = [t for t in _TOKEN_SPLIT_UP.split(s) if t]
    tokens = [t for t in tokens if t not in CORP_STOPWORDS]
    return " ".join(tokens).strip()


def normalize_company_key_lower(s: str) -> str:
    """
    Lowercased normalization key (useful for quick dict lookups).
    """
    if not s:
        return ""
    s = strip_diacritics(s).strip().lower()
    tokens = [t for t in _TOKEN_SPLIT_LO.split(s) if t]
    tokens = [t for t in tokens if t.upper() not in CORP_STOPWORDS]
    return " ".join(tokens).strip()

def normalize_space(s: str | None) -> str | None:
    if s is None:
        return None
    return re.sub(r"\s+", " ", s).strip() or None

# Slug / case helpers
def kebab(s: Optional[Any]) -> Optional[str]:
    """
    Return a kebab-case slug (lowercase words joined by '-'), or None for empty.
    Example: 'Free Float Requirement' -> 'free-float-requirement'
    """
    if s is None:
        return None
    s = strip_diacritics(str(s)).strip()
    if not s:
        return None
    s = _slug_non_alnum.sub("-", s).strip("-").lower()
    return s or None


def slugify(s: Optional[Any]) -> Optional[str]:
    """
    Basic slug (words joined by single dash).
    """
    if s is None:
        return None
    s = _ws.sub(" ", str(s).strip())
    s = _slug_non_alnum.sub("-", s).strip("-").lower()
    return s or None


# Safe scalar conversions
def to_int(v: Any) -> Optional[int]:
    """
    Parse value as int (tolerates commas/whitespace/decimals) or return None on failure.
    """
    if v is None or v == "":
        return None
    try:
        return int(float(str(v).replace(",", "").strip()))
    except Exception:
        return None


def to_float(v: Any, ndigits: Optional[int] = None) -> Optional[float]:
    """Parse value as float (tolerates commas/whitespace) or return None on failure."""
    if v is None or v == "":
        return None
    try:
        x = float(str(v).replace(",", "").strip())
        return round(x, ndigits) if ndigits is not None else x
    except Exception:
        return None


def to_bool(v: Any) -> Optional[bool]:
    """Parse common truthy/falsey strings to bool; return None if unknown."""
    if v is None:
        return None
    s = str(v).strip().lower()
    if s in {"1", "true", "yes", "y", "on"}:
        return True
    if s in {"0", "false", "no", "n", "off"}:
        return False
    return None