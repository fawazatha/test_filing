"""Text utilities: slug, kebab, and safe conversions (py39 compatible)."""

from __future__ import annotations
import re
from typing import Optional, Any


_slug_non_alnum = re.compile(r"[^A-Za-z0-9]+")
_ws = re.compile(r"\s+")


def kebab(s: Optional[Any]) -> Optional[str]:
    """Return kebab-case slug or None for empty."""
    if s is None:
        return None
    s = str(s).strip()
    if not s:
        return None
    s = _slug_non_alnum.sub("-", s).strip("-").lower()
    return s or None


def slugify(s: Optional[Any]) -> Optional[str]:
    """Basic slug (words joined by single dash)."""
    if s is None:
        return None
    s = _ws.sub(" ", str(s).strip())
    s = _slug_non_alnum.sub("-", s).strip("-").lower()
    return s or None


def to_int(v: Any) -> Optional[int]:
    """Parse as int or return None."""
    if v is None or v == "":
        return None
    try:
        return int(str(v).replace(",", "").strip())
    except Exception:
        return None


def to_float(v: Any, ndigits: Optional[int] = None) -> Optional[float]:
    """Parse as float (optionally rounded) or return None."""
    if v is None or v == "":
        return None
    try:
        x = float(str(v).replace(",", "").strip())
        return round(x, ndigits) if ndigits is not None else x
    except Exception:
        return None


def to_bool(v: Any) -> Optional[bool]:
    """Parse as bool from common truthy/falsey strings or return None."""
    if v is None:
        return None
    s = str(v).strip().lower()
    if s in {"1", "true", "yes", "y", "on"}:
        return True
    if s in {"0", "false", "no", "n", "off"}:
        return False
    return None
