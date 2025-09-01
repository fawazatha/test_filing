from __future__ import annotations
from typing import Any, Optional
import unicodedata, re

def string_to_slug(s: str) -> str:
    if not s:
        return ""
    s = s.strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.translate(str.maketrans({
        "ñ": "n", "ç": "c", "·": "-", "/": "-", "_": "-",
        ",": "-", ":": "-", ";": "-"
    }))
    s = re.sub(r"[^a-z0-9 \-]", "", s)
    s = re.sub(r"\s+", "-", s)
    s = re.sub(r"-{2,}", "-", s)
    return s.strip("-")

def fmt_num_as_str(v) -> str:
    try:
        if v is None: return "0"
        if isinstance(v, int): return str(v)
        if isinstance(v, float): return str(v)
        fv = float(v)
        return str(int(fv)) if fv.is_integer() else str(fv)
    except Exception:
        return str(v)

def last_segment(name_or_url: str) -> str:
    s = (name_or_url or "").strip()
    if not s: return ""
    return s.rsplit("/", 1)[-1]

def ensure_symbol_suffix(sym: str) -> str:
    s = (sym or "").strip()
    if not s: return s
    return s if s.upper().endswith(".JK") else f"{s}.JK"

def timestamp_to_output(ts: Optional[str]) -> Optional[str]:
    if not ts: return None
    return ts.replace("T", " ")

def dump_model(obj: Any):
    if hasattr(obj, "model_dump"):  # pydantic v2
        return obj.model_dump()
    if hasattr(obj, "dict"):        # pydantic v1
        return obj.dict()
    return obj

def get_attr(obj: Any, name: str, default=None):
    if isinstance(obj, dict):
        return obj.get(name, default)
    return getattr(obj, name, default)

def build_title(holder_name: str, direction: str, company: str) -> str:
    direction = "Unknown" if (not direction or direction == "unknown") else direction.capitalize()
    return f"{holder_name} {direction} Transaction of {company}"

def build_body(date_str: str, holder: str, holder_type: Optional[str], direction: str,
               amount: int, company: str, hold_before: int, hold_after: int) -> str:
    verb = {"buy": "bought", "sell": "sold"}.get(direction, "transacted")
    prefix = f", an {holder_type} shareholder, " if holder_type else ", a shareholder, "
    return (f"On {date_str}, {holder}{prefix}"
            f"{verb} {amount:,} shares of {company}, changing its holding "
            f"from {hold_before:,} to {hold_after:,} shares.")
