import os
import re
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Optional
from urllib.parse import urlparse

JKT = ZoneInfo("Asia/Jakarta")

FILENAME_RE = re.compile(r"[^A-Za-z0-9._-]+")

def safe_filename_from_url(url: str) -> str:
    """Return a safe file name from URL path component (strip query, sanitize)."""
    path = urlparse(url).path
    base = os.path.basename(path) or "download.pdf"
    # keep dots/underscores/dashes; remove anything else
    base = FILENAME_RE.sub("_", base)
    if not base.lower().endswith(".pdf"):
        base += ".pdf"
    return base

def timestamp_jakarta() -> str:
    """Now in Asia/Jakarta as ISO string."""
    return datetime.now(JKT).isoformat(timespec="seconds")

TICKER_BRACKET_RE = re.compile(r"\[([A-Z]{3,5})\s*\]")

def derive_ticker(title: str, company_name: Optional[str]) -> Optional[str]:
    """
    Prefer company_name if present; otherwise, try to parse [TICKER] from the title.
    Returns uppercased ticker or None.
    """
    if company_name and company_name.strip():
        return company_name.strip().upper()[:5]
    if not title:
        return None
    m = TICKER_BRACKET_RE.search(title.upper())
    if m:
        return m.group(1).strip().upper()
    return None

def filename_from_url(url: str) -> str:
    """Convenience alias to emphasize we only want the name (not full path)."""
    return os.path.basename(urlparse(url).path) or safe_filename_from_url(url)
