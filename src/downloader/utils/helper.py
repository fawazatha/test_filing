import os
from urllib.parse import urlparse
from datetime import datetime
from zoneinfo import ZoneInfo

JKT = ZoneInfo("Asia/Jakarta")

def safe_filename_from_url(url: str) -> str:
    """
    Extract a basename from URL and ensure it ends with .pdf.
    """
    path = urlparse(url).path
    name = os.path.basename(path or "download.pdf")
    return name if name.lower().endswith(".pdf") else f"{name}.pdf"

def timestamp_jakarta() -> str:
    """
    Current timestamp in Asia/Jakarta timezone, ISO-8601 format.
    """
    return datetime.now(tz=JKT).isoformat()
