from zoneinfo import ZoneInfo
from typing import Optional, Dict
import os

# Timezone used across the pipeline
JKT = ZoneInfo("Asia/Jakarta")

# IDX API base
IDX_API_URL = "https://www.idx.co.id/primary/NewsAnnouncement/GetAllAnnouncement"

# Default paging
DEFAULT_PAGE_SIZE = 10

# Minimal headers (stable, mirrors the downloader HTTP client)
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/91.0.4472.124 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
}

def proxies_from_env() -> Optional[Dict[str, str]]:
    """
    Read common proxy env vars. Returns a 'requests' proxies dict or None.
    """
    for key in ("PROXY", "HTTPS_PROXY", "HTTP_PROXY", "https_proxy", "http_proxy"):
        v = os.getenv(key)
        if v:
            return {"http": v, "https": v}
    return None
