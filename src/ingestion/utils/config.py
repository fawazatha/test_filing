from typing import Mapping

"""Ingestion-local constants (no side effects)."""

# IDX API base
IDX_API_URL = "https://www.idx.co.id/primary/NewsAnnouncement/GetAllAnnouncement"

# Default paging
DEFAULT_PAGE_SIZE = 10

# Default headers (aligned with downloader)
HEADERS: Mapping[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/91.0.4472.124 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
}
