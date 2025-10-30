from __future__ import annotations
from typing import Any, Dict, Optional
import os
import httpx

from src.common.env import proxies_from_env
from ingestion.utils.config import HEADERS, IDX_API_URL, DEFAULT_PAGE_SIZE

"""HTTP client helpers for IDX endpoints (compatible with older httpx)."""

def _apply_env_proxies() -> None:
    """Set HTTP(S)_PROXY env vars if provided via common env helper."""
    proxies = proxies_from_env()
    if not proxies:
        return
    # Expect keys "http://" and "https://"
    http_p = proxies.get("http://")
    https_p = proxies.get("https://")
    if http_p:
        os.environ.setdefault("HTTP_PROXY", http_p)
        os.environ.setdefault("http_proxy", http_p)
    if https_p:
        os.environ.setdefault("HTTPS_PROXY", https_p)
        os.environ.setdefault("https_proxy", https_p)


def make_client(timeout: float = 60.0, transport: Optional[httpx.BaseTransport] = None) -> httpx.Client:
    """
    Return configured httpx.Client.
    - Uses env proxies (trust_env=True) for widest compatibility.
    - Avoids 'proxies=' and 'follow_redirects=' in ctor for older httpx.
    """
    _apply_env_proxies()
    return httpx.Client(
        timeout=timeout,
        headers=HEADERS,
        verify=False,          # keep parity with legacy; flip to True when server certs are stable
        transport=transport,   # optional: enables mocking in tests
        # trust_env=True is default → httpx reads HTTP(S)_PROXY automatically
    )


def fetch_page(
    client: httpx.Client,
    start_yyyymmdd: str,
    end_yyyymmdd: str,
    page: int,
    page_size: int = DEFAULT_PAGE_SIZE,
) -> Dict[str, Any]:
    """Call IDX API for a single page and return JSON payload."""
    params = {
        "keywords": "ownership",
        "pageNumber": page,
        "pageSize": page_size,
        "dateFrom": start_yyyymmdd,
        "dateTo": end_yyyymmdd,
        "lang": "en",
    }
    r = client.get(IDX_API_URL, params=params, follow_redirects=True)
    r.raise_for_status()
    return r.json()
