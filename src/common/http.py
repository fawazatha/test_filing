from __future__ import annotations
import httpx
from typing import Optional, Mapping

"""HTTP client helpers (centralized retries/headers/timeouts)."""

_DEFAULT_TIMEOUT = 30.0


def init_http(
    timeout: float = _DEFAULT_TIMEOUT,
    headers: Optional[Mapping[str, str]] = None,
) -> httpx.Client:
    """Create a configured httpx Client."""
    return httpx.Client(timeout=timeout, headers=headers or {})


def get_pdf_bytes_minimal(client: httpx.Client, url: str) -> bytes:
    """Fetch raw bytes from a PDF URL with basic error handling."""
    r = client.get(url, follow_redirects=True)
    r.raise_for_status()
    return r.content
