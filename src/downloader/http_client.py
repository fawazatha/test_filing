import os
import warnings
import urllib3
import requests

# Minimal headers exactly like the working script
MINIMAL_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/91.0.4472.124 Safari/537.36"
    ),
    "Referer": "https://www.idx.co.id/en/news-announcements/announcement-summary",
}

def _proxies_from_env():
    """
    Read common proxy env var names.
    Returns a requests-compatible proxies dict or None.
    """
    for key in ("PROXY", "HTTPS_PROXY", "HTTP_PROXY", "https_proxy", "http_proxy"):
        v = os.getenv(key)
        if v:
            return {"http": v, "https": v}
    return None

def init_http(insecure: bool = True, silence_warnings: bool = True):
    """
    Match legacy behavior:
      - insecure=True -> verify=False in actual GET helpers
      - silence urllib3 SSL warnings if requested
    """
    if silence_warnings:
        warnings.filterwarnings("ignore", category=urllib3.exceptions.InsecureRequestWarning)
        # Optional: silence NotOpenSSLWarning on macOS/LibreSSL (it's harmless)
        try:
            from urllib3.exceptions import NotOpenSSLWarning  # type: ignore
            warnings.filterwarnings("ignore", category=NotOpenSSLWarning)
        except Exception:
            pass

def get_pdf_bytes_minimal(url: str, timeout: int = 60) -> bytes:
    """
    EXACT call shape that worked in the original script:
      requests.get(url, headers=MINIMAL_HEADERS, proxies=..., verify=False)
    """
    r = requests.get(
        url,
        headers=MINIMAL_HEADERS,
        proxies=_proxies_from_env(),
        verify=False,               # legacy
        timeout=timeout,
        allow_redirects=True,
    )
    r.raise_for_status()
    return r.content

def seed_and_retry_minimal(url: str, timeout: int = 60) -> bytes:
    """
    If a server returns 403, seed the referer once (to set cookies) then retry.
    """
    try:
        requests.get(
            MINIMAL_HEADERS["Referer"],
            headers=MINIMAL_HEADERS,
            proxies=_proxies_from_env(),
            verify=False,
            timeout=timeout,
            allow_redirects=True,
        )
    except Exception:
        # Ignore failures; still attempt retry
        pass
    return get_pdf_bytes_minimal(url, timeout=timeout)
