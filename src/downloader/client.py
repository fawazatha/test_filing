from __future__ import annotations
from typing import Optional
import os
import requests
import urllib3

try:
    from dotenv import load_dotenv 
except Exception:
    def load_dotenv(*_a, **_k):  
        return False

"""Requests-based HTTP helpers for PDF downloads (env-proxy + silent SSL)."""

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0 Safari/537.36"
)
REFERER = "https://www.idx.co.id/en/news-announcements/announcement-summary"


def init_http(insecure: bool = True, silence_warnings: bool = True, load_env: bool = True) -> None:
    """Prepare environment proxies and optionally silence SSL warnings."""
    if load_env:
        load_dotenv(override=True)

    proxy = os.getenv("PROXY")
    if proxy:
        for k in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"):
            os.environ.setdefault(k, proxy)

    if insecure and silence_warnings:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def _headers(seed: bool = False) -> dict:
    """Minimal headers for PDF endpoints; add Referer when seeding cookies."""
    h = {
        "User-Agent": UA,
        "Accept": "application/pdf,application/octet-stream;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
        "DNT": "1",
        "Upgrade-Insecure-Requests": "1",
    }
    if seed:
        h["Referer"] = REFERER
    return h


def get_pdf_bytes_minimal(url: str, timeout: int = 60) -> bytes:
    """Single-shot GET using UA (+Referer). Proxies come from env (HTTP[S]_PROXY)."""
    with requests.Session() as s:
        r = s.get(url, headers=_headers(seed=True), timeout=timeout, verify=False, allow_redirects=True)
        r.raise_for_status()
        return r.content


def seed_and_retry_minimal(url: str, timeout: int = 60) -> bytes:
    """Touch the IDX summary page to establish cookies, then retry the PDF."""
    with requests.Session() as s:
        try:
            s.get(REFERER, headers=_headers(seed=True), timeout=timeout, verify=False)
        except Exception:
            pass
        r = s.get(url, headers=_headers(seed=True), timeout=timeout, verify=False, allow_redirects=True)
        r.raise_for_status()
        return r.content