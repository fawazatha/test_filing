from typing import Optional
import os
import requests
import urllib3
from dotenv import load_dotenv

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0 Safari/537.36"
)
REFERER = "https://www.idx.co.id/en/news-announcements/announcement-summary"


def init_http(insecure: bool = True, silence_warnings: bool = True, load_env: bool = True) -> None:
    """
    Initialize HTTP behavior:
      - Optionally load `.env`
      - If PROXY is present, propagate it to HTTP(S)_PROXY so `requests` uses it
      - Optionally silence SSL warnings (we use verify=False on purpose)
    """
    if load_env:
        load_dotenv(override=True)

    proxy = os.getenv("PROXY")
    if proxy:
        # populate both upper/lower so requests picks them up
        for k in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"):
            os.environ.setdefault(k, proxy)

    if insecure and silence_warnings:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def _headers(seed: bool = False) -> dict:
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
    """
    Single-shot GET using UA + Referer. Proxies come from env (HTTP[S]_PROXY).
    """
    with requests.Session() as s:
        r = s.get(url, headers=_headers(seed=True), timeout=timeout, verify=False, allow_redirects=True)
        r.raise_for_status()
        return r.content


def seed_and_retry_minimal(url: str, timeout: int = 60) -> bytes:
    """
    Touch the announcement summary page to establish cookies, then retry the PDF.
    """
    with requests.Session() as s:
        try:
            s.get(REFERER, headers=_headers(seed=True), timeout=timeout, verify=False)
        except Exception:
            # Seeding failure is not fatal; continue to the target URL.
            pass
        r = s.get(url, headers=_headers(seed=True), timeout=timeout, verify=False, allow_redirects=True)
        r.raise_for_status()
        return r.content
