from typing import Dict, Any
import warnings
import requests
import urllib3

from ingestion.utils.config import HEADERS, IDX_API_URL, DEFAULT_PAGE_SIZE, proxies_from_env

def make_session() -> requests.Session:
    """
    Create a requests.Session configured with proxies and with SSL warnings silenced.
    We keep verify=False at call sites to mirror legacy behavior.
    """
    warnings.filterwarnings("ignore", category=urllib3.exceptions.InsecureRequestWarning)
    try:
        from urllib3.exceptions import NotOpenSSLWarning 
        warnings.filterwarnings("ignore", category=NotOpenSSLWarning)
    except Exception:
        pass

    s = requests.Session()
    proxies = proxies_from_env()
    if proxies:
        s.proxies.update(proxies)
    return s

def fetch_page(
    session: requests.Session,
    start_yyyymmdd: str,
    end_yyyymmdd: str,
    page: int,
    page_size: int = DEFAULT_PAGE_SIZE,
) -> Dict[str, Any]:
    """
    Call the IDX API for a single page and return the JSON payload.
    """
    params = {
        "keywords": "ownership",
        "pageNumber": page,
        "pageSize": page_size,
        "dateFrom": start_yyyymmdd,
        "dateTo": end_yyyymmdd,
        "lang": "en",
    }
    resp = session.get(IDX_API_URL, headers=HEADERS, params=params, verify=False, timeout=60)
    resp.raise_for_status()
    return resp.json()
