import os
from typing import Optional, Dict

"""Environment-derived helpers (kept side-effect free)."""

def proxies_from_env() -> Optional[Dict[str, str]]:
    """Build httpx/requests proxies mapping from common env vars."""
    for key in ("PROXY", "HTTPS_PROXY", "HTTP_PROXY", "https"
    "_proxy", "http_proxy"):
        v = os.getenv(key)
        if v:
            # httpx accepts scheme keys; requests accepts both formats.
            return {"http://": v, "https://": v}
    return None
