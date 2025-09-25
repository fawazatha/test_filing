from __future__ import annotations
from typing import Any, Dict, Iterable, List, Optional, Tuple
import os
import httpx
from dotenv import load_dotenv
load_dotenv()

def _sb_base() -> str:
    url = os.getenv("SUPABASE_URL")
    if not url: raise RuntimeError("SUPABASE_URL is not set")
    return url.rstrip("/")

def _sb_headers() -> Dict[str, str]:
    key = os.getenv("SUPABASE_KEY")
    if not key: raise RuntimeError("SUPABASE_KEY is not set")
    return {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Accept": "application/json",
        "Prefer": "count=exact",
    }

def _build_query_params(
    *,
    select: str = "*",
    order: Optional[str] = None,
    filters: Optional[List[Tuple[str, str]]] = None,
    in_filters: Optional[Dict[str, Iterable[Any]]] = None,
) -> List[Tuple[str, str]]:
    qs: List[Tuple[str, str]] = [("select", select)]
    if filters:
        qs.extend(filters)
    if in_filters:
        for k, vals in in_filters.items():
            items: List[str] = []
            for v in vals or []:
                s = str(v)
                if ("," in s) or (" " in s):
                    s = f'"{s}"'
                items.append(s)
            qs.append((k, f"in.({','.join(items)})"))
    if order:
        qs.append(("order", order))
    return qs

async def rest_get_all(
    table: str,
    *,
    select: str = "*",
    filters: Optional[List[Tuple[str, str]]] = None,
    in_filters: Optional[Dict[str, Iterable[Any]]] = None,
    order: Optional[str] = None,
    page_size: int = 1000,
    timeout: float = 60.0,
) -> List[Dict[str, Any]]:
    """Paginated GET with Range headers. Keeps duplicate keys (e.g., timestamp=gt.. & timestamp=lt..)."""
    base = _sb_base()
    headers = _sb_headers()
    out: List[Dict[str, Any]] = []
    start = 0
    async with httpx.AsyncClient(timeout=timeout) as client:
        while True:
            qs = _build_query_params(select=select, order=order, filters=filters, in_filters=in_filters)
            url = f"{base}/rest/v1/{table}?{httpx.QueryParams(qs)}"
            hdrs = dict(headers)
            hdrs["Range-Unit"] = "items"
            hdrs["Range"] = f"{start}-{start + page_size - 1}"
            r = await client.get(url, headers=hdrs)
            r.raise_for_status()
            batch = r.json()
            out.extend(batch)
            cr = r.headers.get("content-range")
            got = len(batch)
            if not cr or got < page_size or got == 0:
                break
            start += got
    return out
