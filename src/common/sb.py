# src/generate/reports/utils/sb.py
from __future__ import annotations
import os
import json
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

try:
    import httpx  # pip install httpx>=0.27
except Exception as e:
    raise RuntimeError("Please `pip install httpx` to use utils.sb") from e


def _base_url() -> str:
    url = os.getenv("SUPABASE_URL", "").rstrip("/")
    if not url:
        raise RuntimeError("Missing SUPABASE_URL env")
    return f"{url}/rest/v1"

def _headers() -> Dict[str, str]:
    key = os.getenv("SUPABASE_KEY", "")
    if not key:
        raise RuntimeError("Missing SUPABASE_KEY env")
    return {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Prefer": "count=exact",
    }

def _apply_filters(params: Dict[str, str], filters: Optional[List[Tuple[str, str]]]) -> None:
    """
    filters: list of (column, "op.value"), e.g. ("timestamp", "gt.2025-09-25T17:45:00+07:00")
    Will set params["timestamp"] = "gt.2025-09-25T17:45:00+07:00"
    """
    if not filters:
        return
    for col, expr in filters:
        if not col or not expr:
            continue
        params[col] = expr

def _apply_in_filters(params: Dict[str, str], in_filters: Optional[Mapping[str, Sequence[Any]]]) -> None:
    """
    in_filters: {"symbol": ["BBCA.JK","BBRI.JK"]}
    -> params["symbol"] = "in.(BBCA.JK,BBRI.JK)"
    """
    if not in_filters:
        return
    for col, seq in in_filters.items():
        if not seq:
            continue
        items = ",".join(str(x) for x in seq)
        params[col] = f"in.({items})"

async def fetch(
    *,
    table: str,
    select: str = "*",
    order: Optional[str] = None,   # e.g. "timestamp.asc,id.asc"
    page_size: int = 1000,         # mapped to `limit`
    offset: int = 0,
    timeout: float = 30.0,
    # two styles of filters supported to match core.py usage:
    filters: Optional[List[Tuple[str, str]]] = None,          # ("col","op.value")
    in_filters: Optional[Mapping[str, Sequence[Any]]] = None, # {"col":[...]}
    # also support explicit operators if someday needed:
    eq: Optional[Mapping[str, Any]] = None,
    gte: Optional[Mapping[str, Any]] = None,
    lte: Optional[Mapping[str, Any]] = None,
    lt: Optional[Mapping[str, Any]] = None,
    gt: Optional[Mapping[str, Any]] = None,
    ilike: Optional[Mapping[str, str]] = None,
) -> List[Dict[str, Any]]:
    """
    One-shot GET (single page). Compatible with core.py's call:
      sbapi.fetch(table=..., select=..., filters=[...], in_filters={...}, order="ts.asc,id.asc", page_size=1000)
    """
    url = f"{_base_url()}/{table}"
    params: Dict[str, str] = {"select": select, "limit": str(page_size), "offset": str(offset)}
    if order:
        # PostgREST supports multi-order with commas, e.g. order=ts.asc,id.asc
        params["order"] = order

    # Apply flexible filters
    _apply_filters(params, filters)
    _apply_in_filters(params, in_filters)

    # Also support direct ops (optional)
    def _set_ops(prefix: str, m: Optional[Mapping[str, Any]]):
        if not m:
            return
        for col, val in m.items():
            params[col] = f"{prefix}.{val}"

    _set_ops("eq", eq)
    _set_ops("gte", gte)
    _set_ops("lte", lte)
    _set_ops("lt", lt)
    _set_ops("gt", gt)

    if ilike:
        for col, pat in ilike.items():
            params[col] = f"ilike.{pat}"

    async with httpx.AsyncClient(timeout=timeout) as client:
        r = await client.get(url, headers=_headers(), params=params)
        r.raise_for_status()
        try:
            return r.json()
        except Exception:
            text = r.text.strip()
            return json.loads(text) if text else []

async def fetch_all(
    *,
    table: str,
    select: str = "*",
    order: Optional[str] = None,
    page_size: int = 1000,
    timeout: float = 30.0,
    filters: Optional[List[Tuple[str, str]]] = None,
    in_filters: Optional[Mapping[str, Sequence[Any]]] = None,
) -> List[Dict[str, Any]]:
    """
    Simple paginator: keeps calling fetch with increasing offset until page < page_size.
    """
    out: List[Dict[str, Any]] = []
    offset = 0
    while True:
        page = await fetch(
            table=table, select=select, order=order,
            page_size=page_size, offset=offset, timeout=timeout,
            filters=filters, in_filters=in_filters
        )
        if not page:
            break
        out.extend(page)
        if len(page) < page_size:
            break
        offset += page_size
    return out
