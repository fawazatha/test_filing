from __future__ import annotations
from typing import List, Dict
from datetime import datetime

from src.common.datetime import JAKARTA_TZ as JKT
from ingestion.utils.filters import parse_publish_wib

"""Sorting utilities for announcements."""

def sort_announcements(items: List[Dict], order: str = "desc") -> List[Dict]:
    """Sort by publish time (WIB), then title, then link."""
    reverse = (order or "desc").lower() == "desc"

    def key_fn(d: Dict):
        try:
            dt = parse_publish_wib(d.get("date", ""))
        except Exception:
            dt = datetime.min.replace(tzinfo=JKT) if not reverse else datetime.max.replace(tzinfo=JKT)
        return (dt, d.get("title") or "", d.get("link") or "")

    return sorted(items, key=key_fn, reverse=reverse)