from typing import List, Dict
from datetime import datetime

from ingestion.utils.config import JKT
from ingestion.utils.filters import parse_publish_wib

def sort_announcements(items: List[Dict], order: str = "desc") -> List[Dict]:
    """
    Sort announcements by publish datetime (WIB).
    - order="desc": newest first (default)
    - order="asc": oldest first
    Secondary keys (title, link) make ties stable.
    """
    reverse = (order or "desc").lower() == "desc"

    def key_fn(d: Dict):
        # 'date' comes from the API as 'YYYY-MM-DDTHH:MM:SS' (no tz) → treat as WIB
        dt = None
        try:
            dt = parse_publish_wib(d.get("date", ""))
        except Exception:
            # Put unparsable dates at the extreme beginning/end depending on order
            dt = datetime.min.replace(tzinfo=JKT) if not reverse else datetime.max.replace(tzinfo=JKT)
        return (dt, d.get("title") or "", d.get("link") or "")

    return sorted(items, key=key_fn, reverse=reverse)
