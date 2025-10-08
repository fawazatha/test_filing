from __future__ import annotations
from typing import Dict, List, Set

WHITELIST: Set[str] = {
    "bullish","bearish","takeover","investment","divestment",
    "free-float-requirement","MESOP","inheritance","share-transfer",
}

def _titlecase_like(s: str | None) -> str | None:
    if not s:
        return s
    # Title Case while preserving dashes (Software-It-Services)
    parts = []
    for token in str(s).split("-"):
        parts.append(token.strip().title())
    return "-".join(p for p in parts if p)

def normalize_row(row: Dict) -> Dict:
    # tags: whitelist + lowercase + list
    tags = row.get("tags") or []
    if isinstance(tags, str):
        # if someone accidentally json-dumped into a string
        import json
        try:
            tags = json.loads(tags)
        except Exception:
            tags = []
    tags = sorted({str(t).strip().lower() for t in tags if str(t).strip().lower() in WHITELIST})
    row["tags"] = tags

    # price_transaction: ensure dict, not string
    pt = row.get("price_transaction")
    if isinstance(pt, str):
        import json
        try:
            row["price_transaction"] = json.loads(pt)
        except Exception:
            pass

    # sector/sub_sector Title Case
    row["sector"] = _titlecase_like(row.get("sector"))
    row["sub_sector"] = _titlecase_like(row.get("sub_sector"))

    return row

def normalize_all(rows: List[Dict]) -> List[Dict]:
    return [normalize_row(r) for r in rows]
