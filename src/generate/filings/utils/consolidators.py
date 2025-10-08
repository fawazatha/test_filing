from __future__ import annotations
from typing import Dict, List, Tuple
from datetime import datetime

def _key(row: Dict) -> Tuple:
    # group key per DoD
    # date::date → we derive from announcement_published_at or timestamp (YYYY-MM-DD)
    ts = row.get("announcement_published_at") or row.get("timestamp") or ""
    try:
        d = datetime.fromisoformat(str(ts).replace(" ", "T")).date().isoformat()
    except Exception:
        d = ""
    return (
        row.get("symbol"),
        row.get("holder_name"),
        row.get("holding_before"),
        row.get("holding_after"),
        row.get("share_percentage_before"),
        row.get("share_percentage_after"),
        row.get("source"),
        d,
    )

def dedupe_rows(rows: List[Dict]) -> List[Dict]:
    groups: Dict[Tuple, List[Dict]] = {}
    for r in rows:
        groups.setdefault(_key(r), []).append(r)

    out: List[Dict] = []
    for k, grp in groups.items():
        if len(grp) == 1:
            out.append(grp[0])
            continue
        # keep earliest created / or one already edited (heuristic)
        def score(x: Dict):
            edited = 1 if x.get("edited_by") or x.get("review_notes") else 0
            created = x.get("created_at") or ""
            return (edited * 10, str(created))
        grp.sort(key=score, reverse=True)
        keep, rest = grp[0], grp[1:]
        out.append(keep)
        for r in rest:
            r["is_duplicate"] = True
            r["skip_reason"] = "duplicate"
    return out
