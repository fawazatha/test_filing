# src/generate/reports/utils/datetimes.py
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

JKT = timezone(timedelta(hours=7))

@dataclass
class Window:
    start: datetime
    end: datetime

def parse_local_or_iso(s: Optional[str], default_tz=JKT) -> Optional[datetime]:
    if not s: return None
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=default_tz)
    return dt

def now_jkt() -> datetime:
    return datetime.now(JKT).replace(microsecond=0)

def resolve_window(
    from_s: Optional[str],
    to_s: Optional[str],
    default_hours: int = 24,
) -> Window:
    """If from is None -> now-24h .. now (JKT); to None -> now (JKT)."""
    now = now_jkt()
    start = parse_local_or_iso(from_s) or (now - timedelta(hours=default_hours))
    end = parse_local_or_iso(to_s) or now
    if start >= end:
        raise ValueError("start must be earlier than end")
    return Window(start, end)

def fmt_for_ts_kind(dt: datetime, ts_kind: str) -> str:
    """Return string for PostgREST filter depending on column kind."""
    if ts_kind == "timestamptz":
        return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    # plain timestamp (no tz): format as local JKT without tz suffix
    return dt.astimezone(JKT).strftime("%Y-%m-%d %H:%M:%S")
