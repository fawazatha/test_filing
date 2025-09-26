# src/generate/reports/utils/daily_window.py
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timedelta
try:
    import zoneinfo
    JKT = zoneinfo.ZoneInfo("Asia/Jakarta")
except Exception:
    JKT = None

@dataclass
class Window:
    start: datetime
    end: datetime

def _to_jkt(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=JKT) if JKT else dt
    return dt.astimezone(JKT) if JKT else dt

def _last_weekday(dt: datetime) -> datetime:
    # Return previous working day (Mon–Fri). If Monday -> last Friday.
    wd = dt.weekday()  # Mon=0..Sun=6
    if wd == 0:  # Monday -> go back 3 days to Friday
        return dt - timedelta(days=3)
    elif wd in (1,2,3,4):  # Tue..Fri -> back 1 day
        return dt - timedelta(days=1)
    else:
        # Sat/Sun: find last Friday
        delta = (wd - 4)  # e.g. Sat(5)->1, Sun(6)->2
        return dt - timedelta(days=delta)

def daily_1745_window(now: datetime | None = None) -> Window:
    """Return [last_workday 17:45, today 17:45] in JKT (half-open recommended downstream)."""
    now = _to_jkt(now or datetime.now(tz=JKT))
    anchor_today = now.replace(hour=17, minute=45, second=0, microsecond=0)
    # If we run before 17:45, treat 'today 17:45' as not yet closed: shift to previous workday.
    if now < anchor_today:
        end_day = _last_weekday(now)
    else:
        end_day = now
    start_day = _last_weekday(end_day)
    start = _to_jkt(start_day.replace(hour=17, minute=45, second=0, microsecond=0))
    end   = _to_jkt(end_day.replace(hour=17, minute=45, second=0, microsecond=0))
    return Window(start=start, end=end)
