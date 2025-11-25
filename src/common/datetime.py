from __future__ import annotations 
import re
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional 

# Constants
# Asia/Jakarta (UTC+7) without external tz dependencies
JAKARTA_TZ = timezone(timedelta(hours=7))

MONTHS_EN: Dict[str, int] = {m.lower(): i for i, m in enumerate(
    ["January","February","March","April","May","June","July","August","September","October","November","December"], 1)}

MONTHS_ID: Dict[str, int] = {
    "januari":1,"februari":2,"maret":3,"april":4,"mei":5,"juni":6,
    "juli":7,"agustus":8,"september":9,"oktober":10,"november":11,"desember":12
}

# MONTHS_EN = {
#     "january":1, "february":2, "march":3, "april":4, "may":5, "june":6,
#     "july":7, "august":8, "september":9, "october":10, "november":11, "december":12
# }


PAT_EN_FULL = re.compile(r"\b(\d{1,2})\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})\b", re.I)
PAT_ID_FULL = re.compile(r"\b(\d{1,2})\s+(Januari|Februari|Maret|April|Mei|Juni|Juli|Agustus|September|Oktober|November|Desember)\s+(\d{4})\b", re.I)

# Functions 
def parse_id_en_date(s: str) -> Optional[str]:
    """
    Parses an Indonesian or English full date string into 'YYYYMMDD' format.
    """
    m = PAT_EN_FULL.search(s)
    if m:
        d, mon, y = int(m.group(1)), MONTHS_EN[m.group(2).lower()], int(m.group(3))
        return f"{y:04d}{mon:02d}{d:02d}"
    
    m = PAT_ID_FULL.search(s)
    if m:
        d, mon, y = int(m.group(1)), MONTHS_ID[m.group(2).lower()], int(m.group(3))
        return f"{y:04d}{mon:02d}{d:02d}"
    
    return None

def timestamp_jakarta() -> str:
    """Return ISO timestamp in Asia/Jakarta timezone (YYYY-MM-DDTHH:MM:SS)."""
    return datetime.now(JAKARTA_TZ).replace(microsecond=0).isoformat()

def now_wib() -> datetime:
    return datetime.now(tz=JAKARTA_TZ)

def iso_wib(dt: Optional[datetime] = None, seconds_only: bool = True) -> str:
    dt = dt or now_wib()
    if seconds_only:
        return dt.replace(microsecond=0).isoformat()
    return dt.isoformat()

def iso_utc(dt: Optional[datetime] = None, seconds_only: bool = True) -> str:
    dt = dt or datetime.now(timezone.utc)
    if seconds_only:
        return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")
    return dt.isoformat().replace("+00:00", "Z")

def _ensure_wib(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=JAKARTA_TZ)
    return dt.astimezone(JAKARTA_TZ)


def fmt_wib_date(dt: datetime, fmt: str = "%d %b %Y") -> str:
    wib = _ensure_wib(dt)
    return wib.strftime(fmt)


def fmt_wib_range(start: datetime, end: datetime) -> str:
    """
    Format a WIB date-time range as a compact string.
    """
    s = _ensure_wib(start)
    e = _ensure_wib(end)

    if s.date() == e.date():
        return f"{s.strftime('%d %b %Y %H:%M')} – {e.strftime('%H:%M')} WIB"

    return f"{s.strftime('%d %b %Y %H:%M')} – {e.strftime('%d %b %Y %H:%M')} WIB"
