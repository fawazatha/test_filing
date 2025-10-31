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