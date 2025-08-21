from datetime import datetime, timedelta
from typing import Optional, Tuple
import calendar

from ingestion.utils.config import JKT

# ---------- Parsers & Validators ----------

def parse_publish_wib(publish_iso_no_tz: str) -> datetime:
    """
    Parse 'YYYY-MM-DDTHH:MM:SS' (no timezone) and treat it explicitly as Asia/Jakarta.
    """
    dt_naive = datetime.strptime(publish_iso_no_tz, "%Y-%m-%dT%H:%M:%S")
    return dt_naive.replace(tzinfo=JKT)

def validate_yyyymmdd(date_str: str) -> None:
    """
    Raise ValueError if the date string is not in YYYYMMDD.
    """
    datetime.strptime(date_str, "%Y%m%d")

def parse_year_month(year_month: str) -> Tuple[int, int]:
    """
    Accept 'YYYYMM' or 'YYYY-MM' and return (year, month).
    """
    s = year_month.strip()
    if "-" in s:
        year_s, month_s = s.split("-", 1)
    else:
        year_s, month_s = s[:4], s[4:]
    year, month = int(year_s), int(month_s)
    if not (1 <= month <= 12):
        raise ValueError("Month must be in 1..12")
    return year, month

# ---------- Windows & Ranges ----------

def in_window(dt: datetime, start_dt: Optional[datetime], end_dt: Optional[datetime]) -> bool:
    """
    Inclusive window check in WIB. If no window provided, returns True.
    """
    if start_dt is None or end_dt is None:
        return True
    return start_dt <= dt <= end_dt

def compute_range_and_window(
    date_yyyymmdd: str,
    start_hhmm: Optional[str],
    end_hhmm: Optional[str],
) -> Tuple[str, str, Optional[datetime], Optional[datetime]]:
    """
    Build dateFrom/dateTo (YYYYMMDD) and WIB-aware start/end datetimes for a single-day time window.
    If the time window crosses midnight, extend dateTo by +1 day.
    """
    validate_yyyymmdd(date_yyyymmdd)

    start_date = date_yyyymmdd
    end_date = date_yyyymmdd

    start_dt = None
    end_dt = None
    if start_hhmm and end_hhmm:
        base = datetime.strptime(date_yyyymmdd, "%Y%m%d").replace(tzinfo=JKT)
        s_h, s_m = [int(x) for x in start_hhmm.split(":")]
        e_h, e_m = [int(x) for x in end_hhmm.split(":")]
        start_dt = base.replace(hour=s_h, minute=s_m, second=0, microsecond=0)
        end_dt = base.replace(hour=e_h, minute=e_m, second=0, microsecond=0)

        # Cross-midnight range (e.g., 22:00→02:00) should extend dateTo
        if end_dt < start_dt:
            end_date = (base + timedelta(days=1)).strftime("%Y%m%d")

    return start_date, end_date, start_dt, end_dt

def compute_month_range(year_month: str) -> Tuple[str, str]:
    """
    Return (YYYYMMDD_start, YYYYMMDD_end) for the given month string ('YYYYMM' or 'YYYY-MM').
    """
    year, month = parse_year_month(year_month)
    first_day = 1
    last_day = calendar.monthrange(year, month)[1]
    start_yyyymmdd = f"{year:04d}{month:02d}{first_day:02d}"
    end_yyyymmdd = f"{year:04d}{month:02d}{last_day:02d}"
    return start_yyyymmdd, end_yyyymmdd

def compute_span_from_date_hour(
    start_yyyymmdd: str,
    start_hour: int,
    end_yyyymmdd: str,
    end_hour: int,
) -> Tuple[str, str, datetime, datetime]:
    """
    Generic span:
      Inputs:
        - start_yyyymmdd: 'YYYYMMDD'
        - start_hour: 0..23
        - end_yyyymmdd: 'YYYYMMDD'
        - end_hour: 0..23
      Returns:
        (date_from, date_to, start_dt_wib, end_dt_wib)

    Validates format and that end >= start.
    """
    validate_yyyymmdd(start_yyyymmdd)
    validate_yyyymmdd(end_yyyymmdd)
    if not (0 <= int(start_hour) <= 23 and 0 <= int(end_hour) <= 23):
        raise ValueError("Hours must be in 0..23")

    s_base = datetime.strptime(start_yyyymmdd, "%Y%m%d").replace(tzinfo=JKT)
    e_base = datetime.strptime(end_yyyymmdd, "%Y%m%d").replace(tzinfo=JKT)
    start_dt = s_base.replace(hour=int(start_hour), minute=0, second=0, microsecond=0)
    end_dt = e_base.replace(hour=int(end_hour), minute=0, second=0, microsecond=0)

    if end_dt < start_dt:
        raise ValueError("End datetime must be >= start datetime")

    # API dateFrom/dateTo are full-day bounds (inclusive); we hand them the calendar span
    date_from = start_dt.strftime("%Y%m%d")
    date_to = end_dt.strftime("%Y%m%d")
    return date_from, date_to, start_dt, end_dt
