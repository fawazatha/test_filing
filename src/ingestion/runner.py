from __future__ import annotations
from typing import List, Dict, Optional, Iterable
from datetime import datetime, timedelta
from pathlib import Path
import json

from src.common.log import get_logger
from src.common.datetime import timestamp_jakarta, JAKARTA_TZ as JKT

from ingestion.utils.config import DEFAULT_PAGE_SIZE
from ingestion.client import make_client, fetch_page
from ingestion.utils.filters import (
    parse_publish_wib,
    in_window,
    compute_range_and_window,
    validate_yyyymmdd,
    compute_span_from_date_hour,
)
from ingestion.utils.normalizer import normalize_item

"""Orchestrates fetch -> normalize -> save for IDX announcements."""

def _daterange_yyyymmdd(a: str, b: str) -> Iterable[str]:
    """Yield YYYYMMDD from a..b inclusive."""
    s = datetime.strptime(a, "%Y%m%d")
    e = datetime.strptime(b, "%Y%m%d")
    while s <= e:
        yield s.strftime("%Y%m%d")
        s += timedelta(days=1)


def _dedupe(items: List[Dict]) -> List[Dict]:
    """Deduplicate by (id) or (link,title,date)."""
    seen = set()
    out: List[Dict] = []
    for it in items:
        key = it.get("id") or (it.get("main_link") or it.get("link"), it.get("title"), it.get("date"))
        if key in seen:
            continue
        seen.add(key)
        out.append(it)
    return out


def get_ownership_announcements_range(
    start_yyyymmdd: str,
    end_yyyymmdd: str,
    start_dt: Optional[datetime] = None,
    end_dt: Optional[datetime] = None,
    logger_name: str = "ingestion",
) -> List[Dict]:
    """Fetch across date range (WIB) with optional time window."""
    logger = get_logger(logger_name)

    validate_yyyymmdd(start_yyyymmdd)
    validate_yyyymmdd(end_yyyymmdd)

    # Normalize tz for window
    if start_dt and start_dt.tzinfo is None:
        start_dt = start_dt.replace(tzinfo=JKT)
    if end_dt and end_dt.tzinfo is None:
        end_dt = end_dt.replace(tzinfo=JKT)

    if start_dt and end_dt:
        logger.info("Window: %s -> %s", start_dt, end_dt)
    else:
        logger.info("Window: full days")

    client = make_client()
    out: List[Dict] = []
    stamp = timestamp_jakarta()  # one scrape timestamp for batch

    for day in _daterange_yyyymmdd(start_yyyymmdd, end_yyyymmdd):
        page = 1
        logger.info("=== Day %s ===", day)
        while True:
            try:
                payload = fetch_page(client, day, day, page=page, page_size=DEFAULT_PAGE_SIZE)
            except Exception as e:
                logger.error("Fetch failed at %s page %d: %s", day, page, e)
                break

            items = payload.get("Items") or []
            if not items:
                logger.info("No more data at page %d for %s", page, day)
                break

            logger.info("Fetched %d items (page %d)", len(items), page)

            for item in items:
                pub = item.get("PublishDate")
                if not isinstance(pub, str):
                    continue
                try:
                    pub_dt = parse_publish_wib(pub)
                except Exception:
                    logger.warning("Bad PublishDate: %r", pub)
                    continue

                if not in_window(pub_dt, start_dt, end_dt):
                    continue

                item["_scraped_at"] = stamp
                n = normalize_item(item)
                if n:
                    out.append(n)

            if len(items) < DEFAULT_PAGE_SIZE:
                break
            page += 1

    return _dedupe(out)


def get_ownership_announcements(
    date_yyyymmdd: str,
    start_hhmm: Optional[str] = None,
    end_hhmm: Optional[str] = None,
    logger_name: str = "ingestion",
) -> List[Dict]:
    """Single-day fetch with optional HH:MM window (cross-midnight supported)."""
    start_date, end_date, start_dt, end_dt = compute_range_and_window(date_yyyymmdd, start_hhmm, end_hhmm)
    return get_ownership_announcements_range(start_date, end_date, start_dt, end_dt, logger_name)


def get_ownership_announcements_span(
    start_yyyymmdd: str,
    start_hour: int,
    end_yyyymmdd: str,
    end_hour: int,
    logger_name: str = "ingestion",
) -> List[Dict]:
    """Multi-day fetch with hour precision (WIB)."""
    date_from, date_to, start_dt, end_dt = compute_span_from_date_hour(
        start_yyyymmdd, start_hour, end_yyyymmdd, end_hour
    )
    return get_ownership_announcements_range(date_from, date_to, start_dt, end_dt, logger_name)


def save_json(data: List[Dict], out_path: Path, logger_name: str = "ingestion") -> None:
    """Write items to a JSON file (UTF-8)."""
    logger = get_logger(logger_name)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("Saved %d announcements -> %s", len(data), out_path)
