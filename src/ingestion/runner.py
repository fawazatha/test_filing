from typing import List, Dict, Optional
from datetime import datetime
from pathlib import Path
import json

from downloader.utils.logger import get_logger
from ingestion.utils.config import JKT, DEFAULT_PAGE_SIZE
from ingestion.client import make_session, fetch_page
from ingestion.utils.filters import (
    parse_publish_wib,
    in_window,
    compute_range_and_window,
    validate_yyyymmdd,
    compute_span_from_date_hour,
)
from ingestion.utils.normalizer import normalize_item

def get_ownership_announcements_range(
    start_yyyymmdd: str,
    end_yyyymmdd: str,
    start_dt: Optional[datetime] = None,
    end_dt: Optional[datetime] = None,
    logger_name: str = "ingestion",
) -> List[Dict]:
    """
    Fetch announcements across an arbitrary date range [start_yyyymmdd, end_yyyymmdd] (inclusive).
    If start_dt/end_dt are provided, they will be used as a publish-time window (WIB) filter.
    """
    logger = get_logger(logger_name, verbose=True)

    # Validate date strings
    validate_yyyymmdd(start_yyyymmdd)
    validate_yyyymmdd(end_yyyymmdd)

    if start_dt and start_dt.tzinfo is None:
        start_dt = start_dt.replace(tzinfo=JKT)
    if end_dt and end_dt.tzinfo is None:
        end_dt = end_dt.replace(tzinfo=JKT)

    if start_dt and end_dt:
        logger.info("[FILTER] Publish window: %s → %s", start_dt, end_dt)
    else:
        logger.info("[FILTER] No time window (full days)")

    session = make_session()
    page = 1
    out: List[Dict] = []

    while True:
        try:
            payload = fetch_page(session, start_yyyymmdd, end_yyyymmdd, page=page, page_size=DEFAULT_PAGE_SIZE)
        except Exception as e:
            logger.error("API fetch failed at page %d: %s", page, e)
            break

        items = payload.get("Items") or []
        if not items:
            logger.info("No more data at page %d", page)
            break

        logger.info("Fetched %d announcements from page %d", len(items), page)

        for item in items:
            pub = item.get("PublishDate")
            if not isinstance(pub, str):
                continue
            try:
                pub_dt = parse_publish_wib(pub)
            except Exception:
                logger.warning("Failed to parse PublishDate '%s'", pub)
                continue

            if not in_window(pub_dt, start_dt, end_dt):
                continue

            item["_scraped_at"] = datetime.now(JKT).isoformat()
            normalized = normalize_item(item)
            if normalized:
                out.append(normalized)

        if len(items) < DEFAULT_PAGE_SIZE:
            break
        page += 1

    return out

def get_ownership_announcements(
    date_yyyymmdd: str,
    start_hhmm: Optional[str] = None,
    end_hhmm: Optional[str] = None,
    logger_name: str = "ingestion",
) -> List[Dict]:
    """
    Convenience wrapper for single-day fetch with an optional HH:MM time window.
    """
    start_date, end_date, start_dt, end_dt = compute_range_and_window(date_yyyymmdd, start_hhmm, end_hhmm)
    return get_ownership_announcements_range(
        start_yyyymmdd=start_date,
        end_yyyymmdd=end_date,
        start_dt=start_dt,
        end_dt=end_dt,
        logger_name=logger_name,
    )

def get_ownership_announcements_span(
    start_yyyymmdd: str,
    start_hour: int,
    end_yyyymmdd: str,
    end_hour: int,
    logger_name: str = "ingestion",
) -> List[Dict]:
    """
    Generic multi-day span with hour precision (WIB).
    Example:
      start_yyyymmdd='20250725', start_hour=0
      end_yyyymmdd='20250801', end_hour=23
    """
    date_from, date_to, start_dt, end_dt = compute_span_from_date_hour(
        start_yyyymmdd, start_hour, end_yyyymmdd, end_hour
    )
    logger = get_logger(logger_name, verbose=True)
    logger.info("[RUN] Span: %s %02d:00 → %s %02d:00 (WIB)", start_yyyymmdd, start_hour, end_yyyymmdd, end_hour)

    return get_ownership_announcements_range(
        start_yyyymmdd=date_from,
        end_yyyymmdd=date_to,
        start_dt=start_dt,
        end_dt=end_dt,
        logger_name=logger_name,
    )

def save_json(data: List[Dict], out_path: Path, logger_name: str = "ingestion") -> None:
    """
    Save announcements to a JSON file.
    """
    logger = get_logger(logger_name, verbose=True)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("[SAVED] %d announcements to %s", len(data), out_path)
