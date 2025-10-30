from __future__ import annotations
import argparse
from pathlib import Path
from dotenv import load_dotenv

from src.common.log import get_logger
from ingestion.runner import (
    get_ownership_announcements,
    get_ownership_announcements_range,
    get_ownership_announcements_span,
    save_json,
)
from ingestion.utils.filters import validate_yyyymmdd, compute_month_range
from ingestion.utils.sorters import sort_announcements

"""CLI for fetching IDX ownership announcements."""

def main():
    """Parse args, dispatch a fetch mode, sort, and save."""
    p = argparse.ArgumentParser(description="Fetch IDX ownership announcements into a JSON file.")

    # Modes
    p.add_argument("--date", help="Single day (WIB), YYYYMMDD. Can pair with --start-hhmm/--end-hhmm.")
    p.add_argument("--start-hhmm", help="Optional window start (HH:MM, WIB); only with --date.")
    p.add_argument("--end-hhmm", help="Optional window end (HH:MM, WIB); only with --date.")
    p.add_argument("--from-date", dest="from_date", help="Range start (YYYYMMDD, WIB).")
    p.add_argument("--to-date", help="Range end (YYYYMMDD, WIB).")
    p.add_argument("--month", help="Full month: YYYYMM or YYYY-MM (WIB).")

    # Generic span with hours
    p.add_argument("--start", nargs=2, metavar=("YYYYMMDD", "HOUR"), help="Span start (WIB): YYYYMMDD HOUR[0-23].")
    p.add_argument("--end", nargs=2, metavar=("YYYYMMDD", "HOUR"), help="Span end (WIB): YYYYMMDD HOUR[0-23].")

    # Sorting + output
    p.add_argument("--sort", choices=["asc", "desc"], default="desc", help="Sort by publish time. Default: desc")
    p.add_argument("--out", default="data/ingestion.json", help="Output JSON path.")
    p.add_argument("--env-file", default=".env", help="Path to .env for PROXY, etc.")
    args = p.parse_args()

    # Env + logger
    load_dotenv(args.env_file, override=False)
    logger = get_logger("ingestion.cli")

    # Mode dispatch
    if args.start and args.end:
        s_date, s_hour = args.start[0], int(args.start[1])
        e_date, e_hour = args.end[0], int(args.end[1])
        logger.info("Mode: span (WIB) %s %02d:00 → %s %02d:59", s_date, s_hour, e_date, e_hour)
        data = get_ownership_announcements_span(s_date, s_hour, e_date, e_hour)
    elif args.date:
        logger.info("Mode: single-day (WIB) %s", args.date)
        data = get_ownership_announcements(args.date, args.start_hhmm, args.end_hhmm)
    elif args.from_date or args.to_date:
        if not (args.from_date and args.to_date):
            p.error("--from-date and --to-date must be provided together.")
        validate_yyyymmdd(args.from_date)
        validate_yyyymmdd(args.to_date)
        logger.info("Mode: range (WIB) %s → %s", args.from_date, args.to_date)
        data = get_ownership_announcements_range(args.from_date, args.to_date)
    elif args.month:
        start, end = compute_month_range(args.month)
        logger.info("Mode: month (WIB) %s → %s", start, end)
        data = get_ownership_announcements_range(start, end)
    else:
        p.error("Choose a mode: (--start ... --end ...) | --date | --from-date/--to-date | --month")

    # Sort + save
    data = sort_announcements(data, order=args.sort)
    save_json(data, Path(args.out))
    logger.info("Done. Wrote %d items → %s", len(data), args.out)


if __name__ == "__main__":
    main()