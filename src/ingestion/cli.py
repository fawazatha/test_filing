import argparse
from pathlib import Path
from dotenv import load_dotenv

# from downloader.utils.logger import get_logger
from downloader.utils.logger import get_logger 
from ingestion.runner import (
    get_ownership_announcements,
    get_ownership_announcements_range,
    get_ownership_announcements_span,
    save_json,
)
from ingestion.utils.filters import (
    validate_yyyymmdd,
    compute_month_range,
)
from ingestion.utils.sorters import sort_announcements  # <-- NEW

def main():
    p = argparse.ArgumentParser(description="Fetch IDX ownership announcements into a JSON file.")

    # --- Modes ---
    p.add_argument("--date", help="Single day (WIB), YYYYMMDD. Can be paired with --start-hhmm/--end-hhmm.")
    p.add_argument("--start-hhmm", help="Optional publish window start (HH:MM in WIB), only with --date.")
    p.add_argument("--end-hhmm", help="Optional publish window end (HH:MM in WIB), only with --date.")

    p.add_argument("--from-date", dest="from_date", help="Start date (WIB) in YYYYMMDD for full-day RANGE.")
    p.add_argument("--to-date", help="End date (WIB) in YYYYMMDD for full-day RANGE.")

    p.add_argument("--month", help="Month shorthand: YYYYMM or YYYY-MM (full month).")

    # Generic span: --start 20250725 0 --end 20250801 23
    p.add_argument("--start", nargs=2, metavar=("YYYYMMDD", "HOUR"),
                   help="Generic span start (WIB): YYYYMMDD HOUR (0-23).")
    p.add_argument("--end", nargs=2, metavar=("YYYYMMDD", "HOUR"),
                   help="Generic span end (WIB): YYYYMMDD HOUR (0-23).")

    # Sorting control
    p.add_argument("--sort", choices=["asc", "desc"], default="desc",
                   help="Sort output by publish time (asc=oldest first, desc=newest first). Default: desc")

    # Common
    p.add_argument("--out", default="data/idx_announcements.json", help="Output JSON path.")
    p.add_argument("--env-file", default=".env", help="Path to .env (for PROXY, etc.)")
    p.add_argument("--verbose", action="store_true")
    args = p.parse_args()

    load_dotenv(args.env_file, override=False)
    logger = get_logger("ingestion", verbose=args.verbose)

    data = None

    # 1) Generic span (date+hour)
    if args.start and args.end:
        s_date, s_hour = args.start[0], int(args.start[1])
        e_date, e_hour = args.end[0], int(args.end[1])
        logger.info("[MODE] Generic span (date+hour) WIB")
        data = get_ownership_announcements_span(
            start_yyyymmdd=s_date,
            start_hour=s_hour,
            end_yyyymmdd=e_date,
            end_hour=e_hour,
            logger_name="ingestion",
        )

    # 2) Single-day mode
    elif args.date:
        logger.info("[MODE] Single-day (WIB)")
        data = get_ownership_announcements(
            date_yyyymmdd=args.date,
            start_hhmm=args.start_hhmm,
            end_hhmm=args.end_hhmm,
            logger_name="ingestion",
        )

    # 3) Range (full days)
    elif args.from_date or args.to_date:
        if not (args.from_date and args.to_date):
            p.error("--from-date and --to-date must be provided together.")
        if args.start_hhmm or args.end_hhmm:
            p.error("--start-hhmm/--end-hhmm only valid with --date.")
        validate_yyyymmdd(args.from_date)
        validate_yyyymmdd(args.to_date)

        logger.info("[MODE] Range (full days, WIB)")
        data = get_ownership_announcements_range(
            start_yyyymmdd=args.from_date,
            end_yyyymmdd=args.to_date,
            start_dt=None,
            end_dt=None,
            logger_name="ingestion",
        )

    # 4) Month (full days)
    elif args.month:
        start_yyyymmdd, end_yyyymmdd = compute_month_range(args.month)
        logger.info("[MODE] Month (full days, WIB): %s → %s", start_yyyymmdd, end_yyyymmdd)
        data = get_ownership_announcements_range(
            start_yyyymmdd=start_yyyymmdd,
            end_yyyymmdd=end_yyyymmdd,
            start_dt=None,
            end_dt=None,
            logger_name="ingestion",
        )
    else:
        p.error("Choose one mode: --start ... --end ... | --date [...] | --from-date --to-date | --month")

    # Apply sorting preference
    data = sort_announcements(data, order=args.sort)

    # Save
    out_path = Path(args.out)
    save_json(data, out_path, logger_name="ingestion")

if __name__ == "__main__":
    main()
