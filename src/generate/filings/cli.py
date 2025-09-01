import argparse
import logging
from .config import LOG_LEVEL
from .runner import run

def main():
    parser = argparse.ArgumentParser(description="Generate filings from parsed IDX/non-IDX + downloads map")
    parser.add_argument("--idx", default="data/parsed_idx_output.json")
    parser.add_argument("--non-idx", default="data/parsed_non_idx_output.json")
    parser.add_argument("--downloads", default="data/downloaded_pdfs.json")
    parser.add_argument("--out", default="data/filings_data.json")
    parser.add_argument("--alerts", default="alerts/inconsistent_alerts.json")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    level = logging.DEBUG if args.verbose else getattr(logging, LOG_LEVEL, logging.INFO)
    logging.basicConfig(level=level, format="%(asctime)s [%(levelname)s] %(message)s")

    count = run(
        parsed_files=[args.non_idx, args.idx],
        downloads_file=args.downloads,
        output_file=args.out,
        alerts_file=args.alerts,
    )
    print(f"[SUCCESS] Generated {count} filings -> {args.out}")

if __name__ == "__main__":
    main()
