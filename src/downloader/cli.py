from __future__ import annotations
import argparse
import json
from pathlib import Path

from src.common.log import get_logger
from downloader.runner import download_pdfs
from models.announcement import Announcement

"""CLI wrapper to download PDFs from announcements JSON."""

def main():
    p = argparse.ArgumentParser(description="Download IDX/NON-IDX PDFs and output metadata + alerts JSON.")
    p.add_argument("--input", required=True, help="Path to announcements JSON (output of ingestion).")
    p.add_argument("--out-idx", default="downloads/idx-format", help="Destination folder for IDX-format PDFs.")
    p.add_argument("--out-non-idx", default="downloads/non-idx-format", help="Destination folder for NON-IDX PDFs.")
    p.add_argument("--meta-out", default="data/downloaded_pdfs.json", help="Metadata output JSON path.")
    p.add_argument("--alerts-out", default="alerts/low_title_similarity_alerts.json", help="Alerts output JSON path.")
    p.add_argument("--retries", type=int, default=3, help="Total attempts per URL (1 minimal + retries-1 seeded).")
    p.add_argument("--min-similarity", type=int, default=80, help="Fuzzy threshold for IDX/NON-IDX title match.")
    p.add_argument("--dry-run", action="store_true", help="Do not download, only print and record metadata skeleton.")
    p.add_argument("--verbose", action="store_true", help="Verbose logs.")
    p.add_argument("--clean-out", action="store_true", help="Remove outputs before running.")
    args = p.parse_args()

    logger = get_logger("downloader.cli", args.verbose)

    data = json.loads(Path(args.input).read_text(encoding="utf-8"))
    anns = [Announcement(**d) for d in data]

    download_pdfs(
        announcements=anns,
        out_idx=args.out_idx,
        out_non_idx=args.out_non_idx,
        meta_out=args.meta_out,
        alerts_out=args.alerts_out,
        retries=args.retries,
        min_similarity=args.min_similarity,
        dry_run=args.dry_run,
        verbose=args.verbose,
        clean_out=args.clean_out,
    )

    logger.info("Done.")


if __name__ == "__main__":
    main()