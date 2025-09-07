import argparse
import json
from pathlib import Path

from downloader.utils.logger import get_logger
from models.announcement import Announcement
from downloader.runner import download_pdfs


def main():
    p = argparse.ArgumentParser(description="Download IDX/NON-IDX PDFs and produce simple metadata JSON.")
    p.add_argument("--input", required=True, help="Path to idx_announcements.json")
    p.add_argument("--out-idx", default="downloads/idx-format")
    p.add_argument("--out-non-idx", default="downloads/non-idx-format")
    p.add_argument("--meta-out", default="data/downloaded_pdfs.json")
    p.add_argument("--alerts-out", default="alerts/low_title_similarity_alerts.json")
    p.add_argument("--retries", type=int, default=3) 
    p.add_argument("--min-similarity", type=int, default=80)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--verbose", action="store_true")
    p.add_argument("--clean-out", action="store_true",   
                   help="Hapus folder output & reset meta/alerts sebelum download.")  
    args = p.parse_args()

    logger = get_logger("downloader", verbose=args.verbose)

    # Load announcements
    data = json.loads(Path(args.input).read_text(encoding="utf-8"))
    anns = [Announcement(**d) for d in data]

    # Run
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

if __name__ == "__main__":
    main()
