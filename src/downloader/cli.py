import argparse
import json
from pathlib import Path

from dotenv import load_dotenv  # auto-load .env
from downloader.runner import download_pdfs
from models.announcement import Announcement

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--input", required=True, help="Path to IDX announcements JSON")
    p.add_argument("--out-idx", default="downloads/idx-format")
    p.add_argument("--out-non-idx", default="downloads/non-idx-format")
    p.add_argument("--meta-out", default="data/downloaded_pdfs.json")
    p.add_argument("--alerts-out", default="alerts/low_title_similarity_alerts.json")
    p.add_argument("--retries", type=int, default=3)        # kept for compatibility
    p.add_argument("--min-similarity", type=int, default=80)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--verbose", action="store_true")
    p.add_argument("--env-file", default=".env", help="Path to .env (e.g., contains PROXY)")
    args = p.parse_args()

    # Load .env so PROXY and other variables are picked up automatically
    load_dotenv(args.env_file, override=False)

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
    )

if __name__ == "__main__":
    main()
