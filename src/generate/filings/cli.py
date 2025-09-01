from __future__ import annotations

import argparse
from pathlib import Path
from .runner import main


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Generate filings_data.json from parsed IDX & NON-IDX inputs (with UID scenarios)",
    )
    p.add_argument("--idx-json", type=Path, required=False, help="Path to parsed_idx_output.json")
    p.add_argument("--non-idx-json", type=Path, required=False, help="Path to parsed_non_idx_output.json")
    p.add_argument("--downloads-json", type=Path, required=False, help="Optional: downloaded_pdfs.json (not mandatory)")
    p.add_argument("--company-map", type=Path, required=False, help="Optional: company_map.json for symbol->company_name lookup")
    p.add_argument("--out", type=Path, required=True, help="Output path for filings_data.json")
    p.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    return p


def entrypoint() -> None:
    p = _build_parser()
    args = p.parse_args()

    main(
        idx_path=args.idx_json,
        non_idx_path=args.non_idx_json,
        downloads_path=args.downloads_json,
        company_map_path=args.company_map,
        out_path=args.out,
        verbose=args.verbose,
    )


if __name__ == "__main__":
    entrypoint()
