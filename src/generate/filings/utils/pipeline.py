from __future__ import annotations

import json
from pathlib import Path
from typing import List

from .loaders import load_parsed_files, build_downloads_meta_map
from .processors import process_all
from .normalizers import normalize_all
from .consolidators import dedupe_rows

def run(
    *,
    parsed_files: List[str],
    downloads_file: str,
    output_file: str,
    alerts_file: str | None = None,  # reserved (suspicious)
) -> int:
    # 1) load inputs
    parsed_lists = load_parsed_files(parsed_files)
    meta_map = build_downloads_meta_map(downloads_file)

    # 2) process → rows
    rows = process_all(parsed_lists, meta_map)

    # 3) normalize
    rows = normalize_all(rows)

    # 4) dedupe
    rows = dedupe_rows(rows)

    # 5) (optional) filter out duplicates from downstream alerts
    # leave all rows in the JSON; email/gating can skip is_duplicate

    # 6) save
    outp = Path(output_file)
    outp.parent.mkdir(parents=True, exist_ok=True)
    outp.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")

    return len(rows)
