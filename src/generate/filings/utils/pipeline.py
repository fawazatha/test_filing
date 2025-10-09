from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple

from .loaders import load_parsed_files, build_downloads_meta_map
from .processors import process_all
from .normalizers import normalize_all
from .consolidators import dedupe_rows

log = logging.getLogger("filings.pipeline")

def _stage_log(label: str, chunks: List[List[Dict[str, Any]]]) -> None:
    # Print ringkas per-file jumlahnya agar mudah dilihat
    totals = [len(c) for c in chunks]
    log.info("[STAGE] %-12s chunks=%s totals=%s sum=%d",
             label, len(chunks), totals, sum(totals))

def _split_by_source(rows: List[Dict[str, Any]]) -> Tuple[int, int]:
    # Heuristik: parsed IDX biasanya punya path 'idx-format' di source/filename
    idx = sum(1 for r in rows if str(r.get("source") or "").find("idx-format") >= 0)
    non = len(rows) - idx
    return idx, non

def run(
    *,
    parsed_files: List[str],
    downloads_file: str,
    output_file: str,
    alerts_file: Optional[str] = None,
    **kwargs,
) -> int:
    # 1) LOAD PARSED
    parsed_chunks = load_parsed_files(parsed_files)
    _stage_log("loaded", parsed_chunks)

    # 2) DOWNLOADS META (opsional)
    downloads_meta_map = build_downloads_meta_map(downloads_file)

    # 3) PROCESS
    rows = process_all(parsed_chunks, downloads_meta_map)
    idx_n, non_n = _split_by_source(rows)
    log.info("[STAGE] processed  → total=%d (idx-like=%d, non-idx-like=%d)", len(rows), idx_n, non_n)

    # 4) NORMALIZE
    rows = normalize_all(rows)
    idx_n, non_n = _split_by_source(rows)
    log.info("[STAGE] normalized → total=%d (idx-like=%d, non-idx-like=%d)", len(rows), idx_n, non_n)

    # 5) DEDUPE
    rows = dedupe_rows(rows)
    idx_n, non_n = _split_by_source(rows)
    log.info("[STAGE] deduped    → total=%d (idx-like=%d, non-idx-like=%d)", len(rows), idx_n, non_n)

    # 6) SAVE
    outp = Path(output_file)
    outp.parent.mkdir(parents=True, exist_ok=True)
    outp.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    log.info("[STAGE] wrote      → %s", outp)
    return len(rows)
