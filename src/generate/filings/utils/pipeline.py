from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional

# Core transformer
from src.core.transformer import transform_many

# Pipeline steps
# Impor build_ingestion_map yang sudah diperbarui
from .loaders import load_parsed_files, build_downloads_meta_map, build_ingestion_map
from .processors import process_all_records
from .consolidators import dedupe_rows

log = logging.getLogger("filings.pipeline")

def _stage_log(label: str, count: int, note: str = ""):
    log.info("[STAGE] %-12s → %d records %s", label, count, note)


def run(
    *,
    parsed_files: List[str],
    downloads_file: str,
    output_file: str,
    ingestion_file: str, # Ini adalah argumen dari cli.py
    alerts_file: Optional[str] = None,
    **kwargs,
) -> int:
    
    # 1) LOAD
    parsed_chunks = load_parsed_files(parsed_files)
    raw_rows: List[Dict[str, Any]] = [row for chunk in parsed_chunks for row in chunk]
    _stage_log("Loaded", len(raw_rows))
    
    # 2) LOAD MAPS
    downloads_meta_map = build_downloads_meta_map(downloads_file)
    # Memuat peta ingestion (sekarang berisi dict penuh)
    ingestion_map = build_ingestion_map(ingestion_file)

    # 3) TRANSFORM (Meneruskan ingestion_map yang baru)
    records = transform_many(raw_rows, ingestion_map=ingestion_map)
    _stage_log("Transformed", len(records), "(Standardized to FilingRecord)")

    # 4) PROCESS (Audit, Price Checks)
    records = process_all_records(records, downloads_meta_map)
    _stage_log("Processed", len(records), "(Price checks & audits done)")

    # 5) DEDUPE (In-batch)
    records = dedupe_rows(records)
    _stage_log("Deduped", len(records))

    # 6) SAVE
    outp = Path(output_file)
    outp.parent.mkdir(parents=True, exist_ok=True)
    
    output_data = [rec.to_db_dict() for rec in records]
    
    outp.write_text(
        json.dumps(output_data, ensure_ascii=False, indent=2, default=str), 
        encoding="utf-8"
    )
    log.info("[STAGE] Wrote      → %s", outp)
    return len(records)