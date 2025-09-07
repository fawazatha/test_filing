from __future__ import annotations
import logging
from typing import List, Optional
from .loaders import build_download_map, load_parsed_items, save_json, ensure_dir
from .processors import enrich_and_filter_items

logger = logging.getLogger(__name__)

def generate_filings(
    parsed_files: Optional[List[str]] = None,
    downloads_file: str = "data/downloaded_pdfs.json",
    output_file: str = "data/filings_data.json",
    alerts_file: str = "alerts/suspicious_alerts.json",
) -> int:
    parsed_files = parsed_files or ["data/parsed_non_idx_output.json", "data/parsed_idx_output.json"]
    logger.info("Starting filings generation...")
    download_map = build_download_map(downloads_file)
    parsed_items = load_parsed_items(parsed_files)

    results, alerts = enrich_and_filter_items(parsed_items, download_map)

    ensure_dir(output_file);  save_json(output_file, results)
    logger.info("Wrote %d filings → %s", len(results), output_file)

    ensure_dir(alerts_file);  save_json(alerts_file, alerts)
    logger.info("Wrote %d alerts → %s", len(alerts), alerts_file)

    return len(results)
