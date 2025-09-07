import os
import json
import shutil  # NEW
from pathlib import Path
from typing import List, Any, Optional

from downloader.utils.logger import get_logger
from downloader.utils.helper import (
    safe_filename_from_url,
    timestamp_jakarta,
    derive_ticker,
    filename_from_url,
)
from downloader.utils.classifier import classify_format
from downloader.client import init_http, get_pdf_bytes_minimal, seed_and_retry_minimal
from models.announcement import Announcement


def _attachment_to_url(att: Any) -> Optional[str]:
    """
    Normalize an attachment entry to a URL string if possible.
    Accepts:
      - str URL
      - dict with any of common URL-ish keys
    """
    if isinstance(att, str):
        return att if att.strip().lower().startswith("http") else None
    if isinstance(att, dict):
        for k in ["url", "link", "href", "download_url", "file_url", "FullSavePath", "filename", "path"]:
            v = att.get(k)
            if isinstance(v, str) and v.strip().lower().startswith("http"):
                return v
    return None


def download_pdfs(
    announcements: List[Announcement],
    out_idx: str,
    out_non_idx: str,
    meta_out: str,
    alerts_out: str,
    retries: int = 3,          
    min_similarity: int = 80,
    dry_run: bool = False,
    verbose: bool = False,
    clean_out: bool = False,   
):
    """
    Main downloader:
      - Classify title as IDX or NON-IDX using fuzz logic.
      - IDX     -> download from main_link.
      - NON-IDX -> download each URL in attachments.
      - If BOTH similarities are below threshold -> label UNKNOWN:
        * Do NOT download.
        * Append an alert entry only.
      - Minimal HTTP request (UA+Referer, verify=False); if it fails, seed referer then retry.
      - Metadata records follow the requested schema:
        {ticker, title, url, filename, timestamp}
    """
    logger = get_logger("downloader", verbose)

    # --- NEW: optional bersihkan output lebih dulu ---
    if clean_out:
        for d in (out_idx, out_non_idx):
            try:
                if os.path.isdir(d):
                    shutil.rmtree(d)
                    logger.info("Cleaned output folder: %s", d)
            except Exception as e:
                logger.warning("Failed to remove folder %s: %s", d, e)
        for f in (meta_out, alerts_out):
            try:
                os.remove(f)
                logger.info("Removed file: %s", f)
            except FileNotFoundError:
                pass
            except Exception as e:
                logger.warning("Failed to remove file %s: %s", f, e)

    # Initialize HTTP behavior (verify=False, silence SSL warnings)
    init_http(insecure=True, silence_warnings=True)

    # Ensure output directories exist
    Path(out_idx).mkdir(parents=True, exist_ok=True)
    Path(out_non_idx).mkdir(parents=True, exist_ok=True)
    Path(os.path.dirname(meta_out)).mkdir(parents=True, exist_ok=True)
    Path(os.path.dirname(alerts_out)).mkdir(parents=True, exist_ok=True)

    # Log if a proxy is detected (masking any secrets)
    proxy_env = (
        os.getenv("PROXY")
        or os.getenv("HTTPS_PROXY")
        or os.getenv("HTTP_PROXY")
        or os.getenv("https_proxy")
        or os.getenv("http_proxy")
    )
    if proxy_env:
        logger.info("Proxy detected from env.")

    records, alerts = [], []

    for i, ann in enumerate(announcements, start=1):
        label, best, sim_idx, sim_non = classify_format(ann.title, threshold=min_similarity)
        logger.info("[%d] %s", i, ann.title)
        logger.info("    Classification: %s (best=%d, idx=%d, non=%d)", label, best, sim_idx, sim_non)

        # Determine ticker (prefer company_name; fallback to [TICKER] in title)
        ticker = derive_ticker(ann.title, ann.company_name)

        # If UNKNOWN: no download, only alert
        if label == "UNKNOWN":
            ref_url = ann.main_link
            if not ref_url and ann.attachments:
                ref_url = _attachment_to_url(ann.attachments[0])

            alerts.append({
                "title": ann.title,
                "url": ref_url,
                "similarity_idx": sim_idx,
                "similarity_non_idx": sim_non,
                "threshold": min_similarity,
                "reason": "low_title_similarity_both",
                "severity": "warning",
                "created_at": timestamp_jakarta(),
            })
            logger.info("    Skipped download due to low similarity (UNKNOWN). Alert recorded.")
            continue

        # Build URL list by label
        urls: List[str] = []
        out_folder = out_non_idx
        if label == "IDX" and ann.main_link:
            urls = [ann.main_link]
            out_folder = out_idx
        elif label == "NON-IDX" and ann.attachments:
            urls = [u for u in (_attachment_to_url(x) for x in ann.attachments) if u]
            out_folder = out_non_idx

        if not urls:
            logger.warning("    No URLs found for this announcement (label=%s).", label)
            continue

        # For each URL, attempt download and record metadata
        for url in urls:
            filename = safe_filename_from_url(url)
            out_path = os.path.join(out_folder, filename)

            if dry_run:
                logger.info("    [DRY] Would download → %s -> %s", url, out_path)
                # record with requested schema
                records.append({
                    "ticker": ticker,
                    "title": ann.title,
                    "url": url,
                    "filename": filename_from_url(url),
                    "timestamp": ann.date,  # keep as-is from input
                })
                continue

            # Step 1: Minimal GET (same behavior as legacy)
            try:
                blob = get_pdf_bytes_minimal(url, timeout=60)
                with open(out_path, "wb") as f:
                    f.write(blob)
                logger.info("    Downloaded → %s", out_path)
            except Exception as e1:
                logger.error("    Minimal GET failed (%s): %s", url, e1)
                # Step 2: Seed referer and retry
                try:
                    blob = seed_and_retry_minimal(url, timeout=60)
                    with open(out_path, "wb") as f:
                        f.write(blob)
                    logger.info("    [retry] Downloaded → %s", out_path)
                except Exception as e2:
                    logger.error("    Minimal retry failed (%s): %s", url, e2)
                    continue  # next URL

            # Record success (requested schema)
            records.append({
                "ticker": ticker,
                "title": ann.title,
                "url": url,
                "filename": filename_from_url(url),
                "timestamp": ann.date,  # keep as-is from input
            })

    # Write outputs
    with open(meta_out, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2, ensure_ascii=False)

    with open(alerts_out, "w", encoding="utf-8") as f:
        json.dump(alerts, f, indent=2, ensure_ascii=False)

    logger.info("Finished. %d announcements processed.", len(announcements))
