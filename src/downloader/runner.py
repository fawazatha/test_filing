from __future__ import annotations
import os
import json
import shutil
from pathlib import Path
from typing import List, Any, Optional, Dict

from src.common.log import get_logger
from src.common.time import timestamp_jakarta
from src.common.files import ensure_dir, ensure_parent, safe_filename_from_url

from downloader.utils.classifier import classify_format
from downloader.client import init_http, get_pdf_bytes_minimal, seed_and_retry_minimal
from models.announcement import Announcement

"""Download PDFs (IDX / non-IDX) and emit lightweight metadata & alerts."""

# helpers
def _attachment_to_url(att: Any) -> Optional[str]:
    """Extract a URL string from various attachment shapes."""
    if isinstance(att, str):
        s = att.strip()
        return s if s.lower().startswith("http") else None
    if isinstance(att, dict):
        for k in ("url", "link", "href", "download_url", "file_url", "FullSavePath", "filename", "path"):
            v = att.get(k)
            if isinstance(v, str) and v.strip().lower().startswith("http"):
                return v
    return None


def _derive_ticker(title: str, company_name: Optional[str]) -> Optional[str]:
    """Prefer 'company_name'; fallback to [TICKER] pattern in title."""
    if company_name and company_name.strip():
        return company_name.strip().upper()[:5]
    import re
    m = re.search(r"\[([A-Z]{3,5})\s*\]", (title or "").upper())
    return m.group(1) if m else None


def _maybe_clean_outputs(paths: Dict[str, str], logger) -> None:
    """Optionally clean output folders/files."""
    for d in (paths["out_idx"], paths["out_non_idx"]):
        try:
            if os.path.isdir(d):
                shutil.rmtree(d)
                logger.info("Cleaned output folder: %s", d)
        except Exception as e:
            logger.warning("Failed to remove folder %s: %s", d, e)

    for f in (paths["meta_out"], paths["alerts_out"]):
        try:
            os.remove(f)
            logger.info("Removed file: %s", f)
        except FileNotFoundError:
            pass
        except Exception as e:
            logger.warning("Failed to remove file %s: %s", f, e)


def _prepare_outputs(paths: Dict[str, str]) -> None:
    """Create required folders."""
    ensure_dir(paths["out_idx"])
    ensure_dir(paths["out_non_idx"])
    ensure_parent(paths["meta_out"])
    ensure_parent(paths["alerts_out"])


def _download_with_retries(url: str, out_path: Path, retries: int, logger) -> bool:
    """
    Try minimal GET once, then up to (retries-1) seed+retry attempts.
    Returns True on success.
    """
    # Attempt 1: minimal GET
    try:
        blob = get_pdf_bytes_minimal(url, timeout=60)
        out_path.write_bytes(blob)
        logger.info("Downloaded → %s", out_path)
        return True
    except Exception as e1:
        logger.error("Minimal GET failed (%s): %s", url, e1)

    # Attempts 2..retries: seed referer then retry
    for attempt in range(2, max(2, retries) + 1):
        try:
            blob = seed_and_retry_minimal(url, timeout=60)
            out_path.write_bytes(blob)
            logger.info("[retry %d/%d] Downloaded → %s", attempt, retries, out_path)
            return True
        except Exception as e2:
            logger.error("[retry %d/%d] Failed (%s): %s", attempt, retries, url, e2)

    return False


# main 
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
) -> None:
    """
    - Classify title as IDX / NON-IDX / UNKNOWN (fuzzy).
    - IDX     → download main_link.
    - NON-IDX → download all attachments URLs.
    - UNKNOWN → no download; record alert.
    - Write two JSONs: metadata list & low-similarity alerts.
    """
    logger = get_logger("downloader", verbose)

    paths = {
        "out_idx": out_idx,
        "out_non_idx": out_non_idx,
        "meta_out": meta_out,
        "alerts_out": alerts_out,
    }

    if clean_out:
        _maybe_clean_outputs(paths, logger)

    # Setup HTTP (proxies via env; SSL warnings silenced)
    init_http(insecure=True, silence_warnings=True, load_env=True)
    _prepare_outputs(paths)

    # Log proxy presence (masking value)
    if any(os.getenv(k) for k in ("PROXY", "HTTPS_PROXY", "HTTP_PROXY", "https_proxy", "http_proxy")):
        logger.info("Proxy detected in environment.")

    records: List[dict] = []
    alerts: List[dict] = []
    now = timestamp_jakarta()

    for i, ann in enumerate(announcements, start=1):
        label, best, sim_idx, sim_non = classify_format(ann.title, threshold=min_similarity)
        logger.info("[%d] %s", i, ann.title)
        logger.info("    Classification: %s (best=%d, idx=%d, non=%d)", label, best, sim_idx, sim_non)

        ticker = _derive_ticker(ann.title, ann.company_name)

        if label == "UNKNOWN":
            ref_url = ann.main_link or (_attachment_to_url(ann.attachments[0]) if ann.attachments else None)
            alerts.append({
                "title": ann.title,
                "url": ref_url,
                "similarity_idx": sim_idx,
                "similarity_non_idx": sim_non,
                "threshold": min_similarity,
                "reason": "low_title_similarity_both",
                "severity": "warning",
                "created_at": now,
            })
            logger.info("    Skipped download (UNKNOWN). Alert recorded.")
            continue

        # Build URL list + output folder
        urls: List[str] = []
        out_folder = paths["out_non_idx"]
        if label == "IDX" and ann.main_link:
            urls = [ann.main_link]
            out_folder = paths["out_idx"]
        elif label == "NON-IDX" and ann.attachments:
            urls = [u for u in (_attachment_to_url(x) for x in ann.attachments) if u]
            out_folder = paths["out_non_idx"]

        if not urls:
            logger.warning("    No URLs found for this announcement (label=%s).", label)
            continue

        for url in urls:
            filename = safe_filename_from_url(url)
            out_path = Path(out_folder) / filename

            if dry_run:
                logger.info("    [DRY] Would download → %s -> %s", url, out_path)
                records.append({
                    "ticker": ticker,
                    "title": ann.title,
                    "url": url,
                    "filename": filename,
                    "timestamp": ann.date,
                })
                continue

            ok = _download_with_retries(url, out_path, retries=retries, logger=logger)
            if ok:
                records.append({
                    "ticker": ticker,
                    "title": ann.title,
                    "url": url,
                    "filename": filename,
                    "timestamp": ann.date,
                })

    # Write outputs
    Path(paths["meta_out"]).write_text(json.dumps(records, indent=2, ensure_ascii=False), encoding="utf-8")
    Path(paths["alerts_out"]).write_text(json.dumps(alerts, indent=2, ensure_ascii=False), encoding="utf-8")
    logger.info("Finished. %d announcements processed.", len(announcements))