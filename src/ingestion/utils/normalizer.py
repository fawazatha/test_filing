from __future__ import annotations
from typing import Dict, List, Optional

from src.common.text import kebab
from src.common.files import safe_filename_from_url

"""Shape raw IDX items into a stable schema consumed by the pipeline."""

def normalize_item(item: Dict) -> Optional[Dict]:
    """Return normalized announcement or None when unusable (e.g., no attachments)."""
    attachments = item.get("Attachments") or []
    if not isinstance(attachments, list) or not attachments:
        return None

    main = attachments[0]
    main_link = main.get("FullSavePath")
    if not isinstance(main_link, str) or not main_link.lower().startswith("http"):
        return None

    # Keep remaining attachments (1..n)
    extra: List[Dict] = []
    for att in attachments[1:]:
        url = att.get("FullSavePath")
        if isinstance(url, str) and url.lower().startswith("http"):
            extra.append({"filename": att.get("OriginalFilename"), "url": url})

    title = item.get("Title") or ""
    code = (item.get("Code") or "").strip()

    return {
        "date": item.get("PublishDate"),
        "title": title,
        "title_slug": kebab(title),
        "company_name": code or None,
        "main_link": main_link,
        "filename": safe_filename_from_url(main_link),
        "attachments": extra,
        "attachment_count": len(extra),
        "category": "Ownership Report",
        "description": (
            f"Ownership report for {code}" if (title and code)
            else "Ownership Report or Any Changes in Ownership of Public Company Shares"
        ),
        "link": main_link,
        "scraped_at": item.get("_scraped_at"),
    }