from typing import Dict, List, Optional

def normalize_item(item: Dict) -> Optional[Dict]:
    """
    Map a raw IDX API item into the schema expected by the downloader.
    Returns None if the item is not usable (e.g., no attachments).
    """
    attachments = item.get("Attachments") or []
    if not isinstance(attachments, list) or not attachments:
        return None

    main = attachments[0]
    main_link = main.get("FullSavePath")

    # Keep remaining attachments (1..n) as 'attachments'
    tail = attachments[1:]
    filtered_attachments: List[Dict] = []
    for att in tail:
        url = att.get("FullSavePath")
        if isinstance(url, str) and url.lower().startswith("http"):
            filtered_attachments.append({
                "filename": att.get("OriginalFilename"),
                "url": url,
            })

    title = item.get("Title")
    code = (item.get("Code") or "").strip()
    description = (
        f"Ownership report for {code}"
        if (title and code) else
        "Ownership Report or Any Changes in Ownership of Public Company Shares"
    )

    return {
        # Downloader reads these and ignores extras gracefully
        "date": item.get("PublishDate"),
        "title": title,
        "company_name": code,
        "main_link": main_link,
        "attachments": filtered_attachments,
        "attachment_count": len(filtered_attachments),
        "category": "Ownership Report",
        "description": description,
        "link": main_link,
        "scraped_at": item.get("_scraped_at"),  # filled in runner
    }
