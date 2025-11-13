from __future__ import annotations
from typing import Any, Dict, Optional, List
from datetime import datetime
from src.common.datetime import iso_utc

MESSAGE_TEMPLATES = {
    # Downloader
    "download_failed": "Failed to download the document after several attempts.",
    "unsupported_format": "Attachment is not a valid PDF or the format is unsupported.",
    "content_mismatch": "Attachment content does not match the expected announcement.",
    "low_title_similarity": "Announcement title and document name have low similarity.",
}

def build_alert(
    *,
    category: str,                   # "inserted" | "not_inserted"
    stage: str,                      # "downloader" | "parser" | "filings"
    code: str,
    doc_filename: Optional[str] = None,
    context_doc_url: Optional[str] = None,
    context_doc_title: Optional[str] = None,
    announcement: Optional[Dict[str, Any]] = None,   
    message: Optional[str] = None,
    reasons: Optional[List[Dict[str, Any]]] = None,
    ctx: Optional[Dict[str, Any]] = None,
    needs_review: bool = True,
    severity: str = "warning",
    ts: Optional[str] = None,        # iso utc
) -> Dict[str, Any]:
    msg = message or MESSAGE_TEMPLATES.get(code) or code
    alert = {
        "timestamp": ts or iso_utc(),
        "category": category,
        "stage": stage,
        "code": code,
        "message": msg,
        "reasons": reasons or [],
        "context": {
            "doc": {
                "filename": doc_filename,
                "url": context_doc_url,
                "title": context_doc_title,
            },
            "announcement": announcement,
        },
        "severity": severity,
        "needs_review": bool(needs_review),
    }
    # If reasons empty, synthesize from code
    if not alert["reasons"]:
        alert["reasons"] = [{
            "scope": "system",
            "code": code,
            "message": msg,
            "details": ctx or {},
        }]
    elif ctx:
        # append a system reason for extra details
        alert["reasons"].append({
            "scope": "system",
            "code": code,
            "message": msg,
            "details": ctx,
        })
    return alert
