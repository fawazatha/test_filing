# src/generate/filings/utils/types.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Optional

@dataclass
class DownloadMeta:
    """Dataclass for download metadata."""
    filename: str
    url: Optional[str]
    timestamp: Optional[str] = None  
    title: Optional[str] = None     

@dataclass
class CompanyInfo:
    """Dataclass for company provider info."""
    company_name: str = ""
    sector: Optional[str] = None
    sub_sector: Optional[str] = None