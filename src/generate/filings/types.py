from __future__ import annotations
from dataclasses import dataclass
from typing import Optional

@dataclass
class DownloadMeta:
    filename: str
    url: Optional[str]
    timestamp: Optional[str] = None  
    title: Optional[str] = None     

@dataclass
class CompanyInfo:
    company_name: str = ""
    sector: Optional[str] = None
    sub_sector: Optional[str] = None
