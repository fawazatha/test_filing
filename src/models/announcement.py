from typing import List, Optional, Union, Dict
from pydantic import BaseModel, Field


class Announcement(BaseModel):
    # Raw fields from idx_announcements.json
    date: Optional[str] = None
    title: str
    company_name: Optional[str] = None
    main_link: Optional[str] = None
    attachments: List[Union[str, Dict]] = Field(default_factory=list)

    # Optional extras we may receive
    attachment_count: Optional[int] = None
    category: Optional[str] = None
    description: Optional[str] = None
    link: Optional[str] = None
    scraped_at: Optional[str] = None
