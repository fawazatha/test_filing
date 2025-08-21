from pydantic import BaseModel, Field
from typing import Optional, Any, List

class Announcement(BaseModel):
    """
    Tolerant model for incoming announcements.

    Fields expected from the IDX announcements JSON:
      - title: str
      - date: str (ISO string)
      - main_link: optional URL string for IDX-format PDFs
      - attachments: can be a list of strings OR list of objects (any shape).
        We normalize them to URLs in the runner.
    Extra fields (e.g., company_name, category, etc.) are ignored by default.
    """
    title: str
    date: str
    main_link: Optional[str] = None
    attachments: List[Any] = Field(default_factory=list)
