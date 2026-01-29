# PYDANTIC MODELS

from pydantic import BaseModel
from datetime import datetime
from typing import Any, List, Optional

# Job table
class JobOut(BaseModel):
    event_id: str
    ingested_at: datetime
    company: Optional[str] = None
    title: Optional[str] = None
    location: Optional[str] = None
    seniority: Optional[str] = None
    role_category: Optional[str] = None
    description: Optional[str] = None
    url: Optional[str] = None
    techs: List[Any] = []