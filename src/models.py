from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field, HttpUrl


class BulletinLink(BaseModel):
    title: str = Field(min_length=3)
    url: HttpUrl
    source_page: HttpUrl


class ParsedTradeRecord(BaseModel):
    source_file: Path
    file_type: Literal["pdf", "xls", "xlsx"]
    parsed_at: datetime
    record_count: int = Field(ge=0)
    sample_text: str = Field(default="")
