from datetime import datetime
from typing import Any, ClassVar

from pydantic import BaseModel, Field


class BaseDocument(BaseModel):
    """BaseDocument"""

    """MongoDB Collection"""
    collection: ClassVar[str] = NotImplemented
    """Autogenerated _id"""
    object_id: Any = Field(alias="_id")
    """Short unique id"""
    id: str
    """Date last modified"""
    updated_at: datetime
    """Date created"""
    created_at: datetime
