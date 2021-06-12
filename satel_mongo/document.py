from datetime import datetime
from inspect import isawaitable
from typing import Any, ClassVar, List, Type, TypeVar, cast

from pydantic import BaseModel, Field

from .events import ChangeHandler, EventType, RegisteredChangeEvent, RegisteredEvent

TDocument = TypeVar("TDocument", bound="BaseDocument")


class BaseDocument(BaseModel):
    """BaseDocument"""

    """Registered events"""
    __events: ClassVar[List[RegisteredEvent]] = []
    """MongoDB Collection"""
    _collection: ClassVar[str] = NotImplemented
    """MongoDB Sort Options"""
    _sort_options: ClassVar[str] = NotImplemented
    """Autogenerated _id"""
    object_id: Any = Field(alias="_id")
    """Short unique id"""
    id: str
    """Date last modified"""
    updated_at: datetime
    """Date created"""
    created_at: datetime

    @classmethod
    def on_change(cls: Type[TDocument], handler: ChangeHandler[TDocument]):
        cls.__events.append(
            RegisteredChangeEvent(type=EventType.CHANGE, handler=handler)
        )

    @classmethod
    async def _trigger_create(cls: Type[TDocument], value: TDocument, context=None):
        for registered_handler in cls.__events:
            result = None
            if registered_handler.type == EventType.CHANGE:
                handler = cast(ChangeHandler, registered_handler.handler)
                result = handler(EventType.CREATE, value, context=context)
            if result and isawaitable(result):
                await result
