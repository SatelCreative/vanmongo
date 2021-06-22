from enum import Enum
from typing import TYPE_CHECKING, Any, Literal, Protocol, TypeVar, Union

from pydantic import BaseModel

if TYPE_CHECKING:
    from .document import BaseDocument


class EventType(Enum):
    CHANGE = "change"
    CREATE = "create"
    UPDATE = "update"
    DELETE = "delete"


TDocument = TypeVar("TDocument", bound="BaseDocument", contravariant=True)


class ChangeHandler(Protocol[TDocument]):
    async def __call__(self, type: EventType, document: TDocument, context: Any = None):
        ...


class RegisteredChangeEvent(BaseModel):
    type: Literal[EventType.CHANGE]
    handler: Any


RegisteredEvent = Union[RegisteredChangeEvent]
