from __future__ import annotations

from base64 import b64decode, b64encode
from typing import Generic, List, Optional, Type, TypeVar

from pydantic import BaseModel
from pydantic.generics import GenericModel

Node = TypeVar("Node")
Model = TypeVar("Model", bound=BaseModel)


def base64_encode_model(model: Model) -> str:
    return b64encode(model.json(exclude_none=True).encode()).decode()


def base64_decode_model(Model: Type[Model], value: str) -> Model:
    return Model.parse_raw(b64decode(value.encode()))


class MongoCursor(BaseModel):
    id: str
    sort: Optional[str] = None
    value: Optional[str] = None

    def base64_encode(self):
        return base64_encode_model(self)

    @classmethod
    def base64_decode(cls, value: str):
        return base64_decode_model(cls, value)


class Edge(GenericModel, Generic[Node]):
    node: Node
    cursor: str


class PageInfo(BaseModel):
    has_next_page: bool
    has_previous_page: bool


class Connection(GenericModel, Generic[Node]):
    edges: List[Edge[Node]]
    page_info: PageInfo
