from __future__ import annotations

from typing import Generic, List, TypeVar

from pydantic import BaseModel
from pydantic.generics import GenericModel

Node = TypeVar("Node")


class Edge(GenericModel, Generic[Node]):
    node: Node
    cursor: str


class PageInfo(BaseModel):
    has_next_page: bool
    has_previous_page: bool


class Connection(GenericModel, Generic[Node]):
    edges: List[Edge[Node]]
    page_info: PageInfo
