"""A library to handle MongoDB Operations"""

__version__ = '0.1.0'

from .connection import Connection, Edge, PageInfo
from .events import EventType
from .main import BaseCollection, BaseDocument, Client

__all__ = [
    "Client",
    "BaseCollection",
    "BaseDocument",
    "Connection",
    "Edge",
    "PageInfo",
    "EventType",
]
