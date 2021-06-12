from __future__ import annotations

from datetime import datetime
from typing import (
    Any,
    AsyncGenerator,
    ClassVar,
    Dict,
    Generic,
    List,
    Optional,
    Type,
    TypeVar,
    overload,
)

from motor.motor_asyncio import AsyncIOMotorClient
from pydantic import BaseModel, Field

# from pymongo import ASCENDING, DESCENDING
from shortuuid import ShortUUID

from .collection import Collection
from .document import BaseDocument as InternalBaseDocument

TContext = TypeVar("TContext", bound="BaseModel")
TDocument = TypeVar("TDocument", bound="BaseDocument")
TCollection = TypeVar("TCollection", bound="BaseCollection")


class Config(BaseModel):
    mongo_url: str
    mongo_database: str
    meilsearch_url: Optional[str] = None
    meilsearch_key: Optional[str] = None


class Client(Generic[TContext]):
    """Client"""

    __client: ClassVar[Any] = NotImplemented
    __documents: ClassVar[Dict[str, Type[BaseDocument]]] = {}
    config: ClassVar[Config] = NotImplemented
    context: Optional[TContext] = None

    def __init__(self, context: TContext = None):
        if self.__client == NotImplemented:
            raise Exception("Client cannot be used before it has been initialized")

        self.context = context

    @classmethod
    async def initialize(cls, mongo_url: str = None, mongo_database: str = None):
        cls.config = Config(
            mongo_url=mongo_url,
            mongo_database=mongo_database,
        )
        cls.__client = AsyncIOMotorClient(cls.config.mongo_url)

        db = cls.__client[mongo_database]

        # Setup indexes

        # TODO
        for key, doc in cls.__documents.items():
            collection = db[key]

            await collection.create_index("id", name="id")

    @classmethod
    async def shutdown(cls):
        cls.__client = NotImplemented
        cls.__documents = {}
        cls.config = NotImplemented

    @classmethod
    def _register_document(cls, key: str, Document: Type[TDocument]):
        if key in cls.__documents:
            raise Exception(f'Document with collection "{key}" already exists')
        cls.__documents[key] = Document

    @property
    def db(self):
        return self.__client[self.config.mongo_database]

    @overload
    def use(
        self: "Client[TContext]", DocumentorCollection: Type[TDocument]
    ) -> "Collection[TDocument]":
        ...

    @overload
    def use(
        self: "Client[TContext]", DocumentorCollection: Type[TCollection]
    ) -> TCollection:
        ...

    def use(self: "Client[TContext]", DocumentorCollection):
        if issubclass(DocumentorCollection, BaseDocument):
            return Collection[TDocument](client=self, Document=DocumentorCollection)
        if issubclass(DocumentorCollection, BaseCollection):
            return DocumentorCollection(client=self)
        raise Exception("use must be called with Document or Collection")


class BaseCollection(Generic[TDocument], Collection[TDocument]):
    __document: Type[TDocument] = NotImplemented

    def __init__(self, client: Client):
        super().__init__(client=client, Document=self.__document)

    def __init_subclass__(cls, *args, document: Type[TDocument] = None, **kwargs):

        # NOTE: known issue in mypy
        # https://github.com/python/mypy/issues/4660
        super().__init_subclass__(*args, **kwargs)  # type: ignore

        if not document:
            raise Exception("BaseCollection must be extended with document=")

        cls.__document = document


class BaseDocument(InternalBaseDocument):
    """BaseDocument"""

    def __init_subclass__(cls, *args, collection: Optional[str] = None, **kwargs):

        # NOTE: known issue in mypy
        # https://github.com/python/mypy/issues/4660
        super().__init_subclass__(*args, **kwargs)  # type: ignore

        if collection:
            cls.collection = collection
        else:
            cls.collection = f"{cls.__name__.lower()}s"

        Client._register_document(cls.collection, cls)
