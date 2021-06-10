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

from .connection import Connection, Edge, PageInfo

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


class Collection(Generic[TDocument]):
    """Collection"""

    client: Client
    Document: Type[TDocument]

    def __init__(self, client: Client, Document: Type[TDocument]):
        if Document.collection == NotImplemented:
            raise Exception("invalid Document")

        self.client = client
        self.Document = Document

    @property
    def collection(self):
        return self.client.db[self.Document.collection]

    async def find_one(self, query: Dict[str, Any]) -> Optional[TDocument]:
        raw = await self.collection.find_one(query)
        return self.Document.parse_obj(raw) if raw else None

    async def find_one_by_id(self, id: str) -> Optional[TDocument]:
        return await self.find_one({"id": id})

    async def find(
        self, query: Dict[str, Any] = {}, batch_size=100
    ) -> AsyncGenerator[TDocument, None]:
        cursor = self.collection.find(query, batch_size=batch_size)
        async for raw in cursor:
            yield self.Document.parse_obj(raw)

    async def find_by_ids(self, ids: List[str]) -> List[Optional[TDocument]]:
        """Find documents by ids"""
        documents = {}
        async for document in self.find({"$or": [{"id": i} for i in ids]}):
            documents[document.id] = document
        return [documents.get(i) for i in ids]

    async def find_connection(
        self,
        first: Optional[int] = None,
        after: Optional[str] = None,
        last: Optional[int] = None,
        before: Optional[str] = None,
        query: Optional[Dict[str, Any]] = None,
    ):
        if not first:
            raise NotImplementedError()

        connection_query = query or {}
        if after:
            raise NotImplementedError()

        nodes = await self.collection.find(connection_query).to_list(first + 1)

        has_next_page = False
        has_previous_page = False

        if len(nodes) > first:
            has_next_page = True
            nodes.pop()

        if after:
            has_previous_page = True

        Edge[TDocument].update_forward_refs()

        page_info = PageInfo(
            has_next_page=has_next_page, has_previous_page=has_previous_page
        )
        edges = [
            Edge[TDocument](node=self.Document.parse_obj(raw), cursor="")
            for raw in nodes
        ]
        return Connection[TDocument](edges=edges, page_info=page_info)

    async def create_one(self, document: Dict[str, Any]) -> TDocument:
        """Create a new document"""

        now = datetime.utcnow()
        # Keep same precision as mongo
        now = now.replace(microsecond=int(round(now.microsecond, -3)))

        document.update(
            {
                "_id": "",  # Removed before insert
                "id": ShortUUID().random(length=10),
                "created_at": now,
                "updated_at": now,
            }
        )

        doc = self.Document.parse_obj(document)

        doc_dict = doc.dict(by_alias=True)
        doc_dict.pop("_id", None)  # Remove _id

        inserted_result = await self.collection.insert_one(doc_dict)
        doc.object_id = inserted_result.inserted_id  # Add generated _id

        return doc

    async def update_one(self, query: Dict[str, Any], update: Dict[str, Any] = {}):
        original_document = await self.find_one(query)

        if not original_document:
            raise Exception("Does not exist")

        # Copy does not perform validation
        updated_dict = original_document.copy(update=update, deep=True).dict(
            by_alias=True
        )
        updated_document = self.Document.parse_obj(updated_dict)

        original_dict = original_document.dict(by_alias=True)

        # TODO signifintly improve the diffing here
        updated_values = {}
        for key, new_value in updated_dict.items():
            if new_value == original_dict.get(key):
                continue
            updated_values[key] = new_value

        if updated_values:
            now = datetime.utcnow()
            # Keep same precision as mongo
            now = now.replace(microsecond=int(round(now.microsecond, -3)))
            updated_values["updated_at"] = updated_document.updated_at = now
            await self.collection.update_one(
                {"id": original_document.id}, update={"$set": updated_values}
            )

        return updated_document

    async def update_one_by_id(self, id: str, update: Dict[str, Any] = {}):
        return await self.update_one({"id": id}, update)


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

    def __init_subclass__(cls, *args, collection: Optional[str] = None, **kwargs):

        # NOTE: known issue in mypy
        # https://github.com/python/mypy/issues/4660
        super().__init_subclass__(*args, **kwargs)  # type: ignore

        if collection:
            cls.collection = collection
        else:
            cls.collection = f"{cls.__name__.lower()}s"

        Client._register_document(cls.collection, cls)
