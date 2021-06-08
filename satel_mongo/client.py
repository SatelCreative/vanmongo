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
)

from motor.motor_asyncio import AsyncIOMotorClient
from pydantic import BaseModel, Field
from pymongo import ASCENDING, DESCENDING
from shortuuid import ShortUUID

TContext = TypeVar("TContext", bound="BaseModel")
TDocument = TypeVar("TDocument", bound="BaseDocument")


class Config(BaseModel):
    mongo_url: str
    mongo_database: str
    meilsearch_url: Optional[str] = None
    meilsearch_key: Optional[str] = None


class Client(Generic[TContext]):
    """Client"""

    __client: ClassVar[Any] = NotImplemented
    __documents: ClassVar[Dict[str, Type[TDocument]]] = {}
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
    def _register(cls, key: str, Document: Type[TDocument]):
        if key in cls.__documents:
            raise Exception(f'Document with collection "{key}" already exists')
        cls.__documents[key] = Document

    @property
    def db(self):
        return self.__client[self.config.mongo_database]

    def use(
        self: "Client", Document: Type[TDocument]
    ) -> "Collection[TDocument, TContext]":
        return Collection[TDocument, TContext](client=self, Document=Document)


class Collection(Generic[TDocument, TContext]):
    """Collection"""

    client: Client[TContext]
    Document: Type[TDocument]

    def __init__(self, client: Client[TContext], Document: Type[TDocument]):
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
        raw = await self.collection.find_one({"id": id})
        return self.Document.parse_obj(raw) if raw else None

    async def find_by_ids(self, ids: List[str]) -> List[Optional[TDocument]]:
        """Find documents by ids"""
        cursor = self.collection.find({"$or": [{"id": i} for i in ids]})
        nodes = {}
        async for raw in cursor:
            node = self.Document.parse_obj(raw)
            nodes[node.id] = node
        return [nodes.get(i) for i in ids]

    async def find(
        self, query: Dict[str, Any] = {}, batch_size=100
    ) -> AsyncGenerator[TDocument, None]:
        cursor = self.collection.find(query, batch_size=batch_size)
        async for raw in cursor:
            yield self.Document.parse_obj(raw)

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
