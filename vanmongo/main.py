from __future__ import annotations

from asyncio import gather
from typing import (
    Any,
    ClassVar,
    Dict,
    Generic,
    List,
    Optional,
    Type,
    TypeVar,
    overload,
)

from aiodataloader import DataLoader
from aiostream import stream
from async_search_client import Client as SearchClient
from motor.motor_asyncio import AsyncIOMotorClient
from pydantic import BaseModel

from .collection import Collection
from .document import BaseDocument as InternalBaseDocument

TContext = TypeVar("TContext", bound="BaseModel")
TDocument = TypeVar("TDocument", bound="BaseDocument")
TCollection = TypeVar("TCollection", bound="BaseCollection")


class Config(BaseModel):
    mongo_url: str
    mongo_database: str
    meilisearch_url: Optional[str] = None
    meilisearch_key: Optional[str] = None


def create_find_by_ids(db, doc):
    async def find_by_ids(ids):
        documents = {}
        async for raw in db[doc._collection].find(
            {"$or": [{"id": i} for i in ids]}
        ):
            document = doc.parse_obj(raw)
            documents[document.id] = document
        return [documents.get(i) for i in ids]

    return find_by_ids


async def default_make(instance: TDocument):
    if not instance._search_fields:
        raise AssertionError()
    return instance.dict(include=set(["id"] + instance._search_fields))


class Client(Generic[TContext]):
    """The VanMongo Client class"""

    __client: ClassVar[Any] = NotImplemented
    __search: ClassVar[Any] = NotImplemented
    __documents: ClassVar[Dict[str, Type[BaseDocument]]] = {}
    __loaders: Any = NotImplemented
    config: ClassVar[Config] = NotImplemented
    context: Optional[TContext] = None

    def __init__(self, context: TContext = None):
        """Creates a VanMongo client instance"""
        if self.__client == NotImplemented:
            raise Exception(
                "Client cannot be used before it has been initialized"
            )

        self.context = context

        self.__loaders = {}
        for key, doc in self.__documents.items():
            self.__loaders[key] = DataLoader(create_find_by_ids(self.db, doc))

    @classmethod
    async def __mongo_setup_indexes(cls):
        db = cls.__client[cls.config.mongo_database]

        # Setup indexes

        # TODO only create indexes that don't already exist
        for key, doc in cls.__documents.items():
            collection = db[key]

            await collection.create_index("id", name="id")

            for sort_key in doc._sort_options:
                await collection.create_index(
                    [(sort_key, 1), ("_id", 1)], name=f"sort_{sort_key}"
                )

    @classmethod
    async def __search_setup_indexes(cls):
        search = cls.__search

        for key, doc in cls.__documents.items():
            if not doc._search_fields:
                continue

            index = await search.get_or_create_index(key)
            items = cls().use(doc)

            async with stream.chunks(items.find(), 50).stream() as chunks:
                async for chunk in chunks:
                    items_futures = []

                    # TODO support sync or async

                    for n in chunk:
                        items_futures.append(default_make(n))

                    items = await gather(*items_futures)

                    await index.update_documents(items)

            # TODO make this not terrible

            # Set up listener
            async def test(type, item, context=None):
                await index.update_documents([await default_make(item)])

            doc.on_change(test)

    @classmethod
    async def initialize(
        cls,
        mongo_url: str = None,
        mongo_database: str = None,
        meilisearch_url: Optional[str] = None,
        meilisearch_key: Optional[str] = None,
    ):
        """
        Initialize client setting for Vanmongo

        mongo_url: The connection string URI of MongoDB.
            Eg. "mongodb://localhost:27017"
        mongo_database: The name of the database. Eg. "mydb"
        meilisearch_url: The URL of MeiliSearch’s address
        meilisearch_key: The key for access permission for the MeiliSearch API
        """
        cls.config = Config(
            mongo_url=mongo_url,
            mongo_database=mongo_database,
            meilisearch_url=meilisearch_url,
            meilisearch_key=meilisearch_key,
        )
        cls.__client = AsyncIOMotorClient(cls.config.mongo_url)

        # Setup mongo indexes
        await cls.__mongo_setup_indexes()

        # Setup search
        if cls.config.meilisearch_url:
            cls.__search = SearchClient(
                cls.config.meilisearch_url, cls.config.meilisearch_key
            )

            await cls.__search_setup_indexes()

    @classmethod
    async def shutdown(cls):
        if cls.__search != NotImplemented:
            await cls.__search.aclose()

        cls.__client = NotImplemented
        cls.__search = NotImplemented
        cls.__documents = {}
        cls.__loaders = NotImplemented
        cls.config = NotImplemented

    @classmethod
    def _register_document(cls, Document: Type[TDocument]):
        key = Document._collection
        if key in cls.__documents:
            raise Exception(f'Document with collection "{key}" already exists')
        cls.__documents[key] = Document

    @property
    def db(self):
        return self.__client[self.config.mongo_database]

    @property
    def search(self):
        if not self.__search:
            raise Exception("Search has not been initialized")
        return self.__search

    @property
    def loaders(self):
        return self.__loaders

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
        """
        Access the documents in a collection
        """
        if issubclass(DocumentorCollection, BaseDocument):
            return Collection[TDocument](
                client=self, Document=DocumentorCollection
            )
        if issubclass(DocumentorCollection, BaseCollection):
            return DocumentorCollection(client=self)
        raise Exception("use must be called with Document or Collection")


class BaseCollection(Generic[TDocument], Collection[TDocument]):
    __document: Type[TDocument] = NotImplemented

    def __init__(self, client: Client):
        super().__init__(client=client, Document=self.__document)

    def __init_subclass__(
        cls, *args, document: Type[TDocument] = None, **kwargs
    ):

        # NOTE: known issue in mypy
        # https://github.com/python/mypy/issues/4660
        super().__init_subclass__(*args, **kwargs)  # type: ignore

        if not document:
            raise Exception("BaseCollection must be extended with document=")

        cls.__document = document


DEFAULT_SORT_OPTIONS: List[str] = ["updated_at", "created_at"]


class BaseDocument(InternalBaseDocument):
    """BaseDocument use to declare documents"""

    def __init_subclass__(
        cls,
        *args,
        collection: Optional[str] = None,
        sort_options: Optional[List[str]] = None,
        search: Optional[List[str]] = None,
        **kwargs,
    ):
        # NOTE: known issue in mypy
        # https://github.com/python/mypy/issues/4660
        super().__init_subclass__(*args, **kwargs)  # type: ignore
        cls.__events = []

        if collection:
            cls._collection = collection
        else:
            cls._collection = f"{cls.__name__.lower()}s"

        if sort_options:
            cls._sort_options = sort_options + DEFAULT_SORT_OPTIONS
        else:
            cls._sort_options = DEFAULT_SORT_OPTIONS.copy()

        cls._search_fields = search

        Client._register_document(cls)
