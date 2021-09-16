from __future__ import annotations

from datetime import datetime
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncGenerator,
    Coroutine,
    Dict,
    Generic,
    List,
    Optional,
    Type,
    TypeVar,
    cast,
)

from bson.objectid import ObjectId
from pydantic import BaseModel
from pymongo import ASCENDING, DESCENDING
from shortuuid import ShortUUID

from .connection import Connection, Edge, MeilCursor, MongoCursor, PageInfo
from .document import BaseDocument

if TYPE_CHECKING:
    from vanmongo import Client

TContext = TypeVar("TContext", bound="BaseModel")
TDocument = TypeVar("TDocument", bound="BaseDocument")


class Collection(Generic[TDocument]):
    """Collection"""

    client: Client
    Document: Type[TDocument]

    def __init__(self, client: Client, Document: Type[TDocument]):
        if Document._collection == NotImplemented:
            raise Exception("invalid Document")
        if Document._sort_options == NotImplemented:
            raise Exception("invalid Document")

        self.client = client
        self.Document = Document

    @property
    def collection(self):
        return self.client.db[self.Document._collection]

    @property
    def index(self):
        return self.client.search.index(self.Document._collection)

    @property
    def loader(self):
        return self.client.loaders[self.Document._collection]

    def load_one(self, id: str) -> Coroutine[Any, Any, Optional[TDocument]]:
        return cast(Coroutine[Any, Any, Optional[TDocument]], self.loader.load(id))

    def load(self, ids: List[str]) -> Coroutine[Any, Any, List[Optional[TDocument]]]:
        return cast(
            Coroutine[Any, Any, List[Optional[TDocument]]],
            self.loader.load_many(ids),
        )

    async def find_one(self, query: Dict[str, Any]) -> Optional[TDocument]:
        """
        Find a document base on the query
        Works similar to db.collection.findOne() in MongoDB
        """
        raw = await self.collection.find_one(query)
        return self.Document.parse_obj(raw) if raw else None

    async def find_one_by_id(self, id: str) -> Optional[TDocument]:
        """
        Find a document by ID
        """
        return await self.find_one({"id": id})

    async def find(
        self,
        query: Dict[str, Any] = {},  # TODO rename (gets confusing with search)
        limit: Optional[int] = None,
        sort: Optional[str] = None,
        reverse: bool = False,
        batch_size=100,
    ) -> AsyncGenerator[TDocument, None]:
        """
        Find documents in the collection.
        If no argument is given, it will act similar as
        "db.collection.find({})" in Mongodb.
        """
        cursor = self.collection.find(query, batch_size=batch_size)

        direction = DESCENDING if reverse else ASCENDING
        mongo_sort = [("_id", direction)]
        if sort:
            mongo_sort = [(sort, direction)] + mongo_sort
        cursor.sort(mongo_sort)

        if limit:
            cursor.limit(limit)

        async for raw in cursor:
            yield self.Document.parse_obj(raw)

    async def find_by_ids(self, ids: List[str]) -> List[Optional[TDocument]]:
        """Find documents by a list of IDs"""
        documents = {}
        async for document in self.find({"$or": [{"id": i} for i in ids]}):
            documents[document.id] = document
        return [documents.get(i) for i in ids]

    async def __mongo_find_connection(
        self,
        first: Optional[int] = None,
        after: Optional[str] = None,
        last: Optional[int] = None,
        before: Optional[str] = None,
        sort: Optional[str] = None,
        reverse: bool = False,
    ):
        page_size = first or last
        if not page_size:
            raise Exception("Must provide one of first or last")
        raw_cursor = before if last else after
        if last and not raw_cursor:
            raise Exception("Must provide both last and before")

        if not first:
            reverse = not reverse

        connection_query: Dict[str, Any] = {}
        if raw_cursor:
            cursor = MongoCursor.base64_decode(raw_cursor)
            object_id = ObjectId(cursor.id)
            op = "$lt" if reverse else "$gt"
            connection_query = {"_id": {op: object_id}}

            if cursor.sort and cursor.value and cursor.sort == sort:
                connection_query = {
                    "$or": [
                        {cursor.sort: {op: cursor.value}},
                        # Need secondary comparison for correct pagination
                        # when primary comparison has duplicate values
                        {
                            cursor.sort: cursor.value,
                            "_id": connection_query["_id"],
                        },
                    ]
                }

        cursor = self.find(
            query=connection_query,
            sort=sort,
            reverse=reverse,
            limit=page_size + 1,
        )
        nodes = [node async for node in cursor]

        has_next_page = False
        has_previous_page = False

        extra_node = len(nodes) > page_size
        if extra_node:
            nodes.pop()

        if first:
            has_next_page = extra_node
            has_previous_page = bool(after)
        if before:
            nodes.reverse()
            has_next_page = True
            has_previous_page = extra_node

        Edge[TDocument].update_forward_refs()

        page_info = PageInfo(
            has_next_page=has_next_page, has_previous_page=has_previous_page
        )
        edges: List[Edge[TDocument]] = []
        for node in nodes:
            cursor = MongoCursor(
                id=f"{node.object_id}",
                sort=sort,
                value=getattr(node, sort, None) if sort else None,
            ).base64_encode()
            edges.append(Edge[TDocument](node=node, cursor=cursor))

        return Connection[TDocument](edges=edges, page_info=page_info)

    async def __meil_find_connection(
        self,
        query: Optional[str] = None,
        first: Optional[int] = None,
        after: Optional[str] = None,
        last: Optional[int] = None,
        before: Optional[str] = None,
    ):
        page_size = first or last
        if not page_size:
            raise Exception("Must provide one of first or last")
        raw_cursor = before if last else after
        if last and not raw_cursor:
            raise Exception("Must provide both last and before")

        offset = 0
        limit = page_size
        if raw_cursor:
            cursor = MeilCursor.base64_decode(raw_cursor)

            if cursor.query != query:
                raise Exception("Invalid cursor")

            if after:
                offset = cursor.offset + 1
            if before:
                offset = max(cursor.offset - last, 0)
                limit = min(limit, cursor.offset)

        index = self.index
        result = await index.search(
            query,
            attributes_to_retrieve=["id"],
            limit=page_size,
            offset=offset,
        )

        nodes = await self.load([cast(str, hit["id"]) for hit in result.hits])

        Edge[TDocument].update_forward_refs()

        page_info = PageInfo(
            has_next_page=offset + limit < result.nb_hits,
            has_previous_page=offset != 0,
        )
        edges: List[Edge[TDocument]] = []
        for i, node in enumerate(nodes):
            cursor = MeilCursor(offset=offset + i, query=query).base64_encode()
            edges.append(Edge[TDocument](node=node, cursor=cursor))
        return Connection[TDocument](edges=edges, page_info=page_info)

    async def find_connection(
        self,
        query: Optional[str] = None,
        first: Optional[int] = None,
        after: Optional[str] = None,
        last: Optional[int] = None,
        before: Optional[str] = None,
        sort: Optional[str] = None,
        reverse: bool = False,
    ):
        if query:
            return await self.__meil_find_connection(
                query=query,
                first=first,
                after=after,
                last=last,
                before=before,
            )
        return await self.__mongo_find_connection(
            first=first,
            after=after,
            last=last,
            before=before,
            sort=sort,
            reverse=reverse,
        )

    async def create_one(self, document: Dict[str, Any]) -> TDocument:
        """Create a new document"""

        now = datetime.utcnow()
        # Keep same precision as mongo
        now = now.replace(microsecond=int(round(now.microsecond, -3) % 1000000))

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

        await self.Document._trigger_create(doc, context=self.client.context)

        return doc

    async def update_one(self, query: Dict[str, Any], update: Dict[str, Any] = {}):
        """
        Update a document based on the query
        Works similar to db.collection.updateOne() in MongoDB
        """
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
            now = now.replace(microsecond=int(round(now.microsecond, -3) % 1000000))
            updated_values["updated_at"] = updated_document.updated_at = now
            await self.collection.update_one(
                {"id": original_document.id}, update={"$set": updated_values}
            )

            await self.Document._trigger_update(
                updated_document, context=self.client.context
            )

        return updated_document

    async def update_one_by_id(self, id: str, update: Dict[str, Any] = {}):
        """Update a document with specific ID"""
        return await self.update_one({"id": id}, update)

    async def aggregate(
        self, aggregation_pipeline: List[Dict[str, Any]]
    ) -> AsyncGenerator[Any, None]:
        """Perform aggregation on collection"""
        cursor = self.collection.aggregate(aggregation_pipeline)

        async for raw in cursor:
            yield raw
