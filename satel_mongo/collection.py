from __future__ import annotations

from datetime import datetime
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncGenerator,
    Dict,
    Generic,
    List,
    Optional,
    Type,
    TypeVar,
)
from bson.objectid import ObjectId

from motor.motor_asyncio import AsyncIOMotorClient
from pydantic import BaseModel, Field

from pymongo import ASCENDING, DESCENDING
from shortuuid import ShortUUID

from .connection import Connection, Edge, MongoCursor, PageInfo
from .document import BaseDocument

if TYPE_CHECKING:
    from satel_mongo import Client

TContext = TypeVar("TContext", bound="BaseModel")
TDocument = TypeVar("TDocument", bound="BaseDocument")


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

        connection_query = {}
        if after:
            cursor = MongoCursor.base64_decode(after)
            object_id = ObjectId(cursor.id)
            connection_query = {'_id': {'$gt': object_id}}

        if query:
            connection_query = {
                '$and': [
                    connection_query,
                    query,
                ]
            }

        nodes = await self.collection.find(connection_query).sort([('_id', ASCENDING)]).to_list(first + 1)

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
        edges: List[Edge[TDocument]] = []
        for raw in nodes:
            node = self.Document.parse_obj(raw)
            cursor = MongoCursor(id=f'{node.object_id}').base64_encode()
            edges.append(Edge[TDocument](node=node, cursor=cursor))

        return Connection[TDocument](edges=edges, page_info=page_info)

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
            now = now.replace(microsecond=int(round(now.microsecond, -3) % 1000000))
            updated_values["updated_at"] = updated_document.updated_at = now
            await self.collection.update_one(
                {"id": original_document.id}, update={"$set": updated_values}
            )

        return updated_document

    async def update_one_by_id(self, id: str, update: Dict[str, Any] = {}):
        return await self.update_one({"id": id}, update)
