from typing import Any, List

import pytest
from pydantic import BaseModel

from satel_mongo import BaseDocument, Client, EventType


class Context(BaseModel):
    admin: bool


class Item(BaseDocument):
    index: int


called_with: List[Any] = []

# mypy only validates decorators
# at the root of the file


@Item.on_change
async def change_handler(type: EventType, item: Item, context: Context = None):
    called_with.append(type)
    called_with.append(item)
    called_with.append(context)


@pytest.mark.asyncio
async def test_simple_event(test_config):
    global called_with

    await Client.initialize(
        mongo_url=test_config.mongo_url, mongo_database=test_config.mongo_database
    )

    items = Client(context=Context(admin=False)).use(Item)

    assert called_with == []
    item = await items.create_one({"index": 1})
    assert called_with == [EventType.CREATE, item, Context(admin=False)]

    called_with = []

    item = await items.update_one_by_id(item.id, {"index": 2})
    assert called_with == [EventType.UPDATE, item, Context(admin=False)]
