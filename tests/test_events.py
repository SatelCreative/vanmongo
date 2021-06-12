import pytest
from pydantic import BaseModel

from satel_mongo import BaseDocument, Client, EventType


class Context(BaseModel):
    admin: bool


@pytest.mark.asyncio
async def test_simple_event(test_config):
    class Item(BaseDocument):
        index: int

    called_with = []

    async def change_handler(type, item, context=None):
        called_with.append(type)
        called_with.append(item)
        called_with.append(context)

    Item.on_change(change_handler)

    await Client.initialize(
        mongo_url=test_config.mongo_url, mongo_database=test_config.mongo_database
    )

    items = Client(context=Context(admin=False)).use(Item)

    assert called_with == []
    item = await items.create_one({"index": 1})
    assert called_with[0] == EventType.CREATE
    assert called_with[1] == item
    assert called_with[2] == Context(admin=False)
