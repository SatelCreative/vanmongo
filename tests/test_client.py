import pytest
from pydantic import BaseModel

from satel_mongo import BaseDocument, Client


class Context(BaseModel):
    admin: bool


class Item(BaseDocument):
    pass


@pytest.mark.asyncio
async def test_client():
    context = Context(admin=True)
    client = Client(context=context)

    items = client.use(Item)
    create_result = await items.create_one({})

    assert type(create_result) == Item

    find_result = await items.find_one_by_id(create_result.id)

    assert find_result == create_result
