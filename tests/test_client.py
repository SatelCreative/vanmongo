from typing import Optional

import pytest
from pydantic import BaseModel

from vanmongo import BaseDocument, Client


class Context(BaseModel):
    admin: bool


@pytest.mark.asyncio
async def test_client(test_config):
    class Item(BaseDocument):
        pass

    await Client.initialize(
        mongo_url=test_config.mongo_url, mongo_database=test_config.mongo_database
    )

    context = Context(admin=True)
    client = Client(context=context)

    items = client.use(Item)
    create_result = await items.create_one({})

    assert type(create_result) == Item

    find_result = await items.find_one_by_id(create_result.id)

    assert find_result == create_result


@pytest.mark.asyncio
async def test_find(test_config):
    class Item(BaseDocument):
        index: int

    await Client.initialize(
        mongo_url=test_config.mongo_url, mongo_database=test_config.mongo_database
    )

    items = Client().use(Item)

    assert await items.find_one({}) is None
    assert await items.find_one_by_id("fakeid") is None
    assert await items.find_by_ids(["fakeid"]) == [None]
    assert [item async for item in items.find()] == []

    created = []
    for index in range(10):
        item = await items.create_one({"index": index})
        created.append(item)

    assert await items.find_one({"index": 4}) == created[4]
    assert await items.find_one({"index": 4}) == created[4]
    assert await items.find_one_by_id(created[0].id) == created[0]
    assert await items.find_one_by_id(created[9].id) == created[9]

    result = await items.find_by_ids(
        [created[9].id, "fakeid", created[3].id, created[5].id]
    )
    assert result == [created[9], None, created[3], created[5]]

    assert [item async for item in items.find()] == created
    assert [item async for item in items.find({"index": 4})] == [created[4]]


@pytest.mark.asyncio
async def test_update(test_config):
    class Item(BaseDocument):
        index: int
        description: Optional[str]

    await Client.initialize(
        mongo_url=test_config.mongo_url, mongo_database=test_config.mongo_database
    )

    items = Client().use(Item)

    created = []
    for index in range(10):
        item = await items.create_one({"index": index})
        created.append(item)

    updated = await items.update_one(
        {"index": 4}, {"description": "Hello there how are you?"}
    )
    assert updated.updated_at >= created[4].updated_at
    assert updated.description == "Hello there how are you?"
    assert await items.find_one_by_id(updated.id) == updated


@pytest.mark.asyncio
async def test_update_multiple(test_config):
    class Item(BaseDocument):
        index: int
        description: Optional[str]

    await Client.initialize(
        mongo_url=test_config.mongo_url, mongo_database=test_config.mongo_database
    )

    items = Client().use(Item)

    created = []
    for index in range(10):
        item = await items.create_one({"index": index})
        created.append(item)

    async for updated in items.update_many(
        {"index": {"$gte": 4}}, {"description": "Hello there how are you?"}
    ):
        assert updated.description == "Hello there how are you?"
        assert await items.find_one_by_id(updated.id) == updated


@pytest.mark.asyncio
async def test_multiple_documents(test_config):
    class Product(BaseDocument):
        title: str

    class Order(BaseDocument):
        number: int

    await Client.initialize(
        mongo_url=test_config.mongo_url, mongo_database=test_config.mongo_database
    )

    client = Client()

    products = client.use(Product)
    orders = client.use(Order)

    product = await products.create_one({"title": "tshirt"})
    order = await orders.create_one({"number": 1})

    assert await products.find_one_by_id(product.id) == product
    assert await orders.find_one_by_id(order.id) == order

    # Make sure they are in the correct collections
    assert await client.db.products.find_one({"id": product.id}) is not None
    assert await client.db.orders.find_one({"id": order.id}) is not None
