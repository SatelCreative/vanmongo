import pytest
from pydantic import BaseModel

from satel_mongo import BaseDocument, Client


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


def assert_indexes(collection, **kwargs):
    info = collection.index_information()
    info.pop("_id_")  # Default _id index

    indexes = {}
    for name, value in info.items():
        indexes[name] = value["key"]

    assert indexes == kwargs


@pytest.mark.asyncio
async def test_indexes(db, test_config):
    class Item(BaseDocument):
        pass

    await Client.initialize(
        mongo_url=test_config.mongo_url, mongo_database=test_config.mongo_database
    )

    assert_indexes(db[Item.collection], id=[("id", 1)])
