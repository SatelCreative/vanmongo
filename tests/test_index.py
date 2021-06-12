from typing import List

import pytest

from satel_mongo import BaseDocument, Client


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

    assert_indexes(
        db[Item._collection],
        id=[("id", 1)],
        sort_updated_at=[("updated_at", 1), ("_id", 1)],
        sort_created_at=[("created_at", 1), ("_id", 1)],
    )


@pytest.mark.asyncio
async def test_sort_indexes(db, test_config):
    sort_options = ['index']

    class Item(BaseDocument, sort_options=sort_options):
        index: int

    await Client.initialize(
        mongo_url=test_config.mongo_url, mongo_database=test_config.mongo_database
    )

    assert_indexes(
        db[Item._collection],
        id=[("id", 1)],
        sort_updated_at=[("updated_at", 1), ("_id", 1)],
        sort_created_at=[("created_at", 1), ("_id", 1)],
        sort_index=[("index", 1), ("_id", 1)],
    )
