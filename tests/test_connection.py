from typing import List, Any

import pytest

from satel_mongo import BaseDocument, Client, Connection


def extract_nodes(connection: Connection[Any]):
    return [edge.node for edge in connection.edges]


def extract_last_cursor(connection: Connection[Any]) -> str:
    if len(connection.edges) == 0:
        raise Exception("Cannot extract cursor")

    return connection.edges[-1].cursor


@pytest.mark.asyncio
async def test_simple_connection(test_config):
    class Item(BaseDocument):
        index: int

    await Client.initialize(
        mongo_url=test_config.mongo_url, mongo_database=test_config.mongo_database
    )

    items = Client().use(Item)

    created: List[Item] = []
    for index in range(50):
        item = await items.create_one({"index": index})
        created.append(item)

    first_page = await items.find_connection(first=10)
    assert extract_nodes(first_page) == created[:10]

    second_page = await items.find_connection(
        first=10, after=extract_last_cursor(first_page)
    )
    assert extract_nodes(second_page) == created[10:20]
