from typing import List

import pytest

from satel_mongo import BaseDocument, Client


@pytest.mark.asyncio
async def test_sort(test_config):
    sort_options = ["index"]

    class Item(BaseDocument, sort_options=sort_options):
        index: int

    await Client.initialize(
        mongo_url=test_config.mongo_url, mongo_database=test_config.mongo_database
    )

    client = Client()

    items = client.use(Item)

    fixture: List[Item] = []
    for index in range(50):
        item = await items.create_one({"index": index})
        fixture.append(item)
    fixture_reversed = list(reversed(fixture))

    index_ascending = [item async for item in items.find(sort="index", limit=10)]
    assert index_ascending == fixture[:10]

    index_descending = [
        item async for item in items.find(sort="index", reverse=True, limit=10)
    ]
    assert index_descending == fixture_reversed[:10]

    created_ascending = [item async for item in items.find(sort="created_at", limit=10)]
    assert created_ascending == fixture[:10]

    created_descending = [
        item async for item in items.find(sort="created_at", reverse=True, limit=10)
    ]
    assert created_descending == fixture_reversed[:10]

    fixture_updated = await items.update_one_by_id(fixture[5].id, {"index": 999})

    updated_index_ascending = [
        item async for item in items.find(sort="index", limit=10)
    ]
    assert updated_index_ascending == fixture[:5] + fixture[6:11]

    updated_index_descending = [
        item async for item in items.find(sort="index", reverse=True, limit=10)
    ]
    assert updated_index_descending == [fixture_updated] + fixture_reversed[:9]

    updated_updated_ascending = [
        item async for item in items.find(sort="updated_at", limit=10)
    ]
    assert updated_updated_ascending == fixture[:5] + fixture[6:11]

    updated_updated_descending = [
        item async for item in items.find(sort="updated_at", reverse=True, limit=10)
    ]
    assert updated_updated_descending == [fixture_updated] + fixture_reversed[:9]
