from typing import List

import pytest

from vanmongo import BaseDocument, Client


@pytest.mark.asyncio
async def test_aggregate(test_config):
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

    aggregation_pipeline = [
        {"$match": {"index": {"$exists": True}}},
        {"$sort": {"created_at": 1}},
    ]
    sort_by_creation = [
        item
        async for item in items.aggregate(aggregation_pipeline=aggregation_pipeline)
    ]
    assert sort_by_creation == fixture

    aggregation_pipeline = [
        {"$match": {"index": {"$exists": True}}},
        {"$sort": {"created_at": 1}},
        {"$limit": 5},
    ]

    sort_by_creation_raw = [
        item
        async for item in items.aggregate_raw(aggregation_pipeline=aggregation_pipeline)
    ]
    assert len(sort_by_creation_raw) == 5
