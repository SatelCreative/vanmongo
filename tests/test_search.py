from typing import List

import pytest

from vanmongo import BaseDocument, Client

from .test_connection import extract_nodes


@pytest.mark.asyncio
async def test_initialize_search(test_config, wait_for_index):
    fixture: List[BaseDocument] = []

    async def setup():
        class Product(BaseDocument, search=["title"]):
            title: str

        # Create documents in DB that are not in meil
        await Client.initialize(
            mongo_url=test_config.mongo_url,
            mongo_database=test_config.mongo_database,
        )

        products = Client().use(Product)

        for index in range(50):
            product = await products.create_one({"title": f"pants {index}"})
            fixture.append(product)

        await Client.shutdown()

    # Expect initialize to populate meilisearch
    await setup()

    class Product(BaseDocument, search=["title"]):
        title: str

    await Client.initialize(
        mongo_url=test_config.mongo_url,
        mongo_database=test_config.mongo_database,
        meilisearch_url=test_config.meilisearch_url,
    )

    await wait_for_index("products")

    products = Client().use(Product)

    first_page = await products.find_connection(first=100, query="pants")
    assert extract_nodes(first_page) == fixture
