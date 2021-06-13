from typing import Any, List

import pytest

from satel_mongo import BaseDocument, Client, Connection


def extract_nodes(connection: Connection[Any]):
    return [edge.node for edge in connection.edges]


def extract_first_cursor(connection: Connection[Any]) -> str:
    if len(connection.edges) == 0:
        raise Exception("Cannot extract cursor")

    return connection.edges[0].cursor


def extract_last_cursor(connection: Connection[Any]) -> str:
    if len(connection.edges) == 0:
        raise Exception("Cannot extract cursor")

    return connection.edges[-1].cursor


def assert_page_info(
    connection: Connection[Any], has_next_page=False, has_previous_page=False
):
    page_info = connection.page_info

    assert page_info.has_next_page == has_next_page
    assert page_info.has_previous_page == has_previous_page


@pytest.mark.asyncio
async def test_simple_connection(test_config):
    class Item(BaseDocument, sort_options=["index"]):
        index: int

    await Client.initialize(
        mongo_url=test_config.mongo_url, mongo_database=test_config.mongo_database
    )

    items = Client().use(Item)

    fixture: List[Item] = []
    for index in range(50):
        item = await items.create_one({"index": index})
        fixture.append(item)

    first_page = await items.find_connection(first=10)
    assert extract_nodes(first_page) == fixture[:10]
    assert_page_info(first_page, has_next_page=True)

    second_page = await items.find_connection(
        first=10, after=extract_last_cursor(first_page)
    )
    assert extract_nodes(second_page) == fixture[10:20]
    assert_page_info(second_page, has_next_page=True, has_previous_page=True)

    last_page = await items.find_connection(
        first=30, after=extract_last_cursor(second_page)
    )
    assert extract_nodes(last_page) == fixture[20:]
    assert_page_info(last_page, has_previous_page=True)

    before_first_page = await items.find_connection(
        last=10, before=extract_first_cursor(second_page)
    )
    assert extract_nodes(before_first_page) == fixture[:10]
    assert_page_info(before_first_page, has_next_page=True)

    before_last_page = await items.find_connection(
        last=30, before=extract_last_cursor(last_page)
    )
    assert extract_nodes(before_last_page) == fixture[19:49]
    assert_page_info(before_last_page, has_next_page=True, has_previous_page=True)


@pytest.mark.asyncio
async def test_sort_connection(test_config):
    class Item(BaseDocument, sort_options=["index"]):
        index: int

    await Client.initialize(
        mongo_url=test_config.mongo_url, mongo_database=test_config.mongo_database
    )

    items = Client().use(Item)

    fixture: List[Item] = []
    for index in range(50):
        item = await items.create_one({"index": 50 - index})
        fixture.append(item)
    reversed_fixture = fixture[::-1]

    reversed_first_page = await items.find_connection(first=10, reverse=True)
    assert extract_nodes(reversed_first_page) == reversed_fixture[:10]

    reversed_second_page = await items.find_connection(
        first=10, after=extract_last_cursor(reversed_first_page), reverse=True
    )
    assert extract_nodes(reversed_second_page) == reversed_fixture[10:20]

    index_first_page = await items.find_connection(first=10, sort="index")
    assert extract_nodes(index_first_page) == reversed_fixture[:10]

    index_second_page = await items.find_connection(
        first=10, after=extract_last_cursor(index_first_page), sort="index"
    )
    assert extract_nodes(index_second_page) == reversed_fixture[10:20]

    reversed_index_first_page = await items.find_connection(
        first=10, sort="index", reverse=True
    )
    assert extract_nodes(reversed_index_first_page) == fixture[:10]

    reversed_index_second_page = await items.find_connection(
        first=10,
        after=extract_last_cursor(reversed_index_first_page),
        sort="index",
        reverse=True,
    )
    assert extract_nodes(reversed_index_second_page) == fixture[10:20]


@pytest.mark.asyncio
async def test_search_connection(test_config, wait_for_index):
    class Product(BaseDocument, search=["title"]):
        title: str

    await Client.initialize(
        mongo_url=test_config.mongo_url,
        mongo_database=test_config.mongo_database,
        meilsearch_url=test_config.meilsearch_url,
    )

    products = Client().use(Product)

    fixture: List[Product] = []
    for index in range(50):
        product = await products.create_one({"title": f"pants {index}"})
        fixture.append(product)

    await wait_for_index("products")

    first_page = await products.find_connection(first=10, query="pants")
    assert extract_nodes(first_page) == fixture[:10]
    assert_page_info(first_page, has_next_page=True)

    second_page = await products.find_connection(
        first=10, after=extract_last_cursor(first_page), query="pants"
    )
    assert extract_nodes(second_page) == fixture[10:20]
    assert_page_info(second_page, has_next_page=True, has_previous_page=True)

    last_page = await products.find_connection(
        first=30, after=extract_last_cursor(second_page), query="pants"
    )
    assert extract_nodes(last_page) == fixture[20:]
    assert_page_info(last_page, has_previous_page=True)

    before_first_page = await products.find_connection(
        last=10, before=extract_first_cursor(second_page), query="pants"
    )
    assert extract_nodes(before_first_page) == fixture[:10]
    assert_page_info(before_first_page, has_next_page=True)

    before_last_page = await products.find_connection(
        last=30, before=extract_last_cursor(last_page), query="pants"
    )
    assert extract_nodes(before_last_page) == fixture[19:49]
    assert_page_info(before_last_page, has_next_page=True, has_previous_page=True)

    short_page = await products.find_connection(first=10, query="pants 23")
    assert extract_nodes(short_page) == [fixture[23]]
    assert_page_info(short_page)
