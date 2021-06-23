import asyncio

import pytest
from async_search_client import Client as SearchClient
from pydantic import BaseModel
from pymongo import MongoClient

from vanmongo import Client


class TestConfig(BaseModel):
    mongo_url: str = "mongodb://localhost:27017"
    mongo_database: str = "satel-mongo"
    meilsearch_url: str = "http://localhost:7700"


@pytest.yield_fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for each test case."""
    loop = asyncio.get_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session")
def test_config():
    return TestConfig()


@pytest.fixture(scope="session")
def mongo(test_config):
    return MongoClient(test_config.mongo_url)


@pytest.fixture(scope="session")
def db(mongo, test_config):
    return mongo[test_config.mongo_database]


@pytest.fixture(scope="session")
async def search(test_config):
    async with SearchClient(test_config.meilsearch_url) as client:
        yield client


@pytest.fixture()
async def wait_for_index(search):
    async def wait(index: str):
        while True:
            wait = False
            for update in await search.index(index).get_all_update_status():
                if update.status != "processed":
                    wait = True
                    break
            if not wait:
                break

    return wait


@pytest.fixture(autouse=True)
async def reset(mongo, search, test_config):
    await Client.shutdown()

    # MongoDB
    mongo.drop_database(test_config.mongo_database)

    # Meilsearch
    indexes = await search.get_indexes()
    if not indexes:
        return
    for index in await search.get_indexes():
        await index.delete()
