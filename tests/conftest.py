import pytest
from pydantic import BaseModel
from pymongo import MongoClient


class TestConfig(BaseModel):
    mongo_url: str = "mongodb://localhost:27017"
    mongo_database: str = "satel-mongo"


@pytest.fixture(scope="session")
def test_config():
    return TestConfig()


@pytest.fixture(scope="session")
def mongo(test_config):
    return MongoClient(test_config.mongo_url)


@pytest.fixture(autouse=True)
def reset(mongo, test_config):
    mongo.drop_database(test_config.mongo_database)
