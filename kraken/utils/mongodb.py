from pymongo.mongo_client import MongoClient
from pymongo.database import Database
from mongoengine import connect
from mongoengine import disconnect

import os


class MongoDBClientFactory:
    @classmethod
    def get(cls) -> MongoClient:
        client = MongoClient(
            host=str(os.environ.get("MONGODB_HOST", None)),
            port=int(os.environ.get("MONGODB_PORT", None)),
            username=str(os.environ.get("MONGODB_USER", None)),
            password=str(os.environ.get("MONGODB_PASSWORD", None)),
        )

        return client


class MongoDBDatabaseFactory:
    @classmethod
    def get(cls) -> Database:
        client = MongoClient(
            host=str(os.environ.get("MONGODB_HOST", None)),
            port=int(os.environ.get("MONGODB_PORT", None)),
            username=str(os.environ.get("MONGODB_USER", None)),
            password=str(os.environ.get("MONGODB_PASSWORD", None)),
        )

        return client[str(os.environ.get("MONGODB_DATA_DATABASE", None))]


class MongoEngineConnectionWrapper:
    @classmethod
    def connect(cls, alias="default", parameter=None) -> None:
        if parameter is None:
            return connect(
                host=str(os.environ.get("MONGODB_HOST", None)),
                port=int(os.environ.get("MONGODB_PORT", None)),
                username=str(os.environ["MONGODB_USER"]),
                password=str(os.environ["MONGODB_PASSWORD"]),
                db=str(os.environ.get("MONGODB_DATA_DATABASE", None)),
                authentication_source=str(
                    os.environ.get("MONGODB_AUTHENTICATION_SOURCE", None)
                ),
                alias=alias,
                connect=False,
            )
        else:
            return connect(
                host=parameter["MONGODB_HOST"],
                port=parameter["MONGODB_PORT"],
                username=parameter["MONGODB_USER"],
                password=parameter["MONGODB_PASSWORD"],
                db=parameter["MONGODB_DATA_DATABASE"],
                authentication_source=parameter["MONGODB_AUTHENTICATION_SOURCE"],
                alias=alias,
                connect=False,
            )

    @classmethod
    def disconnect(cls, alias="default") -> None:
        disconnect(alias=alias)


class MongoEngineContextManager:
    def __init__(self, alias="default", parameter=None):
        self.parameter = parameter
        self.alias = alias

    def __enter__(self):
        MongoEngineConnectionWrapper.connect(parameter=self.parameter, alias=self.alias)

    def __exit__(self, type, value, traceback):
        MongoEngineConnectionWrapper.disconnect()
