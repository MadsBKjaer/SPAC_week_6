import pymongo
import polars as pl
from typing import Any
import pymongo.synchronous
import pymongo.synchronous.database


class MongoDB:
    database_name: str
    kwargs: dict[str, Any]
    client: pymongo.MongoClient
    database: pymongo.synchronous.database.Database

    def __init__(self, database: str, **kwargs):
        self.kwargs = kwargs
        self.database_name = database

    def __enter__(self):
        self.client = pymongo.MongoClient(**self.kwargs)
        self.database = self.client[self.database_name]
        return self

    def __exit__(self, exception_type, exception_value, traceback) -> None:
        if exception_type:
            print(f"{exception_type}: {exception_value}\n{traceback}")

    def write_polars(
        self, dataframe: pl.DataFrame, collection: str, overwrite: bool = False
    ) -> None:
        if overwrite:
            self.database[collection].drop()
        self.database[collection].insert_many(dataframe.to_dicts())
