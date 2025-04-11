import polars as pl
from typing import Any
from pymongo import MongoClient
from pymongo.synchronous.database import Database


class MongoDB:
    database_name: str
    kwargs: dict[str, Any]
    client: MongoClient
    database: Database

    def __init__(self, database: str, **kwargs):
        """
        Initialize class for communicating with a MongoDB database.

        Parameters
        ----------
        database: str
            Name of database to connect to.
        kwargs:
            Parameters to pass to the MongoClient
        """
        self.kwargs = kwargs
        self.database_name = database

    def __enter__(self):
        """
        Setup for context management

        Returns
        -------
        MongoDB: class
            Class for communicating with MongoDB database
        """
        self.client = MongoClient(**self.kwargs)
        self.database = self.client[self.database_name]
        return self

    def __exit__(self, exception_type, exception_value, traceback) -> None:
        """
        Exit handling for context management

        Parameters
        ----------
        exception_type: _type_
            _description_
        exception_value: _type_
            _description_
        traceback: _type_
            _description_
        """
        if exception_type:
            print(f"{exception_type}: {exception_value}\n{traceback}")

    def write_polars(
        self, dataframe: pl.DataFrame, collection: str, overwrite: bool = False
    ) -> None:
        """
        Writes a polars dataframe to a desired collection in the database.

        Parameters
        ----------
        dataframe: pl.DataFrame
            Dataframe to write to the database
        collection: str
            The collection where the data should be written
        overwrite: bool, default False
            Whether to overwrite any existing data in the collection.
        """
        if overwrite:
            self.database[collection].drop()
        self.database[collection].insert_many(dataframe.to_dicts())

    def read_polars(self, collection: str) -> pl.DataFrame:
        """
        Read data from a collection to a polars dataframe

        Parameters
        ----------
        collection: str
            Collection to read from

        Returns
        -------
        pl.DataFrame
            Polars dataframe with data from collection.
        """
        return pl.from_dicts(self.database[collection].find())

    def drop_collections(self, collections: list[str]) -> None:
        """
        Drops given collections from the database

        Parameters
        ----------
        collections: list[str]
            Collections to drop.
        """
        for collection in collections:
            self.database.drop_collection(collection)

    def distinct_values(self, collection: str, field: str) -> list[Any]:
        """
        Returns distinct values from the a field in a collection

        Parameters
        ----------
        collection: str
            Targeted collection
        field: str
            Field to list distinct values from

        Returns
        -------
        list[Any]
            List of distinct values from given field in collection.
        """
        return self.database[collection].distinct(field)
