import polars
from collections import defaultdict
import json
from environs import env
import os
import mysql.connector as sql
import requests


class extractor:
    def __init__(self) -> None:
        pass

    def extract_csv(self, path: str) -> polars.LazyFrame:
        # Ensure .csv filetype
        assert os.path.splitext(path)[1] == ".csv", "Path does not point to a csv file"
        return polars.scan_csv(path)

    def extract_api(self, endpoint: str, env_key: str = "api") -> polars.LazyFrame:
        # Collect API info from .env
        environment: defaultdict[str, None | str] = defaultdict(
            lambda: None, env.dict(env_key)
        )
        assert environment["address"] is not None, "No API address found in .env"

        # The join-filter combo ensures that ':' is added only if a port is specified.
        api_address: str = f"http://{":".join(filter(None, [environment["address"], environment["port"]]))}/{endpoint}"

        # Get response from API call
        response: requests.Response = requests.get(api_address)
        assert response.status_code == 200, f"Bad http response: {response.status_code}"
        return polars.from_dicts(json.loads(response.json())).lazy()

    def extract_sql(self, table: str, env_key: str = "sql") -> polars.LazyFrame:
        # Create connection based on info from .env
        connection = sql.connect(**env.dict(env_key))

        # Ensure that the results are dictionaries, so that polars can understand it.
        cursor = connection.cursor(dictionary=True)
        cursor.execute(f"select * from {table}")
        return polars.from_dicts(cursor.fetchall()).lazy() # type: ignore