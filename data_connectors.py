from os import path
from environs import env
from collections import defaultdict
import polars as pl
import requests
import json
import mysql.connector as sql


class ConnectorCSV:
    """
    Class for handling data retrieval from csv files.
    """

    folder_path: str

    def __init__(self, folder_path: str | None = None) -> None:
        """
        Initialize class that handles retrieving data from local csv files.

        Parameters
        ----------
        folder_path: string or None, default None
            Folder to operate within. If None the full path must be provided for methods asking for a file name.
        """
        self.folder_path = folder_path if folder_path is not None else ""

    def get(self, file_name: str) -> pl.DataFrame:
        """
        Retrieves data from local csv file and returns as a polars.DataFrame

        Parameters
        ----------
        file_name: string
            Name of csv file. Should be full path to the file, if no folder_path was provided on initialization.

        Returns
        -------
        output: pl.DataFrame
            polars dataframe with data local CSV file.
        """
        file_name, file_extension = path.splitext(file_name)

        assert file_extension in [
            "",
            ".csv",
        ], f"Unsupported file extension: {repr(file_extension)}"

        full_path: str = path.join(self.folder_path, f"{file_name}.csv")

        assert path.isfile(full_path), f"No file found at {full_path}"
        return pl.read_csv(full_path)


class ConnectorAPI(ConnectorCSV):
    """
    Class for handling data retrieval from api.
    """

    folder_path: str | None
    env_key: str
    timeout: int

    def __init__(
        self,
        env_key: str,
        timeout: int = 2,
        folder_path: str | None = path.join("data", "api"),
    ):
        """
        Initialize class that handles retrieving data from api.

        Parameters
        ----------
        env_key: str
            Key for retrieving appropriate variables from .env file.
        timeout: int | None, default 2
            Timeout time in seconds.
        folder_path: str or None, default os.path.join("data", "api")
            Folder to find local csv files in case bad http response or no provided address. If None the full path must be provided for methods asking for a file name.
        """
        super().__init__(folder_path)
        self.env_key = env_key
        self.timeout = timeout

    def get(self, file_name: str) -> pl.DataFrame:
        """
        Retrieves data from api and returns as a polars.DataFrame. Retrieves data from local csv if api failed.

        Parameters
        ----------
        file_name: str
            Name of file to direct api request to. Is also used as file name for retrieving data locally and should be full path to the file, if no folder_path = None on initialization.

        Returns
        -------
        output: pl.DataFrame
            polars dataframe with data from API or local CSV file.
        """
        environment = defaultdict(lambda: None, env.dict(self.env_key))
        try:
            assert environment["address"], "No address found in .env"

            http_address: str = (
                f"http://{":".join(filter(None, [environment["address"], environment["port"]]))}/{file_name}"  # The join-filter combo ensures that ':' is added only if a port is specified.
            )
            http_response: requests.Response = requests.get(
                http_address, timeout=self.timeout
            )

            assert (
                http_response.status_code == 200
            ), f"Bad response from {http_address}: {http_response.status_code}"

            return pl.from_dicts(json.loads(http_response.json()))
        except Exception as e:
            print(e, "\nWill use local file instead")
            return super().get(file_name)


class ConnectorSQL(ConnectorCSV):
    """
    Class for handling data retrieval from MySQL server.
    """

    env_key: str
    folder_path: str | None
    timeout: int

    def __init__(
        self,
        env_key: str,
        timeout: int = 2,
        folder_path: str | None = path.join("data", "db"),
    ):
        """
        Initialize class that handles retrieving data from MySQL database.

        Parameters
        ----------
        env_key: str
            Key for retrieving appropriate variables from .env file.
        timeout: int | None, default 2
            Timeout time in seconds.
        folder_path: string or None, default os.path.join("data", "db")
            Folder to find local csv files in case bad connection to database or if no connection arguments are provided. If None the full path must be provided for methods asking for a file name.
        """
        super().__init__(folder_path)
        self.env_key = env_key
        self.timeout = timeout

    def get(self, table_name: str) -> pl.DataFrame:
        """
        Retrieves data from MySQL database and returns as a polars.DataFrame. Retrieves data from local csv if database failed.

        Parameters
        ----------
        table_name: str
            Name of table to retrieve. Is also used as file name for retrieving data locally and should be full path to the file, if no folder_path = None on initialization.
        """
        try:
            connection = sql.connect(
                **env.dict(self.env_key), connection_timeout=self.timeout
            )
            cursor = connection.cursor(
                dictionary=True
            )  # Ensure that the results are dictionaries, so that polars can understand it.
            cursor.execute(f"select * from {table_name}")
            return pl.from_dicts(cursor.fetchall())  # type: ignore
        except Exception as e:
            print(e, "\nWill use local file instead")
            return super().get(table_name)
