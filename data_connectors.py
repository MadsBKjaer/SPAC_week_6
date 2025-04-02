import polars as pl
from os import path
import requests
import json
import mysql.connector as sql


class csv_connector:
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
        """
        file_name, file_extension = path.splitext(file_name)

        assert file_extension in [
            "",
            ".csv",
        ], f"Unsupported file extension: {repr(file_extension)}"

        full_path: str = path.join(self.folder_path, f"{file_name}.csv")

        assert path.isfile(full_path), f"No file found at {full_path}"
        return pl.read_csv(full_path)


class api_connector(csv_connector):
    """
    Class for handling data retrieval from api.
    """

    address: str | None
    port: int | None
    folder_path: str | None

    def __init__(
        self,
        address: str | None = None,
        port: int | None = None,
        folder_path: str | None = path.join("data", "api"),
    ):
        """
        Initialize class that handles retrieving data from api.

        Parameters
        ----------
        address: string or None, default None
            address to direct requests towards. Will use csv_connector to retrieve data locally if address is not provided.
        port: int or None, default None
            port to use with the address.
        folder_path: str or None, default None
            Folder to find local csv files in case bad http response or no provided address. If None the full path must be provided for methods asking for a file name.
        """
        super().__init__(folder_path)
        self.address = address
        self.port = port

    def get(self, file_name: str) -> pl.DataFrame:
        """
        Retrieves data from api and returns as a polars.DataFrame. Retrieves data from local csv if api failed.

        Parameters
        ----------
        file_name: str
            Name of file to direct api request to. Is also used as file name for retrieving data locally and should be full path to the file, if no folder_path = None on initialization.
        """
        try:
            assert self.address, "No address provided"

            http_address: str = (
                f"http://{":".join(filter(None, [self.address, self.port]))}/{file_name}"  # The join-filter combo ensures that ':' is added only if a port is specified.
            )
            http_response: str = requests.get(http_address)

            assert (
                http_response.status_code == 200
            ), f"Bad response from {http_address}: {http_response.status_code}"

            return pl.from_dicts(json.loads(http_response.json()))
        except Exception as e:
            print(e, "\nWill use local file instead")
            return super().get(file_name)


class sql_connector(csv_connector):
    """
    Class for handling data retrieval from MySQL server.
    """

    connection_args: dict[str, str | int] | None
    folder_path: str | None

    def __init__(
        self,
        connection_args: dict[str, str | int] | None = None,
        folder_path: str | None = path.join("data", "db"),
    ):
        """
        Initialize class that handles retrieving data from MySQL database.

        Parameters
        ----------
        connection_args: dict[str, str | int] or None, default None
            key value arguments to pass to MySQL connector.
        folder_path: string or None, default None
            Folder to find local csv files in case bad connection to database or if no connection arguments are provided. If None the full path must be provided for methods asking for a file name.
        """
        super().__init__(folder_path)
        self.connection_args = connection_args

    def get(self, table_name: str) -> pl.DataFrame:
        """
        Retrieves data from MySQL database and returns as a polars.DataFrame. Retrieves data from local csv if database failed.

        Parameters
        ----------
        table_name: str
            Name of table to retrieve. Is also used as file name for retrieving data locally and should be full path to the file, if no folder_path = None on initialization.
        """
        try:
            assert self.connection_args, "No connection arguments provided"
            connection = sql.connect(**self.connection_args)
            cursor = connection.cursor(
                dictionary=True
            )  # Ensure that the results are dictionaries, so that polars can understand it.
            cursor.execute(f"select * from {table_name}")
            return pl.from_dicts(cursor.fetchall())
        except Exception as e:
            print(e, "\nWill use local file instead")
            return super().get(table_name)
