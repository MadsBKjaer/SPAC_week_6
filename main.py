from os import path
from data_connectors import ConnectorCSV, ConnectorAPI, ConnectorSQL
from database import MongoDB


if __name__ == "__main__":
    with MongoDB("BikeCorpDB") as db:
        # Does the CSV part
        Connector = ConnectorCSV(path.join("data", "csv"))
        for file_name in ["staffs", "stores"]:
            data = Connector.get(file_name)
            db.write_polars(data, file_name, True)

        # Does the API part
        Connector = ConnectorAPI("api", timeout=1)
        for path in ["customers", "order_items", "orders"]:
            data = Connector.get(path)
            db.write_polars(data, path, True)

        # Does the SQL stuff
        Connector = ConnectorSQL("sql", timeout=1)
        for table_name in ["brands", "categories", "products", "stocks"]:
            data = Connector.get(table_name)
            db.write_polars(data, table_name, True)
