from os import path
from data_connectors import ConnectorCSV, ConnectorAPI, ConnectorSQL
from database import MongoDB
from cleaners import *


def transfer_data(database: MongoDB, timeout: int = 0, overwrite: bool = False):
    """
    Transfers data as is from MySQL server, API and local csv files to a MongoDB database.
    Will use local csv files it if failing to connect to MySQL server or API

    Parameters
    ----------
    database: MongoDB
        Database class for handling the connection to the database
    timeout: int, default 0
        Timeout for connecting to the API and MySQL server. Uses local csv file without attempting the API and MySQL server if timeout is 0
    overwrite: bool, default False
        If any existing data in the database should be overwritten.
    """
    # Does the CSV part
    Connector = ConnectorCSV(path.join("data", "csv"))
    for file_name in ["staffs", "stores"]:
        data = Connector.get(file_name)
        database.write_polars(data, file_name, overwrite)

    # Does the API part
    Connector = ConnectorAPI("api", timeout=timeout)
    for _path in ["customers", "order_items", "orders"]:
        data = Connector.get(_path)
        database.write_polars(data, _path, overwrite)

    # Does the SQL stuff
    Connector = ConnectorSQL("sql", timeout=timeout)
    for table_name in ["brands", "categories", "products", "stocks"]:
        data = Connector.get(table_name)
        database.write_polars(data, table_name, overwrite)


if __name__ == "__main__":
    timeout = 0
    with MongoDB("BikeCorpDB") as db:
        transfer_data(db, timeout, overwrite=True)

        #### Add id's to staff and stores ####
        staffs_df = db.read_polars("staffs")
        staffs_df = add_id(staffs_df, "staff_id")
        db.write_polars(staffs_df, "staffs", True)

        stores_df = db.read_polars("stores")
        stores_df = add_id(stores_df, "store_id")
        db.write_polars(stores_df, "stores", True)

        #### Cleaning of products ####
        merge_collections(  # Replaces id with name in products collection, as it is more typical in MongoDB.
            db.database,
            "products",
            {
                "brands": ("brand_id", "brand_id", "brand_name"),
                "categories": ("category_id", "category_id", "category_name"),
            },
        )
        products_df: pl.DataFrame = db.read_polars("products")
        products_df = trim_prefix(  # Remove brand name from product name
            products_df, "product_name", "brand_name"
        )
        products_df = replace_with_suffix(  # Update model year based on suffix of product name (type: int64 -> str to accommodate "2015/2016")
            products_df, "product_name", "model_year", "-"
        )
        db.write_polars(products_df, "products", True)

        #### Cleaning of customers ####
        customers_df: pl.DataFrame = db.read_polars("customers")
        customers_df = trim_whitespace(customers_df, "street")

        #### Cleaning of orders_items ####
        drop_field(db.database, "order_items", "item_id")
        drop_field(db.database, "order_items", "list_price")

        #### Cleaning of orders ####
        merge_collections(  # Replaces id with name in products collection, as it is more typical in MongoDB.
            db.database,
            "orders",
            {
                "staffs": ("staff_name", "name", "staff_id"),
                "stores": ("store", "name", "store_id"),
            },
        )
        merge_collection(
            db.database,
            target={
                "collection": "orders",
                "bridge_field": "staff_name",
            },
            source={
                "collection": "staffs",
                "source_field": "staff_id",
                "bridge_field": "name",
            },
        )

        #### Cleaning of staffs ####
        merge_collection(
            db.database,
            target={
                "collection": "staffs",
                "bridge_field": "store_name",
            },
            source={
                "collection": "stores",
                "source_field": "store_id",
                "bridge_field": "name",
            },
        )
        drop_field(db.database, "staffs", "street")

        #### Cleaning of stocks ####
        merge_collection(
            db.database,
            target={
                "collection": "stocks",
                "bridge_field": "store_name",
            },
            source={
                "collection": "stores",
                "source_field": "store_id",
                "bridge_field": "name",
            },
        )

        #### Drop redundant collections ####
        db.drop_collections(["brands", "categories"])

        #### Drop empty values ####
        drop_empty(  # removes fields, value pairs where value is null. Saves space on storing "nothing".
            db.database,
            {
                "customers": "phone",
                "orders": "shipped_date",
                "staffs": "manager_id",
            },
        )
