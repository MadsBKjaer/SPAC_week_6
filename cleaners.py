from pymongo.synchronous.database import Database
import polars as pl


def drop_empty(
    database: Database,
    collection_field_pairs: dict[str, str],
    empty_values: list[str] = ["NULL"],
) -> None:
    """
    Drops empty values from given fields

    Parameters
    ----------
    database: Database
        Database class for handling connection to the MongoDB database
    collection_field_pairs: dict[str, str]
        Dictionary with paired collections and fields to remove empty values from
    empty_values: list[str], default ["NULL"]
        values to consider as empty.
    """
    for collection, field in collection_field_pairs.items():
        database[collection].update_many(
            {field: {"$in": empty_values}}, {"$unset": {field: ""}}
        )


def drop_field(database: Database, collection: str, field: str) -> None:
    """
    Drops an entire field from a collection

    Parameters
    ----------
    database: Database
        Database class for handling connection to the MongoDB database
    collection: str
        Collection containing the field to drop
    field: str
        Field to drop
    """
    database[collection].update_many({}, {"$unset": {field: ""}})


def merge_collection(
    database: Database,
    target: dict[str, str],
    source: dict[str, str],
) -> None:
    """
    Replaces a field with a field from another collection joining using given bridge fields

    Parameters
    ----------
    database: Database
        Database class for handling connection to the MongoDB database
    target: dict[str, str]
        Dictionary with keys: "collection", "bridge_field", where bridge field is the key for creating the relation to the source collection
    source: dict[str, str]
        Dictionary with keys: "collection", "bridge_field", "source_field", where bridge field is the key for creating the relation to the target collection and source field is where to find the values to merge into the target collection
    """
    mappings: dict[str, str] = {}
    for id_target_pair in database[source["collection"]].find(
        {}, {source["source_field"]: 1, source["bridge_field"]: 1}
    ):
        mappings[id_target_pair[source["bridge_field"]]] = id_target_pair[
            source["source_field"]
        ]
    for bridge, _target in mappings.items():
        database[target["collection"]].update_many(
            {target["bridge_field"]: bridge},
            {
                "$set": {source["source_field"]: _target},
                "$unset": {target["bridge_field"]: ""},
            },
        )


def merge_collections(
    database: Database,
    target_collection: str,
    source_dict: dict[str, tuple[str, str, str]],
) -> None:
    """
    For repeated use of merge collection when target collection remains constant

    Parameters
    ----------
    database: Database
        Database class for handling connection to the MongoDB database
    target_collection: str
        Collection to merge values into
    source_dict: dict[str, tuple[str, str, str]]
        Dictionary with source collection as keys, and a tuple with following values: (target_bridge_field, source_bridge_field, source_field)
    """
    for source_collection, fields in source_dict.items():
        target_bridge_field, source_bridge_field, source_field = fields
        merge_collection(
            database,
            target={
                "collection": target_collection,
                "bridge_field": target_bridge_field,
            },
            source={
                "collection": source_collection,
                "source_field": source_field,
                "bridge_field": source_bridge_field,
            },
        )


def trim_whitespace(dataframe: pl.DataFrame, field: str) -> pl.DataFrame:
    """
    Trim white space from field in a polars dataframe

    Parameters
    ----------
    dataframe: pl.DataFrame
        Polars dataframe with the field that needs trimming
    field: str
        Field with strings to trim

    Returns
    -------
    pl.DataFrame
        A polars dataframe where values in field is trimmed from whitespace.
    """
    return dataframe.with_columns(pl.col(field).str.strip_chars())

    # database[collection].update_many(
    #     {field: {"$regex": "(^ +)|( +$)"}},
    #     {"$set": {field: {"$trim": {"input": f"${field}"}}}},
    # )


def trim_prefix(
    dataframe: pl.DataFrame,
    field: str,
    prefix_field: str,
    post_trim_whitespace: bool = True,
) -> pl.DataFrame:
    """
    Trim prefix from a field where the prefix is the values of another field

    Parameters
    ----------
    dataframe: pl.DataFrame
        Dataframe with field to trim prefix from
    field: str
        Field with string to trim prefix from
    prefix_field: str
        Field containing prefixes to trim
    post_trim_whitespace: bool, default True
        If the field should have whitespace trimmed after trimming prefix.

    Returns
    -------
    pl.DataFrame
        A polars dataframe where prefixed are trimmed from desired field.
    """
    dataframe = dataframe.with_columns(
        pl.col(field).str.strip_prefix(pl.col(prefix_field))
    )

    if post_trim_whitespace:
        dataframe = trim_whitespace(dataframe, field)

    return dataframe

    # for prefix in database[collection].distinct(prefix_field):
    #     update_result = database[collection].update_many(
    #         {field: {"$regex": f"^{prefix}"}},
    #         [
    #             {
    #                 "$set": {
    #                     field: {
    #                         "$substrCP": [
    #                             f"{field}",
    #                             len(prefix),
    #                             {"$strLenCP": f"{field}"},
    #                         ]
    #                     }
    #                 }
    #             }
    #         ],
    #     )


def replace_with_suffix(
    dataframe: pl.DataFrame,
    field: str,
    suffix_field: str,
    delimiter: str,
    post_trim_whitespace: bool = True,
) -> pl.DataFrame:
    """
    Splits of a suffix in a given field and saves it in another field. Where suffix begins is determined by a delimiter

    Parameters
    ----------
    dataframe: pl.DataFrame
        Dataframe containing a field with strings where a suffix should be split off
    field: str
        Field where a suffix should be split off
    suffix_field: str
        Field where suffix should be saved
    delimiter: str
        Delimiter to determine start of suffix

    Returns
    -------
    pl.DataFrame
        Polars dataframe with a field, now without suffix, and a field with the suffix.
    """
    dataframe = (
        dataframe.with_columns(
            pl.col(field).str.split(delimiter).alias(f"{field}_split_temp")
        )
        .cast({suffix_field: pl.String})
        .with_columns(
            pl.col(f"{field}_split_temp")
            .list.shift()
            .list.join(delimiter)
            .alias(field),
            pl.col(f"{field}_split_temp").list.last().alias(suffix_field),
        )
        .drop(f"{field}_split_temp")
    )

    if post_trim_whitespace:
        dataframe = trim_whitespace(dataframe, field)
        dataframe = trim_whitespace(dataframe, suffix_field)

    return dataframe


def add_id(dataframe: pl.DataFrame, id_name: str) -> pl.DataFrame:
    """
    Adds a integer id to the given dataframe

    Parameters
    ----------
    dataframe: pl.DataFrame
        Dataframe to add the integer id to
    id_name: str
        Name of id

    Returns
    -------
    pl.DataFrame
        Polars dataframe with an integer id.
    """
    return dataframe.with_row_index(id_name)
