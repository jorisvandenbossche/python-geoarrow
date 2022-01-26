import json
import warnings

import geopandas
from geopandas.io.arrow import (
    _create_metadata,
    _decode_metadata,
    _encode_metadata,
    _validate_dataframe,
)

from pyarrow import parquet

from .extension_types import construct_geometry_array


def _arrow_to_geopandas(table):
    # NOTE this is copied and slightly adapted from geopandas
    """
    Helper function with main, shared logic for read_parquet/read_feather.
    """

    metadata = table.schema.metadata
    if metadata is None or b"geo" not in metadata:
        raise ValueError(
            """Missing geo metadata in Parquet/Feather file.
            Use pandas.read_parquet/read_feather() instead."""
        )

    try:
        metadata = _decode_metadata(metadata.get(b"geo", b""))

    except (TypeError, json.decoder.JSONDecodeError):
        raise ValueError("Missing or malformed geo metadata in Parquet/Feather file")

    # _validate_metadata(metadata)

    # Find all geometry columns that were read from the file.  May
    # be a subset if 'columns' parameter is used.
    geometry_columns = list(set(table.column_names).intersection(metadata["columns"]))

    if not len(geometry_columns):
        raise ValueError(
            """No geometry columns are included in the columns read from
            the Parquet/Feather file.  To read this file without geometry columns,
            use pandas.read_parquet/read_feather() instead."""
        )

    geometry = metadata["primary_column"]

    # Missing geometry likely indicates a subset of columns was read;
    # promote the first available geometry to the primary geometry.
    if len(geometry_columns) and geometry not in geometry_columns:
        geometry = geometry_columns[0]

        # if there are multiple non-primary geometry columns, raise a warning
        if len(geometry_columns) > 1:
            warnings.warn(
                "Multiple non-primary geometry columns read from Parquet/Feather "
                "file. The first column read was promoted to the primary geometry."
            )

    # convert attributes
    df = table.drop(geometry_columns).to_pandas()

    # Convert the geometry columns to geopandas format
    for col in geometry_columns:
        df[col] = geopandas.array.GeometryArray(
            table[col].chunk(0).to_numpy(), crs=metadata["columns"][col]["crs"]
        )

    return geopandas.GeoDataFrame(df, geometry=geometry)


def read_parquet(path, columns=None, **kwargs):
    table = parquet.read_table(path, columns=columns, **kwargs)
    return _arrow_to_geopandas(table)


def _geopandas_to_arrow(df, index=None):
    # NOTE this is copied and slightly adapted from geopandas
    """
    Helper function with main, shared logic for to_parquet/to_feather.
    """
    from pyarrow import Table

    _validate_dataframe(df)

    # create geo metadata before altering incoming data frame
    geo_metadata = _create_metadata(df)

    # TODO this hard-codes "geometry" column

    # convert attributes to pyarrow
    df_attr = df.drop(columns=["geometry"])
    table = Table.from_pandas(df_attr, preserve_index=index)

    # convert geometry
    geom_arr = construct_geometry_array(df.geometry.array.data)
    table = table.append_column("geometry", geom_arr)

    encoding = geom_arr.type.extension_name.split(".")[1]
    geo_metadata["columns"]["geometry"]["encoding"] = encoding

    # Store geopandas specific file-level metadata
    # This must be done AFTER creating the table or it is not persisted
    metadata = table.schema.metadata
    metadata.update({b"geo": _encode_metadata(geo_metadata)})
    return table.replace_schema_metadata(metadata)


def to_parquet(df, path, index=None, compression="snappy", **kwargs):
    table = _geopandas_to_arrow(df, index=index)
    parquet.write_table(table, path, compression=compression, **kwargs)
