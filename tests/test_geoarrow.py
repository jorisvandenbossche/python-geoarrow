import pathlib

import pyarrow.parquet as pq
from pyarrow import feather

import pytest

import geoarrow  # noqa


HERE = pathlib.Path(__file__).parent.resolve()


@pytest.mark.parametrize(
    "geometry_type",
    [
        # "point",  # TODO point fails because of null value
        "linestring",
        "polygon",
        "multipoint",
        "multilinestring",
        "multipolygon",
    ],
)
def test_read_reference_data(geometry_type):

    table1 = pq.read_table(
        HERE / "data" / "example_parquet" / f"{geometry_type}-default.parquet"
    )
    table2 = feather.read_table(
        HERE / "data" / "example_feather" / f"{geometry_type}-default.feather"
    )
    assert table1.equals(table2)
    assert isinstance(
        table1["geometry"].type, geoarrow.extension_types.BaseGeometryType
    )
