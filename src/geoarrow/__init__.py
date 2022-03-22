"""
Storing geometry data in Apache Arrow format
"""
# flake8: noqa

__version__ = "0.1.0"


from .extension_types import (
    PointGeometryType,
    LineStringGeometryType,
    PolygonGeometryType,
    MultiPointGeometryType,
    MultiLineStringGeometryType,
    MultiPolygonGeometryType,
    register_geometry_extension_types,
    unregister_geometry_extension_types,
    construct_geometry_array,
    construct_numpy_array,
)
from .coords import get_flat_coords_offset_arrays, get_geometries_from_flatcoords
from .io import read_parquet, to_parquet


# by default register the extension types when importing geoarrow
register_geometry_extension_types()
