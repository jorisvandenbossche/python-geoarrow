"""
Storing geometry data in Apache Arrow format
"""
# flake8: noqa

__version__ = "0.1.0"


from .extension_types import (
    PointGeometryType,
    MultiPointGeometryType,
    MultiLineStringGeometryType,
    MultiPolygonGeometryType,
    construct_geometry_array,
    construct_numpy_array,
)
from .coords import get_flat_coords_offset_arrays, get_geometries_from_flatcoords
