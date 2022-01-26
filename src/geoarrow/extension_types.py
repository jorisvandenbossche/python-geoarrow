import numpy as np
import pyarrow as pa

from .coords import get_flat_coords_offset_arrays, get_geometries_from_flatcoords

# _point_type = pa.list_(
#     pa.field("xy", pa.float64(), nullable=False), 2)
# _multipoint_type = pa.list_(
#     pa.field("parts", pa.list_(
#         pa.field("xy", pa.float64(), nullable=False), 2), nullable=False))
# _multiline_type = pa.list_(
#     pa.field("parts", pa.list_(
#         pa.field("vertices", pa.list_(
#             pa.field("xy", pa.float64(), nullable=False), 2), nullable=False))))
# _multipolygon_type = pa.list_(
#     pa.field("parts", pa.list_(
#         pa.field("rings", pa.list_(
#             pa.field("vertices", pa.list_(
#                 pa.field("xy", pa.float64(), nullable=False), 2),
#         nullable=False)), nullable=False))))


# the simpler versions (because creating arrays with that exact type is hard)
_point_type = pa.list_(pa.float64(), 2)
_multipoint_type = pa.list_(pa.list_(pa.float64(), 2))
_multiline_type = pa.list_(pa.list_(pa.list_(pa.float64(), 2)))
_multipolygon_type = pa.list_(pa.list_(pa.list_(pa.list_(pa.float64(), 2))))


# # TODO temporary version without names for compat with R geoarrow
# _point_type = pa.list_(
#     pa.field("", pa.float64()), 2)
# _multipoint_type = pa.list_(
#     pa.field("", pa.list_(
#         pa.field("", pa.float64()), 2)))
# _multiline_type = pa.list_(
#     pa.field("", pa.list_(
#         pa.field("", pa.list_(
#             pa.field("", pa.float64()), 2)))))
# _multipolygon_type = pa.list_(
#     pa.field("", pa.list_(
#         pa.field("", pa.list_(
#             pa.field("", pa.list_(
#                 pa.field("", pa.float64()), 2)))))))


class ArrowGeometryArray(pa.ExtensionArray):
    def to_numpy(self, **kwargs):
        return construct_numpy_array(self.storage, self.type.extension_name)


class BaseGeometryType(pa.ExtensionType):
    _storage_type: pa.DataType
    _extension_name: str

    def __init__(self, crs=None):
        # attributes need to be set first before calling
        # super init (as that calls serialize)
        self._crs = crs
        pa.ExtensionType.__init__(self, self._storage_type, self._extension_name)

    @property
    def crs(self):
        return self._crs

    def __arrow_ext_serialize__(self):
        return "crs={}".format(self.crs or "").encode()

    @classmethod
    def __arrow_ext_deserialize__(cls, storage_type, serialized):
        # return an instance of this subclass given the serialized
        # metadata.
        # TODO ignore serliaed metadata for now
        # serialized = serialized.decode()
        # assert serialized.startswith("crs=")
        # crs = serialized.split('=')[1]
        # if crs == "":
        #     crs = None
        return cls()

    def __arrow_ext_class__(self):
        return ArrowGeometryArray


class PointGeometryType(BaseGeometryType):
    _storage_type = _point_type
    _extension_name = "geoarrow.point"


class MultiPointGeometryType(BaseGeometryType):
    _storage_type = _multipoint_type
    _extension_name = "geoarrow.multipoint"


class MultiLineStringGeometryType(BaseGeometryType):
    _storage_type = _multiline_type
    _extension_name = "geoarrow.multilinestring"


class MultiPolygonGeometryType(BaseGeometryType):
    _storage_type = _multipolygon_type
    _extension_name = "geoarrow.multipolygon"


_point = PointGeometryType()
pa.register_extension_type(_point)
_multipoint = MultiPointGeometryType()
pa.register_extension_type(_multipoint)
_multilinestring = MultiLineStringGeometryType()
pa.register_extension_type(_multilinestring)
_multipolygon = MultiPolygonGeometryType()
pa.register_extension_type(_multipolygon)


# register polygon as well with same type as MultiPolygon

# class PolygonGeometryType(BaseGeometryType):
#     _storage_type = _multipolygon_type
#     _extension_name = 'geoarrow.polygon'


# _polygon = PolygonGeometryType()
# pa.register_extension_type(_polygon)


def construct_geometry_array(arr):
    typ, coords, offsets = get_flat_coords_offset_arrays(arr)

    if typ == "point":
        parr = pa.FixedSizeListArray.from_arrays(coords, 2)
        return pa.ExtensionArray.from_storage(PointGeometryType(), parr)

    elif typ == "multipoint":
        _parr = pa.FixedSizeListArray.from_arrays(coords, 2)
        parr = pa.ListArray.from_arrays(pa.array(offsets), _parr)
        return pa.ExtensionArray.from_storage(MultiPointGeometryType(), parr)

    elif typ == "multilinestring":
        offsets1, offsets2 = offsets
        _parr = pa.FixedSizeListArray.from_arrays(coords, 2)
        _parr1 = pa.ListArray.from_arrays(pa.array(offsets1), _parr)
        parr = pa.ListArray.from_arrays(pa.array(offsets2), _parr1)
        return pa.ExtensionArray.from_storage(MultiLineStringGeometryType(), parr)

    elif typ == "multipolygon":
        offsets1, offsets2, offsets3 = offsets
        _parr = pa.FixedSizeListArray.from_arrays(coords, 2)
        _parr1 = pa.ListArray.from_arrays(pa.array(offsets1), _parr)
        _parr2 = pa.ListArray.from_arrays(pa.array(offsets2), _parr1)
        parr = pa.ListArray.from_arrays(pa.array(offsets3), _parr2)
        return pa.ExtensionArray.from_storage(MultiPolygonGeometryType(), parr)

    else:
        raise ValueError("wrong type ", typ)


def construct_numpy_array(arr, extension_name):

    if extension_name == "geoarrow.point":
        coords = np.asarray(arr.values)
        # TODO copy is needed because of read-only memoryview bug in pygeos
        return get_geometries_from_flatcoords("point", coords.copy(), None)

    elif extension_name == "geoarrow.multipoint":
        coords = np.asarray(arr.values.values)
        offsets = np.asarray(arr.offsets)
        # TODO copy is needed because of read-only memoryview bug in pygeos
        return get_geometries_from_flatcoords("multipoint", coords.copy(), offsets)

    elif extension_name == "geoarrow.multilinestring":
        coords = np.asarray(arr.values.values.values)
        offsets2 = np.asarray(arr.offsets)
        offsets1 = np.asarray(arr.values.offsets)
        offsets = (offsets1, offsets2)
        # TODO copy is needed because of read-only memoryview bug in pygeos
        return get_geometries_from_flatcoords("multilinestring", coords.copy(), offsets)

    elif extension_name == "geoarrow.multipolygon":
        coords = np.asarray(arr.values.values.values.values)
        offsets3 = np.asarray(arr.offsets)
        offsets2 = np.asarray(arr.values.offsets)
        offsets1 = np.asarray(arr.values.values.offsets)
        offsets = (offsets1, offsets2, offsets3)
        # TODO copy is needed because of read-only memoryview bug in pygeos
        return get_geometries_from_flatcoords("multipolygon", coords.copy(), offsets)

    else:
        raise ValueError(extension_name)
