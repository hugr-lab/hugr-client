"""WKB → native GeoArrow conversion for Arrow RecordBatches.

Converts WKB binary geometry columns to native GeoArrow nested Arrow types,
matching the Go kernel geoarrow package behavior:

- Point/MultiPoint → geoarrow.multipoint: List<FixedSizeList<Float64>[2]>
- LineString/MultiLineString → geoarrow.multilinestring: List<List<FixedSizeList<Float64>[2]>>
- Polygon/MultiPolygon → geoarrow.multipolygon: List<List<List<FixedSizeList<Float64>[2]>>>

All single types normalized to Multi* for schema consistency.
Z/M dimensions stripped — only X,Y preserved.
"""

import struct
from typing import Optional

import pyarrow as pa

# WKB geometry type codes
_WKB_POINT = 1
_WKB_LINESTRING = 2
_WKB_POLYGON = 3
_WKB_MULTIPOINT = 4
_WKB_MULTILINESTRING = 5
_WKB_MULTIPOLYGON = 6

# Coordinate type: FixedSizeList(2, Float64) — interleaved X,Y
_COORD_TYPE = pa.list_(pa.float64(), 2)

# GeoArrow types (always Multi* for schema consistency)
MULTIPOINT_TYPE = pa.list_(_COORD_TYPE)
MULTILINESTRING_TYPE = pa.list_(pa.list_(_COORD_TYPE))
MULTIPOLYGON_TYPE = pa.list_(pa.list_(pa.list_(_COORD_TYPE)))

# Extension names
_EXT_NAMES = {
    "multipoint": "geoarrow.multipoint",
    "multilinestring": "geoarrow.multilinestring",
    "multipolygon": "geoarrow.multipolygon",
}


def convert_batch(
    batch: pa.RecordBatch,
    geo_fields: dict[str, dict[str, str]],
) -> pa.RecordBatch:
    """Convert WKB geometry columns in batch to native GeoArrow.

    Args:
        batch: Input RecordBatch with WKB binary geometry columns
        geo_fields: Geometry field metadata from X-Hugr-Geometry-Fields header
            e.g. {"geom": {"format": "wkb", "srid": "EPSG:4326"}}

    Returns:
        RecordBatch with geometry columns converted to native GeoArrow types.
        Non-geometry columns pass through unchanged.
    """
    if not geo_fields:
        return batch

    columns = []
    fields = []
    for i, field in enumerate(batch.schema):
        if field.name in geo_fields:
            fmt = geo_fields[field.name].get("format", "wkb").lower()
            if fmt in ("wkb", "geojson", "geojsonstring"):
                col = batch.column(i)
                converted, new_field = _convert_geo_column(col, field, geo_fields[field.name])
                if converted is not None:
                    columns.append(converted)
                    fields.append(new_field)
                    continue
        columns.append(batch.column(i))
        fields.append(field)

    return pa.record_batch(columns, schema=pa.schema(fields))


def _convert_geo_column(
    col: pa.Array,
    field: pa.Field,
    geo_meta: dict,
) -> tuple[Optional[pa.Array], Optional[pa.Field]]:
    """Convert a single WKB geometry column to native GeoArrow."""
    # Detect dominant geometry type from first non-null value
    geo_type = None
    for val in col:
        if val.is_valid:
            wkb = val.as_py()
            if isinstance(wkb, bytes) and len(wkb) >= 5:
                geo_type = _detect_type(wkb)
                break

    if geo_type is None:
        return None, None

    srid_str = geo_meta.get("srid", "EPSG:4326")

    if geo_type in (_WKB_POINT, _WKB_MULTIPOINT):
        arr = _build_multipoint_array(col)
        ext_name = _EXT_NAMES["multipoint"]
        arrow_type = MULTIPOINT_TYPE
    elif geo_type in (_WKB_LINESTRING, _WKB_MULTILINESTRING):
        arr = _build_multilinestring_array(col)
        ext_name = _EXT_NAMES["multilinestring"]
        arrow_type = MULTILINESTRING_TYPE
    elif geo_type in (_WKB_POLYGON, _WKB_MULTIPOLYGON):
        arr = _build_multipolygon_array(col)
        ext_name = _EXT_NAMES["multipolygon"]
        arrow_type = MULTIPOLYGON_TYPE
    else:
        return None, None

    new_field = pa.field(
        field.name,
        arrow_type,
        metadata={
            b"ARROW:extension:name": ext_name.encode(),
            b"ARROW:extension:metadata": f'{{"srid":{_parse_srid(srid_str)}}}'.encode(),
        },
    )
    return arr, new_field


def _detect_type(wkb: bytes) -> int:
    """Detect WKB geometry type from header."""
    byte_order = wkb[0]
    fmt = "<I" if byte_order == 1 else ">I"
    raw_type = struct.unpack_from(fmt, wkb, 1)[0]
    # Strip Z/M/ZM flags (types 1000+, 2000+, 3000+)
    return raw_type % 1000


def _parse_srid(srid_str: str) -> int:
    """Parse SRID from string like 'EPSG:4326' or '4326'."""
    s = srid_str.replace("EPSG:", "").strip()
    try:
        return int(s)
    except ValueError:
        return 4326


def _read_coords(wkb: bytes, offset: int, byte_order: int, n: int, dims: int) -> tuple[list, int]:
    """Read n coordinates, extracting only X,Y. Returns (coords_list, new_offset)."""
    fmt_char = "<" if byte_order == 1 else ">"
    coords = []
    for _ in range(n):
        x, y = struct.unpack_from(f"{fmt_char}dd", wkb, offset)
        coords.append([x, y])
        offset += dims * 8
    return coords, offset


def _dims_from_type(raw_type: int) -> int:
    """Get coordinate dimensions from raw WKB type."""
    if raw_type >= 3000:
        return 4  # ZM
    if raw_type >= 2000:
        return 3  # M
    if raw_type >= 1000:
        return 3  # Z
    return 2


def _parse_wkb_header(wkb: bytes, offset: int = 0) -> tuple[int, int, int, int]:
    """Parse WKB header. Returns (byte_order, base_type, dims, new_offset)."""
    byte_order = wkb[offset]
    fmt = "<I" if byte_order == 1 else ">I"
    raw_type = struct.unpack_from(fmt, wkb, offset + 1)[0]
    base_type = raw_type % 1000
    dims = _dims_from_type(raw_type)
    return byte_order, base_type, dims, offset + 5


def _parse_point(wkb: bytes, offset: int = 0) -> list | None:
    """Parse WKB Point → [x, y] or None if empty."""
    bo, _, dims, off = _parse_wkb_header(wkb, offset)
    coords, _ = _read_coords(wkb, off, bo, 1, dims)
    return coords[0] if coords else None


def _parse_linestring(wkb: bytes, offset: int = 0) -> tuple[list, int]:
    """Parse WKB LineString → list of [x, y], new_offset."""
    bo, _, dims, off = _parse_wkb_header(wkb, offset)
    fmt = "<I" if bo == 1 else ">I"
    n_points = struct.unpack_from(fmt, wkb, off)[0]
    off += 4
    coords, off = _read_coords(wkb, off, bo, n_points, dims)
    return coords, off


def _parse_polygon(wkb: bytes, offset: int = 0) -> tuple[list, int]:
    """Parse WKB Polygon → list of rings (each ring = list of [x,y]), new_offset."""
    bo, _, dims, off = _parse_wkb_header(wkb, offset)
    fmt = "<I" if bo == 1 else ">I"
    n_rings = struct.unpack_from(fmt, wkb, off)[0]
    off += 4
    rings = []
    for _ in range(n_rings):
        n_points = struct.unpack_from(fmt, wkb, off)[0]
        off += 4
        coords, off = _read_coords(wkb, off, bo, n_points, dims)
        rings.append(coords)
    return rings, off


def _build_multipoint_array(col: pa.Array) -> pa.Array:
    """Convert WKB column to GeoArrow MultiPoint array."""
    values = []
    for val in col:
        if not val.is_valid:
            values.append(None)
            continue
        wkb = val.as_py()
        bo, base_type, dims, off = _parse_wkb_header(wkb)
        fmt = "<I" if bo == 1 else ">I"
        if base_type == _WKB_POINT:
            coords, _ = _read_coords(wkb, off, bo, 1, dims)
            values.append(coords)
        elif base_type == _WKB_MULTIPOINT:
            n = struct.unpack_from(fmt, wkb, off)[0]
            off += 4
            points = []
            for _ in range(n):
                pt = _parse_point(wkb, off)
                if pt:
                    points.append(pt)
                off += 5 + dims * 8  # header + coords
            values.append(points)
        else:
            values.append(None)
    return pa.array(values, type=MULTIPOINT_TYPE)


def _build_multilinestring_array(col: pa.Array) -> pa.Array:
    """Convert WKB column to GeoArrow MultiLineString array."""
    values = []
    for val in col:
        if not val.is_valid:
            values.append(None)
            continue
        wkb = val.as_py()
        bo, base_type, dims, off = _parse_wkb_header(wkb)
        fmt = "<I" if bo == 1 else ">I"
        if base_type == _WKB_LINESTRING:
            coords, _ = _parse_linestring(wkb)
            values.append([coords])
        elif base_type == _WKB_MULTILINESTRING:
            n = struct.unpack_from(fmt, wkb, off)[0]
            off += 4
            lines = []
            for _ in range(n):
                coords, off = _parse_linestring(wkb, off)
                lines.append(coords)
            values.append(lines)
        else:
            values.append(None)
    return pa.array(values, type=MULTILINESTRING_TYPE)


def _build_multipolygon_array(col: pa.Array) -> pa.Array:
    """Convert WKB column to GeoArrow MultiPolygon array."""
    values = []
    for val in col:
        if not val.is_valid:
            values.append(None)
            continue
        wkb = val.as_py()
        bo, base_type, dims, off = _parse_wkb_header(wkb)
        fmt = "<I" if bo == 1 else ">I"
        if base_type == _WKB_POLYGON:
            rings, _ = _parse_polygon(wkb)
            values.append([rings])
        elif base_type == _WKB_MULTIPOLYGON:
            n = struct.unpack_from(fmt, wkb, off)[0]
            off += 4
            polygons = []
            for _ in range(n):
                rings, off = _parse_polygon(wkb, off)
                polygons.append(rings)
            values.append(polygons)
        else:
            values.append(None)
    return pa.array(values, type=MULTIPOLYGON_TYPE)
