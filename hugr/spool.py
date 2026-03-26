"""Arrow spool file management — write query results to temp files for Perspective viewer.

Spool files are Arrow IPC streams stored in /tmp/hugr-client/. They are:
- Written lazily on first Jupyter display (_repr_mimebundle_)
- Served by hugr-perspective-viewer spool proxy
- Cleaned up by __del__ on GC or TTL expiry

In headless/ETL mode, spool files are never created.
"""

import os
import tempfile
import time
import uuid

import pyarrow as pa
import pyarrow.ipc as ipc

from .arrow_flatten import flatten_batch, needs_flatten

SPOOL_DIR_NAME = "hugr-client"
DEFAULT_TTL = 24 * 3600  # 24 hours

# Map Hugr geometry format to Arrow extension name
_GEO_FORMAT_TO_EXT = {
    "wkb": "ogc.wkb",
    "geojson": "ogc.wkb",  # stored as WKB in Arrow
    "geojsonstring": "ogc.wkb",
    "h3cell": "",  # H3 cells are strings, no extension
}


def spool_base() -> str:
    """Return spool directory path."""
    return os.path.join(tempfile.gettempdir(), SPOOL_DIR_NAME)


def write_spool(
    batches: list[pa.RecordBatch],
    schema: pa.Schema,
    geom_fields: dict[str, dict[str, str]] | None = None,
) -> str:
    """Write Arrow batches to spool file. Returns spool ID.

    Applies flatten before writing (struct → dot columns, list → JSON string).
    Adds Arrow extension metadata for geometry columns so spool proxy can detect them.
    """
    spool_id = uuid.uuid4().hex[:16]
    base = spool_base()
    os.makedirs(base, exist_ok=True)
    path = os.path.join(base, f"{spool_id}.arrow")

    # Flatten if needed (Perspective expects flat schema)
    do_flatten = needs_flatten(schema)
    if do_flatten:
        batches = [flatten_batch(b) for b in batches]
        if batches:
            schema = batches[0].schema
        else:
            from .arrow_flatten import flatten_schema
            schema = flatten_schema(schema)

    # Convert WKB geometry → native GeoArrow (same as Go kernels)
    if geom_fields:
        from .geoarrow import convert_batch as geo_convert
        batches = [geo_convert(b, geom_fields) for b in batches]
        if batches:
            schema = batches[0].schema

    tmp_path = path + ".tmp"
    try:
        with pa.OSFile(tmp_path, "wb") as f:
            writer = ipc.new_stream(
                f, schema,
                options=ipc.IpcWriteOptions(compression="lz4"),
            )
            for batch in batches:
                writer.write_batch(batch)
            writer.close()
        os.replace(tmp_path, path)  # atomic
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise

    return spool_id


def _add_geo_metadata(schema: pa.Schema, geom_fields: dict) -> pa.Schema:
    """Add ARROW:extension:name metadata to geometry columns."""
    fields = []
    for field in schema:
        if field.name in geom_fields:
            fmt = geom_fields[field.name].get("format", "wkb").lower()
            ext_name = _GEO_FORMAT_TO_EXT.get(fmt, "ogc.wkb")
            if ext_name:
                meta = dict(field.metadata or {})
                meta[b"ARROW:extension:name"] = ext_name.encode()
                field = field.with_metadata(meta)
        fields.append(field)
    return pa.schema(fields)


def delete_spool(spool_id: str):
    """Delete a spool file."""
    path = os.path.join(spool_base(), f"{spool_id}.arrow")
    try:
        os.remove(path)
    except OSError:
        pass


def cleanup_stale(max_age: int = DEFAULT_TTL):
    """Remove spool files older than max_age seconds."""
    base = spool_base()
    if not os.path.isdir(base):
        return
    now = time.time()
    for f in os.scandir(base):
        if f.name.endswith(".arrow") and (now - f.stat().st_mtime) > max_age:
            try:
                os.remove(f.path)
            except OSError:
                pass
