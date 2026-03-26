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


def spool_base() -> str:
    """Return spool directory path."""
    return os.path.join(tempfile.gettempdir(), SPOOL_DIR_NAME)


def write_spool(batches: list[pa.RecordBatch], schema: pa.Schema) -> str:
    """Write Arrow batches to spool file. Returns spool ID.

    Applies flatten before writing (struct → dot columns, list → JSON string).
    Geometry columns stored as-is — spool proxy handles replacement at streaming time.
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
