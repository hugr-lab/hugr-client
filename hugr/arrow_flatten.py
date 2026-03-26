"""Arrow schema/batch flattening — Python port of Go flatten logic.

Converts complex Arrow types to flat columns for Perspective viewer:
- struct → dot-separated child columns (recursive)
- list/large_list/fixed_size_list → JSON string
- map → JSON string
- dense_union/sparse_union → JSON string

Geometry metadata (ARROW:extension:name) is preserved through flattening.
"""

import json

import pyarrow as pa


def needs_flatten(schema: pa.Schema) -> bool:
    """Check if any column needs flattening."""
    for field in schema:
        if _is_complex(field.type):
            return True
    return False


def flatten_schema(schema: pa.Schema) -> pa.Schema:
    """Flatten schema: structs expand to dot-separated fields, complex types become string."""
    fields = []
    for field in schema:
        fields.extend(_flatten_field(field, ""))
    return pa.schema(fields)


def flatten_batch(batch: pa.RecordBatch) -> pa.RecordBatch:
    """Flatten a RecordBatch. Returns original if no flattening needed."""
    if not needs_flatten(batch.schema):
        return batch
    out_schema = flatten_schema(batch.schema)
    columns = []
    for i, field in enumerate(batch.schema):
        col = batch.column(i)
        columns.extend(_flatten_column(col, field, ""))
    return pa.record_batch(columns, schema=out_schema)


def _is_complex(t: pa.DataType) -> bool:
    """Check if type needs flattening."""
    return (
        pa.types.is_struct(t)
        or pa.types.is_list(t)
        or pa.types.is_large_list(t)
        or pa.types.is_fixed_size_list(t)
        or pa.types.is_map(t)
        or pa.types.is_union(t)
    )


def _flatten_field(field: pa.Field, prefix: str) -> list[pa.Field]:
    """Flatten a single field definition. Returns list of output fields."""
    name = f"{prefix}.{field.name}" if prefix else field.name

    if pa.types.is_struct(field.type):
        result = []
        for child in field.type:
            result.extend(_flatten_field(child, name))
        return result

    if _is_complex(field.type):
        # Complex types → JSON string, preserve metadata (geometry etc.)
        return [pa.field(name, pa.string(), metadata=field.metadata)]

    return [pa.field(name, field.type, nullable=field.nullable, metadata=field.metadata)]


def _flatten_column(col: pa.Array, field: pa.Field, prefix: str) -> list[pa.Array]:
    """Flatten a single column. Returns list of output arrays."""
    if pa.types.is_struct(col.type):
        return _flatten_struct(col, field, prefix)

    if _is_complex(col.type):
        return [_to_json_string(col)]

    return [col]


def _flatten_struct(col: pa.StructArray, field: pa.Field, prefix: str) -> list[pa.Array]:
    """Expand struct column into child columns, propagating parent nulls."""
    name = f"{prefix}.{field.name}" if prefix else field.name
    result = []

    for i, child_field in enumerate(field.type):
        child = col.field(i)

        # Propagate parent struct nulls to children
        if col.null_count > 0:
            parent_valid = col.is_valid()
            if child.null_count == 0:
                child = pa.compute.if_else(parent_valid, child, None)
            else:
                child_valid = child.is_valid()
                combined = pa.compute.and_(parent_valid, child_valid)
                child = pa.compute.if_else(combined, child, None)

        result.extend(_flatten_column(child, child_field, name))

    return result


def _to_json_string(col: pa.Array) -> pa.StringArray:
    """Convert complex array to JSON string array."""
    values = []
    for val in col.to_pylist():
        if val is None:
            values.append(None)
        else:
            values.append(json.dumps(val, default=str, ensure_ascii=False))
    return pa.array(values, type=pa.string())
