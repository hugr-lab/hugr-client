"""Offline unit tests for HugrClient.ingest.

These don't need a running hugr server — they stub requests.post and assert
on the request the client builds (URL, headers, Arrow IPC body) and on the
parsing of the JSON response. Run with:

    .venv-test/bin/python -m pytest test_ingest.py -v
    # or without pytest:
    .venv-test/bin/python test_ingest.py
"""
import io
import json
from unittest import mock

import pyarrow as pa
import pandas as pd

import hugr
from hugr.client import HugrClient, _to_batches


def _fake_response(status=200, payload=None, text=""):
    resp = mock.Mock()
    resp.status_code = status
    resp.json.return_value = payload if payload is not None else {}
    resp.text = text
    resp.raise_for_status.return_value = None
    return resp


def _decode_ingest_body(call) -> pa.Table:
    """Extract the Arrow IPC stream POSTed by the client and decode it. The
    body is a generator (streaming/chunked), so join its chunks first."""
    body = call.kwargs.get("data")
    assert body is not None, "request body (data=) must be set"
    raw = body if isinstance(body, (bytes, bytearray)) else b"".join(body)
    reader = pa.ipc.open_stream(io.BytesIO(raw))
    return reader.read_all()


# --- URL building ----------------------------------------------------------

def test_ingest_url_from_ipc_base():
    c = HugrClient(url="http://localhost:15000/ipc")
    assert c._ingest_url("pg.events") == \
        "http://localhost:15000/ipc/ingest?data_object=pg.events"


def test_ingest_url_from_bare_base():
    c = HugrClient(url="http://localhost:15000")
    assert c._ingest_url("pg.events") == \
        "http://localhost:15000/ipc/ingest?data_object=pg.events"


def test_ingest_url_already_ingest():
    c = HugrClient(url="http://localhost:15000/ipc/ingest")
    assert c._ingest_url("pg.events") == \
        "http://localhost:15000/ipc/ingest?data_object=pg.events"


def test_ingest_url_encodes_dotted_path():
    c = HugrClient(url="http://localhost:15000/ipc")
    url = c._ingest_url("pg_store.public.events")
    assert "data_object=pg_store.public.events" in url


# --- type coercion ---------------------------------------------------------

def _materialize(schema, batches):
    return pa.Table.from_batches(list(batches), schema=schema)


def test_to_batches_from_dataframe_drops_index():
    df = pd.DataFrame({"name": ["a", "b"], "value": [1.0, 2.0]})
    schema, batches = _to_batches(df)
    # No __index_level_0__ leaking through.
    assert schema.names == ["name", "value"]
    assert _materialize(schema, batches).num_rows == 2


def test_to_batches_from_table():
    tbl = pa.table({"x": [1, 2, 3]})
    schema, batches = _to_batches(tbl)
    assert schema == tbl.schema
    assert _materialize(schema, batches).num_rows == 3


def test_to_batches_from_record_batch():
    rb = pa.record_batch({"x": [1, 2]})
    schema, batches = _to_batches(rb)
    assert _materialize(schema, batches).num_rows == 2


def test_to_batches_from_record_batch_reader_is_lazy():
    schema = pa.schema([("x", pa.int64())])
    batches = [pa.record_batch({"x": [1, 2]}, schema=schema),
               pa.record_batch({"x": [3]}, schema=schema)]
    rbr = pa.RecordBatchReader.from_batches(schema, batches)
    out_schema, out_batches = _to_batches(rbr)
    # Crucial: the reader is returned as-is (lazy), NOT drained via read_all().
    assert out_batches is rbr
    assert out_schema == schema
    # Consuming it still yields all rows.
    assert _materialize(out_schema, out_batches).num_rows == 3


def test_to_batches_rejects_unsupported():
    try:
        _to_batches([1, 2, 3])
    except TypeError as e:
        assert "Unsupported data type" in str(e)
    else:
        raise AssertionError("expected TypeError")


# --- request construction & response parsing -------------------------------

def test_ingest_posts_arrow_stream_and_parses_result():
    c = HugrClient(url="http://localhost:15000/ipc")
    df = pd.DataFrame({
        "name": ["alpha", "beta", "gamma"],
        "value": [1.5, 2.5, 3.5],
        "is_active": [True, False, True],
    })

    payload = {"data_object": "pg.events", "inserted": 3,
               "columns": ["name", "value", "is_active"]}
    with mock.patch("hugr.client.requests.post",
                    return_value=_fake_response(payload=payload)) as post:
        res = c.ingest("pg.events", df)

    assert res == payload
    assert post.call_count == 1
    call = post.call_args

    # URL + content type
    assert call.args[0] == "http://localhost:15000/ipc/ingest?data_object=pg.events"
    assert call.kwargs["headers"]["Content-Type"] == \
        "application/vnd.apache.arrow.stream"

    # Body must be a valid Arrow IPC stream with our data.
    tbl = _decode_ingest_body(call)
    assert tbl.num_rows == 3
    assert tbl.schema.names == ["name", "value", "is_active"]
    assert tbl.column("name").to_pylist() == ["alpha", "beta", "gamma"]


def test_ingest_sends_auth_and_role_headers():
    c = HugrClient(url="http://localhost:15000/ipc", api_key="secret", role="admin")
    df = pd.DataFrame({"x": [1]})
    with mock.patch("hugr.client.requests.post",
                    return_value=_fake_response(payload={"inserted": 1})) as post:
        c.ingest("pg.events", df)
    headers = post.call_args.kwargs["headers"]
    assert headers["X-Hugr-Api-Key"] == "secret"
    assert headers["X-Hugr-Role"] == "admin"


def test_ingest_empty_data_object_raises():
    c = HugrClient(url="http://localhost:15000/ipc")
    try:
        c.ingest("", pd.DataFrame({"x": [1]}))
    except ValueError as e:
        assert "data_object" in str(e)
    else:
        raise AssertionError("expected ValueError")


def test_ingest_server_error_surfaces_message():
    c = HugrClient(url="http://localhost:15000/ipc")
    df = pd.DataFrame({"not_a_column": [1]})
    err = _fake_response(status=400, payload={"error": "column \"not_a_column\" is not defined"})
    with mock.patch("hugr.client.requests.post", return_value=err):
        try:
            c.ingest("pg.events", df)
        except ValueError as e:
            assert "not_a_column" in str(e)
            assert "400" in str(e)
        else:
            raise AssertionError("expected ValueError")


def test_ingest_403_raises_permission_error():
    c = HugrClient(url="http://localhost:15000/ipc")
    with mock.patch("hugr.client.requests.post",
                    return_value=_fake_response(status=403)):
        try:
            c.ingest("pg.events", pd.DataFrame({"x": [1]}))
        except PermissionError:
            pass
        else:
            raise AssertionError("expected PermissionError")


def test_module_level_ingest():
    df = pd.DataFrame({"x": [1, 2]})
    with mock.patch("hugr.client.requests.post",
                    return_value=_fake_response(payload={"inserted": 2})) as post:
        res = hugr.ingest("pg.events", df, url="http://localhost:15000/ipc")
    assert res == {"inserted": 2}
    assert post.call_count == 1


if __name__ == "__main__":
    import sys
    failures = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print(f"PASS {name}")
            except Exception as e:  # noqa: BLE001
                failures += 1
                print(f"FAIL {name}: {type(e).__name__}: {e}")
    print(f"\n{'OK' if failures == 0 else 'FAILED'}: {failures} failure(s)")
    sys.exit(1 if failures else 0)
