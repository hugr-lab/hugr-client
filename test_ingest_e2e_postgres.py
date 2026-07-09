"""
End-to-end test for HugrClient.ingest against a running hugr server.

Usage:
    python test_ingest_e2e_postgres.py [URL]

Default URL: http://localhost:15055/ipc

The script registers (idempotently) a postgres data source named `pg_ingest`
exposing an `events` table, then ingests data three ways — a pandas DataFrame,
a pyarrow.Table, and a 50k-row bulk DataFrame — and verifies the results both
from the ingest response (server-side RowsAffected) and by reading rows back
through hugr itself (no direct DB access needed).

Environment:
    HUGR_E2E_URL          server /ipc URL (overridden by argv[1])
    HUGR_E2E_PG_DSN       postgres DSN the *server* uses to ATTACH the source
                          (default: postgres://test:test@127.0.0.1:5437/ingestdb)
    HUGR_E2E_SCHEMA_DIR   localFS catalog dir with events schema.graphql
                          (default: sibling hugr-query-engine ingest testdata)

This mirrors the convention of test_subscription.py: a standalone script run
against a live server. run_e2e_postgres.sh orchestrates postgres + dev-server + this
script for a one-command reproduction.
"""
import datetime as dt
import json
import os
import subprocess
import sys
import time

import pandas as pd
import pyarrow as pa

import hugr


DEFAULT_URL = "http://localhost:15055/ipc"
DEFAULT_DSN = "postgres://test:test@127.0.0.1:5437/ingestdb"


def _default_schema_dir() -> str:
    here = os.path.dirname(os.path.abspath(__file__))
    # hugr-client and hugr-query-engine are sibling checkouts.
    return os.path.normpath(os.path.join(
        here, "..", "hugr-query-engine",
        "integration-test", "ingest-postgres", "testdata", "schemas", "pg_ingest",
    ))


def _query_url(ipc_url: str) -> str:
    return ipc_url[:-len("/ipc")] + "/query" if ipc_url.endswith("/ipc") else ipc_url


def _gql(query_url: str, query: str, variables: dict = None):
    import requests
    resp = requests.post(query_url, json={"query": query, "variables": variables or {}})
    resp.raise_for_status()
    body = resp.json()
    if body.get("errors"):
        raise RuntimeError(f"graphql errors: {body['errors']}")
    return body.get("data")


def ensure_source(query_url: str, dsn: str, schema_dir: str):
    """Register + load pg_ingest if it isn't already present."""
    print("=== ensure pg_ingest data source ===")
    if not os.path.isdir(schema_dir):
        raise SystemExit(f"schema dir not found: {schema_dir} (set HUGR_E2E_SCHEMA_DIR)")

    # Register (ignore "already exists").
    try:
        _gql(
            query_url,
            "mutation($data: core_data_sources_mut_input_data!){ "
            "core { insert_data_sources(data: $data){ name } } }",
            {"data": {
                "name": "pg_ingest", "type": "postgres", "prefix": "pg_ingest",
                "as_module": True, "path": dsn,
                "catalogs": [{"name": "pg_ingest", "type": "localFS", "path": schema_dir}],
            }},
        )
        print("  registered pg_ingest")
    except RuntimeError as e:
        if "exist" in str(e).lower() or "duplicate" in str(e).lower():
            print("  pg_ingest already registered")
        else:
            raise

    # Load (idempotent on the server).
    data = _gql(
        query_url,
        'mutation { function { core { load_data_source(name: "pg_ingest"){ success message } } } }',
    )
    res = data["function"]["core"]["load_data_source"]
    if not res["success"]:
        raise SystemExit(f"load_data_source failed: {res['message']}")
    print(f"  loaded: {res['message']}")


def truncate(query_url: str):
    """Clear this test's rows so re-runs against a long-lived server stay
    idempotent. Scoped to the e2e- name prefix (an empty filter is rejected
    by the planner)."""
    # Note: affected_rows is unreliable through the postgres extension's delete
    # path (often reports 0 even when rows are removed), so we don't assert on
    # it — the bulk test's prefix-scoped aggregate count is the real check.
    try:
        _gql(query_url,
             'mutation { pg_ingest { delete_events('
             'filter: {name: {like: "e2e-%"}}) { success } } }')
        print("  cleared prior e2e- rows")
    except RuntimeError as e:
        print(f"  [warn] truncate skipped: {e}")


def test_dataframe(client, url: str):
    print("=== Test: pandas.DataFrame ===")
    df = pd.DataFrame({
        "name": ["e2e-df-a", "e2e-df-b", "e2e-df-c"],
        "value": [1.5, 2.5, 3.5],
        "is_active": [True, False, True],
        "payload": ['{"k":"v"}', None, '{"x":1}'],
        "created_at": pd.to_datetime(
            ["2026-05-21T12:00:00", "2026-05-21T12:00:01", "2026-05-21T12:00:02"]
        ),
    })
    res = client.ingest("pg_ingest.events", df)
    print(f"  result: {res}")
    assert res["inserted"] == 3, res
    print("  PASS\n")


def test_arrow_table(client, url: str):
    print("=== Test: pyarrow.Table ===")
    tbl = pa.table({
        "name": ["e2e-pa-1", "e2e-pa-2"],
        "value": [10.0, 20.0],
        "is_active": [True, True],
    })
    res = client.ingest("pg_ingest.events", tbl)
    print(f"  result: {res}")
    assert res["inserted"] == 2, res
    print("  PASS\n")


def test_bulk(client, url: str, query_url: str):
    print("=== Test: bulk 50 * 1000 DataFrame ===")
    n = 50 * 1000
    base = dt.datetime(2026, 5, 21)
    bulk = pd.DataFrame({
        "name": [f"e2e-bulk-{i:06d}" for i in range(n)],
        "value": [i * 0.5 for i in range(n)],
        "is_active": [i % 2 == 0 for i in range(n)],
        "payload": [None if i % 5 == 0 else f'{{"row":{i}}}' for i in range(n)],
        "created_at": [base + dt.timedelta(milliseconds=i) for i in range(n)],
    })
    start = time.time()
    res = client.ingest("pg_ingest.events", bulk)
    elapsed = time.time() - start
    print(f"  result: {res} in {elapsed*1000:.1f}ms ({n/elapsed:,.0f} rows/s)")
    assert res["inserted"] == n, res

    # Verify by reading back through hugr (not the ingest echo): the first
    # three bulk rows ordered by value.
    data = _gql(
        query_url,
        'query { pg_ingest { events('
        '  filter: {name: {like: "e2e-bulk-%"}}, '
        '  order_by: [{field: "value", direction: ASC}], limit: 3'
        ') { name value is_active } } }',
    )
    rows = data["pg_ingest"]["events"]
    print(f"  read-back first 3: {rows}")
    assert [r["name"] for r in rows] == ["e2e-bulk-000000", "e2e-bulk-000001", "e2e-bulk-000002"], rows
    assert [r["value"] for r in rows] == [0.0, 0.5, 1.0], rows

    # Aggregate count of the bulk rows, again through hugr.
    agg = _gql(
        query_url,
        'query { pg_ingest { events_aggregation('
        '  filter: {name: {like: "e2e-bulk-%"}}'
        ') { _rows_count } } }',
    )
    cnt = agg["pg_ingest"]["events_aggregation"]["_rows_count"]
    print(f"  read-back aggregate count: {cnt}")
    assert cnt == n, f"expected {n}, got {cnt}"
    print("  PASS\n")


def test_streaming_memory(url: str, query_url: str):
    """Prove ingest streams a large RecordBatchReader with bounded memory.

    Runs the ingest in a fresh subprocess (so peak RSS reflects only that work)
    that builds a lazy reader producing ~DATA_MB of Arrow data and reports the
    RSS it grew by. If the client buffered the whole stream (the old read_all +
    full-buffer path) the delta would be >= the data size; streaming keeps it
    to roughly one batch.
    """
    print("=== Test: streaming memory (lazy RecordBatchReader) ===")
    num_batches = 128
    rows_per_batch = 256
    payload_bytes = 4096
    approx_total_mb = num_batches * rows_per_batch * payload_bytes / (1024 * 1024)
    expected_rows = num_batches * rows_per_batch

    worker = os.path.join(os.path.dirname(os.path.abspath(__file__)), "e2e_ingest_memory_worker.py")
    proc = subprocess.run(
        [sys.executable, worker, url, "pg_ingest.events",
         str(num_batches), str(rows_per_batch), str(payload_bytes)],
        capture_output=True, text=True,
    )
    if proc.returncode != 0:
        raise SystemExit(f"mem worker failed:\nSTDOUT:{proc.stdout}\nSTDERR:{proc.stderr}")
    stats = json.loads(proc.stdout.strip().splitlines()[-1])
    print(f"  worker stats: {stats}")

    assert stats["inserted"] == expected_rows, stats

    # Verify the rows actually landed (count through hugr).
    agg = _gql(
        query_url,
        'query { pg_ingest { events_aggregation('
        '  filter: {name: {like: "mem-%"}}'
        ') { _rows_count } } }',
    )
    cnt = agg["pg_ingest"]["events_aggregation"]["_rows_count"]
    assert cnt == expected_rows, f"expected {expected_rows} in pg, got {cnt}"

    # The streaming assertion: memory growth must be a small fraction of the
    # total streamed data. One batch is ~1MB here; allow generous headroom for
    # pyarrow/requests scratch. A buffering implementation would grow by
    # >= approx_total_mb (and ~2-3x with the old to_pybytes copy).
    threshold_mb = max(64.0, approx_total_mb * 0.25)
    assert stats["delta_mb"] < threshold_mb, (
        f"memory grew {stats['delta_mb']}MB streaming ~{approx_total_mb:.0f}MB; "
        f"expected < {threshold_mb:.0f}MB — looks like the stream was buffered, "
        f"not streamed"
    )
    print(f"  OK: grew {stats['delta_mb']}MB streaming ~{approx_total_mb:.0f}MB "
          f"(threshold {threshold_mb:.0f}MB)")
    print("  PASS\n")


def main():
    url = sys.argv[1] if len(sys.argv) > 1 else os.environ.get("HUGR_E2E_URL", DEFAULT_URL)
    dsn = os.environ.get("HUGR_E2E_PG_DSN", DEFAULT_DSN)
    schema_dir = os.environ.get("HUGR_E2E_SCHEMA_DIR", _default_schema_dir())
    query_url = _query_url(url)

    print(f"Testing Python ingest client against {url}\n")

    ensure_source(query_url, dsn, schema_dir)
    truncate(query_url)
    print()

    client = hugr.HugrClient(url=url)
    test_dataframe(client, url)
    test_arrow_table(client, url)
    test_bulk(client, url, query_url)
    test_streaming_memory(url, query_url)

    print("All ingest e2e tests passed!")


if __name__ == "__main__":
    main()
