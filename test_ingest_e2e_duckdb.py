"""
End-to-end test for HugrClient.ingest against a DuckDB-backed data source.

Usage:
    python test_ingest_e2e_duckdb.py [URL]

Default URL: http://localhost:15055/ipc

Mirrors test_ingest_e2e_postgres.py — same three ingest paths
(pandas DataFrame, pyarrow.Table, bulk 50k via RecordBatchReader) plus the
streaming-memory subprocess test. Differences from the Postgres variant:
  - the backing store is a local .duckdb file in a tempdir (no docker);
  - the file is seeded with `CREATE SEQUENCE + CREATE TABLE events` via the
    Python `duckdb` package before hugr ATTACHes it;
  - verification happens BOTH through hugr query AND through a direct
    READ_ONLY duckdb.connect() — the latter is the independent channel
    that proves the server didn't just echo back inserted counts.

Environment:
    HUGR_E2E_URL          server /ipc URL (overridden by argv[1])
    HUGR_E2E_DUCKDB_PATH  path to the .duckdb file the test will create + ATTACH
                          (default: tempdir/test.duckdb)
    HUGR_E2E_SCHEMA_DIR   localFS catalog dir with events schema.graphql
                          (default: sibling hugr-query-engine ingest-duckdb testdata)

run_e2e_duckdb.sh orchestrates dev-server + this script for one-command repro.
"""
import datetime as dt
import json
import os
import subprocess
import sys
import tempfile
import time

import duckdb
import pandas as pd
import pyarrow as pa

import hugr


DEFAULT_URL = "http://localhost:15055/ipc"
DS_NAME = "duck_ingest"
DATA_OBJECT = f"{DS_NAME}.events"


def _default_schema_dir() -> str:
    here = os.path.dirname(os.path.abspath(__file__))
    return os.path.normpath(os.path.join(
        here, "..", "hugr-query-engine",
        "integration-test", "ingest-duckdb", "testdata", "schemas", "duck_ingest",
    ))


def _default_db_path() -> str:
    # A per-run tempdir keeps tests isolated. The path is reported on stdout so
    # the user can inspect it if a test fails. tempdir auto-cleanup is opt-in;
    # we leave the dir behind so the file is inspectable post-mortem (it's in
    # /tmp anyway).
    d = tempfile.mkdtemp(prefix="hugr-duckdb-e2e-")
    return os.path.join(d, "test.duckdb")


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


def seed_database(path: str):
    """Create the events table in the .duckdb file before hugr opens it."""
    print(f"=== seed .duckdb file at {path} ===")
    conn = duckdb.connect(path)
    try:
        conn.execute("""
            CREATE SEQUENCE IF NOT EXISTS events_id_seq;
            CREATE TABLE IF NOT EXISTS events (
                id BIGINT PRIMARY KEY DEFAULT nextval('events_id_seq'),
                name VARCHAR NOT NULL,
                value DOUBLE NOT NULL,
                is_active BOOLEAN NOT NULL DEFAULT true,
                payload JSON,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
        """)
        print("  table events ready")
    finally:
        conn.close()  # Must close before hugr ATTACHes (file write-lock).


def ensure_source(query_url: str, db_path: str, schema_dir: str):
    """Register + load the DuckDB data source on a running hugr server."""
    print("=== ensure duck_ingest data source ===")
    if not os.path.isdir(schema_dir):
        raise SystemExit(f"schema dir not found: {schema_dir} (set HUGR_E2E_SCHEMA_DIR)")

    try:
        _gql(
            query_url,
            "mutation($data: core_data_sources_mut_input_data!){ "
            "core { insert_data_sources(data: $data){ name } } }",
            {"data": {
                "name": DS_NAME, "type": "duckdb", "prefix": DS_NAME,
                "as_module": True, "path": db_path,
                "catalogs": [{"name": DS_NAME, "type": "localFS", "path": schema_dir}],
            }},
        )
        print(f"  registered {DS_NAME}")
    except RuntimeError as e:
        if "exist" in str(e).lower() or "duplicate" in str(e).lower():
            print(f"  {DS_NAME} already registered")
        else:
            raise

    data = _gql(
        query_url,
        'mutation($name: String!) { function { core { load_data_source(name: $name){ success message } } } }',
        {"name": DS_NAME},
    )
    res = data["function"]["core"]["load_data_source"]
    if not res["success"]:
        raise SystemExit(f"load_data_source failed: {res['message']}")
    print(f"  loaded: {res['message']}")


def cleanup_source(query_url: str):
    """Unload the data source on test completion so DETACH releases the file."""
    try:
        _gql(query_url,
             'mutation($name: String!, $hard: Boolean) { function { core { '
             'unload_data_source(name: $name, hard: $hard){ success } } } }',
             {"name": DS_NAME, "hard": True})
        print("  unloaded duck_ingest")
    except RuntimeError as e:
        print(f"  [warn] unload skipped: {e}")


def post_unload_verify(db_path: str, expected_mem_rows: int):
    """Open the .duckdb file independently (after dev-server released its
    write lock via DETACH) and assert all suite totals are present. This is
    the cross-process independent verification — proves data actually landed
    in the file, not just in hugr's in-memory view.

    DuckDB file locking is per-process; while dev-server holds the file as a
    writer, no external process can open it (even RO). We can only do this
    check AFTER cleanup_source has run unload (→ DETACH → lock released).
    """
    print("=== post-unload RO verification (independent channel) ===")
    ro = duckdb.connect(db_path, read_only=True)
    try:
        checks = [
            ("dk-df-%",   3,                  "DataFrame test rows"),
            ("dk-pa-%",   2,                  "pyarrow.Table test rows"),
            ("dk-bulk-%", 50_000,             "bulk test rows"),
            ("mem-%",     expected_mem_rows,  "memory test rows"),
        ]
        for pattern, expected, desc in checks:
            n = ro.execute(
                "SELECT COUNT(*) FROM events WHERE name LIKE ?", [pattern]
            ).fetchone()[0]
            assert n == expected, (
                f"{desc}: expected {expected} rows in file, got {n}")
            print(f"  {desc}: {n} rows ✓")
    finally:
        ro.close()
    print("  PASS\n")


# --- Tests ----------------------------------------------------------------

def test_dataframe(client, query_url: str):
    print("=== Test: pandas.DataFrame ===")
    df = pd.DataFrame({
        "name": ["dk-df-a", "dk-df-b", "dk-df-c"],
        "value": [1.5, 2.5, 3.5],
        "is_active": [True, False, True],
        "payload": ['{"k":"v"}', None, '{"x":1}'],
        "created_at": pd.to_datetime(
            ["2026-05-21T12:00:00", "2026-05-21T12:00:01", "2026-05-21T12:00:02"]
        ),
    })
    res = client.ingest(DATA_OBJECT, df)
    print(f"  result: {res}")
    assert res["inserted"] == 3, res

    # Per-test verification through hugr (the only available channel while
    # dev-server holds the .duckdb file lock — see post_unload_verify for the
    # independent file check that runs at suite end).
    agg = _gql(
        query_url,
        'query { duck_ingest { events_aggregation('
        '  filter: {name: {like: "dk-df-%"}}'
        ') { _rows_count } } }',
    )
    cnt = agg["duck_ingest"]["events_aggregation"]["_rows_count"]
    assert cnt == 3, f"expected 3 dk-df rows via hugr, got {cnt}"
    print(f"  hugr verifier: {cnt} dk-df rows")
    print("  PASS\n")


def test_arrow_table(client, query_url: str):
    print("=== Test: pyarrow.Table ===")
    tbl = pa.table({
        "name": ["dk-pa-1", "dk-pa-2"],
        "value": [10.0, 20.0],
        "is_active": [True, True],
    })
    res = client.ingest(DATA_OBJECT, tbl)
    print(f"  result: {res}")
    assert res["inserted"] == 2, res

    agg = _gql(
        query_url,
        'query { duck_ingest { events_aggregation('
        '  filter: {name: {like: "dk-pa-%"}}'
        ') { _rows_count } } }',
    )
    cnt = agg["duck_ingest"]["events_aggregation"]["_rows_count"]
    assert cnt == 2, f"expected 2 dk-pa rows via hugr, got {cnt}"
    print(f"  hugr verifier: {cnt} dk-pa rows")
    print("  PASS\n")


def test_bulk(client, db_path: str, query_url: str):
    print("=== Test: bulk 50 * 1000 DataFrame ===")
    n = 50 * 1000
    base = dt.datetime(2026, 5, 21)
    bulk = pd.DataFrame({
        "name": [f"dk-bulk-{i:06d}" for i in range(n)],
        "value": [i * 0.5 for i in range(n)],
        "is_active": [i % 2 == 0 for i in range(n)],
        "payload": [None if i % 5 == 0 else f'{{"row":{i}}}' for i in range(n)],
        "created_at": [base + dt.timedelta(milliseconds=i) for i in range(n)],
    })
    start = time.time()
    res = client.ingest(DATA_OBJECT, bulk)
    elapsed = time.time() - start
    print(f"  result: {res} in {elapsed*1000:.1f}ms ({n/elapsed:,.0f} rows/s)")
    assert res["inserted"] == n, res

    # 1) Read-back through hugr (the GraphQL channel): first three bulk rows.
    data = _gql(
        query_url,
        'query { duck_ingest { events('
        '  filter: {name: {like: "dk-bulk-%"}}, '
        '  order_by: [{field: "value", direction: ASC}], limit: 3'
        ') { name value is_active } } }',
    )
    rows = data["duck_ingest"]["events"]
    print(f"  read-back (hugr) first 3: {rows}")
    assert [r["name"] for r in rows] == ["dk-bulk-000000", "dk-bulk-000001", "dk-bulk-000002"], rows
    assert [r["value"] for r in rows] == [0.0, 0.5, 1.0], rows

    # 2) Aggregate count through hugr.
    agg = _gql(
        query_url,
        'query { duck_ingest { events_aggregation('
        '  filter: {name: {like: "dk-bulk-%"}}'
        ') { _rows_count } } }',
    )
    cnt_hugr = agg["duck_ingest"]["events_aggregation"]["_rows_count"]
    print(f"  read-back (hugr) aggregate count: {cnt_hugr}")
    assert cnt_hugr == n, f"expected {n}, got {cnt_hugr}"

    print("  PASS\n")


def test_streaming_memory(url: str, query_url: str, db_path: str):
    """Prove ingest streams a large RecordBatchReader with bounded memory.

    Runs the ingest in a fresh subprocess (so peak RSS reflects only that
    work) that builds a lazy reader producing ~128MB of Arrow data and
    reports the RSS it grew by. Mirror of the Postgres variant.
    """
    print("=== Test: streaming memory (lazy RecordBatchReader) ===")
    num_batches = 128
    rows_per_batch = 256
    payload_bytes = 4096
    approx_total_mb = num_batches * rows_per_batch * payload_bytes / (1024 * 1024)
    expected_rows = num_batches * rows_per_batch

    worker = os.path.join(os.path.dirname(os.path.abspath(__file__)), "mem_ingest_worker.py")
    proc = subprocess.run(
        [sys.executable, worker, url, DATA_OBJECT,
         str(num_batches), str(rows_per_batch), str(payload_bytes)],
        capture_output=True, text=True,
    )
    if proc.returncode != 0:
        raise SystemExit(f"mem worker failed:\nSTDOUT:{proc.stdout}\nSTDERR:{proc.stderr}")
    stats = json.loads(proc.stdout.strip().splitlines()[-1])
    print(f"  worker stats: {stats}")

    assert stats["inserted"] == expected_rows, stats

    # Verify the rows actually landed — through hugr.
    agg = _gql(
        query_url,
        'query { duck_ingest { events_aggregation('
        '  filter: {name: {like: "mem-%"}}'
        ') { _rows_count } } }',
    )
    cnt = agg["duck_ingest"]["events_aggregation"]["_rows_count"]
    assert cnt == expected_rows, f"expected {expected_rows} in hugr, got {cnt}"

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
    db_path = os.environ.get("HUGR_E2E_DUCKDB_PATH") or _default_db_path()
    schema_dir = os.environ.get("HUGR_E2E_SCHEMA_DIR", _default_schema_dir())
    query_url = _query_url(url)

    print(f"Testing Python ingest client against {url}")
    print(f"DuckDB file: {db_path}\n")

    seed_database(db_path)
    ensure_source(query_url, db_path, schema_dir)
    print()

    mem_rows = 128 * 256  # must match test_streaming_memory's parameters

    try:
        client = hugr.HugrClient(url=url)
        test_dataframe(client, query_url)
        test_arrow_table(client, query_url)
        test_bulk(client, db_path, query_url)
        test_streaming_memory(url, query_url, db_path)
    finally:
        cleanup_source(query_url)

    # Now that dev-server released its write lock via DETACH, open the file
    # independently and verify the totals — the cross-process check we
    # couldn't do mid-suite. This is the only independent channel available
    # when dev-server is a separate process (Go in-process tests don't have
    # this constraint).
    post_unload_verify(db_path, expected_mem_rows=mem_rows)

    print("All DuckDB ingest e2e tests passed!")


if __name__ == "__main__":
    main()
