#!/usr/bin/env bash
# End-to-end runner for the Python ingest client against a DuckDB-backed
# data source. Mirror of run_e2e_postgres.sh but without the postgres container —
# DuckDB is a local file the test seeds itself.
#
# Usage:
#   ./run_e2e_duckdb.sh          # full run with teardown
#   ./run_e2e_duckdb.sh keep     # leave dev-server running afterwards
#
# Requires: go (with CGo), python3. The sibling repo ../hugr-query-engine
# must be present (for dev-server source and the duck_ingest schema).
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
QE="$(cd "$HERE/../hugr-query-engine" && pwd)"
SCHEMA_DIR="$QE/integration-test/ingest-duckdb/testdata/schemas/duck_ingest"

PORT="${HUGR_E2E_PORT:-15055}"
URL="http://localhost:${PORT}/ipc"
BIN="${TMPDIR:-/tmp}/hugr-dev-server-e2e"
SERVER_LOG="${TMPDIR:-/tmp}/hugr-dev-server-e2e.log"
VENV="$HERE/.venv-e2e"
SERVER_PID=""

cleanup() {
    if [[ "${1:-}" != "keep" ]]; then
        if [[ -n "$SERVER_PID" ]]; then
            kill "$SERVER_PID" 2>/dev/null || true
        fi
        rm -f "$BIN" "$SERVER_LOG"
    else
        echo "Kept dev-server (pid $SERVER_PID) running. Tear down with:"
        echo "  kill $SERVER_PID"
    fi
}
trap 'cleanup "${1:-}"' EXIT

echo "=== 1/4 build dev-server ==="
( cd "$QE" && go build -tags=duckdb_arrow -o "$BIN" ./cmd/dev-server )

echo "=== 2/4 start dev-server on :$PORT ==="
BIND=":$PORT" ADMIN_UI="false" ALLOWED_ANONYMOUS="true" ANONYMOUS_ROLE="admin" \
    "$BIN" >"$SERVER_LOG" 2>&1 &
SERVER_PID=$!
for i in $(seq 1 60); do
    if curl -sf "http://localhost:${PORT}/query?query=%7B__typename%7D" >/dev/null 2>&1; then
        echo "  healthy after ${i}s"; break
    fi
    if ! kill -0 "$SERVER_PID" 2>/dev/null; then
        echo "  dev-server died; log:"; tail -20 "$SERVER_LOG"; exit 1
    fi
    sleep 1
done

echo "=== 3/4 prepare python venv ==="
if [[ ! -d "$VENV" ]]; then
    python3 -m venv "$VENV"
    "$VENV/bin/pip" install --quiet --disable-pip-version-check -e "$HERE"
fi
# duckdb is a test-only dep (used to seed the .duckdb file + RO verifier),
# not in pyproject.toml. Install on top of the venv idempotently.
"$VENV/bin/pip" install --quiet --disable-pip-version-check duckdb

echo "=== 4/4 run e2e ==="
HUGR_E2E_URL="$URL" HUGR_E2E_SCHEMA_DIR="$SCHEMA_DIR" \
    "$VENV/bin/python" "$HERE/test_ingest_e2e_duckdb.py" "$URL"
