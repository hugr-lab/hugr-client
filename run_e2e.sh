#!/usr/bin/env bash
# End-to-end runner for the Python ingest client.
#
# Brings up the postgres container + a hugr dev-server (built from the sibling
# hugr-query-engine checkout), then runs test_ingest_e2e.py against it.
#
# Usage:
#   ./run_e2e.sh          # full run with teardown
#   ./run_e2e.sh keep     # leave postgres + dev-server running afterwards
#
# Requires: docker, go (with CGo), python3. The sibling repo
# ../hugr-query-engine must be present (for docker-compose, schema, dev-server).
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
QE="$(cd "$HERE/../hugr-query-engine" && pwd)"
COMPOSE_FILE="$QE/integration-test/ingest/docker-compose.yml"
SCHEMA_DIR="$QE/integration-test/ingest/testdata/schemas/pg_ingest"

PORT="${HUGR_E2E_PORT:-15055}"
URL="http://localhost:${PORT}/ipc"
DSN="postgres://test:test@127.0.0.1:5437/ingestdb"
BIN="${TMPDIR:-/tmp}/hugr-dev-server-e2e"
SERVER_LOG="${TMPDIR:-/tmp}/hugr-dev-server-e2e.log"
VENV="$HERE/.venv-e2e"
SERVER_PID=""

cleanup() {
    if [[ "${1:-}" != "keep" ]]; then
        if [[ -n "$SERVER_PID" ]]; then
            kill "$SERVER_PID" 2>/dev/null || true
        fi
        docker compose -f "$COMPOSE_FILE" down -v 2>/dev/null || true
        rm -f "$BIN" "$SERVER_LOG"
    else
        echo "Kept postgres + dev-server (pid $SERVER_PID) running. Tear down with:"
        echo "  docker compose -f $COMPOSE_FILE down -v; kill $SERVER_PID"
    fi
}
trap 'cleanup "${1:-}"' EXIT

echo "=== 1/5 start postgres ==="
docker compose -f "$COMPOSE_FILE" up -d --wait

echo "=== 2/5 build dev-server ==="
( cd "$QE" && CGO_CFLAGS="-O1 -g" go build -tags=duckdb_arrow -o "$BIN" ./cmd/dev-server )

echo "=== 3/5 start dev-server on :$PORT ==="
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

echo "=== 4/5 prepare python venv ==="
if [[ ! -d "$VENV" ]]; then
    python3 -m venv "$VENV"
    "$VENV/bin/pip" install --quiet --disable-pip-version-check -e "$HERE"
fi

echo "=== 5/5 run e2e ==="
HUGR_E2E_URL="$URL" HUGR_E2E_PG_DSN="$DSN" HUGR_E2E_SCHEMA_DIR="$SCHEMA_DIR" \
    "$VENV/bin/python" "$HERE/test_ingest_e2e.py" "$URL"
