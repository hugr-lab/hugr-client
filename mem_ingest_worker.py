"""Subprocess worker for the streaming-memory integration test.

Builds a LAZY pyarrow.RecordBatchReader that generates `num_batches` batches
of `rows_per_batch` rows on demand (each row carries a ~`payload_bytes`-byte
string), streams it into hugr via client.ingest, and prints peak RSS as JSON.

Run in a fresh subprocess so peak RSS reflects only this work, not whatever
the parent test process already allocated.

Usage:
    python mem_ingest_worker.py URL DATA_OBJECT NUM_BATCHES ROWS_PER_BATCH PAYLOAD_BYTES
"""
import datetime as dt
import json
import resource
import sys

import pyarrow as pa

import hugr


def _peak_rss_mb() -> float:
    r = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # macOS reports bytes; Linux reports kilobytes.
    if sys.platform == "darwin":
        return r / (1024 * 1024)
    return r / 1024


def main():
    url = sys.argv[1]
    data_object = sys.argv[2]
    num_batches = int(sys.argv[3])
    rows_per_batch = int(sys.argv[4])
    payload_bytes = int(sys.argv[5])

    schema = pa.schema([
        ("name", pa.string()),
        ("value", pa.float64()),
        ("is_active", pa.bool_()),
        ("payload", pa.string()),
        ("created_at", pa.timestamp("us")),
    ])
    base = dt.datetime(2026, 5, 21)
    pad = "x" * payload_bytes

    def batch_gen():
        for b in range(num_batches):
            start = b * rows_per_batch
            names = [f"mem-{start + i:09d}" for i in range(rows_per_batch)]
            values = [(start + i) * 0.5 for i in range(rows_per_batch)]
            active = [(start + i) % 2 == 0 for i in range(rows_per_batch)]
            payload = ['{"p":"%s","i":%d}' % (pad, start + i) for i in range(rows_per_batch)]
            ts = [base for _ in range(rows_per_batch)]
            yield pa.record_batch([
                pa.array(names, pa.string()),
                pa.array(values, pa.float64()),
                pa.array(active, pa.bool_()),
                pa.array(payload, pa.string()),
                pa.array(ts, pa.timestamp("us")),
            ], schema=schema)

    reader = pa.RecordBatchReader.from_batches(schema, batch_gen())

    # Baseline AFTER imports + reader construction (reader is lazy, no batches
    # built yet) and BEFORE the ingest consumes the stream.
    baseline_mb = _peak_rss_mb()

    client = hugr.HugrClient(url=url)
    res = client.ingest(data_object, reader)

    peak_mb = _peak_rss_mb()
    approx_total_mb = (num_batches * rows_per_batch * payload_bytes) / (1024 * 1024)

    print(json.dumps({
        "inserted": res["inserted"],
        "baseline_mb": round(baseline_mb, 1),
        "peak_mb": round(peak_mb, 1),
        "delta_mb": round(peak_mb - baseline_mb, 1),
        "approx_total_data_mb": round(approx_total_mb, 1),
    }))


if __name__ == "__main__":
    main()
