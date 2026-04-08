"""
Test subscription support against a running hugr server.
Usage: python test_subscription.py [URL]
Default URL: http://localhost:15004/ipc
"""
import asyncio
import json
import sys
from hugr import connect_stream


async def test_query_streaming(url: str):
    """Test basic query streaming — one event per path."""
    print("=== Test: Query Streaming ===")
    client = connect_stream(url)

    sub = await client.subscribe(
        'subscription { query { core { catalog { types(limit: 3, order_by: [{field: "name", direction: ASC}]) { name kind } } } } }'
    )

    event_count = 0
    row_count = 0
    async for event in sub.events():
        event_count += 1
        print(f"  event: path={event.path}")
        async for row in event.rows():
            row_count += 1
            if row_count <= 3:
                print(f"    row {row_count}: {row}")

    await client.disconnect()
    assert event_count == 1, f"Expected 1 event (one path), got {event_count}"
    assert row_count > 0, "Should have rows"
    print(f"  Result: {event_count} events, {row_count} rows")
    print("  PASS\n")


async def test_periodic_polling(url: str):
    """Test periodic — one event per path, batches from all ticks."""
    print("=== Test: Periodic Polling (interval=500ms, count=3) ===")
    client = connect_stream(url)

    sub = await client.subscribe(
        'subscription { query(interval: "500ms", count: 3) { core { catalog { types(limit: 5, order_by: [{field: "name", direction: ASC}]) { name } } } } }'
    )

    event_count = 0
    total_rows = 0
    async for event in sub.events():
        event_count += 1
        async for batch in event.chunks():
            total_rows += batch.num_rows

    await client.disconnect()
    assert event_count == 1, f"Expected 1 event (one path across ticks), got {event_count}"
    assert total_rows == 15, f"Expected 15 rows (3 ticks × 5), got {total_rows}"
    print(f"  Result: {event_count} events, {total_rows} total rows")
    print("  PASS\n")


async def test_multi_path(url: str):
    """Test multi-path query — two events (one per path)."""
    print("=== Test: Multi-Path Query ===")
    client = connect_stream(url)

    sub = await client.subscribe('''subscription { query {
        core { catalog {
            types(limit: 3, order_by: [{field: "name", direction: ASC}]) { name kind }
            fields(limit: 3, order_by: [{field: "name", direction: ASC}]) { name field_type }
        } }
    } }''')

    paths = {}
    async for event in sub.events():
        rows = 0
        async for batch in event.chunks():
            rows += batch.num_rows
        paths[event.path] = rows
        print(f"  path={event.path}: {rows} rows")

    await client.disconnect()
    assert len(paths) == 2, f"Expected 2 paths, got {len(paths)}"
    for path, rows in paths.items():
        assert rows > 0, f"Path {path} should have rows"
    print(f"  Result: {len(paths)} paths")
    print("  PASS\n")


async def test_two_subscriptions(url: str):
    """Test two subscriptions on same connection — same data."""
    print("=== Test: Two Subscriptions Same Data ===")
    client = connect_stream(url)

    query = '''subscription { query(interval: "500ms", count: 5) {
        core { catalog {
            types(limit: 10, order_by: [{field: "name", direction: ASC}]) { name kind }
        } }
    } }'''

    sub1 = await client.subscribe(query)
    sub2 = await client.subscribe(query)

    async def collect(sub):
        result = {}
        async for event in sub.events():
            rows = []
            async for batch in event.chunks():
                for i in range(batch.num_rows):
                    row = {batch.schema.field(j).name: batch.column(j)[i].as_py()
                           for j in range(batch.num_columns)}
                    rows.append(json.dumps(row, sort_keys=True))
            result[event.path] = rows
        return result

    data1, data2 = await asyncio.gather(collect(sub1), collect(sub2))

    await client.disconnect()

    assert len(data1) > 0, "sub1 should have data"
    assert len(data2) > 0, "sub2 should have data"

    for path in data1:
        assert path in data2, f"sub2 missing path {path}"
        assert len(data1[path]) == len(data2[path]), \
            f"path {path}: sub1={len(data1[path])} rows, sub2={len(data2[path])} rows"
        # First row should match (sorted, deterministic)
        assert data1[path][0] == data2[path][0], \
            f"path {path}: first row mismatch"
        print(f"  path={path}: {len(data1[path])} rows match")

    print("  PASS\n")


async def test_cancel(url: str):
    """Test subscription cancellation."""
    print("=== Test: Cancel ===")
    client = connect_stream(url)

    sub = await client.subscribe(
        'subscription { query(interval: "60s") { core { catalog { types(limit: 1) { name } } } } }'
    )

    # Get first event
    async for event in sub.events():
        async for batch in event.chunks():
            print(f"  first batch: {batch.num_rows} rows")
            break
        break

    await sub.cancel()
    await client.disconnect()
    print("  Cancelled successfully")
    print("  PASS\n")


async def main():
    url = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:15004/ipc"
    print(f"Testing Python subscription client against {url}\n")

    await test_query_streaming(url)
    await test_periodic_polling(url)
    await test_multi_path(url)
    await test_two_subscriptions(url)
    await test_cancel(url)

    print("All tests passed!")


if __name__ == "__main__":
    asyncio.run(main())
