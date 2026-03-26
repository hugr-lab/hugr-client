# hugr-client

Python client for the [Hugr](https://hugr-lab.github.io) Data Mesh platform. Query data via GraphQL, get results as Arrow tables, pandas DataFrames, or interactive Perspective viewers.

Uses the [Hugr IPC protocol](https://github.com/hugr-lab/query-engine/blob/main/hugr-ipc.md) (multipart/mixed with Arrow IPC) for efficient data transfer.

## Installation

```bash
pip install hugr-client
```

For interactive map visualizations (KeplerGL):

```bash
pip install hugr-client[viz]
```

## Quick Start

```python
from hugr import HugrClient

client = HugrClient()  # reads connection from ~/.hugr/connections.json
result = client.query("{ core { data_sources { name } } }")

# Interactive Perspective viewer in JupyterLab
result

# pandas DataFrame
df = result.df("data.core.data_sources")

# pyarrow Table (zero-copy, no pandas overhead)
table = result.parts["data.core.data_sources"].to_arrow()
```

## Connection

### From connections.json (recommended)

When using JupyterLab with [hugr-kernel](https://github.com/hugr-lab/hugr-kernel), connections are managed via the connection manager UI. hugr-client reads the same configuration:

```python
# Default connection
client = HugrClient()

# Named connection
client = HugrClient.from_connection("production")
```

### From environment variables

```python
# Uses HUGR_URL, HUGR_API_KEY, HUGR_TOKEN env vars
client = HugrClient()
```

| Variable | Description |
|----------|-------------|
| `HUGR_URL` | Hugr server URL (e.g., `http://localhost:15000/ipc`) |
| `HUGR_API_KEY` | API key for authentication |
| `HUGR_TOKEN` | Bearer token for authentication |
| `HUGR_API_KEY_HEADER` | Custom API key header name (default: `X-Hugr-Api-Key`) |
| `HUGR_ROLE_HEADER` | Custom role header name (default: `X-Hugr-Role`) |
| `HUGR_CONFIG_PATH` | Custom path to connections.json |

### Explicit parameters

```python
client = HugrClient(
    url="http://localhost:15000/ipc",
    api_key="sk-...",
    api_key_header="X-Custom-Key",  # optional custom header
    role="analyst",
)
```

Priority: explicit parameters > environment variables > connections.json

## Working with Results

### Multipart responses

Hugr returns multipart responses with multiple data parts:

```python
result = client.query("""
{
    devices { id name geom }
    drivers { id name }
}
""")

# Access individual parts
result.parts["data.devices"].df()
result.parts["data.drivers"].to_arrow()

# Display all parts (Perspective viewer in JupyterLab)
result
```

### Data access methods

```python
part = result.parts["data.devices"]

# pandas DataFrame
df = part.df()

# pyarrow Table (zero-copy)
table = part.to_arrow()

# GeoDataFrame (with geometry decoding)
gdf = part.to_geo_dataframe("geom")

# or via shortcut
gdf = result.gdf("data.devices", "geom")

# JSON record (for object parts)
record = result.record("data.drivers_by_pk")
```

### Geometry support

Geometry fields are automatically detected from server metadata. Supported formats: WKB, GeoJSON, H3Cell.

```python
# GeoDataFrame with CRS
gdf = result.gdf("data.devices", "geom")
print(gdf.crs)  # EPSG:4326

# Nested geometry (auto-flattens to target field)
gdf = result.gdf("data.drivers", "devices.geom")

# GeoJSON export
layers = result.geojson_layers()
```

### Interactive visualization

With `hugr-client[viz]`:

```python
result.explore_map()  # KeplerGL interactive map
```

In JupyterLab with [hugr-perspective-viewer](https://github.com/hugr-lab/duckdb-kernel):

```python
result  # renders as Perspective viewer with table/map/charts
```

## Streaming API

For large datasets, use WebSocket streaming to process data in batches:

```python
import asyncio
from hugr import connect_stream

async def main():
    client = connect_stream()

    # Stream Arrow batches
    async with await client.stream("{ devices { id name geom } }") as stream:
        async for batch in stream.chunks():
            print(f"Batch: {batch.num_rows} rows")

    # Collect into DataFrame
    async with await client.stream("{ devices { id name } }") as stream:
        df = await stream.to_pandas()

    # Row-by-row processing
    async with await client.stream("{ devices { id status } }") as stream:
        async for row in stream.rows():
            if row["status"] == "active":
                print(row["id"])

asyncio.run(main())
```

### Stream methods

| Method | Description |
|--------|-------------|
| `stream.chunks()` | Async generator of Arrow RecordBatch |
| `stream.rows()` | Async generator of dict rows |
| `stream.to_pandas()` | Collect all batches into DataFrame |
| `stream.count()` | Count total rows |

### Cancel long queries

```python
async with await client.stream("{ large_dataset { ... } }") as stream:
    count = 0
    async for batch in stream.chunks():
        count += batch.num_rows
        if count > 10000:
            await client.cancel_current_query()
            break
```

## ETL / Headless Usage

hugr-client works without Jupyter. No spool files, no display overhead:

```python
from hugr import HugrClient

client = HugrClient()
result = client.query("{ data_source { id value } }")

# Pure data access — no side effects
table = result.to_arrow("data.data_source")  # pyarrow.Table
df = result.df("data.data_source")            # pandas.DataFrame
```

## Dependencies

**Required:**
`requests`, `requests-toolbelt`, `pyarrow`, `pandas`, `numpy`, `geopandas`, `shapely`, `websockets`

**Optional (`[viz]`):**
`keplergl`, `pydeck`, `folium`, `matplotlib`, `mapclassify`

## License

MIT License. See [LICENSE](LICENSE).
