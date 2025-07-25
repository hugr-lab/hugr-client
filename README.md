# This is a python client for the Hugr IPC protocol

The `hugr` is a Data Mesh platform that allows you to query and explore data from various sources in a unified way. It provides a GraphQL interface to access data from different data sources, such as databases, APIs, and files. The `hugr-client` is a Python client for the Hugr platform that allows you to query data from the Hugr server and process it in a Pythonic way.

For more information about the Hugr platform, please visit the [Hugr website](https://hugr-lab.github.io) or the [Hugr GitHub repository](https://github.com/hugr-lab/hugr).

The client can request from the hugr and process them in a pythonic way. For the effective data transmission, the client uses the [hugr ipc protocol](https://github.com/hugr-lab/query-engine/blob/main/hugr-ipc.md) to communicate with the server.

## Installation

```bash
pip install hugr-client
```

 or

```bash
uv pip install hugr-client
```

## Usage

```python
import hugr

# connect to the server
client = hugr.Client("http://localhost:15001/ipc")

# query data
data = client.query("""
    {
        devices {
            id
            name
            geom
            last_seen{
                time
                value
            }
        }
        drivers {
            id
            name
            devices {
                id
                name
                geom
                last_seen{
                    time
                    value
                }
            }
        }
        drivers_by_pk(id: "driver_id") {
            id
            name
            devices {
                id
                name
                geom
                last_seen{
                    time
                    value
                }
            }
        }
    }
""")

# get results as a pandas dataframe
df = data.df('data.devices') # or df = data["data.devices"].df()

# get results as a geopandas dataframe
gdf = data.gdf('data.devices', 'geom') # or gdf = data["data.devices"].gdf("geom")

# if the geometry field is placed in the nested object or arrays `gdf` will flatten the data until the geometry field is found
# field name is optional if data has only one geometry field
gdf = data.gdf('data.drivers', 'devices.geom') # or gdf = data["data.drivers"].gdf("devices.geom")

# get record as a dictionary
d = data.record('data.iot.drivers_by_pk')

# operate parts of results
part = data["data.devices"] 

# get pandas dataframe from the record
df = data.df('data.iot.drivers_by_pk') # or df = part.df()

# get geopandas dataframe from the record, dataframe will be flattened until the geometry field is found
gdf = data.gdf('data.iot.drivers_by_pk', 'devices.geom') # or gdf = part.gdf("devices.geom") or gdf = part.gdf() if only one geometry field is present

# explore geography data in the Jupyter Notebooks (labs or notebooks)

data.explore_map() # or part.explore_map() or hugr.explore_map(data) or hugr.explore_map(part)
```

### Connection parameters

- `url` - the url of the hugr server
- `api_key` - the api key for the hugr server (if using api key authentication)
- `token` - the token for the hugr server (if using token authentication)
- `role` - the role for the hugr server (if user has a few roles in the token)

It also support querying by set up connection parameters.

Parameters will be passed from the environment variables:

- HUGR_URL - the url of the hugr server
- HUGR_API_KEY - the api key for the hugr server (if using api key authentication)
- HUGR_TOKEN - the token for the hugr server (if using token authentication)
- HUGR_API_KEY_HEADER - the header name for the api key (if using api key authentication)
- HUGR_ROLE_HEADER - the header name for the role (if user has a few roles in the token).

```python
import hugr

hugr.query(
    query="""
        {
            devices {
                id
                name
                geom
                last_seen{
                    time
                    value
                }
            }
        }
    """
)
```

## Streaming API

In addition to standard HTTP queries, `hugr-client` supports asynchronous streaming of data via WebSocket. This allows you to receive large datasets in batches or row-by-row, without waiting for the entire result to be loaded into memory.

### Quick Start

```python
import asyncio
from hugr.stream import connect_stream

async def main():
    client = connect_stream("http://localhost:15001/ipc")

    # HTTP query for total count
    result = client.query("query { devices_aggregation { _rows_count } }")
    print("Total devices:", result.record()['_rows_count'])

    # Stream data in batches (Arrow RecordBatch)
    async with await client.stream(
        """
        query {
            devices {
                id
                name
                geom
            }
        }
        """
    ) as stream:
        async for batch in stream.chunks():
            df = batch.to_pandas()
            print("Batch:", len(df), "rows")

    # Stream data row by row
    async with await client.stream(
        "query { devices { id name status } }"
    ) as stream:
        async for row in stream.rows():
            print(row)

asyncio.run(main())
```

### Main Features

- **connect_stream** — create a streaming client (WebSocket).
- **client.stream(query, variables=None)** — asynchronously get a stream of Arrow RecordBatch for a GraphQL query.
- **stream.chunks()** — async generator for batches (RecordBatch).
- **stream.rows()** — async generator for rows (dict).
- **stream.to_pandas()** — collect all streamed data into a pandas.DataFrame.
- **stream.count()** — count the number of rows in the stream.
- **stream_data_object(data_object, fields, variables=None)** — stream a specific data object and fields.

### Example: Collect DataFrame via Streaming

```python
import asyncio
from hugr.stream import connect_stream

async def main():
    client = connect_stream("http://localhost:15001/ipc")
    async with await client.stream(
        "query { devices { id name geom } }"
    ) as stream:
        df = await stream.to_pandas()
        print(df.head())

asyncio.run(main())
```

### Example: Row-by-row Processing

```python
import asyncio
from hugr.stream import connect_stream

async def main():
    client = connect_stream()
    async with await client.stream(
        "query { devices { id name status } }"
    ) as stream:
        async for row in stream.rows():
            if row.get("status") == "active":
                print("Active device:", row["name"])

asyncio.run(main())
```

### Example: Query Cancellation

```python
import asyncio
from hugr.stream import connect_stream

async def main():
    client = connect_stream()
    async with await client.stream(
        "query { devices { id name } }"
    ) as stream:
        count = 0
        async for batch in stream.chunks():
            count += batch.num_rows
            if count > 1000:
                await client.cancel_current_query()
                break

asyncio.run(main())
```

### Notes

- All streaming functions are asynchronous and require `async`/`await`.
- Dependencies: `websockets`, `pyarrow`, `pandas`.
- You can use both a pure streaming client and an enhanced client with HTTP and WebSocket support.

See more in [hugr/stream.py](hugr/stream.py) and the code examples in the source files.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome!

## Dependencies

- "requests",
- "pyarrow",
- "pandas",
- "geopandas",
- "shapely",
- "requests_toolbelt",
- "numpy",
- "shapely",
