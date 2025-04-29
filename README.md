# This is a python client for the Hugr IPC protocol

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
