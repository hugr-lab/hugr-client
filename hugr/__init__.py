from .client import (
    HugrClient,
    HugrIPCObject,
    HugrIPCTable,
    HugrIPCResponse,
    connect,
    query,
    explore_map,
)

from .stream import (
    HugrStreamConnection,
    HugrStreamingClient,
    HugrStream,
    connect_stream,
    new_stream_connection,
)

__all__ = [
    "HugrClient",
    "HugrIPCResponse",
    "HugrIPCObject",
    "HugrIPCTable",
    "connect",
    "query",
    "explore_map",
    "HugrStreamConnection",
    "HugrStreamingClient",
    "HugrStream",
    "connect_stream",
    "new_stream_connection",
]

__version__ = "0.1.1"
