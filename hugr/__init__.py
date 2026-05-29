from .client import (
    HugrClient,
    HugrIPCObject,
    HugrIPCTable,
    HugrIPCResponse,
    connect,
    query,
    ingest,
    explore_map,
)

from .stream import (
    HugrStreamConnection,
    HugrStreamingClient,
    HugrStream,
    HugrSubscription,
    SubscriptionEvent,
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
    "ingest",
    "explore_map",
    "HugrStreamConnection",
    "HugrStreamingClient",
    "HugrStream",
    "HugrSubscription",
    "SubscriptionEvent",
    "connect_stream",
    "new_stream_connection",
]

__version__ = "0.3.0"
