import asyncio
import json
import websockets
import pandas as pd
import pyarrow as pa
import io
from typing import AsyncGenerator, Dict, Any, Optional, List
import os
import logging
from hugr.client import HugrClient

logger = logging.getLogger(__name__)


class HugrStream:
    """Stream for Arrow data chunks"""

    def __init__(self, client: 'HugrStreamingClient'):
        self.client = client
        self._completed = False
        self._error = None

    async def chunks(self) -> AsyncGenerator[pa.RecordBatch, None]:
        """Stream Arrow RecordBatch chunks"""
        try:
            while not self._completed:
                message = await self.client._receive_message()

                if message is None:
                    # Connection closed
                    break

                if isinstance(message, str):
                    # JSON message
                    msg = json.loads(message)
                    if msg["type"] == "complete":
                        self._completed = True
                        self.client._query_active = False
                        if self._error:
                            raise Exception(f"Stream error: {self._error}")
                        print("Stream completed")
                        break
                    elif msg["type"] == "error":
                        self._error = msg["error"]
                elif isinstance(message, bytes):
                    # Binary Arrow data
                    try:
                        reader = pa.ipc.open_stream(io.BytesIO(message))
                        for batch in reader:
                            yield batch
                    except Exception as e:
                        logger.error(f"Failed to parse Arrow data: {e}")
                        raise Exception(f"Failed to parse Arrow data: {e}")

        except websockets.exceptions.ConnectionClosed:
            if not self._completed:
                raise Exception("WebSocket connection closed unexpectedly")
        finally:
            self.client._query_active = False

    async def rows(self) -> AsyncGenerator[Dict, None]:
        """Stream individual rows"""
        async for batch in self.chunks():
            if batch is None:
                continue

            # Iterate through rows in batch
            for i in range(batch.num_rows):
                row_dict = {}
                for j, column_name in enumerate(batch.schema.names):
                    column = batch.column(j)
                    row_dict[column_name] = column[i].as_py()
                yield row_dict

    async def to_pandas(self) -> pd.DataFrame:
        """Collect all chunks into DataFrame"""
        batches = []
        async for batch in self.chunks():
            batches.append(batch)

        if batches:
            table = pa.Table.from_batches(batches)
            return table.to_pandas()
        else:
            return pd.DataFrame()

    async def count(self) -> int:
        """Count total rows"""
        total = 0
        async for batch in self.chunks():
            total += batch.num_rows
        return total

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.client._query_active = False
        if exc_type is not None:
            # Cancel stream on exception
            await self.client.cancel_current_query()


class HugrStreamingClient:
    """WebSocket streaming client for hugr"""

    def __init__(
        self,
        url: str = None,
        headers: Dict[str, str] = None,
        max_frame_size: int = 128 * 1024 * 1024,
    ):
        if not url:
            url = os.environ.get("HUGR_URL")

        self.base_url = url.rstrip('/') if url else None

        # WebSocket URL
        if self.base_url:
            if self.base_url.startswith('http://'):
                self.ws_url = self.base_url.replace('http://', 'ws://')
            elif self.base_url.startswith('https://'):
                self.ws_url = self.base_url.replace('https://', 'wss://')
            else:
                self.ws_url = f"ws://{self.base_url}"
        else:
            raise ValueError("URL is required")

        self._headers = (headers.copy() if headers else {}).update(
            {
                'Upgrade': 'websocket',
                'Connection': 'Upgrade',
                'Sec-WebSocket-Version': '13',
                'Sec-WebSocket-Key': 'hugr-streaming-client',
            }
        )

        self.websocket = None
        self._connected = False
        self._query_active = False
        self._max_frame_size = max_frame_size

    async def connect(self):
        """Establish WebSocket connection"""
        if self._connected and self.websocket:
            return

        headers = self._headers

        try:
            # Try newer websockets API first
            self.websocket = await websockets.connect(
                self.ws_url,
                additional_headers=headers,
                ping_interval=30,
                ping_timeout=10,
                close_timeout=10,
                max_size=self._max_frame_size,  # Customizable max message size
            )
        except TypeError:
            # Fallback for older websockets versions
            try:
                self.websocket = await websockets.connect(
                    self.ws_url,
                    extra_headers=headers,
                    ping_interval=30,
                    ping_timeout=10,
                    max_size=self._max_frame_size,  # Customizable max message size
                )
            except TypeError:
                # Last resort - no headers in connection
                self.websocket = await websockets.connect(self.ws_url)

        self._connected = True
        logger.info("WebSocket connection established")

    async def disconnect(self):
        """Close WebSocket connection"""
        if self.websocket and self._connected:
            try:
                await self.websocket.close()
            except Exception as e:
                logger.warning(f"Error closing WebSocket: {e}")
            finally:
                self.websocket = None
                self._connected = False
                self._query_active = False

    async def _send_message(self, message: Dict):
        """Send JSON message"""
        if not self.websocket:
            raise Exception("Not connected")
        await self.websocket.send(json.dumps(message))

    async def _receive_message(self):
        """Receive message (JSON or binary)"""
        if not self.websocket:
            return None

        try:
            return await self.websocket.recv()
        except websockets.exceptions.ConnectionClosed:
            self._connected = False
            self._query_active = False
            return None
        except Exception as e:
            logger.error(f"Error receiving message: {e}")
            return None

    async def stream_query(self, query: str, variables: dict = None) -> HugrStream:
        """Stream GraphQL query"""
        await self.connect()

        if self._query_active:
            raise Exception("Another query is already active on this connection")

        self._query_active = True

        try:
            await self._send_message(
                {"type": "query", "query": query, "variables": variables or {}}
            )

            return HugrStream(self)
        except Exception as e:
            self._query_active = False
            raise e

    async def stream_data_object(
        self, data_object: str, fields: List[str], variables: dict = None
    ) -> HugrStream:
        """Stream data object with specific fields"""
        await self.connect()

        if self._query_active:
            raise Exception("Another query is already active on this connection")

        self._query_active = True

        try:
            await self._send_message(
                {
                    "type": "query_object",
                    "data_object": data_object,
                    "selected_fields": fields,
                    "variables": variables or {},
                }
            )

            return HugrStream(self)
        except Exception as e:
            self._query_active = False
            raise e

    async def cancel_current_query(self):
        """Cancel currently active query"""
        if self._query_active and self.websocket:
            try:
                await self._send_message({"type": "cancel"})
                self._query_active = False
            except Exception as e:
                logger.warning(f"Error cancelling query: {e}")
                self._query_active = False

    async def _wait_for_completion(self, stream: HugrStream):
        """Wait for stream completion and mark query as inactive"""
        try:
            # Consume the stream to completion
            async for _ in stream.chunks():
                pass
        finally:
            self._query_active = False

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._query_active:
            await self.cancel_current_query()
        await self.disconnect()


class HugrStreamConnection(HugrClient):
    """Enhanced client with both HTTP and streaming"""

    def __init__(self, *args, max_frame_size: int = 128 * 1024 * 1024, **kwargs):
        super().__init__(*args, **kwargs)
        self._streaming_client = None
        self._max_frame_size = max_frame_size

    def _get_streaming_client(self):
        if not self._streaming_client:
            self._streaming_client = HugrStreamingClient(
                self._url, self._headers(), max_frame_size=self._max_frame_size
            )
        return self._streaming_client

    async def disconnect(self):
        """Disconnect streaming client if connected"""
        if self._streaming_client:
            try:
                await self._streaming_client.disconnect()
            except Exception as e:
                logger.warning(f"Error disconnecting streaming client: {e}")
            self._streaming_client = None

    async def cancel_current_query(self):
        """Cancel currently active query on streaming client"""
        if self._streaming_client and self._streaming_client._query_active:
            try:
                await self._streaming_client.cancel_current_query()
            except Exception as e:
                logger.warning(f"Error cancelling streaming query: {e}")
            self._streaming_client._query_active = False

    async def stream(self, query: str, variables: dict = None) -> HugrStream:
        """Create stream (auto-connects)"""
        client = self._get_streaming_client()
        return await client.stream_query(query, variables)

    async def stream_data_object(
        self, data_object: str, fields: List[str], variables: dict = None
    ) -> HugrStream:
        """Stream specific data object fields"""
        client = self._get_streaming_client()
        return await client.stream_data_object(data_object, fields, variables)

    async def streaming_context(self):
        """Get streaming client for manual connection management"""
        return self._get_streaming_client()


# Main interface


def connect_stream(
    url: str = None,
    api_key: str = None,
    token: str = None,
    role: str = None,
    max_frame_size: int = 128 * 1024 * 1024,
) -> HugrStreamConnection:
    return HugrStreamConnection(
        url, api_key, token, role, max_frame_size=max_frame_size
    )


def new_stream_connection(client: HugrClient) -> HugrStreamConnection:
    """Get streaming client from existing HugrClient"""
    if not isinstance(client, HugrStreamConnection):
        client = HugrStreamConnection(
            client._url, client._api_key, client._token, client._role
        )
    return client


# Usage examples
async def example_simple_query():
    """Simple GraphQL query streaming"""
    client = connect_stream('http://localhost:15000/ipc')

    # HTTP query first
    result = client.query("query { devices_aggregation { _rows_count } }")
    total = result.record()['_rows_count']
    print(f"Total devices: {total}")

    # Stream GraphQL query
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
        count = 0
        async for batch in stream.chunks():
            count += batch.num_rows
            print(f"Received batch: {batch.num_rows} rows")

            # Process batch
            df = batch.to_pandas()
            print(f"Device names in batch: {df['name'].tolist()[:5]}...")  # First 5

        print(f"Total streamed: {count} rows")


async def example_data_object():
    """Stream specific data object"""
    async with HugrStreamingClient('http://localhost:15000/ipc') as client:

        # Stream data object with specific fields
        async with await client.stream_data_object(
            data_object="devices", fields=["id", "name", "status"]
        ) as stream:
            async for batch in stream.chunks():
                df = batch.to_pandas()
                print(f"Batch: {len(df)} devices")
                active_devices = df[df['status'] == 'active']
                print(f"Active devices: {len(active_devices)}")


async def example_row_processing():
    """Process individual rows"""
    async with HugrStreamingClient() as client:

        async with await client.stream_query(
            "query { devices { id name status } }"
        ) as stream:
            device_count = 0
            active_count = 0

            async for row in stream.rows():
                device_count += 1
                if row.get('status') == 'active':
                    active_count += 1

                if device_count % 1000 == 0:
                    print(f"Processed {device_count} devices, {active_count} active")

            print(f"Final: {device_count} total, {active_count} active")


async def example_collect_dataframe():
    """Collect all streaming data into DataFrame"""
    client = connect_stream()

    async with await client.stream(
        "query { devices { id name geom location } }"
    ) as stream:
        df = await stream.to_pandas()
        print(f"Collected {len(df)} devices into DataFrame")
        print(f"Columns: {df.columns.tolist()}")
        print(df.head())


async def example_reusable_connection():
    """Multiple queries on same connection"""
    async with HugrStreamingClient('http://localhost:15000/ipc') as client:

        # Query 1: Count devices
        async with await client.stream_data_object("devices", ["id"]) as stream:
            device_count = await stream.count()
            print(f"Device count: {device_count}")

        # Query 2: Get active devices (reusing same connection)
        async with await client.stream_query(
            """
            query {
                devices(filter: {status: {eq: "active"}}) {
                    id
                    name
                    last_seen
                }
            }
        """
        ) as stream:
            async for batch in stream.chunks():
                df = batch.to_pandas()
                print(f"Active devices batch: {len(df)} rows")


async def example_with_cancellation():
    """Example with query cancellation"""
    async with HugrStreamingClient() as client:

        try:
            async with await client.stream_query(
                "query { devices { id name } }"
            ) as stream:
                count = 0
                async for batch in stream.chunks():
                    count += batch.num_rows
                    print(f"Processed {count} rows")

                    # Cancel after processing 1000 rows
                    if count >= 1000:
                        print("Cancelling query...")
                        await client.cancel_current_query()
                        break

        except Exception as e:
            print(f"Query cancelled or error: {e}")


async def example_error_handling():
    """Error handling example"""
    async with HugrStreamingClient() as client:

        try:
            # This query might fail
            async with await client.stream_query(
                "query { nonexistent_table { id } }"
            ) as stream:
                async for batch in stream.chunks():
                    print(f"Got batch: {batch.num_rows}")

        except Exception as e:
            print(f"Query failed: {e}")

        # Connection is still alive, try another query
        try:
            async with await client.stream_data_object(
                "devices", ["id", "name"]
            ) as stream:
                count = await stream.count()
                print(f"Second query succeeded: {count} devices")
        except Exception as e:
            print(f"Second query failed: {e}")


if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(level=logging.INFO)

    # Run examples
    asyncio.run(example_simple_query())
