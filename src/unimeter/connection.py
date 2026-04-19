"""Single persistent TCP connection with request multiplexing."""

from __future__ import annotations

import asyncio
import logging
import struct
from typing import Callable, Optional

from . import protocol as proto

logger = logging.getLogger(__name__)


class Connection:
    """A single TCP connection to one Unimeter node.

    Supports multiplexed requests via request_id and server-initiated
    broadcasts (request_id == 0).
    """

    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.addr = f"{host}:{port}"
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._pending: dict[int, asyncio.Future[tuple[proto.StatusCode, bytes]]] = {}
        self._next_id = 1
        self._read_task: Optional[asyncio.Task] = None
        self._write_lock = asyncio.Lock()
        self._connected = False
        self.on_broadcast: Optional[Callable[[int, bytes], None]] = None

    @property
    def connected(self) -> bool:
        return self._connected

    async def connect(self) -> None:
        self._reader, self._writer = await asyncio.open_connection(self.host, self.port)
        self._connected = True
        self._read_task = asyncio.create_task(self._reader_loop())

    async def close(self) -> None:
        self._connected = False
        if self._read_task:
            self._read_task.cancel()
            try:
                await self._read_task
            except (asyncio.CancelledError, Exception):
                pass
        if self._writer:
            self._writer.close()
            try:
                await self._writer.wait_closed()
            except Exception:
                pass
        # Fail all pending requests.
        for fut in self._pending.values():
            if not fut.done():
                fut.set_exception(ConnectionError("connection closed"))
        self._pending.clear()

    async def send(
        self,
        packet_type: proto.PacketType,
        payload: bytes,
        *,
        partition: int = 0xFFFF,
    ) -> tuple[proto.StatusCode, bytes]:
        """Send a request and wait for the response."""
        request_id = self._next_id
        self._next_id = (self._next_id + 1) & 0xFFFFFFFF
        if self._next_id == 0:
            self._next_id = 1

        header = proto.encode_request_header(packet_type, len(payload), request_id, partition)

        loop = asyncio.get_running_loop()
        fut: asyncio.Future[tuple[proto.StatusCode, bytes]] = loop.create_future()
        self._pending[request_id] = fut

        try:
            async with self._write_lock:
                if not self._writer:
                    raise ConnectionError("not connected")
                self._writer.write(header + payload)
                await self._writer.drain()
            return await fut
        except Exception:
            self._pending.pop(request_id, None)
            raise

    async def _reader_loop(self) -> None:
        try:
            while self._connected and self._reader:
                header_data = await self._reader.readexactly(proto.RESPONSE_HEADER_SIZE)
                status, request_id, payload_len = proto.decode_response_header(header_data)

                payload = b""
                if payload_len > 0:
                    payload = await self._reader.readexactly(payload_len)

                if request_id == 0:
                    # Server broadcast.
                    if self.on_broadcast:
                        self.on_broadcast(status, payload)
                    continue

                fut = self._pending.pop(request_id, None)
                if fut and not fut.done():
                    fut.set_result((status, payload))
        except asyncio.IncompleteReadError:
            pass
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.debug("reader loop error: %s", e)
        finally:
            self._connected = False
            for fut in self._pending.values():
                if not fut.done():
                    fut.set_exception(ConnectionError("connection lost"))
            self._pending.clear()
