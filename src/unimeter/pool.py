"""Connection pool: one persistent connection per Unimeter node."""

from __future__ import annotations

import asyncio
import logging
from typing import Callable, Optional

from . import protocol as proto
from .connection import Connection

logger = logging.getLogger(__name__)


class ConnectionPool:
    """Manages one Connection per node address.

    Reconnects with exponential backoff on failure.
    """

    def __init__(self):
        self._conns: dict[str, Connection] = {}
        self._lock = asyncio.Lock()
        # Handler signature: (node_addr, packet_type_byte, payload).
        self._broadcast_handler: Optional[Callable[[str, int, bytes], None]] = None

    def set_broadcast_handler(
        self, handler: Optional[Callable[[str, int, bytes], None]]
    ) -> None:
        """Register a callback invoked for every server-pushed broadcast
        (request_id == 0). Applied to all current and future connections.
        Pass None to clear.
        """
        self._broadcast_handler = handler
        for addr, conn in self._conns.items():
            self._wire_broadcast(addr, conn)

    def _wire_broadcast(self, addr: str, conn: Connection) -> None:
        if self._broadcast_handler is None:
            conn.on_broadcast = None
            return
        handler = self._broadcast_handler
        conn.on_broadcast = lambda pkt_type, payload: handler(addr, pkt_type, payload)

    async def get_or_connect(self, addr: str) -> Connection:
        async with self._lock:
            conn = self._conns.get(addr)
            if conn and conn.connected:
                return conn

            host, port_str = addr.rsplit(":", 1)
            port = int(port_str)
            conn = Connection(host, port)
            await conn.connect()
            self._wire_broadcast(addr, conn)
            self._conns[addr] = conn
            return conn

    async def send(
        self,
        addr: str,
        packet_type: proto.PacketType,
        payload: bytes,
        *,
        partition: int = 0xFFFF,
    ) -> tuple[proto.StatusCode, bytes]:
        conn = await self.get_or_connect(addr)
        return await conn.send(packet_type, payload, partition=partition)

    async def close_all(self) -> None:
        for conn in self._conns.values():
            await conn.close()
        self._conns.clear()
