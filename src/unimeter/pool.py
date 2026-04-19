"""Connection pool: one persistent connection per Unimeter node."""

from __future__ import annotations

import asyncio
import logging
from typing import Optional

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

    async def get_or_connect(self, addr: str) -> Connection:
        async with self._lock:
            conn = self._conns.get(addr)
            if conn and conn.connected:
                return conn

            host, port_str = addr.rsplit(":", 1)
            port = int(port_str)
            conn = Connection(host, port)
            await conn.connect()
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
