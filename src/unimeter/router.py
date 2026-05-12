"""Partition map cache and routing logic."""

from __future__ import annotations

import asyncio
import logging
from typing import Optional

from . import protocol as proto
from .pool import ConnectionPool

logger = logging.getLogger(__name__)

PARTITION_COUNT = 256
REFRESH_INTERVAL = 30.0  # seconds


class Router:
    """Caches the partition map and routes requests to the correct node."""

    def __init__(self, seeds: list[str], pool: ConnectionPool):
        self._seeds = seeds
        self._pool = pool
        # (leader_addr, replica0_addr, replica1_addr) per partition
        self._map: list[tuple[str, str, str]] = [("", "", "")] * PARTITION_COUNT
        self._refresh_task: Optional[asyncio.Task] = None

    @staticmethod
    def partition_of(account_id: int) -> int:
        return account_id % PARTITION_COUNT

    def leader_for(self, account_id: int) -> str:
        return self._map[self.partition_of(account_id)][0]

    def replica_for(self, account_id: int) -> str:
        """Round-robin across replicas, falling back to leader."""
        p = self.partition_of(account_id)
        _, r0, r1 = self._map[p]
        if r0:
            return r0
        return self._map[p][0]

    def nodes(self) -> list[str]:
        """All unique node addresses across leaders and replicas."""
        seen: set[str] = set()
        out: list[str] = []
        for leader, r0, r1 in self._map:
            for addr in (leader, r0, r1):
                if addr and addr not in seen:
                    seen.add(addr)
                    out.append(addr)
        return out

    def update_partition(self, partition_id: int, new_leader: str) -> None:
        old = self._map[partition_id]
        self._map[partition_id] = (new_leader, old[1], old[2])

    async def bootstrap(self) -> None:
        """Fetch partition map from the first reachable seed node."""
        for seed in self._seeds:
            try:
                status, payload = await self._pool.send(
                    seed, proto.PacketType.GET_PARTITION_MAP, b"",
                )
                if status == proto.StatusCode.OK and len(payload) == proto.PARTITION_MAP_PAYLOAD_SIZE:
                    self._map = proto.decode_partition_map(payload)
                    logger.info("bootstrapped partition map from %s", seed)
                    return
            except Exception as e:
                logger.debug("seed %s failed: %s", seed, e)
                continue
        raise ConnectionError("no seed nodes reachable")

    def start_refresh(self) -> None:
        if self._refresh_task is None:
            self._refresh_task = asyncio.create_task(self._refresh_loop())

    async def stop_refresh(self) -> None:
        if self._refresh_task:
            self._refresh_task.cancel()
            try:
                await self._refresh_task
            except asyncio.CancelledError:
                pass
            self._refresh_task = None

    async def _refresh_loop(self) -> None:
        while True:
            await asyncio.sleep(REFRESH_INTERVAL)
            for seed in self._seeds:
                try:
                    status, payload = await self._pool.send(
                        seed, proto.PacketType.GET_PARTITION_MAP, b"",
                    )
                    if status == proto.StatusCode.OK and len(payload) == proto.PARTITION_MAP_PAYLOAD_SIZE:
                        self._map = proto.decode_partition_map(payload)
                        break
                except Exception:
                    continue
