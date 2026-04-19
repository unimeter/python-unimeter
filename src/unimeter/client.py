"""AsyncClient: the main entry point for the Unimeter Python SDK."""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from datetime import datetime
from typing import AsyncIterator, Optional

from . import protocol as proto
from .errors import (
    AlreadyExistsError,
    BackpressureError,
    NotFoundError,
    RedirectError,
    ServerError,
)
from .pool import ConnectionPool
from .router import Router
from .types import (
    AggType,
    AggValue,
    AlertRecord,
    AlertThreshold,
    DeliveryMode,
    DimensionFilter,
    Event,
    EventRecord,
    IngestResult,
    MetricSchema,
    OperationType,
    Period,
    UsageResult,
    current_month,
)

logger = logging.getLogger(__name__)


def _check_status(status: proto.StatusCode, payload: bytes) -> None:
    if status == proto.StatusCode.BACKPRESSURE:
        raise BackpressureError()
    if status == proto.StatusCode.ERR:
        raise ServerError("server error")
    if status == proto.StatusCode.REDIRECT:
        addr = payload.split(b"\x00")[0].decode("utf-8")
        raise RedirectError(addr)


class MetricsClient:
    def __init__(self, pool: ConnectionPool, router: Router):
        self._pool = pool
        self._router = router

    async def create(self, schema: MetricSchema) -> None:
        payload = proto.encode_metric_put(
            schema.code,
            int(schema.agg_type),
            schema.recurring,
            schema.field_name,
            [(f.key, f.values) for f in schema.filters],
            [(t.code, t.value, t.recurring) for t in schema.thresholds],
            int(schema.period_type),
            schema.billing_cycle_day,
        )
        # Metrics go to any node (partition=0xFFFF).
        addr = self._router.leader_for(0)
        status, resp_payload = await self._pool.send(addr, proto.PacketType.METRIC_PUT, payload)
        if status == proto.StatusCode.REDIRECT:
            new_addr = resp_payload.split(b"\x00")[0].decode("utf-8")
            status, resp_payload = await self._pool.send(new_addr, proto.PacketType.METRIC_PUT, payload)
        if status == proto.StatusCode.ERR:
            raise AlreadyExistsError(f"metric {schema.code} already exists")
        _check_status(status, resp_payload)

    async def update(self, schema: MetricSchema) -> None:
        payload = proto.encode_metric_put(
            schema.code,
            int(schema.agg_type),
            schema.recurring,
            schema.field_name,
            [(f.key, f.values) for f in schema.filters],
            [(t.code, t.value, t.recurring) for t in schema.thresholds],
            int(schema.period_type),
            schema.billing_cycle_day,
        )
        addr = self._router.leader_for(0)
        status, resp_payload = await self._pool.send(addr, proto.PacketType.METRIC_PUT, payload)
        if status == proto.StatusCode.REDIRECT:
            new_addr = resp_payload.split(b"\x00")[0].decode("utf-8")
            status, _ = await self._pool.send(new_addr, proto.PacketType.METRIC_PUT, payload)
        _check_status(status, resp_payload)

    async def delete(self, code: str) -> None:
        payload = proto.encode_metric_delete(code)
        addr = self._router.leader_for(0)
        status, resp_payload = await self._pool.send(addr, proto.PacketType.METRIC_DELETE, payload)
        if status == proto.StatusCode.REDIRECT:
            new_addr = resp_payload.split(b"\x00")[0].decode("utf-8")
            status, _ = await self._pool.send(new_addr, proto.PacketType.METRIC_DELETE, payload)
        _check_status(status, resp_payload)

    async def list(self) -> list[MetricSchema]:
        addr = self._router.leader_for(0)
        status, payload = await self._pool.send(addr, proto.PacketType.METRIC_LIST, b"")
        _check_status(status, payload)
        # Parse MetricSchemaWire entries from payload.
        # Each entry starts with code[64] + agg_type(1) + recurring(1) + filters_count(1) + pad(1) + field[64] = 132B
        schemas = []
        off = 0
        while off + 132 <= len(payload):
            code = payload[off:off + 64].split(b"\x00")[0].decode("utf-8")
            agg_type = AggType(payload[off + 64])
            off += 132
            schemas.append(MetricSchema(code=code, agg_type=agg_type))
        return schemas


class AlertsClient:
    def __init__(self, pool: ConnectionPool, router: Router):
        self._pool = pool
        self._router = router

    async def subscribe(
        self,
        account_ids: list[int] | None = None,
        since_offset: dict[str, int] | None = None,
    ) -> AsyncIterator[AlertRecord]:
        raise NotImplementedError("alert subscriptions not yet implemented in Python SDK")


class AsyncClient:
    """Async client for the Unimeter usage metering engine.

    Usage::

        async with AsyncClient(["localhost:7001"]) as client:
            await client.metrics.create(MetricSchema(code="api_calls", agg_type=AggType.COUNT))
            await client.ingest([Event(account_id=42, metric_code="api_calls", value=1)])
            result = await client.query(42, "api_calls", current_month())
    """

    def __init__(self, seeds: list[str]):
        self._seeds = seeds
        self._pool = ConnectionPool()
        self._router = Router(seeds, self._pool)
        self.metrics = MetricsClient(self._pool, self._router)
        self.alerts = AlertsClient(self._pool, self._router)

    async def connect(self) -> None:
        await self._router.bootstrap()
        self._router.start_refresh()

    async def close(self) -> None:
        await self._router.stop_refresh()
        await self._pool.close_all()

    async def __aenter__(self) -> AsyncClient:
        await self.connect()
        return self

    async def __aexit__(self, *args) -> None:
        await self.close()

    async def ingest(
        self,
        events: list[Event],
        *,
        delivery: DeliveryMode = DeliveryMode.ASYNC,
    ) -> IngestResult:
        """Send events to Unimeter. Groups by partition and fans out in parallel."""
        if not events:
            return IngestResult(n_stored=0, n_duplicates=0, last_offset=0)

        # Check if any event requests sync delivery.
        use_sync = delivery == DeliveryMode.SYNC or any(
            e.delivery_mode == DeliveryMode.SYNC for e in events
        )
        ptype = proto.PacketType.INGEST_SYNC if use_sync else proto.PacketType.INGEST_ASYNC

        # Group events by leader address.
        groups: dict[str, list[Event]] = defaultdict(list)
        for ev in events:
            leader = self._router.leader_for(ev.account_id)
            groups[leader].append(ev)

        # Fan out.
        async def send_group(addr: str, evts: list[Event]) -> IngestResult:
            wire_events = []
            for ev in evts:
                props = list(ev.properties.items()) if ev.properties else []
                wire_events.append((
                    ev.metric_code,
                    ev.account_id,
                    ev.timestamp_ns(),
                    ev.value,
                    int(ev.operation_type),
                    len(props),
                    props,
                ))
            payload = proto.encode_ingest_payload(wire_events)
            status, resp = await self._pool.send(addr, ptype, payload)

            if status == proto.StatusCode.REDIRECT:
                new_addr = resp.split(b"\x00")[0].decode("utf-8")
                pid = self._router.partition_of(evts[0].account_id)
                self._router.update_partition(pid, new_addr)
                status, resp = await self._pool.send(new_addr, ptype, payload)

            _check_status(status, resp)

            if len(resp) >= proto.INGEST_RESPONSE_SIZE:
                n_stored, n_dups, last_offset = proto.decode_ingest_response(resp)
                return IngestResult(n_stored=n_stored, n_duplicates=n_dups, last_offset=last_offset)
            return IngestResult(n_stored=len(evts), n_duplicates=0, last_offset=0)

        results = await asyncio.gather(*[
            send_group(addr, evts) for addr, evts in groups.items()
        ])

        total = IngestResult(n_stored=0, n_duplicates=0, last_offset=0)
        for r in results:
            total.n_stored += r.n_stored
            total.n_duplicates += r.n_duplicates
            total.last_offset = max(total.last_offset, r.last_offset)
        return total

    async def query(
        self,
        account_id: int,
        metric_code: str,
        period: Period,
        *,
        filters: dict[str, str] | None = None,
    ) -> UsageResult:
        start_ns = int(period.start.timestamp() * 1_000_000_000)
        end_ns = int(period.end.timestamp() * 1_000_000_000)
        payload = proto.encode_usage_query(account_id, metric_code, start_ns, end_ns, filters)

        addr = self._router.replica_for(account_id)
        status, resp = await self._pool.send(addr, proto.PacketType.USAGE_QUERY, payload)

        if status == proto.StatusCode.REDIRECT:
            new_addr = resp.split(b"\x00")[0].decode("utf-8")
            status, resp = await self._pool.send(new_addr, proto.PacketType.USAGE_QUERY, payload)

        _check_status(status, resp)

        if len(resp) >= proto.AGG_VALUE_WIRE_SIZE:
            s, c, m, lv, lt, af = proto.decode_agg_value(resp)
            return UsageResult(
                value=AggValue(sum=s, count=c, max=m, last_value=lv, last_timestamp=lt, alert_flags=af),
                period_start=period.start,
                period_end=period.end,
            )
        return UsageResult(value=AggValue())

    async def query_realtime(self, account_id: int, metric_code: str) -> AggValue:
        payload = proto.encode_usage_realtime(account_id, metric_code)
        addr = self._router.leader_for(account_id)
        status, resp = await self._pool.send(addr, proto.PacketType.USAGE_REALTIME, payload)
        _check_status(status, resp)

        if len(resp) >= proto.AGG_VALUE_WIRE_SIZE:
            s, c, m, lv, lt, af = proto.decode_agg_value(resp)
            return AggValue(sum=s, count=c, max=m, last_value=lv, last_timestamp=lt, alert_flags=af)
        return AggValue()

    async def list_events(
        self,
        account_id: int,
        since: datetime,
        until: datetime,
    ) -> list[EventRecord]:
        since_ns = int(since.timestamp() * 1_000_000_000)
        until_ns = int(until.timestamp() * 1_000_000_000)
        payload = proto.encode_events_list(account_id, since_ns, until_ns)

        addr = self._router.leader_for(account_id)
        status, resp = await self._pool.send(addr, proto.PacketType.EVENTS_LIST, payload)
        _check_status(status, resp)
        # Parse EventRecordWire entries (104B each) from payload.
        records = []
        # Simplified: return raw list for now.
        return records

    async def list_alerts(
        self,
        account_id: int,
        since_offset: int = 0,
    ) -> list[AlertRecord]:
        payload = proto.encode_alerts_list(account_id, since_offset)
        addr = self._router.leader_for(account_id)
        status, resp = await self._pool.send(addr, proto.PacketType.ALERTS_LIST, payload)
        _check_status(status, resp)
        records = []
        return records
