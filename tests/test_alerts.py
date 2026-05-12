"""Tests for alerts.subscribe() and AlertPush wire format."""

from __future__ import annotations

import asyncio
import struct

import pytest

from unimeter.client import Subscription
from unimeter.protocol import (
    ALERT_PUSH_PAYLOAD_SIZE,
    ALERT_RECORD_WIRE_SIZE,
    PacketType,
    decode_alert_push,
    decode_alert_record,
    decode_alerts_list_response,
    encode_alert_push_enable,
)
from unimeter.types import AlertRecord
from datetime import datetime, timezone


# ---- Wire format ----

def _build_alert_record(
    log_offset: int = 7,
    account_id: int = 42,
    metric_code: str = "tokens",
    threshold_code: str = "soft",
    value: int = 1000,
    triggered_at_ns: int = 1_700_000_000_000_000_000,
) -> bytes:
    mc = metric_code.encode().ljust(64, b"\x00")
    tc = threshold_code.encode().ljust(32, b"\x00")
    # _ALERT_RECORD_WIRE = "<Q Q 64s 32s Q q 8x" = 136 bytes
    return struct.pack(
        "<Q Q 64s 32s Q q 8x",
        log_offset, account_id, mc, tc, value, triggered_at_ns,
    )


def test_alert_record_wire_size_matches_constant():
    buf = _build_alert_record()
    assert len(buf) == ALERT_RECORD_WIRE_SIZE


def test_decode_alert_record():
    buf = _build_alert_record(
        log_offset=99, account_id=12345, metric_code="api_calls",
        threshold_code="hard_cap", value=5_000_000,
        triggered_at_ns=1_700_000_000_000_000_000,
    )
    log_offset, acc, metric, threshold, value, ts = decode_alert_record(buf)
    assert log_offset == 99
    assert acc == 12345
    assert metric == "api_calls"
    assert threshold == "hard_cap"
    assert value == 5_000_000
    assert ts == 1_700_000_000_000_000_000


def test_decode_alerts_list_response_multiple():
    buf = (_build_alert_record(log_offset=1)
           + _build_alert_record(log_offset=2, account_id=88))
    records = decode_alerts_list_response(buf)
    assert len(records) == 2
    assert records[0][0] == 1
    assert records[1][0] == 2
    assert records[1][1] == 88


def test_decode_alerts_list_response_ignores_trailing_garbage():
    """Partial trailing bytes shorter than one record are dropped."""
    buf = _build_alert_record(log_offset=1) + b"\x00" * 10
    records = decode_alerts_list_response(buf)
    assert len(records) == 1


def test_decode_alert_push_layout():
    """Push payload = 136B record + 1B node_id + 7B pad = 144B."""
    rec = _build_alert_record(log_offset=42, account_id=7)
    push = rec + bytes([3]) + b"\x00" * 7
    assert len(push) == ALERT_PUSH_PAYLOAD_SIZE
    node_id, log_offset, acc, metric, threshold, value, ts = decode_alert_push(push)
    assert node_id == 3
    assert log_offset == 42
    assert acc == 7


def test_encode_alert_push_enable_is_empty():
    """ALERT_PUSH_ENABLE has no payload."""
    assert encode_alert_push_enable() == b""


def test_alert_push_packet_type_constant():
    """The packet type byte must match the server's wire constant."""
    assert int(PacketType.ALERT_PUSH) == 0x35
    assert int(PacketType.ALERT_PUSH_ENABLE) == 0x34


# ---- Subscription ----

def _record(offset: int, account: int = 42, metric: str = "tokens") -> AlertRecord:
    return AlertRecord(
        node_addr="node-a:7001",
        log_offset=offset,
        account_id=account,
        metric_code=metric,
        threshold_code="soft",
        value_at_cross=1000,
        triggered_at=datetime.fromtimestamp(0, tz=timezone.utc),
    )


async def test_subscription_delivers_matching_records():
    sub = Subscription(account_ids={42}, metric_codes=set(),
                       offsets={}, buffer_size=10)

    sub._deliver(_record(offset=1, account=42))
    sub._deliver(_record(offset=2, account=99))   # filtered out
    sub._deliver(_record(offset=3, account=42))

    out: list[AlertRecord] = []
    async with asyncio.timeout(1.0):
        async for alert in sub:
            out.append(alert)
            if len(out) == 2:
                break

    assert [r.log_offset for r in out] == [1, 3]
    # Offset advances even for filtered records so they don't replay.
    assert sub.offsets() == {"node-a:7001": 4}


async def test_subscription_metric_filter():
    sub = Subscription(account_ids=set(), metric_codes={"tokens"},
                       offsets={}, buffer_size=10)

    sub._deliver(_record(offset=1, metric="tokens"))
    sub._deliver(_record(offset=2, metric="other"))   # filtered out
    sub._deliver(_record(offset=3, metric="tokens"))

    out: list[AlertRecord] = []
    async with asyncio.timeout(1.0):
        async for alert in sub:
            out.append(alert)
            if len(out) == 2:
                break

    assert [r.metric_code for r in out] == ["tokens", "tokens"]


async def test_subscription_close_stops_iteration():
    sub = Subscription(account_ids=set(), metric_codes=set(),
                       offsets={}, buffer_size=10)
    sub._deliver(_record(offset=1))

    out: list[AlertRecord] = []
    await sub.close()

    async with asyncio.timeout(1.0):
        async for alert in sub:
            out.append(alert)

    # Drains queued items before stopping (one record was queued
    # before close, then the sentinel stopped iteration).
    assert len(out) == 1


def test_subscription_tracks_dropped():
    """Queue overflow increments dropped counter, doesn't raise."""
    sub = Subscription(account_ids=set(), metric_codes=set(),
                       offsets={}, buffer_size=2)
    for i in range(5):
        sub._deliver(_record(offset=i))
    assert sub.dropped() == 3
    assert sub.offsets() == {"node-a:7001": 5}


def test_subscription_offset_only_advances_forward():
    sub = Subscription(account_ids=set(), metric_codes=set(),
                       offsets={"node-a:7001": 100}, buffer_size=10)
    # Old record with lower offset must not regress the bookmark.
    sub._deliver(_record(offset=5))
    assert sub.offsets() == {"node-a:7001": 100}
