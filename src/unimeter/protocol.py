"""Binary protocol encoding/decoding for the Unimeter wire format.

All integers are little-endian. Strings are null-padded to fixed widths.
"""

from __future__ import annotations

import struct
from enum import IntEnum

MAGIC = 0xC0FFEE01
VERSION = 1

REQUEST_HEADER_SIZE = 16
RESPONSE_HEADER_SIZE = 12
WIRE_EVENT_SIZE = 96
PROP_PAIR_SIZE = 96
INGEST_PAYLOAD_HEADER_SIZE = 8
INGEST_RESPONSE_SIZE = 16
AGG_VALUE_WIRE_SIZE = 56
ADDR_LEN = 24
PARTITION_MAP_PAYLOAD_SIZE = 24576


class PacketType(IntEnum):
    INGEST_ASYNC = 0x00
    INGEST_SYNC = 0x01
    GET_PARTITION_MAP = 0x10
    PARTITION_MAP_UPDATE = 0x11
    METRIC_PUT = 0x20
    METRIC_DELETE = 0x21
    METRIC_LIST = 0x22
    USAGE_QUERY = 0x30
    USAGE_REALTIME = 0x31
    EVENTS_LIST = 0x32
    ALERTS_LIST = 0x33
    ALERT_PUSH_ENABLE = 0x34
    ALERT_PUSH = 0x35
    USAGE_QUERY_BREAKDOWN = 0x36
    CLUSTER_STATUS = 0x40
    CLUSTER_REBALANCE = 0x41


class StatusCode(IntEnum):
    OK = 0
    DUPLICATE = 1
    ERR = 2
    UNKNOWN_METRIC = 3
    REDIRECT = 4
    NOT_LEADER = 5
    BACKPRESSURE = 6


# ---- Request header ----

_REQ_HDR = struct.Struct("<IBBHII")  # magic, version, packet_type, partition, payload_len, request_id


def encode_request_header(
    packet_type: PacketType,
    payload_len: int,
    request_id: int,
    partition: int = 0xFFFF,
) -> bytes:
    return _REQ_HDR.pack(
        MAGIC, VERSION, int(packet_type), partition, payload_len, request_id,
    )


# ---- Response header ----

_RESP_HDR = struct.Struct("<B3xII")  # status(1) + pad(3) + request_id(4) + payload_len(4)


def decode_response_header(data: bytes) -> tuple[StatusCode, int, int]:
    """Returns (status, request_id, payload_len)."""
    status, request_id, payload_len = _RESP_HDR.unpack_from(data)
    return StatusCode(status), request_id, payload_len


# ---- Ingest ----

_INGEST_HDR = struct.Struct("<II")  # event_count, props_count

_WIRE_EVENT = struct.Struct("<64s Q q Q B B 6x")  # metric_code[64], account_id, timestamp, value, op_type, props_count


def encode_ingest_payload(
    events: list[tuple[str, int, int, int, int, int, list[tuple[str, str]]]],
) -> bytes:
    """Encode events into ingest payload.

    Each event tuple: (metric_code, account_id, timestamp_ns, value, op_type, props_count, props).
    """
    total_props = sum(len(e[6]) for e in events)
    parts = [_INGEST_HDR.pack(len(events), total_props)]

    for metric_code, account_id, timestamp_ns, value, op_type, _, props in events:
        mc_bytes = metric_code.encode("utf-8")[:63].ljust(64, b"\x00")
        parts.append(_WIRE_EVENT.pack(mc_bytes, account_id, timestamp_ns, value, op_type, len(props)))

    for ev in events:
        for key, val in ev[6]:
            k = key.encode("utf-8")[:31].ljust(32, b"\x00")
            v = val.encode("utf-8")[:63].ljust(64, b"\x00")
            parts.append(k + v)

    return b"".join(parts)


_INGEST_RESP = struct.Struct("<IIQ")  # n_stored, n_duplicates, last_offset


def decode_ingest_response(data: bytes) -> tuple[int, int, int]:
    return _INGEST_RESP.unpack_from(data)


# ---- Metric schema ----

_METRIC_PUT_BASE = struct.Struct("<64s B B B x 64s")  # code[64], agg_type, recurring, filters_count, _, field[64]
_DIM_FILTER_WIRE = struct.Struct("<32s B 3x 256s")     # key[32], values_count, _pad, values[32*8]
_THRESHOLD_WIRE = struct.Struct("<32s Q B 7x")          # code[32], value, recurring, _pad


def encode_metric_put(
    code: str,
    agg_type: int,
    recurring: bool = False,
    field_name: str = "",
    filters: list[tuple[str, list[str]]] | None = None,
    thresholds: list[tuple[str, int, bool]] | None = None,
    period_type: int = 0,
    billing_cycle_day: int = 0,
) -> bytes:
    filters = filters or []
    thresholds = thresholds or []
    code_b = code.encode("utf-8")[:63].ljust(64, b"\x00")
    field_b = field_name.encode("utf-8")[:63].ljust(64, b"\x00")

    parts = [_METRIC_PUT_BASE.pack(code_b, agg_type, int(recurring), len(filters), field_b)]

    for key, values in filters:
        k = key.encode("utf-8")[:31].ljust(32, b"\x00")
        vals_buf = b""
        for v in values[:8]:
            vals_buf += v.encode("utf-8")[:31].ljust(32, b"\x00")
        vals_buf = vals_buf.ljust(256, b"\x00")
        parts.append(_DIM_FILTER_WIRE.pack(k, len(values), vals_buf))

    # Alert thresholds section
    if thresholds:
        parts.append(struct.pack("<B7x", len(thresholds)))
        for t_code, t_value, t_recurring in thresholds:
            tc = t_code.encode("utf-8")[:31].ljust(32, b"\x00")
            parts.append(_THRESHOLD_WIRE.pack(tc, t_value, int(t_recurring)))
    else:
        parts.append(struct.pack("<B7x", 0))

    # Period config section: [period_type:u8][billing_cycle_day:u8][6B pad]
    parts.append(struct.pack("<BB6x", period_type, billing_cycle_day))

    return b"".join(parts)


def encode_metric_delete(code: str) -> bytes:
    return code.encode("utf-8")[:63].ljust(64, b"\x00")


# ---- Query ----

_QUERY_BASE = struct.Struct("<Q q q 64s B 7x")  # account_id, period_start, period_end, metric[64], filters_count, pad


def encode_usage_query(
    account_id: int,
    metric_code: str,
    period_start_ns: int,
    period_end_ns: int,
    filters: dict[str, str] | None = None,
) -> bytes:
    mc = metric_code.encode("utf-8")[:63].ljust(64, b"\x00")
    filter_list = list((filters or {}).items())

    parts = [_QUERY_BASE.pack(account_id, period_start_ns, period_end_ns, mc, len(filter_list))]

    for key, value in filter_list:
        k = key.encode("utf-8")[:31].ljust(32, b"\x00")
        # Single-value filter as DimensionFilterWire
        vals_buf = value.encode("utf-8")[:31].ljust(32, b"\x00")
        vals_buf = vals_buf.ljust(256, b"\x00")
        parts.append(_DIM_FILTER_WIRE.pack(k, 1, vals_buf))

    return b"".join(parts)


_REALTIME_PAYLOAD = struct.Struct("<Q 64s")  # account_id, metric_code[64]


def encode_usage_realtime(account_id: int, metric_code: str) -> bytes:
    mc = metric_code.encode("utf-8")[:63].ljust(64, b"\x00")
    return _REALTIME_PAYLOAD.pack(account_id, mc)


_AGG_VALUE = struct.Struct("<QQ Q Q Q q Q")  # sum_lo, sum_hi, count, max, last_value, last_ts, alert_flags


def decode_agg_value(data: bytes) -> tuple[int, int, int, int, int, int]:
    """Returns (sum, count, max, last_value, last_timestamp, alert_flags)."""
    sum_lo, sum_hi, count, max_val, last_value, last_ts, alert_flags = _AGG_VALUE.unpack_from(data)
    total_sum = (sum_hi << 64) | sum_lo
    return total_sum, count, max_val, last_value, last_ts, alert_flags


# ---- Breakdown query ----

# Wire layout:
#   account_id:u64 + period_start:i64 + period_end:i64
#   + metric_code:[64]u8 + group_by_count:u8 + _pad:[7]u8
#   + group_by_keys: group_by_count × [32]u8
_BREAKDOWN_BASE = struct.Struct("<Q q q 64s B 7x")


def encode_usage_query_breakdown(
    account_id: int,
    metric_code: str,
    period_start_ns: int,
    period_end_ns: int,
    group_by: list[str],
) -> bytes:
    if not group_by:
        raise ValueError("group_by must list at least one dimension key")
    if len(group_by) > 4:
        raise ValueError("group_by may not exceed 4 keys (MAX_FILTERS)")
    mc = metric_code.encode("utf-8")[:63].ljust(64, b"\x00")
    parts = [_BREAKDOWN_BASE.pack(
        account_id, period_start_ns, period_end_ns, mc, len(group_by),
    )]
    for key in group_by:
        parts.append(key.encode("utf-8")[:31].ljust(32, b"\x00"))
    return b"".join(parts)


BREAKDOWN_ENTRY_WIRE_SIZE = 320

# Layout per entry (matches BreakdownEntryWire in protocol.zig):
#   dims_count:u8 + _pad:[7]u8 + dim_keys:[4][32] + dim_values:[4][32]
#   + sum_lo:u64 + sum_hi:u64 + count:u64 + max:u64
#   + last_value:u64 + last_ts:i64 + alert_flags:u64
_BREAKDOWN_ENTRY = struct.Struct("<B 7x 128s 128s Q Q Q Q Q q Q")


def decode_usage_query_breakdown_response(
    payload: bytes,
) -> list[tuple[list[tuple[str, str]], int, int, int, int, int, int]]:
    """Decode a USAGE_QUERY_BREAKDOWN response.

    Returns a list of (dims, sum, count, max, last_value, last_ts, alert_flags)
    where dims is the list of (key, value) pairs identifying the cell.
    """
    out = []
    off = 0
    while off + BREAKDOWN_ENTRY_WIRE_SIZE <= len(payload):
        (dims_count, keys_blob, values_blob,
         sum_lo, sum_hi, count, max_val,
         last_value, last_ts, alert_flags) = _BREAKDOWN_ENTRY.unpack_from(payload, off)
        dims: list[tuple[str, str]] = []
        for i in range(dims_count):
            k = _null_str(keys_blob[i * 32:(i + 1) * 32])
            v = _null_str(values_blob[i * 32:(i + 1) * 32])
            dims.append((k, v))
        total_sum = (sum_hi << 64) | sum_lo
        out.append((dims, total_sum, count, max_val, last_value, last_ts, alert_flags))
        off += BREAKDOWN_ENTRY_WIRE_SIZE
    return out


# ---- Events list ----

_EVENTS_LIST_REQ = struct.Struct("<Q q q")  # account_id, since_ns, until_ns


def encode_events_list(account_id: int, since_ns: int, until_ns: int) -> bytes:
    return _EVENTS_LIST_REQ.pack(account_id, since_ns, until_ns)


# ---- Alerts list / push ----

_ALERTS_LIST_REQ = struct.Struct("<Q Q")  # account_id, since_offset


def encode_alerts_list(account_id: int, since_offset: int) -> bytes:
    return _ALERTS_LIST_REQ.pack(account_id, since_offset)


# AlertRecordWire layout (must match Zig wire format and Go SDK):
#   Offset(u64) + AccountID(u64) + MetricCode[64] + ThresholdCode[32]
#   + ValueAtCross(u64) + TriggeredAt(i64) + _pad[8]  = 136 bytes
_ALERT_RECORD_WIRE = struct.Struct("<Q Q 64s 32s Q q 8x")
ALERT_RECORD_WIRE_SIZE = 136

# AlertPushPayload = AlertRecordWire(136) + NodeID(u8) + _pad[7] = 144 bytes
ALERT_PUSH_PAYLOAD_SIZE = ALERT_RECORD_WIRE_SIZE + 8


def _null_str(b: bytes) -> str:
    return b.split(b"\x00", 1)[0].decode("utf-8", errors="replace")


def decode_alert_record(buf: bytes, offset: int = 0) -> tuple[int, int, str, str, int, int]:
    """Decode one AlertRecordWire entry.

    Returns (log_offset, account_id, metric_code, threshold_code,
    value_at_cross, triggered_at_ns).
    """
    log_offset, account_id, mc, tc, vac, tat = _ALERT_RECORD_WIRE.unpack_from(buf, offset)
    return log_offset, account_id, _null_str(mc), _null_str(tc), vac, tat


def decode_alerts_list_response(payload: bytes) -> list[tuple[int, int, str, str, int, int]]:
    """Decode an ALERTS_LIST response into a list of records."""
    records = []
    off = 0
    while off + ALERT_RECORD_WIRE_SIZE <= len(payload):
        records.append(decode_alert_record(payload, off))
        off += ALERT_RECORD_WIRE_SIZE
    return records


def decode_alert_push(payload: bytes) -> tuple[int, int, int, str, str, int, int]:
    """Decode an ALERT_PUSH broadcast payload (144 bytes).

    Returns (node_id, log_offset, account_id, metric_code,
    threshold_code, value_at_cross, triggered_at_ns).
    """
    rec = decode_alert_record(payload, 0)
    node_id = payload[ALERT_RECORD_WIRE_SIZE]
    return (node_id, *rec)


def encode_alert_push_enable() -> bytes:
    """ALERT_PUSH_ENABLE has an empty payload — header alone."""
    return b""


# ---- Partition map ----

def decode_partition_map(data: bytes) -> list[tuple[str, str, str]]:
    """Decode partition map payload into list of (leader, replica0, replica1) address strings."""
    entries = []
    entry_size = ADDR_LEN * 4  # leader + replica0 + replica1 + pad
    for i in range(256):
        off = i * entry_size
        leader = data[off:off + ADDR_LEN].split(b"\x00")[0].decode("utf-8")
        r0 = data[off + ADDR_LEN:off + 2 * ADDR_LEN].split(b"\x00")[0].decode("utf-8")
        r1 = data[off + 2 * ADDR_LEN:off + 3 * ADDR_LEN].split(b"\x00")[0].decode("utf-8")
        entries.append((leader, r0, r1))
    return entries
