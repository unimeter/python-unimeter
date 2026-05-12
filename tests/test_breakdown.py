"""Wire format tests for USAGE_QUERY_BREAKDOWN."""

from __future__ import annotations

import struct

import pytest

from unimeter.protocol import (
    BREAKDOWN_ENTRY_WIRE_SIZE,
    PacketType,
    decode_usage_query_breakdown_response,
    encode_usage_query_breakdown,
)


def test_packet_type_constant():
    assert int(PacketType.USAGE_QUERY_BREAKDOWN) == 0x36


def test_encode_payload_layout():
    """Payload: account+period+metric base (96B) + group_by_count×32B keys."""
    payload = encode_usage_query_breakdown(
        account_id=42,
        metric_code="api_calls",
        period_start_ns=1_000_000,
        period_end_ns=2_000_000,
        group_by=["model", "token_type"],
    )
    assert len(payload) == 96 + 2 * 32

    account_id, period_start, period_end, mc_blob, group_by_count = (
        struct.unpack_from("<Q q q 64s B 7x", payload, 0)
    )
    assert account_id == 42
    assert period_start == 1_000_000
    assert period_end == 2_000_000
    assert mc_blob.split(b"\x00", 1)[0] == b"api_calls"
    assert group_by_count == 2

    key0 = payload[96:128].split(b"\x00", 1)[0]
    key1 = payload[128:160].split(b"\x00", 1)[0]
    assert key0 == b"model"
    assert key1 == b"token_type"


def test_encode_rejects_empty_group_by():
    with pytest.raises(ValueError):
        encode_usage_query_breakdown(
            account_id=1, metric_code="m",
            period_start_ns=0, period_end_ns=1, group_by=[],
        )


def test_encode_rejects_too_many_keys():
    with pytest.raises(ValueError):
        encode_usage_query_breakdown(
            account_id=1, metric_code="m",
            period_start_ns=0, period_end_ns=1,
            group_by=["a", "b", "c", "d", "e"],
        )


def _build_entry(
    dims: list[tuple[str, str]],
    total_sum: int = 1500,
    count: int = 3,
    max_val: int = 800,
    last_value: int = 500,
    last_ts: int = 1_700_000_000_000_000_000,
    alert_flags: int = 0,
) -> bytes:
    keys = b""
    values = b""
    for i in range(4):
        if i < len(dims):
            k, v = dims[i]
            keys += k.encode().ljust(32, b"\x00")
            values += v.encode().ljust(32, b"\x00")
        else:
            keys += b"\x00" * 32
            values += b"\x00" * 32
    sum_lo = total_sum & ((1 << 64) - 1)
    sum_hi = total_sum >> 64
    return struct.pack(
        "<B 7x 128s 128s Q Q Q Q Q q Q",
        len(dims), keys, values,
        sum_lo, sum_hi, count, max_val,
        last_value, last_ts, alert_flags,
    )


def test_decode_single_entry():
    buf = _build_entry([("model", "opus"), ("token_type", "input")], total_sum=1500)
    assert len(buf) == BREAKDOWN_ENTRY_WIRE_SIZE

    entries = decode_usage_query_breakdown_response(buf)
    assert len(entries) == 1
    dims, total_sum, count, max_val, last_value, last_ts, alert_flags = entries[0]
    assert dims == [("model", "opus"), ("token_type", "input")]
    assert total_sum == 1500
    assert count == 3
    assert max_val == 800


def test_decode_sparse_multiple_entries():
    buf = (_build_entry([("model", "opus"), ("token_type", "input")],  total_sum=1000)
         + _build_entry([("model", "opus"), ("token_type", "output")], total_sum=2000)
         + _build_entry([("model", "sonnet"), ("token_type", "input")], total_sum=8000))

    entries = decode_usage_query_breakdown_response(buf)
    assert len(entries) == 3
    sums = [e[1] for e in entries]
    assert sums == [1000, 2000, 8000]


def test_decode_handles_u128_sum():
    """SumLo/SumHi reassemble correctly for values > 2^64."""
    big = (1 << 70) + 12345
    buf = _build_entry([("k", "v")], total_sum=big)
    entries = decode_usage_query_breakdown_response(buf)
    assert entries[0][1] == big


def test_decode_ignores_trailing_garbage():
    buf = _build_entry([("k", "v")]) + b"\x00" * 50
    assert len(decode_usage_query_breakdown_response(buf)) == 1
