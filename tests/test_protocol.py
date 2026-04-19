"""Wire format encoding/decoding tests."""

import struct

from unimeter.protocol import (
    MAGIC,
    VERSION,
    PacketType,
    StatusCode,
    decode_agg_value,
    decode_ingest_response,
    decode_response_header,
    encode_ingest_payload,
    encode_metric_delete,
    encode_metric_put,
    encode_request_header,
    encode_usage_query,
    encode_usage_realtime,
)


def test_request_header_magic():
    hdr = encode_request_header(PacketType.INGEST_ASYNC, 100, 42)
    magic = struct.unpack_from("<I", hdr)[0]
    assert magic == MAGIC


def test_request_header_fields():
    hdr = encode_request_header(PacketType.USAGE_QUERY, 96, 7, partition=5)
    magic, version, ptype, partition, payload_len, req_id = struct.unpack("<IBBHII", hdr)
    assert magic == MAGIC
    assert version == VERSION
    assert ptype == PacketType.USAGE_QUERY
    assert partition == 5
    assert payload_len == 96
    assert req_id == 7


def test_response_header_decode():
    data = struct.pack("<B3xII", 0, 42, 56)
    status, req_id, plen = decode_response_header(data)
    assert status == StatusCode.OK
    assert req_id == 42
    assert plen == 56


def test_ingest_payload_single_event():
    events = [("api_calls", 42, 0, 1, 0, 0, [])]
    payload = encode_ingest_payload(events)
    # First 8 bytes: event_count=1, props_count=0
    ec, pc = struct.unpack_from("<II", payload)
    assert ec == 1
    assert pc == 0
    # Wire event starts at offset 8, metric_code is first 64 bytes
    mc = payload[8:72].split(b"\x00")[0].decode()
    assert mc == "api_calls"


def test_ingest_payload_with_props():
    events = [("compute", 1, 0, 100, 0, 1, [("region", "us-east")])]
    payload = encode_ingest_payload(events)
    ec, pc = struct.unpack_from("<II", payload)
    assert ec == 1
    assert pc == 1


def test_ingest_response_decode():
    data = struct.pack("<IIQ", 10, 2, 999)
    stored, dups, offset = decode_ingest_response(data)
    assert stored == 10
    assert dups == 2
    assert offset == 999


def test_agg_value_decode():
    data = struct.pack("<QQQQQ q Q", 5000, 0, 10, 500, 100, 1234567890, 0)
    s, c, m, lv, lt, af = decode_agg_value(data)
    assert s == 5000
    assert c == 10
    assert m == 500
    assert lv == 100


def test_metric_put_encode():
    payload = encode_metric_put("api_calls", 0)
    # code is first 64 bytes
    code = payload[:64].split(b"\x00")[0].decode()
    assert code == "api_calls"
    assert payload[64] == 0  # agg_type = COUNT


def test_metric_delete_encode():
    payload = encode_metric_delete("api_calls")
    assert len(payload) == 64
    code = payload.split(b"\x00")[0].decode()
    assert code == "api_calls"


def test_usage_query_encode():
    payload = encode_usage_query(42, "api_calls", 1000, 2000)
    account_id = struct.unpack_from("<Q", payload)[0]
    assert account_id == 42


def test_usage_realtime_encode():
    payload = encode_usage_realtime(42, "api_calls")
    account_id = struct.unpack_from("<Q", payload)[0]
    assert account_id == 42
    mc = payload[8:72].split(b"\x00")[0].decode()
    assert mc == "api_calls"
