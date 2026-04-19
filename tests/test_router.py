"""Partition routing tests."""

from unimeter.router import Router, PARTITION_COUNT


def test_partition_of_deterministic():
    assert Router.partition_of(0) == 0
    assert Router.partition_of(1) == 1
    assert Router.partition_of(255) == 255
    assert Router.partition_of(256) == 0
    assert Router.partition_of(12345) == 12345 % PARTITION_COUNT


def test_partition_of_consistency():
    """Same account_id always routes to the same partition."""
    for account_id in [0, 1, 42, 999, 12345, 2**32, 2**63]:
        p1 = Router.partition_of(account_id)
        p2 = Router.partition_of(account_id)
        assert p1 == p2
        assert 0 <= p1 < PARTITION_COUNT
