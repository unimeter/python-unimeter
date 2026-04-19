from __future__ import annotations

import enum
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

SCALE = 1_000_000
PARTITION_COUNT = 256


def scale(value: float) -> int:
    return round(value * SCALE)


def unscale(value: int) -> float:
    return value / SCALE


class DeliveryMode(enum.IntEnum):
    ASYNC = 0
    SYNC = 1


class OperationType(enum.IntEnum):
    NONE = 0
    ADD = 1
    REMOVE = 2


class AggType(enum.IntEnum):
    COUNT = 0
    SUM = 1
    MAX = 2
    LATEST = 3
    COUNT_UNIQUE = 4


class PeriodType(enum.IntEnum):
    FIXED = 0
    CALENDAR = 1


@dataclass
class Event:
    account_id: int
    metric_code: str
    value: int = 1
    timestamp: Optional[datetime] = None
    operation_type: OperationType = OperationType.NONE
    delivery_mode: DeliveryMode = DeliveryMode.ASYNC
    properties: dict[str, str] = field(default_factory=dict)

    def timestamp_ns(self) -> int:
        if self.timestamp is None:
            return 0
        return int(self.timestamp.timestamp() * 1_000_000_000)


@dataclass
class IngestResult:
    n_stored: int
    n_duplicates: int
    last_offset: int


@dataclass
class AggValue:
    sum: int = 0
    count: int = 0
    max: int = 0
    last_value: int = 0
    last_timestamp: int = 0
    alert_flags: int = 0


@dataclass
class UsageResult:
    value: AggValue
    period_start: Optional[datetime] = None
    period_end: Optional[datetime] = None


@dataclass
class DimensionFilter:
    key: str
    values: list[str]


@dataclass
class AlertThreshold:
    code: str
    value: int
    recurring: bool = False


@dataclass
class MetricSchema:
    code: str
    agg_type: AggType
    recurring: bool = False
    field_name: str = ""
    period_type: PeriodType = PeriodType.FIXED
    billing_cycle_day: int = 1  # 1-28; day of month when billing period starts (calendar only)
    filters: list[DimensionFilter] = field(default_factory=list)
    thresholds: list[AlertThreshold] = field(default_factory=list)


@dataclass
class Period:
    start: datetime
    end: datetime


def current_month() -> Period:
    now = datetime.now(timezone.utc)
    start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if now.month == 12:
        end = start.replace(year=now.year + 1, month=1)
    else:
        end = start.replace(month=now.month + 1)
    return Period(start=start, end=end)


def last_month() -> Period:
    now = datetime.now(timezone.utc)
    end = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if end.month == 1:
        start = end.replace(year=end.year - 1, month=12)
    else:
        start = end.replace(month=end.month - 1)
    return Period(start=start, end=end)


def current_billing_period(cycle_day: int = 1) -> Period:
    """Current billing period for a custom cycle start day (1-28)."""
    cycle_day = max(1, min(28, cycle_day))
    now = datetime.now(timezone.utc)
    y, m, d = now.year, now.month, now.day
    if d >= cycle_day:
        start = now.replace(day=cycle_day, hour=0, minute=0, second=0, microsecond=0)
        nm = m + 1 if m < 12 else 1
        ny = y if m < 12 else y + 1
        end = start.replace(year=ny, month=nm)
    else:
        pm = m - 1 if m > 1 else 12
        py = y if m > 1 else y - 1
        start = now.replace(year=py, month=pm, day=cycle_day, hour=0, minute=0, second=0, microsecond=0)
        end = now.replace(day=cycle_day, hour=0, minute=0, second=0, microsecond=0)
    return Period(start=start, end=end)


def last_billing_period(cycle_day: int = 1) -> Period:
    """Previous billing period for a custom cycle start day (1-28)."""
    current = current_billing_period(cycle_day)
    end = current.start
    y, m = end.year, end.month
    pm = m - 1 if m > 1 else 12
    py = y if m > 1 else y - 1
    start = end.replace(year=py, month=pm)
    return Period(start=start, end=end)


@dataclass
class EventRecord:
    account_id: int
    metric_code: str
    value: int
    operation_type: OperationType
    timestamp: datetime
    offset: int


@dataclass
class AlertRecord:
    node_addr: str
    log_offset: int
    account_id: int
    metric_code: str
    threshold_code: str
    value_at_cross: int
    triggered_at: datetime
