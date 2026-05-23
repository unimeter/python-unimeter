# python-unimeter

Python client for the [Unimeter](https://github.com/unimeter/unimeter) usage metering engine. Async-first design built on `asyncio` — connects to your cluster, routes requests to the right node, and handles failover automatically.

Zero external dependencies. Standard library only.

---

## Quick start

```python
import asyncio
from unimeter import AsyncClient, Event, MetricSchema, AggType, PeriodType, current_month

async def main():
    async with AsyncClient(["localhost:7001"]) as client:
        # Define a metric
        await client.metrics.create(MetricSchema(
            code="api_calls",
            agg_type=AggType.COUNT,
            period_type=PeriodType.CALENDAR,
            billing_cycle_day=1,
        ))

        # Record usage
        await client.ingest([
            Event(account_id=42, metric_code="api_calls", value=1),
            Event(account_id=42, metric_code="api_calls", value=1),
        ])

        # Query
        result = await client.query(42, "api_calls", current_month())
        print(f"API calls: {result.value.count}")

asyncio.run(main())
```

---

## Installation

```bash
pip install unimeter-python
```

Requires Python 3.11 or later.

---

## API reference

### Connect

```python
from unimeter import AsyncClient

# As a context manager (recommended)
async with AsyncClient(["node0:7001", "node1:7001"]) as client:
    ...

# Or manually
client = AsyncClient(["node0:7001"])
await client.connect()
# ...
await client.close()
```

---

### Ingest events

```python
from unimeter import Event, DeliveryMode

result = await client.ingest([
    Event(account_id=42, metric_code="api_calls", value=1),
    Event(account_id=99, metric_code="api_calls", value=1),
])
print(result.n_stored, result.n_duplicates)
```

Events for different accounts are routed to the correct nodes in parallel. Duplicates are detected and discarded automatically.

| Delivery mode | Behavior |
|------|-----------|
| `DeliveryMode.ASYNC` (default) | Returns immediately; data flushed in background |
| `DeliveryMode.SYNC` | Waits for durable write before returning |

---

### Query usage

```python
from unimeter import current_month, last_month, current_billing_period

result = await client.query(42, "api_calls", current_month())
print(result.value.count)
```

**Period helpers:**

```python
current_month()              # first of this month to first of next
last_month()                 # previous calendar month
current_billing_period(15)   # current period starting on the 15th
last_billing_period(15)      # previous period starting on the 15th
```

**Filtering by dimension:**

```python
# Single dimension
result = await client.query(42, "compute_seconds", current_month(),
    filters={"provider": "aws"})

# AND query across multiple dimensions
result = await client.query(42, "compute_seconds", current_month(),
    filters={"provider": "aws", "region": "us-east"})
```

---

### Real-time query

```python
agg = await client.query_realtime(42, "api_calls")
print(agg.count)
```

---

### Raw events and alert history

```python
events = await client.list_events(42, since, until)
alerts = await client.list_alerts(42, since_offset=0)
```

---

### Metric management

```python
from unimeter import MetricSchema, AggType, PeriodType, DimensionFilter, AlertThreshold

await client.metrics.create(MetricSchema(
    code="compute_seconds",
    agg_type=AggType.SUM,
    period_type=PeriodType.CALENDAR,
    billing_cycle_day=1,
    filters=[
        DimensionFilter(key="provider", values=["aws", "gcp", "azure"]),
    ],
    thresholds=[
        AlertThreshold(code="soft_cap", value=100_000),
    ],
))

await client.metrics.update(schema)
await client.metrics.delete("compute_seconds")
schemas = await client.metrics.list()
```

**Aggregation types:** `COUNT`, `SUM`, `MAX`, `LATEST`, `COUNT_UNIQUE`

**Period types:** `FIXED` (default), `CALENDAR`

---

### COUNT UNIQUE and OperationType

```python
from unimeter import OperationType

await client.ingest([
    Event(account_id=42, metric_code="active_seats",
          value=user_id, operation_type=OperationType.ADD),
])
```

Use `ADD` / `REMOVE` with a COUNT UNIQUE metric to track active members (seats, users, devices).

---

### Value scaling

All values are scaled integers with 6 decimal places of precision.

```python
from unimeter import scale, unscale

scale(1.5)         # 1_500_000
unscale(1_500_000) # 1.5
```

---

## Examples

Working examples are in [unimeter/examples](https://github.com/unimeter/examples):

| Example | Demonstrates |
|---------|-------------|
| `python/saas-api.py` | Per-request counter, monthly query |
| `python/infra-metering.py` | SUM with provider/region filters |
| `python/seat-based.py` | COUNT UNIQUE active seats, entitlement gate |
| `python/high-throughput.py` | Buffered async ingest |
| `python/free-tier-alerts.py` | Alert thresholds and enforcement |
| `python/stripe-integration.py` | Stripe webhook simulation → invoice |

---

## Documentation

- [Python SDK reference](https://unimeter.io/sdk/python/)
- [Quickstart](https://unimeter.io/quickstart/)
- [Stripe integration guide](https://unimeter.io/guides/stripe/)

## License

[O'SaaSy](LICENSE.md)
