# Performance methodology

The relevant performance unit is a Pendo bulk request, not an individual Singer
record. Pendo documents that any number of records may be submitted, but each call
has a five-minute execution limit. The target therefore batches records and overlaps
independent requests while preserving a strict global concurrency ceiling.

## Reproduce the local benchmark

```bash
uv run python benchmarks/benchmark_throughput.py
```

The benchmark includes compiled field mapping, same-ID coalescing, JSON batch
construction, and the actual `BulkBatchWriter`. It uses an in-memory client with
fixed request latency; it never sends traffic to Pendo. This makes hot-path scheduler
comparisons reproducible without an integration key or unsafe load testing against a
customer's subscription.

The output reports median wall time, records per second, observed concurrency, and
speedup over the serial path. Results depend on the machine and chosen latency model.

## Recorded development result

Command run on 2026-10-17:

```bash
uv run python benchmarks/benchmark_throughput.py \
  --latency-ms 100 --concurrency 1 2 4 8 16 32
```

Environment: Python 3.12.13 on Windows 11; 50,000 records; eight metadata fields per
record; 2,500 records/request; 20 requests; three repetitions.

| Concurrency | Median seconds | Records/second | Speedup vs serial |
| ---: | ---: | ---: | ---: |
| 1 | 2.615 | 19,123 | 1.00x |
| 2 | 1.519 | 32,910 | 1.72x |
| 4 | 1.027 | 48,668 | 2.55x |
| 8 | 0.786 | 63,642 | 3.33x |
| 16 | 0.675 | 74,120 | 3.88x |
| 32 | 0.696 | 71,840 | 3.76x |

Only 20 requests existed in this workload. Serialization and request scheduling
overlapped, so the 32-worker case peaked at 14 active requests before earlier requests
completed. The default remains 8 because a local latency model cannot establish a safe
live Pendo rate. Higher settings are available for controlled subscription-specific
tests.

## What the benchmark proves

It can establish that bounded parallel scheduling removes avoidable serial wait time,
that configured concurrency is actually enforced, and that JSON preparation does not
erase the gain. It cannot establish Pendo's account-specific service capacity or rate
limit. A real end-to-end performance claim requires a dedicated Pendo subscription,
representative metadata, an agreed request budget, and server-observed validation.

## Tuning

- `batch_size_rows` controls the Singer buffer. Larger buffers expose more requests
  to the parallel scheduler but use more memory.
- `request_batch_size_rows` controls records per Pendo request.
- `max_batch_bytes` is an exact encoded-payload ceiling.
- `request_concurrency` bounds active HTTP calls globally across all streams.
- `max_parallelism` bounds how many Singer stream sinks drain concurrently.

Start with the defaults. Increase `request_concurrency` only while Pendo latency falls
or throughput rises without sustained `429` responses. A `408` or `413` is split into
smaller requests automatically; other failures remain fatal so Singer state cannot
advance past an unconfirmed write.
