# target-pendo

`target-pendo` is a source-independent Singer destination that bulk-loads visitor
and account metadata into Pendo. It accepts output from `tap-redshift` or any
other Singer-compatible tap; no source connector is bundled or forked.

The target uses Pendo's documented bulk metadata endpoint:

```text
POST /api/v1/metadata/{visitor|account}/{agent|custom}/value
```

Status: pre-release. The API contract, failure handling, concurrency bounds, and
packaging are covered by automated tests. A live benchmark against a dedicated
Pendo subscription has not yet been run.

## Project lineage

Carl Salazar originally built a Redshift-to-Pendo Singer integration in 2021. This
repository is a clean implementation of the destination side of that work: it keeps
the Pendo mapping, batching, and warehouse-to-Pendo purpose while separating the tap
from the target and replacing the original experimental request scheduler. The code
here is new; the connector's lineage is not.

## Why this target

A [public-ecosystem survey](docs/ecosystem-survey.md) on 2026-10-17 found Pendo
taps and one-off destination functions, but no other publicly discoverable Singer
target for loading metadata into Pendo. This implementation is designed around
Pendo's bulk endpoint rather than one-record-per-request delivery.

## Performance path

- Reliable Singer state handling: state advances only after Pendo accepts the batch.
- Explicit stream mappings rather than assumptions tied to one warehouse schema.
- Compiled source selectors and same-ID coalescing before serialization.
- One-pass, exact JSON encoding reused by the HTTP client.
- Exact request-size and row-count bounds.
- HTTP/2 connection pooling and bounded parallel bulk requests.
- One global request-concurrency ceiling across all Singer streams.
- Automatic request splitting after Pendo `408` or `413` responses.
- Retries for rate limits, transport errors, and transient server failures.
- `Retry-After` support and exponential backoff with jitter.
- Strict validation of Pendo's bulk response, including partial failures.
- No record payloads, IDs, or integration keys in logs.
- Support for every current Pendo region through `base_url` configuration.

The reproducible local hot-path benchmark maps and encodes 50,000 eight-field records
in a simulated 100 ms/request environment. On the development machine, bounded
concurrency 8 reached 63,642 records/second versus 19,123 records/second for the
serial path: a 3.33x speedup. Concurrency 16 reached 74,120 records/second and 3.88x,
but the default remains 8 until a live Pendo rate test can establish a safe ceiling.
These are not live Pendo throughput claims. See
[performance methodology and results](docs/performance.md).

## Installation

```bash
uv tool install git+https://github.com/kc-salazar/target-pendo.git
```

The project has not yet been released to PyPI.

For local development:

```bash
uv sync --all-groups
```

## Configuration

Copy `config.example.json` and provide a Pendo integration key. Do not commit
the populated file.

```json
{
  "integration_key": "...",
  "base_url": "https://app.pendo.io",
  "batch_size_rows": 50000,
  "request_batch_size_rows": 2500,
  "max_batch_bytes": 3000000,
  "request_concurrency": 8,
  "max_parallelism": 4,
  "http2": true,
  "deduplicate_records": true,
  "streams": {
    "warehouse_visitors": {
      "kind": "visitor",
      "group": "custom",
      "id_field": "user_id",
      "field_mappings": {
        "planName": "plan_name",
        "country": "/profile/country"
      }
    }
  }
}
```

`field_mappings` maps each Pendo metadata field to either a top-level source
field or an RFC 6901 JSON Pointer. For example, `/profile/country` reads the
nested `country` value from `record.profile`.

`batch_size_rows` is the in-memory Singer buffer. `request_batch_size_rows` and
`max_batch_bytes` independently bound each Pendo request. `request_concurrency`
is a global ceiling, even when multiple streams drain at the same time.

Pendo's current regional base URLs include:

- `https://app.pendo.io`
- `https://app.eu.pendo.io`
- `https://us1.app.pendo.io`
- `https://app.jpn.pendo.io`
- `https://app.au.pendo.io`

### Failure behavior

The safe defaults are:

- Missing visitor/account ID: fail the sync.
- Missing mapped source field: omit that field.
- Null mapped value: omit that field.
- Record with no remaining values: skip it and log only the count.
- Pendo partial failure: fail the batch and do not advance Singer state.
- Pendo `408` or `413`: split the request recursively and retry the smaller halves.

Each behavior can be changed per stream with `missing_id_behavior`,
`missing_field_behavior`, `null_value_behavior`, and
`empty_values_behavior`.

## Usage

```bash
tap-redshift --config tap.json --catalog catalog.json \
  | target-pendo --config target.json > state.json
```

The target is not coupled to Redshift. Any tap that emits the configured
streams and fields can be used.

Writes are at-least-once. If one parallel request fails after other requests have
succeeded, Singer state is withheld and a rerun can repeat already accepted metadata
updates. Setting the same metadata value is idempotent at the Pendo endpoint.

## Development

```bash
uv sync --all-groups
uv run pytest
uv run ruff check .
uv run ruff format --check .
uv run mypy
uv build
uv run python benchmarks/benchmark_throughput.py
```

No live Pendo account is required for the test suite or local benchmark. HTTP
behavior is tested against an in-memory transport.
