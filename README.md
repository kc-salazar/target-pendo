# target-pendo

`target-pendo` is a Singer target that bulk-loads visitor and account metadata
into Pendo. It is a destination connector, so it can consume Singer messages
from `tap-redshift` or any other Singer-compatible tap without bundling or
forking the source connector.

The target uses Pendo's documented bulk metadata endpoint:

```text
POST /api/v1/metadata/{visitor|account}/{agent|custom}/value
```

## Design goals

- Reliable Singer state handling: state advances only after Pendo accepts the batch.
- Explicit stream mappings rather than assumptions tied to one warehouse schema.
- Exact request-size and row-count bounds.
- Connection pooling and bounded cross-stream concurrency.
- Retries for timeouts, rate limits, and transient server failures.
- `Retry-After` support and exponential backoff with jitter.
- Strict validation of Pendo's bulk response, including partial failures.
- No record payloads, IDs, or integration keys in logs.
- Support for every current Pendo region through `base_url` configuration.

## Installation

```bash
uv tool install target-pendo
```

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
  "batch_size_rows": 1000,
  "max_batch_bytes": 3000000,
  "max_parallelism": 4,
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

## Development

```bash
uv sync --all-groups
uv run pytest
uv run ruff check .
uv run ruff format --check .
uv run mypy
```

No live Pendo account is required for the test suite; HTTP behavior is tested
against an in-memory transport.
