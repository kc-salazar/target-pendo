"""Reproducible local throughput benchmark for target-pendo's request scheduler.

This benchmark never contacts Pendo. It models fixed per-request network/service
latency so serial and bounded-parallel scheduling can be compared deterministically.
"""

from __future__ import annotations

import argparse
import json
import platform
import statistics
import threading
import time
from dataclasses import asdict, dataclass
from typing import Any

from target_pendo.batching import JsonBatch, iter_encoded_json_batches
from target_pendo.client import PendoBulkResult
from target_pendo.mapping import StreamMapping, map_and_coalesce_records
from target_pendo.writer import BulkBatchWriter


class LatencyClient:
    """In-memory client with a fixed delay per request."""

    def __init__(self, latency_seconds: float) -> None:
        self.latency_seconds = latency_seconds
        self._lock = threading.Lock()
        self.active = 0
        self.max_active = 0

    def update_metadata(
        self,
        *,
        kind: str,
        group: str,
        batch: JsonBatch,
        create_missing: bool,
    ) -> PendoBulkResult:
        del group, create_missing
        with self._lock:
            self.active += 1
            self.max_active = max(self.max_active, self.active)
        try:
            time.sleep(self.latency_seconds)
            return PendoBulkResult(
                total=len(batch),
                updated=len(batch),
                failed=0,
                kind=kind,
            )
        finally:
            with self._lock:
                self.active -= 1


@dataclass(frozen=True, slots=True)
class Result:
    concurrency: int
    median_seconds: float
    records_per_second: float
    requests: int
    max_active_requests: int
    speedup_vs_serial: float


def make_records(count: int, field_count: int) -> list[dict[str, Any]]:
    return [
        {
            "source_id": f"visitor-{index:09d}",
            "attributes": {
                f"source_field_{field_index}": f"value-{field_index:02d}"
                for field_index in range(field_count)
            },
        }
        for index in range(count)
    ]


def make_mapping(field_count: int) -> StreamMapping:
    """Build the same compiled mapping used by the target hot path."""
    return StreamMapping.from_config(
        "benchmark_stream",
        {
            "kind": "visitor",
            "id_field": "source_id",
            "field_mappings": {
                f"field_{index}": f"/attributes/source_field_{index}"
                for index in range(field_count)
            },
        },
        default_create_missing=False,
    )


def run_once(
    records: list[dict[str, Any]],
    mapping: StreamMapping,
    *,
    request_batch_size: int,
    max_batch_bytes: int,
    latency_seconds: float,
    concurrency: int,
) -> tuple[float, int, int]:
    client = LatencyClient(latency_seconds)
    writer = BulkBatchWriter(client, concurrency=concurrency)
    started_at = time.perf_counter()
    try:
        mapped = map_and_coalesce_records(records, mapping, deduplicate=True)
        result = writer.write(
            iter_encoded_json_batches(
                mapped.records,
                max_records=request_batch_size,
                max_bytes=max_batch_bytes,
            ),
            kind="visitor",
            group="custom",
            create_missing=False,
        )
    finally:
        writer.close()
    return time.perf_counter() - started_at, result.requests, client.max_active


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--records", type=int, default=50_000)
    parser.add_argument("--fields", type=int, default=8)
    parser.add_argument("--request-batch-size", type=int, default=2_500)
    parser.add_argument("--max-batch-bytes", type=int, default=3_000_000)
    parser.add_argument("--latency-ms", type=float, default=25.0)
    parser.add_argument("--concurrency", type=int, nargs="+", default=[1, 2, 4, 8])
    parser.add_argument("--repeats", type=int, default=3)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if 1 not in args.concurrency:
        raise SystemExit("--concurrency must include 1 so speedup has a serial baseline")
    if args.records < 1 or args.request_batch_size < 1 or args.repeats < 1:
        raise SystemExit("records, request batch size, and repeats must be positive")
    records = make_records(args.records, args.fields)
    mapping = make_mapping(args.fields)
    raw: list[tuple[int, float, int, int]] = []
    for concurrency in args.concurrency:
        samples = [
            run_once(
                records,
                mapping,
                request_batch_size=args.request_batch_size,
                max_batch_bytes=args.max_batch_bytes,
                latency_seconds=args.latency_ms / 1_000,
                concurrency=concurrency,
            )
            for _ in range(args.repeats)
        ]
        median_seconds = statistics.median(sample[0] for sample in samples)
        raw.append((concurrency, median_seconds, samples[0][1], max(s[2] for s in samples)))

    serial_seconds = next(seconds for concurrency, seconds, _, _ in raw if concurrency == 1)
    results = [
        Result(
            concurrency=concurrency,
            median_seconds=round(seconds, 6),
            records_per_second=round(args.records / seconds, 1),
            requests=request_count,
            max_active_requests=max_active,
            speedup_vs_serial=round(serial_seconds / seconds, 2),
        )
        for concurrency, seconds, request_count, max_active in raw
    ]
    output = {
        "benchmark": (
            "mapping, duplicate coalescing, JSON encoding, and bounded scheduling "
            "with simulated fixed-latency bulk requests; no Pendo traffic"
        ),
        "python": platform.python_version(),
        "platform": platform.platform(),
        "parameters": {
            "records": args.records,
            "fields_per_record": args.fields,
            "request_batch_size": args.request_batch_size,
            "max_batch_bytes": args.max_batch_bytes,
            "simulated_latency_ms": args.latency_ms,
            "repeats": args.repeats,
        },
        "results": [asdict(result) for result in results],
    }
    print(json.dumps(output, indent=2))


if __name__ == "__main__":
    main()
