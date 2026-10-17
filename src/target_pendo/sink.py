"""Singer batch sink for Pendo metadata."""

from __future__ import annotations

import time
from collections.abc import Sequence
from typing import Any

from singer_sdk import BatchSink, Target

from target_pendo.batching import iter_encoded_json_batches
from target_pendo.client import PendoClient
from target_pendo.mapping import StreamMapping, map_and_coalesce_records
from target_pendo.writer import BulkBatchWriter


class PendoSink(BatchSink):
    """Map and write one Singer stream to one Pendo metadata endpoint."""

    MAX_SIZE_DEFAULT = 50_000

    def __init__(
        self,
        target: Target,
        stream_name: str,
        schema: dict[str, Any],
        key_properties: Sequence[str] | None,
    ) -> None:
        self._target = target
        super().__init__(target, stream_name, schema, key_properties)

    def setup(self) -> None:
        """Validate stream configuration and initialize the API client."""
        super().setup()
        streams = self.config["streams"]
        self.mapping = StreamMapping.from_config(
            self.stream_name,
            streams[self.stream_name],
            default_create_missing=bool(self.config.get("create_missing", False)),
        )
        self.client = PendoClient(
            integration_key=str(self.config["integration_key"]),
            base_url=str(self.config.get("base_url", "https://app.pendo.io")),
            request_timeout_seconds=float(self.config.get("request_timeout_seconds", 300)),
            max_retries=int(self.config.get("max_retries", 5)),
            backoff_base_seconds=float(self.config.get("backoff_base_seconds", 1)),
            backoff_max_seconds=float(self.config.get("backoff_max_seconds", 60)),
            http2=bool(self.config.get("http2", True)),
            max_connections=int(self.config.get("request_concurrency", 8)),
            request_limiter=getattr(self._target, "request_limiter", None),
            logger=self.logger,
        )
        self.writer = BulkBatchWriter(
            self.client,
            concurrency=int(self.config.get("request_concurrency", 8)),
        )

    def process_batch(self, context: dict[str, Any]) -> None:
        """Map records and send bounded requests to Pendo."""
        source_records = context.get("records", [])
        started_at = time.perf_counter()
        mapped = map_and_coalesce_records(
            source_records,
            self.mapping,
            deduplicate=bool(self.config.get("deduplicate_records", True)),
        )
        batches = iter_encoded_json_batches(
            mapped.records,
            max_records=int(self.config.get("request_batch_size_rows", 2_500)),
            max_bytes=int(self.config.get("max_batch_bytes", 3_000_000)),
        )
        result = self.writer.write(
            batches,
            kind=self.mapping.kind,
            group=self.mapping.group,
            create_missing=self.mapping.create_missing,
        )
        elapsed = time.perf_counter() - started_at
        throughput = result.updated / elapsed if elapsed else 0.0

        self.logger.info(
            "Pendo batch completed: input=%d unique=%d updated=%d skipped=%d "
            "duplicates=%d requests=%d timeout_splits=%d seconds=%.3f records_per_second=%.1f",
            len(source_records),
            len(mapped.records),
            result.updated,
            mapped.skipped,
            mapped.duplicates,
            result.requests,
            result.timeout_splits,
            elapsed,
            throughput,
        )

    def clean_up(self) -> None:
        """Close the Pendo HTTP connection pool."""
        self.writer.close()
        self.client.close()
