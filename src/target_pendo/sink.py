"""Singer batch sink for Pendo metadata."""

from __future__ import annotations

from typing import Any

from singer_sdk import BatchSink

from target_pendo.batching import iter_json_batches
from target_pendo.client import PendoClient
from target_pendo.mapping import StreamMapping


class PendoSink(BatchSink):
    """Map and write one Singer stream to one Pendo metadata endpoint."""

    MAX_SIZE_DEFAULT = 1_000

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
            logger=self.logger,
        )

    def process_batch(self, context: dict[str, Any]) -> None:
        """Map records and send bounded requests to Pendo."""
        source_records = context.get("records", [])
        mapped_records: list[dict[str, Any]] = []
        skipped = 0
        for record in source_records:
            mapped = self.mapping.map_record(record)
            if mapped is None:
                skipped += 1
            else:
                mapped_records.append(mapped)

        request_count = 0
        updated_count = 0
        for batch in iter_json_batches(
            mapped_records,
            max_records=self.max_size,
            max_bytes=int(self.config.get("max_batch_bytes", 3_000_000)),
        ):
            result = self.client.update_metadata(
                kind=self.mapping.kind,
                group=self.mapping.group,
                records=batch,
                create_missing=self.mapping.create_missing,
            )
            request_count += 1
            updated_count += result.updated

        self.logger.info(
            "Pendo batch completed: input=%d updated=%d skipped=%d requests=%d",
            len(source_records),
            updated_count,
            skipped,
            request_count,
        )

    def clean_up(self) -> None:
        """Close the Pendo HTTP connection pool."""
        self.client.close()
