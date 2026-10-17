"""Singer target entry point."""

from __future__ import annotations

import threading
from collections.abc import Mapping
from typing import Any, ClassVar

from singer_sdk import Target

from target_pendo.exceptions import ConfigurationError
from target_pendo.mapping import StreamMapping
from target_pendo.sink import PendoSink

_STREAM_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["kind", "id_field", "field_mappings"],
    "properties": {
        "kind": {"type": "string", "enum": ["visitor", "account"]},
        "group": {"type": "string", "enum": ["agent", "custom"]},
        "id_field": {"type": "string", "minLength": 1},
        "field_mappings": {
            "type": "object",
            "minProperties": 1,
            "additionalProperties": {"type": "string", "minLength": 1},
        },
        "create_missing": {"type": "boolean"},
        "missing_id_behavior": {
            "type": "string",
            "enum": ["fail", "skip"],
        },
        "missing_field_behavior": {
            "type": "string",
            "enum": ["fail", "omit"],
        },
        "null_value_behavior": {
            "type": "string",
            "enum": ["fail", "omit", "send"],
        },
        "empty_values_behavior": {
            "type": "string",
            "enum": ["fail", "skip"],
        },
    },
}


class TargetPendo(Target):
    """Load Singer streams into Pendo account and visitor metadata."""

    name = "target-pendo"
    default_sink_class = PendoSink
    config_jsonschema: ClassVar[dict[str, Any]] = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "additionalProperties": True,
        "required": ["integration_key", "streams"],
        "properties": {
            "integration_key": {
                "type": "string",
                "minLength": 1,
                "secret": True,
            },
            "base_url": {
                "type": "string",
                "format": "uri",
                "default": "https://app.pendo.io",
            },
            "streams": {
                "type": "object",
                "minProperties": 1,
                "additionalProperties": _STREAM_SCHEMA,
            },
            "create_missing": {"type": "boolean", "default": False},
            "batch_size_rows": {
                "type": "integer",
                "minimum": 1,
                "default": 50_000,
            },
            "request_batch_size_rows": {
                "type": "integer",
                "minimum": 1,
                "default": 2_500,
            },
            "max_batch_bytes": {
                "type": "integer",
                "minimum": 2,
                "default": 3_000_000,
            },
            "request_timeout_seconds": {
                "type": "number",
                "exclusiveMinimum": 0,
                "default": 300,
            },
            "max_retries": {
                "type": "integer",
                "minimum": 0,
                "default": 5,
            },
            "backoff_base_seconds": {
                "type": "number",
                "minimum": 0,
                "default": 1,
            },
            "backoff_max_seconds": {
                "type": "number",
                "minimum": 0,
                "default": 60,
            },
            "max_parallelism": {
                "type": "integer",
                "minimum": 1,
                "maximum": 8,
                "default": 4,
            },
            "request_concurrency": {
                "type": "integer",
                "minimum": 1,
                "maximum": 64,
                "default": 8,
            },
            "http2": {"type": "boolean", "default": True},
            "deduplicate_records": {"type": "boolean", "default": True},
            "validate_records": {"type": "boolean", "default": True},
        },
    }

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._validate_streams()
        self.max_parallelism = int(self.config.get("max_parallelism", 4))
        self.request_limiter = threading.BoundedSemaphore(
            int(self.config.get("request_concurrency", 8))
        )

    def _validate_streams(self) -> None:
        streams = self.config.get("streams")
        if not isinstance(streams, Mapping) or not streams:
            raise ConfigurationError("streams must contain at least one stream mapping")
        default_create = bool(self.config.get("create_missing", False))
        for stream_name, stream_config in streams.items():
            if not isinstance(stream_name, str) or not stream_name:
                raise ConfigurationError("stream names must be non-empty strings")
            if not isinstance(stream_config, Mapping):
                raise ConfigurationError(f"streams.{stream_name} must be an object")
            StreamMapping.from_config(
                stream_name,
                stream_config,
                default_create_missing=default_create,
            )

    def get_sink_class(self, stream_name: str) -> type[PendoSink]:
        """Reject unconfigured input streams before consuming their records."""
        streams = self.config["streams"]
        if stream_name not in streams:
            raise ConfigurationError(
                f"Singer stream {stream_name!r} has no entry in target config 'streams'"
            )
        return PendoSink


if __name__ == "__main__":
    TargetPendo.cli()
