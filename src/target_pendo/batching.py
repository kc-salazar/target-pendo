"""Exact JSON request batching helpers."""

from __future__ import annotations

import json
from collections.abc import Iterable, Iterator, Mapping
from typing import Any

from target_pendo.exceptions import BatchSizeError


def encode_json(value: Any) -> bytes:
    """Encode JSON exactly as it will be sent to Pendo."""
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
    ).encode("utf-8")


def iter_json_batches(
    records: Iterable[Mapping[str, Any]],
    *,
    max_records: int,
    max_bytes: int,
) -> Iterator[list[dict[str, Any]]]:
    """Yield batches bounded by both record count and encoded request size."""
    if max_records < 1:
        raise ValueError("max_records must be at least 1")
    if max_bytes < 2:
        raise ValueError("max_bytes must be at least 2")

    batch: list[dict[str, Any]] = []
    encoded_size = 2  # Opening and closing brackets.

    for record in records:
        normalized = dict(record)
        record_size = len(encode_json(normalized))
        if record_size + 2 > max_bytes:
            raise BatchSizeError(
                f"A mapped Pendo record exceeds max_batch_bytes ({record_size + 2} > {max_bytes})"
            )

        delimiter_size = 1 if batch else 0
        exceeds_count = len(batch) >= max_records
        exceeds_bytes = encoded_size + delimiter_size + record_size > max_bytes
        if batch and (exceeds_count or exceeds_bytes):
            yield batch
            batch = []
            encoded_size = 2
            delimiter_size = 0

        batch.append(normalized)
        encoded_size += delimiter_size + record_size

    if batch:
        yield batch
