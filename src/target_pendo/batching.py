"""Exact JSON request batching helpers."""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from typing import Any

import rapidjson

from target_pendo.exceptions import BatchSizeError


@dataclass(frozen=True, slots=True)
class JsonBatch:
    """A bounded Pendo request with its JSON encoded exactly once."""

    records: tuple[dict[str, Any], ...]
    content: bytes

    def __len__(self) -> int:
        return len(self.records)


def encode_json(value: Any) -> bytes:
    """Encode JSON exactly as it will be sent to Pendo."""
    return rapidjson.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        write_mode=rapidjson.WM_COMPACT,
    ).encode("utf-8")


def make_json_batch(records: Iterable[Mapping[str, Any]]) -> JsonBatch:
    """Build one pre-encoded request batch."""
    normalized = tuple(dict(record) for record in records)
    encoded_records = tuple(encode_json(record) for record in normalized)
    return JsonBatch(
        records=normalized,
        content=b"[" + b",".join(encoded_records) + b"]",
    )


def iter_encoded_json_batches(
    records: Iterable[Mapping[str, Any]],
    *,
    max_records: int,
    max_bytes: int,
) -> Iterator[JsonBatch]:
    """Yield pre-encoded batches bounded by record count and exact byte size."""
    if max_records < 1:
        raise ValueError("max_records must be at least 1")
    if max_bytes < 2:
        raise ValueError("max_bytes must be at least 2")

    batch: list[dict[str, Any]] = []
    encoded_records: list[bytes] = []
    encoded_size = 2  # Opening and closing brackets.

    for record in records:
        normalized = dict(record)
        encoded_record = encode_json(normalized)
        record_size = len(encoded_record)
        if record_size + 2 > max_bytes:
            raise BatchSizeError(
                f"A mapped Pendo record exceeds max_batch_bytes ({record_size + 2} > {max_bytes})"
            )

        delimiter_size = 1 if batch else 0
        exceeds_count = len(batch) >= max_records
        exceeds_bytes = encoded_size + delimiter_size + record_size > max_bytes
        if batch and (exceeds_count or exceeds_bytes):
            yield JsonBatch(
                records=tuple(batch),
                content=b"[" + b",".join(encoded_records) + b"]",
            )
            batch = []
            encoded_records = []
            encoded_size = 2
            delimiter_size = 0

        batch.append(normalized)
        encoded_records.append(encoded_record)
        encoded_size += delimiter_size + record_size

    if batch:
        yield JsonBatch(
            records=tuple(batch),
            content=b"[" + b",".join(encoded_records) + b"]",
        )


def iter_json_batches(
    records: Iterable[Mapping[str, Any]],
    *,
    max_records: int,
    max_bytes: int,
) -> Iterator[list[dict[str, Any]]]:
    """Yield record lists bounded by both count and exact encoded size."""
    for batch in iter_encoded_json_batches(
        records,
        max_records=max_records,
        max_bytes=max_bytes,
    ):
        yield list(batch.records)
