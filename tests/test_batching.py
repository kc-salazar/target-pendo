from __future__ import annotations

import math

import pytest

from target_pendo.batching import (
    encode_json,
    iter_encoded_json_batches,
    iter_json_batches,
    make_json_batch,
)
from target_pendo.exceptions import BatchSizeError


def test_batches_split_on_record_count() -> None:
    records = [{"n": number} for number in range(5)]

    batches = list(iter_json_batches(records, max_records=2, max_bytes=1_000))

    assert [len(batch) for batch in batches] == [2, 2, 1]
    assert [item for batch in batches for item in batch] == records


def test_batches_split_on_exact_encoded_byte_size() -> None:
    first = {"visitorId": "one", "values": {"city": "Montréal"}}
    second = {"visitorId": "two", "values": {"city": "東京"}}
    one_record_bytes = len(encode_json([first]))
    both_records_bytes = len(encode_json([first, second]))

    assert both_records_bytes > one_record_bytes
    batches = list(
        iter_json_batches(
            [first, second],
            max_records=10,
            max_bytes=both_records_bytes - 1,
        )
    )

    assert batches == [[first], [second]]
    assert len(encode_json(batches[0])) <= both_records_bytes - 1
    assert len(encode_json(batches[1])) <= both_records_bytes - 1


def test_batch_accepts_payload_at_exact_byte_limit() -> None:
    records = [{"value": "é"}, {"value": "ß"}]
    exact_size = len(encode_json(records))

    assert list(iter_json_batches(records, max_records=2, max_bytes=exact_size)) == [records]


def test_oversized_single_record_fails() -> None:
    record = {"value": "too large"}
    exact_size = len(encode_json([record]))

    with pytest.raises(BatchSizeError, match="exceeds max_batch_bytes"):
        list(iter_json_batches([record], max_records=1, max_bytes=exact_size - 1))


@pytest.mark.parametrize(
    ("max_records", "max_bytes", "message"),
    [(0, 10, "max_records"), (1, 1, "max_bytes")],
)
def test_invalid_limits_fail(max_records: int, max_bytes: int, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        list(iter_json_batches([], max_records=max_records, max_bytes=max_bytes))


def test_non_finite_json_number_is_rejected() -> None:
    with pytest.raises(ValueError, match="Out of range"):
        encode_json({"value": math.nan})


@pytest.mark.parametrize("value", [{1: "bad key"}, {"value": object()}])
def test_non_json_values_are_rejected(value: object) -> None:
    with pytest.raises(TypeError):
        encode_json(value)


def test_encoded_batch_reuses_exact_request_bytes() -> None:
    records = [{"visitorId": "ü", "values": {"plan": "Prø"}}]

    batch = make_json_batch(records)

    assert batch.records == tuple(records)
    assert batch.content == encode_json(records)
    assert len(batch) == 1


def test_encoded_batch_iterator_returns_exact_bounded_payloads() -> None:
    records = [{"n": number} for number in range(3)]

    batches = list(iter_encoded_json_batches(records, max_records=2, max_bytes=1_000))

    assert [len(batch) for batch in batches] == [2, 1]
    assert [batch.content for batch in batches] == [
        encode_json(records[:2]),
        encode_json(records[2:]),
    ]
