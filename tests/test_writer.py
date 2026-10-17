from __future__ import annotations

import threading
import time
from typing import ClassVar

import pytest

from target_pendo.batching import JsonBatch, make_json_batch
from target_pendo.client import PendoBulkResult
from target_pendo.exceptions import PendoAPIError
from target_pendo.writer import BulkBatchWriter, BulkWriteSummary


def batch(start: int, count: int) -> JsonBatch:
    return make_json_batch(
        {"visitorId": str(number), "values": {"number": number}}
        for number in range(start, start + count)
    )


class ControlledClient:
    failure_status: ClassVar[int | None] = None
    fail_above: ClassVar[int | None] = None

    def __init__(self, *, latency: float = 0.0) -> None:
        self.latency = latency
        self.lock = threading.Lock()
        self.active = 0
        self.max_active = 0
        self.calls: list[int] = []

    def update_metadata(
        self,
        *,
        kind: str,
        group: str,
        batch: JsonBatch,
        create_missing: bool,
    ) -> PendoBulkResult:
        del group, create_missing
        with self.lock:
            self.active += 1
            self.max_active = max(self.max_active, self.active)
            self.calls.append(len(batch))
        try:
            time.sleep(self.latency)
            if self.fail_above is not None and len(batch) > self.fail_above:
                raise PendoAPIError(408, "request timed out")
            if self.failure_status is not None:
                raise PendoAPIError(self.failure_status, "failed")
            return PendoBulkResult(
                total=len(batch),
                updated=len(batch),
                failed=0,
                kind=kind,
            )
        finally:
            with self.lock:
                self.active -= 1


@pytest.fixture(autouse=True)
def reset_client_controls() -> None:
    ControlledClient.failure_status = None
    ControlledClient.fail_above = None


def test_parallel_writer_uses_configured_concurrency() -> None:
    client = ControlledClient(latency=0.02)
    writer = BulkBatchWriter(client, concurrency=4)
    try:
        result = writer.write(
            [batch(number, 1) for number in range(8)],
            kind="visitor",
            group="custom",
            create_missing=False,
        )
    finally:
        writer.close()

    assert result == BulkWriteSummary(updated=8, requests=8, timeout_splits=0)
    assert client.max_active == 4


def test_sequential_writer_and_empty_input() -> None:
    client = ControlledClient()
    writer = BulkBatchWriter(client, concurrency=1)
    try:
        empty = writer.write([], kind="visitor", group="custom", create_missing=False)
        result = writer.write(
            [batch(0, 2), batch(2, 1)],
            kind="visitor",
            group="custom",
            create_missing=False,
        )
    finally:
        writer.close()

    assert empty == BulkWriteSummary()
    assert result == BulkWriteSummary(updated=3, requests=2, timeout_splits=0)
    assert client.max_active == 1


def test_timeout_batch_is_adaptively_split_until_it_succeeds() -> None:
    ControlledClient.fail_above = 2
    client = ControlledClient()
    writer = BulkBatchWriter(client, concurrency=2)
    try:
        result = writer.write(
            [batch(0, 5)],
            kind="visitor",
            group="custom",
            create_missing=False,
        )
    finally:
        writer.close()

    assert result == BulkWriteSummary(updated=5, requests=5, timeout_splits=2)
    assert client.calls == [5, 2, 3, 1, 2]


@pytest.mark.parametrize("status", [400, 408, 413, 500])
def test_unsplittable_or_non_size_failure_is_propagated(status: int) -> None:
    ControlledClient.failure_status = status
    client = ControlledClient()
    writer = BulkBatchWriter(client, concurrency=2)
    try:
        with pytest.raises(PendoAPIError) as error:
            writer.write(
                [batch(0, 1)],
                kind="visitor",
                group="custom",
                create_missing=False,
            )
    finally:
        writer.close()

    assert error.value.status_code == status


def test_parallel_writer_waits_for_started_requests_and_stops_queued_work() -> None:
    ControlledClient.failure_status = 400
    client = ControlledClient(latency=0.01)
    writer = BulkBatchWriter(client, concurrency=3)
    try:
        with pytest.raises(PendoAPIError):
            writer.write(
                [batch(number, 1) for number in range(6)],
                kind="visitor",
                group="custom",
                create_missing=False,
            )
        assert client.active == 0
        assert len(client.calls) == 3
    finally:
        writer.close()


def test_parallel_writer_propagates_earliest_submitted_failure() -> None:
    class OrderedFailureClient(ControlledClient):
        def update_metadata(
            self,
            *,
            kind: str,
            group: str,
            batch: JsonBatch,
            create_missing: bool,
        ) -> PendoBulkResult:
            del kind, group, create_missing
            visitor_id = batch.records[0]["visitorId"]
            if visitor_id == "0":
                time.sleep(0.02)
                raise PendoAPIError(400, "earlier batch")
            raise PendoAPIError(409, "later batch")

    writer = BulkBatchWriter(OrderedFailureClient(), concurrency=2)
    try:
        with pytest.raises(PendoAPIError) as error:
            writer.write(
                [batch(0, 1), batch(1, 1)],
                kind="visitor",
                group="custom",
                create_missing=False,
            )
    finally:
        writer.close()

    assert error.value.status_code == 400


def test_invalid_concurrency_is_rejected() -> None:
    with pytest.raises(ValueError, match="at least 1"):
        BulkBatchWriter(ControlledClient(), concurrency=0)
