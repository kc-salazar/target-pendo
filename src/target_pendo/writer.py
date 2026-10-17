"""Bounded parallel writer for Pendo bulk metadata requests."""

from __future__ import annotations

from collections.abc import Iterable
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from typing import Protocol

from target_pendo.batching import JsonBatch, make_json_batch
from target_pendo.client import PendoBulkResult
from target_pendo.exceptions import PendoAPIError


class BulkClient(Protocol):
    """Protocol implemented by the production Pendo client and benchmark fakes."""

    def update_metadata(
        self,
        *,
        kind: str,
        group: str,
        batch: JsonBatch,
        create_missing: bool,
    ) -> PendoBulkResult: ...


@dataclass(frozen=True, slots=True)
class BulkWriteSummary:
    """Aggregate result from one group of parallel bulk requests."""

    updated: int = 0
    requests: int = 0
    timeout_splits: int = 0

    def __add__(self, other: BulkWriteSummary) -> BulkWriteSummary:
        return BulkWriteSummary(
            updated=self.updated + other.updated,
            requests=self.requests + other.requests,
            timeout_splits=self.timeout_splits + other.timeout_splits,
        )


class BulkBatchWriter:
    """Send independent batches concurrently with deterministic failure propagation."""

    def __init__(self, client: BulkClient, *, concurrency: int) -> None:
        if concurrency < 1:
            raise ValueError("concurrency must be at least 1")
        self.client = client
        self.concurrency = concurrency
        self._executor = ThreadPoolExecutor(
            max_workers=concurrency,
            thread_name_prefix="target-pendo",
        )

    def close(self) -> None:
        """Wait for workers and release their threads."""
        self._executor.shutdown(wait=True, cancel_futures=False)

    def write(
        self,
        batches: Iterable[JsonBatch],
        *,
        kind: str,
        group: str,
        create_missing: bool,
    ) -> BulkWriteSummary:
        """Write batches with at most ``concurrency`` requests materialized at once."""
        if self.concurrency == 1:
            summary = BulkWriteSummary()
            for batch in batches:
                summary += self._write_one(
                    batch,
                    kind=kind,
                    group=group,
                    create_missing=create_missing,
                )
            return summary

        iterator = iter(batches)
        futures: dict[Future[BulkWriteSummary], int] = {}
        submission_index = 0

        def submit_next() -> bool:
            nonlocal submission_index
            try:
                batch = next(iterator)
            except StopIteration:
                return False
            future = self._executor.submit(
                self._write_one,
                batch,
                kind=kind,
                group=group,
                create_missing=create_missing,
            )
            futures[future] = submission_index
            submission_index += 1
            return True

        while len(futures) < self.concurrency and submit_next():
            pass

        summary = BulkWriteSummary()
        errors: list[tuple[int, Exception]] = []
        while futures:
            completed, _ = wait(futures, return_when=FIRST_COMPLETED)
            for future in completed:
                index = futures.pop(future)
                try:
                    summary += future.result()
                except Exception as exc:  # Re-raised after all in-flight work settles.
                    errors.append((index, exc))
            if not errors:
                while len(futures) < self.concurrency and submit_next():
                    pass
        if errors:
            raise min(errors, key=lambda item: item[0])[1]
        return summary

    def _write_one(
        self,
        batch: JsonBatch,
        *,
        kind: str,
        group: str,
        create_missing: bool,
    ) -> BulkWriteSummary:
        try:
            result = self.client.update_metadata(
                kind=kind,
                group=group,
                batch=batch,
                create_missing=create_missing,
            )
        except PendoAPIError as exc:
            if exc.status_code not in {408, 413} or len(batch) == 1:
                raise
            midpoint = len(batch) // 2
            left = make_json_batch(batch.records[:midpoint])
            right = make_json_batch(batch.records[midpoint:])
            return (
                BulkWriteSummary(requests=1, timeout_splits=1)
                + self._write_one(
                    left,
                    kind=kind,
                    group=group,
                    create_missing=create_missing,
                )
                + self._write_one(
                    right,
                    kind=kind,
                    group=group,
                    create_missing=create_missing,
                )
            )
        return BulkWriteSummary(updated=result.updated, requests=1)
