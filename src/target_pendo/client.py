"""Pendo API client with bounded retries and strict response validation."""

from __future__ import annotations

import email.utils
import logging
import random
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import httpx

from target_pendo import __version__
from target_pendo.batching import encode_json
from target_pendo.exceptions import (
    PendoAPIError,
    PendoPartialFailureError,
    PendoProtocolError,
)

_RETRYABLE_STATUS_CODES = frozenset({408, 429, 500, 502, 503, 504})


@dataclass(frozen=True)
class PendoBulkResult:
    """Validated Pendo bulk metadata response."""

    total: int
    updated: int
    failed: int
    kind: str


def _retry_after_seconds(value: str | None, *, now: datetime | None = None) -> float | None:
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        pass

    try:
        retry_at = email.utils.parsedate_to_datetime(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if retry_at.tzinfo is None:
        retry_at = retry_at.replace(tzinfo=timezone.utc)
    current = now or datetime.now(tz=timezone.utc)
    return max(0.0, (retry_at - current).total_seconds())


class PendoClient:
    """Minimal client for Pendo's bulk metadata update API."""

    def __init__(
        self,
        *,
        integration_key: str,
        base_url: str,
        request_timeout_seconds: float,
        max_retries: int,
        backoff_base_seconds: float,
        backoff_max_seconds: float,
        transport: httpx.BaseTransport | None = None,
        sleep: Callable[[float], None] = time.sleep,
        uniform: Callable[[float, float], float] = random.uniform,
        logger: logging.Logger | None = None,
    ) -> None:
        if not integration_key:
            raise ValueError("integration_key must not be empty")
        if request_timeout_seconds <= 0:
            raise ValueError("request_timeout_seconds must be positive")
        if max_retries < 0:
            raise ValueError("max_retries must not be negative")

        self.max_retries = max_retries
        self.backoff_base_seconds = backoff_base_seconds
        self.backoff_max_seconds = backoff_max_seconds
        self._sleep = sleep
        self._uniform = uniform
        self.logger = logger or logging.getLogger(__name__)
        timeout = httpx.Timeout(
            connect=min(30.0, request_timeout_seconds),
            read=request_timeout_seconds,
            write=request_timeout_seconds,
            pool=min(30.0, request_timeout_seconds),
        )
        self._client = httpx.Client(
            base_url=base_url.rstrip("/"),
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
                "User-Agent": f"target-pendo/{__version__}",
                "X-Pendo-Integration-Key": integration_key,
            },
            timeout=timeout,
            transport=transport,
        )

    def close(self) -> None:
        """Close the underlying connection pool."""
        self._client.close()

    def __enter__(self) -> PendoClient:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def update_metadata(
        self,
        *,
        kind: str,
        group: str,
        records: Sequence[Mapping[str, Any]],
        create_missing: bool,
    ) -> PendoBulkResult:
        """Write one validated bulk metadata request."""
        if not records:
            raise ValueError("records must not be empty")
        path = f"/api/v1/metadata/{kind}/{group}/value"
        content = encode_json(list(records))
        params = {"create": "true"} if create_missing else None

        for attempt in range(self.max_retries + 1):
            try:
                response = self._client.post(path, params=params, content=content)
            except (httpx.TimeoutException, httpx.TransportError) as exc:
                if attempt >= self.max_retries:
                    raise PendoAPIError(0, f"request failed after {attempt + 1} attempts") from exc
                self._wait_before_retry(attempt=attempt, retry_after=None)
                continue

            if response.status_code in _RETRYABLE_STATUS_CODES:
                if attempt >= self.max_retries:
                    raise PendoAPIError(
                        response.status_code,
                        f"retry limit reached after {attempt + 1} attempts",
                    )
                self._wait_before_retry(
                    attempt=attempt,
                    retry_after=response.headers.get("Retry-After"),
                )
                continue

            if not response.is_success:
                raise PendoAPIError(response.status_code, self._safe_error_summary(response))

            return self._parse_result(response, expected_kind=kind, expected_total=len(records))

        raise AssertionError("unreachable")

    def _wait_before_retry(self, *, attempt: int, retry_after: str | None) -> None:
        delay = _retry_after_seconds(retry_after)
        if delay is None:
            ceiling = min(
                self.backoff_max_seconds,
                self.backoff_base_seconds * (2**attempt),
            )
            delay = self._uniform(0.0, ceiling)
        self.logger.warning("Retrying Pendo request in %.2f seconds", delay)
        self._sleep(delay)

    @staticmethod
    def _safe_error_summary(response: httpx.Response) -> str:
        """Return useful error metadata without logging record IDs or values."""
        content_type = response.headers.get("Content-Type", "unknown")
        return f"request rejected (content-type={content_type!r}, bytes={len(response.content)})"

    @staticmethod
    def _parse_result(
        response: httpx.Response,
        *,
        expected_kind: str,
        expected_total: int,
    ) -> PendoBulkResult:
        try:
            payload = response.json()
        except ValueError as exc:
            raise PendoProtocolError("Pendo returned non-JSON success content") from exc
        if not isinstance(payload, Mapping):
            raise PendoProtocolError("Pendo returned a non-object success payload")

        required = {"total", "updated", "failed"}
        if not required.issubset(payload):
            missing = ", ".join(sorted(required - set(payload)))
            raise PendoProtocolError(f"Pendo success payload is missing: {missing}")

        try:
            total = int(payload["total"])
            updated = int(payload["updated"])
            failed = int(payload["failed"])
        except (TypeError, ValueError) as exc:
            raise PendoProtocolError("Pendo success counts are not integers") from exc

        kind = str(payload.get("kind", expected_kind))
        if total != expected_total:
            raise PendoProtocolError(
                f"Pendo reported {total} records for a request containing {expected_total}"
            )
        if updated + failed != total:
            raise PendoProtocolError(
                "Pendo response counts are inconsistent: updated + failed != total"
            )
        if kind != expected_kind:
            raise PendoProtocolError(
                f"Pendo returned kind {kind!r} for a {expected_kind!r} request"
            )
        if failed:
            summary = {
                "missing_count": len(payload.get("missing", [])),
                "error_count": len(payload.get("errors", [])),
            }
            raise PendoPartialFailureError(
                total=total,
                updated=updated,
                failed=failed,
                kind=kind,
                error_summary=summary,
            )
        return PendoBulkResult(total=total, updated=updated, failed=failed, kind=kind)
