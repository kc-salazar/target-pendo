from __future__ import annotations

import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Any

import httpx
import pytest

from target_pendo.batching import make_json_batch
from target_pendo.client import PendoBulkResult, PendoClient, _retry_after_seconds
from target_pendo.exceptions import (
    PendoAPIError,
    PendoPartialFailureError,
    PendoProtocolError,
)


def response_payload(total: int = 1, **overrides: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "total": total,
        "updated": total,
        "failed": 0,
        "kind": "visitor",
    }
    payload.update(overrides)
    return payload


def make_client(
    handler: httpx.MockTransport,
    *,
    max_retries: int = 2,
    sleeps: list[float] | None = None,
) -> PendoClient:
    recorded_sleeps = sleeps if sleeps is not None else []
    return PendoClient(
        integration_key="secret-key",
        base_url="https://app.eu.pendo.io/",
        request_timeout_seconds=10,
        max_retries=max_retries,
        backoff_base_seconds=2,
        backoff_max_seconds=10,
        transport=handler,
        sleep=recorded_sleeps.append,
        uniform=lambda lower, upper: upper,
    )


def test_bulk_update_sends_expected_request_without_exposing_key() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured.update(
            path=request.url.path,
            query=request.url.query.decode(),
            key=request.headers["X-Pendo-Integration-Key"],
            content_type=request.headers["Content-Type"],
            body=json.loads(request.content),
        )
        return httpx.Response(200, json=response_payload(2))

    transport = httpx.MockTransport(handler)
    records = [
        {"visitorId": "u1", "values": {"plan": "Pro"}},
        {"visitorId": "u2", "values": {"plan": "Free"}},
    ]
    with make_client(transport) as client:
        result = client.update_metadata(
            kind="visitor",
            group="custom",
            records=records,
            create_missing=True,
        )

    assert result == PendoBulkResult(total=2, updated=2, failed=0, kind="visitor")
    assert captured == {
        "path": "/api/v1/metadata/visitor/custom/value",
        "query": "create=true",
        "key": "secret-key",
        "content_type": "application/json",
        "body": records,
    }


def test_create_false_omits_query_parameter() -> None:
    queries: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        queries.append(request.url.query.decode())
        return httpx.Response(200, json=response_payload())

    with make_client(httpx.MockTransport(handler)) as client:
        client.update_metadata(
            kind="visitor",
            group="agent",
            records=[{"visitorId": "u1", "values": {"role": "admin"}}],
            create_missing=False,
        )

    assert queries == [""]


def test_rate_limit_honors_retry_after_then_succeeds() -> None:
    attempts = 0
    sleeps: list[float] = []

    def handler(_: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            return httpx.Response(429, headers={"Retry-After": "3.5"})
        return httpx.Response(200, json=response_payload())

    with make_client(httpx.MockTransport(handler), sleeps=sleeps) as client:
        result = client.update_metadata(
            kind="visitor",
            group="custom",
            records=[{"visitorId": "u1", "values": {"plan": "Pro"}}],
            create_missing=False,
        )

    assert result.updated == 1
    assert attempts == 2
    assert sleeps == [3.5]


def test_server_error_uses_bounded_exponential_backoff() -> None:
    attempts = 0
    sleeps: list[float] = []

    def handler(_: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            return httpx.Response(503)
        return httpx.Response(200, json=response_payload())

    with make_client(httpx.MockTransport(handler), sleeps=sleeps) as client:
        client.update_metadata(
            kind="visitor",
            group="custom",
            records=[{"visitorId": "u1", "values": {"plan": "Pro"}}],
            create_missing=False,
        )

    assert attempts == 3
    assert sleeps == [2, 4]


def test_transport_error_is_retried() -> None:
    attempts = 0
    sleeps: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise httpx.ReadTimeout("timed out", request=request)
        return httpx.Response(200, json=response_payload())

    with make_client(httpx.MockTransport(handler), sleeps=sleeps) as client:
        client.update_metadata(
            kind="visitor",
            group="custom",
            records=[{"visitorId": "u1", "values": {"plan": "Pro"}}],
            create_missing=False,
        )

    assert attempts == 2
    assert sleeps == [2]


def test_non_retryable_error_is_sanitized_and_not_retried() -> None:
    attempts = 0

    def handler(_: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        return httpx.Response(
            400,
            headers={"Content-Type": "application/json"},
            content=b'{"message":"contains visitor u-secret"}',
        )

    with make_client(httpx.MockTransport(handler)) as client, pytest.raises(PendoAPIError) as error:
        client.update_metadata(
            kind="visitor",
            group="custom",
            records=[{"visitorId": "u-secret", "values": {"plan": "Pro"}}],
            create_missing=False,
        )

    assert error.value.status_code == 400
    assert "u-secret" not in str(error.value)
    assert "bytes=39" in str(error.value)
    assert attempts == 1


@pytest.mark.parametrize("status", [429, 500, 502, 503, 504])
def test_retryable_status_fails_after_retry_limit(status: int) -> None:
    transport = httpx.MockTransport(lambda _: httpx.Response(status))

    with (
        make_client(transport, max_retries=0) as client,
        pytest.raises(PendoAPIError, match="retry limit reached") as error,
    ):
        client.update_metadata(
            kind="visitor",
            group="custom",
            records=[{"visitorId": "u1", "values": {"plan": "Pro"}}],
            create_missing=False,
        )

    assert error.value.status_code == status


def test_timeout_is_returned_to_writer_for_adaptive_split() -> None:
    transport = httpx.MockTransport(lambda _: httpx.Response(408))

    with (
        make_client(transport, max_retries=2) as client,
        pytest.raises(PendoAPIError, match="HTTP 408") as error,
    ):
        client.update_metadata(
            kind="visitor",
            group="custom",
            records=[{"visitorId": "u1", "values": {"plan": "Pro"}}],
            create_missing=False,
        )

    assert error.value.status_code == 408


def test_transport_error_fails_after_retry_limit() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("offline", request=request)

    with (
        make_client(httpx.MockTransport(handler), max_retries=0) as client,
        pytest.raises(PendoAPIError, match="request failed after 1 attempts") as error,
    ):
        client.update_metadata(
            kind="visitor",
            group="custom",
            records=[{"visitorId": "u1", "values": {"plan": "Pro"}}],
            create_missing=False,
        )

    assert error.value.status_code == 0


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        ([], "non-object"),
        ({"updated": 1, "failed": 0}, "missing: total"),
        (response_payload(total="nope"), "counts are not integers"),
        (response_payload(total=2), "reported 2 records"),
        (response_payload(updated=0, failed=0), r"updated \+ failed"),
        (response_payload(kind="account"), "returned kind"),
    ],
)
def test_invalid_success_payload_is_rejected(payload: Any, message: str) -> None:
    transport = httpx.MockTransport(lambda _: httpx.Response(200, json=payload))

    with make_client(transport) as client, pytest.raises(PendoProtocolError, match=message):
        client.update_metadata(
            kind="visitor",
            group="custom",
            records=[{"visitorId": "u1", "values": {"plan": "Pro"}}],
            create_missing=False,
        )


def test_non_json_success_payload_is_rejected() -> None:
    transport = httpx.MockTransport(lambda _: httpx.Response(200, content=b"not-json"))

    with make_client(transport) as client, pytest.raises(PendoProtocolError, match="non-JSON"):
        client.update_metadata(
            kind="visitor",
            group="custom",
            records=[{"visitorId": "u1", "values": {"plan": "Pro"}}],
            create_missing=False,
        )


def test_partial_failure_is_fatal_and_summarized_without_values() -> None:
    payload = response_payload(
        updated=0,
        failed=1,
        missing=["u-secret"],
        errors=[{"visitorId": "u-secret", "message": "bad value"}],
    )
    transport = httpx.MockTransport(lambda _: httpx.Response(200, json=payload))

    with make_client(transport) as client, pytest.raises(PendoPartialFailureError) as error:
        client.update_metadata(
            kind="visitor",
            group="custom",
            records=[{"visitorId": "u-secret", "values": {"plan": "Pro"}}],
            create_missing=False,
        )

    assert error.value.error_summary == {"missing_count": 1, "error_count": 1}
    assert "u-secret" not in str(error.value)


def test_empty_request_is_rejected_before_http() -> None:
    transport = httpx.MockTransport(lambda _: pytest.fail("HTTP should not be called"))
    with make_client(transport) as client, pytest.raises(ValueError, match="must not be empty"):
        client.update_metadata(
            kind="visitor",
            group="custom",
            records=[],
            create_missing=False,
        )


def test_preencoded_batch_and_records_are_mutually_exclusive() -> None:
    transport = httpx.MockTransport(lambda _: pytest.fail("HTTP should not be called"))
    with (
        make_client(transport) as client,
        pytest.raises(ValueError, match="either batch or records"),
    ):
        client.update_metadata(
            kind="visitor",
            group="custom",
            batch=make_json_batch([{"visitorId": "u1", "values": {"plan": "Pro"}}]),
            records=[{"visitorId": "u1", "values": {"plan": "Pro"}}],
            create_missing=False,
        )


def test_global_request_limiter_bounds_parallel_http_calls() -> None:
    lock = threading.Lock()
    active = 0
    max_active = 0

    def handler(_: httpx.Request) -> httpx.Response:
        nonlocal active, max_active
        with lock:
            active += 1
            max_active = max(max_active, active)
        time.sleep(0.02)
        with lock:
            active -= 1
        return httpx.Response(200, json=response_payload())

    client = PendoClient(
        integration_key="key",
        base_url="https://app.pendo.io",
        request_timeout_seconds=1,
        max_retries=0,
        backoff_base_seconds=0,
        backoff_max_seconds=0,
        max_connections=8,
        request_limiter=threading.BoundedSemaphore(2),
        transport=httpx.MockTransport(handler),
    )
    batch = make_json_batch([{"visitorId": "u1", "values": {"plan": "Pro"}}])
    try:
        with ThreadPoolExecutor(max_workers=8) as executor:
            results = list(
                executor.map(
                    lambda _: client.update_metadata(
                        kind="visitor",
                        group="custom",
                        batch=batch,
                        create_missing=False,
                    ),
                    range(8),
                )
            )
    finally:
        client.close()

    assert len(results) == 8
    assert max_active == 2


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"integration_key": ""}, "integration_key"),
        ({"request_timeout_seconds": 0}, "request_timeout_seconds"),
        ({"max_retries": -1}, "max_retries"),
        ({"backoff_base_seconds": -1}, "backoff"),
        ({"backoff_max_seconds": -1}, "backoff"),
        ({"max_connections": 0}, "max_connections"),
    ],
)
def test_invalid_client_configuration(kwargs: dict[str, Any], message: str) -> None:
    arguments: dict[str, Any] = {
        "integration_key": "key",
        "base_url": "https://app.pendo.io",
        "request_timeout_seconds": 1,
        "max_retries": 0,
        "backoff_base_seconds": 0,
        "backoff_max_seconds": 0,
    }
    arguments.update(kwargs)

    with pytest.raises(ValueError, match=message):
        PendoClient(**arguments)


def test_retry_after_parses_seconds_and_http_dates() -> None:
    now = datetime(2026, 10, 17, 12, 0, tzinfo=timezone.utc)

    assert _retry_after_seconds("2.5", now=now) == 2.5
    assert _retry_after_seconds("-1", now=now) == 0
    assert _retry_after_seconds("Sat, 17 Oct 2026 12:00:05 GMT", now=now) == 5
    assert _retry_after_seconds("Sat, 17 Oct 2026 11:59:00 GMT", now=now) == 0
    assert _retry_after_seconds("invalid", now=now) is None
    assert _retry_after_seconds(None, now=now) is None
