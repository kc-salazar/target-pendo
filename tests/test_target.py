from __future__ import annotations

import io
import json
from typing import Any, ClassVar

import pytest

from target_pendo.batching import JsonBatch
from target_pendo.client import PendoBulkResult
from target_pendo.exceptions import ConfigurationError, PendoAPIError
from target_pendo.sink import PendoSink
from target_pendo.target import TargetPendo


def target_config(**overrides: Any) -> dict[str, Any]:
    config: dict[str, Any] = {
        "integration_key": "secret-key",
        "batch_size_rows": 3,
        "max_batch_bytes": 10_000,
        "streams": {
            "users": {
                "kind": "visitor",
                "id_field": "id",
                "field_mappings": {"planName": "plan"},
            }
        },
    }
    config.update(overrides)
    return config


def schema() -> dict[str, Any]:
    return {
        "type": "object",
        "properties": {
            "id": {"type": "string"},
            "plan": {"type": ["string", "null"]},
        },
    }


class RecordingClient:
    instances: ClassVar[list[RecordingClient]] = []
    error: ClassVar[Exception | None] = None

    def __init__(self, **_: Any) -> None:
        self.calls: list[dict[str, Any]] = []
        self.closed = False
        self.__class__.instances.append(self)

    def update_metadata(
        self,
        *,
        kind: str,
        group: str,
        batch: JsonBatch,
        create_missing: bool,
    ) -> PendoBulkResult:
        self.calls.append(
            {
                "kind": kind,
                "group": group,
                "records": [dict(record) for record in batch.records],
                "create_missing": create_missing,
            }
        )
        if self.error:
            raise self.error
        return PendoBulkResult(
            total=len(batch),
            updated=len(batch),
            failed=0,
            kind=kind,
        )

    def close(self) -> None:
        self.closed = True


@pytest.fixture(autouse=True)
def reset_recording_client() -> None:
    RecordingClient.instances = []
    RecordingClient.error = None


def singer_input(*, include_state: bool = True) -> io.StringIO:
    messages = [
        {"type": "SCHEMA", "stream": "users", "schema": schema(), "key_properties": ["id"]},
        {"type": "RECORD", "stream": "users", "record": {"id": "u1", "plan": "Pro"}},
        {"type": "RECORD", "stream": "users", "record": {"id": "u2", "plan": None}},
    ]
    if include_state:
        messages.append({"type": "STATE", "value": {"bookmark": 2}})
    return io.StringIO("".join(f"{json.dumps(message)}\n" for message in messages))


def test_target_processes_singer_input_and_emits_state_after_success(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr("target_pendo.sink.PendoClient", RecordingClient)
    target = TargetPendo(config=target_config())

    target.listen(singer_input())

    assert capsys.readouterr().out == '{"bookmark": 2}\n'
    assert len(RecordingClient.instances) == 1
    client = RecordingClient.instances[0]
    assert client.calls == [
        {
            "kind": "visitor",
            "group": "custom",
            "records": [{"visitorId": "u1", "values": {"planName": "Pro"}}],
            "create_missing": False,
        }
    ]
    assert client.closed is True


def test_state_is_not_emitted_when_pendo_write_fails(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr("target_pendo.sink.PendoClient", RecordingClient)
    RecordingClient.error = PendoAPIError(503, "retry limit reached")
    target = TargetPendo(config=target_config())

    with pytest.raises(PendoAPIError, match="503"):
        target.listen(singer_input())

    assert capsys.readouterr().out == ""


def test_sink_splits_mapped_records_by_configured_request_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("target_pendo.sink.PendoClient", RecordingClient)
    target = TargetPendo(config=target_config(request_batch_size_rows=2))
    sink = target.add_sink("users", schema(), ["id"])

    sink.process_batch(
        {
            "records": [
                {"id": "u1", "plan": "one"},
                {"id": "u2", "plan": "two"},
                {"id": "u3", "plan": "three"},
            ]
        }
    )
    sink.clean_up()

    assert sorted(len(call["records"]) for call in RecordingClient.instances[0].calls) == [1, 2]


def test_sink_coalesces_duplicate_ids_before_sending(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("target_pendo.sink.PendoClient", RecordingClient)
    target = TargetPendo(config=target_config())
    sink = target.add_sink("users", schema(), ["id"])

    sink.process_batch(
        {
            "records": [
                {"id": "u1", "plan": "one"},
                {"id": "u1", "plan": "two"},
            ]
        }
    )
    sink.clean_up()

    assert RecordingClient.instances[0].calls[0]["records"] == [
        {"visitorId": "u1", "values": {"planName": "two"}}
    ]


def test_duplicate_coalescing_can_be_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("target_pendo.sink.PendoClient", RecordingClient)
    target = TargetPendo(config=target_config(deduplicate_records=False))
    sink = target.add_sink("users", schema(), ["id"])

    sink.process_batch(
        {
            "records": [
                {"id": "u1", "plan": "one"},
                {"id": "u1", "plan": "two"},
            ]
        }
    )
    sink.clean_up()

    assert RecordingClient.instances[0].calls[0]["records"] == [
        {"visitorId": "u1", "values": {"planName": "one"}},
        {"visitorId": "u1", "values": {"planName": "two"}},
    ]


def test_performance_defaults_are_applied() -> None:
    config = target_config()
    config.pop("batch_size_rows")
    target = TargetPendo(config=config)

    assert target.config["batch_size_rows"] == 50_000
    assert target.config["request_batch_size_rows"] == 2_500
    assert target.config["request_concurrency"] == 8
    assert target.config["http2"] is True
    assert target.config["deduplicate_records"] is True


def test_target_uses_configured_parallelism_and_sink_class() -> None:
    target = TargetPendo(config=target_config(max_parallelism=2))

    assert target.max_parallelism == 2
    assert target.get_sink_class("users") is PendoSink
    with pytest.raises(ConfigurationError, match="has no entry"):
        target.get_sink_class("unknown")


@pytest.mark.parametrize(
    ("streams", "message"),
    [({}, "at least one"), ({"": {"kind": "visitor"}}, "stream names"), ({"users": []}, "object")],
)
def test_invalid_stream_collection_is_rejected(streams: Any, message: str) -> None:
    with pytest.raises(ConfigurationError, match=message):
        TargetPendo(config=target_config(streams=streams), validate_config=False)
