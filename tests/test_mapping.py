from __future__ import annotations

from typing import Any

import pytest

from target_pendo.exceptions import ConfigurationError, RecordMappingError
from target_pendo.mapping import StreamMapping, resolve_selector


def mapping_config(**overrides: Any) -> dict[str, Any]:
    config: dict[str, Any] = {
        "kind": "visitor",
        "id_field": "user_id",
        "field_mappings": {"planName": "plan", "country": "/profile/country"},
    }
    config.update(overrides)
    return config


def make_mapping(**overrides: Any) -> StreamMapping:
    return StreamMapping.from_config(
        "users",
        mapping_config(**overrides),
        default_create_missing=False,
    )


def test_resolve_selector_supports_top_level_keys_and_json_pointer() -> None:
    record = {
        "plain": "top-level",
        "nested": {"a/b": {"~key": ["zero", "one"]}},
    }

    assert resolve_selector(record, "plain") == "top-level"
    assert resolve_selector(record, "/nested/a~1b/~0key/1") == "one"
    assert resolve_selector(record, "") == record


def test_missing_or_invalid_array_pointer_returns_missing() -> None:
    mapping = make_mapping(field_mappings={"missing": "/items/-1", "kept": "/items/0"})

    assert mapping.map_record({"user_id": "u1", "items": ["first"]}) == {
        "visitorId": "u1",
        "values": {"kept": "first"},
    }


def test_invalid_json_pointer_escape_is_rejected_during_configuration() -> None:
    with pytest.raises(ConfigurationError, match="Invalid JSON Pointer"):
        make_mapping(field_mappings={"bad": "/profile/~2bad"})


def test_map_visitor_record_with_nested_value() -> None:
    mapping = make_mapping(create_missing=True)

    result = mapping.map_record({"user_id": 42, "plan": "Pro", "profile": {"country": "US"}})

    assert result == {
        "visitorId": "42",
        "values": {"planName": "Pro", "country": "US"},
    }
    assert mapping.create_missing is True


def test_map_account_record_uses_account_id() -> None:
    mapping = make_mapping(
        kind="account",
        id_field="/account/id",
        field_mappings={"tier": "tier"},
    )

    assert mapping.map_record({"account": {"id": "acme"}, "tier": "gold"}) == {
        "accountId": "acme",
        "values": {"tier": "gold"},
    }


@pytest.mark.parametrize("source_id", [None, "", "   "])
def test_missing_id_fails_by_default(source_id: object) -> None:
    with pytest.raises(RecordMappingError, match="missing a usable ID"):
        make_mapping(field_mappings={"plan": "plan"}).map_record(
            {"user_id": source_id, "plan": "Pro"}
        )


def test_missing_id_can_skip_record() -> None:
    mapping = make_mapping(
        missing_id_behavior="skip",
        field_mappings={"plan": "plan"},
    )

    assert mapping.map_record({"plan": "Pro"}) is None


def test_missing_and_null_fields_are_omitted_by_default() -> None:
    mapping = make_mapping(field_mappings={"missing": "absent", "null": "nothing", "kept": "value"})

    assert mapping.map_record({"user_id": "u1", "nothing": None, "value": 0}) == {
        "visitorId": "u1",
        "values": {"kept": 0},
    }


def test_missing_field_can_fail() -> None:
    mapping = make_mapping(
        missing_field_behavior="fail",
        field_mappings={"plan": "missing"},
    )

    with pytest.raises(RecordMappingError, match="missing selector"):
        mapping.map_record({"user_id": "u1"})


def test_null_value_can_fail_or_be_sent() -> None:
    failing = make_mapping(
        null_value_behavior="fail",
        field_mappings={"plan": "plan"},
    )
    sending = make_mapping(
        null_value_behavior="send",
        field_mappings={"plan": "plan"},
    )

    with pytest.raises(RecordMappingError, match="has null"):
        failing.map_record({"user_id": "u1", "plan": None})
    assert sending.map_record({"user_id": "u1", "plan": None}) == {
        "visitorId": "u1",
        "values": {"plan": None},
    }


def test_empty_values_can_skip_or_fail() -> None:
    skipping = make_mapping(field_mappings={"plan": "missing"})
    failing = make_mapping(
        empty_values_behavior="fail",
        field_mappings={"plan": "missing"},
    )

    assert skipping.map_record({"user_id": "u1"}) is None
    with pytest.raises(RecordMappingError, match="produced no Pendo metadata values"):
        failing.map_record({"user_id": "u1"})


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"kind": "person"}, "kind"),
        ({"group": "system"}, "group"),
        ({"id_field": ""}, "id_field"),
        ({"field_mappings": {}}, "field_mappings"),
        ({"field_mappings": {"": "source"}}, "invalid Pendo field"),
        ({"field_mappings": {"target": 1}}, "must be a source field"),
        ({"missing_id_behavior": "omit"}, "missing_id_behavior"),
        ({"missing_field_behavior": "skip"}, "missing_field_behavior"),
        ({"null_value_behavior": "skip"}, "null_value_behavior"),
        ({"empty_values_behavior": "omit"}, "empty_values_behavior"),
        ({"create_missing": "yes"}, "create_missing"),
    ],
)
def test_invalid_mapping_config_is_rejected(overrides: dict[str, Any], message: str) -> None:
    with pytest.raises(ConfigurationError, match=message):
        make_mapping(**overrides)


def test_global_create_missing_default_is_used() -> None:
    mapping = StreamMapping.from_config(
        "users",
        mapping_config(),
        default_create_missing=True,
    )

    assert mapping.create_missing is True
