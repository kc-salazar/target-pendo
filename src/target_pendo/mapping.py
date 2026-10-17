"""Map Singer records into Pendo bulk metadata records."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Literal, cast

from target_pendo.exceptions import ConfigurationError, RecordMappingError

_MISSING = object()
Kind = Literal["visitor", "account"]
FieldGroup = Literal["agent", "custom"]


def _resolve_json_pointer(document: Mapping[str, Any], pointer: str) -> Any:
    if pointer == "":
        return document

    current: Any = document
    for raw_token in pointer[1:].split("/"):
        token = raw_token.replace("~1", "/").replace("~0", "~")
        if isinstance(current, Mapping) and token in current:
            current = current[token]
        elif isinstance(current, list) and token.isdigit() and int(token) < len(current):
            current = current[int(token)]
        else:
            return _MISSING
    return current


def resolve_selector(record: Mapping[str, Any], selector: str) -> Any:
    """Resolve a top-level key or RFC 6901 JSON Pointer."""
    if selector.startswith("/") or selector == "":
        return _resolve_json_pointer(record, selector)
    return record.get(selector, _MISSING)


@dataclass(frozen=True)
class StreamMapping:
    """Validated mapping configuration for one Singer stream."""

    stream_name: str
    kind: Kind
    group: FieldGroup
    id_field: str
    field_mappings: dict[str, str]
    create_missing: bool
    missing_id_behavior: Literal["fail", "skip"]
    missing_field_behavior: Literal["fail", "omit"]
    null_value_behavior: Literal["fail", "omit", "send"]
    empty_values_behavior: Literal["fail", "skip"]

    @classmethod
    def from_config(
        cls,
        stream_name: str,
        raw: Mapping[str, Any],
        *,
        default_create_missing: bool,
    ) -> StreamMapping:
        """Parse and validate one stream mapping."""
        kind = raw.get("kind")
        if kind not in {"visitor", "account"}:
            raise ConfigurationError(f"streams.{stream_name}.kind must be 'visitor' or 'account'")

        group = raw.get("group", "custom")
        if group not in {"agent", "custom"}:
            raise ConfigurationError(f"streams.{stream_name}.group must be 'agent' or 'custom'")

        id_field = raw.get("id_field")
        if not isinstance(id_field, str) or not id_field:
            raise ConfigurationError(f"streams.{stream_name}.id_field must be a string")

        raw_mappings = raw.get("field_mappings")
        if not isinstance(raw_mappings, Mapping) or not raw_mappings:
            raise ConfigurationError(
                f"streams.{stream_name}.field_mappings must contain at least one mapping"
            )
        field_mappings: dict[str, str] = {}
        for target_field, selector in raw_mappings.items():
            if not isinstance(target_field, str) or not target_field:
                raise ConfigurationError(
                    f"streams.{stream_name}.field_mappings contains an invalid Pendo field name"
                )
            if not isinstance(selector, str) or not selector:
                raise ConfigurationError(
                    f"streams.{stream_name}.field_mappings.{target_field} "
                    "must be a source field or JSON Pointer"
                )
            field_mappings[target_field] = selector

        missing_id_behavior = raw.get("missing_id_behavior", "fail")
        missing_field_behavior = raw.get("missing_field_behavior", "omit")
        null_value_behavior = raw.get("null_value_behavior", "omit")
        empty_values_behavior = raw.get("empty_values_behavior", "skip")

        allowed = {
            "missing_id_behavior": (missing_id_behavior, {"fail", "skip"}),
            "missing_field_behavior": (missing_field_behavior, {"fail", "omit"}),
            "null_value_behavior": (null_value_behavior, {"fail", "omit", "send"}),
            "empty_values_behavior": (empty_values_behavior, {"fail", "skip"}),
        }
        for name, (value, choices) in allowed.items():
            if value not in choices:
                rendered = ", ".join(sorted(choices))
                raise ConfigurationError(f"streams.{stream_name}.{name} must be one of: {rendered}")

        create_missing = raw.get("create_missing", default_create_missing)
        if not isinstance(create_missing, bool):
            raise ConfigurationError(f"streams.{stream_name}.create_missing must be true or false")

        return cls(
            stream_name=stream_name,
            kind=cast("Kind", kind),
            group=cast("FieldGroup", group),
            id_field=id_field,
            field_mappings=field_mappings,
            create_missing=create_missing,
            missing_id_behavior=cast("Literal['fail', 'skip']", missing_id_behavior),
            missing_field_behavior=cast("Literal['fail', 'omit']", missing_field_behavior),
            null_value_behavior=cast("Literal['fail', 'omit', 'send']", null_value_behavior),
            empty_values_behavior=cast("Literal['fail', 'skip']", empty_values_behavior),
        )

    @property
    def pendo_id_key(self) -> str:
        """Return the ID key required by Pendo's bulk request format."""
        return "visitorId" if self.kind == "visitor" else "accountId"

    def map_record(self, record: Mapping[str, Any]) -> dict[str, Any] | None:
        """Convert a Singer record into one Pendo bulk-update item."""
        source_id = resolve_selector(record, self.id_field)
        if source_id is _MISSING or source_id is None or str(source_id).strip() == "":
            if self.missing_id_behavior == "skip":
                return None
            raise RecordMappingError(
                f"Stream {self.stream_name!r} record is missing a usable ID at "
                f"selector {self.id_field!r}"
            )

        values: dict[str, Any] = {}
        for pendo_field, selector in self.field_mappings.items():
            value = resolve_selector(record, selector)
            if value is _MISSING:
                if self.missing_field_behavior == "fail":
                    raise RecordMappingError(
                        f"Stream {self.stream_name!r} record is missing selector "
                        f"{selector!r} for Pendo field {pendo_field!r}"
                    )
                continue
            if value is None:
                if self.null_value_behavior == "fail":
                    raise RecordMappingError(
                        f"Stream {self.stream_name!r} record has null at selector "
                        f"{selector!r} for Pendo field {pendo_field!r}"
                    )
                if self.null_value_behavior == "omit":
                    continue
            values[pendo_field] = value

        if not values:
            if self.empty_values_behavior == "skip":
                return None
            raise RecordMappingError(
                f"Stream {self.stream_name!r} record produced no Pendo metadata values"
            )

        return {self.pendo_id_key: str(source_id), "values": values}
