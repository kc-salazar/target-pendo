"""Target-specific exceptions."""

from __future__ import annotations

from typing import Any


class TargetPendoError(Exception):
    """Base exception for target-pendo."""


class ConfigurationError(TargetPendoError, ValueError):
    """Raised when target or stream configuration is invalid."""


class RecordMappingError(TargetPendoError, ValueError):
    """Raised when a Singer record cannot be mapped to a Pendo payload."""


class BatchSizeError(TargetPendoError, ValueError):
    """Raised when one mapped record exceeds the configured request-size limit."""


class PendoAPIError(TargetPendoError):
    """Raised for a non-successful Pendo HTTP response."""

    def __init__(self, status_code: int, message: str) -> None:
        self.status_code = status_code
        super().__init__(f"Pendo API returned HTTP {status_code}: {message}")


class PendoProtocolError(TargetPendoError):
    """Raised when a successful Pendo response violates the documented contract."""


class PendoPartialFailureError(TargetPendoError):
    """Raised when Pendo accepts a batch but rejects one or more records."""

    def __init__(
        self,
        *,
        total: int,
        updated: int,
        failed: int,
        kind: str,
        error_summary: dict[str, Any] | None = None,
    ) -> None:
        self.total = total
        self.updated = updated
        self.failed = failed
        self.kind = kind
        self.error_summary = error_summary or {}
        super().__init__(
            f"Pendo rejected {failed} of {total} {kind} metadata records; "
            "Singer state was not advanced"
        )
