from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
import json
import re
from types import MappingProxyType
from typing import Any, Protocol, runtime_checkable


@dataclass(frozen=True)
class DiscoveryRequest:
    parameters: Mapping[str, Any]
    requested_at: datetime
    seed_source_ids: tuple[int, ...] = ()

    def __post_init__(self) -> None:
        _aware(self.requested_at, "requested_at")
        if any(source_id <= 0 for source_id in self.seed_source_ids):
            raise ValueError("seed_source_ids must contain positive identifiers")
        parameters = _json_mapping(self.parameters, "parameters")
        object.__setattr__(self, "parameters", MappingProxyType(parameters))
        object.__setattr__(self, "seed_source_ids", tuple(self.seed_source_ids))

    def to_payload(self) -> dict[str, Any]:
        return {
            "parameters": dict(self.parameters),
            "requested_at": self.requested_at.isoformat(),
            "seed_source_ids": list(self.seed_source_ids),
        }


@dataclass(frozen=True)
class DiscoveredSourceCandidate:
    result_key: str
    platform: str
    external_id: str
    access_type: str
    display_name: str
    discovered_at: datetime
    handle: str | None = None
    canonical_url: str | None = None
    seed_source_id: int | None = None
    seed_reference: str | None = None
    context: Mapping[str, Any] | None = None

    def __post_init__(self) -> None:
        platform = normalize_identifier(self.platform, "platform", max_length=32)
        access_type = _required_text(self.access_type, "access_type").lower()
        if access_type not in {"public", "private"}:
            raise ValueError("access_type must be public or private")
        if self.seed_source_id is not None and self.seed_source_id <= 0:
            raise ValueError("seed_source_id must be positive")
        _aware(self.discovered_at, "discovered_at")

        object.__setattr__(
            self,
            "result_key",
            _bounded_text(self.result_key, "result_key", 255),
        )
        object.__setattr__(self, "platform", platform)
        object.__setattr__(
            self,
            "external_id",
            _bounded_text(self.external_id, "external_id", 255),
        )
        object.__setattr__(self, "access_type", access_type)
        object.__setattr__(self, "display_name", _required_text(self.display_name, "display_name"))
        object.__setattr__(
            self,
            "handle",
            None
            if self.handle is None
            else _bounded_text(self.handle, "handle", 255).lower(),
        )
        object.__setattr__(
            self,
            "canonical_url",
            None
            if self.canonical_url is None
            else _required_text(self.canonical_url, "canonical_url"),
        )
        object.__setattr__(
            self,
            "seed_reference",
            None
            if self.seed_reference is None
            else _required_text(self.seed_reference, "seed_reference"),
        )
        context = _json_mapping(self.context or {}, "context")
        object.__setattr__(self, "context", MappingProxyType(context))


@runtime_checkable
class DiscoveryProvider(Protocol):
    @property
    def name(self) -> str: ...

    @property
    def kind(self) -> str: ...

    async def discover(
        self,
        request: DiscoveryRequest,
    ) -> Sequence[DiscoveredSourceCandidate]: ...


def normalize_provider_name(value: str) -> str:
    return normalize_identifier(value, "provider name", max_length=64)


def normalize_provider_kind(value: str) -> str:
    return normalize_identifier(value, "provider kind", max_length=32)


def normalize_identifier(value: str, field: str, *, max_length: int) -> str:
    normalized = _required_text(value, field).lower()
    if len(normalized) > max_length or not re.fullmatch(
        r"[a-z][a-z0-9_-]*",
        normalized,
    ):
        raise ValueError(
            f"{field} must be a lowercase-compatible identifier up to "
            f"{max_length} characters"
        )
    return normalized


def _json_mapping(value: Mapping[str, Any], field: str) -> dict[str, Any]:
    normalized = dict(value)
    try:
        json.dumps(normalized, allow_nan=False)
    except (TypeError, ValueError):
        raise ValueError(f"{field} must be JSON-compatible") from None
    return normalized


def _aware(value: datetime, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must include a timezone")
    return value


def _required_text(value: str, field: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field} must not be blank")
    return normalized


def _bounded_text(value: str, field: str, max_length: int) -> str:
    normalized = _required_text(value, field)
    if len(normalized) > max_length:
        raise ValueError(f"{field} must not exceed {max_length} characters")
    return normalized
