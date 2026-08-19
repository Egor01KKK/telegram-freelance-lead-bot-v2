from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any


DEFAULT_SOURCES_PATH = Path("config/sources.json")
HANDLE_RE = re.compile(r"^@?[A-Za-z0-9_]{5,32}$")


@dataclass(frozen=True)
class Source:
    handle: str
    title: str
    reason: str
    enabled: bool = True
    tags: tuple[str, ...] = ()

    @property
    def username(self) -> str:
        return self.handle.removeprefix("@")

    @property
    def telegram_url(self) -> str:
        return f"https://t.me/{self.username}"


def _required_text(item: dict[str, Any], field: str, index: int) -> str:
    value = item.get(field)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"Source #{index}: '{field}' must be a non-empty string")
    return value.strip()


def _normalize_handle(value: str, index: int) -> str:
    handle = value.strip()
    if not HANDLE_RE.fullmatch(handle):
        raise ValueError(
            f"Source #{index}: invalid Telegram username '{value}'. "
            "Use 5-32 Latin letters, digits or underscores, with optional @."
        )
    return f"@{handle.removeprefix('@')}"


def load_sources(path: Path = DEFAULT_SOURCES_PATH) -> list[Source]:
    """Load and validate source metadata from a JSON file."""
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ValueError(f"Sources config was not found: {path}") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(f"Sources config is not valid JSON: {path}: {exc.msg}") from exc
    except OSError as exc:
        raise ValueError(f"Could not read sources config {path}: {exc}") from exc

    if not isinstance(payload, list):
        raise ValueError(f"Sources config must contain a JSON array: {path}")

    sources: list[Source] = []
    seen_handles: set[str] = set()
    for index, item in enumerate(payload, 1):
        if not isinstance(item, dict):
            raise ValueError(f"Source #{index}: expected a JSON object")

        handle = _normalize_handle(_required_text(item, "handle", index), index)
        if handle.lower() in seen_handles:
            raise ValueError(f"Source #{index}: duplicate handle '{handle}'")
        seen_handles.add(handle.lower())

        enabled = item.get("enabled", True)
        if not isinstance(enabled, bool):
            raise ValueError(f"Source #{index}: 'enabled' must be true or false")

        tags = item.get("tags", [])
        if not isinstance(tags, list) or any(not isinstance(tag, str) or not tag.strip() for tag in tags):
            raise ValueError(f"Source #{index}: 'tags' must be an array of non-empty strings")

        sources.append(
            Source(
                handle=handle,
                title=_required_text(item, "title", index),
                reason=_required_text(item, "reason", index),
                enabled=enabled,
                tags=tuple(tag.strip() for tag in tags),
            )
        )

    if not sources:
        raise ValueError(f"Sources config must contain at least one source: {path}")
    return sources


def enabled_sources(path: Path = DEFAULT_SOURCES_PATH) -> list[Source]:
    return [source for source in load_sources(path) if source.enabled]
