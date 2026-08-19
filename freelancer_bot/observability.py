from __future__ import annotations

import contextvars
import json
import logging
import re
import traceback
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, TextIO
from urllib.parse import quote
from uuid import UUID, uuid4

from pydantic import SecretStr

from .config import RuntimeConfig, Sensitivity


REDACTED = "[REDACTED]"
CONTENT_REDACTED = "[CONTENT_REDACTED]"
TRACE_ID: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "freelancer_bot_trace_id",
    default=None,
)

SECRET_FIELD_NAMES = {
    "api_hash",
    "authorization",
    "cookie",
    "cookies",
    "database_url",
    "password",
    "secret",
    "session",
    "session_id",
    "set_cookie",
    "token",
}
CONTENT_FIELD_NAMES = {
    "body",
    "content",
    "message_body",
    "message_text",
    "raw_message",
    "telegram_text",
    "text",
    "user_content",
}

URL_CREDENTIAL = re.compile(
    r"(?P<prefix>[A-Za-z][A-Za-z0-9+.-]*://[^\s:/@]+:)(?P<secret>[^\s/@]+)(?=@)",
)
AUTHORIZATION_VALUE = re.compile(r"(?i)\b(Bearer|Basic)\s+[A-Za-z0-9._~+/=-]+")
COOKIE_VALUE = re.compile(r"(?i)\b(Set-Cookie|Cookie)\s*[:=]\s*[^\r\n]+")
TELEGRAM_TOKEN = re.compile(r"\b\d{6,}:[A-Za-z0-9_-]{20,}\b")
OPENAI_TOKEN = re.compile(r"\bsk-(?:proj-)?[A-Za-z0-9_-]{20,}\b")


class Redactor:
    def __init__(self, sensitive_values: set[str] | None = None) -> None:
        variants: set[str] = set()
        for value in sensitive_values or set():
            if not value:
                continue
            variants.add(value)
            variants.add(quote(value, safe=""))
        self._sensitive_values = tuple(sorted(variants, key=len, reverse=True))

    @classmethod
    def from_config(cls, config: RuntimeConfig) -> Redactor:
        values: set[str] = set()
        for name, field in config.__class__.model_fields.items():
            metadata = field.json_schema_extra or {}
            if metadata.get("sensitivity") not in {
                Sensitivity.SENSITIVE.value,
                Sensitivity.SECRET.value,
            }:
                continue
            value = getattr(config, name)
            if value is None:
                continue
            if isinstance(value, SecretStr):
                value = value.get_secret_value()
            values.add(str(value))
        return cls(values)

    def redact(self, value: Any, *, field_name: str | None = None) -> Any:
        normalized_name = _normalize_name(field_name)
        if _is_secret_field(normalized_name):
            return REDACTED
        if normalized_name in CONTENT_FIELD_NAMES:
            return CONTENT_REDACTED
        if isinstance(value, BaseException):
            return self.redact_exception(value)
        if isinstance(value, Mapping):
            return {
                str(key): self.redact(item, field_name=str(key))
                for key, item in value.items()
            }
        if isinstance(value, (list, tuple, set, frozenset)):
            return [self.redact(item) for item in value]
        if isinstance(value, UUID):
            return str(value)
        if isinstance(value, datetime):
            return value.astimezone(timezone.utc).isoformat()
        if isinstance(value, str):
            return self.scrub_string(value)
        if value is None or isinstance(value, (bool, int, float)):
            if str(value) in self._sensitive_values:
                return REDACTED
            return value
        return self.scrub_string(str(value))

    def scrub_string(self, value: str) -> str:
        scrubbed = value
        for secret in self._sensitive_values:
            scrubbed = scrubbed.replace(secret, REDACTED)
        scrubbed = URL_CREDENTIAL.sub(r"\g<prefix>" + REDACTED, scrubbed)
        scrubbed = AUTHORIZATION_VALUE.sub(lambda match: f"{match.group(1)} {REDACTED}", scrubbed)
        scrubbed = COOKIE_VALUE.sub(lambda match: f"{match.group(1)}: {REDACTED}", scrubbed)
        scrubbed = TELEGRAM_TOKEN.sub(REDACTED, scrubbed)
        scrubbed = OPENAI_TOKEN.sub(REDACTED, scrubbed)
        return scrubbed

    def redact_exception(self, error: BaseException) -> dict[str, Any]:
        stack = "".join(traceback.format_exception(type(error), error, error.__traceback__))
        result: dict[str, Any] = {
            "type": type(error).__name__,
            "message": self.scrub_string(str(error)),
            "stack": self.scrub_string(stack),
        }
        cause = error.__cause__ or error.__context__
        if cause is not None and cause is not error:
            result["cause"] = {
                "type": type(cause).__name__,
                "message": self.scrub_string(str(cause)),
            }
        return result


class StructuredLogFormatter(logging.Formatter):
    def __init__(self, redactor: Redactor) -> None:
        super().__init__()
        self._redactor = redactor

    def format(self, record: logging.LogRecord) -> str:
        fields = dict(getattr(record, "structured_fields", {}))
        trace_id = fields.pop("correlation_id", None) or current_trace_id()
        payload: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, timezone.utc).isoformat(),
            "level": record.levelname.lower(),
            "event": getattr(record, "event", "application.log"),
        }
        if trace_id is not None:
            payload["correlation_id"] = trace_id
        message = record.getMessage()
        if message:
            payload["message"] = message
        payload.update(
            {
                key: value
                for key, value in fields.items()
                if key not in {"timestamp", "level", "event", "correlation_id"}
            }
        )
        if record.exc_info:
            payload["error"] = record.exc_info[1]
        return json.dumps(
            self._redactor.redact(payload),
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )


def configure_structured_logger(
    name: str,
    *,
    redactor: Redactor,
    stream: TextIO | None = None,
    level: int = logging.INFO,
) -> logging.Logger:
    handler = logging.StreamHandler(stream)
    handler.setFormatter(StructuredLogFormatter(redactor))
    logger = logging.getLogger(name)
    logger.disabled = False
    logger.handlers.clear()
    logger.addHandler(handler)
    logger.setLevel(level)
    logger.propagate = False
    return logger


def log_event(
    logger: logging.Logger,
    level: int,
    event: str,
    **fields: Any,
) -> None:
    logger.log(
        level,
        "",
        extra={"event": event, "structured_fields": fields},
    )


def new_correlation_id() -> UUID:
    return uuid4()


def current_trace_id() -> str | None:
    return TRACE_ID.get()


@contextmanager
def trace_context(correlation_id: UUID | str) -> Iterator[None]:
    token = TRACE_ID.set(str(correlation_id))
    try:
        yield
    finally:
        TRACE_ID.reset(token)


def _normalize_name(value: str | None) -> str:
    if value is None:
        return ""
    return re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")


def _is_secret_field(name: str) -> bool:
    if name in SECRET_FIELD_NAMES:
        return True
    return name.endswith(
        (
            "_api_hash",
            "_api_key",
            "_authorization",
            "_cookie",
            "_password",
            "_secret",
            "_session",
            "_token",
        )
    )
