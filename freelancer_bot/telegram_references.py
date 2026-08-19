"""Pure, local normalization for Telegram source and message references.

The bootstrap pipeline uses this module before any Telegram API call.  It is
intentionally conservative: an uncertain reference is rejected locally rather
than being resolved with a scarce collector request.
"""

from __future__ import annotations

from dataclasses import dataclass
import re
from urllib.parse import unquote, urlsplit


_HOSTS = {"t.me", "telegram.me", "telegram.dog"}
_RESERVED = {
    "addemoji",
    "addlist",
    "addstickers",
    "addtheme",
    "boost",
    "contact",
    "confirmphone",
    "invoice",
    "joinchat",
    "login",
    "proxy",
    "resolve",
    "share",
    "socks",
    "stars_topup",
}
_HANDLE = re.compile(r"^[a-z][a-z0-9_]{3,31}$", re.IGNORECASE)
_NUMERIC_PEER = re.compile(r"^-?\d{5,20}$")


@dataclass(frozen=True)
class TelegramReference:
    """A locally validated Telegram reference.

    ``source_key`` never includes a message id.  ``message_id`` is retained
    separately so a message URL can provide source evidence without becoming a
    second source identity.
    """

    raw: str
    normalized: str
    source_key: str
    reference_kind: str
    handle: str | None = None
    numeric_peer: str | None = None
    message_id: int | None = None
    is_invite: bool = False

    @property
    def is_source_reference(self) -> bool:
        return self.reference_kind == "source"


class InvalidTelegramReference(ValueError):
    pass


def normalize_telegram_reference(value: str) -> TelegramReference:
    """Normalize supported Telegram URL forms without network access."""

    raw = value.strip()
    if not raw:
        raise InvalidTelegramReference("telegram reference is empty")
    candidate = raw if "://" in raw else f"https://{raw}"
    try:
        parsed = urlsplit(candidate)
    except ValueError:
        raise InvalidTelegramReference("telegram reference is malformed") from None
    if parsed.scheme not in {"http", "https"}:
        raise InvalidTelegramReference("telegram reference must use HTTP(S)")
    host = (parsed.hostname or "").lower().removeprefix("www.")
    if host not in _HOSTS:
        raise InvalidTelegramReference("reference is not a Telegram URL")
    if parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise InvalidTelegramReference("Telegram reference contains unsupported URL parts")
    parts = [unquote(part).strip() for part in parsed.path.split("/") if part.strip()]
    if parts and parts[0].casefold() == "s":
        parts = parts[1:]
    if not parts:
        raise InvalidTelegramReference("Telegram source path is missing")

    first = parts[0]
    lowered = first.casefold()
    invite_token: str | None = None
    if first.startswith("+"):
        if len(parts) != 1:
            raise InvalidTelegramReference("Telegram invite reference is malformed")
        invite_token = first.removeprefix("+")
    elif lowered in {"joinchat", "invite"}:
        if len(parts) != 2:
            raise InvalidTelegramReference("Telegram invite reference is malformed")
        invite_token = parts[1]
    if invite_token is not None:
        token = invite_token
        if not re.fullmatch(r"[a-z0-9_-]{8,128}", token, re.IGNORECASE):
            raise InvalidTelegramReference("Telegram invite reference is malformed")
        normalized = f"invite:{token.casefold()}"
        return TelegramReference(
            raw=raw,
            normalized=normalized,
            source_key=normalized,
            reference_kind="invite",
            is_invite=True,
        )
    if lowered == "c":
        if len(parts) not in {2, 3} or not parts[1].isdigit() or int(parts[1]) <= 0:
            raise InvalidTelegramReference("Telegram numeric source reference is malformed")
        if len(parts) == 3 and (not parts[2].isdigit() or int(parts[2]) <= 0):
            raise InvalidTelegramReference("Telegram message reference is malformed")
        numeric_peer = f"-100{int(parts[1])}"
        message_id = None if len(parts) == 2 else int(parts[2])
        source_key = f"peer:{numeric_peer}"
        normalized = source_key if message_id is None else f"{source_key}/{message_id}"
        return TelegramReference(
            raw=raw,
            normalized=normalized,
            source_key=source_key,
            reference_kind="message" if message_id is not None else "source",
            numeric_peer=numeric_peer,
            message_id=message_id,
        )
    if lowered in _RESERVED or lowered.startswith("bot"):
        raise InvalidTelegramReference("Telegram reference is not a community source")

    message_id: int | None = None
    if len(parts) > 1:
        if len(parts) != 2 or not parts[1].isdigit() or int(parts[1]) <= 0:
            raise InvalidTelegramReference("Telegram message reference is malformed")
        message_id = int(parts[1])
    if _NUMERIC_PEER.fullmatch(first):
        source_key = f"peer:{first}"
        normalized = source_key if message_id is None else f"{source_key}/{message_id}"
        return TelegramReference(
            raw=raw,
            normalized=normalized,
            source_key=source_key,
            reference_kind="message" if message_id is not None else "source",
            numeric_peer=first,
            message_id=message_id,
        )
    if not _HANDLE.fullmatch(first):
        raise InvalidTelegramReference("Telegram handle is malformed")
    handle = first.casefold()
    source_key = f"username:{handle}"
    normalized = f"https://t.me/{handle}"
    if message_id is not None:
        normalized = f"{normalized}/{message_id}"
    return TelegramReference(
        raw=raw,
        normalized=normalized,
        source_key=source_key,
        reference_kind="message" if message_id is not None else "source",
        handle=handle,
        message_id=message_id,
    )


def normalize_telegram_url(value: str) -> str:
    """Compatibility helper returning a stable normalized source/message URL."""

    return normalize_telegram_reference(value).normalized


def telegram_source_identity(value: str) -> str:
    """Return the pre-resolution deduplication key for a source reference."""

    return normalize_telegram_reference(value).source_key
