"""Process-level coordination for Telethon file-backed user sessions."""

from __future__ import annotations

import fcntl
import os
from pathlib import Path
import re
from typing import ClassVar


class TelegramSessionInUseError(RuntimeError):
    """Raised when another process already owns the same Telethon session."""


class TelegramSessionFileLock:
    """Hold a non-blocking advisory lock for one Telethon session path.

    Telethon's SQLite session file is not a safe coordination primitive for two
    independent clients.  The sidecar lock is deliberately not removed on
    release: deleting a lock file while another process may be opening it can
    create a race.  The kernel lock itself is released when the descriptor is
    closed or the process exits.
    """

    _held_paths: ClassVar[set[Path]] = set()

    def __init__(self, session_path: str | Path, *, role: str = "telegram") -> None:
        self.session_path = Path(session_path)
        self.lock_path = Path(f"{self.session_path}.lock")
        self.role = _safe_role(role)
        self._descriptor: int | None = None
        self._key = self.session_path.resolve(strict=False)

    def acquire(self) -> None:
        if self._descriptor is not None:
            return
        if self._key in self._held_paths:
            raise TelegramSessionInUseError(
                "SESSION_ALREADY_IN_USE role="
                f"{self.role} owner_pid={os.getpid()} ownership=this_process"
            )

        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        descriptor = os.open(self.lock_path, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            owner = _read_owner_metadata(self.lock_path)
            os.close(descriptor)
            raise TelegramSessionInUseError(
                "SESSION_ALREADY_IN_USE "
                f"role={owner.get('role', 'unknown')} "
                f"owner_pid={owner.get('pid', 'unknown')} ownership=another_process"
            ) from None
        except BaseException:
            os.close(descriptor)
            raise

        self._descriptor = descriptor
        self._held_paths.add(self._key)
        _write_owner_metadata(descriptor, role=self.role, pid=os.getpid())

    def release(self) -> None:
        descriptor = self._descriptor
        if descriptor is None:
            return
        self._descriptor = None
        self._held_paths.discard(self._key)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)

    def __enter__(self) -> "TelegramSessionFileLock":
        self.acquire()
        return self

    def __exit__(self, *_exc: object) -> None:
        self.release()


_ROLE_RE = re.compile(r"[^A-Za-z0-9_.:-]")


def _safe_role(role: str) -> str:
    value = _ROLE_RE.sub("_", str(role).strip())[:48]
    return value or "telegram"


def _write_owner_metadata(descriptor: int, *, role: str, pid: int) -> None:
    payload = f"pid={pid} role={role}\n".encode("ascii", "replace")
    os.lseek(descriptor, 0, os.SEEK_SET)
    os.ftruncate(descriptor, 0)
    os.write(descriptor, payload)
    os.fsync(descriptor)


def _read_owner_metadata(path: Path) -> dict[str, str]:
    try:
        raw = path.read_text(encoding="ascii", errors="ignore")[:256]
    except OSError:
        return {}
    values: dict[str, str] = {}
    for key, value in re.findall(r"\b(pid|role)=([^\s]+)", raw):
        if key == "pid" and not value.isdigit():
            continue
        if key == "role":
            value = _safe_role(value)
        values[key] = value
    return values
