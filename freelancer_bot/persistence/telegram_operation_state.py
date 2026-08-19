from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Any
from uuid import UUID

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncConnection

from .schema import (
    collector_accounts,
    telegram_collector_operation_events,
    telegram_collector_operation_state,
)


class TelegramCollectorStatus(str, Enum):
    READY = "ready"
    PACING = "pacing"
    FLOODWAIT = "floodwait"
    PAUSED = "paused"


class TelegramOperationOutcome(str, Enum):
    COMPLETED = "completed"
    ERROR = "error"
    FLOODWAIT = "floodwait"


@dataclass(frozen=True)
class TelegramCollectorOperationState:
    collector_account_id: int
    status: TelegramCollectorStatus
    active_request_token: UUID | None
    active_request_category: str | None
    active_request_started_at: datetime | None
    active_request_lease_until: datetime | None
    last_request_at: datetime | None
    next_allowed_request_at: datetime | None
    cooldown_until: datetime | None
    last_request_category: str | None
    last_floodwait_detected_at: datetime | None
    last_floodwait_seconds: int | None
    updated_at: datetime


@dataclass(frozen=True)
class TelegramCollectorStatusRecord:
    collector_account_id: int
    status: TelegramCollectorStatus
    active_request_category: str | None
    last_request_at: datetime | None
    next_allowed_request_at: datetime | None
    cooldown_until: datetime | None
    last_floodwait_detected_at: datetime | None
    last_floodwait_seconds: int | None
    requests_last_5m: int


@dataclass(frozen=True)
class TelegramRequestReservation:
    acquired: bool
    wait_until: datetime | None
    state: TelegramCollectorOperationState


class TelegramCollectorFloodWaitActive(RuntimeError):
    def __init__(self, state: TelegramCollectorOperationState) -> None:
        self.state = state
        remaining = _remaining_seconds(state.cooldown_until)
        super().__init__(
            f"Telegram collector {state.collector_account_id} is under FloodWait "
            f"for approximately {remaining} seconds"
        )


class TelegramCollectorPaused(RuntimeError):
    pass


class TelegramCollectorOperationRepository:
    async def ensure(
        self,
        connection: AsyncConnection,
        *,
        collector_account_id: int,
    ) -> TelegramCollectorOperationState:
        _positive_account_id(collector_account_id)
        await connection.execute(
            pg_insert(telegram_collector_operation_state)
            .values(collector_account_id=collector_account_id)
            .on_conflict_do_nothing(
                index_elements=[
                    telegram_collector_operation_state.c.collector_account_id
                ]
            )
        )
        return await self.get(connection, collector_account_id)

    async def get(
        self,
        connection: AsyncConnection,
        collector_account_id: int,
    ) -> TelegramCollectorOperationState:
        row = (
            await connection.execute(
                sa.select(telegram_collector_operation_state).where(
                    telegram_collector_operation_state.c.collector_account_id
                    == collector_account_id
                )
            )
        ).mappings().one_or_none()
        if row is None:
            raise LookupError(
                f"Telegram collector operation state {collector_account_id} does not exist"
            )
        return _state_record(row)

    async def reserve(
        self,
        connection: AsyncConnection,
        *,
        collector_account_id: int,
        request_token: UUID,
        request_category: str,
        now: datetime,
        lease_seconds: float,
    ) -> TelegramRequestReservation:
        _aware(now, "now")
        category = _safe_category(request_category)
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        await self.ensure(connection, collector_account_id=collector_account_id)
        row = (
            await connection.execute(
                sa.select(telegram_collector_operation_state)
                .where(
                    telegram_collector_operation_state.c.collector_account_id
                    == collector_account_id
                )
                .with_for_update()
            )
        ).mappings().one()
        state = _state_record(row)
        if state.status is TelegramCollectorStatus.PAUSED:
            raise TelegramCollectorPaused(
                f"Telegram collector {collector_account_id} is paused"
            )
        if (
            state.status is TelegramCollectorStatus.FLOODWAIT
            and state.cooldown_until is not None
            and state.cooldown_until > now
        ):
            raise TelegramCollectorFloodWaitActive(state)

        if (
            state.active_request_token is not None
            and state.active_request_lease_until is not None
            and state.active_request_lease_until > now
        ):
            return TelegramRequestReservation(
                acquired=False,
                wait_until=state.active_request_lease_until,
                state=state,
            )

        if (
            state.next_allowed_request_at is not None
            and state.next_allowed_request_at > now
        ):
            return TelegramRequestReservation(
                acquired=False,
                wait_until=state.next_allowed_request_at,
                state=state,
            )

        lease_until = now + timedelta(seconds=lease_seconds)
        await connection.execute(
            sa.update(telegram_collector_operation_state)
            .where(
                telegram_collector_operation_state.c.collector_account_id
                == collector_account_id
            )
            .values(
                status=TelegramCollectorStatus.PACING.value,
                active_request_token=request_token,
                active_request_category=category,
                active_request_started_at=now,
                active_request_lease_until=lease_until,
                cooldown_until=None,
                updated_at=sa.func.now(),
            )
        )
        return TelegramRequestReservation(
            acquired=True,
            wait_until=None,
            state=await self.get(connection, collector_account_id),
        )

    async def finish(
        self,
        connection: AsyncConnection,
        *,
        collector_account_id: int,
        request_token: UUID,
        finished_at: datetime,
        outcome: TelegramOperationOutcome | str,
        next_allowed_request_at: datetime | None,
        floodwait_seconds: int | None = None,
    ) -> TelegramCollectorOperationState:
        _aware(finished_at, "finished_at")
        result_outcome = _outcome(outcome)
        if next_allowed_request_at is not None:
            _aware(next_allowed_request_at, "next_allowed_request_at")
        if result_outcome is TelegramOperationOutcome.FLOODWAIT:
            if floodwait_seconds is None or floodwait_seconds <= 0:
                raise ValueError("floodwait_seconds must be positive for FloodWait")
            cooldown_until = finished_at + timedelta(seconds=floodwait_seconds)
            next_allowed_request_at = cooldown_until
            status = TelegramCollectorStatus.FLOODWAIT.value
            last_floodwait_at = finished_at
            last_floodwait_value = floodwait_seconds
        else:
            cooldown_until = None
            status = TelegramCollectorStatus.READY.value
            last_floodwait_at = None
            last_floodwait_value = None

        row = (
            await connection.execute(
                sa.select(telegram_collector_operation_state)
                .where(
                    telegram_collector_operation_state.c.collector_account_id
                    == collector_account_id
                )
                .with_for_update()
            )
        ).mappings().one_or_none()
        if row is None:
            raise LookupError(
                f"Telegram collector operation state {collector_account_id} does not exist"
            )
        state = _state_record(row)
        if state.active_request_token != request_token:
            raise RuntimeError("Telegram request lease is no longer owned by this worker")
        if result_outcome is not TelegramOperationOutcome.FLOODWAIT:
            last_floodwait_at = state.last_floodwait_detected_at
            last_floodwait_value = state.last_floodwait_seconds

        await connection.execute(
            sa.insert(telegram_collector_operation_events).values(
                collector_account_id=collector_account_id,
                request_token=request_token,
                request_category=state.active_request_category,
                started_at=state.active_request_started_at,
                finished_at=finished_at,
                outcome=result_outcome.value,
                floodwait_seconds=(
                    floodwait_seconds
                    if result_outcome is TelegramOperationOutcome.FLOODWAIT
                    else None
                ),
            )
        )
        await connection.execute(
            sa.update(telegram_collector_operation_state)
            .where(
                telegram_collector_operation_state.c.collector_account_id
                == collector_account_id
            )
            .values(
                status=status,
                active_request_token=None,
                active_request_category=None,
                active_request_started_at=None,
                active_request_lease_until=None,
                last_request_at=finished_at,
                next_allowed_request_at=next_allowed_request_at,
                cooldown_until=cooldown_until,
                last_request_category=state.active_request_category,
                last_floodwait_detected_at=last_floodwait_at,
                last_floodwait_seconds=last_floodwait_value,
                updated_at=sa.func.now(),
            )
        )
        return await self.get(connection, collector_account_id)

    async def list_status(
        self,
        connection: AsyncConnection,
        *,
        now: datetime,
        limit: int = 100,
    ) -> tuple[TelegramCollectorStatusRecord, ...]:
        _aware(now, "now")
        if not 1 <= limit <= 1000:
            raise ValueError("limit must be between 1 and 1000")
        accounts = (
            await connection.execute(
                sa.select(collector_accounts.c.id)
                .where(collector_accounts.c.is_active.is_(True))
                .order_by(collector_accounts.c.id)
                .limit(limit)
            )
        ).scalars().all()
        records: list[TelegramCollectorStatusRecord] = []
        for account_id in accounts:
            state = (
                await connection.execute(
                    sa.select(telegram_collector_operation_state).where(
                        telegram_collector_operation_state.c.collector_account_id
                        == account_id
                    )
                )
            ).mappings().one_or_none()
            if state is None:
                records.append(
                    TelegramCollectorStatusRecord(
                        collector_account_id=int(account_id),
                        status=TelegramCollectorStatus.READY,
                        active_request_category=None,
                        last_request_at=None,
                        next_allowed_request_at=None,
                        cooldown_until=None,
                        last_floodwait_detected_at=None,
                        last_floodwait_seconds=None,
                        requests_last_5m=0,
                    )
                )
                continue
            count = await connection.scalar(
                sa.select(sa.func.count())
                .select_from(telegram_collector_operation_events)
                .where(
                    telegram_collector_operation_events.c.collector_account_id
                    == account_id,
                    telegram_collector_operation_events.c.finished_at
                    >= now - timedelta(minutes=5),
                )
            )
            item = _state_record(state)
            records.append(
                TelegramCollectorStatusRecord(
                    collector_account_id=item.collector_account_id,
                    status=item.status,
                    active_request_category=item.active_request_category,
                    last_request_at=item.last_request_at,
                    next_allowed_request_at=item.next_allowed_request_at,
                    cooldown_until=item.cooldown_until,
                    last_floodwait_detected_at=item.last_floodwait_detected_at,
                    last_floodwait_seconds=item.last_floodwait_seconds,
                    requests_last_5m=int(count or 0),
                )
            )
        return tuple(records)


def _state_record(row: Mapping[str, Any]) -> TelegramCollectorOperationState:
    return TelegramCollectorOperationState(
        collector_account_id=int(row["collector_account_id"]),
        status=TelegramCollectorStatus(row["status"]),
        active_request_token=row["active_request_token"],
        active_request_category=row["active_request_category"],
        active_request_started_at=row["active_request_started_at"],
        active_request_lease_until=row["active_request_lease_until"],
        last_request_at=row["last_request_at"],
        next_allowed_request_at=row["next_allowed_request_at"],
        cooldown_until=row["cooldown_until"],
        last_request_category=row["last_request_category"],
        last_floodwait_detected_at=row["last_floodwait_detected_at"],
        last_floodwait_seconds=row["last_floodwait_seconds"],
        updated_at=row["updated_at"],
    )


def _outcome(value: TelegramOperationOutcome | str) -> TelegramOperationOutcome:
    try:
        return TelegramOperationOutcome(value)
    except ValueError:
        raise ValueError(f"Unknown Telegram operation outcome: {value}") from None


def _safe_category(value: str) -> str:
    normalized = value.strip().lower()
    if not normalized or len(normalized) > 64:
        raise ValueError("request_category must contain 1 to 64 characters")
    if not all(ch.isascii() and (ch.isalnum() or ch in "_.-") for ch in normalized):
        raise ValueError("request_category must be a safe identifier")
    if not normalized[0].isalpha():
        raise ValueError("request_category must start with a letter")
    return normalized


def _positive_account_id(value: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError("collector_account_id must be positive")


def _aware(value: datetime, field: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must include a timezone")


def _remaining_seconds(value: datetime | None) -> int:
    if value is None:
        return 0
    return max(0, int((value - datetime.now(value.tzinfo)).total_seconds()))
