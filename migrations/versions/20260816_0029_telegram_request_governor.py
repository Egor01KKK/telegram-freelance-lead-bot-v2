"""Persist per-collector Telegram discovery/audit request governance state.

Revision ID: 20260816_0029
Revises: 20260815_0028
Create Date: 2026-08-16
"""

from typing import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID


revision: str = "20260816_0029"
down_revision: str | None = "20260815_0028"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "telegram_collector_operation_state",
        sa.Column("collector_account_id", sa.BigInteger(), nullable=False),
        sa.Column("status", sa.String(length=16), server_default="ready", nullable=False),
        sa.Column("active_request_token", UUID(as_uuid=True)),
        sa.Column("active_request_category", sa.String(length=64)),
        sa.Column("active_request_started_at", sa.DateTime(timezone=True)),
        sa.Column("active_request_lease_until", sa.DateTime(timezone=True)),
        sa.Column("last_request_at", sa.DateTime(timezone=True)),
        sa.Column("next_allowed_request_at", sa.DateTime(timezone=True)),
        sa.Column("cooldown_until", sa.DateTime(timezone=True)),
        sa.Column("last_request_category", sa.String(length=64)),
        sa.Column("last_floodwait_detected_at", sa.DateTime(timezone=True)),
        sa.Column("last_floodwait_seconds", sa.Integer()),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.CheckConstraint(
            "status IN ('ready', 'pacing', 'floodwait', 'paused')",
            name=op.f("ck_telegram_collector_operation_state_status_valid"),
        ),
        sa.CheckConstraint(
            "(active_request_token IS NULL AND active_request_category IS NULL "
            "AND active_request_started_at IS NULL AND active_request_lease_until IS NULL) "
            "OR (active_request_token IS NOT NULL AND active_request_category IS NOT NULL "
            "AND active_request_started_at IS NOT NULL "
            "AND active_request_lease_until IS NOT NULL)",
            name=op.f(
                "ck_telegram_collector_operation_state_active_request_consistent"
            ),
        ),
        sa.CheckConstraint(
            "last_floodwait_seconds IS NULL OR last_floodwait_seconds > 0",
            name=op.f(
                "ck_telegram_collector_operation_state_last_floodwait_seconds_positive"
            ),
        ),
        sa.ForeignKeyConstraint(
            ["collector_account_id"],
            ["collector_accounts.id"],
            name=op.f(
                "fk_telegram_collector_operation_state_account_collector_accounts"
            ),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint(
            "collector_account_id",
            name=op.f("pk_telegram_collector_operation_state"),
        ),
    )
    op.create_index(
        "ix_telegram_collector_operation_state_status_cooldown",
        "telegram_collector_operation_state",
        ["status", "cooldown_until"],
    )

    op.create_table(
        "telegram_collector_operation_events",
        sa.Column("id", sa.BigInteger(), sa.Identity(), nullable=False),
        sa.Column("collector_account_id", sa.BigInteger(), nullable=False),
        sa.Column("request_token", UUID(as_uuid=True), nullable=False),
        sa.Column("request_category", sa.String(length=64), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("outcome", sa.String(length=16), nullable=False),
        sa.Column("floodwait_seconds", sa.Integer()),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.CheckConstraint(
            "request_category ~ '^[a-z][a-z0-9_.-]{0,63}$'",
            name=op.f("ck_telegram_collector_operation_events_request_category_safe"),
        ),
        sa.CheckConstraint(
            "outcome IN ('completed', 'error', 'floodwait')",
            name=op.f("ck_telegram_collector_operation_events_outcome_valid"),
        ),
        sa.CheckConstraint(
            "finished_at >= started_at",
            name=op.f("ck_telegram_collector_operation_events_finished_after_started"),
        ),
        sa.CheckConstraint(
            "(outcome = 'floodwait' AND floodwait_seconds IS NOT NULL "
            "AND floodwait_seconds > 0) OR "
            "(outcome <> 'floodwait' AND floodwait_seconds IS NULL)",
            name=op.f(
                "ck_telegram_collector_operation_events_floodwait_fields_consistent"
            ),
        ),
        sa.ForeignKeyConstraint(
            ["collector_account_id"],
            ["collector_accounts.id"],
            name=op.f(
                "fk_telegram_collector_operation_events_account_collector_accounts"
            ),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint(
            "id", name=op.f("pk_telegram_collector_operation_events")
        ),
    )
    op.create_index(
        "ix_telegram_collector_operation_events_account_finished",
        "telegram_collector_operation_events",
        ["collector_account_id", "finished_at"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_telegram_collector_operation_events_account_finished",
        table_name="telegram_collector_operation_events",
    )
    op.drop_table("telegram_collector_operation_events")
    op.drop_index(
        "ix_telegram_collector_operation_state_status_cooldown",
        table_name="telegram_collector_operation_state",
    )
    op.drop_table("telegram_collector_operation_state")
