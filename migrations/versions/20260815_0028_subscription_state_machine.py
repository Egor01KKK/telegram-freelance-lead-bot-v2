"""Persist provider-neutral subscription state projections and transitions.

Revision ID: 20260815_0028
Revises: 20260815_0027
Create Date: 2026-08-15
"""
from typing import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID


revision: str = "20260815_0028"
down_revision: str | None = "20260815_0027"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "subscription_states",
        sa.Column("user_id", UUID(as_uuid=True), nullable=False),
        sa.Column("state", sa.String(length=24), nullable=False),
        sa.Column("state_version", sa.Integer(), server_default="1", nullable=False),
        sa.Column("provider", sa.String(length=64)),
        sa.Column("current_period_id", UUID(as_uuid=True)),
        sa.Column("current_period_start_at", sa.DateTime(timezone=True)),
        sa.Column("current_period_end_at", sa.DateTime(timezone=True)),
        sa.Column("reason", sa.String(length=64), nullable=False),
        sa.Column("changed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "state IN ('trial_not_started', 'trial_active', 'paid_active', "
            "'expired', 'cancelled', 'paused')",
            name=op.f("ck_subscription_states_state_valid"),
        ),
        sa.CheckConstraint(
            "state_version >= 1",
            name=op.f("ck_subscription_states_state_version_valid"),
        ),
        sa.CheckConstraint(
            "provider IS NULL OR (provider = lower(provider) "
            "AND provider ~ '^[a-z][a-z0-9_-]{0,63}$')",
            name=op.f("ck_subscription_states_provider_valid"),
        ),
        sa.CheckConstraint(
            "(current_period_id IS NULL AND current_period_start_at IS NULL "
            "AND current_period_end_at IS NULL) OR "
            "(current_period_id IS NOT NULL "
            "AND current_period_start_at IS NOT NULL "
            "AND current_period_end_at IS NOT NULL "
            "AND current_period_end_at > current_period_start_at)",
            name=op.f("ck_subscription_states_current_period_consistent"),
        ),
        sa.CheckConstraint(
            "state <> 'paid_active' OR current_period_id IS NOT NULL",
            name=op.f("ck_subscription_states_paid_state_requires_period"),
        ),
        sa.CheckConstraint(
            "reason = btrim(reason) AND reason <> '' "
            "AND reason ~ '^[a-z][a-z0-9._-]{0,63}$'",
            name=op.f("ck_subscription_states_reason_valid"),
        ),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["users.id"],
            name=op.f("fk_subscription_states_user_id_users"),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["current_period_id"],
            ["subscription_periods.id"],
            name=op.f(
                "fk_subscription_states_current_period_id_subscription_periods"
            ),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("user_id", name=op.f("pk_subscription_states")),
    )

    op.create_table(
        "subscription_state_events",
        sa.Column("id", UUID(as_uuid=True), nullable=False),
        sa.Column("idempotency_key", sa.String(length=64), nullable=False),
        sa.Column("schema_version", sa.String(length=64), nullable=False),
        sa.Column("user_id", UUID(as_uuid=True), nullable=False),
        sa.Column("state_version", sa.Integer(), nullable=False),
        sa.Column("from_state", sa.String(length=24)),
        sa.Column("to_state", sa.String(length=24), nullable=False),
        sa.Column("provider", sa.String(length=64)),
        sa.Column("subscription_period_id", UUID(as_uuid=True)),
        sa.Column("reason", sa.String(length=64), nullable=False),
        sa.Column("effective_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "idempotency_key ~ '^[0-9a-f]{64}$'",
            name=op.f("ck_subscription_state_events_idempotency_key_sha256"),
        ),
        sa.CheckConstraint(
            "schema_version ~ '^[a-z0-9][a-z0-9._-]{0,63}$'",
            name=op.f("ck_subscription_state_events_schema_version_valid"),
        ),
        sa.CheckConstraint(
            "state_version >= 1",
            name=op.f("ck_subscription_state_events_state_version_valid"),
        ),
        sa.CheckConstraint(
            "from_state IS NULL OR from_state IN ('trial_not_started', 'trial_active', "
            "'paid_active', 'expired', 'cancelled', 'paused')",
            name=op.f("ck_subscription_state_events_from_state_valid"),
        ),
        sa.CheckConstraint(
            "to_state IN ('trial_not_started', 'trial_active', 'paid_active', "
            "'expired', 'cancelled', 'paused')",
            name=op.f("ck_subscription_state_events_to_state_valid"),
        ),
        sa.CheckConstraint(
            "provider IS NULL OR (provider = lower(provider) "
            "AND provider ~ '^[a-z][a-z0-9_-]{0,63}$')",
            name=op.f("ck_subscription_state_events_provider_valid"),
        ),
        sa.CheckConstraint(
            "to_state <> 'paid_active' OR subscription_period_id IS NOT NULL",
            name=op.f("ck_subscription_state_events_paid_state_requires_period"),
        ),
        sa.CheckConstraint(
            "reason = btrim(reason) AND reason <> '' "
            "AND reason ~ '^[a-z][a-z0-9._-]{0,63}$'",
            name=op.f("ck_subscription_state_events_reason_valid"),
        ),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["users.id"],
            name=op.f("fk_subscription_state_events_user_id_users"),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["subscription_period_id"],
            ["subscription_periods.id"],
            name=op.f(
                "fk_subscription_state_events_subscription_period_id_subscription_periods"
            ),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_subscription_state_events")),
        sa.UniqueConstraint(
            "user_id",
            "state_version",
            name="uq_subscription_state_events_user_version",
        ),
        sa.UniqueConstraint(
            "idempotency_key",
            name="uq_subscription_state_events_idempotency_key",
        ),
    )
    op.create_index(
        "ix_subscription_state_events_user_created",
        "subscription_state_events",
        ["user_id", "created_at", "id"],
    )

    # The function is created by 20260815_0027 and protects all immutable
    # billing history, including this transition log.
    op.execute(
        sa.text(
            "CREATE TRIGGER subscription_state_events_append_only "
            "BEFORE UPDATE OR DELETE ON subscription_state_events "
            "FOR EACH ROW EXECUTE FUNCTION prevent_payment_history_mutation()"
        )
    )


def downgrade() -> None:
    op.execute(
        sa.text(
            "DROP TRIGGER subscription_state_events_append_only "
            "ON subscription_state_events"
        )
    )
    op.drop_index(
        "ix_subscription_state_events_user_created",
        table_name="subscription_state_events",
    )
    op.drop_table("subscription_state_events")
    op.drop_table("subscription_states")
