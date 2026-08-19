"""Persist verified provider events and immutable paid periods.

Revision ID: 20260815_0027
Revises: 20260815_0026
Create Date: 2026-08-15
"""
from typing import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB, UUID


revision: str = "20260815_0027"
down_revision: str | None = "20260815_0026"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "payment_provider_events",
        sa.Column("id", UUID(as_uuid=True), nullable=False),
        sa.Column("schema_version", sa.String(length=64), nullable=False),
        sa.Column("provider", sa.String(length=64), nullable=False),
        sa.Column("provider_event_id", sa.String(length=255), nullable=False),
        sa.Column("event_type", sa.String(length=64), nullable=False),
        sa.Column("provider_payment_id", sa.String(length=255), nullable=False),
        sa.Column("user_id", UUID(as_uuid=True), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("amount", sa.Numeric(12, 2), nullable=False),
        sa.Column("currency", sa.String(length=3), nullable=False),
        sa.Column("period_start_at", sa.DateTime(timezone=True)),
        sa.Column("period_end_at", sa.DateTime(timezone=True)),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("received_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("verification_version", sa.String(length=64), nullable=False),
        sa.Column("payload", JSONB(), nullable=False),
        sa.CheckConstraint(
            "schema_version ~ '^[a-z0-9][a-z0-9._-]{0,63}$'",
            name=op.f("ck_payment_provider_events_schema_version_valid"),
        ),
        sa.CheckConstraint(
            "provider = lower(provider) "
            "AND provider ~ '^[a-z][a-z0-9_-]{0,63}$'",
            name=op.f("ck_payment_provider_events_provider_valid"),
        ),
        sa.CheckConstraint(
            "provider_event_id = btrim(provider_event_id) "
            "AND provider_event_id <> ''",
            name=op.f("ck_payment_provider_events_provider_event_id_valid"),
        ),
        sa.CheckConstraint(
            "event_type = btrim(event_type) "
            "AND event_type <> '' "
            "AND event_type ~ '^[a-z][a-z0-9._-]{0,63}$'",
            name=op.f("ck_payment_provider_events_event_type_valid"),
        ),
        sa.CheckConstraint(
            "provider_payment_id = btrim(provider_payment_id) "
            "AND provider_payment_id <> ''",
            name=op.f("ck_payment_provider_events_provider_payment_id_valid"),
        ),
        sa.CheckConstraint(
            "status IN ('pending', 'succeeded', 'failed', 'cancelled')",
            name=op.f("ck_payment_provider_events_status_valid"),
        ),
        sa.CheckConstraint(
            "amount >= 0",
            name=op.f("ck_payment_provider_events_amount_nonnegative"),
        ),
        sa.CheckConstraint(
            "currency = upper(currency) "
            "AND currency ~ '^[A-Z]{3}$'",
            name=op.f("ck_payment_provider_events_currency_valid"),
        ),
        sa.CheckConstraint(
            "(period_start_at IS NULL AND period_end_at IS NULL) "
            "OR (period_start_at IS NOT NULL "
            "AND period_end_at IS NOT NULL "
            "AND period_end_at > period_start_at)",
            name=op.f("ck_payment_provider_events_period_consistent"),
        ),
        sa.CheckConstraint(
            "status <> 'succeeded' OR (amount > 0 "
            "AND period_start_at IS NOT NULL AND period_end_at IS NOT NULL)",
            name=op.f("ck_payment_provider_events_success_evidence_complete"),
        ),
        sa.CheckConstraint(
            "verification_version ~ '^[a-z0-9][a-z0-9._-]{0,63}$'",
            name=op.f("ck_payment_provider_events_verification_version_valid"),
        ),
        sa.CheckConstraint(
            "jsonb_typeof(payload) = 'object'",
            name=op.f("ck_payment_provider_events_payload_object"),
        ),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["users.id"],
            name=op.f("fk_payment_provider_events_user_id_users"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_payment_provider_events")),
        sa.UniqueConstraint(
            "provider",
            "provider_event_id",
            name="uq_payment_provider_events_provider_event",
        ),
    )
    op.create_index(
        "ix_payment_provider_events_user_received",
        "payment_provider_events",
        ["user_id", "received_at", "id"],
    )
    op.create_index(
        "ix_payment_provider_events_payment",
        "payment_provider_events",
        ["provider", "provider_payment_id", "occurred_at"],
    )

    op.create_table(
        "subscription_periods",
        sa.Column("id", UUID(as_uuid=True), nullable=False),
        sa.Column("schema_version", sa.String(length=64), nullable=False),
        sa.Column("provider", sa.String(length=64), nullable=False),
        sa.Column("provider_payment_id", sa.String(length=255), nullable=False),
        sa.Column("payment_provider_event_id", UUID(as_uuid=True), nullable=False),
        sa.Column("user_id", UUID(as_uuid=True), nullable=False),
        sa.Column("amount", sa.Numeric(12, 2), nullable=False),
        sa.Column("currency", sa.String(length=3), nullable=False),
        sa.Column("period_start_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("period_end_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "schema_version ~ '^[a-z0-9][a-z0-9._-]{0,63}$'",
            name=op.f("ck_subscription_periods_schema_version_valid"),
        ),
        sa.CheckConstraint(
            "provider = lower(provider) "
            "AND provider ~ '^[a-z][a-z0-9_-]{0,63}$'",
            name=op.f("ck_subscription_periods_provider_valid"),
        ),
        sa.CheckConstraint(
            "provider_payment_id = btrim(provider_payment_id) "
            "AND provider_payment_id <> ''",
            name=op.f("ck_subscription_periods_provider_payment_id_valid"),
        ),
        sa.CheckConstraint(
            "amount > 0",
            name=op.f("ck_subscription_periods_amount_positive"),
        ),
        sa.CheckConstraint(
            "currency = upper(currency) "
            "AND currency ~ '^[A-Z]{3}$'",
            name=op.f("ck_subscription_periods_currency_valid"),
        ),
        sa.CheckConstraint(
            "period_end_at > period_start_at",
            name=op.f("ck_subscription_periods_period_consistent"),
        ),
        sa.ForeignKeyConstraint(
            ["payment_provider_event_id"],
            ["payment_provider_events.id"],
            name=op.f(
                "fk_subscription_periods_payment_provider_event_id_payment_provider_events"
            ),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["users.id"],
            name=op.f("fk_subscription_periods_user_id_users"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_subscription_periods")),
        sa.UniqueConstraint(
            "provider",
            "provider_payment_id",
            name="uq_subscription_periods_provider_payment",
        ),
    )
    op.create_index(
        "ix_subscription_periods_user_period",
        "subscription_periods",
        ["user_id", "period_start_at", "period_end_at", "id"],
    )

    # Payment evidence and confirmed periods are historical facts. The
    # application repository never updates them; this database guard also
    # protects the invariant from accidental direct UPDATE/DELETE statements.
    op.execute(
        sa.text(
            "CREATE OR REPLACE FUNCTION prevent_payment_history_mutation() "
            "RETURNS trigger LANGUAGE plpgsql AS $$ "
            "BEGIN RAISE EXCEPTION 'payment history is append-only'; END; "
            "$$"
        )
    )
    for table_name in ("payment_provider_events", "subscription_periods"):
        op.execute(
            sa.text(
                f"CREATE TRIGGER {table_name}_append_only "
                f"BEFORE UPDATE OR DELETE ON {table_name} "
                "FOR EACH ROW EXECUTE FUNCTION prevent_payment_history_mutation()"
            )
        )


def downgrade() -> None:
    for table_name in ("subscription_periods", "payment_provider_events"):
        op.execute(sa.text(f"DROP TRIGGER {table_name}_append_only ON {table_name}"))
    op.execute(sa.text("DROP FUNCTION prevent_payment_history_mutation()"))
    op.drop_index("ix_subscription_periods_user_period", table_name="subscription_periods")
    op.drop_table("subscription_periods")
    op.drop_index("ix_payment_provider_events_payment", table_name="payment_provider_events")
    op.drop_index(
        "ix_payment_provider_events_user_received",
        table_name="payment_provider_events",
    )
    op.drop_table("payment_provider_events")
