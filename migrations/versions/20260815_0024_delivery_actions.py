"""Add auditable personalized-delivery action events.

Revision ID: 20260815_0024
Revises: 20260814_0023
Create Date: 2026-08-15
"""
from typing import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID


revision: str = "20260815_0024"
down_revision: str | None = "20260814_0023"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "delivery_action_events",
        sa.Column("id", UUID(as_uuid=True), nullable=False),
        sa.Column("idempotency_key", sa.String(length=64), nullable=False),
        sa.Column("schema_version", sa.String(length=64), nullable=False),
        sa.Column("action_type", sa.String(length=24), nullable=False),
        sa.Column("delivery_id", UUID(as_uuid=True), nullable=False),
        sa.Column("match_trace_id", UUID(as_uuid=True), nullable=False),
        sa.Column("match_run_id", UUID(as_uuid=True), nullable=False),
        sa.Column("opportunity_id", UUID(as_uuid=True), nullable=False),
        sa.Column("search_profile_id", UUID(as_uuid=True), nullable=False),
        sa.Column("profile_revision", sa.Integer(), nullable=False),
        sa.Column("user_id", UUID(as_uuid=True), nullable=False),
        sa.Column("source_id", sa.BigInteger(), nullable=False),
        sa.Column("source_raw_message_id", UUID(as_uuid=True), nullable=False),
        sa.Column("source_url", sa.Text(), nullable=False),
        sa.Column("actor_platform", sa.String(length=32), nullable=False),
        sa.Column(
            "actor_external_user_id",
            sa.String(length=255),
            nullable=False,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "action_type IN ('open', 'not_suitable', 'got_job')",
            name=op.f("ck_delivery_action_events_action_type_valid"),
        ),
        sa.CheckConstraint(
            "actor_platform = 'telegram' "
            "AND actor_external_user_id ~ '^[1-9][0-9]{0,19}$'",
            name=op.f("ck_delivery_action_events_actor_valid"),
        ),
        sa.CheckConstraint(
            "idempotency_key ~ '^[0-9a-f]{64}$'",
            name=op.f("ck_delivery_action_events_idempotency_key_sha256"),
        ),
        sa.CheckConstraint(
            "profile_revision >= 1",
            name=op.f("ck_delivery_action_events_profile_revision_valid"),
        ),
        sa.CheckConstraint(
            "schema_version ~ '^[a-z0-9][a-z0-9._-]{0,63}$'",
            name=op.f("ck_delivery_action_events_schema_version_valid"),
        ),
        sa.CheckConstraint(
            "length(source_url) BETWEEN 1 AND 2048",
            name=op.f("ck_delivery_action_events_source_url_bounded"),
        ),
        sa.ForeignKeyConstraint(
            ["delivery_id"],
            ["personalized_deliveries.id"],
            name=op.f(
                "fk_delivery_action_events_delivery_id_personalized_deliveries"
            ),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["match_run_id"],
            ["match_evaluation_runs.id"],
            name=op.f(
                "fk_delivery_action_events_match_run_id_match_evaluation_runs"
            ),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["match_trace_id"],
            ["match_traces.id"],
            name=op.f("fk_delivery_action_events_match_trace_id_match_traces"),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["opportunity_id"],
            ["opportunities.id"],
            name=op.f("fk_delivery_action_events_opportunity_id_opportunities"),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["search_profile_id"],
            ["search_profiles.id"],
            name=op.f(
                "fk_delivery_action_events_search_profile_id_search_profiles"
            ),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["source_id"],
            ["sources.id"],
            name=op.f("fk_delivery_action_events_source_id_sources"),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["source_raw_message_id"],
            ["raw_messages.id"],
            name=op.f(
                "fk_delivery_action_events_source_raw_message_id_raw_messages"
            ),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["users.id"],
            name=op.f("fk_delivery_action_events_user_id_users"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_delivery_action_events")),
        sa.UniqueConstraint(
            "delivery_id",
            "action_type",
            name="uq_delivery_action_events_delivery_action",
        ),
        sa.UniqueConstraint(
            "idempotency_key",
            name="uq_delivery_action_events_idempotency_key",
        ),
    )
    op.create_index(
        "ix_delivery_action_events_opportunity_action",
        "delivery_action_events",
        ["opportunity_id", "action_type", "created_at"],
        unique=False,
    )
    op.create_index(
        "ix_delivery_action_events_user_created",
        "delivery_action_events",
        ["user_id", "created_at", "id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_delivery_action_events_user_created",
        table_name="delivery_action_events",
    )
    op.drop_index(
        "ix_delivery_action_events_opportunity_action",
        table_name="delivery_action_events",
    )
    op.drop_table("delivery_action_events")
