"""Add canonical opportunity lifecycle state and immutable history.

Revision ID: 20260809_0016
Revises: 20260809_0015
Create Date: 2026-08-09
"""
from typing import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "20260809_0016"
down_revision: str | None = "20260809_0015"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

LIFECYCLE_STATUSES = "'active', 'stale', 'closed', 'retracted', 'suppressed'"


def upgrade() -> None:
    op.add_column(
        "opportunities",
        sa.Column(
            "lifecycle_status",
            sa.String(length=16),
            server_default="active",
            nullable=False,
        ),
    )
    op.add_column(
        "opportunities",
        sa.Column(
            "lifecycle_changed_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
    )
    op.execute(
        "UPDATE opportunities SET lifecycle_changed_at = created_at"
    )
    op.create_check_constraint(
        op.f("ck_opportunities_lifecycle_status_valid"),
        "opportunities",
        f"lifecycle_status IN ({LIFECYCLE_STATUSES})",
    )
    op.create_index(
        "ix_opportunities_lifecycle_status_last_seen_at",
        "opportunities",
        ["lifecycle_status", "last_seen_at"],
    )

    op.create_table(
        "opportunity_lifecycle_events",
        sa.Column("id", sa.BigInteger(), sa.Identity(), nullable=False),
        sa.Column("opportunity_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("from_status", sa.String(length=16)),
        sa.Column("to_status", sa.String(length=16), nullable=False),
        sa.Column(
            "evidence_raw_message_id",
            postgresql.UUID(as_uuid=True),
        ),
        sa.Column("actor_kind", sa.String(length=16), nullable=False),
        sa.Column("actor_id", sa.String(length=128)),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column(
            "changed_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.CheckConstraint(
            f"from_status IS NULL OR from_status IN ({LIFECYCLE_STATUSES})",
            name=op.f("ck_opportunity_lifecycle_events_from_status_valid"),
        ),
        sa.CheckConstraint(
            f"to_status IN ({LIFECYCLE_STATUSES})",
            name=op.f("ck_opportunity_lifecycle_events_to_status_valid"),
        ),
        sa.CheckConstraint(
            "(from_status IS NULL AND to_status = 'active') "
            "OR (from_status IS NOT NULL AND from_status <> to_status)",
            name=op.f("ck_opportunity_lifecycle_events_status_changed"),
        ),
        sa.CheckConstraint(
            "actor_kind IN ('migration', 'system', 'operator')",
            name=op.f("ck_opportunity_lifecycle_events_actor_kind_valid"),
        ),
        sa.CheckConstraint(
            "(actor_kind = 'operator' AND actor_id IS NOT NULL) "
            "OR (actor_kind <> 'operator' AND actor_id IS NULL)",
            name=op.f(
                "ck_opportunity_lifecycle_events_actor_identity_consistent"
            ),
        ),
        sa.CheckConstraint(
            "actor_id IS NULL OR "
            "(actor_id = btrim(actor_id) AND actor_id <> '')",
            name=op.f("ck_opportunity_lifecycle_events_actor_id_valid"),
        ),
        sa.CheckConstraint(
            "reason = btrim(reason) AND reason <> ''",
            name=op.f("ck_opportunity_lifecycle_events_reason_nonempty"),
        ),
        sa.ForeignKeyConstraint(
            ["evidence_raw_message_id"],
            ["raw_messages.id"],
            ondelete="RESTRICT",
            name=op.f(
                "fk_opportunity_lifecycle_events_evidence_raw_message_id_raw_messages"
            ),
        ),
        sa.ForeignKeyConstraint(
            ["opportunity_id"],
            ["opportunities.id"],
            ondelete="RESTRICT",
            name=op.f(
                "fk_opportunity_lifecycle_events_opportunity_id_opportunities"
            ),
        ),
        sa.PrimaryKeyConstraint(
            "id",
            name=op.f("pk_opportunity_lifecycle_events"),
        ),
    )
    op.create_index(
        "ix_opportunity_lifecycle_events_opportunity_changed_at",
        "opportunity_lifecycle_events",
        ["opportunity_id", "changed_at", "id"],
    )
    op.execute(
        sa.text(
            "INSERT INTO opportunity_lifecycle_events "
            "(opportunity_id, from_status, to_status, actor_kind, reason, changed_at) "
            "SELECT id, NULL, 'active', 'migration', "
            ":reason, lifecycle_changed_at FROM opportunities"
        ).bindparams(reason="G5 lifecycle history backfill")
    )


def downgrade() -> None:
    op.drop_index(
        "ix_opportunity_lifecycle_events_opportunity_changed_at",
        table_name="opportunity_lifecycle_events",
    )
    op.drop_table("opportunity_lifecycle_events")
    op.drop_index(
        "ix_opportunities_lifecycle_status_last_seen_at",
        table_name="opportunities",
    )
    op.drop_constraint(
        op.f("ck_opportunities_lifecycle_status_valid"),
        "opportunities",
        type_="check",
    )
    op.drop_column("opportunities", "lifecycle_changed_at")
    op.drop_column("opportunities", "lifecycle_status")
