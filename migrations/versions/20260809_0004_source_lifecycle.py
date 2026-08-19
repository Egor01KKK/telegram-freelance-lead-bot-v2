"""Add source lifecycle history and discovery lineage.

Revision ID: 20260809_0004
Revises: 20260809_0003
Create Date: 2026-08-09
"""
from typing import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "20260809_0004"
down_revision: str | None = "20260809_0003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


SOURCE_STATUSES = "'candidate', 'approved', 'paused', 'rejected', 'needs_review'"


def upgrade() -> None:
    op.drop_constraint(
        op.f("ck_sources_lifecycle_status_valid"),
        "sources",
        type_="check",
    )
    op.create_check_constraint(
        op.f("ck_sources_lifecycle_status_valid"),
        "sources",
        f"lifecycle_status IN ({SOURCE_STATUSES})",
    )

    op.create_table(
        "source_discovery_lineage",
        sa.Column("id", sa.BigInteger(), sa.Identity(), nullable=False),
        sa.Column("source_id", sa.BigInteger(), nullable=False),
        sa.Column("provider", sa.String(length=64), nullable=False),
        sa.Column("lineage_key", sa.String(length=255), nullable=False),
        sa.Column("provider_run_id", sa.String(length=255), nullable=True),
        sa.Column("seed_source_id", sa.BigInteger(), nullable=True),
        sa.Column("seed_reference", sa.Text(), nullable=True),
        sa.Column(
            "discovered_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "context",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.CheckConstraint(
            "lineage_key = btrim(lineage_key) AND lineage_key <> ''",
            name=op.f("ck_source_discovery_lineage_lineage_key_nonempty"),
        ),
        sa.CheckConstraint(
            "provider = lower(provider) "
            "AND provider ~ '^[a-z][a-z0-9_-]{0,63}$'",
            name=op.f("ck_source_discovery_lineage_provider_valid"),
        ),
        sa.CheckConstraint(
            "provider_run_id IS NULL OR "
            "(provider_run_id = btrim(provider_run_id) AND provider_run_id <> '')",
            name=op.f("ck_source_discovery_lineage_provider_run_id_valid"),
        ),
        sa.CheckConstraint(
            "seed_reference IS NULL OR "
            "(seed_reference = btrim(seed_reference) AND seed_reference <> '')",
            name=op.f("ck_source_discovery_lineage_seed_reference_valid"),
        ),
        sa.CheckConstraint(
            "seed_source_id IS NULL OR seed_source_id <> source_id",
            name=op.f("ck_source_discovery_lineage_seed_source_not_self"),
        ),
        sa.ForeignKeyConstraint(
            ["seed_source_id"],
            ["sources.id"],
            name=op.f("fk_source_discovery_lineage_seed_source_id_sources"),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["source_id"],
            ["sources.id"],
            name=op.f("fk_source_discovery_lineage_source_id_sources"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_source_discovery_lineage")),
        sa.UniqueConstraint(
            "source_id",
            "provider",
            "lineage_key",
            name="uq_source_discovery_lineage_source_provider_key",
        ),
    )
    op.create_index(
        "ix_source_discovery_lineage_provider_run_id",
        "source_discovery_lineage",
        ["provider", "provider_run_id"],
        unique=False,
    )
    op.create_index(
        "ix_source_discovery_lineage_source_discovered_at",
        "source_discovery_lineage",
        ["source_id", "discovered_at"],
        unique=False,
    )

    op.create_table(
        "source_lifecycle_events",
        sa.Column("id", sa.BigInteger(), sa.Identity(), nullable=False),
        sa.Column("source_id", sa.BigInteger(), nullable=False),
        sa.Column("from_status", sa.String(length=20), nullable=True),
        sa.Column("to_status", sa.String(length=20), nullable=False),
        sa.Column("actor_kind", sa.String(length=16), nullable=False),
        sa.Column("actor_id", sa.String(length=128), nullable=True),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column(
            "is_override",
            sa.Boolean(),
            server_default=sa.false(),
            nullable=False,
        ),
        sa.Column(
            "changed_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.CheckConstraint(
            "actor_id IS NULL OR (actor_id = btrim(actor_id) AND actor_id <> '')",
            name=op.f("ck_source_lifecycle_events_actor_id_valid"),
        ),
        sa.CheckConstraint(
            "actor_kind IN ('seed', 'system', 'operator')",
            name=op.f("ck_source_lifecycle_events_actor_kind_valid"),
        ),
        sa.CheckConstraint(
            "from_status IS NULL OR from_status IN "
            f"({SOURCE_STATUSES})",
            name=op.f("ck_source_lifecycle_events_from_status_valid"),
        ),
        sa.CheckConstraint(
            "actor_kind <> 'operator' OR actor_id IS NOT NULL",
            name=op.f("ck_source_lifecycle_events_operator_actor_present"),
        ),
        sa.CheckConstraint(
            "NOT is_override OR actor_kind = 'operator'",
            name=op.f("ck_source_lifecycle_events_override_is_operator"),
        ),
        sa.CheckConstraint(
            "reason = btrim(reason) AND reason <> ''",
            name=op.f("ck_source_lifecycle_events_reason_nonempty"),
        ),
        sa.CheckConstraint(
            "from_status IS NULL OR from_status <> to_status",
            name=op.f("ck_source_lifecycle_events_status_changed"),
        ),
        sa.CheckConstraint(
            f"to_status IN ({SOURCE_STATUSES})",
            name=op.f("ck_source_lifecycle_events_to_status_valid"),
        ),
        sa.ForeignKeyConstraint(
            ["source_id"],
            ["sources.id"],
            name=op.f("fk_source_lifecycle_events_source_id_sources"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_source_lifecycle_events")),
    )
    op.create_index(
        "ix_source_lifecycle_events_source_changed_at",
        "source_lifecycle_events",
        ["source_id", "changed_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_table("source_lifecycle_events")
    op.drop_table("source_discovery_lineage")

    op.execute(
        "UPDATE sources SET lifecycle_status = 'approved' "
        "WHERE lifecycle_status = 'paused'"
    )
    op.drop_constraint(
        op.f("ck_sources_lifecycle_status_valid"),
        "sources",
        type_="check",
    )
    op.create_check_constraint(
        op.f("ck_sources_lifecycle_status_valid"),
        "sources",
        "lifecycle_status IN "
        "('candidate', 'approved', 'rejected', 'needs_review')",
    )
