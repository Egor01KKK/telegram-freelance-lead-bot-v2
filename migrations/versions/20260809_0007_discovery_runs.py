"""Add discovery runs, normalized results and linked source lineage.

Revision ID: 20260809_0007
Revises: 20260809_0006
Create Date: 2026-08-09
"""
from typing import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "20260809_0007"
down_revision: str | None = "20260809_0006"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "discovery_runs",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("provider", sa.String(length=64), nullable=False),
        sa.Column("provider_kind", sa.String(length=32), nullable=False),
        sa.Column("run_key", sa.String(length=255), nullable=False),
        sa.Column("request", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("result_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column(
            "materialized_count",
            sa.Integer(),
            server_default="0",
            nullable=False,
        ),
        sa.Column("failure_code", sa.String(length=64)),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True)),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.CheckConstraint(
            "result_count >= 0 AND materialized_count >= 0 "
            "AND materialized_count <= result_count",
            name=op.f("ck_discovery_runs_counts_valid"),
        ),
        sa.CheckConstraint(
            "failure_code IS NULL OR "
            "failure_code ~ '^[a-z][a-z0-9_.-]{0,63}$'",
            name=op.f("ck_discovery_runs_failure_code_safe"),
        ),
        sa.CheckConstraint(
            "finished_at IS NULL OR finished_at >= started_at",
            name=op.f("ck_discovery_runs_finished_after_started"),
        ),
        sa.CheckConstraint(
            "provider_kind = lower(provider_kind) "
            "AND provider_kind ~ '^[a-z][a-z0-9_-]{0,31}$'",
            name=op.f("ck_discovery_runs_provider_kind_valid"),
        ),
        sa.CheckConstraint(
            "provider = lower(provider) "
            "AND provider ~ '^[a-z][a-z0-9_-]{0,63}$'",
            name=op.f("ck_discovery_runs_provider_valid"),
        ),
        sa.CheckConstraint(
            "run_key = btrim(run_key) AND run_key <> ''",
            name=op.f("ck_discovery_runs_run_key_nonempty"),
        ),
        sa.CheckConstraint(
            "(status = 'running' AND finished_at IS NULL "
            "AND failure_code IS NULL) "
            "OR (status = 'completed' AND finished_at IS NOT NULL "
            "AND failure_code IS NULL AND materialized_count = result_count) "
            "OR (status = 'failed' AND finished_at IS NOT NULL "
            "AND failure_code IS NOT NULL)",
            name=op.f("ck_discovery_runs_status_fields_consistent"),
        ),
        sa.CheckConstraint(
            "status IN ('running', 'completed', 'failed')",
            name=op.f("ck_discovery_runs_status_valid"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_discovery_runs")),
        sa.UniqueConstraint(
            "provider",
            "run_key",
            name="uq_discovery_runs_provider_run_key",
        ),
    )
    op.create_index(
        "ix_discovery_runs_status_started_at",
        "discovery_runs",
        ["status", "started_at"],
        unique=False,
    )

    op.add_column(
        "source_discovery_lineage",
        sa.Column("discovery_run_id", postgresql.UUID(as_uuid=True)),
    )
    op.create_foreign_key(
        op.f("fk_source_discovery_lineage_discovery_run_id_discovery_runs"),
        "source_discovery_lineage",
        "discovery_runs",
        ["discovery_run_id"],
        ["id"],
        ondelete="RESTRICT",
    )
    op.create_index(
        "ix_source_discovery_lineage_discovery_run_id",
        "source_discovery_lineage",
        ["discovery_run_id"],
        unique=False,
    )

    op.create_table(
        "discovery_results",
        sa.Column("id", sa.BigInteger(), sa.Identity(), nullable=False),
        sa.Column("run_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("provider_result_key", sa.String(length=255), nullable=False),
        sa.Column("source_id", sa.BigInteger(), nullable=False),
        sa.Column("outcome", sa.String(length=16), nullable=False),
        sa.Column("platform", sa.String(length=32), nullable=False),
        sa.Column("external_id", sa.String(length=255), nullable=False),
        sa.Column("access_type", sa.String(length=16), nullable=False),
        sa.Column("display_name", sa.Text(), nullable=False),
        sa.Column("handle", sa.String(length=255)),
        sa.Column("canonical_url", sa.Text()),
        sa.Column("discovered_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("seed_source_id", sa.BigInteger()),
        sa.Column("seed_reference", sa.Text()),
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
            "access_type IN ('public', 'private')",
            name=op.f("ck_discovery_results_access_type_valid"),
        ),
        sa.CheckConstraint(
            "canonical_url IS NULL OR (canonical_url = btrim(canonical_url) "
            "AND canonical_url <> '')",
            name=op.f("ck_discovery_results_canonical_url_valid"),
        ),
        sa.CheckConstraint(
            "display_name = btrim(display_name) AND display_name <> ''",
            name=op.f("ck_discovery_results_display_name_nonempty"),
        ),
        sa.CheckConstraint(
            "external_id = btrim(external_id) AND external_id <> ''",
            name=op.f("ck_discovery_results_external_id_nonempty"),
        ),
        sa.CheckConstraint(
            "handle IS NULL OR (handle = lower(handle) "
            "AND handle = btrim(handle) AND handle <> '')",
            name=op.f("ck_discovery_results_handle_normalized"),
        ),
        sa.CheckConstraint(
            "outcome IN ('created', 'existing')",
            name=op.f("ck_discovery_results_outcome_valid"),
        ),
        sa.CheckConstraint(
            "platform = lower(platform) "
            "AND platform ~ '^[a-z][a-z0-9_-]{0,31}$'",
            name=op.f("ck_discovery_results_platform_valid"),
        ),
        sa.CheckConstraint(
            "provider_result_key = btrim(provider_result_key) "
            "AND provider_result_key <> ''",
            name=op.f("ck_discovery_results_provider_result_key_nonempty"),
        ),
        sa.CheckConstraint(
            "seed_reference IS NULL OR (seed_reference = btrim(seed_reference) "
            "AND seed_reference <> '')",
            name=op.f("ck_discovery_results_seed_reference_valid"),
        ),
        sa.ForeignKeyConstraint(
            ["run_id"],
            ["discovery_runs.id"],
            name=op.f("fk_discovery_results_run_id_discovery_runs"),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["seed_source_id"],
            ["sources.id"],
            name=op.f("fk_discovery_results_seed_source_id_sources"),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["source_id"],
            ["sources.id"],
            name=op.f("fk_discovery_results_source_id_sources"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_discovery_results")),
        sa.UniqueConstraint(
            "run_id",
            "provider_result_key",
            name="uq_discovery_results_run_provider_result_key",
        ),
    )
    op.create_index(
        "ix_discovery_results_source_id_run_id",
        "discovery_results",
        ["source_id", "run_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_table("discovery_results")
    op.drop_index(
        "ix_source_discovery_lineage_discovery_run_id",
        table_name="source_discovery_lineage",
    )
    op.drop_constraint(
        op.f("fk_source_discovery_lineage_discovery_run_id_discovery_runs"),
        "source_discovery_lineage",
        type_="foreignkey",
    )
    op.drop_column("source_discovery_lineage", "discovery_run_id")
    op.drop_table("discovery_runs")
