"""Add durable high-recall message prefilter results.

Revision ID: 20260809_0010
Revises: 20260809_0009
Create Date: 2026-08-09
"""
from typing import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "20260809_0010"
down_revision: str | None = "20260809_0009"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "message_prefilter_results",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("raw_message_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "parent_raw_message_id",
            postgresql.UUID(as_uuid=True),
            nullable=True,
        ),
        sa.Column("analysis_job_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("schema_version", sa.String(length=32), nullable=False),
        sa.Column("decision", sa.String(length=16), nullable=False),
        sa.Column(
            "reason_codes",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.CheckConstraint(
            "schema_version ~ '^[a-z][a-z0-9_.-]{0,31}$'",
            name=op.f("ck_message_prefilter_results_schema_version_safe"),
        ),
        sa.CheckConstraint(
            "decision IN ('passed', 'rejected')",
            name=op.f("ck_message_prefilter_results_decision_valid"),
        ),
        sa.CheckConstraint(
            "jsonb_typeof(reason_codes) = 'array'",
            name=op.f("ck_message_prefilter_results_reason_codes_array"),
        ),
        sa.CheckConstraint(
            "(decision = 'passed' AND analysis_job_id IS NOT NULL "
            "AND jsonb_array_length(reason_codes) = 0) "
            "OR (decision = 'rejected' AND analysis_job_id IS NULL "
            "AND parent_raw_message_id IS NULL "
            "AND jsonb_array_length(reason_codes) > 0)",
            name=op.f("ck_message_prefilter_results_outcome_consistent"),
        ),
        sa.CheckConstraint(
            "parent_raw_message_id IS NULL OR "
            "parent_raw_message_id <> raw_message_id",
            name=op.f("ck_message_prefilter_results_parent_differs"),
        ),
        sa.ForeignKeyConstraint(
            ["analysis_job_id"],
            ["durable_jobs.id"],
            name=op.f(
                "fk_message_prefilter_results_analysis_job_id_durable_jobs"
            ),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["parent_raw_message_id"],
            ["raw_messages.id"],
            name=op.f(
                "fk_message_prefilter_results_parent_raw_message_id_raw_messages"
            ),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["raw_message_id"],
            ["raw_messages.id"],
            name=op.f(
                "fk_message_prefilter_results_raw_message_id_raw_messages"
            ),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_message_prefilter_results")),
        sa.UniqueConstraint(
            "analysis_job_id",
            name=op.f("uq_message_prefilter_results_analysis_job_id"),
        ),
        sa.UniqueConstraint(
            "raw_message_id",
            "schema_version",
            name="uq_message_prefilter_results_raw_schema",
        ),
    )
    op.create_index(
        "ix_message_prefilter_results_decision_created_at",
        "message_prefilter_results",
        ["decision", "created_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_table("message_prefilter_results")
