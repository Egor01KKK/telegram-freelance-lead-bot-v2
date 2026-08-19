"""Add the generic durable job queue.

Revision ID: 20260808_0002
Revises: 20260808_0001
Create Date: 2026-08-08
"""
from typing import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "20260808_0002"
down_revision: str | None = "20260808_0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "durable_jobs",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("job_type", sa.String(length=100), nullable=False),
        sa.Column("idempotency_key", sa.String(length=255), nullable=False),
        sa.Column("state", sa.String(length=16), server_default="queued", nullable=False),
        sa.Column("attempt_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column("max_attempts", sa.Integer(), server_default="3", nullable=False),
        sa.Column(
            "available_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("claimed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("lease_owner", sa.String(length=128), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("correlation_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("failure_code", sa.String(length=64), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("failed_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "attempt_count >= 0 AND attempt_count <= max_attempts",
            name=op.f("ck_durable_jobs_attempt_count_bounded"),
        ),
        sa.CheckConstraint(
            "failure_code IS NULL OR failure_code ~ '^[A-Za-z][A-Za-z0-9_.-]{0,63}$'",
            name=op.f("ck_durable_jobs_failure_code_safe"),
        ),
        sa.CheckConstraint(
            "idempotency_key <> ''",
            name=op.f("ck_durable_jobs_idempotency_key_nonempty"),
        ),
        sa.CheckConstraint("job_type <> ''", name=op.f("ck_durable_jobs_job_type_nonempty")),
        sa.CheckConstraint("max_attempts > 0", name=op.f("ck_durable_jobs_max_attempts_positive")),
        sa.CheckConstraint(
            "(state = 'queued' AND claimed_at IS NULL AND lease_owner IS NULL "
            "AND lease_expires_at IS NULL AND completed_at IS NULL AND failed_at IS NULL) "
            "OR (state = 'running' AND attempt_count > 0 AND claimed_at IS NOT NULL "
            "AND lease_owner IS NOT NULL AND lease_owner <> '' AND lease_expires_at IS NOT NULL "
            "AND completed_at IS NULL AND failed_at IS NULL) "
            "OR (state = 'completed' AND attempt_count > 0 AND claimed_at IS NULL "
            "AND lease_owner IS NULL AND lease_expires_at IS NULL AND completed_at IS NOT NULL "
            "AND failed_at IS NULL AND failure_code IS NULL) "
            "OR (state = 'failed' AND attempt_count > 0 AND claimed_at IS NULL "
            "AND lease_owner IS NULL AND lease_expires_at IS NULL AND completed_at IS NULL "
            "AND failed_at IS NOT NULL AND failure_code IS NOT NULL)",
            name=op.f("ck_durable_jobs_state_fields_consistent"),
        ),
        sa.CheckConstraint(
            "state IN ('queued', 'running', 'completed', 'failed')",
            name=op.f("ck_durable_jobs_state_valid"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_durable_jobs")),
        sa.UniqueConstraint(
            "job_type",
            "idempotency_key",
            name="uq_durable_jobs_type_idempotency_key",
        ),
    )
    op.create_index(
        "ix_durable_jobs_claimable",
        "durable_jobs",
        ["available_at", "created_at"],
        unique=False,
        postgresql_where=sa.text("state = 'queued'"),
    )
    op.create_index(
        "ix_durable_jobs_correlation_id",
        "durable_jobs",
        ["correlation_id"],
        unique=False,
    )
    op.create_index(
        "ix_durable_jobs_expired_lease",
        "durable_jobs",
        ["lease_expires_at"],
        unique=False,
        postgresql_where=sa.text("state = 'running'"),
    )


def downgrade() -> None:
    op.drop_table("durable_jobs")
