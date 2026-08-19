"""Add source quality snapshots and current health metrics.

Revision ID: 20260809_0006
Revises: 20260809_0005
Create Date: 2026-08-09
"""
from typing import Sequence

from alembic import op
import sqlalchemy as sa


revision: str = "20260809_0006"
down_revision: str | None = "20260809_0005"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "source_quality_snapshots",
        sa.Column("id", sa.BigInteger(), sa.Identity(), nullable=False),
        sa.Column("source_id", sa.BigInteger(), nullable=False),
        sa.Column("audit_key", sa.String(length=255), nullable=False),
        sa.Column("audited_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("window_started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("window_ended_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("sampled_message_count", sa.Integer(), nullable=False),
        sa.Column(
            "opportunity_yield",
            sa.Numeric(precision=8, scale=7),
            nullable=False,
        ),
        sa.Column(
            "buyer_intent_ratio",
            sa.Numeric(precision=8, scale=7),
            nullable=False,
        ),
        sa.Column("seller_ratio", sa.Numeric(precision=8, scale=7), nullable=False),
        sa.Column("spam_ratio", sa.Numeric(precision=8, scale=7), nullable=False),
        sa.Column("duplicate_ratio", sa.Numeric(precision=8, scale=7), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.CheckConstraint(
            "audit_key = btrim(audit_key) AND audit_key <> ''",
            name=op.f("ck_source_quality_snapshots_audit_key_nonempty"),
        ),
        sa.CheckConstraint(
            "audited_at >= window_ended_at",
            name=op.f("ck_source_quality_snapshots_audited_after_window"),
        ),
        sa.CheckConstraint(
            "opportunity_yield BETWEEN 0 AND 1 "
            "AND buyer_intent_ratio BETWEEN 0 AND 1 "
            "AND seller_ratio BETWEEN 0 AND 1 "
            "AND spam_ratio BETWEEN 0 AND 1 "
            "AND duplicate_ratio BETWEEN 0 AND 1",
            name=op.f("ck_source_quality_snapshots_metrics_unit_interval"),
        ),
        sa.CheckConstraint(
            "sampled_message_count > 0",
            name=op.f(
                "ck_source_quality_snapshots_sampled_message_count_positive"
            ),
        ),
        sa.CheckConstraint(
            "window_ended_at > window_started_at",
            name=op.f("ck_source_quality_snapshots_window_valid"),
        ),
        sa.ForeignKeyConstraint(
            ["source_id"],
            ["sources.id"],
            name=op.f("fk_source_quality_snapshots_source_id_sources"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_source_quality_snapshots")),
        sa.UniqueConstraint(
            "source_id",
            "audit_key",
            name="uq_source_quality_snapshots_source_audit_key",
        ),
    )
    op.create_index(
        "ix_source_quality_snapshots_source_audited_at",
        "source_quality_snapshots",
        ["source_id", "audited_at"],
        unique=False,
    )

    op.create_table(
        "source_health",
        sa.Column("source_id", sa.BigInteger(), nullable=False),
        sa.Column(
            "health_status",
            sa.String(length=16),
            server_default="unknown",
            nullable=False,
        ),
        sa.Column("last_message_at", sa.DateTime(timezone=True)),
        sa.Column("last_audited_at", sa.DateTime(timezone=True)),
        sa.Column("messages_per_day", sa.Numeric(precision=14, scale=4)),
        sa.Column("opportunities_per_day", sa.Numeric(precision=14, scale=4)),
        sa.Column("activity_observed_at", sa.DateTime(timezone=True)),
        sa.Column(
            "status_changed_at",
            sa.DateTime(timezone=True),
        ),
        sa.Column("degraded_at", sa.DateTime(timezone=True)),
        sa.Column("degradation_reason", sa.Text()),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.CheckConstraint(
            "(activity_observed_at IS NULL AND last_message_at IS NULL "
            "AND messages_per_day IS NULL AND opportunities_per_day IS NULL) "
            "OR (activity_observed_at IS NOT NULL "
            "AND (last_message_at IS NULL OR last_message_at <= activity_observed_at))",
            name=op.f("ck_source_health_activity_observation_consistent"),
        ),
        sa.CheckConstraint(
            "(health_status = 'degraded' AND degraded_at IS NOT NULL "
            "AND degraded_at = status_changed_at "
            "AND degradation_reason IS NOT NULL "
            "AND degradation_reason = btrim(degradation_reason) "
            "AND degradation_reason <> '') "
            "OR (health_status <> 'degraded' AND degraded_at IS NULL "
            "AND degradation_reason IS NULL)",
            name=op.f("ck_source_health_degradation_state_consistent"),
        ),
        sa.CheckConstraint(
            "health_status IN ('unknown', 'healthy', 'degraded')",
            name=op.f("ck_source_health_health_status_valid"),
        ),
        sa.CheckConstraint(
            "messages_per_day IS NULL OR messages_per_day >= 0",
            name=op.f("ck_source_health_messages_per_day_nonnegative"),
        ),
        sa.CheckConstraint(
            "opportunities_per_day IS NULL OR opportunities_per_day >= 0",
            name=op.f("ck_source_health_opportunities_per_day_nonnegative"),
        ),
        sa.CheckConstraint(
            "(health_status = 'unknown' AND status_changed_at IS NULL) "
            "OR (health_status <> 'unknown' AND status_changed_at IS NOT NULL)",
            name=op.f("ck_source_health_status_timestamp_consistent"),
        ),
        sa.ForeignKeyConstraint(
            ["source_id"],
            ["sources.id"],
            name=op.f("fk_source_health_source_id_sources"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("source_id", name=op.f("pk_source_health")),
    )
    op.create_index(
        "ix_source_health_last_message_at",
        "source_health",
        ["last_message_at"],
        unique=False,
    )
    op.create_index(
        "ix_source_health_status_last_audited_at",
        "source_health",
        ["health_status", "last_audited_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_table("source_health")
    op.drop_table("source_quality_snapshots")
