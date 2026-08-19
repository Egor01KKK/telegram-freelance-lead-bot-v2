"""Add strict source audit results and lifecycle linkage.

Revision ID: 20260809_0008
Revises: 20260809_0007
Create Date: 2026-08-09
"""
from typing import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "20260809_0008"
down_revision: str | None = "20260809_0007"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "source_audits",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("source_id", sa.BigInteger(), nullable=False),
        sa.Column("audit_key", sa.String(length=255), nullable=False),
        sa.Column("schema_version", sa.String(length=32), nullable=False),
        sa.Column("provider", sa.String(length=64), nullable=False),
        sa.Column("model", sa.String(length=128), nullable=False),
        sa.Column("analyzer_version", sa.String(length=64), nullable=False),
        sa.Column("audited_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("window_started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("window_ended_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("sampled_from", sa.DateTime(timezone=True)),
        sa.Column("sampled_to", sa.DateTime(timezone=True)),
        sa.Column("sampled_message_count", sa.Integer(), nullable=False),
        sa.Column("probe_message_count", sa.Integer(), nullable=False),
        sa.Column("expanded", sa.Boolean(), nullable=False),
        sa.Column("high_volume", sa.Boolean(), nullable=False),
        sa.Column("sample_fingerprint", sa.String(length=64), nullable=False),
        sa.Column("commercial_opportunity_count", sa.Integer(), nullable=False),
        sa.Column("buyer_intent_count", sa.Integer(), nullable=False),
        sa.Column("seller_promotion_count", sa.Integer(), nullable=False),
        sa.Column("ads_spam_count", sa.Integer(), nullable=False),
        sa.Column("duplicate_count", sa.Integer(), nullable=False),
        sa.Column("content_mix", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("primary_language", sa.String(length=100)),
        sa.Column("languages", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("categories", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "decision_policy",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column("decision", sa.String(length=16), nullable=False),
        sa.Column("reason_codes", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("reasons", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.CheckConstraint(
            "audit_key = btrim(audit_key) AND audit_key <> ''",
            name=op.f("ck_source_audits_audit_key_nonempty"),
        ),
        sa.CheckConstraint(
            "schema_version ~ '^[a-z][a-z0-9_.-]{0,31}$'",
            name=op.f("ck_source_audits_schema_version_safe"),
        ),
        sa.CheckConstraint(
            "provider = lower(provider) AND provider ~ '^[a-z][a-z0-9_-]{0,63}$'",
            name=op.f("ck_source_audits_provider_valid"),
        ),
        sa.CheckConstraint(
            "model = btrim(model) AND model <> ''",
            name=op.f("ck_source_audits_model_nonempty"),
        ),
        sa.CheckConstraint(
            "analyzer_version ~ '^[a-z0-9][a-z0-9_.-]{0,63}$'",
            name=op.f("ck_source_audits_analyzer_version_safe"),
        ),
        sa.CheckConstraint(
            "audited_at >= window_ended_at AND window_ended_at > window_started_at",
            name=op.f("ck_source_audits_window_valid"),
        ),
        sa.CheckConstraint(
            "sampled_message_count >= 0 AND probe_message_count >= sampled_message_count",
            name=op.f("ck_source_audits_sample_counts_valid"),
        ),
        sa.CheckConstraint(
            "(sampled_message_count = 0 AND sampled_from IS NULL AND sampled_to IS NULL) "
            "OR (sampled_message_count > 0 AND sampled_from IS NOT NULL "
            "AND sampled_to IS NOT NULL AND sampled_from >= window_started_at "
            "AND sampled_to <= window_ended_at AND sampled_to >= sampled_from)",
            name=op.f("ck_source_audits_sample_range_valid"),
        ),
        sa.CheckConstraint(
            "length(sample_fingerprint) = 64 "
            "AND sample_fingerprint ~ '^[0-9a-f]{64}$'",
            name=op.f("ck_source_audits_sample_fingerprint_valid"),
        ),
        sa.CheckConstraint(
            "commercial_opportunity_count BETWEEN 0 AND sampled_message_count "
            "AND buyer_intent_count BETWEEN 0 AND sampled_message_count "
            "AND seller_promotion_count BETWEEN 0 AND sampled_message_count "
            "AND ads_spam_count BETWEEN 0 AND sampled_message_count "
            "AND duplicate_count BETWEEN 0 AND sampled_message_count",
            name=op.f("ck_source_audits_classification_counts_valid"),
        ),
        sa.CheckConstraint(
            "primary_language IS NULL OR "
            "(primary_language = lower(primary_language) "
            "AND primary_language ~ '^[a-z0-9][a-z0-9._:-]{0,99}$')",
            name=op.f("ck_source_audits_primary_language_valid"),
        ),
        sa.CheckConstraint(
            "decision IN ('approved', 'rejected', 'needs_review')",
            name=op.f("ck_source_audits_decision_valid"),
        ),
        sa.ForeignKeyConstraint(
            ["source_id"],
            ["sources.id"],
            name=op.f("fk_source_audits_source_id_sources"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_source_audits")),
        sa.UniqueConstraint(
            "source_id",
            "audit_key",
            name="uq_source_audits_source_audit_key",
        ),
    )
    op.create_index(
        "ix_source_audits_source_audited_at",
        "source_audits",
        ["source_id", "audited_at"],
        unique=False,
    )
    op.create_index(
        "ix_source_audits_decision_audited_at",
        "source_audits",
        ["decision", "audited_at"],
        unique=False,
    )

    op.add_column(
        "source_lifecycle_events",
        sa.Column("source_audit_id", postgresql.UUID(as_uuid=True)),
    )
    op.create_foreign_key(
        op.f("fk_source_lifecycle_events_source_audit_id_source_audits"),
        "source_lifecycle_events",
        "source_audits",
        ["source_audit_id"],
        ["id"],
        ondelete="RESTRICT",
    )
    op.create_check_constraint(
        op.f("ck_source_lifecycle_events_audit_is_system"),
        "source_lifecycle_events",
        "source_audit_id IS NULL OR (actor_kind = 'system' AND NOT is_override)",
    )
    op.create_index(
        "uq_source_lifecycle_events_source_audit_id",
        "source_lifecycle_events",
        ["source_audit_id"],
        unique=True,
        postgresql_where=sa.text("source_audit_id IS NOT NULL"),
    )


def downgrade() -> None:
    op.drop_index(
        "uq_source_lifecycle_events_source_audit_id",
        table_name="source_lifecycle_events",
    )
    op.drop_constraint(
        op.f("ck_source_lifecycle_events_audit_is_system"),
        "source_lifecycle_events",
        type_="check",
    )
    op.drop_constraint(
        op.f("fk_source_lifecycle_events_source_audit_id_source_audits"),
        "source_lifecycle_events",
        type_="foreignkey",
    )
    op.drop_column("source_lifecycle_events", "source_audit_id")
    op.drop_table("source_audits")
