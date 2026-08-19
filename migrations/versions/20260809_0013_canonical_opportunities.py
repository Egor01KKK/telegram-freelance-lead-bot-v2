"""Add canonical opportunity persistence and base observation timestamps.

Revision ID: 20260809_0013
Revises: 20260809_0012
Create Date: 2026-08-09
"""
from typing import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "20260809_0013"
down_revision: str | None = "20260809_0012"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "opportunities",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("schema_version", sa.String(length=32), nullable=False),
        sa.Column("canonical_title", sa.Text()),
        sa.Column("task_summary", sa.Text()),
        sa.Column("market_direction", sa.String(length=32), nullable=False),
        sa.Column("intent_stage", sa.String(length=24), nullable=False),
        sa.Column("opportunity_type", sa.String(length=32), nullable=False),
        sa.Column("category", sa.Text()),
        sa.Column("role_title", sa.Text()),
        sa.Column("skills", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("budget_known", sa.Boolean(), nullable=False),
        sa.Column("budget_min", sa.Numeric(precision=18, scale=4)),
        sa.Column("budget_max", sa.Numeric(precision=18, scale=4)),
        sa.Column("budget_currency", sa.String(length=32)),
        sa.Column("budget_period", sa.String(length=32)),
        sa.Column("budget_explicit", sa.Boolean(), nullable=False),
        sa.Column("work_remote", sa.Boolean()),
        sa.Column("work_location", sa.Text()),
        sa.Column("work_full_time", sa.Boolean()),
        sa.Column("work_part_time", sa.Boolean()),
        sa.Column("language", sa.String(length=64)),
        sa.Column("contact_telegram", sa.Text()),
        sa.Column("contact_email", sa.Text()),
        sa.Column("contact_url", sa.Text()),
        sa.Column(
            "analysis_confidence", sa.Numeric(precision=5, scale=4), nullable=False
        ),
        sa.Column(
            "quality_actionability", sa.Numeric(precision=5, scale=4), nullable=False
        ),
        sa.Column(
            "quality_commercial_plausibility",
            sa.Numeric(precision=5, scale=4),
            nullable=False,
        ),
        sa.Column(
            "quality_specificity", sa.Numeric(precision=5, scale=4), nullable=False
        ),
        sa.Column(
            "quality_credibility", sa.Numeric(precision=5, scale=4), nullable=False
        ),
        sa.Column("red_flags", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False),
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
        sa.CheckConstraint(
            "schema_version ~ '^[a-z][a-z0-9_.-]{0,31}$'",
            name=op.f("ck_opportunities_schema_version_safe"),
        ),
        sa.CheckConstraint(
            "canonical_title IS NULL OR (canonical_title = btrim(canonical_title) AND canonical_title <> '' AND length(canonical_title) <= 240)",
            name=op.f("ck_opportunities_canonical_title_valid"),
        ),
        sa.CheckConstraint(
            "task_summary IS NULL OR (task_summary = btrim(task_summary) AND task_summary <> '' AND length(task_summary) <= 2000)",
            name=op.f("ck_opportunities_task_summary_valid"),
        ),
        sa.CheckConstraint(
            "market_direction = 'buyer_to_specialist'",
            name=op.f("ck_opportunities_market_direction_valid"),
        ),
        sa.CheckConstraint(
            "intent_stage IN ('active', 'recommendation', 'research', 'weak')",
            name=op.f("ck_opportunities_intent_stage_valid"),
        ),
        sa.CheckConstraint(
            "opportunity_type IN ('one_off_order', 'project', 'vacancy', 'part_time_contractor', 'consultation', 'unknown')",
            name=op.f("ck_opportunities_opportunity_type_valid"),
        ),
        sa.CheckConstraint(
            "jsonb_typeof(skills) = 'array'", name=op.f("ck_opportunities_skills_array")
        ),
        sa.CheckConstraint(
            "(budget_known AND budget_explicit AND (budget_min IS NOT NULL OR budget_max IS NOT NULL)) OR (NOT budget_known AND budget_min IS NULL AND budget_max IS NULL AND budget_currency IS NULL AND budget_period IS NULL)",
            name=op.f("ck_opportunities_budget_known_consistent"),
        ),
        sa.CheckConstraint(
            "(budget_min IS NULL OR budget_min >= 0) AND (budget_max IS NULL OR budget_max >= 0) AND (budget_min IS NULL OR budget_max IS NULL OR budget_min <= budget_max)",
            name=op.f("ck_opportunities_budget_amounts_valid"),
        ),
        sa.CheckConstraint(
            "analysis_confidence BETWEEN 0 AND 1 AND quality_actionability BETWEEN 0 AND 1 AND quality_commercial_plausibility BETWEEN 0 AND 1 AND quality_specificity BETWEEN 0 AND 1 AND quality_credibility BETWEEN 0 AND 1",
            name=op.f("ck_opportunities_analysis_quality_bounded"),
        ),
        sa.CheckConstraint(
            "jsonb_typeof(red_flags) = 'array'",
            name=op.f("ck_opportunities_red_flags_array"),
        ),
        sa.CheckConstraint(
            "first_seen_at <= last_seen_at",
            name=op.f("ck_opportunities_seen_window_valid"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_opportunities")),
    )
    op.create_index("ix_opportunities_last_seen_at", "opportunities", ["last_seen_at"])
    op.create_table(
        "opportunity_analysis_links",
        sa.Column("analysis_cache_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("opportunity_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "linked_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["analysis_cache_id"],
            ["opportunity_analysis_cache.id"],
            ondelete="RESTRICT",
            name=op.f(
                "fk_opportunity_analysis_links_analysis_cache_id_opportunity_analysis_cache"
            ),
        ),
        sa.ForeignKeyConstraint(
            ["opportunity_id"],
            ["opportunities.id"],
            ondelete="RESTRICT",
            name=op.f("fk_opportunity_analysis_links_opportunity_id_opportunities"),
        ),
        sa.PrimaryKeyConstraint(
            "analysis_cache_id", name=op.f("pk_opportunity_analysis_links")
        ),
    )
    op.create_index(
        "ix_opportunity_analysis_links_opportunity_id",
        "opportunity_analysis_links",
        ["opportunity_id"],
    )
    op.create_table(
        "opportunity_source_messages",
        sa.Column("raw_message_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("opportunity_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "linked_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["opportunity_id"],
            ["opportunities.id"],
            ondelete="RESTRICT",
            name=op.f("fk_opportunity_source_messages_opportunity_id_opportunities"),
        ),
        sa.ForeignKeyConstraint(
            ["raw_message_id"],
            ["raw_messages.id"],
            ondelete="RESTRICT",
            name=op.f("fk_opportunity_source_messages_raw_message_id_raw_messages"),
        ),
        sa.PrimaryKeyConstraint(
            "raw_message_id", name=op.f("pk_opportunity_source_messages")
        ),
    )
    op.create_index(
        "ix_opportunity_source_messages_opportunity_id",
        "opportunity_source_messages",
        ["opportunity_id"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_opportunity_source_messages_opportunity_id",
        table_name="opportunity_source_messages",
    )
    op.drop_table("opportunity_source_messages")
    op.drop_index(
        "ix_opportunity_analysis_links_opportunity_id",
        table_name="opportunity_analysis_links",
    )
    op.drop_table("opportunity_analysis_links")
    op.drop_index("ix_opportunities_last_seen_at", table_name="opportunities")
    op.drop_table("opportunities")
