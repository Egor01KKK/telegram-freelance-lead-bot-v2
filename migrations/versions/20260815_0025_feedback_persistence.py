"""Persist traceable feedback and derived source feedback signals.

Revision ID: 20260815_0025
Revises: 20260815_0024
Create Date: 2026-08-15
"""
from typing import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID


revision: str = "20260815_0025"
down_revision: str | None = "20260815_0024"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "feedback_events",
        sa.Column("id", UUID(as_uuid=True), nullable=False),
        sa.Column("schema_version", sa.String(length=64), nullable=False),
        sa.Column("delivery_action_event_id", UUID(as_uuid=True), nullable=False),
        sa.Column("feedback_type", sa.String(length=24), nullable=False),
        sa.Column("signal_scope", sa.String(length=32), nullable=False),
        sa.Column("delivery_id", UUID(as_uuid=True), nullable=False),
        sa.Column("match_trace_id", UUID(as_uuid=True), nullable=False),
        sa.Column("match_run_id", UUID(as_uuid=True), nullable=False),
        sa.Column("opportunity_id", UUID(as_uuid=True), nullable=False),
        sa.Column("opportunity_type", sa.String(length=32), nullable=False),
        sa.Column("search_profile_id", UUID(as_uuid=True), nullable=False),
        sa.Column("profile_revision", sa.Integer(), nullable=False),
        sa.Column("user_id", UUID(as_uuid=True), nullable=False),
        sa.Column("source_id", sa.BigInteger(), nullable=False),
        sa.Column("source_raw_message_id", UUID(as_uuid=True), nullable=False),
        sa.Column("source_url", sa.Text(), nullable=False),
        sa.Column("match_score", sa.Numeric(6, 5), nullable=False),
        sa.Column("match_score_version", sa.String(length=64), nullable=False),
        sa.Column("match_policy_version", sa.String(length=64), nullable=False),
        sa.Column("feedback_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "feedback_type IN ('not_suitable', 'got_job')",
            name=op.f("ck_feedback_events_feedback_type_valid"),
        ),
        sa.CheckConstraint(
            "schema_version ~ '^[a-z0-9][a-z0-9._-]{0,63}$'",
            name=op.f("ck_feedback_events_schema_version_valid"),
        ),
        sa.CheckConstraint(
            "(feedback_type = 'not_suitable' AND signal_scope = 'personal_match') "
            "OR (feedback_type = 'got_job' AND signal_scope = 'conversion')",
            name=op.f("ck_feedback_events_signal_scope_consistent"),
        ),
        sa.CheckConstraint(
            "opportunity_type IN ('one_off_order', 'project', 'vacancy', "
            "'part_time_contractor', 'consultation', 'unknown')",
            name=op.f("ck_feedback_events_opportunity_type_valid"),
        ),
        sa.CheckConstraint(
            "profile_revision >= 1",
            name=op.f("ck_feedback_events_profile_revision_valid"),
        ),
        sa.CheckConstraint(
            "match_score BETWEEN 0 AND 1",
            name=op.f("ck_feedback_events_match_score_valid"),
        ),
        sa.CheckConstraint(
            "match_score_version ~ '^[a-z0-9][a-z0-9._-]{0,63}$' "
            "AND match_policy_version ~ '^[a-z0-9][a-z0-9._-]{0,63}$'",
            name=op.f("ck_feedback_events_versions_valid"),
        ),
        sa.CheckConstraint(
            "length(source_url) BETWEEN 1 AND 2048",
            name=op.f("ck_feedback_events_source_url_bounded"),
        ),
        sa.ForeignKeyConstraint(
            ["delivery_action_event_id"],
            ["delivery_action_events.id"],
            name=op.f(
                "fk_feedback_events_delivery_action_event_id_delivery_action_events"
            ),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["delivery_id"],
            ["personalized_deliveries.id"],
            name=op.f("fk_feedback_events_delivery_id_personalized_deliveries"),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["match_trace_id"],
            ["match_traces.id"],
            name=op.f("fk_feedback_events_match_trace_id_match_traces"),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["match_run_id"],
            ["match_evaluation_runs.id"],
            name=op.f("fk_feedback_events_match_run_id_match_evaluation_runs"),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["opportunity_id"],
            ["opportunities.id"],
            name=op.f("fk_feedback_events_opportunity_id_opportunities"),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["search_profile_id"],
            ["search_profiles.id"],
            name=op.f("fk_feedback_events_search_profile_id_search_profiles"),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["source_id"],
            ["sources.id"],
            name=op.f("fk_feedback_events_source_id_sources"),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["source_raw_message_id"],
            ["raw_messages.id"],
            name=op.f("fk_feedback_events_source_raw_message_id_raw_messages"),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["users.id"],
            name=op.f("fk_feedback_events_user_id_users"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_feedback_events")),
        sa.UniqueConstraint(
            "delivery_action_event_id",
            name="uq_feedback_events_delivery_action_event_id",
        ),
    )
    op.create_index(
        "ix_feedback_events_source_feedback_at",
        "feedback_events",
        ["source_id", "feedback_at", "id"],
        unique=False,
    )
    op.create_index(
        "ix_feedback_events_profile_feedback_at",
        "feedback_events",
        ["search_profile_id", "feedback_at", "id"],
        unique=False,
    )
    op.create_index(
        "ix_feedback_events_opportunity_type_feedback_at",
        "feedback_events",
        ["opportunity_type", "feedback_at", "id"],
        unique=False,
    )

    op.create_table(
        "source_feedback_signals",
        sa.Column("source_id", sa.BigInteger(), nullable=False),
        sa.Column("signal_version", sa.String(length=64), nullable=False),
        sa.Column("feedback_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column(
            "not_suitable_count",
            sa.Integer(),
            nullable=False,
            server_default="0",
        ),
        sa.Column("got_job_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("last_feedback_at", sa.DateTime(timezone=True)),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "signal_version ~ '^[a-z0-9][a-z0-9._-]{0,63}$'",
            name=op.f("ck_source_feedback_signals_signal_version_valid"),
        ),
        sa.CheckConstraint(
            "feedback_count >= 0 AND not_suitable_count >= 0 "
            "AND got_job_count >= 0",
            name=op.f("ck_source_feedback_signals_counts_nonnegative"),
        ),
        sa.CheckConstraint(
            "feedback_count = not_suitable_count + got_job_count",
            name=op.f("ck_source_feedback_signals_counts_consistent"),
        ),
        sa.CheckConstraint(
            "(feedback_count = 0 AND last_feedback_at IS NULL) "
            "OR (feedback_count > 0 AND last_feedback_at IS NOT NULL)",
            name=op.f("ck_source_feedback_signals_timestamp_consistent"),
        ),
        sa.ForeignKeyConstraint(
            ["source_id"],
            ["sources.id"],
            name=op.f("fk_source_feedback_signals_source_id_sources"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("source_id", name=op.f("pk_source_feedback_signals")),
    )

    # Existing action rows are immutable evidence. This only creates the new
    # derived projection for feedback actions that predate this migration.
    op.execute(
        sa.text(
            "INSERT INTO feedback_events ("
            "id, schema_version, delivery_action_event_id, feedback_type, "
            "signal_scope, "
            "delivery_id, match_trace_id, match_run_id, opportunity_id, "
            "opportunity_type, search_profile_id, profile_revision, user_id, "
            "source_id, source_raw_message_id, source_url, match_score, "
            "match_score_version, match_policy_version, feedback_at"
            ") "
            "SELECT md5('feedback:' || events.id::text)::uuid, "
            "'feedback.v1', events.id, "
            "events.action_type, "
            "CASE WHEN events.action_type = 'not_suitable' "
            "THEN 'personal_match' ELSE 'conversion' END, "
            "events.delivery_id, events.match_trace_id, events.match_run_id, "
            "events.opportunity_id, opportunities.opportunity_type, "
            "events.search_profile_id, events.profile_revision, events.user_id, "
            "events.source_id, events.source_raw_message_id, events.source_url, "
            "traces.final_rank_score, traces.decision_algorithm_version, "
            "runs.policy_version, events.created_at "
            "FROM delivery_action_events AS events "
            "JOIN match_traces AS traces ON traces.id = events.match_trace_id "
            "JOIN match_evaluation_runs AS runs ON runs.id = events.match_run_id "
            "JOIN opportunities ON opportunities.id = events.opportunity_id "
            "WHERE events.action_type IN ('not_suitable', 'got_job')"
        )
    )
    op.execute(
        sa.text(
            "INSERT INTO source_feedback_signals ("
            "source_id, signal_version, feedback_count, not_suitable_count, "
            "got_job_count, last_feedback_at"
            ") "
            "SELECT source_id, 'source-feedback-signal.v1', count(*)::integer, "
            "count(*) FILTER (WHERE feedback_type = 'not_suitable')::integer, "
            "count(*) FILTER (WHERE feedback_type = 'got_job')::integer, "
            "max(feedback_at) "
            "FROM feedback_events GROUP BY source_id"
        )
    )


def downgrade() -> None:
    op.drop_table("source_feedback_signals")
    op.drop_index(
        "ix_feedback_events_opportunity_type_feedback_at",
        table_name="feedback_events",
    )
    op.drop_index(
        "ix_feedback_events_profile_feedback_at",
        table_name="feedback_events",
    )
    op.drop_index(
        "ix_feedback_events_source_feedback_at",
        table_name="feedback_events",
    )
    op.drop_table("feedback_events")
