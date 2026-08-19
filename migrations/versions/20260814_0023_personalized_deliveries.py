"""Add idempotent personalized Telegram deliveries.

Revision ID: 20260814_0023
Revises: 20260814_0022
Create Date: 2026-08-14
"""
from typing import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID


revision: str = "20260814_0023"
down_revision: str | None = "20260814_0022"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "personalized_deliveries",
        sa.Column("id", UUID(as_uuid=True), nullable=False),
        sa.Column("idempotency_key", sa.String(length=64), nullable=False),
        sa.Column("schema_version", sa.String(length=64), nullable=False),
        sa.Column("renderer_schema_version", sa.String(length=64), nullable=False),
        sa.Column("match_trace_id", UUID(as_uuid=True), nullable=False),
        sa.Column("match_run_id", UUID(as_uuid=True), nullable=False),
        sa.Column("opportunity_id", UUID(as_uuid=True), nullable=False),
        sa.Column("search_profile_id", UUID(as_uuid=True), nullable=False),
        sa.Column("profile_revision", sa.Integer(), nullable=False),
        sa.Column("user_id", UUID(as_uuid=True), nullable=False),
        sa.Column("recipient_platform", sa.String(length=32), nullable=False),
        sa.Column(
            "recipient_external_user_id",
            sa.String(length=255),
            nullable=False,
        ),
        sa.Column("job_id", UUID(as_uuid=True), nullable=False),
        sa.Column(
            "status",
            sa.String(length=16),
            server_default="queued",
            nullable=False,
        ),
        sa.Column("card_body_html", sa.Text(), nullable=False),
        sa.Column("source_url", sa.Text(), nullable=True),
        sa.Column("parse_mode", sa.String(length=16), nullable=False),
        sa.Column("link_preview", sa.Boolean(), nullable=False),
        sa.Column("rendered_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "attempt_count",
            sa.Integer(),
            server_default="0",
            nullable=False,
        ),
        sa.Column("last_attempt_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("failure_code", sa.String(length=64), nullable=True),
        sa.Column("telegram_message_id", sa.BigInteger(), nullable=True),
        sa.Column("sent_at", sa.DateTime(timezone=True), nullable=True),
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
            "attempt_count >= 0",
            name=op.f("ck_personalized_deliveries_attempt_count_nonnegative"),
        ),
        sa.CheckConstraint(
            "length(card_body_html) BETWEEN 1 AND 4096",
            name=op.f("ck_personalized_deliveries_card_body_bounded"),
        ),
        sa.CheckConstraint(
            "failure_code IS NULL OR "
            "failure_code ~ '^[A-Za-z][A-Za-z0-9_.-]{0,63}$'",
            name=op.f("ck_personalized_deliveries_failure_code_valid"),
        ),
        sa.CheckConstraint(
            "idempotency_key ~ '^[0-9a-f]{64}$'",
            name=op.f("ck_personalized_deliveries_idempotency_key_sha256"),
        ),
        sa.CheckConstraint(
            "parse_mode = 'html'",
            name=op.f("ck_personalized_deliveries_parse_mode_html"),
        ),
        sa.CheckConstraint(
            "profile_revision >= 1",
            name=op.f("ck_personalized_deliveries_profile_revision_valid"),
        ),
        sa.CheckConstraint(
            "recipient_platform = 'telegram' "
            "AND recipient_external_user_id ~ '^[1-9][0-9]{0,19}$'",
            name=op.f("ck_personalized_deliveries_recipient_valid"),
        ),
        sa.CheckConstraint(
            "source_url IS NULL OR length(source_url) <= 2048",
            name=op.f("ck_personalized_deliveries_source_url_bounded"),
        ),
        sa.CheckConstraint(
            "(status = 'queued' AND sent_at IS NULL "
            "AND telegram_message_id IS NULL "
            "AND ((attempt_count = 0 AND last_attempt_at IS NULL "
            "AND failure_code IS NULL) OR (attempt_count > 0 "
            "AND last_attempt_at IS NOT NULL AND failure_code IS NOT NULL))) "
            "OR (status = 'sending' AND attempt_count > 0 "
            "AND last_attempt_at IS NOT NULL AND failure_code IS NULL "
            "AND sent_at IS NULL AND telegram_message_id IS NULL) "
            "OR (status = 'sent' AND attempt_count > 0 "
            "AND last_attempt_at IS NOT NULL AND failure_code IS NULL "
            "AND sent_at IS NOT NULL AND telegram_message_id > 0) "
            "OR (status IN ('failed', 'suppressed') AND attempt_count > 0 "
            "AND last_attempt_at IS NOT NULL AND failure_code IS NOT NULL "
            "AND sent_at IS NULL AND telegram_message_id IS NULL)",
            name=op.f("ck_personalized_deliveries_state_consistent"),
        ),
        sa.CheckConstraint(
            "schema_version ~ '^[a-z0-9][a-z0-9._-]{0,63}$' "
            "AND renderer_schema_version ~ '^[a-z0-9][a-z0-9._-]{0,63}$'",
            name=op.f("ck_personalized_deliveries_versions_valid"),
        ),
        sa.ForeignKeyConstraint(
            ["job_id"],
            ["durable_jobs.id"],
            name=op.f("fk_personalized_deliveries_job_id_durable_jobs"),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["match_run_id"],
            ["match_evaluation_runs.id"],
            name=op.f(
                "fk_personalized_deliveries_match_run_id_match_evaluation_runs"
            ),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["match_trace_id"],
            ["match_traces.id"],
            name=op.f("fk_personalized_deliveries_match_trace_id_match_traces"),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["opportunity_id"],
            ["opportunities.id"],
            name=op.f(
                "fk_personalized_deliveries_opportunity_id_opportunities"
            ),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["search_profile_id"],
            ["search_profiles.id"],
            name=op.f(
                "fk_personalized_deliveries_search_profile_id_search_profiles"
            ),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["users.id"],
            name=op.f("fk_personalized_deliveries_user_id_users"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_personalized_deliveries")),
        sa.UniqueConstraint(
            "idempotency_key",
            name="uq_personalized_deliveries_idempotency_key",
        ),
        sa.UniqueConstraint(
            "job_id",
            name="uq_personalized_deliveries_job_id",
        ),
        sa.UniqueConstraint(
            "match_trace_id",
            "renderer_schema_version",
            name="uq_personalized_deliveries_trace_renderer",
        ),
    )
    op.create_index(
        "ix_personalized_deliveries_status_created",
        "personalized_deliveries",
        ["status", "created_at", "id"],
        unique=False,
    )
    op.create_index(
        "ix_personalized_deliveries_user_opportunity",
        "personalized_deliveries",
        ["user_id", "opportunity_id", "search_profile_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_personalized_deliveries_user_opportunity",
        table_name="personalized_deliveries",
    )
    op.drop_index(
        "ix_personalized_deliveries_status_created",
        table_name="personalized_deliveries",
    )
    op.drop_table("personalized_deliveries")
