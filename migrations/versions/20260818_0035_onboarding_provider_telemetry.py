"""Persist bounded onboarding provider attempts and transport telemetry.

Revision ID: 20260818_0036
Revises: 20260817_0035
"""

from typing import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "20260818_0036"
down_revision: str | None = "20260817_0035"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    for name, default in (
        ("provider_attempts", "1"),
        ("completed_calls", "1"),
        ("timeout_count", "0"),
        ("transient_failure_count", "0"),
        ("non_retryable_failure_count", "0"),
        ("invalid_output_retry_count", "0"),
    ):
        op.add_column(
            "search_profile_analysis_cache",
            sa.Column(name, sa.Integer(), nullable=False, server_default=default),
        )

    op.create_table(
        "search_profile_onboarding_attempts",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("platform", sa.String(32), nullable=False),
        sa.Column("external_user_id", sa.String(255), nullable=False),
        sa.Column("input_sha256", sa.String(64), nullable=False),
        sa.Column("cache_version", sa.String(255), nullable=False),
        sa.Column("provider", sa.String(64), nullable=False),
        sa.Column("requested_model", sa.String(128), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("retryable", sa.Boolean(), nullable=False),
        sa.Column("provider_attempts", sa.Integer(), nullable=False),
        sa.Column("completed_calls", sa.Integer(), nullable=False),
        sa.Column("timeout_count", sa.Integer(), nullable=False),
        sa.Column("transient_failure_count", sa.Integer(), nullable=False),
        sa.Column("non_retryable_failure_count", sa.Integer(), nullable=False),
        sa.Column("invalid_output_retry_count", sa.Integer(), nullable=False),
        sa.Column("error_code", sa.String(64)),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint(
            "platform = lower(platform) AND platform ~ '^[a-z][a-z0-9_-]{0,31}$'",
            name=op.f("ck_search_profile_onboarding_attempts_platform_valid"),
        ),
        sa.CheckConstraint(
            "external_user_id = btrim(external_user_id) AND external_user_id <> ''",
            name=op.f("ck_search_profile_onboarding_attempts_external_user_id_valid"),
        ),
        sa.CheckConstraint(
            "length(input_sha256) = 64",
            name=op.f("ck_search_profile_onboarding_attempts_input_sha256_length"),
        ),
        sa.CheckConstraint(
            "cache_version = btrim(cache_version) AND cache_version <> ''",
            name=op.f("ck_search_profile_onboarding_attempts_cache_version_valid"),
        ),
        sa.CheckConstraint(
            "provider = lower(provider) AND provider ~ '^[a-z][a-z0-9_-]{0,63}$'",
            name=op.f("ck_search_profile_onboarding_attempts_provider_valid"),
        ),
        sa.CheckConstraint(
            "requested_model = btrim(requested_model) AND requested_model <> ''",
            name=op.f("ck_search_profile_onboarding_attempts_requested_model_valid"),
        ),
        sa.CheckConstraint(
            "status IN ('succeeded', 'failed')",
            name=op.f("ck_search_profile_onboarding_attempts_status_valid"),
        ),
        sa.CheckConstraint(
            "provider_attempts >= 0 AND completed_calls >= 0 AND timeout_count >= 0 "
            "AND transient_failure_count >= 0 AND non_retryable_failure_count >= 0 "
            "AND invalid_output_retry_count >= 0",
            name=op.f("ck_search_profile_onboarding_attempts_counters_nonnegative"),
        ),
        sa.CheckConstraint(
            "(status = 'succeeded' AND completed_calls > 0) OR "
            "(status = 'failed' AND completed_calls = 0)",
            name=op.f("ck_search_profile_onboarding_attempts_status_counters_consistent"),
        ),
        sa.PrimaryKeyConstraint(
            "id",
            name=op.f("pk_search_profile_onboarding_attempts"),
        ),
    )
    op.create_index(
        "ix_search_profile_onboarding_attempts_input_created",
        "search_profile_onboarding_attempts",
        ["input_sha256", "created_at"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_search_profile_onboarding_attempts_input_created",
        table_name="search_profile_onboarding_attempts",
    )
    op.drop_table("search_profile_onboarding_attempts")
    for name in (
        "invalid_output_retry_count",
        "non_retryable_failure_count",
        "transient_failure_count",
        "timeout_count",
        "completed_calls",
        "provider_attempts",
    ):
        op.drop_column("search_profile_analysis_cache", name)
