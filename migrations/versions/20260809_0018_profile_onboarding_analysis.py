"""Add versioned onboarding-profile analysis cache and lineage.

Revision ID: 20260809_0018
Revises: 20260809_0017
Create Date: 2026-08-09
"""
from typing import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "20260809_0018"
down_revision: str | None = "20260809_0017"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "search_profile_analysis_cache",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("input_sha256", sa.String(length=64), nullable=False),
        sa.Column("normalized_input_text", sa.Text(), nullable=False),
        sa.Column("cache_version", sa.String(length=255), nullable=False),
        sa.Column("schema_version", sa.String(length=64), nullable=False),
        sa.Column("provider", sa.String(length=64), nullable=False),
        sa.Column("requested_model", sa.String(length=128), nullable=False),
        sa.Column("response_model", sa.String(length=128), nullable=False),
        sa.Column("analyzer_version", sa.String(length=64), nullable=False),
        sa.Column("prompt_version", sa.String(length=100), nullable=False),
        sa.Column("attempt_count", sa.Integer(), nullable=False),
        sa.Column("input_tokens", sa.Integer(), nullable=False),
        sa.Column("output_tokens", sa.Integer(), nullable=False),
        sa.Column("total_tokens", sa.Integer(), nullable=False),
        sa.Column(
            "result",
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
            "length(input_sha256) = 64",
            name=op.f("ck_search_profile_analysis_cache_input_sha256_length"),
        ),
        sa.CheckConstraint(
            "normalized_input_text = btrim(normalized_input_text) "
            "AND normalized_input_text <> '' "
            "AND length(normalized_input_text) <= 10000",
            name=op.f(
                "ck_search_profile_analysis_cache_normalized_input_text_valid"
            ),
        ),
        sa.CheckConstraint(
            "cache_version = btrim(cache_version) AND cache_version <> ''",
            name=op.f("ck_search_profile_analysis_cache_cache_version_valid"),
        ),
        sa.CheckConstraint(
            "schema_version ~ '^[a-z0-9][a-z0-9._-]{0,63}$'",
            name=op.f("ck_search_profile_analysis_cache_schema_version_valid"),
        ),
        sa.CheckConstraint(
            "provider ~ '^[a-z][a-z0-9_-]{0,63}$'",
            name=op.f("ck_search_profile_analysis_cache_provider_valid"),
        ),
        sa.CheckConstraint(
            "requested_model = btrim(requested_model) AND requested_model <> ''",
            name=op.f("ck_search_profile_analysis_cache_requested_model_valid"),
        ),
        sa.CheckConstraint(
            "response_model = btrim(response_model) AND response_model <> ''",
            name=op.f("ck_search_profile_analysis_cache_response_model_valid"),
        ),
        sa.CheckConstraint(
            "analyzer_version ~ '^[a-z0-9][a-z0-9._-]{0,63}$'",
            name=op.f("ck_search_profile_analysis_cache_analyzer_version_valid"),
        ),
        sa.CheckConstraint(
            "prompt_version ~ '^[a-z0-9][a-z0-9._-]{0,99}$'",
            name=op.f("ck_search_profile_analysis_cache_prompt_version_valid"),
        ),
        sa.CheckConstraint(
            "attempt_count BETWEEN 1 AND 5",
            name=op.f("ck_search_profile_analysis_cache_attempt_count_bounded"),
        ),
        sa.CheckConstraint(
            "input_tokens >= 0 AND output_tokens >= 0 AND total_tokens >= 0 "
            "AND total_tokens = input_tokens + output_tokens",
            name=op.f("ck_search_profile_analysis_cache_token_usage_valid"),
        ),
        sa.CheckConstraint(
            "jsonb_typeof(result) = 'object'",
            name=op.f("ck_search_profile_analysis_cache_result_object"),
        ),
        sa.PrimaryKeyConstraint(
            "id",
            name=op.f("pk_search_profile_analysis_cache"),
        ),
        sa.UniqueConstraint(
            "input_sha256",
            "cache_version",
            name="uq_search_profile_analysis_cache_input_version",
        ),
    )
    op.add_column(
        "search_profiles",
        sa.Column(
            "analysis_cache_id",
            postgresql.UUID(as_uuid=True),
            nullable=True,
        ),
    )
    op.create_foreign_key(
        op.f("fk_search_profiles_analysis_cache_id_search_profile_analysis_cache"),
        "search_profiles",
        "search_profile_analysis_cache",
        ["analysis_cache_id"],
        ["id"],
        ondelete="RESTRICT",
    )
    op.create_index(
        "ix_search_profiles_analysis_cache_id",
        "search_profiles",
        ["analysis_cache_id"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_search_profiles_analysis_cache_id",
        table_name="search_profiles",
    )
    op.drop_constraint(
        op.f("fk_search_profiles_analysis_cache_id_search_profile_analysis_cache"),
        "search_profiles",
        type_="foreignkey",
    )
    op.drop_column("search_profiles", "analysis_cache_id")
    op.drop_table("search_profile_analysis_cache")
