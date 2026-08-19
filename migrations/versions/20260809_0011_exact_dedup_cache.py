"""Add exact message deduplication and versioned analysis cache.

Revision ID: 20260809_0011
Revises: 20260809_0010
Create Date: 2026-08-09
"""
import unicodedata
from hashlib import sha256
from typing import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "20260809_0011"
down_revision: str | None = "20260809_0010"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

ANALYZER_VERSION = "opportunity-analyzer.v1"
ANALYSIS_SCHEMA_VERSION = "opportunity-analysis.v1"
DEDUP_WINDOW_SECONDS = 7 * 24 * 60 * 60


def upgrade() -> None:
    op.add_column(
        "message_prefilter_results",
        sa.Column("canonical_prefilter_result_id", postgresql.UUID(as_uuid=True)),
    )
    op.add_column(
        "message_prefilter_results", sa.Column("normalized_content", sa.Text())
    )
    op.add_column(
        "message_prefilter_results",
        sa.Column("normalized_content_sha256", sa.String(length=64)),
    )
    op.add_column(
        "message_prefilter_results",
        sa.Column("analysis_input_sha256", sa.String(length=64)),
    )
    op.add_column(
        "message_prefilter_results",
        sa.Column("analyzer_version", sa.String(length=64)),
    )
    op.add_column(
        "message_prefilter_results",
        sa.Column("analysis_schema_version", sa.String(length=32)),
    )
    op.add_column(
        "message_prefilter_results",
        sa.Column("dedup_relation", sa.String(length=16)),
    )
    op.add_column(
        "message_prefilter_results", sa.Column("dedup_window_seconds", sa.Integer())
    )
    op.create_foreign_key(
        "fk_prefilter_result_canonical",
        "message_prefilter_results",
        "message_prefilter_results",
        ["canonical_prefilter_result_id"],
        ["id"],
        ondelete="RESTRICT",
    )

    bind = op.get_bind()
    rows = bind.execute(
        sa.text(
            "SELECT p.id, p.raw_message_id, p.parent_raw_message_id, "
            "r.content, parent.content AS parent_content "
            "FROM message_prefilter_results p "
            "JOIN raw_messages r ON r.id = p.raw_message_id "
            "LEFT JOIN raw_messages parent ON parent.id = p.parent_raw_message_id "
            "WHERE p.decision = 'passed'"
        )
    ).mappings().all()
    for row in rows:
        normalized = _normalize(row["content"])
        parent = (
            None
            if row["parent_content"] is None
            else _normalize(row["parent_content"])
        )
        bind.execute(
            sa.text(
                "UPDATE message_prefilter_results SET "
                "normalized_content = :normalized_content, "
                "normalized_content_sha256 = :content_hash, "
                "analysis_input_sha256 = :input_hash, "
                "analyzer_version = :analyzer_version, "
                "analysis_schema_version = :analysis_schema_version, "
                "dedup_relation = 'canonical', "
                "dedup_window_seconds = :window_seconds "
                "WHERE id = :id"
            ),
            {
                "id": row["id"],
                "normalized_content": normalized,
                "content_hash": _hash(normalized),
                "input_hash": _input_hash(normalized, parent),
                "analyzer_version": ANALYZER_VERSION,
                "analysis_schema_version": ANALYSIS_SCHEMA_VERSION,
                "window_seconds": DEDUP_WINDOW_SECONDS,
            },
        )

    op.drop_constraint(
        op.f("ck_message_prefilter_results_outcome_consistent"),
        "message_prefilter_results",
        type_="check",
    )
    op.drop_constraint(
        "uq_message_prefilter_results_analysis_job_id",
        "message_prefilter_results",
        type_="unique",
    )
    op.create_check_constraint(
        op.f("ck_message_prefilter_results_outcome_consistent"),
        "message_prefilter_results",
        "(decision = 'passed' AND analysis_job_id IS NOT NULL "
        "AND jsonb_array_length(reason_codes) = 0 "
        "AND normalized_content IS NOT NULL "
        "AND normalized_content_sha256 IS NOT NULL "
        "AND analysis_input_sha256 IS NOT NULL "
        "AND analyzer_version IS NOT NULL "
        "AND analysis_schema_version IS NOT NULL "
        "AND dedup_window_seconds > 0 "
        "AND ((dedup_relation = 'canonical' "
        "AND canonical_prefilter_result_id IS NULL) "
        "OR (dedup_relation = 'exact_duplicate' "
        "AND canonical_prefilter_result_id IS NOT NULL))) "
        "OR (decision = 'rejected' AND analysis_job_id IS NULL "
        "AND parent_raw_message_id IS NULL "
        "AND canonical_prefilter_result_id IS NULL "
        "AND normalized_content IS NULL "
        "AND normalized_content_sha256 IS NULL "
        "AND analysis_input_sha256 IS NULL "
        "AND analyzer_version IS NULL "
        "AND analysis_schema_version IS NULL "
        "AND dedup_relation IS NULL "
        "AND dedup_window_seconds IS NULL "
        "AND jsonb_array_length(reason_codes) > 0)",
    )
    op.create_index(
        "ix_message_prefilter_results_exact_lookup",
        "message_prefilter_results",
        [
            "normalized_content_sha256",
            "analysis_input_sha256",
            "analyzer_version",
            "analysis_schema_version",
            "created_at",
        ],
    )
    op.create_index(
        "uq_message_prefilter_results_canonical_analysis_job",
        "message_prefilter_results",
        ["analysis_job_id"],
        unique=True,
        postgresql_where=sa.text("dedup_relation = 'canonical'"),
    )

    op.create_table(
        "opportunity_analysis_cache",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("normalized_content", sa.Text(), nullable=False),
        sa.Column("normalized_content_sha256", sa.String(length=64), nullable=False),
        sa.Column("analysis_input_sha256", sa.String(length=64), nullable=False),
        sa.Column("analyzer_version", sa.String(length=64), nullable=False),
        sa.Column("analysis_schema_version", sa.String(length=32), nullable=False),
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
            "length(normalized_content_sha256) = 64 "
            "AND normalized_content_sha256 ~ '^[0-9a-f]{64}$'",
            name=op.f("ck_opportunity_analysis_cache_content_hash_valid"),
        ),
        sa.CheckConstraint(
            "length(analysis_input_sha256) = 64 "
            "AND analysis_input_sha256 ~ '^[0-9a-f]{64}$'",
            name=op.f("ck_opportunity_analysis_cache_input_hash_valid"),
        ),
        sa.CheckConstraint(
            "analyzer_version ~ '^[a-z0-9][a-z0-9_.-]{0,63}$'",
            name=op.f("ck_opportunity_analysis_cache_analyzer_version_safe"),
        ),
        sa.CheckConstraint(
            "analysis_schema_version ~ '^[a-z][a-z0-9_.-]{0,31}$'",
            name=op.f("ck_opportunity_analysis_cache_analysis_schema_version_safe"),
        ),
        sa.CheckConstraint(
            "jsonb_typeof(result) = 'object'",
            name=op.f("ck_opportunity_analysis_cache_result_object"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_opportunity_analysis_cache")),
        sa.UniqueConstraint(
            "normalized_content_sha256",
            "analysis_input_sha256",
            "analyzer_version",
            "analysis_schema_version",
            name="uq_opportunity_analysis_cache_compatible_input",
        ),
    )


def downgrade() -> None:
    op.drop_table("opportunity_analysis_cache")
    op.drop_index(
        "uq_message_prefilter_results_canonical_analysis_job",
        table_name="message_prefilter_results",
    )
    op.drop_index(
        "ix_message_prefilter_results_exact_lookup",
        table_name="message_prefilter_results",
    )
    op.drop_constraint(
        op.f("ck_message_prefilter_results_outcome_consistent"),
        "message_prefilter_results",
        type_="check",
    )
    op.create_check_constraint(
        op.f("ck_message_prefilter_results_outcome_consistent"),
        "message_prefilter_results",
        "(decision = 'passed' AND analysis_job_id IS NOT NULL "
        "AND jsonb_array_length(reason_codes) = 0) "
        "OR (decision = 'rejected' AND analysis_job_id IS NULL "
        "AND parent_raw_message_id IS NULL "
        "AND jsonb_array_length(reason_codes) > 0)",
    )
    op.create_unique_constraint(
        "uq_message_prefilter_results_analysis_job_id",
        "message_prefilter_results",
        ["analysis_job_id"],
    )
    op.drop_constraint(
        "fk_prefilter_result_canonical",
        "message_prefilter_results",
        type_="foreignkey",
    )
    for column in (
        "dedup_window_seconds",
        "dedup_relation",
        "analysis_schema_version",
        "analyzer_version",
        "analysis_input_sha256",
        "normalized_content_sha256",
        "normalized_content",
        "canonical_prefilter_result_id",
    ):
        op.drop_column("message_prefilter_results", column)


def _normalize(content: str) -> str:
    return " ".join(unicodedata.normalize("NFKC", content).casefold().split())


def _hash(value: str) -> str:
    return sha256(value.encode("utf-8")).hexdigest()


def _input_hash(current: str, parent: str | None) -> str:
    parent_value = "<none>" if parent is None else parent
    return _hash(f"current\0{current}\0parent\0{parent_value}")
