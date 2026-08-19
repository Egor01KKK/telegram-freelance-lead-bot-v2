"""Add versioned matching evaluation runs and explainable traces.

Revision ID: 20260814_0022
Revises: 20260814_0021
Create Date: 2026-08-14
"""
from typing import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB, UUID


revision: str = "20260814_0022"
down_revision: str | None = "20260814_0021"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "match_evaluation_runs",
        sa.Column("id", UUID(as_uuid=True), nullable=False),
        sa.Column("idempotency_key", sa.String(length=64), nullable=False),
        sa.Column("schema_version", sa.String(length=64), nullable=False),
        sa.Column("algorithm_version", sa.String(length=64), nullable=False),
        sa.Column("policy_version", sa.String(length=64), nullable=False),
        sa.Column("policy_config", JSONB(), nullable=False),
        sa.Column("evaluated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("trace_count", sa.Integer(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "idempotency_key ~ '^[0-9a-f]{64}$'",
            name=op.f("ck_match_evaluation_runs_idempotency_key_sha256"),
        ),
        sa.CheckConstraint(
            "jsonb_typeof(policy_config) = 'object'",
            name=op.f("ck_match_evaluation_runs_policy_config_object"),
        ),
        sa.CheckConstraint(
            "trace_count >= 0",
            name=op.f("ck_match_evaluation_runs_trace_count_nonnegative"),
        ),
        sa.CheckConstraint(
            "schema_version ~ '^[a-z0-9][a-z0-9._-]{0,63}$' "
            "AND algorithm_version ~ '^[a-z0-9][a-z0-9._-]{0,63}$' "
            "AND policy_version ~ '^[a-z0-9][a-z0-9._-]{0,63}$'",
            name=op.f("ck_match_evaluation_runs_versions_valid"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_match_evaluation_runs")),
        sa.UniqueConstraint(
            "idempotency_key",
            name="uq_match_evaluation_runs_idempotency_key",
        ),
    )
    op.create_index(
        "ix_match_evaluation_runs_evaluated_at",
        "match_evaluation_runs",
        ["evaluated_at", "id"],
        unique=False,
    )
    op.create_table(
        "match_traces",
        sa.Column("id", UUID(as_uuid=True), nullable=False),
        sa.Column("run_id", UUID(as_uuid=True), nullable=False),
        sa.Column("opportunity_id", UUID(as_uuid=True), nullable=False),
        sa.Column("search_profile_id", UUID(as_uuid=True), nullable=False),
        sa.Column("profile_revision", sa.Integer(), nullable=False),
        sa.Column("profile_schema_version", sa.String(length=64), nullable=False),
        sa.Column("preferences_schema_version", sa.String(length=64), nullable=False),
        sa.Column("input_sha256", sa.String(length=64), nullable=False),
        sa.Column("opportunity_lifecycle_status", sa.String(length=16), nullable=False),
        sa.Column("opportunity_last_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("filter_version", sa.String(length=64), nullable=False),
        sa.Column("hard_filter_eligible", sa.Boolean(), nullable=False),
        sa.Column("hard_filter_reasons", JSONB(), nullable=False),
        sa.Column("nonblocking_unknowns", JSONB(), nullable=False),
        sa.Column("structured_scoring_version", sa.String(length=64)),
        sa.Column("structured_policy_version", sa.String(length=64)),
        sa.Column("structured_components", JSONB(), nullable=False),
        sa.Column("user_relevance_score", sa.Numeric(6, 5)),
        sa.Column("structured_score", sa.Numeric(6, 5)),
        sa.Column("semantic_matching_version", sa.String(length=64)),
        sa.Column("semantic_policy_version", sa.String(length=64)),
        sa.Column("semantic_status", sa.String(length=24), nullable=False),
        sa.Column("semantic_degraded_reason", sa.String(length=64)),
        sa.Column("semantic_similarity", sa.Numeric(6, 5)),
        sa.Column("semantic_provider", sa.String(length=64)),
        sa.Column("semantic_model", sa.String(length=128)),
        sa.Column("semantic_model_version", sa.String(length=64)),
        sa.Column("opportunity_representation_sha256", sa.String(length=64)),
        sa.Column("profile_representation_sha256", sa.String(length=64)),
        sa.Column("combined_relevance_score", sa.Numeric(6, 5)),
        sa.Column("opportunity_quality_score", sa.Numeric(6, 5)),
        sa.Column("source_quality_score", sa.Numeric(6, 5)),
        sa.Column("source_quality_snapshot_id", sa.BigInteger()),
        sa.Column("red_flag_penalty", sa.Numeric(6, 5)),
        sa.Column("base_combined_score", sa.Numeric(6, 5)),
        sa.Column("freshness_age_seconds", sa.BigInteger(), nullable=False),
        sa.Column("freshness_score", sa.Numeric(6, 5), nullable=False),
        sa.Column("final_rank_score", sa.Numeric(6, 5)),
        sa.Column("minimum_relevance_threshold", sa.Numeric(6, 5), nullable=False),
        sa.Column("minimum_rank_score_threshold", sa.Numeric(6, 5), nullable=False),
        sa.Column("decision_code", sa.String(length=40), nullable=False),
        sa.Column("eligible", sa.Boolean(), nullable=False),
        sa.Column("rank", sa.Integer()),
        sa.Column("decision_schema_version", sa.String(length=64), nullable=False),
        sa.Column("decision_algorithm_version", sa.String(length=64), nullable=False),
        sa.Column("decision_policy_version", sa.String(length=64), nullable=False),
        sa.Column("evaluated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "decision_code IN ('eligible', 'hard_rejected', 'freshness_expired', "
            "'below_relevance_threshold', 'below_rank_score_threshold')",
            name=op.f("ck_match_traces_decision_code_valid"),
        ),
        sa.CheckConstraint(
            "(eligible AND decision_code = 'eligible' AND rank IS NOT NULL "
            "AND rank >= 1 AND hard_filter_eligible AND final_rank_score IS NOT NULL) "
            "OR (NOT eligible AND decision_code <> 'eligible' AND rank IS NULL)",
            name=op.f("ck_match_traces_decision_rank_consistent"),
        ),
        sa.CheckConstraint(
            "freshness_age_seconds >= 0 AND freshness_score BETWEEN 0 AND 1 "
            "AND minimum_relevance_threshold BETWEEN 0 AND 1 "
            "AND minimum_rank_score_threshold BETWEEN 0 AND 1",
            name=op.f("ck_match_traces_freshness_thresholds_valid"),
        ),
        sa.CheckConstraint(
            "(hard_filter_eligible AND jsonb_array_length(hard_filter_reasons) = 0) "
            "OR (NOT hard_filter_eligible "
            "AND jsonb_array_length(hard_filter_reasons) > 0)",
            name=op.f("ck_match_traces_hard_filter_evidence_consistent"),
        ),
        sa.CheckConstraint(
            "input_sha256 ~ '^[0-9a-f]{64}$'",
            name=op.f("ck_match_traces_input_sha256_valid"),
        ),
        sa.CheckConstraint(
            "profile_revision >= 1",
            name=op.f("ck_match_traces_profile_revision_valid"),
        ),
        sa.CheckConstraint(
            "semantic_status IN ('available', 'degraded', 'unavailable_input')",
            name=op.f("ck_match_traces_semantic_status_valid"),
        ),
        sa.CheckConstraint(
            "jsonb_typeof(hard_filter_reasons) = 'array' "
            "AND jsonb_typeof(nonblocking_unknowns) = 'array' "
            "AND jsonb_typeof(structured_components) = 'array'",
            name=op.f("ck_match_traces_trace_arrays_valid"),
        ),
        sa.ForeignKeyConstraint(
            ["opportunity_id"],
            ["opportunities.id"],
            name=op.f("fk_match_traces_opportunity_id_opportunities"),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["run_id"],
            ["match_evaluation_runs.id"],
            name=op.f("fk_match_traces_run_id_match_evaluation_runs"),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["search_profile_id"],
            ["search_profiles.id"],
            name=op.f("fk_match_traces_search_profile_id_search_profiles"),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["source_quality_snapshot_id"],
            ["source_quality_snapshots.id"],
            name=op.f("fk_match_traces_source_quality_snapshot_id_source_quality_snapshots"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_match_traces")),
        sa.UniqueConstraint(
            "run_id",
            "opportunity_id",
            "search_profile_id",
            "profile_revision",
            name="uq_match_traces_run_opportunity_profile_revision",
        ),
    )
    op.create_index(
        "ix_match_traces_opportunity_profile_evaluated",
        "match_traces",
        ["opportunity_id", "search_profile_id", "evaluated_at"],
        unique=False,
    )
    op.create_index(
        "ix_match_traces_profile_eligible_rank",
        "match_traces",
        ["search_profile_id", "eligible", "rank"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_match_traces_profile_eligible_rank",
        table_name="match_traces",
    )
    op.drop_index(
        "ix_match_traces_opportunity_profile_evaluated",
        table_name="match_traces",
    )
    op.drop_table("match_traces")
    op.drop_index(
        "ix_match_evaluation_runs_evaluated_at",
        table_name="match_evaluation_runs",
    )
    op.drop_table("match_evaluation_runs")
