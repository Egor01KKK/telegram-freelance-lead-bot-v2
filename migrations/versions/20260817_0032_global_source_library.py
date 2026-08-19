"""Add durable Global Source Library campaign, evidence and monitoring state.

Revision ID: 20260817_0032
Revises: 20260816_0031
Create Date: 2026-08-17
"""

from typing import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB, UUID


revision: str = "20260817_0032"
down_revision: str | None = "20260816_0031"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SOURCE_STATUSES = (
    "'candidate', 'approved', 'active', 'degraded', 'paused', 'rejected', "
    "'needs_review', 'review_required', 'retired'"
)


def upgrade() -> None:
    op.drop_constraint(op.f("ck_sources_lifecycle_status_valid"), "sources", type_="check")
    op.create_check_constraint(
        op.f("ck_sources_lifecycle_status_valid"),
        "sources",
        f"lifecycle_status IN ({SOURCE_STATUSES})",
    )
    for column, statuses in (
        ("from_status", SOURCE_STATUSES),
        ("to_status", SOURCE_STATUSES),
    ):
        constraint = op.f(f"ck_source_lifecycle_events_{column}_valid")
        op.drop_constraint(constraint, "source_lifecycle_events", type_="check")
        op.create_check_constraint(
            constraint,
            "source_lifecycle_events",
            f"{column} IS NULL OR {column} IN ({statuses})"
            if column == "from_status"
            else f"{column} IN ({statuses})",
        )

    op.create_table(
        "discovery_campaigns",
        sa.Column("id", UUID(as_uuid=True), nullable=False),
        sa.Column("campaign_key", sa.String(128), nullable=False),
        sa.Column("campaign_type", sa.String(32), nullable=False),
        sa.Column("status", sa.String(16), server_default="planned", nullable=False),
        sa.Column("languages", JSONB(), nullable=False),
        sa.Column("geo_constraints", JSONB(), nullable=False),
        sa.Column("specialist_concepts", JSONB(), nullable=False),
        sa.Column("buyer_concepts", JSONB(), nullable=False),
        sa.Column("buyer_habitats", JSONB(), nullable=False),
        sa.Column("industry_contexts", JSONB(), nullable=False),
        sa.Column("query_strategy_version", sa.String(64), nullable=False),
        sa.Column("priority", sa.Integer(), server_default="50", nullable=False),
        sa.Column("created_from", sa.String(32), nullable=False),
        sa.Column("budget", JSONB(), server_default=sa.text("'{}'::jsonb"), nullable=False),
        sa.Column("progress", JSONB(), server_default=sa.text("'{}'::jsonb"), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("last_run_at", sa.DateTime(timezone=True)),
        sa.Column("next_run_at", sa.DateTime(timezone=True)),
        sa.Column("paused_at", sa.DateTime(timezone=True)),
        sa.Column("pause_reason", sa.Text()),
        sa.CheckConstraint("campaign_key = btrim(campaign_key) AND campaign_key <> '' AND campaign_key ~ '^[a-z0-9][a-z0-9:-]{0,127}$'", name=op.f("ck_discovery_campaigns_campaign_key_valid")),
        sa.CheckConstraint("campaign_type IN ('bootstrap', 'profile_gap', 'source_graph_expansion', 'manual_operator')", name=op.f("ck_discovery_campaigns_campaign_type_valid")),
        sa.CheckConstraint("status IN ('planned', 'running', 'paused', 'completed', 'failed')", name=op.f("ck_discovery_campaigns_status_valid")),
        sa.CheckConstraint("priority BETWEEN 0 AND 100", name=op.f("ck_discovery_campaigns_priority_valid")),
        sa.CheckConstraint("jsonb_typeof(languages) = 'array' AND jsonb_typeof(geo_constraints) = 'array'", name=op.f("ck_discovery_campaigns_language_geo_arrays")),
        sa.CheckConstraint("jsonb_typeof(specialist_concepts) = 'array' AND jsonb_typeof(buyer_concepts) = 'array' AND jsonb_typeof(buyer_habitats) = 'array' AND jsonb_typeof(industry_contexts) = 'array'", name=op.f("ck_discovery_campaigns_concept_arrays")),
        sa.CheckConstraint("jsonb_typeof(budget) = 'object' AND jsonb_typeof(progress) = 'object'", name=op.f("ck_discovery_campaigns_campaign_payloads_object")),
        sa.CheckConstraint("(status = 'paused' AND paused_at IS NOT NULL AND pause_reason IS NOT NULL) OR (status <> 'paused' AND paused_at IS NULL AND pause_reason IS NULL)", name=op.f("ck_discovery_campaigns_pause_fields_consistent")),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_discovery_campaigns")),
        sa.UniqueConstraint("campaign_key", name=op.f("uq_discovery_campaigns_campaign_key")),
    )
    op.create_index(op.f("ix_discovery_campaigns_status_priority_next_run"), "discovery_campaigns", ["status", "priority", "next_run_at"])

    op.create_table(
        "discovery_campaign_queries",
        sa.Column("id", UUID(as_uuid=True), nullable=False),
        sa.Column("campaign_id", UUID(as_uuid=True), nullable=False),
        sa.Column("normalized_query_key", sa.String(255), nullable=False),
        sa.Column("query_sha256", sa.String(64), nullable=False),
        sa.Column("query_text", sa.String(2000), nullable=False),
        sa.Column("query_family", sa.String(40), nullable=False),
        sa.Column("language", sa.String(16), nullable=False),
        sa.Column("strategy_version", sa.String(64), nullable=False),
        sa.Column("status", sa.String(16), server_default="queued", nullable=False),
        sa.Column("last_run_at", sa.DateTime(timezone=True)),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.CheckConstraint("normalized_query_key = btrim(normalized_query_key) AND normalized_query_key <> ''", name=op.f("ck_discovery_campaign_queries_normalized_key_valid")),
        sa.CheckConstraint("length(query_sha256) = 64 AND query_sha256 ~ '^[0-9a-f]{64}$'", name=op.f("ck_discovery_campaign_queries_query_hash_valid")),
        sa.CheckConstraint("query_text = btrim(query_text) AND query_text <> '' AND length(query_text) <= 2000", name=op.f("ck_discovery_campaign_queries_query_text_valid")),
        sa.CheckConstraint("query_family IN ('DIRECT_TELEGRAM_SOURCE', 'SITE_TELEGRAM', 'COMMUNITY_DIRECTORY', 'BUYER_HABITAT', 'HUB_LISTICLE', 'PROFILE_GAP')", name=op.f("ck_discovery_campaign_queries_query_family_valid")),
        sa.CheckConstraint("language ~ '^[a-z]{2,3}(?:-[a-z]{2})?$'", name=op.f("ck_discovery_campaign_queries_language_valid")),
        sa.CheckConstraint("status IN ('queued', 'running', 'completed', 'failed')", name=op.f("ck_discovery_campaign_queries_status_valid")),
        sa.ForeignKeyConstraint(["campaign_id"], ["discovery_campaigns.id"], ondelete="RESTRICT", name=op.f("fk_discovery_campaign_queries_campaign_id_discovery_campaigns")),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_discovery_campaign_queries")),
        sa.UniqueConstraint("campaign_id", "normalized_query_key", name=op.f("uq_discovery_campaign_queries_campaign_key")),
    )
    op.create_index(op.f("ix_discovery_campaign_queries_status"), "discovery_campaign_queries", ["status", "updated_at"])

    op.create_table(
        "discovery_campaign_profiles",
        sa.Column("campaign_id", UUID(as_uuid=True), nullable=False),
        sa.Column("search_profile_id", UUID(as_uuid=True), nullable=False),
        sa.Column("gap_key", sa.String(64), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.CheckConstraint("gap_key = btrim(gap_key) AND gap_key <> ''", name=op.f("ck_discovery_campaign_profiles_gap_key_valid")),
        sa.ForeignKeyConstraint(["campaign_id"], ["discovery_campaigns.id"], ondelete="RESTRICT", name=op.f("fk_discovery_campaign_profiles_campaign_id_discovery_campaigns")),
        sa.ForeignKeyConstraint(["search_profile_id"], ["search_profiles.id"], ondelete="RESTRICT", name=op.f("fk_discovery_campaign_profiles_search_profile_id_search_profiles")),
        sa.PrimaryKeyConstraint("campaign_id", "search_profile_id", name=op.f("pk_discovery_campaign_profiles")),
    )

    op.create_table(
        "source_reference_aliases",
        sa.Column("id", sa.BigInteger(), sa.Identity(), nullable=False),
        sa.Column("source_id", sa.BigInteger(), nullable=False),
        sa.Column("platform", sa.String(32), nullable=False),
        sa.Column("normalized_reference", sa.String(255), nullable=False),
        sa.Column("reference_kind", sa.String(32), nullable=False),
        sa.Column("canonical_peer_identity", sa.String(255)),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.CheckConstraint("platform = lower(platform) AND platform <> ''", name=op.f("ck_source_reference_aliases_platform_valid")),
        sa.CheckConstraint("normalized_reference = btrim(normalized_reference) AND normalized_reference <> ''", name=op.f("ck_source_reference_aliases_reference_valid")),
        sa.CheckConstraint("reference_kind IN ('source', 'message', 'invite', 'username', 'numeric_peer')", name=op.f("ck_source_reference_aliases_reference_kind_valid")),
        sa.ForeignKeyConstraint(["source_id"], ["sources.id"], ondelete="RESTRICT", name=op.f("fk_source_reference_aliases_source_id_sources")),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_source_reference_aliases")),
        sa.UniqueConstraint("platform", "normalized_reference", name=op.f("uq_source_reference_aliases_platform_reference")),
    )
    op.create_index(op.f("ix_source_reference_aliases_source"), "source_reference_aliases", ["source_id", "last_seen_at"])
    op.create_index(op.f("ix_source_reference_aliases_canonical_peer"), "source_reference_aliases", ["platform", "canonical_peer_identity"])

    op.create_table(
        "source_discovery_evidence",
        sa.Column("id", UUID(as_uuid=True), nullable=False),
        sa.Column("source_id", sa.BigInteger(), nullable=False),
        sa.Column("campaign_id", UUID(as_uuid=True)),
        sa.Column("discovery_run_id", UUID(as_uuid=True)),
        sa.Column("provider", sa.String(64), nullable=False),
        sa.Column("provider_kind", sa.String(32), nullable=False),
        sa.Column("query_family", sa.String(40)),
        sa.Column("query_key", sa.String(255)),
        sa.Column("query_sha256", sa.String(64)),
        sa.Column("result_domain", sa.String(255)),
        sa.Column("extraction_kind", sa.String(32), nullable=False),
        sa.Column("independent_evidence_key", sa.String(255), nullable=False),
        sa.Column("profile_gap_keys", JSONB(), server_default=sa.text("'[]'::jsonb"), nullable=False),
        sa.Column("source_graph_provenance", JSONB(), server_default=sa.text("'{}'::jsonb"), nullable=False),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.CheckConstraint("provider = lower(provider) AND provider <> ''", name=op.f("ck_source_discovery_evidence_provider_valid")),
        sa.CheckConstraint("provider_kind = lower(provider_kind) AND provider_kind <> ''", name=op.f("ck_source_discovery_evidence_provider_kind_valid")),
        sa.CheckConstraint("extraction_kind IN ('direct_result', 'page_extracted', 'source_graph', 'global_search', 'operator')", name=op.f("ck_source_discovery_evidence_extraction_kind_valid")),
        sa.CheckConstraint("independent_evidence_key = btrim(independent_evidence_key) AND independent_evidence_key <> ''", name=op.f("ck_source_discovery_evidence_evidence_key_valid")),
        sa.CheckConstraint("query_sha256 IS NULL OR (length(query_sha256) = 64 AND query_sha256 ~ '^[0-9a-f]{64}$')", name=op.f("ck_source_discovery_evidence_query_hash_valid")),
        sa.CheckConstraint("jsonb_typeof(profile_gap_keys) = 'array' AND jsonb_typeof(source_graph_provenance) = 'object'", name=op.f("ck_source_discovery_evidence_payloads_valid")),
        sa.ForeignKeyConstraint(["source_id"], ["sources.id"], ondelete="RESTRICT", name=op.f("fk_source_discovery_evidence_source_id_sources")),
        sa.ForeignKeyConstraint(["campaign_id"], ["discovery_campaigns.id"], ondelete="RESTRICT", name=op.f("fk_source_discovery_evidence_campaign_id_discovery_campaigns")),
        sa.ForeignKeyConstraint(["discovery_run_id"], ["discovery_runs.id"], ondelete="RESTRICT", name=op.f("fk_source_discovery_evidence_discovery_run_id_discovery_runs")),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_source_discovery_evidence")),
        sa.UniqueConstraint("source_id", "independent_evidence_key", name=op.f("uq_source_discovery_evidence_independent_key")),
    )
    op.create_index(op.f("ix_source_discovery_evidence_campaign"), "source_discovery_evidence", ["campaign_id", "last_seen_at"])
    op.create_index(op.f("ix_source_discovery_evidence_provider_kind"), "source_discovery_evidence", ["provider_kind", "extraction_kind"])

    op.create_table(
        "telegram_source_validations",
        sa.Column("source_id", sa.BigInteger(), nullable=False),
        sa.Column("collector_account_id", sa.BigInteger(), nullable=False),
        sa.Column("state", sa.String(24), server_default="discovered", nullable=False),
        sa.Column("access_mode", sa.String(24)),
        sa.Column("canonical_peer_identity", sa.String(255)),
        sa.Column("failure_code", sa.String(64)),
        sa.Column("checked_at", sa.DateTime(timezone=True)),
        sa.Column("checked_by", sa.String(128)),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.CheckConstraint("state IN ('discovered', 'local_valid', 'validation_pending', 'accessible', 'audit_pending', 'approved', 'rejected', 'needs_review', 'unavailable')", name=op.f("ck_telegram_source_validations_state_valid")),
        sa.CheckConstraint("access_mode IS NULL OR access_mode IN ('public_readable', 'joined', 'join_required', 'unavailable')", name=op.f("ck_telegram_source_validations_access_mode_valid")),
        sa.CheckConstraint("failure_code IS NULL OR failure_code ~ '^[A-Za-z][A-Za-z0-9_.-]{0,63}$'", name=op.f("ck_telegram_source_validations_failure_code_valid")),
        sa.ForeignKeyConstraint(["source_id"], ["sources.id"], ondelete="RESTRICT", name=op.f("fk_telegram_source_validations_source_id_sources")),
        sa.ForeignKeyConstraint(["collector_account_id"], ["collector_accounts.id"], ondelete="RESTRICT", name=op.f("fk_telegram_source_validations_collector_account_id_collector_accounts")),
        sa.PrimaryKeyConstraint("source_id", "collector_account_id", name=op.f("pk_telegram_source_validations")),
    )

    op.create_table(
        "source_monitoring_assignments",
        sa.Column("source_id", sa.BigInteger(), nullable=False),
        sa.Column("collector_account_id", sa.BigInteger(), nullable=False),
        sa.Column("tier", sa.String(1), server_default="B", nullable=False),
        sa.Column("state", sa.String(16), server_default="ready", nullable=False),
        sa.Column("cursor", JSONB(), server_default=sa.text("'{}'::jsonb"), nullable=False),
        sa.Column("next_due_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_started_at", sa.DateTime(timezone=True)),
        sa.Column("last_completed_at", sa.DateTime(timezone=True)),
        sa.Column("last_failure_code", sa.String(64)),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.CheckConstraint("tier IN ('A', 'B', 'C', 'D')", name=op.f("ck_source_monitoring_assignments_tier_valid")),
        sa.CheckConstraint("state IN ('ready', 'pacing', 'floodwait', 'paused', 'unavailable')", name=op.f("ck_source_monitoring_assignments_state_valid")),
        sa.CheckConstraint("jsonb_typeof(cursor) = 'object'", name=op.f("ck_source_monitoring_assignments_cursor_object")),
        sa.ForeignKeyConstraint(["source_id"], ["sources.id"], ondelete="RESTRICT", name=op.f("fk_source_monitoring_assignments_source_id_sources")),
        sa.ForeignKeyConstraint(["collector_account_id"], ["collector_accounts.id"], ondelete="RESTRICT", name=op.f("fk_source_monitoring_assignments_collector_account_id_collector_accounts")),
        sa.PrimaryKeyConstraint("source_id", name=op.f("pk_source_monitoring_assignments")),
        sa.UniqueConstraint("collector_account_id", "source_id", name=op.f("uq_source_monitoring_assignments_account_source")),
    )
    op.create_index(op.f("ix_source_monitoring_assignments_due"), "source_monitoring_assignments", ["state", "next_due_at"])

    op.create_table(
        "discovery_cost_events",
        sa.Column("id", UUID(as_uuid=True), nullable=False),
        sa.Column("campaign_id", UUID(as_uuid=True), nullable=False),
        sa.Column("stage", sa.String(32), nullable=False),
        sa.Column("provider", sa.String(64), nullable=False),
        sa.Column("units", sa.Integer(), server_default="1", nullable=False),
        sa.Column("estimated_cost_usd", sa.Numeric(18, 9), server_default="0", nullable=False),
        sa.Column("idempotency_key", sa.String(255), nullable=False),
        sa.Column("recorded_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.CheckConstraint("stage IN ('web_search', 'page_fetch', 'telegram_validation', 'source_audit')", name=op.f("ck_discovery_cost_events_stage_valid")),
        sa.CheckConstraint("provider = lower(provider) AND provider <> ''", name=op.f("ck_discovery_cost_events_provider_valid")),
        sa.CheckConstraint("units > 0 AND estimated_cost_usd >= 0", name=op.f("ck_discovery_cost_events_cost_nonnegative")),
        sa.ForeignKeyConstraint(["campaign_id"], ["discovery_campaigns.id"], ondelete="RESTRICT", name=op.f("fk_discovery_cost_events_campaign_id_discovery_campaigns")),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_discovery_cost_events")),
        sa.UniqueConstraint("idempotency_key", name=op.f("uq_discovery_cost_events_idempotency_key")),
    )


def downgrade() -> None:
    op.drop_table("discovery_cost_events")
    op.drop_index(op.f("ix_source_monitoring_assignments_due"), table_name="source_monitoring_assignments")
    op.drop_table("source_monitoring_assignments")
    op.drop_table("telegram_source_validations")
    op.drop_index(op.f("ix_source_discovery_evidence_provider_kind"), table_name="source_discovery_evidence")
    op.drop_index(op.f("ix_source_discovery_evidence_campaign"), table_name="source_discovery_evidence")
    op.drop_table("source_discovery_evidence")
    op.drop_index(op.f("ix_source_reference_aliases_canonical_peer"), table_name="source_reference_aliases")
    op.drop_index(op.f("ix_source_reference_aliases_source"), table_name="source_reference_aliases")
    op.drop_table("source_reference_aliases")
    op.drop_table("discovery_campaign_profiles")
    op.drop_index(op.f("ix_discovery_campaign_queries_status"), table_name="discovery_campaign_queries")
    op.drop_table("discovery_campaign_queries")
    op.drop_index(op.f("ix_discovery_campaigns_status_priority_next_run"), table_name="discovery_campaigns")
    op.drop_table("discovery_campaigns")

    op.execute("UPDATE sources SET lifecycle_status = 'approved' WHERE lifecycle_status IN ('active', 'degraded', 'retired')")
    op.execute("UPDATE sources SET lifecycle_status = 'needs_review' WHERE lifecycle_status = 'review_required'")
    for column in ("from_status", "to_status"):
        constraint = op.f(f"ck_source_lifecycle_events_{column}_valid")
        op.drop_constraint(constraint, "source_lifecycle_events", type_="check")
        expression = (
            f"{column} IS NULL OR {column} IN ('candidate', 'approved', 'paused', 'rejected', 'needs_review')"
            if column == "from_status"
            else f"{column} IN ('candidate', 'approved', 'paused', 'rejected', 'needs_review')"
        )
        op.create_check_constraint(constraint, "source_lifecycle_events", expression)
    op.drop_constraint(op.f("ck_sources_lifecycle_status_valid"), "sources", type_="check")
    op.create_check_constraint(
        op.f("ck_sources_lifecycle_status_valid"),
        "sources",
        "lifecycle_status IN ('candidate', 'approved', 'paused', 'rejected', 'needs_review')",
    )
