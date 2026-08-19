"""Add structured dedup evidence and deterministic preferred source.

Revision ID: 20260809_0015
Revises: 20260809_0014
Create Date: 2026-08-09
"""
from typing import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "20260809_0015"
down_revision: str | None = "20260809_0014"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

PREFERRED_SOURCE_POLICY_VERSION = "canonical-source-earliest-message.v1"


def upgrade() -> None:
    op.drop_constraint(
        op.f("ck_opportunity_analysis_links_dedup_evidence_consistent"),
        "opportunity_analysis_links",
        type_="check",
    )
    op.drop_constraint(
        op.f("ck_opportunity_analysis_links_dedup_relation_valid"),
        "opportunity_analysis_links",
        type_="check",
    )
    op.alter_column(
        "opportunity_analysis_links",
        "dedup_relation",
        existing_type=sa.String(length=16),
        type_=sa.String(length=24),
        existing_nullable=False,
    )
    op.add_column(
        "opportunity_analysis_links",
        sa.Column(
            "dedup_evidence",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
    )
    _create_dedup_constraints()
    op.create_check_constraint(
        op.f("ck_opportunity_analysis_links_dedup_evidence_object"),
        "opportunity_analysis_links",
        "jsonb_typeof(dedup_evidence) = 'object'",
    )

    op.add_column(
        "opportunities",
        sa.Column("preferred_raw_message_id", postgresql.UUID(as_uuid=True)),
    )
    op.add_column(
        "opportunities",
        sa.Column("preferred_source_policy_version", sa.String(length=64)),
    )
    op.create_foreign_key(
        op.f("fk_opportunities_preferred_raw_message_id_raw_messages"),
        "opportunities",
        "raw_messages",
        ["preferred_raw_message_id"],
        ["id"],
        ondelete="RESTRICT",
    )
    op.create_check_constraint(
        op.f("ck_opportunities_preferred_source_consistent"),
        "opportunities",
        "(preferred_raw_message_id IS NULL "
        "AND preferred_source_policy_version IS NULL) "
        "OR (preferred_raw_message_id IS NOT NULL "
        "AND preferred_source_policy_version IS NOT NULL "
        "AND preferred_source_policy_version "
        "~ '^[a-z0-9][a-z0-9_.-]{0,63}$')",
    )
    op.create_index(
        "ix_opportunities_preferred_raw_message_id",
        "opportunities",
        ["preferred_raw_message_id"],
    )
    op.execute(
        sa.text(
            "WITH ranked AS ("
            "SELECT links.opportunity_id, links.raw_message_id, "
            "row_number() OVER ("
            "PARTITION BY links.opportunity_id "
            "ORDER BY raw.message_date, raw.observed_at, raw.source_id, "
            "raw.external_message_id, raw.id"
            ") AS preference_rank "
            "FROM opportunity_source_messages links "
            "JOIN raw_messages raw ON raw.id = links.raw_message_id"
            ") "
            "UPDATE opportunities AS opportunity SET "
            "preferred_raw_message_id = ranked.raw_message_id, "
            "preferred_source_policy_version = :policy_version "
            "FROM ranked WHERE ranked.preference_rank = 1 "
            "AND ranked.opportunity_id = opportunity.id"
        ).bindparams(policy_version=PREFERRED_SOURCE_POLICY_VERSION)
    )


def downgrade() -> None:
    op.drop_index(
        "ix_opportunities_preferred_raw_message_id",
        table_name="opportunities",
    )
    op.drop_constraint(
        op.f("ck_opportunities_preferred_source_consistent"),
        "opportunities",
        type_="check",
    )
    op.drop_constraint(
        op.f("fk_opportunities_preferred_raw_message_id_raw_messages"),
        "opportunities",
        type_="foreignkey",
    )
    op.drop_column("opportunities", "preferred_source_policy_version")
    op.drop_column("opportunities", "preferred_raw_message_id")

    op.drop_constraint(
        op.f("ck_opportunity_analysis_links_dedup_evidence_object"),
        "opportunity_analysis_links",
        type_="check",
    )
    op.drop_constraint(
        op.f("ck_opportunity_analysis_links_dedup_evidence_consistent"),
        "opportunity_analysis_links",
        type_="check",
    )
    op.drop_constraint(
        op.f("ck_opportunity_analysis_links_dedup_relation_valid"),
        "opportunity_analysis_links",
        type_="check",
    )
    op.execute(
        sa.text(
            "UPDATE opportunity_analysis_links SET "
            "dedup_relation = 'near_duplicate', "
            "dedup_similarity = least(dedup_similarity, 0.99999) "
            "WHERE dedup_relation = 'semantic_duplicate'"
        )
    )
    op.drop_column("opportunity_analysis_links", "dedup_evidence")
    op.alter_column(
        "opportunity_analysis_links",
        "dedup_relation",
        existing_type=sa.String(length=24),
        type_=sa.String(length=16),
        existing_nullable=False,
    )
    op.create_check_constraint(
        op.f("ck_opportunity_analysis_links_dedup_relation_valid"),
        "opportunity_analysis_links",
        "dedup_relation IN ('canonical', 'exact_duplicate', 'near_duplicate')",
    )
    op.create_check_constraint(
        op.f("ck_opportunity_analysis_links_dedup_evidence_consistent"),
        "opportunity_analysis_links",
        "(dedup_relation = 'canonical' AND dedup_similarity IS NULL "
        "AND matched_analysis_cache_id IS NULL) "
        "OR (dedup_relation = 'exact_duplicate' AND dedup_similarity = 1 "
        "AND matched_analysis_cache_id IS NOT NULL) "
        "OR (dedup_relation = 'near_duplicate' "
        "AND dedup_similarity > 0 AND dedup_similarity < 1 "
        "AND matched_analysis_cache_id IS NOT NULL)",
    )


def _create_dedup_constraints() -> None:
    op.create_check_constraint(
        op.f("ck_opportunity_analysis_links_dedup_relation_valid"),
        "opportunity_analysis_links",
        "dedup_relation IN "
        "('canonical', 'exact_duplicate', 'near_duplicate', "
        "'semantic_duplicate')",
    )
    op.create_check_constraint(
        op.f("ck_opportunity_analysis_links_dedup_evidence_consistent"),
        "opportunity_analysis_links",
        "(dedup_relation = 'canonical' AND dedup_similarity IS NULL "
        "AND matched_analysis_cache_id IS NULL) "
        "OR (dedup_relation = 'exact_duplicate' AND dedup_similarity = 1 "
        "AND matched_analysis_cache_id IS NOT NULL) "
        "OR (dedup_relation = 'near_duplicate' "
        "AND dedup_similarity > 0 AND dedup_similarity < 1 "
        "AND matched_analysis_cache_id IS NOT NULL) "
        "OR (dedup_relation = 'semantic_duplicate' "
        "AND dedup_similarity > 0 AND dedup_similarity <= 1 "
        "AND matched_analysis_cache_id IS NOT NULL)",
    )
