"""Persist profile-driven discovery intents and source relevance projections.

Revision ID: 20260816_0030
Revises: 20260816_0029
Create Date: 2026-08-16
"""

from typing import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB, UUID


revision: str = "20260816_0030"
down_revision: str | None = "20260816_0029"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "profile_discovery_intents",
        sa.Column("id", UUID(as_uuid=True), nullable=False),
        sa.Column("search_profile_id", UUID(as_uuid=True), nullable=False),
        sa.Column("profile_revision", sa.Integer(), nullable=False),
        sa.Column("roles", JSONB(), nullable=False),
        sa.Column("services", JSONB(), nullable=False),
        sa.Column("skills", JSONB(), nullable=False),
        sa.Column("industries", JSONB(), nullable=False),
        sa.Column("languages", JSONB(), nullable=False),
        sa.Column("geo_remote", JSONB(), nullable=False),
        sa.Column("likely_buyer_roles", JSONB(), nullable=False),
        sa.Column("buyer_contexts", JSONB(), nullable=False),
        sa.Column("buyer_habitats", JSONB(), nullable=False),
        sa.Column("literal_concepts", JSONB(), nullable=False),
        sa.Column("adjacent_concepts", JSONB(), nullable=False),
        sa.Column("generated_web_queries", JSONB(), nullable=False),
        sa.Column("version", sa.String(length=64), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "profile_revision >= 1",
            name=op.f("ck_profile_discovery_intents_profile_revision_valid"),
        ),
        sa.CheckConstraint(
            "version ~ '^[a-z0-9][a-z0-9._-]{0,63}$'",
            name=op.f("ck_profile_discovery_intents_version_valid"),
        ),
        sa.CheckConstraint(
            "jsonb_typeof(roles) = 'array' AND jsonb_typeof(services) = 'array' "
            "AND jsonb_typeof(skills) = 'array' "
            "AND jsonb_typeof(industries) = 'array' "
            "AND jsonb_typeof(languages) = 'array' "
            "AND jsonb_typeof(likely_buyer_roles) = 'array' "
            "AND jsonb_typeof(buyer_contexts) = 'array' "
            "AND jsonb_typeof(buyer_habitats) = 'array' "
            "AND jsonb_typeof(literal_concepts) = 'array' "
            "AND jsonb_typeof(adjacent_concepts) = 'array' "
            "AND jsonb_typeof(generated_web_queries) = 'array'",
            name=op.f("ck_profile_discovery_intents_arrays_valid"),
        ),
        sa.CheckConstraint(
            "jsonb_typeof(geo_remote) = 'object'",
            name=op.f("ck_profile_discovery_intents_geo_remote_valid"),
        ),
        sa.ForeignKeyConstraint(
            ["search_profile_id"],
            ["search_profiles.id"],
            name=op.f(
                "fk_profile_discovery_intents_search_profile_id_search_profiles"
            ),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_profile_discovery_intents")),
        sa.UniqueConstraint(
            "search_profile_id",
            "profile_revision",
            "version",
            name=op.f("uq_profile_discovery_intents_profile_revision_version"),
        ),
    )
    op.create_index(
        op.f("ix_profile_discovery_intents_profile_created"),
        "profile_discovery_intents",
        ["search_profile_id", "created_at"],
    )

    op.create_table(
        "source_profile_relevance",
        sa.Column("id", sa.BigInteger(), sa.Identity(), nullable=False),
        sa.Column("source_id", sa.BigInteger(), nullable=False),
        sa.Column("search_profile_id", UUID(as_uuid=True), nullable=False),
        sa.Column("discovery_intent_id", UUID(as_uuid=True), nullable=False),
        sa.Column("profile_revision", sa.Integer(), nullable=False),
        sa.Column("relevance_score", sa.Numeric(6, 5), nullable=False),
        sa.Column("relevance_class", sa.String(length=16), nullable=False),
        sa.Column("evidence_categories", JSONB(), nullable=False),
        sa.Column("last_evaluated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("version", sa.String(length=64), nullable=False),
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
            "profile_revision >= 1",
            name=op.f("ck_source_profile_relevance_profile_revision_valid"),
        ),
        sa.CheckConstraint(
            "relevance_score BETWEEN 0 AND 1",
            name=op.f("ck_source_profile_relevance_relevance_score_valid"),
        ),
        sa.CheckConstraint(
            "relevance_class IN ('weak', 'adequate', 'strong')",
            name=op.f("ck_source_profile_relevance_relevance_class_valid"),
        ),
        sa.CheckConstraint(
            "jsonb_typeof(evidence_categories) = 'array'",
            name=op.f("ck_source_profile_relevance_evidence_categories_valid"),
        ),
        sa.CheckConstraint(
            "version ~ '^[a-z0-9][a-z0-9._-]{0,63}$'",
            name=op.f("ck_source_profile_relevance_version_valid"),
        ),
        sa.ForeignKeyConstraint(
            ["source_id"],
            ["sources.id"],
            name=op.f("fk_source_profile_relevance_source_id_sources"),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["search_profile_id"],
            ["search_profiles.id"],
            name=op.f(
                "fk_source_profile_relevance_search_profile_id_search_profiles"
            ),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["discovery_intent_id"],
            ["profile_discovery_intents.id"],
            name=op.f(
                "fk_source_profile_relevance_discovery_intent_id_profile_discovery_intents"
            ),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_source_profile_relevance")),
        sa.UniqueConstraint(
            "source_id",
            "discovery_intent_id",
            name=op.f("uq_source_profile_relevance_source_intent"),
        ),
    )
    op.create_index(
        op.f("ix_source_profile_relevance_profile_class"),
        "source_profile_relevance",
        ["search_profile_id", "relevance_class", "last_evaluated_at"],
    )


def downgrade() -> None:
    op.drop_index(
        op.f("ix_source_profile_relevance_profile_class"),
        table_name="source_profile_relevance",
    )
    op.drop_table("source_profile_relevance")
    op.drop_index(
        op.f("ix_profile_discovery_intents_profile_created"),
        table_name="profile_discovery_intents",
    )
    op.drop_table("profile_discovery_intents")
