"""Add the source library domain foundation.

Revision ID: 20260809_0003
Revises: 20260808_0002
Create Date: 2026-08-09
"""
from typing import Sequence

from alembic import op
import sqlalchemy as sa


revision: str = "20260809_0003"
down_revision: str | None = "20260808_0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "sources",
        sa.Column("id", sa.BigInteger(), sa.Identity(), nullable=False),
        sa.Column("platform", sa.String(length=32), nullable=False),
        sa.Column("external_id", sa.String(length=255), nullable=False),
        sa.Column("access_type", sa.String(length=16), nullable=False),
        sa.Column(
            "lifecycle_status",
            sa.String(length=20),
            server_default="candidate",
            nullable=False,
        ),
        sa.Column("display_name", sa.Text(), nullable=False),
        sa.Column("handle", sa.String(length=255), nullable=True),
        sa.Column("canonical_url", sa.Text(), nullable=True),
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
            "access_type IN ('public', 'private')",
            name=op.f("ck_sources_access_type_valid"),
        ),
        sa.CheckConstraint(
            "canonical_url IS NULL OR (canonical_url = btrim(canonical_url) "
            "AND canonical_url <> '')",
            name=op.f("ck_sources_canonical_url_valid"),
        ),
        sa.CheckConstraint(
            "display_name = btrim(display_name) AND display_name <> ''",
            name=op.f("ck_sources_display_name_nonempty"),
        ),
        sa.CheckConstraint(
            "external_id = btrim(external_id) AND external_id <> ''",
            name=op.f("ck_sources_external_id_nonempty"),
        ),
        sa.CheckConstraint(
            "handle IS NULL OR (handle = lower(handle) AND handle = btrim(handle) "
            "AND handle <> '')",
            name=op.f("ck_sources_handle_normalized"),
        ),
        sa.CheckConstraint(
            "lifecycle_status IN ('candidate', 'approved', 'rejected', 'needs_review')",
            name=op.f("ck_sources_lifecycle_status_valid"),
        ),
        sa.CheckConstraint(
            "platform = lower(platform) "
            "AND platform ~ '^[a-z][a-z0-9_-]{0,31}$'",
            name=op.f("ck_sources_platform_valid"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_sources")),
        sa.UniqueConstraint(
            "platform",
            "external_id",
            name="uq_sources_platform_external_id",
        ),
    )
    op.create_index(
        "ix_sources_lifecycle_status_platform",
        "sources",
        ["lifecycle_status", "platform"],
        unique=False,
    )
    op.create_index(
        "uq_sources_platform_handle",
        "sources",
        ["platform", "handle"],
        unique=True,
        postgresql_where=sa.text("handle IS NOT NULL"),
    )

    op.create_table(
        "source_taxonomy_terms",
        sa.Column("id", sa.BigInteger(), sa.Identity(), nullable=False),
        sa.Column("dimension", sa.String(length=32), nullable=False),
        sa.Column("key", sa.String(length=100), nullable=False),
        sa.Column("display_name", sa.Text(), nullable=False),
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
            "dimension ~ '^[a-z][a-z0-9_]{0,31}$'",
            name=op.f("ck_source_taxonomy_terms_dimension_valid"),
        ),
        sa.CheckConstraint(
            "display_name = btrim(display_name) AND display_name <> ''",
            name=op.f("ck_source_taxonomy_terms_display_name_nonempty"),
        ),
        sa.CheckConstraint(
            "key ~ '^[a-z0-9][a-z0-9._:-]{0,99}$'",
            name=op.f("ck_source_taxonomy_terms_key_valid"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_source_taxonomy_terms")),
        sa.UniqueConstraint(
            "dimension",
            "key",
            name="uq_source_taxonomy_terms_dimension_key",
        ),
    )

    op.create_table(
        "source_taxonomy_assignments",
        sa.Column("source_id", sa.BigInteger(), nullable=False),
        sa.Column("term_id", sa.BigInteger(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["source_id"],
            ["sources.id"],
            name=op.f("fk_source_taxonomy_assignments_source_id_sources"),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["term_id"],
            ["source_taxonomy_terms.id"],
            name=op.f(
                "fk_source_taxonomy_assignments_term_id_source_taxonomy_terms"
            ),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint(
            "source_id",
            "term_id",
            name=op.f("pk_source_taxonomy_assignments"),
        ),
    )
    op.create_index(
        "ix_source_taxonomy_assignments_term_id_source_id",
        "source_taxonomy_assignments",
        ["term_id", "source_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_table("source_taxonomy_assignments")
    op.drop_table("source_taxonomy_terms")
    op.drop_table("sources")
