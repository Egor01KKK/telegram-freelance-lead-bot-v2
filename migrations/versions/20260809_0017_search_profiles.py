"""Add the versioned user and search-profile domain foundation.

Revision ID: 20260809_0017
Revises: 20260809_0016
Create Date: 2026-08-09
"""
from typing import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "20260809_0017"
down_revision: str | None = "20260809_0016"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "users",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("platform", sa.String(length=32), nullable=False),
        sa.Column("external_user_id", sa.String(length=255), nullable=False),
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
            "platform = lower(platform) "
            "AND platform ~ '^[a-z][a-z0-9_-]{0,31}$'",
            name=op.f("ck_users_platform_valid"),
        ),
        sa.CheckConstraint(
            "external_user_id = btrim(external_user_id) "
            "AND external_user_id <> ''",
            name=op.f("ck_users_external_user_id_valid"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_users")),
        sa.UniqueConstraint(
            "platform",
            "external_user_id",
            name="uq_users_platform_external_user_id",
        ),
    )
    op.create_table(
        "search_profiles",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("user_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("schema_version", sa.String(length=64), nullable=False),
        sa.Column("parser_version", sa.String(length=64), nullable=False),
        sa.Column("roles", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("skills", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "categories",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column("semantic_text_original", sa.Text(), nullable=False),
        sa.Column("semantic_text_normalized", sa.Text(), nullable=False),
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
            "schema_version ~ '^[a-z0-9][a-z0-9._-]{0,63}$'",
            name=op.f("ck_search_profiles_schema_version_valid"),
        ),
        sa.CheckConstraint(
            "parser_version ~ '^[a-z0-9][a-z0-9._-]{0,63}$'",
            name=op.f("ck_search_profiles_parser_version_valid"),
        ),
        sa.CheckConstraint(
            "jsonb_typeof(roles) = 'array' AND jsonb_array_length(roles) <= 64",
            name=op.f("ck_search_profiles_roles_array_bounded"),
        ),
        sa.CheckConstraint(
            "jsonb_typeof(skills) = 'array' AND jsonb_array_length(skills) <= 64",
            name=op.f("ck_search_profiles_skills_array_bounded"),
        ),
        sa.CheckConstraint(
            "jsonb_typeof(categories) = 'array' "
            "AND jsonb_array_length(categories) <= 64",
            name=op.f("ck_search_profiles_categories_array_bounded"),
        ),
        sa.CheckConstraint(
            "btrim(semantic_text_original) <> '' "
            "AND length(semantic_text_original) <= 10000",
            name=op.f("ck_search_profiles_semantic_text_original_valid"),
        ),
        sa.CheckConstraint(
            "semantic_text_normalized = btrim(semantic_text_normalized) "
            "AND semantic_text_normalized <> '' "
            "AND length(semantic_text_normalized) <= 10000",
            name=op.f("ck_search_profiles_semantic_text_normalized_valid"),
        ),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["users.id"],
            name=op.f("fk_search_profiles_user_id_users"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_search_profiles")),
    )
    op.create_index(
        "ix_search_profiles_user_id_created_at",
        "search_profiles",
        ["user_id", "created_at", "id"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_search_profiles_user_id_created_at",
        table_name="search_profiles",
    )
    op.drop_table("search_profiles")
    op.drop_table("users")
