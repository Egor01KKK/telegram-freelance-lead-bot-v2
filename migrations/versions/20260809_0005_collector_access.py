"""Add collector accounts and source access assignments.

Revision ID: 20260809_0005
Revises: 20260809_0004
Create Date: 2026-08-09
"""
from typing import Sequence

from alembic import op
import sqlalchemy as sa


revision: str = "20260809_0005"
down_revision: str | None = "20260809_0004"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "collector_accounts",
        sa.Column("id", sa.BigInteger(), sa.Identity(), nullable=False),
        sa.Column("platform", sa.String(length=32), nullable=False),
        sa.Column("external_account_id", sa.String(length=255), nullable=False),
        sa.Column("display_name", sa.Text(), nullable=False),
        sa.Column(
            "is_active",
            sa.Boolean(),
            server_default=sa.true(),
            nullable=False,
        ),
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
            "display_name = btrim(display_name) AND display_name <> ''",
            name=op.f("ck_collector_accounts_display_name_nonempty"),
        ),
        sa.CheckConstraint(
            "external_account_id = btrim(external_account_id) "
            "AND external_account_id <> ''",
            name=op.f("ck_collector_accounts_external_account_id_nonempty"),
        ),
        sa.CheckConstraint(
            "platform = lower(platform) "
            "AND platform ~ '^[a-z][a-z0-9_-]{0,31}$'",
            name=op.f("ck_collector_accounts_platform_valid"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_collector_accounts")),
        sa.UniqueConstraint(
            "platform",
            "external_account_id",
            name="uq_collector_accounts_platform_external_account_id",
        ),
    )
    op.create_index(
        "ix_collector_accounts_platform_active",
        "collector_accounts",
        ["platform", "is_active"],
        unique=False,
    )

    op.create_table(
        "source_collector_access",
        sa.Column("source_id", sa.BigInteger(), nullable=False),
        sa.Column("collector_account_id", sa.BigInteger(), nullable=False),
        sa.Column("access_status", sa.String(length=16), nullable=False),
        sa.Column("checked_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("checked_by", sa.String(length=128), nullable=False),
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
            "access_status IN ('permitted', 'inaccessible', 'revoked')",
            name=op.f("ck_source_collector_access_access_status_valid"),
        ),
        sa.CheckConstraint(
            "checked_by ~ '^[A-Za-z0-9][A-Za-z0-9_.:@-]{0,127}$'",
            name=op.f("ck_source_collector_access_checked_by_safe"),
        ),
        sa.ForeignKeyConstraint(
            ["collector_account_id"],
            ["collector_accounts.id"],
            name=op.f(
                "fk_source_collector_access_collector_account_id_collector_accounts"
            ),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["source_id"],
            ["sources.id"],
            name=op.f("fk_source_collector_access_source_id_sources"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint(
            "source_id",
            "collector_account_id",
            name=op.f("pk_source_collector_access"),
        ),
    )
    op.create_index(
        "ix_source_collector_access_account_status_source",
        "source_collector_access",
        ["collector_account_id", "access_status", "source_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_table("source_collector_access")
    op.drop_table("collector_accounts")
