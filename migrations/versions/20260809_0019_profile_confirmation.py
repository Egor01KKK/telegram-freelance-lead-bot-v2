"""Add the search-profile confirmation lifecycle.

Revision ID: 20260809_0019
Revises: 20260809_0018
Create Date: 2026-08-09
"""
from typing import Sequence

from alembic import op
import sqlalchemy as sa


revision: str = "20260809_0019"
down_revision: str | None = "20260809_0018"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "search_profiles",
        sa.Column(
            "confirmation_status",
            sa.String(length=16),
            server_default="draft",
            nullable=False,
        ),
    )
    op.add_column(
        "search_profiles",
        sa.Column("revision", sa.Integer(), server_default="1", nullable=False),
    )
    op.add_column(
        "search_profiles",
        sa.Column("confirmed_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_check_constraint(
        op.f("ck_search_profiles_confirmation_status_valid"),
        "search_profiles",
        "confirmation_status IN ('draft', 'confirmed')",
    )
    op.create_check_constraint(
        op.f("ck_search_profiles_revision_valid"),
        "search_profiles",
        "revision >= 1",
    )
    op.create_check_constraint(
        op.f("ck_search_profiles_confirmation_timestamp_consistent"),
        "search_profiles",
        "(confirmation_status = 'draft' AND confirmed_at IS NULL) OR "
        "(confirmation_status = 'confirmed' AND confirmed_at IS NOT NULL)",
    )


def downgrade() -> None:
    op.drop_constraint(
        op.f("ck_search_profiles_confirmation_timestamp_consistent"),
        "search_profiles",
        type_="check",
    )
    op.drop_constraint(
        op.f("ck_search_profiles_revision_valid"),
        "search_profiles",
        type_="check",
    )
    op.drop_constraint(
        op.f("ck_search_profiles_confirmation_status_valid"),
        "search_profiles",
        type_="check",
    )
    op.drop_column("search_profiles", "confirmed_at")
    op.drop_column("search_profiles", "revision")
    op.drop_column("search_profiles", "confirmation_status")
