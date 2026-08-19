"""Add search-profile activation and first-trial start state.

Revision ID: 20260814_0021
Revises: 20260810_0020
Create Date: 2026-08-14
"""
from typing import Sequence

from alembic import op
import sqlalchemy as sa


revision: str = "20260814_0021"
down_revision: str | None = "20260810_0020"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "users",
        sa.Column("trial_started_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_check_constraint(
        op.f("ck_users_trial_started_at_valid"),
        "users",
        "trial_started_at IS NULL OR trial_started_at >= created_at",
    )

    op.add_column(
        "search_profiles",
        sa.Column(
            "is_active",
            sa.Boolean(),
            server_default=sa.false(),
            nullable=False,
        ),
    )
    op.add_column(
        "search_profiles",
        sa.Column(
            "is_primary",
            sa.Boolean(),
            server_default=sa.false(),
            nullable=False,
        ),
    )
    op.add_column(
        "search_profiles",
        sa.Column("activated_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "search_profiles",
        sa.Column("deactivated_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_check_constraint(
        op.f("ck_search_profiles_activation_state_consistent"),
        "search_profiles",
        "(is_active "
        "AND confirmation_status = 'confirmed' "
        "AND activated_at IS NOT NULL "
        "AND deactivated_at IS NULL) "
        "OR (NOT is_active "
        "AND NOT is_primary "
        "AND ((activated_at IS NULL AND deactivated_at IS NULL) "
        "OR (activated_at IS NOT NULL "
        "AND deactivated_at IS NOT NULL "
        "AND deactivated_at >= activated_at)))",
    )
    op.create_check_constraint(
        op.f("ck_search_profiles_primary_requires_active"),
        "search_profiles",
        "NOT is_primary OR is_active",
    )
    op.create_index(
        "ix_search_profiles_user_id_active",
        "search_profiles",
        ["user_id", "is_active", "created_at", "id"],
        unique=False,
    )
    op.create_index(
        "uq_search_profiles_user_primary",
        "search_profiles",
        ["user_id"],
        unique=True,
        postgresql_where=sa.text("is_primary"),
    )


def downgrade() -> None:
    op.drop_index(
        "uq_search_profiles_user_primary",
        table_name="search_profiles",
        postgresql_where=sa.text("is_primary"),
    )
    op.drop_index(
        "ix_search_profiles_user_id_active",
        table_name="search_profiles",
    )
    op.drop_constraint(
        op.f("ck_search_profiles_primary_requires_active"),
        "search_profiles",
        type_="check",
    )
    op.drop_constraint(
        op.f("ck_search_profiles_activation_state_consistent"),
        "search_profiles",
        type_="check",
    )
    op.drop_column("search_profiles", "deactivated_at")
    op.drop_column("search_profiles", "activated_at")
    op.drop_column("search_profiles", "is_primary")
    op.drop_column("search_profiles", "is_active")

    op.drop_constraint(
        op.f("ck_users_trial_started_at_valid"),
        "users",
        type_="check",
    )
    op.drop_column("users", "trial_started_at")
