"""Persist immutable first-trial expiry and policy identity.

Revision ID: 20260815_0026
Revises: 20260815_0025
Create Date: 2026-08-15
"""
from typing import Sequence

from alembic import op
import sqlalchemy as sa


revision: str = "20260815_0026"
down_revision: str | None = "20260815_0025"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "users",
        sa.Column("trial_expires_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "users",
        sa.Column("trial_policy_version", sa.String(length=64), nullable=True),
    )
    op.execute(
        sa.text(
            "UPDATE users "
            "SET trial_expires_at = trial_started_at + interval '3 days', "
            "trial_policy_version = 'trial-entitlement.v1' "
            "WHERE trial_started_at IS NOT NULL"
        )
    )
    op.create_check_constraint(
        op.f("ck_users_trial_lifecycle_consistent"),
        "users",
        "(trial_started_at IS NULL AND trial_expires_at IS NULL "
        "AND trial_policy_version IS NULL) OR "
        "(trial_started_at IS NOT NULL AND trial_expires_at IS NOT NULL "
        "AND trial_expires_at > trial_started_at "
        "AND trial_policy_version IS NOT NULL "
        "AND trial_policy_version ~ '^[a-z0-9][a-z0-9._-]{0,63}$')",
    )


def downgrade() -> None:
    op.drop_constraint(
        op.f("ck_users_trial_lifecycle_consistent"),
        "users",
        type_="check",
    )
    op.drop_column("users", "trial_policy_version")
    op.drop_column("users", "trial_expires_at")
