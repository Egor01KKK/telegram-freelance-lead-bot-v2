"""Persist Web provider/backend health and cooldown state.

Revision ID: 20260816_0031
Revises: 20260816_0030
Create Date: 2026-08-16
"""

from typing import Sequence

from alembic import op
import sqlalchemy as sa


revision: str = "20260816_0031"
down_revision: str | None = "20260816_0030"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "web_provider_health",
        sa.Column("provider", sa.String(length=64), nullable=False),
        sa.Column("backend", sa.String(length=64), nullable=False),
        sa.Column("state", sa.String(length=16), nullable=False),
        sa.Column("successful_searches", sa.Integer(), server_default="0", nullable=False),
        sa.Column("http_403", sa.Integer(), server_default="0", nullable=False),
        sa.Column("http_429", sa.Integer(), server_default="0", nullable=False),
        sa.Column(
            "captcha_or_suspension",
            sa.Integer(),
            server_default="0",
            nullable=False,
        ),
        sa.Column("consecutive_failures", sa.Integer(), server_default="0", nullable=False),
        sa.Column("last_failure_category", sa.String(length=64)),
        sa.Column("last_failure_at", sa.DateTime(timezone=True)),
        sa.Column("backoff_until", sa.DateTime(timezone=True)),
        sa.Column("last_success_at", sa.DateTime(timezone=True)),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "provider = lower(provider) AND provider ~ '^[a-z][a-z0-9_-]{0,63}$'",
            name=op.f("ck_web_provider_health_provider_valid"),
        ),
        sa.CheckConstraint(
            "backend = lower(backend) AND backend ~ '^[a-z][a-z0-9_-]{0,63}$'",
            name=op.f("ck_web_provider_health_backend_valid"),
        ),
        sa.CheckConstraint(
            "state IN ('READY', 'DEGRADED', 'BACKOFF', 'UNAVAILABLE')",
            name=op.f("ck_web_provider_health_state_valid"),
        ),
        sa.CheckConstraint(
            "successful_searches >= 0 AND http_403 >= 0 AND http_429 >= 0 "
            "AND captcha_or_suspension >= 0 AND consecutive_failures >= 0",
            name=op.f("ck_web_provider_health_counts_valid"),
        ),
        sa.CheckConstraint(
            "last_failure_category IS NULL OR "
            "last_failure_category ~ '^[a-z][a-z0-9_-]{0,63}$'",
            name=op.f("ck_web_provider_health_last_failure_category_valid"),
        ),
        sa.PrimaryKeyConstraint(
            "provider",
            "backend",
            name=op.f("pk_web_provider_health"),
        ),
    )
    op.create_index(
        op.f("ix_web_provider_health_state_backoff"),
        "web_provider_health",
        ["state", "backoff_until"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        op.f("ix_web_provider_health_state_backoff"),
        table_name="web_provider_health",
    )
    op.drop_table("web_provider_health")
