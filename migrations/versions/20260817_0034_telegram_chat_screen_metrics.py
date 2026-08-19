"""Persist exact bounded history and AI screen request counters.

Revision ID: 20260817_0034
Revises: 20260817_0033
"""

from alembic import op
import sqlalchemy as sa


revision: str = "20260817_0034"
down_revision: str | None = "20260817_0033"
branch_labels: str | tuple[str, ...] | None = None
depends_on: str | tuple[str, ...] | None = None


def upgrade() -> None:
    op.add_column(
        "telegram_chat_discovery_screen_attempts",
        sa.Column("history_request_count", sa.Integer(), nullable=False, server_default="0"),
    )
    op.add_column(
        "telegram_chat_discovery_screen_attempts",
        sa.Column("ai_call_count", sa.Integer(), nullable=False, server_default="0"),
    )
    op.create_check_constraint(
        "screen_attempts_request_counters_nonnegative",
        "telegram_chat_discovery_screen_attempts",
        "history_request_count >= 0 AND ai_call_count >= 0",
    )


def downgrade() -> None:
    op.drop_constraint(
        "screen_attempts_request_counters_nonnegative",
        "telegram_chat_discovery_screen_attempts",
        type_="check",
    )
    op.drop_column("telegram_chat_discovery_screen_attempts", "ai_call_count")
    op.drop_column("telegram_chat_discovery_screen_attempts", "history_request_count")
