"""Keep the historical chat-screen migration revision in the linear graph.

The production database may already be stamped at 20260817_0035 from the
pre-merge Chat Discovery branch. The current branch's 20260817_0034 migration
owns the same columns on a clean database, so this bridge is intentionally a
no-op and only preserves a single Alembic chain.

Revision ID: 20260817_0035
Revises: 20260817_0034
"""

from typing import Sequence


revision: str = "20260817_0035"
down_revision: str | None = "20260817_0034"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    return None


def downgrade() -> None:
    return None
