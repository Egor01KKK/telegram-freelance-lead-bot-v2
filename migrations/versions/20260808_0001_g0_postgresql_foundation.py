"""Create the bounded G0 PostgreSQL compatibility foundation.

Revision ID: 20260808_0001
Revises:
Create Date: 2026-08-08
"""
from typing import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "20260808_0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "subscribers",
        sa.Column("id", sa.BigInteger(), sa.Identity(), nullable=False),
        sa.Column("telegram_chat_id", sa.BigInteger(), nullable=False),
        sa.Column("is_active", sa.Boolean(), server_default=sa.true(), nullable=False),
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
            "telegram_chat_id <> 0",
            name=op.f("ck_subscribers_telegram_chat_id_nonzero"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_subscribers")),
        sa.UniqueConstraint(
            "telegram_chat_id",
            name="uq_subscribers_telegram_chat_id",
        ),
    )

    op.create_table(
        "legacy_import_runs",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("source_sha256", sa.String(length=64), nullable=False),
        sa.Column("source_size_bytes", sa.BigInteger(), nullable=False),
        sa.Column("attempt_number", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("subscribers_seen", sa.Integer(), server_default="0", nullable=False),
        sa.Column("messages_seen", sa.Integer(), server_default="0", nullable=False),
        sa.Column("deliveries_seen", sa.Integer(), server_default="0", nullable=False),
        sa.Column("error_code", sa.String(length=64), nullable=True),
        sa.Column(
            "started_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "length(source_sha256) = 64",
            name=op.f("ck_legacy_import_runs_source_sha256_length"),
        ),
        sa.CheckConstraint(
            "source_size_bytes >= 0",
            name=op.f("ck_legacy_import_runs_source_size_nonnegative"),
        ),
        sa.CheckConstraint(
            "attempt_number > 0",
            name=op.f("ck_legacy_import_runs_attempt_number_positive"),
        ),
        sa.CheckConstraint(
            "subscribers_seen >= 0 AND messages_seen >= 0 AND deliveries_seen >= 0",
            name=op.f("ck_legacy_import_runs_counts_nonnegative"),
        ),
        sa.CheckConstraint(
            "status IN ('running', 'completed', 'failed')",
            name=op.f("ck_legacy_import_runs_status_valid"),
        ),
        sa.CheckConstraint(
            "(status = 'running' AND finished_at IS NULL) "
            "OR (status IN ('completed', 'failed') AND finished_at IS NOT NULL)",
            name=op.f("ck_legacy_import_runs_status_finished_at_consistent"),
        ),
        sa.CheckConstraint(
            "error_code IS NULL OR status = 'failed'",
            name=op.f("ck_legacy_import_runs_error_only_on_failure"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_legacy_import_runs")),
        sa.UniqueConstraint(
            "source_sha256",
            "attempt_number",
            name="uq_legacy_import_runs_snapshot_attempt",
        ),
    )
    op.create_index(
        "uq_legacy_import_runs_completed_snapshot",
        "legacy_import_runs",
        ["source_sha256"],
        unique=True,
        postgresql_where=sa.text("status = 'completed'"),
    )

    op.create_table(
        "legacy_processed_messages",
        sa.Column("id", sa.BigInteger(), sa.Identity(), nullable=False),
        sa.Column("first_import_run_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("legacy_lead_id", sa.BigInteger(), nullable=False),
        sa.Column("source_key", sa.Text(), nullable=False),
        sa.Column("telegram_message_id", sa.BigInteger(), nullable=False),
        sa.Column("state", sa.String(length=16), nullable=False),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("legacy_created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "imported_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.CheckConstraint(
            "legacy_lead_id > 0",
            name=op.f("ck_legacy_processed_messages_legacy_lead_id_positive"),
        ),
        sa.CheckConstraint(
            "source_key <> ''",
            name=op.f("ck_legacy_processed_messages_source_key_nonempty"),
        ),
        sa.CheckConstraint(
            "telegram_message_id > 0",
            name=op.f("ck_legacy_processed_messages_telegram_message_id_positive"),
        ),
        sa.CheckConstraint(
            "state IN ('pending', 'processed')",
            name=op.f("ck_legacy_processed_messages_state_valid"),
        ),
        sa.CheckConstraint(
            "(state = 'processed' AND processed_at IS NOT NULL) "
            "OR (state = 'pending' AND processed_at IS NULL)",
            name=op.f("ck_legacy_processed_messages_state_processed_at_consistent"),
        ),
        sa.ForeignKeyConstraint(
            ["first_import_run_id"],
            ["legacy_import_runs.id"],
            name=op.f(
                "fk_legacy_processed_messages_first_import_run_id_legacy_import_runs"
            ),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_legacy_processed_messages")),
        sa.UniqueConstraint(
            "source_key",
            "telegram_message_id",
            name="uq_legacy_processed_messages_source_message",
        ),
        sa.UniqueConstraint(
            "first_import_run_id",
            "legacy_lead_id",
            name="uq_legacy_processed_messages_import_lead",
        ),
    )

    op.create_table(
        "legacy_recipient_deliveries",
        sa.Column("id", sa.BigInteger(), sa.Identity(), nullable=False),
        sa.Column("legacy_processed_message_id", sa.BigInteger(), nullable=False),
        sa.Column("subscriber_id", sa.BigInteger(), nullable=False),
        sa.Column("state", sa.String(length=16), nullable=False),
        sa.Column("telegram_message_id", sa.BigInteger(), nullable=True),
        sa.Column("attempt_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column("error_code", sa.String(length=64), nullable=True),
        sa.Column("sent_at", sa.DateTime(timezone=True), nullable=True),
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
            "state IN ('pending', 'sent', 'failed', 'unknown')",
            name=op.f("ck_legacy_recipient_deliveries_state_valid"),
        ),
        sa.CheckConstraint(
            "attempt_count >= 0",
            name=op.f("ck_legacy_recipient_deliveries_attempt_count_nonnegative"),
        ),
        sa.CheckConstraint(
            "state <> 'sent' OR (telegram_message_id IS NOT NULL AND sent_at IS NOT NULL)",
            name=op.f("ck_legacy_recipient_deliveries_sent_metadata_present"),
        ),
        sa.ForeignKeyConstraint(
            ["legacy_processed_message_id"],
            ["legacy_processed_messages.id"],
            name=op.f(
                "fk_legacy_recipient_deliveries_legacy_processed_message_id_legacy_processed_messages"
            ),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["subscriber_id"],
            ["subscribers.id"],
            name=op.f("fk_legacy_recipient_deliveries_subscriber_id_subscribers"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_legacy_recipient_deliveries")),
        sa.UniqueConstraint(
            "legacy_processed_message_id",
            "subscriber_id",
            name="uq_legacy_recipient_deliveries_message_subscriber",
        ),
    )
    op.create_index(
        "uq_legacy_recipient_deliveries_subscriber_telegram_message",
        "legacy_recipient_deliveries",
        ["subscriber_id", "telegram_message_id"],
        unique=True,
        postgresql_where=sa.text("telegram_message_id IS NOT NULL"),
    )


def downgrade() -> None:
    op.drop_table("legacy_recipient_deliveries")
    op.drop_table("legacy_processed_messages")
    op.drop_table("legacy_import_runs")
    op.drop_table("subscribers")
