"""Add durable raw Telegram message ingestion.

Revision ID: 20260809_0009
Revises: 20260809_0008
Create Date: 2026-08-09
"""
from typing import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "20260809_0009"
down_revision: str | None = "20260809_0008"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "raw_messages",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("source_id", sa.BigInteger(), nullable=False),
        sa.Column("collector_account_id", sa.BigInteger(), nullable=False),
        sa.Column("processing_job_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("schema_version", sa.String(length=32), nullable=False),
        sa.Column("platform", sa.String(length=32), nullable=False),
        sa.Column("external_source_id", sa.String(length=255), nullable=False),
        sa.Column("external_message_id", sa.BigInteger(), nullable=False),
        sa.Column("message_date", sa.DateTime(timezone=True), nullable=False),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("message_url", sa.Text(), nullable=False),
        sa.Column("content", sa.Text(), nullable=False),
        sa.Column(
            "transport_metadata",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column("ingestion_origin", sa.String(length=16), nullable=False),
        sa.Column("correlation_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.CheckConstraint(
            "schema_version ~ '^[a-z][a-z0-9_.-]{0,31}$'",
            name=op.f("ck_raw_messages_schema_version_safe"),
        ),
        sa.CheckConstraint(
            "platform = lower(platform) "
            "AND platform ~ '^[a-z][a-z0-9_-]{0,31}$'",
            name=op.f("ck_raw_messages_platform_valid"),
        ),
        sa.CheckConstraint(
            "external_source_id = btrim(external_source_id) "
            "AND external_source_id <> ''",
            name=op.f("ck_raw_messages_external_source_id_nonempty"),
        ),
        sa.CheckConstraint(
            "external_message_id > 0",
            name=op.f("ck_raw_messages_external_message_id_positive"),
        ),
        sa.CheckConstraint(
            "message_url = btrim(message_url) AND message_url <> ''",
            name=op.f("ck_raw_messages_message_url_nonempty"),
        ),
        sa.CheckConstraint(
            "jsonb_typeof(transport_metadata) = 'object'",
            name=op.f("ck_raw_messages_transport_metadata_object"),
        ),
        sa.CheckConstraint(
            "ingestion_origin IN ('live', 'catch_up')",
            name=op.f("ck_raw_messages_ingestion_origin_valid"),
        ),
        sa.ForeignKeyConstraint(
            ["collector_account_id"],
            ["collector_accounts.id"],
            name=op.f(
                "fk_raw_messages_collector_account_id_collector_accounts"
            ),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["processing_job_id"],
            ["durable_jobs.id"],
            name=op.f("fk_raw_messages_processing_job_id_durable_jobs"),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["source_id"],
            ["sources.id"],
            name=op.f("fk_raw_messages_source_id_sources"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_raw_messages")),
        sa.UniqueConstraint(
            "processing_job_id",
            name="uq_raw_messages_processing_job_id",
        ),
        sa.UniqueConstraint(
            "source_id",
            "external_message_id",
            name="uq_raw_messages_source_message",
        ),
    )
    op.create_index(
        "ix_raw_messages_source_message_date",
        "raw_messages",
        ["source_id", "message_date"],
        unique=False,
    )
    op.create_index(
        "ix_raw_messages_correlation_id",
        "raw_messages",
        ["correlation_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_table("raw_messages")
