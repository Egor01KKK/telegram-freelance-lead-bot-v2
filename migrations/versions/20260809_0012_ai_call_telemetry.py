"""Add versioned AI call and cost telemetry.

Revision ID: 20260809_0012
Revises: 20260809_0011
Create Date: 2026-08-09
"""
from typing import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "20260809_0012"
down_revision: str | None = "20260809_0011"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "ai_call_telemetry",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("raw_message_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("stage", sa.String(length=64), nullable=False),
        sa.Column("provider", sa.String(length=64), nullable=False),
        sa.Column("requested_model", sa.String(length=128), nullable=False),
        sa.Column("response_model", sa.String(length=128)),
        sa.Column("analyzer_version", sa.String(length=64), nullable=False),
        sa.Column("prompt_version", sa.String(length=100), nullable=False),
        sa.Column("schema_version", sa.String(length=32), nullable=False),
        sa.Column("routing_version", sa.String(length=100), nullable=False),
        sa.Column("route_reason", sa.String(length=64), nullable=False),
        sa.Column("provider_attempt", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=24), nullable=False),
        sa.Column("pricing_version", sa.String(length=100), nullable=False),
        sa.Column("input_usd_per_million", sa.Numeric(18, 9), nullable=False),
        sa.Column("output_usd_per_million", sa.Numeric(18, 9), nullable=False),
        sa.Column("input_tokens", sa.BigInteger()),
        sa.Column("output_tokens", sa.BigInteger()),
        sa.Column("total_tokens", sa.BigInteger()),
        sa.Column("latency_ms", sa.BigInteger()),
        sa.Column("estimated_cost_usd", sa.Numeric(18, 9)),
        sa.Column("error_code", sa.String(length=64)),
        sa.Column("started_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True)),
        sa.CheckConstraint("stage ~ '^[a-z0-9][a-z0-9._-]{0,63}$'", name=op.f("ck_ai_call_telemetry_stage_safe")),
        sa.CheckConstraint("provider ~ '^[a-z0-9][a-z0-9_-]{0,63}$'", name=op.f("ck_ai_call_telemetry_provider_safe")),
        sa.CheckConstraint("provider_attempt BETWEEN 1 AND 5", name=op.f("ck_ai_call_telemetry_provider_attempt_bounded")),
        sa.CheckConstraint("status IN ('started', 'succeeded', 'invalid_output', 'request_failed')", name=op.f("ck_ai_call_telemetry_status_valid")),
        sa.CheckConstraint("input_usd_per_million >= 0 AND output_usd_per_million >= 0", name=op.f("ck_ai_call_telemetry_prices_nonnegative")),
        sa.CheckConstraint("(input_tokens IS NULL OR input_tokens >= 0) AND (output_tokens IS NULL OR output_tokens >= 0) AND (total_tokens IS NULL OR total_tokens >= 0) AND (latency_ms IS NULL OR latency_ms >= 0) AND (estimated_cost_usd IS NULL OR estimated_cost_usd >= 0)", name=op.f("ck_ai_call_telemetry_measurements_nonnegative")),
        sa.CheckConstraint("(status = 'started' AND finished_at IS NULL AND latency_ms IS NULL) OR (status <> 'started' AND finished_at IS NOT NULL AND latency_ms IS NOT NULL)", name=op.f("ck_ai_call_telemetry_completion_consistent")),
        sa.ForeignKeyConstraint(["raw_message_id"], ["raw_messages.id"], ondelete="RESTRICT", name=op.f("fk_ai_call_telemetry_raw_message_id_raw_messages")),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_ai_call_telemetry")),
    )
    op.create_index("ix_ai_call_telemetry_started_at_stage", "ai_call_telemetry", ["started_at", "stage"])
    op.create_index("ix_ai_call_telemetry_raw_message_id", "ai_call_telemetry", ["raw_message_id"])


def downgrade() -> None:
    op.drop_index("ix_ai_call_telemetry_raw_message_id", table_name="ai_call_telemetry")
    op.drop_index("ix_ai_call_telemetry_started_at_stage", table_name="ai_call_telemetry")
    op.drop_table("ai_call_telemetry")
