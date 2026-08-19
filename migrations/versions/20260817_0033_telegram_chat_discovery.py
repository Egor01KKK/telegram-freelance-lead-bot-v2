"""Durable Telegram chat discovery topics, observations and screens.

Revision ID: 20260817_0033
Revises: 20260817_0032
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB, UUID


revision: str = "20260817_0033"
down_revision: str | None = "20260817_0032"
branch_labels: str | tuple[str, ...] | None = None
depends_on: str | tuple[str, ...] | None = None


def upgrade() -> None:
    op.create_table(
        "telegram_chat_discovery_topics",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("topic_key", sa.String(255), nullable=False),
        sa.Column("topic_text", sa.String(255), nullable=False),
        sa.Column("normalized_topic", sa.String(255), nullable=False),
        sa.Column("language", sa.String(16), nullable=False),
        sa.Column("topic_kind", sa.String(16), nullable=False),
        sa.Column("origin_key", sa.String(255)),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("priority", sa.Integer(), nullable=False, server_default="50"),
        sa.Column("refresh_interval_seconds", sa.Integer(), nullable=False),
        sa.Column("last_searched_at", sa.DateTime(timezone=True)),
        sa.Column("next_eligible_at", sa.DateTime(timezone=True)),
        sa.Column("last_collector_account_id", sa.BigInteger(), sa.ForeignKey("collector_accounts.id", ondelete="RESTRICT")),
        sa.Column("search_status", sa.String(16), nullable=False, server_default="queued"),
        sa.Column("search_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("message_hit_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("chat_entity_occurrence_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("unique_peer_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("known_peer_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("new_peer_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("last_error_code", sa.String(64)),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint("topic_key = btrim(topic_key) AND topic_key <> ''", name="topic_key_valid"),
        sa.CheckConstraint("topic_text = btrim(topic_text) AND topic_text <> '' AND length(topic_text) <= 255", name="topic_text_valid"),
        sa.CheckConstraint("normalized_topic = btrim(normalized_topic) AND normalized_topic <> ''", name="normalized_topic_valid"),
        sa.CheckConstraint("language ~ '^[a-z]{2,3}(?:-[a-z]{2})?$'", name="language_valid"),
        sa.CheckConstraint("topic_kind IN ('base', 'profile')", name="topic_kind_valid"),
        sa.CheckConstraint("priority BETWEEN 0 AND 100", name="priority_valid"),
        sa.CheckConstraint("refresh_interval_seconds >= 300", name="refresh_interval_valid"),
        sa.CheckConstraint("search_status IN ('queued', 'running', 'completed', 'failed', 'paused')", name="search_status_valid"),
        sa.CheckConstraint("search_count >= 0 AND message_hit_count >= 0 AND chat_entity_occurrence_count >= 0 AND unique_peer_count >= 0 AND known_peer_count >= 0 AND new_peer_count >= 0", name="counters_nonnegative"),
        sa.UniqueConstraint("normalized_topic", "language", name="uq_telegram_chat_discovery_topics_normalized_language"),
        sa.UniqueConstraint("topic_key", name="uq_telegram_chat_discovery_topics_topic_key"),
    )
    op.create_index(
        "ix_telegram_chat_discovery_topics_due",
        "telegram_chat_discovery_topics",
        ["is_active", "next_eligible_at", "priority"],
    )

    op.create_table(
        "telegram_chat_discovery_search_runs",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("topic_id", UUID(as_uuid=True), sa.ForeignKey("telegram_chat_discovery_topics.id", ondelete="RESTRICT"), nullable=False),
        sa.Column("collector_account_id", sa.BigInteger(), sa.ForeignKey("collector_accounts.id", ondelete="RESTRICT"), nullable=False),
        sa.Column("search_mode", sa.String(16), nullable=False),
        sa.Column("idempotency_key", sa.String(255), nullable=False),
        sa.Column("status", sa.String(16), nullable=False, server_default="running"),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("finished_at", sa.DateTime(timezone=True)),
        sa.Column("request_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("message_hit_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("chat_entity_occurrence_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("unique_peer_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("known_peer_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("new_peer_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("group_peer_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("broadcast_peer_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("error_code", sa.String(64)),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint("search_mode IN ('global', 'groups', 'broadcasts')", name="search_mode_valid"),
        sa.CheckConstraint("status IN ('running', 'completed', 'failed')", name="status_valid"),
        sa.CheckConstraint("request_count >= 0 AND message_hit_count >= 0 AND chat_entity_occurrence_count >= 0 AND unique_peer_count >= 0 AND known_peer_count >= 0 AND new_peer_count >= 0 AND group_peer_count >= 0 AND broadcast_peer_count >= 0", name="counters_nonnegative"),
        sa.UniqueConstraint("idempotency_key", name="uq_telegram_chat_discovery_search_runs_idempotency_key"),
    )
    op.create_index("ix_telegram_chat_discovery_search_runs_topic_started", "telegram_chat_discovery_search_runs", ["topic_id", "started_at"])

    op.create_table(
        "telegram_chat_discovery_peers",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("canonical_peer_identity", sa.String(255), nullable=False),
        sa.Column("peer_type", sa.String(16), nullable=False),
        sa.Column("telegram_peer_id", sa.BigInteger()),
        sa.Column("telegram_access_hash", sa.BigInteger()),
        sa.Column("display_name", sa.String(255), nullable=False),
        sa.Column("username", sa.String(255)),
        sa.Column("canonical_url", sa.String(2048)),
        sa.Column("access_type", sa.String(16), nullable=False),
        sa.Column("source_id", sa.BigInteger(), sa.ForeignKey("sources.id", ondelete="RESTRICT")),
        sa.Column("dedup_bucket", sa.String(32), nullable=False, server_default="GENUINELY_NEW"),
        sa.Column("screen_status", sa.String(16), nullable=False, server_default="SCREEN_PENDING"),
        sa.Column("screen_attempt_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("next_screen_at", sa.DateTime(timezone=True)),
        sa.Column("last_screened_at", sa.DateTime(timezone=True)),
        sa.Column("screen_decision", sa.String(16)),
        sa.Column("screen_policy_version", sa.String(64)),
        sa.Column("screen_model", sa.String(128)),
        sa.Column("screen_sample_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("screen_useful_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("screen_confidence", sa.Numeric(5, 4)),
        sa.Column("screen_error_code", sa.String(64)),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("last_collector_account_id", sa.BigInteger(), sa.ForeignKey("collector_accounts.id", ondelete="RESTRICT")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint("canonical_peer_identity = btrim(canonical_peer_identity) AND canonical_peer_identity <> ''", name="canonical_peer_identity_valid"),
        sa.CheckConstraint("peer_type IN ('group', 'supergroup', 'channel', 'broadcast')", name="peer_type_valid"),
        sa.CheckConstraint("access_type IN ('public', 'private')", name="access_type_valid"),
        sa.CheckConstraint("dedup_bucket IN ('ALREADY_APPROVED', 'ALREADY_CANDIDATE', 'ALREADY_REJECTED', 'ALREADY_NEEDS_REVIEW', 'GENUINELY_NEW')", name="dedup_bucket_valid"),
        sa.CheckConstraint("screen_status IN ('SCREEN_PENDING', 'SCREEN_RUNNING', 'WATCH', 'SKIP', 'UNCLEAR', 'SCREEN_FAILED')", name="screen_status_valid"),
        sa.CheckConstraint("screen_decision IS NULL OR screen_decision IN ('WATCH', 'SKIP', 'UNCLEAR')", name="screen_decision_valid"),
        sa.CheckConstraint("screen_attempt_count >= 0 AND screen_sample_count >= 0 AND screen_useful_count >= 0", name="screen_counters_nonnegative"),
        sa.CheckConstraint("screen_confidence IS NULL OR screen_confidence BETWEEN 0 AND 1", name="screen_confidence_valid"),
        sa.UniqueConstraint("canonical_peer_identity", name="uq_telegram_chat_discovery_peers_canonical_identity"),
    )
    op.create_index("ix_telegram_chat_discovery_peers_screen_due", "telegram_chat_discovery_peers", ["screen_status", "next_screen_at"])
    op.create_index("ix_telegram_chat_discovery_peers_bucket", "telegram_chat_discovery_peers", ["dedup_bucket", "created_at"])

    op.create_table(
        "telegram_chat_discovery_peer_aliases",
        sa.Column("id", sa.BigInteger(), sa.Identity(), primary_key=True),
        sa.Column("peer_id", UUID(as_uuid=True), sa.ForeignKey("telegram_chat_discovery_peers.id", ondelete="RESTRICT"), nullable=False),
        sa.Column("normalized_reference", sa.String(255), nullable=False),
        sa.Column("reference_kind", sa.String(32), nullable=False),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint("normalized_reference = btrim(normalized_reference) AND normalized_reference <> ''", name="reference_valid"),
        sa.CheckConstraint("reference_kind IN ('peer', 'username', 'canonical_url')", name="reference_kind_valid"),
        sa.UniqueConstraint("normalized_reference", name="uq_telegram_chat_discovery_peer_aliases_reference"),
    )
    op.create_index("ix_telegram_chat_discovery_peer_aliases_peer", "telegram_chat_discovery_peer_aliases", ["peer_id"])

    op.create_table(
        "telegram_chat_discovery_observations",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("peer_id", UUID(as_uuid=True), sa.ForeignKey("telegram_chat_discovery_peers.id", ondelete="RESTRICT"), nullable=False),
        sa.Column("topic_id", UUID(as_uuid=True), sa.ForeignKey("telegram_chat_discovery_topics.id", ondelete="RESTRICT"), nullable=False),
        sa.Column("search_run_id", UUID(as_uuid=True), sa.ForeignKey("telegram_chat_discovery_search_runs.id", ondelete="RESTRICT"), nullable=False),
        sa.Column("collector_account_id", sa.BigInteger(), sa.ForeignKey("collector_accounts.id", ondelete="RESTRICT"), nullable=False),
        sa.Column("provider", sa.String(64), nullable=False, server_default="telegram_chat_search"),
        sa.Column("language", sa.String(16), nullable=False),
        sa.Column("search_mode", sa.String(16), nullable=False),
        sa.Column("message_hit_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("chat_entity_occurrence_count", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint("provider = lower(provider) AND provider <> ''", name="provider_valid"),
        sa.CheckConstraint("language ~ '^[a-z]{2,3}(?:-[a-z]{2})?$'", name="language_valid"),
        sa.CheckConstraint("search_mode IN ('global', 'groups', 'broadcasts')", name="search_mode_valid"),
        sa.CheckConstraint("message_hit_count >= 0 AND chat_entity_occurrence_count >= 1", name="counters_nonnegative"),
        sa.UniqueConstraint("peer_id", "search_run_id", name="uq_telegram_chat_discovery_observations_peer_run"),
    )
    op.create_index("ix_telegram_chat_discovery_observations_topic_seen", "telegram_chat_discovery_observations", ["topic_id", "last_seen_at"])

    op.create_table(
        "telegram_chat_discovery_screen_attempts",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("peer_id", UUID(as_uuid=True), sa.ForeignKey("telegram_chat_discovery_peers.id", ondelete="RESTRICT"), nullable=False),
        sa.Column("collector_account_id", sa.BigInteger(), sa.ForeignKey("collector_accounts.id", ondelete="RESTRICT"), nullable=False),
        sa.Column("attempt_number", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("decision", sa.String(16)),
        sa.Column("policy_version", sa.String(64), nullable=False),
        sa.Column("provider", sa.String(64), nullable=False),
        sa.Column("model", sa.String(128)),
        sa.Column("sample_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("useful_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("confidence", sa.Numeric(5, 4)),
        sa.Column("category_counts", JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("reason_codes", JSONB(), nullable=False, server_default=sa.text("'[]'::jsonb")),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("finished_at", sa.DateTime(timezone=True)),
        sa.Column("error_code", sa.String(64)),
        sa.CheckConstraint("attempt_number > 0", name="attempt_number_valid"),
        sa.CheckConstraint("status IN ('SCREEN_RUNNING', 'WATCH', 'SKIP', 'UNCLEAR', 'SCREEN_FAILED')", name="status_valid"),
        sa.CheckConstraint("decision IS NULL OR decision IN ('WATCH', 'SKIP', 'UNCLEAR')", name="decision_valid"),
        sa.CheckConstraint("provider = lower(provider) AND provider <> ''", name="provider_valid"),
        sa.CheckConstraint("sample_count >= 0 AND useful_count >= 0", name="counters_nonnegative"),
        sa.CheckConstraint("confidence IS NULL OR confidence BETWEEN 0 AND 1", name="confidence_valid"),
        sa.CheckConstraint("jsonb_typeof(category_counts) = 'object' AND jsonb_typeof(reason_codes) = 'array'", name="payloads_valid"),
        sa.UniqueConstraint("peer_id", "attempt_number", name="uq_telegram_chat_discovery_screen_attempts_peer_attempt"),
    )
    op.create_index("ix_telegram_chat_discovery_screen_attempts_peer_started", "telegram_chat_discovery_screen_attempts", ["peer_id", "started_at"])


def downgrade() -> None:
    op.drop_index("ix_telegram_chat_discovery_screen_attempts_peer_started", table_name="telegram_chat_discovery_screen_attempts")
    op.drop_table("telegram_chat_discovery_screen_attempts")
    op.drop_index("ix_telegram_chat_discovery_observations_topic_seen", table_name="telegram_chat_discovery_observations")
    op.drop_table("telegram_chat_discovery_observations")
    op.drop_index("ix_telegram_chat_discovery_peer_aliases_peer", table_name="telegram_chat_discovery_peer_aliases")
    op.drop_table("telegram_chat_discovery_peer_aliases")
    op.drop_index("ix_telegram_chat_discovery_peers_bucket", table_name="telegram_chat_discovery_peers")
    op.drop_index("ix_telegram_chat_discovery_peers_screen_due", table_name="telegram_chat_discovery_peers")
    op.drop_table("telegram_chat_discovery_peers")
    op.drop_index("ix_telegram_chat_discovery_search_runs_topic_started", table_name="telegram_chat_discovery_search_runs")
    op.drop_table("telegram_chat_discovery_search_runs")
    op.drop_index("ix_telegram_chat_discovery_topics_due", table_name="telegram_chat_discovery_topics")
    op.drop_table("telegram_chat_discovery_topics")
