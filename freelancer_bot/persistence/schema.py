from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB, UUID


NAMING_CONVENTION = {
    "ix": "ix_%(table_name)s_%(column_0_N_name)s",
    "uq": "uq_%(table_name)s_%(column_0_N_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_N_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}

metadata = sa.MetaData(naming_convention=NAMING_CONVENTION)

subscribers = sa.Table(
    "subscribers",
    metadata,
    sa.Column("id", sa.BigInteger(), sa.Identity(), primary_key=True),
    sa.Column("telegram_chat_id", sa.BigInteger(), nullable=False),
    sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.true()),
    sa.Column(
        "created_at",
        sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.func.now(),
    ),
    sa.Column(
        "updated_at",
        sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.func.now(),
    ),
    sa.CheckConstraint("telegram_chat_id <> 0", name="telegram_chat_id_nonzero"),
    sa.UniqueConstraint("telegram_chat_id", name="uq_subscribers_telegram_chat_id"),
)

users = sa.Table(
    "users",
    metadata,
    sa.Column("id", UUID(as_uuid=True), primary_key=True),
    sa.Column("platform", sa.String(32), nullable=False),
    sa.Column("external_user_id", sa.String(255), nullable=False),
    sa.Column(
        "created_at",
        sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.func.now(),
    ),
    sa.Column(
        "updated_at",
        sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.func.now(),
    ),
    sa.Column("trial_started_at", sa.DateTime(timezone=True)),
    sa.Column("trial_expires_at", sa.DateTime(timezone=True)),
    sa.Column("trial_policy_version", sa.String(64)),
    sa.CheckConstraint(
        "platform = lower(platform) "
        "AND platform ~ '^[a-z][a-z0-9_-]{0,31}$'",
        name="platform_valid",
    ),
    sa.CheckConstraint(
        "external_user_id = btrim(external_user_id) "
        "AND external_user_id <> ''",
        name="external_user_id_valid",
    ),
    sa.CheckConstraint(
        "trial_started_at IS NULL OR trial_started_at >= created_at",
        name="trial_started_at_valid",
    ),
    sa.CheckConstraint(
        "(trial_started_at IS NULL AND trial_expires_at IS NULL "
        "AND trial_policy_version IS NULL) OR "
        "(trial_started_at IS NOT NULL AND trial_expires_at IS NOT NULL "
        "AND trial_expires_at > trial_started_at "
        "AND trial_policy_version IS NOT NULL "
        "AND trial_policy_version ~ '^[a-z0-9][a-z0-9._-]{0,63}$')",
        name="trial_lifecycle_consistent",
    ),
    sa.UniqueConstraint(
        "platform",
        "external_user_id",
        name="uq_users_platform_external_user_id",
    ),
)

payment_provider_events = sa.Table(
    "payment_provider_events",
    metadata,
    sa.Column("id", UUID(as_uuid=True), primary_key=True),
    sa.Column("schema_version", sa.String(64), nullable=False),
    sa.Column("provider", sa.String(64), nullable=False),
    sa.Column("provider_event_id", sa.String(255), nullable=False),
    sa.Column("event_type", sa.String(64), nullable=False),
    sa.Column("provider_payment_id", sa.String(255), nullable=False),
    sa.Column(
        "user_id",
        UUID(as_uuid=True),
        sa.ForeignKey("users.id", ondelete="RESTRICT"),
        nullable=False,
    ),
    sa.Column("status", sa.String(16), nullable=False),
    sa.Column("amount", sa.Numeric(12, 2), nullable=False),
    sa.Column("currency", sa.String(3), nullable=False),
    sa.Column("period_start_at", sa.DateTime(timezone=True)),
    sa.Column("period_end_at", sa.DateTime(timezone=True)),
    sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("received_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("verification_version", sa.String(64), nullable=False),
    sa.Column("payload", JSONB(), nullable=False),
    sa.CheckConstraint(
        "schema_version ~ '^[a-z0-9][a-z0-9._-]{0,63}$'",
        name="schema_version_valid",
    ),
    sa.CheckConstraint(
        "provider = lower(provider) "
        "AND provider ~ '^[a-z][a-z0-9_-]{0,63}$'",
        name="provider_valid",
    ),
    sa.CheckConstraint(
        "provider_event_id = btrim(provider_event_id) "
        "AND provider_event_id <> ''",
        name="provider_event_id_valid",
    ),
    sa.CheckConstraint(
        "event_type = btrim(event_type) "
        "AND event_type <> '' "
        "AND event_type ~ '^[a-z][a-z0-9._-]{0,63}$'",
        name="event_type_valid",
    ),
    sa.CheckConstraint(
        "provider_payment_id = btrim(provider_payment_id) "
        "AND provider_payment_id <> ''",
        name="provider_payment_id_valid",
    ),
    sa.CheckConstraint(
        "status IN ('pending', 'succeeded', 'failed', 'cancelled')",
        name="status_valid",
    ),
    sa.CheckConstraint("amount >= 0", name="amount_nonnegative"),
    sa.CheckConstraint(
        "currency = upper(currency) "
        "AND currency ~ '^[A-Z]{3}$'",
        name="currency_valid",
    ),
    sa.CheckConstraint(
        "(period_start_at IS NULL AND period_end_at IS NULL) "
        "OR (period_start_at IS NOT NULL "
        "AND period_end_at IS NOT NULL "
        "AND period_end_at > period_start_at)",
        name="period_consistent",
    ),
    sa.CheckConstraint(
        "status <> 'succeeded' OR (amount > 0 "
        "AND period_start_at IS NOT NULL AND period_end_at IS NOT NULL)",
        name="success_evidence_complete",
    ),
    sa.CheckConstraint(
        "verification_version ~ '^[a-z0-9][a-z0-9._-]{0,63}$'",
        name="verification_version_valid",
    ),
    sa.CheckConstraint(
        "jsonb_typeof(payload) = 'object'",
        name="payload_object",
    ),
    sa.UniqueConstraint(
        "provider",
        "provider_event_id",
        name="uq_payment_provider_events_provider_event",
    ),
)

sa.Index(
    "ix_payment_provider_events_user_received",
    payment_provider_events.c.user_id,
    payment_provider_events.c.received_at,
    payment_provider_events.c.id,
)
sa.Index(
    "ix_payment_provider_events_payment",
    payment_provider_events.c.provider,
    payment_provider_events.c.provider_payment_id,
    payment_provider_events.c.occurred_at,
)

subscription_periods = sa.Table(
    "subscription_periods",
    metadata,
    sa.Column("id", UUID(as_uuid=True), primary_key=True),
    sa.Column("schema_version", sa.String(64), nullable=False),
    sa.Column("provider", sa.String(64), nullable=False),
    sa.Column("provider_payment_id", sa.String(255), nullable=False),
    sa.Column(
        "payment_provider_event_id",
        UUID(as_uuid=True),
        sa.ForeignKey("payment_provider_events.id", ondelete="RESTRICT"),
        nullable=False,
    ),
    sa.Column(
        "user_id",
        UUID(as_uuid=True),
        sa.ForeignKey("users.id", ondelete="RESTRICT"),
        nullable=False,
    ),
    sa.Column("amount", sa.Numeric(12, 2), nullable=False),
    sa.Column("currency", sa.String(3), nullable=False),
    sa.Column("period_start_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("period_end_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    sa.CheckConstraint(
        "schema_version ~ '^[a-z0-9][a-z0-9._-]{0,63}$'",
        name="schema_version_valid",
    ),
    sa.CheckConstraint(
        "provider = lower(provider) "
        "AND provider ~ '^[a-z][a-z0-9_-]{0,63}$'",
        name="provider_valid",
    ),
    sa.CheckConstraint(
        "provider_payment_id = btrim(provider_payment_id) "
        "AND provider_payment_id <> ''",
        name="provider_payment_id_valid",
    ),
    sa.CheckConstraint("amount > 0", name="amount_positive"),
    sa.CheckConstraint(
        "currency = upper(currency) "
        "AND currency ~ '^[A-Z]{3}$'",
        name="currency_valid",
    ),
    sa.CheckConstraint(
        "period_end_at > period_start_at",
        name="period_consistent",
    ),
    sa.UniqueConstraint(
        "provider",
        "provider_payment_id",
        name="uq_subscription_periods_provider_payment",
    ),
)

sa.Index(
    "ix_subscription_periods_user_period",
    subscription_periods.c.user_id,
    subscription_periods.c.period_start_at,
    subscription_periods.c.period_end_at,
    subscription_periods.c.id,
)

subscription_states = sa.Table(
    "subscription_states",
    metadata,
    sa.Column(
        "user_id",
        UUID(as_uuid=True),
        sa.ForeignKey("users.id", ondelete="RESTRICT"),
        primary_key=True,
    ),
    sa.Column("state", sa.String(24), nullable=False),
    sa.Column("state_version", sa.Integer(), nullable=False, server_default="1"),
    sa.Column("provider", sa.String(64)),
    sa.Column(
        "current_period_id",
        UUID(as_uuid=True),
        sa.ForeignKey("subscription_periods.id", ondelete="RESTRICT"),
    ),
    sa.Column("current_period_start_at", sa.DateTime(timezone=True)),
    sa.Column("current_period_end_at", sa.DateTime(timezone=True)),
    sa.Column("reason", sa.String(64), nullable=False),
    sa.Column("changed_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column(
        "created_at",
        sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.func.now(),
    ),
    sa.Column(
        "updated_at",
        sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.func.now(),
    ),
    sa.CheckConstraint(
        "state IN ('trial_not_started', 'trial_active', 'paid_active', "
        "'expired', 'cancelled', 'paused')",
        name="state_valid",
    ),
    sa.CheckConstraint("state_version >= 1", name="state_version_valid"),
    sa.CheckConstraint(
        "provider IS NULL OR (provider = lower(provider) "
        "AND provider ~ '^[a-z][a-z0-9_-]{0,63}$')",
        name="provider_valid",
    ),
    sa.CheckConstraint(
        "(current_period_id IS NULL AND current_period_start_at IS NULL "
        "AND current_period_end_at IS NULL) OR "
        "(current_period_id IS NOT NULL "
        "AND current_period_start_at IS NOT NULL "
        "AND current_period_end_at IS NOT NULL "
        "AND current_period_end_at > current_period_start_at)",
        name="current_period_consistent",
    ),
    sa.CheckConstraint(
        "state <> 'paid_active' OR current_period_id IS NOT NULL",
        name="paid_state_requires_period",
    ),
    sa.CheckConstraint(
        "reason = btrim(reason) AND reason <> '' "
        "AND reason ~ '^[a-z][a-z0-9._-]{0,63}$'",
        name="reason_valid",
    ),
)

subscription_state_events = sa.Table(
    "subscription_state_events",
    metadata,
    sa.Column("id", UUID(as_uuid=True), primary_key=True),
    sa.Column("idempotency_key", sa.String(64), nullable=False),
    sa.Column("schema_version", sa.String(64), nullable=False),
    sa.Column(
        "user_id",
        UUID(as_uuid=True),
        sa.ForeignKey("users.id", ondelete="RESTRICT"),
        nullable=False,
    ),
    sa.Column("state_version", sa.Integer(), nullable=False),
    sa.Column("from_state", sa.String(24)),
    sa.Column("to_state", sa.String(24), nullable=False),
    sa.Column("provider", sa.String(64)),
    sa.Column(
        "subscription_period_id",
        UUID(as_uuid=True),
        sa.ForeignKey("subscription_periods.id", ondelete="RESTRICT"),
    ),
    sa.Column("reason", sa.String(64), nullable=False),
    sa.Column("effective_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column(
        "created_at",
        sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.func.now(),
    ),
    sa.CheckConstraint(
        "idempotency_key ~ '^[0-9a-f]{64}$'",
        name="idempotency_key_sha256",
    ),
    sa.CheckConstraint(
        "schema_version ~ '^[a-z0-9][a-z0-9._-]{0,63}$'",
        name="schema_version_valid",
    ),
    sa.CheckConstraint("state_version >= 1", name="state_version_valid"),
    sa.CheckConstraint(
        "from_state IS NULL OR from_state IN ('trial_not_started', 'trial_active', "
        "'paid_active', 'expired', 'cancelled', 'paused')",
        name="from_state_valid",
    ),
    sa.CheckConstraint(
        "to_state IN ('trial_not_started', 'trial_active', 'paid_active', "
        "'expired', 'cancelled', 'paused')",
        name="to_state_valid",
    ),
    sa.CheckConstraint(
        "provider IS NULL OR (provider = lower(provider) "
        "AND provider ~ '^[a-z][a-z0-9_-]{0,63}$')",
        name="provider_valid",
    ),
    sa.CheckConstraint(
        "to_state <> 'paid_active' OR subscription_period_id IS NOT NULL",
        name="paid_state_requires_period",
    ),
    sa.CheckConstraint(
        "reason = btrim(reason) AND reason <> '' "
        "AND reason ~ '^[a-z][a-z0-9._-]{0,63}$'",
        name="reason_valid",
    ),
    sa.UniqueConstraint(
        "user_id",
        "state_version",
        name="uq_subscription_state_events_user_version",
    ),
    sa.UniqueConstraint(
        "idempotency_key",
        name="uq_subscription_state_events_idempotency_key",
    ),
)

sa.Index(
    "ix_subscription_state_events_user_created",
    subscription_state_events.c.user_id,
    subscription_state_events.c.created_at,
    subscription_state_events.c.id,
)

search_profiles = sa.Table(
    "search_profiles",
    metadata,
    sa.Column("id", UUID(as_uuid=True), primary_key=True),
    sa.Column(
        "user_id",
        UUID(as_uuid=True),
        sa.ForeignKey("users.id", ondelete="RESTRICT"),
        nullable=False,
    ),
    sa.Column("schema_version", sa.String(64), nullable=False),
    sa.Column("parser_version", sa.String(64), nullable=False),
    sa.Column(
        "analysis_cache_id",
        UUID(as_uuid=True),
        sa.ForeignKey("search_profile_analysis_cache.id", ondelete="RESTRICT"),
    ),
    sa.Column("roles", JSONB(), nullable=False),
    sa.Column("skills", JSONB(), nullable=False),
    sa.Column("categories", JSONB(), nullable=False),
    sa.Column("semantic_text_original", sa.Text(), nullable=False),
    sa.Column("semantic_text_normalized", sa.Text(), nullable=False),
    sa.Column(
        "preferences",
        JSONB(),
        nullable=False,
        server_default=sa.text(
            "jsonb_build_object("
            "'schema_version', 'search_profile_preferences.v1', "
            "'work_types', NULL, 'minimum_budget', NULL, 'currency', NULL, "
            "'budget_policy', NULL, 'languages', NULL, 'geographies', NULL, "
            "'work_modes', NULL, 'excluded_categories', NULL)"
        ),
    ),
    sa.Column(
        "confirmation_status",
        sa.String(16),
        nullable=False,
        server_default="draft",
    ),
    sa.Column("revision", sa.Integer(), nullable=False, server_default="1"),
    sa.Column("confirmed_at", sa.DateTime(timezone=True)),
    sa.Column(
        "is_active",
        sa.Boolean(),
        nullable=False,
        server_default=sa.false(),
    ),
    sa.Column(
        "is_primary",
        sa.Boolean(),
        nullable=False,
        server_default=sa.false(),
    ),
    sa.Column("activated_at", sa.DateTime(timezone=True)),
    sa.Column("deactivated_at", sa.DateTime(timezone=True)),
    sa.Column(
        "created_at",
        sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.func.now(),
    ),
    sa.Column(
        "updated_at",
        sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.func.now(),
    ),
    sa.CheckConstraint(
        "schema_version ~ '^[a-z0-9][a-z0-9._-]{0,63}$'",
        name="schema_version_valid",
    ),
    sa.CheckConstraint(
        "parser_version ~ '^[a-z0-9][a-z0-9._-]{0,63}$'",
        name="parser_version_valid",
    ),
    sa.CheckConstraint(
        "jsonb_typeof(roles) = 'array' AND jsonb_array_length(roles) <= 64",
        name="roles_array_bounded",
    ),
    sa.CheckConstraint(
        "jsonb_typeof(skills) = 'array' AND jsonb_array_length(skills) <= 64",
        name="skills_array_bounded",
    ),
    sa.CheckConstraint(
        "jsonb_typeof(categories) = 'array' "
        "AND jsonb_array_length(categories) <= 64",
        name="categories_array_bounded",
    ),
    sa.CheckConstraint(
        "btrim(semantic_text_original) <> '' "
        "AND length(semantic_text_original) <= 10000",
        name="semantic_text_original_valid",
    ),
    sa.CheckConstraint(
        "semantic_text_normalized = btrim(semantic_text_normalized) "
        "AND semantic_text_normalized <> '' "
        "AND length(semantic_text_normalized) <= 10000",
        name="semantic_text_normalized_valid",
    ),
    sa.CheckConstraint(
        "jsonb_typeof(preferences) = 'object' "
        "AND preferences ->> 'schema_version' = "
        "'search_profile_preferences.v1' "
        "AND preferences ?& ARRAY['work_types', 'minimum_budget', 'currency', "
        "'budget_policy', 'languages', 'geographies', 'work_modes', "
        "'excluded_categories'] "
        "AND (preferences -> 'work_types' = 'null'::jsonb OR "
        "jsonb_typeof(preferences -> 'work_types') = 'array') "
        "AND (preferences -> 'minimum_budget' = 'null'::jsonb OR "
        "jsonb_typeof(preferences -> 'minimum_budget') = 'string') "
        "AND (preferences -> 'currency' = 'null'::jsonb OR "
        "jsonb_typeof(preferences -> 'currency') = 'string') "
        "AND (preferences -> 'budget_policy' = 'null'::jsonb OR "
        "preferences ->> 'budget_policy' IN "
        "('allow_unknown', 'require_explicit')) "
        "AND (preferences -> 'languages' = 'null'::jsonb OR "
        "jsonb_typeof(preferences -> 'languages') = 'array') "
        "AND (preferences -> 'geographies' = 'null'::jsonb OR "
        "jsonb_typeof(preferences -> 'geographies') = 'array') "
        "AND (preferences -> 'work_modes' = 'null'::jsonb OR "
        "jsonb_typeof(preferences -> 'work_modes') = 'array') "
        "AND (preferences -> 'excluded_categories' = 'null'::jsonb OR "
        "jsonb_typeof(preferences -> 'excluded_categories') = 'array')",
        name="preferences_contract_valid",
    ),
    sa.CheckConstraint(
        "confirmation_status IN ('draft', 'confirmed')",
        name="confirmation_status_valid",
    ),
    sa.CheckConstraint("revision >= 1", name="revision_valid"),
    sa.CheckConstraint(
        "(confirmation_status = 'draft' AND confirmed_at IS NULL) OR "
        "(confirmation_status = 'confirmed' AND confirmed_at IS NOT NULL)",
        name="confirmation_timestamp_consistent",
    ),
    sa.CheckConstraint(
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
        name="activation_state_consistent",
    ),
    sa.CheckConstraint(
        "NOT is_primary OR is_active",
        name="primary_requires_active",
    ),
)

sa.Index(
    "ix_search_profiles_user_id_created_at",
    search_profiles.c.user_id,
    search_profiles.c.created_at,
    search_profiles.c.id,
)
sa.Index(
    "ix_search_profiles_analysis_cache_id",
    search_profiles.c.analysis_cache_id,
)
sa.Index(
    "ix_search_profiles_user_id_active",
    search_profiles.c.user_id,
    search_profiles.c.is_active,
    search_profiles.c.created_at,
    search_profiles.c.id,
)
sa.Index(
    "uq_search_profiles_user_primary",
    search_profiles.c.user_id,
    unique=True,
    postgresql_where=search_profiles.c.is_primary,
)

profile_discovery_intents = sa.Table(
    "profile_discovery_intents",
    metadata,
    sa.Column("id", UUID(as_uuid=True), primary_key=True),
    sa.Column(
        "search_profile_id",
        UUID(as_uuid=True),
        sa.ForeignKey("search_profiles.id", ondelete="RESTRICT"),
        nullable=False,
    ),
    sa.Column("profile_revision", sa.Integer(), nullable=False),
    sa.Column("roles", JSONB(), nullable=False),
    sa.Column("services", JSONB(), nullable=False),
    sa.Column("skills", JSONB(), nullable=False),
    sa.Column("industries", JSONB(), nullable=False),
    sa.Column("languages", JSONB(), nullable=False),
    sa.Column("geo_remote", JSONB(), nullable=False),
    sa.Column("likely_buyer_roles", JSONB(), nullable=False),
    sa.Column("buyer_contexts", JSONB(), nullable=False),
    sa.Column("buyer_habitats", JSONB(), nullable=False),
    sa.Column("literal_concepts", JSONB(), nullable=False),
    sa.Column("adjacent_concepts", JSONB(), nullable=False),
    sa.Column("generated_web_queries", JSONB(), nullable=False),
    sa.Column("version", sa.String(64), nullable=False),
    sa.Column(
        "created_at",
        sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.func.now(),
    ),
    sa.CheckConstraint("profile_revision >= 1", name="profile_revision_valid"),
    sa.CheckConstraint(
        "version ~ '^[a-z0-9][a-z0-9._-]{0,63}$'",
        name="version_valid",
    ),
    sa.CheckConstraint(
        "jsonb_typeof(roles) = 'array' AND jsonb_typeof(services) = 'array' "
        "AND jsonb_typeof(skills) = 'array' AND jsonb_typeof(industries) = 'array' "
        "AND jsonb_typeof(languages) = 'array' "
        "AND jsonb_typeof(likely_buyer_roles) = 'array' "
        "AND jsonb_typeof(buyer_contexts) = 'array' "
        "AND jsonb_typeof(buyer_habitats) = 'array' "
        "AND jsonb_typeof(literal_concepts) = 'array' "
        "AND jsonb_typeof(adjacent_concepts) = 'array' "
        "AND jsonb_typeof(generated_web_queries) = 'array'",
        name="arrays_valid",
    ),
    sa.CheckConstraint(
        "jsonb_typeof(geo_remote) = 'object'",
        name="geo_remote_valid",
    ),
    sa.UniqueConstraint(
        "search_profile_id",
        "profile_revision",
        "version",
        name="uq_profile_discovery_intents_profile_revision_version",
    ),
)

sa.Index(
    "ix_profile_discovery_intents_profile_created",
    profile_discovery_intents.c.search_profile_id,
    profile_discovery_intents.c.created_at,
)

source_profile_relevance = sa.Table(
    "source_profile_relevance",
    metadata,
    sa.Column("id", sa.BigInteger(), sa.Identity(), primary_key=True),
    sa.Column(
        "source_id",
        sa.BigInteger(),
        sa.ForeignKey("sources.id", ondelete="RESTRICT"),
        nullable=False,
    ),
    sa.Column(
        "search_profile_id",
        UUID(as_uuid=True),
        sa.ForeignKey("search_profiles.id", ondelete="RESTRICT"),
        nullable=False,
    ),
    sa.Column(
        "discovery_intent_id",
        UUID(as_uuid=True),
        sa.ForeignKey("profile_discovery_intents.id", ondelete="RESTRICT"),
        nullable=False,
    ),
    sa.Column("profile_revision", sa.Integer(), nullable=False),
    sa.Column("relevance_score", sa.Numeric(6, 5), nullable=False),
    sa.Column("relevance_class", sa.String(16), nullable=False),
    sa.Column("evidence_categories", JSONB(), nullable=False),
    sa.Column("last_evaluated_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("version", sa.String(64), nullable=False),
    sa.Column(
        "created_at",
        sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.func.now(),
    ),
    sa.Column(
        "updated_at",
        sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.func.now(),
    ),
    sa.CheckConstraint("profile_revision >= 1", name="profile_revision_valid"),
    sa.CheckConstraint(
        "relevance_score BETWEEN 0 AND 1",
        name="relevance_score_valid",
    ),
    sa.CheckConstraint(
        "relevance_class IN ('weak', 'adequate', 'strong')",
        name="relevance_class_valid",
    ),
    sa.CheckConstraint(
        "jsonb_typeof(evidence_categories) = 'array'",
        name="evidence_categories_valid",
    ),
    sa.CheckConstraint(
        "version ~ '^[a-z0-9][a-z0-9._-]{0,63}$'",
        name="version_valid",
    ),
    sa.UniqueConstraint(
        "source_id",
        "discovery_intent_id",
        name="uq_source_profile_relevance_source_intent",
    ),
)

sa.Index(
    "ix_source_profile_relevance_profile_class",
    source_profile_relevance.c.search_profile_id,
    source_profile_relevance.c.relevance_class,
    source_profile_relevance.c.last_evaluated_at,
)

search_profile_analysis_cache = sa.Table(
    "search_profile_analysis_cache",
    metadata,
    sa.Column("id", UUID(as_uuid=True), primary_key=True),
    sa.Column("input_sha256", sa.String(64), nullable=False),
    sa.Column("normalized_input_text", sa.Text(), nullable=False),
    sa.Column("cache_version", sa.String(255), nullable=False),
    sa.Column("schema_version", sa.String(64), nullable=False),
    sa.Column("provider", sa.String(64), nullable=False),
    sa.Column("requested_model", sa.String(128), nullable=False),
    sa.Column("response_model", sa.String(128), nullable=False),
    sa.Column("analyzer_version", sa.String(64), nullable=False),
    sa.Column("prompt_version", sa.String(100), nullable=False),
    sa.Column("attempt_count", sa.Integer(), nullable=False),
    sa.Column("provider_attempts", sa.Integer(), nullable=False, server_default="1"),
    sa.Column("completed_calls", sa.Integer(), nullable=False, server_default="1"),
    sa.Column("timeout_count", sa.Integer(), nullable=False, server_default="0"),
    sa.Column("transient_failure_count", sa.Integer(), nullable=False, server_default="0"),
    sa.Column("non_retryable_failure_count", sa.Integer(), nullable=False, server_default="0"),
    sa.Column("invalid_output_retry_count", sa.Integer(), nullable=False, server_default="0"),
    sa.Column("input_tokens", sa.Integer(), nullable=False),
    sa.Column("output_tokens", sa.Integer(), nullable=False),
    sa.Column("total_tokens", sa.Integer(), nullable=False),
    sa.Column("result", JSONB(), nullable=False),
    sa.Column(
        "created_at",
        sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.func.now(),
    ),
    sa.CheckConstraint("length(input_sha256) = 64", name="input_sha256_length"),
    sa.CheckConstraint(
        "normalized_input_text = btrim(normalized_input_text) "
        "AND normalized_input_text <> '' "
        "AND length(normalized_input_text) <= 10000",
        name="normalized_input_text_valid",
    ),
    sa.CheckConstraint(
        "cache_version = btrim(cache_version) AND cache_version <> ''",
        name="cache_version_valid",
    ),
    sa.CheckConstraint(
        "schema_version ~ '^[a-z0-9][a-z0-9._-]{0,63}$'",
        name="schema_version_valid",
    ),
    sa.CheckConstraint(
        "provider ~ '^[a-z][a-z0-9_-]{0,63}$'",
        name="provider_valid",
    ),
    sa.CheckConstraint(
        "requested_model = btrim(requested_model) AND requested_model <> ''",
        name="requested_model_valid",
    ),
    sa.CheckConstraint(
        "response_model = btrim(response_model) AND response_model <> ''",
        name="response_model_valid",
    ),
    sa.CheckConstraint(
        "analyzer_version ~ '^[a-z0-9][a-z0-9._-]{0,63}$'",
        name="analyzer_version_valid",
    ),
    sa.CheckConstraint(
        "prompt_version ~ '^[a-z0-9][a-z0-9._-]{0,99}$'",
        name="prompt_version_valid",
    ),
    sa.CheckConstraint(
        "attempt_count BETWEEN 1 AND 5",
        name="attempt_count_bounded",
    ),
    sa.CheckConstraint(
        "input_tokens >= 0 AND output_tokens >= 0 AND total_tokens >= 0 "
        "AND total_tokens = input_tokens + output_tokens",
        name="token_usage_valid",
    ),
    sa.CheckConstraint("jsonb_typeof(result) = 'object'", name="result_object"),
    sa.UniqueConstraint(
        "input_sha256",
        "cache_version",
        name="uq_search_profile_analysis_cache_input_version",
    ),
)

search_profile_onboarding_attempts = sa.Table(
    "search_profile_onboarding_attempts",
    metadata,
    sa.Column("id", UUID(as_uuid=True), primary_key=True),
    sa.Column("platform", sa.String(32), nullable=False),
    sa.Column("external_user_id", sa.String(255), nullable=False),
    sa.Column("input_sha256", sa.String(64), nullable=False),
    sa.Column("cache_version", sa.String(255), nullable=False),
    sa.Column("provider", sa.String(64), nullable=False),
    sa.Column("requested_model", sa.String(128), nullable=False),
    sa.Column("status", sa.String(16), nullable=False),
    sa.Column("retryable", sa.Boolean(), nullable=False),
    sa.Column("provider_attempts", sa.Integer(), nullable=False),
    sa.Column("completed_calls", sa.Integer(), nullable=False),
    sa.Column("timeout_count", sa.Integer(), nullable=False),
    sa.Column("transient_failure_count", sa.Integer(), nullable=False),
    sa.Column("non_retryable_failure_count", sa.Integer(), nullable=False),
    sa.Column("invalid_output_retry_count", sa.Integer(), nullable=False),
    sa.Column("error_code", sa.String(64)),
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    sa.Column("finished_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    sa.CheckConstraint(
        "platform = lower(platform) AND platform ~ '^[a-z][a-z0-9_-]{0,31}$'",
        name="platform_valid",
    ),
    sa.CheckConstraint(
        "external_user_id = btrim(external_user_id) AND external_user_id <> ''",
        name="external_user_id_valid",
    ),
    sa.CheckConstraint("length(input_sha256) = 64", name="input_sha256_length"),
    sa.CheckConstraint(
        "cache_version = btrim(cache_version) AND cache_version <> ''",
        name="cache_version_valid",
    ),
    sa.CheckConstraint(
        "provider = lower(provider) AND provider ~ '^[a-z][a-z0-9_-]{0,63}$'",
        name="provider_valid",
    ),
    sa.CheckConstraint(
        "requested_model = btrim(requested_model) AND requested_model <> ''",
        name="requested_model_valid",
    ),
    sa.CheckConstraint("status IN ('succeeded', 'failed')", name="status_valid"),
    sa.CheckConstraint(
        "provider_attempts >= 0 AND completed_calls >= 0 AND timeout_count >= 0 "
        "AND transient_failure_count >= 0 AND non_retryable_failure_count >= 0 "
        "AND invalid_output_retry_count >= 0",
        name="counters_nonnegative",
    ),
    sa.CheckConstraint(
        "(status = 'succeeded' AND completed_calls > 0) OR "
        "(status = 'failed' AND completed_calls = 0)",
        name="status_counters_consistent",
    ),
)

sa.Index(
    "ix_search_profile_onboarding_attempts_input_created",
    search_profile_onboarding_attempts.c.input_sha256,
    search_profile_onboarding_attempts.c.created_at,
)

legacy_import_runs = sa.Table(
    "legacy_import_runs",
    metadata,
    sa.Column("id", UUID(as_uuid=True), primary_key=True),
    sa.Column("source_sha256", sa.String(64), nullable=False),
    sa.Column("source_size_bytes", sa.BigInteger(), nullable=False),
    sa.Column("attempt_number", sa.Integer(), nullable=False),
    sa.Column("status", sa.String(16), nullable=False),
    sa.Column("subscribers_seen", sa.Integer(), nullable=False, server_default="0"),
    sa.Column("messages_seen", sa.Integer(), nullable=False, server_default="0"),
    sa.Column("deliveries_seen", sa.Integer(), nullable=False, server_default="0"),
    sa.Column("error_code", sa.String(64)),
    sa.Column(
        "started_at",
        sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.func.now(),
    ),
    sa.Column("finished_at", sa.DateTime(timezone=True)),
    sa.CheckConstraint(
        "length(source_sha256) = 64",
        name="source_sha256_length",
    ),
    sa.CheckConstraint("source_size_bytes >= 0", name="source_size_nonnegative"),
    sa.CheckConstraint("attempt_number > 0", name="attempt_number_positive"),
    sa.CheckConstraint(
        "subscribers_seen >= 0 AND messages_seen >= 0 AND deliveries_seen >= 0",
        name="counts_nonnegative",
    ),
    sa.CheckConstraint(
        "status IN ('running', 'completed', 'failed')",
        name="status_valid",
    ),
    sa.CheckConstraint(
        "(status = 'running' AND finished_at IS NULL) "
        "OR (status IN ('completed', 'failed') AND finished_at IS NOT NULL)",
        name="status_finished_at_consistent",
    ),
    sa.CheckConstraint(
        "error_code IS NULL OR status = 'failed'",
        name="error_only_on_failure",
    ),
    sa.UniqueConstraint(
        "source_sha256",
        "attempt_number",
        name="uq_legacy_import_runs_snapshot_attempt",
    ),
)

sa.Index(
    "uq_legacy_import_runs_completed_snapshot",
    legacy_import_runs.c.source_sha256,
    unique=True,
    postgresql_where=legacy_import_runs.c.status == "completed",
)

legacy_processed_messages = sa.Table(
    "legacy_processed_messages",
    metadata,
    sa.Column("id", sa.BigInteger(), sa.Identity(), primary_key=True),
    sa.Column(
        "first_import_run_id",
        UUID(as_uuid=True),
        sa.ForeignKey("legacy_import_runs.id", ondelete="RESTRICT"),
        nullable=False,
    ),
    sa.Column("legacy_lead_id", sa.BigInteger(), nullable=False),
    sa.Column("source_key", sa.Text(), nullable=False),
    sa.Column("telegram_message_id", sa.BigInteger(), nullable=False),
    sa.Column("state", sa.String(16), nullable=False),
    sa.Column("processed_at", sa.DateTime(timezone=True)),
    sa.Column("legacy_created_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column(
        "imported_at",
        sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.func.now(),
    ),
    sa.CheckConstraint("legacy_lead_id > 0", name="legacy_lead_id_positive"),
    sa.CheckConstraint("source_key <> ''", name="source_key_nonempty"),
    sa.CheckConstraint("telegram_message_id > 0", name="telegram_message_id_positive"),
    sa.CheckConstraint("state IN ('pending', 'processed')", name="state_valid"),
    sa.CheckConstraint(
        "(state = 'processed' AND processed_at IS NOT NULL) "
        "OR (state = 'pending' AND processed_at IS NULL)",
        name="state_processed_at_consistent",
    ),
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

legacy_recipient_deliveries = sa.Table(
    "legacy_recipient_deliveries",
    metadata,
    sa.Column("id", sa.BigInteger(), sa.Identity(), primary_key=True),
    sa.Column(
        "legacy_processed_message_id",
        sa.BigInteger(),
        sa.ForeignKey("legacy_processed_messages.id", ondelete="RESTRICT"),
        nullable=False,
    ),
    sa.Column(
        "subscriber_id",
        sa.BigInteger(),
        sa.ForeignKey("subscribers.id", ondelete="RESTRICT"),
        nullable=False,
    ),
    sa.Column("state", sa.String(16), nullable=False),
    sa.Column("telegram_message_id", sa.BigInteger()),
    sa.Column("attempt_count", sa.Integer(), nullable=False, server_default="0"),
    sa.Column("error_code", sa.String(64)),
    sa.Column("sent_at", sa.DateTime(timezone=True)),
    sa.Column(
        "created_at",
        sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.func.now(),
    ),
    sa.Column(
        "updated_at",
        sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.func.now(),
    ),
    sa.CheckConstraint(
        "state IN ('pending', 'sent', 'failed', 'unknown')",
        name="state_valid",
    ),
    sa.CheckConstraint("attempt_count >= 0", name="attempt_count_nonnegative"),
    sa.CheckConstraint(
        "state <> 'sent' OR (telegram_message_id IS NOT NULL AND sent_at IS NOT NULL)",
        name="sent_metadata_present",
    ),
    sa.UniqueConstraint(
        "legacy_processed_message_id",
        "subscriber_id",
        name="uq_legacy_recipient_deliveries_message_subscriber",
    ),
)

sa.Index(
    "uq_legacy_recipient_deliveries_subscriber_telegram_message",
    legacy_recipient_deliveries.c.subscriber_id,
    legacy_recipient_deliveries.c.telegram_message_id,
    unique=True,
    postgresql_where=legacy_recipient_deliveries.c.telegram_message_id.is_not(None),
)

durable_jobs = sa.Table(
    "durable_jobs",
    metadata,
    sa.Column("id", UUID(as_uuid=True), primary_key=True),
    sa.Column("job_type", sa.String(100), nullable=False),
    sa.Column("idempotency_key", sa.String(255), nullable=False),
    sa.Column("state", sa.String(16), nullable=False, server_default="queued"),
    sa.Column("attempt_count", sa.Integer(), nullable=False, server_default="0"),
    sa.Column("max_attempts", sa.Integer(), nullable=False, server_default="3"),
    sa.Column(
        "available_at",
        sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.func.now(),
    ),
    sa.Column("claimed_at", sa.DateTime(timezone=True)),
    sa.Column("lease_owner", sa.String(128)),
    sa.Column("lease_expires_at", sa.DateTime(timezone=True)),
    sa.Column("correlation_id", UUID(as_uuid=True), nullable=False),
    sa.Column("failure_code", sa.String(64)),
    sa.Column(
        "created_at",
        sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.func.now(),
    ),
    sa.Column(
        "updated_at",
        sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.func.now(),
    ),
    sa.Column("completed_at", sa.DateTime(timezone=True)),
    sa.Column("failed_at", sa.DateTime(timezone=True)),
    sa.CheckConstraint("job_type <> ''", name="job_type_nonempty"),
    sa.CheckConstraint("idempotency_key <> ''", name="idempotency_key_nonempty"),
    sa.CheckConstraint("max_attempts > 0", name="max_attempts_positive"),
    sa.CheckConstraint(
        "attempt_count >= 0 AND attempt_count <= max_attempts",
        name="attempt_count_bounded",
    ),
    sa.CheckConstraint(
        "state IN ('queued', 'running', 'completed', 'failed')",
        name="state_valid",
    ),
    sa.CheckConstraint(
        "failure_code IS NULL OR failure_code ~ '^[A-Za-z][A-Za-z0-9_.-]{0,63}$'",
        name="failure_code_safe",
    ),
    sa.CheckConstraint(
        "(state = 'queued' AND claimed_at IS NULL AND lease_owner IS NULL "
        "AND lease_expires_at IS NULL AND completed_at IS NULL AND failed_at IS NULL) "
        "OR (state = 'running' AND attempt_count > 0 AND claimed_at IS NOT NULL "
        "AND lease_owner IS NOT NULL AND lease_owner <> '' AND lease_expires_at IS NOT NULL "
        "AND completed_at IS NULL AND failed_at IS NULL) "
        "OR (state = 'completed' AND attempt_count > 0 AND claimed_at IS NULL "
        "AND lease_owner IS NULL AND lease_expires_at IS NULL AND completed_at IS NOT NULL "
        "AND failed_at IS NULL AND failure_code IS NULL) "
        "OR (state = 'failed' AND attempt_count > 0 AND claimed_at IS NULL "
        "AND lease_owner IS NULL AND lease_expires_at IS NULL AND completed_at IS NULL "
        "AND failed_at IS NOT NULL AND failure_code IS NOT NULL)",
        name="state_fields_consistent",
    ),
    sa.UniqueConstraint(
        "job_type",
        "idempotency_key",
        name="uq_durable_jobs_type_idempotency_key",
    ),
)

sa.Index(
    "ix_durable_jobs_claimable",
    durable_jobs.c.available_at,
    durable_jobs.c.created_at,
    postgresql_where=durable_jobs.c.state == "queued",
)
sa.Index(
    "ix_durable_jobs_expired_lease",
    durable_jobs.c.lease_expires_at,
    postgresql_where=durable_jobs.c.state == "running",
)
sa.Index("ix_durable_jobs_correlation_id", durable_jobs.c.correlation_id)

sources = sa.Table(
    "sources",
    metadata,
    sa.Column("id", sa.BigInteger(), sa.Identity(), primary_key=True),
    sa.Column("platform", sa.String(32), nullable=False),
    sa.Column("external_id", sa.String(255), nullable=False),
    sa.Column("access_type", sa.String(16), nullable=False),
    sa.Column(
        "lifecycle_status",
        sa.String(20),
        nullable=False,
        server_default="candidate",
    ),
    sa.Column("display_name", sa.Text(), nullable=False),
    sa.Column("handle", sa.String(255)),
    sa.Column("canonical_url", sa.Text()),
    sa.Column(
        "created_at",
        sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.func.now(),
    ),
    sa.Column(
        "updated_at",
        sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.func.now(),
    ),
    sa.CheckConstraint(
        "platform = lower(platform) "
        "AND platform ~ '^[a-z][a-z0-9_-]{0,31}$'",
        name="platform_valid",
    ),
    sa.CheckConstraint(
        "external_id = btrim(external_id) AND external_id <> ''",
        name="external_id_nonempty",
    ),
    sa.CheckConstraint(
        "access_type IN ('public', 'private')",
        name="access_type_valid",
    ),
    sa.CheckConstraint(
        "lifecycle_status IN "
        "('candidate', 'approved', 'active', 'degraded', 'paused', 'rejected', "
        "'needs_review', 'review_required', 'retired')",
        name="lifecycle_status_valid",
    ),
    sa.CheckConstraint(
        "display_name = btrim(display_name) AND display_name <> ''",
        name="display_name_nonempty",
    ),
    sa.CheckConstraint(
        "handle IS NULL OR (handle = lower(handle) AND handle = btrim(handle) "
        "AND handle <> '')",
        name="handle_normalized",
    ),
    sa.CheckConstraint(
        "canonical_url IS NULL OR (canonical_url = btrim(canonical_url) "
        "AND canonical_url <> '')",
        name="canonical_url_valid",
    ),
    sa.UniqueConstraint(
        "platform",
        "external_id",
        name="uq_sources_platform_external_id",
    ),
)

sa.Index(
    "ix_sources_lifecycle_status_platform",
    sources.c.lifecycle_status,
    sources.c.platform,
)
sa.Index(
    "uq_sources_platform_handle",
    sources.c.platform,
    sources.c.handle,
    unique=True,
    postgresql_where=sources.c.handle.is_not(None),
)

collector_accounts = sa.Table(
    "collector_accounts",
    metadata,
    sa.Column("id", sa.BigInteger(), sa.Identity(), primary_key=True),
    sa.Column("platform", sa.String(32), nullable=False),
    sa.Column("external_account_id", sa.String(255), nullable=False),
    sa.Column("display_name", sa.Text(), nullable=False),
    sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.true()),
    sa.Column(
        "created_at",
        sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.func.now(),
    ),
    sa.Column(
        "updated_at",
        sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.func.now(),
    ),
    sa.CheckConstraint(
        "platform = lower(platform) "
        "AND platform ~ '^[a-z][a-z0-9_-]{0,31}$'",
        name="platform_valid",
    ),
    sa.CheckConstraint(
        "external_account_id = btrim(external_account_id) "
        "AND external_account_id <> ''",
        name="external_account_id_nonempty",
    ),
    sa.CheckConstraint(
        "display_name = btrim(display_name) AND display_name <> ''",
        name="display_name_nonempty",
    ),
    sa.UniqueConstraint(
        "platform",
        "external_account_id",
        name="uq_collector_accounts_platform_external_account_id",
    ),
)

sa.Index(
    "ix_collector_accounts_platform_active",
    collector_accounts.c.platform,
    collector_accounts.c.is_active,
)

telegram_collector_operation_state = sa.Table(
    "telegram_collector_operation_state",
    metadata,
    sa.Column(
        "collector_account_id",
        sa.BigInteger(),
        sa.ForeignKey("collector_accounts.id", ondelete="RESTRICT"),
        primary_key=True,
    ),
    sa.Column("status", sa.String(16), nullable=False, server_default="ready"),
    sa.Column("active_request_token", UUID(as_uuid=True)),
    sa.Column("active_request_category", sa.String(64)),
    sa.Column("active_request_started_at", sa.DateTime(timezone=True)),
    sa.Column("active_request_lease_until", sa.DateTime(timezone=True)),
    sa.Column("last_request_at", sa.DateTime(timezone=True)),
    sa.Column("next_allowed_request_at", sa.DateTime(timezone=True)),
    sa.Column("cooldown_until", sa.DateTime(timezone=True)),
    sa.Column("last_request_category", sa.String(64)),
    sa.Column("last_floodwait_detected_at", sa.DateTime(timezone=True)),
    sa.Column("last_floodwait_seconds", sa.Integer()),
    sa.Column(
        "updated_at",
        sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.func.now(),
    ),
    sa.CheckConstraint(
        "status IN ('ready', 'pacing', 'floodwait', 'paused')",
        name="status_valid",
    ),
    sa.CheckConstraint(
        "(active_request_token IS NULL AND active_request_category IS NULL "
        "AND active_request_started_at IS NULL AND active_request_lease_until IS NULL) "
        "OR (active_request_token IS NOT NULL AND active_request_category IS NOT NULL "
        "AND active_request_started_at IS NOT NULL "
        "AND active_request_lease_until IS NOT NULL)",
        name="active_request_consistent",
    ),
    sa.CheckConstraint(
        "last_floodwait_seconds IS NULL OR last_floodwait_seconds > 0",
        name="last_floodwait_seconds_positive",
    ),
)

sa.Index(
    "ix_telegram_collector_operation_state_status_cooldown",
    telegram_collector_operation_state.c.status,
    telegram_collector_operation_state.c.cooldown_until,
)

telegram_collector_operation_events = sa.Table(
    "telegram_collector_operation_events",
    metadata,
    sa.Column("id", sa.BigInteger(), sa.Identity(), primary_key=True),
    sa.Column(
        "collector_account_id",
        sa.BigInteger(),
        sa.ForeignKey("collector_accounts.id", ondelete="RESTRICT"),
        nullable=False,
    ),
    sa.Column("request_token", UUID(as_uuid=True), nullable=False),
    sa.Column("request_category", sa.String(64), nullable=False),
    sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("finished_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("outcome", sa.String(16), nullable=False),
    sa.Column("floodwait_seconds", sa.Integer()),
    sa.Column(
        "created_at",
        sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.func.now(),
    ),
    sa.CheckConstraint(
        "request_category ~ '^[a-z][a-z0-9_.-]{0,63}$'",
        name="request_category_safe",
    ),
    sa.CheckConstraint(
        "outcome IN ('completed', 'error', 'floodwait')",
        name="outcome_valid",
    ),
    sa.CheckConstraint(
        "finished_at >= started_at",
        name="finished_after_started",
    ),
    sa.CheckConstraint(
        "(outcome = 'floodwait' AND floodwait_seconds IS NOT NULL "
        "AND floodwait_seconds > 0) OR "
        "(outcome <> 'floodwait' AND floodwait_seconds IS NULL)",
        name="floodwait_fields_consistent",
    ),
)

sa.Index(
    "ix_telegram_collector_operation_events_account_finished",
    telegram_collector_operation_events.c.collector_account_id,
    telegram_collector_operation_events.c.finished_at,
)

source_collector_access = sa.Table(
    "source_collector_access",
    metadata,
    sa.Column(
        "source_id",
        sa.BigInteger(),
        sa.ForeignKey("sources.id", ondelete="RESTRICT"),
        primary_key=True,
    ),
    sa.Column(
        "collector_account_id",
        sa.BigInteger(),
        sa.ForeignKey("collector_accounts.id", ondelete="RESTRICT"),
        primary_key=True,
    ),
    sa.Column("access_status", sa.String(16), nullable=False),
    sa.Column("checked_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("checked_by", sa.String(128), nullable=False),
    sa.Column(
        "created_at",
        sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.func.now(),
    ),
    sa.Column(
        "updated_at",
        sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.func.now(),
    ),
    sa.CheckConstraint(
        "access_status IN ('permitted', 'inaccessible', 'revoked')",
        name="access_status_valid",
    ),
    sa.CheckConstraint(
        "checked_by ~ '^[A-Za-z0-9][A-Za-z0-9_.:@-]{0,127}$'",
        name="checked_by_safe",
    ),
)

sa.Index(
    "ix_source_collector_access_account_status_source",
    source_collector_access.c.collector_account_id,
    source_collector_access.c.access_status,
    source_collector_access.c.source_id,
)

raw_messages = sa.Table(
    "raw_messages",
    metadata,
    sa.Column("id", UUID(as_uuid=True), primary_key=True),
    sa.Column(
        "source_id",
        sa.BigInteger(),
        sa.ForeignKey("sources.id", ondelete="RESTRICT"),
        nullable=False,
    ),
    sa.Column(
        "collector_account_id",
        sa.BigInteger(),
        sa.ForeignKey("collector_accounts.id", ondelete="RESTRICT"),
        nullable=False,
    ),
    sa.Column(
        "processing_job_id",
        UUID(as_uuid=True),
        sa.ForeignKey("durable_jobs.id", ondelete="RESTRICT"),
        nullable=False,
        unique=True,
    ),
    sa.Column("schema_version", sa.String(32), nullable=False),
    sa.Column("platform", sa.String(32), nullable=False),
    sa.Column("external_source_id", sa.String(255), nullable=False),
    sa.Column("external_message_id", sa.BigInteger(), nullable=False),
    sa.Column("message_date", sa.DateTime(timezone=True), nullable=False),
    sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("message_url", sa.Text(), nullable=False),
    sa.Column("content", sa.Text(), nullable=False),
    sa.Column("transport_metadata", JSONB(), nullable=False),
    sa.Column("ingestion_origin", sa.String(16), nullable=False),
    sa.Column("correlation_id", UUID(as_uuid=True), nullable=False),
    sa.Column(
        "created_at",
        sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.func.now(),
    ),
    sa.CheckConstraint(
        "schema_version ~ '^[a-z][a-z0-9_.-]{0,31}$'",
        name="schema_version_safe",
    ),
    sa.CheckConstraint(
        "platform = lower(platform) "
        "AND platform ~ '^[a-z][a-z0-9_-]{0,31}$'",
        name="platform_valid",
    ),
    sa.CheckConstraint(
        "external_source_id = btrim(external_source_id) "
        "AND external_source_id <> ''",
        name="external_source_id_nonempty",
    ),
    sa.CheckConstraint(
        "external_message_id > 0",
        name="external_message_id_positive",
    ),
    sa.CheckConstraint(
        "message_url = btrim(message_url) AND message_url <> ''",
        name="message_url_nonempty",
    ),
    sa.CheckConstraint(
        "jsonb_typeof(transport_metadata) = 'object'",
        name="transport_metadata_object",
    ),
    sa.CheckConstraint(
        "ingestion_origin IN ('live', 'catch_up')",
        name="ingestion_origin_valid",
    ),
    sa.UniqueConstraint(
        "source_id",
        "external_message_id",
        name="uq_raw_messages_source_message",
    ),
)

sa.Index(
    "ix_raw_messages_source_message_date",
    raw_messages.c.source_id,
    raw_messages.c.message_date,
)
sa.Index("ix_raw_messages_correlation_id", raw_messages.c.correlation_id)

message_prefilter_results = sa.Table(
    "message_prefilter_results",
    metadata,
    sa.Column("id", UUID(as_uuid=True), primary_key=True),
    sa.Column(
        "raw_message_id",
        UUID(as_uuid=True),
        sa.ForeignKey("raw_messages.id", ondelete="RESTRICT"),
        nullable=False,
    ),
    sa.Column(
        "parent_raw_message_id",
        UUID(as_uuid=True),
        sa.ForeignKey("raw_messages.id", ondelete="RESTRICT"),
    ),
    sa.Column(
        "analysis_job_id",
        UUID(as_uuid=True),
        sa.ForeignKey("durable_jobs.id", ondelete="RESTRICT"),
    ),
    sa.Column(
        "canonical_prefilter_result_id",
        UUID(as_uuid=True),
        sa.ForeignKey(
            "message_prefilter_results.id",
            name="fk_prefilter_result_canonical",
            ondelete="RESTRICT",
        ),
    ),
    sa.Column("schema_version", sa.String(32), nullable=False),
    sa.Column("decision", sa.String(16), nullable=False),
    sa.Column("reason_codes", JSONB(), nullable=False),
    sa.Column("normalized_content", sa.Text()),
    sa.Column("normalized_content_sha256", sa.String(64)),
    sa.Column("analysis_input_sha256", sa.String(64)),
    sa.Column("analyzer_version", sa.String(64)),
    sa.Column("analysis_schema_version", sa.String(32)),
    sa.Column("dedup_relation", sa.String(16)),
    sa.Column("dedup_window_seconds", sa.Integer()),
    sa.Column(
        "created_at",
        sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.func.now(),
    ),
    sa.CheckConstraint(
        "schema_version ~ '^[a-z][a-z0-9_.-]{0,31}$'",
        name="schema_version_safe",
    ),
    sa.CheckConstraint(
        "decision IN ('passed', 'rejected')",
        name="decision_valid",
    ),
    sa.CheckConstraint(
        "jsonb_typeof(reason_codes) = 'array'",
        name="reason_codes_array",
    ),
    sa.CheckConstraint(
        "(decision = 'passed' AND analysis_job_id IS NOT NULL "
        "AND jsonb_array_length(reason_codes) = 0 "
        "AND normalized_content IS NOT NULL "
        "AND normalized_content_sha256 IS NOT NULL "
        "AND analysis_input_sha256 IS NOT NULL "
        "AND analyzer_version IS NOT NULL "
        "AND analysis_schema_version IS NOT NULL "
        "AND dedup_window_seconds > 0 "
        "AND ((dedup_relation = 'canonical' "
        "AND canonical_prefilter_result_id IS NULL) "
        "OR (dedup_relation = 'exact_duplicate' "
        "AND canonical_prefilter_result_id IS NOT NULL))) "
        "OR (decision = 'rejected' AND analysis_job_id IS NULL "
        "AND parent_raw_message_id IS NULL "
        "AND canonical_prefilter_result_id IS NULL "
        "AND normalized_content IS NULL "
        "AND normalized_content_sha256 IS NULL "
        "AND analysis_input_sha256 IS NULL "
        "AND analyzer_version IS NULL "
        "AND analysis_schema_version IS NULL "
        "AND dedup_relation IS NULL "
        "AND dedup_window_seconds IS NULL "
        "AND jsonb_array_length(reason_codes) > 0)",
        name="outcome_consistent",
    ),
    sa.CheckConstraint(
        "parent_raw_message_id IS NULL OR parent_raw_message_id <> raw_message_id",
        name="parent_differs",
    ),
    sa.UniqueConstraint(
        "raw_message_id",
        "schema_version",
        name="uq_message_prefilter_results_raw_schema",
    ),
)

sa.Index(
    "ix_message_prefilter_results_decision_created_at",
    message_prefilter_results.c.decision,
    message_prefilter_results.c.created_at,
)
sa.Index(
    "ix_message_prefilter_results_exact_lookup",
    message_prefilter_results.c.normalized_content_sha256,
    message_prefilter_results.c.analysis_input_sha256,
    message_prefilter_results.c.analyzer_version,
    message_prefilter_results.c.analysis_schema_version,
    message_prefilter_results.c.created_at,
)
sa.Index(
    "uq_message_prefilter_results_canonical_analysis_job",
    message_prefilter_results.c.analysis_job_id,
    unique=True,
    postgresql_where=message_prefilter_results.c.dedup_relation == "canonical",
)

opportunity_analysis_cache = sa.Table(
    "opportunity_analysis_cache",
    metadata,
    sa.Column("id", UUID(as_uuid=True), primary_key=True),
    sa.Column("normalized_content", sa.Text(), nullable=False),
    sa.Column("normalized_content_sha256", sa.String(64), nullable=False),
    sa.Column("analysis_input_sha256", sa.String(64), nullable=False),
    sa.Column("analyzer_version", sa.String(64), nullable=False),
    sa.Column("analysis_schema_version", sa.String(32), nullable=False),
    sa.Column("result", JSONB(), nullable=False),
    sa.Column(
        "created_at",
        sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.func.now(),
    ),
    sa.CheckConstraint(
        "length(normalized_content_sha256) = 64 "
        "AND normalized_content_sha256 ~ '^[0-9a-f]{64}$'",
        name="content_hash_valid",
    ),
    sa.CheckConstraint(
        "length(analysis_input_sha256) = 64 "
        "AND analysis_input_sha256 ~ '^[0-9a-f]{64}$'",
        name="input_hash_valid",
    ),
    sa.CheckConstraint(
        "analyzer_version ~ '^[a-z0-9][a-z0-9_.-]{0,63}$'",
        name="analyzer_version_safe",
    ),
    sa.CheckConstraint(
        "analysis_schema_version ~ '^[a-z][a-z0-9_.-]{0,31}$'",
        name="analysis_schema_version_safe",
    ),
    sa.CheckConstraint(
        "jsonb_typeof(result) = 'object'",
        name="result_object",
    ),
    sa.UniqueConstraint(
        "normalized_content_sha256",
        "analysis_input_sha256",
        "analyzer_version",
        "analysis_schema_version",
        name="uq_opportunity_analysis_cache_compatible_input",
    ),
)

opportunities = sa.Table(
    "opportunities",
    metadata,
    sa.Column("id", UUID(as_uuid=True), primary_key=True),
    sa.Column("schema_version", sa.String(32), nullable=False),
    sa.Column("canonical_title", sa.Text()),
    sa.Column("task_summary", sa.Text()),
    sa.Column("market_direction", sa.String(32), nullable=False),
    sa.Column("intent_stage", sa.String(24), nullable=False),
    sa.Column("opportunity_type", sa.String(32), nullable=False),
    sa.Column("category", sa.Text()),
    sa.Column("role_title", sa.Text()),
    sa.Column("skills", JSONB(), nullable=False),
    sa.Column("budget_known", sa.Boolean(), nullable=False),
    sa.Column("budget_min", sa.Numeric(18, 4)),
    sa.Column("budget_max", sa.Numeric(18, 4)),
    sa.Column("budget_currency", sa.String(32)),
    sa.Column("budget_period", sa.String(32)),
    sa.Column("budget_explicit", sa.Boolean(), nullable=False),
    sa.Column("work_remote", sa.Boolean()),
    sa.Column("work_location", sa.Text()),
    sa.Column("work_full_time", sa.Boolean()),
    sa.Column("work_part_time", sa.Boolean()),
    sa.Column("language", sa.String(64)),
    sa.Column("contact_telegram", sa.Text()),
    sa.Column("contact_email", sa.Text()),
    sa.Column("contact_url", sa.Text()),
    sa.Column("analysis_confidence", sa.Numeric(5, 4), nullable=False),
    sa.Column("quality_actionability", sa.Numeric(5, 4), nullable=False),
    sa.Column("quality_commercial_plausibility", sa.Numeric(5, 4), nullable=False),
    sa.Column("quality_specificity", sa.Numeric(5, 4), nullable=False),
    sa.Column("quality_credibility", sa.Numeric(5, 4), nullable=False),
    sa.Column("red_flags", JSONB(), nullable=False),
    sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column(
        "lifecycle_status",
        sa.String(16),
        nullable=False,
        server_default="active",
    ),
    sa.Column(
        "lifecycle_changed_at",
        sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.func.now(),
    ),
    sa.Column(
        "preferred_raw_message_id",
        UUID(as_uuid=True),
        sa.ForeignKey("raw_messages.id", ondelete="RESTRICT"),
    ),
    sa.Column("preferred_source_policy_version", sa.String(64)),
    sa.Column(
        "created_at",
        sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.func.now(),
    ),
    sa.Column(
        "updated_at",
        sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.func.now(),
    ),
    sa.CheckConstraint(
        "schema_version ~ '^[a-z][a-z0-9_.-]{0,31}$'", name="schema_version_safe"
    ),
    sa.CheckConstraint(
        "canonical_title IS NULL OR (canonical_title = btrim(canonical_title) "
        "AND canonical_title <> '' AND length(canonical_title) <= 240)",
        name="canonical_title_valid",
    ),
    sa.CheckConstraint(
        "task_summary IS NULL OR (task_summary = btrim(task_summary) "
        "AND task_summary <> '' AND length(task_summary) <= 2000)",
        name="task_summary_valid",
    ),
    sa.CheckConstraint(
        "market_direction = 'buyer_to_specialist'", name="market_direction_valid"
    ),
    sa.CheckConstraint(
        "intent_stage IN ('active', 'recommendation', 'research', 'weak')",
        name="intent_stage_valid",
    ),
    sa.CheckConstraint(
        "opportunity_type IN ('one_off_order', 'project', 'vacancy', "
        "'part_time_contractor', 'consultation', 'unknown')",
        name="opportunity_type_valid",
    ),
    sa.CheckConstraint("jsonb_typeof(skills) = 'array'", name="skills_array"),
    sa.CheckConstraint(
        "(budget_known AND budget_explicit "
        "AND (budget_min IS NOT NULL OR budget_max IS NOT NULL)) "
        "OR (NOT budget_known AND budget_min IS NULL AND budget_max IS NULL "
        "AND budget_currency IS NULL AND budget_period IS NULL)",
        name="budget_known_consistent",
    ),
    sa.CheckConstraint(
        "(budget_min IS NULL OR budget_min >= 0) "
        "AND (budget_max IS NULL OR budget_max >= 0) "
        "AND (budget_min IS NULL OR budget_max IS NULL OR budget_min <= budget_max)",
        name="budget_amounts_valid",
    ),
    sa.CheckConstraint(
        "analysis_confidence BETWEEN 0 AND 1 "
        "AND quality_actionability BETWEEN 0 AND 1 "
        "AND quality_commercial_plausibility BETWEEN 0 AND 1 "
        "AND quality_specificity BETWEEN 0 AND 1 "
        "AND quality_credibility BETWEEN 0 AND 1",
        name="analysis_quality_bounded",
    ),
    sa.CheckConstraint("jsonb_typeof(red_flags) = 'array'", name="red_flags_array"),
    sa.CheckConstraint("first_seen_at <= last_seen_at", name="seen_window_valid"),
    sa.CheckConstraint(
        "lifecycle_status IN "
        "('active', 'stale', 'closed', 'retracted', 'suppressed')",
        name="lifecycle_status_valid",
    ),
    sa.CheckConstraint(
        "(preferred_raw_message_id IS NULL "
        "AND preferred_source_policy_version IS NULL) "
        "OR (preferred_raw_message_id IS NOT NULL "
        "AND preferred_source_policy_version IS NOT NULL "
        "AND preferred_source_policy_version "
        "~ '^[a-z0-9][a-z0-9_.-]{0,63}$')",
        name="preferred_source_consistent",
    ),
)

sa.Index("ix_opportunities_last_seen_at", opportunities.c.last_seen_at)
sa.Index(
    "ix_opportunities_preferred_raw_message_id",
    opportunities.c.preferred_raw_message_id,
)
sa.Index(
    "ix_opportunities_lifecycle_status_last_seen_at",
    opportunities.c.lifecycle_status,
    opportunities.c.last_seen_at,
)

opportunity_lifecycle_events = sa.Table(
    "opportunity_lifecycle_events",
    metadata,
    sa.Column("id", sa.BigInteger(), sa.Identity(), primary_key=True),
    sa.Column(
        "opportunity_id",
        UUID(as_uuid=True),
        sa.ForeignKey("opportunities.id", ondelete="RESTRICT"),
        nullable=False,
    ),
    sa.Column("from_status", sa.String(16)),
    sa.Column("to_status", sa.String(16), nullable=False),
    sa.Column(
        "evidence_raw_message_id",
        UUID(as_uuid=True),
        sa.ForeignKey("raw_messages.id", ondelete="RESTRICT"),
    ),
    sa.Column("actor_kind", sa.String(16), nullable=False),
    sa.Column("actor_id", sa.String(128)),
    sa.Column("reason", sa.Text(), nullable=False),
    sa.Column(
        "changed_at",
        sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.func.now(),
    ),
    sa.CheckConstraint(
        "from_status IS NULL OR from_status IN "
        "('active', 'stale', 'closed', 'retracted', 'suppressed')",
        name="from_status_valid",
    ),
    sa.CheckConstraint(
        "to_status IN "
        "('active', 'stale', 'closed', 'retracted', 'suppressed')",
        name="to_status_valid",
    ),
    sa.CheckConstraint(
        "(from_status IS NULL AND to_status = 'active') "
        "OR (from_status IS NOT NULL AND from_status <> to_status)",
        name="status_changed",
    ),
    sa.CheckConstraint(
        "actor_kind IN ('migration', 'system', 'operator')",
        name="actor_kind_valid",
    ),
    sa.CheckConstraint(
        "(actor_kind = 'operator' AND actor_id IS NOT NULL) "
        "OR (actor_kind <> 'operator' AND actor_id IS NULL)",
        name="actor_identity_consistent",
    ),
    sa.CheckConstraint(
        "actor_id IS NULL OR (actor_id = btrim(actor_id) AND actor_id <> '')",
        name="actor_id_valid",
    ),
    sa.CheckConstraint(
        "reason = btrim(reason) AND reason <> ''",
        name="reason_nonempty",
    ),
)

sa.Index(
    "ix_opportunity_lifecycle_events_opportunity_changed_at",
    opportunity_lifecycle_events.c.opportunity_id,
    opportunity_lifecycle_events.c.changed_at,
    opportunity_lifecycle_events.c.id,
)

opportunity_analysis_links = sa.Table(
    "opportunity_analysis_links",
    metadata,
    sa.Column(
        "analysis_cache_id",
        UUID(as_uuid=True),
        sa.ForeignKey("opportunity_analysis_cache.id", ondelete="RESTRICT"),
        primary_key=True,
    ),
    sa.Column(
        "opportunity_id",
        UUID(as_uuid=True),
        sa.ForeignKey("opportunities.id", ondelete="RESTRICT"),
        nullable=False,
    ),
    sa.Column("dedup_relation", sa.String(24), nullable=False),
    sa.Column("dedup_algorithm_version", sa.String(64), nullable=False),
    sa.Column("normalized_text_sha256", sa.String(64), nullable=False),
    sa.Column("dedup_similarity", sa.Numeric(6, 5)),
    sa.Column("dedup_window_seconds", sa.Integer(), nullable=False),
    sa.Column(
        "dedup_evidence",
        JSONB(),
        nullable=False,
        server_default=sa.text("'{}'::jsonb"),
    ),
    sa.Column(
        "matched_analysis_cache_id",
        UUID(as_uuid=True),
        sa.ForeignKey("opportunity_analysis_cache.id", ondelete="RESTRICT"),
    ),
    sa.Column(
        "linked_at",
        sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.func.now(),
    ),
    sa.CheckConstraint(
        "dedup_relation IN "
        "('canonical', 'exact_duplicate', 'near_duplicate', 'semantic_duplicate')",
        name="dedup_relation_valid",
    ),
    sa.CheckConstraint(
        "dedup_algorithm_version ~ '^[a-z0-9][a-z0-9_.-]{0,63}$'",
        name="dedup_algorithm_version_safe",
    ),
    sa.CheckConstraint(
        "length(normalized_text_sha256) = 64 "
        "AND normalized_text_sha256 ~ '^[0-9a-f]{64}$'",
        name="normalized_text_hash_valid",
    ),
    sa.CheckConstraint(
        "dedup_window_seconds > 0",
        name="dedup_window_positive",
    ),
    sa.CheckConstraint(
        "(dedup_relation = 'canonical' AND dedup_similarity IS NULL "
        "AND matched_analysis_cache_id IS NULL) "
        "OR (dedup_relation = 'exact_duplicate' AND dedup_similarity = 1 "
        "AND matched_analysis_cache_id IS NOT NULL) "
        "OR (dedup_relation = 'near_duplicate' "
        "AND dedup_similarity > 0 AND dedup_similarity < 1 "
        "AND matched_analysis_cache_id IS NOT NULL) "
        "OR (dedup_relation = 'semantic_duplicate' "
        "AND dedup_similarity > 0 AND dedup_similarity <= 1 "
        "AND matched_analysis_cache_id IS NOT NULL)",
        name="dedup_evidence_consistent",
    ),
    sa.CheckConstraint(
        "jsonb_typeof(dedup_evidence) = 'object'",
        name="dedup_evidence_object",
    ),
)

sa.Index(
    "ix_opportunity_analysis_links_opportunity_id",
    opportunity_analysis_links.c.opportunity_id,
)
sa.Index(
    "ix_opportunity_analysis_links_normalized_text_window",
    opportunity_analysis_links.c.normalized_text_sha256,
    opportunity_analysis_links.c.linked_at,
)

opportunity_source_messages = sa.Table(
    "opportunity_source_messages",
    metadata,
    sa.Column(
        "raw_message_id",
        UUID(as_uuid=True),
        sa.ForeignKey("raw_messages.id", ondelete="RESTRICT"),
        primary_key=True,
    ),
    sa.Column(
        "opportunity_id",
        UUID(as_uuid=True),
        sa.ForeignKey("opportunities.id", ondelete="RESTRICT"),
        nullable=False,
    ),
    sa.Column(
        "linked_at",
        sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.func.now(),
    ),
)

sa.Index(
    "ix_opportunity_source_messages_opportunity_id",
    opportunity_source_messages.c.opportunity_id,
)

ai_call_telemetry = sa.Table(
    "ai_call_telemetry",
    metadata,
    sa.Column("id", UUID(as_uuid=True), primary_key=True),
    sa.Column(
        "raw_message_id",
        UUID(as_uuid=True),
        sa.ForeignKey("raw_messages.id", ondelete="RESTRICT"),
        nullable=False,
    ),
    sa.Column("stage", sa.String(64), nullable=False),
    sa.Column("provider", sa.String(64), nullable=False),
    sa.Column("requested_model", sa.String(128), nullable=False),
    sa.Column("response_model", sa.String(128)),
    sa.Column("analyzer_version", sa.String(64), nullable=False),
    sa.Column("prompt_version", sa.String(100), nullable=False),
    sa.Column("schema_version", sa.String(32), nullable=False),
    sa.Column("routing_version", sa.String(100), nullable=False),
    sa.Column("route_reason", sa.String(64), nullable=False),
    sa.Column("provider_attempt", sa.Integer(), nullable=False),
    sa.Column("status", sa.String(24), nullable=False),
    sa.Column("pricing_version", sa.String(100), nullable=False),
    sa.Column("input_usd_per_million", sa.Numeric(18, 9), nullable=False),
    sa.Column("output_usd_per_million", sa.Numeric(18, 9), nullable=False),
    sa.Column("input_tokens", sa.BigInteger()),
    sa.Column("output_tokens", sa.BigInteger()),
    sa.Column("total_tokens", sa.BigInteger()),
    sa.Column("latency_ms", sa.BigInteger()),
    sa.Column("estimated_cost_usd", sa.Numeric(18, 9)),
    sa.Column("error_code", sa.String(64)),
    sa.Column(
        "started_at",
        sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.func.now(),
    ),
    sa.Column("finished_at", sa.DateTime(timezone=True)),
    sa.CheckConstraint(
        "stage ~ '^[a-z0-9][a-z0-9._-]{0,63}$'",
        name="stage_safe",
    ),
    sa.CheckConstraint(
        "provider ~ '^[a-z0-9][a-z0-9_-]{0,63}$'",
        name="provider_safe",
    ),
    sa.CheckConstraint(
        "provider_attempt BETWEEN 1 AND 5",
        name="provider_attempt_bounded",
    ),
    sa.CheckConstraint(
        "status IN ('started', 'succeeded', 'invalid_output', 'request_failed')",
        name="status_valid",
    ),
    sa.CheckConstraint(
        "input_usd_per_million >= 0 AND output_usd_per_million >= 0",
        name="prices_nonnegative",
    ),
    sa.CheckConstraint(
        "(input_tokens IS NULL OR input_tokens >= 0) "
        "AND (output_tokens IS NULL OR output_tokens >= 0) "
        "AND (total_tokens IS NULL OR total_tokens >= 0) "
        "AND (latency_ms IS NULL OR latency_ms >= 0) "
        "AND (estimated_cost_usd IS NULL OR estimated_cost_usd >= 0)",
        name="measurements_nonnegative",
    ),
    sa.CheckConstraint(
        "(status = 'started' AND finished_at IS NULL AND latency_ms IS NULL) OR "
        "(status <> 'started' AND finished_at IS NOT NULL AND latency_ms IS NOT NULL)",
        name="completion_consistent",
    ),
)

sa.Index(
    "ix_ai_call_telemetry_started_at_stage",
    ai_call_telemetry.c.started_at,
    ai_call_telemetry.c.stage,
)
sa.Index(
    "ix_ai_call_telemetry_raw_message_id",
    ai_call_telemetry.c.raw_message_id,
)

source_quality_snapshots = sa.Table(
    "source_quality_snapshots",
    metadata,
    sa.Column("id", sa.BigInteger(), sa.Identity(), primary_key=True),
    sa.Column(
        "source_id",
        sa.BigInteger(),
        sa.ForeignKey("sources.id", ondelete="RESTRICT"),
        nullable=False,
    ),
    sa.Column("audit_key", sa.String(255), nullable=False),
    sa.Column("audited_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("window_started_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("window_ended_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("sampled_message_count", sa.Integer(), nullable=False),
    sa.Column("opportunity_yield", sa.Numeric(8, 7), nullable=False),
    sa.Column("buyer_intent_ratio", sa.Numeric(8, 7), nullable=False),
    sa.Column("seller_ratio", sa.Numeric(8, 7), nullable=False),
    sa.Column("spam_ratio", sa.Numeric(8, 7), nullable=False),
    sa.Column("duplicate_ratio", sa.Numeric(8, 7), nullable=False),
    sa.Column(
        "created_at",
        sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.func.now(),
    ),
    sa.CheckConstraint(
        "audit_key = btrim(audit_key) AND audit_key <> ''",
        name="audit_key_nonempty",
    ),
    sa.CheckConstraint(
        "window_ended_at > window_started_at",
        name="window_valid",
    ),
    sa.CheckConstraint(
        "audited_at >= window_ended_at",
        name="audited_after_window",
    ),
    sa.CheckConstraint(
        "sampled_message_count > 0",
        name="sampled_message_count_positive",
    ),
    sa.CheckConstraint(
        "opportunity_yield BETWEEN 0 AND 1 "
        "AND buyer_intent_ratio BETWEEN 0 AND 1 "
        "AND seller_ratio BETWEEN 0 AND 1 "
        "AND spam_ratio BETWEEN 0 AND 1 "
        "AND duplicate_ratio BETWEEN 0 AND 1",
        name="metrics_unit_interval",
    ),
    sa.UniqueConstraint(
        "source_id",
        "audit_key",
        name="uq_source_quality_snapshots_source_audit_key",
    ),
)

sa.Index(
    "ix_source_quality_snapshots_source_audited_at",
    source_quality_snapshots.c.source_id,
    source_quality_snapshots.c.audited_at,
)

source_health = sa.Table(
    "source_health",
    metadata,
    sa.Column(
        "source_id",
        sa.BigInteger(),
        sa.ForeignKey("sources.id", ondelete="RESTRICT"),
        primary_key=True,
    ),
    sa.Column(
        "health_status",
        sa.String(16),
        nullable=False,
        server_default="unknown",
    ),
    sa.Column("last_message_at", sa.DateTime(timezone=True)),
    sa.Column("last_audited_at", sa.DateTime(timezone=True)),
    sa.Column("messages_per_day", sa.Numeric(14, 4)),
    sa.Column("opportunities_per_day", sa.Numeric(14, 4)),
    sa.Column("activity_observed_at", sa.DateTime(timezone=True)),
    sa.Column(
        "status_changed_at",
        sa.DateTime(timezone=True),
    ),
    sa.Column("degraded_at", sa.DateTime(timezone=True)),
    sa.Column("degradation_reason", sa.Text()),
    sa.Column(
        "updated_at",
        sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.func.now(),
    ),
    sa.CheckConstraint(
        "health_status IN ('unknown', 'healthy', 'degraded')",
        name="health_status_valid",
    ),
    sa.CheckConstraint(
        "messages_per_day IS NULL OR messages_per_day >= 0",
        name="messages_per_day_nonnegative",
    ),
    sa.CheckConstraint(
        "opportunities_per_day IS NULL OR opportunities_per_day >= 0",
        name="opportunities_per_day_nonnegative",
    ),
    sa.CheckConstraint(
        "(health_status = 'unknown' AND status_changed_at IS NULL) "
        "OR (health_status <> 'unknown' AND status_changed_at IS NOT NULL)",
        name="status_timestamp_consistent",
    ),
    sa.CheckConstraint(
        "(activity_observed_at IS NULL AND last_message_at IS NULL "
        "AND messages_per_day IS NULL AND opportunities_per_day IS NULL) "
        "OR (activity_observed_at IS NOT NULL "
        "AND (last_message_at IS NULL OR last_message_at <= activity_observed_at))",
        name="activity_observation_consistent",
    ),
    sa.CheckConstraint(
        "(health_status = 'degraded' AND degraded_at IS NOT NULL "
        "AND degraded_at = status_changed_at "
        "AND degradation_reason IS NOT NULL "
        "AND degradation_reason = btrim(degradation_reason) "
        "AND degradation_reason <> '') "
        "OR (health_status <> 'degraded' AND degraded_at IS NULL "
        "AND degradation_reason IS NULL)",
        name="degradation_state_consistent",
    ),
)

sa.Index(
    "ix_source_health_status_last_audited_at",
    source_health.c.health_status,
    source_health.c.last_audited_at,
)
sa.Index("ix_source_health_last_message_at", source_health.c.last_message_at)

source_audits = sa.Table(
    "source_audits",
    metadata,
    sa.Column("id", UUID(as_uuid=True), primary_key=True),
    sa.Column(
        "source_id",
        sa.BigInteger(),
        sa.ForeignKey("sources.id", ondelete="RESTRICT"),
        nullable=False,
    ),
    sa.Column("audit_key", sa.String(255), nullable=False),
    sa.Column("schema_version", sa.String(32), nullable=False),
    sa.Column("provider", sa.String(64), nullable=False),
    sa.Column("model", sa.String(128), nullable=False),
    sa.Column("analyzer_version", sa.String(64), nullable=False),
    sa.Column("audited_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("window_started_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("window_ended_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("sampled_from", sa.DateTime(timezone=True)),
    sa.Column("sampled_to", sa.DateTime(timezone=True)),
    sa.Column("sampled_message_count", sa.Integer(), nullable=False),
    sa.Column("probe_message_count", sa.Integer(), nullable=False),
    sa.Column("expanded", sa.Boolean(), nullable=False),
    sa.Column("high_volume", sa.Boolean(), nullable=False),
    sa.Column("sample_fingerprint", sa.String(64), nullable=False),
    sa.Column("commercial_opportunity_count", sa.Integer(), nullable=False),
    sa.Column("buyer_intent_count", sa.Integer(), nullable=False),
    sa.Column("seller_promotion_count", sa.Integer(), nullable=False),
    sa.Column("ads_spam_count", sa.Integer(), nullable=False),
    sa.Column("duplicate_count", sa.Integer(), nullable=False),
    sa.Column("content_mix", JSONB(), nullable=False),
    sa.Column("primary_language", sa.String(100)),
    sa.Column("languages", JSONB(), nullable=False),
    sa.Column("categories", JSONB(), nullable=False),
    sa.Column("decision_policy", JSONB(), nullable=False),
    sa.Column("decision", sa.String(16), nullable=False),
    sa.Column("reason_codes", JSONB(), nullable=False),
    sa.Column("reasons", JSONB(), nullable=False),
    sa.Column(
        "created_at",
        sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.func.now(),
    ),
    sa.CheckConstraint(
        "audit_key = btrim(audit_key) AND audit_key <> ''",
        name="audit_key_nonempty",
    ),
    sa.CheckConstraint(
        "schema_version ~ '^[a-z][a-z0-9_.-]{0,31}$'",
        name="schema_version_safe",
    ),
    sa.CheckConstraint(
        "provider = lower(provider) AND provider ~ '^[a-z][a-z0-9_-]{0,63}$'",
        name="provider_valid",
    ),
    sa.CheckConstraint("model = btrim(model) AND model <> ''", name="model_nonempty"),
    sa.CheckConstraint(
        "analyzer_version ~ '^[a-z0-9][a-z0-9_.-]{0,63}$'",
        name="analyzer_version_safe",
    ),
    sa.CheckConstraint(
        "audited_at >= window_ended_at AND window_ended_at > window_started_at",
        name="window_valid",
    ),
    sa.CheckConstraint(
        "sampled_message_count >= 0 AND probe_message_count >= sampled_message_count",
        name="sample_counts_valid",
    ),
    sa.CheckConstraint(
        "(sampled_message_count = 0 AND sampled_from IS NULL AND sampled_to IS NULL) "
        "OR (sampled_message_count > 0 AND sampled_from IS NOT NULL "
        "AND sampled_to IS NOT NULL AND sampled_from >= window_started_at "
        "AND sampled_to <= window_ended_at AND sampled_to >= sampled_from)",
        name="sample_range_valid",
    ),
    sa.CheckConstraint(
        "length(sample_fingerprint) = 64 "
        "AND sample_fingerprint ~ '^[0-9a-f]{64}$'",
        name="sample_fingerprint_valid",
    ),
    sa.CheckConstraint(
        "commercial_opportunity_count BETWEEN 0 AND sampled_message_count "
        "AND buyer_intent_count BETWEEN 0 AND sampled_message_count "
        "AND seller_promotion_count BETWEEN 0 AND sampled_message_count "
        "AND ads_spam_count BETWEEN 0 AND sampled_message_count "
        "AND duplicate_count BETWEEN 0 AND sampled_message_count",
        name="classification_counts_valid",
    ),
    sa.CheckConstraint(
        "primary_language IS NULL OR "
        "(primary_language = lower(primary_language) "
        "AND primary_language ~ '^[a-z0-9][a-z0-9._:-]{0,99}$')",
        name="primary_language_valid",
    ),
    sa.CheckConstraint(
        "decision IN ('approved', 'rejected', 'needs_review')",
        name="decision_valid",
    ),
    sa.UniqueConstraint(
        "source_id",
        "audit_key",
        name="uq_source_audits_source_audit_key",
    ),
)

sa.Index(
    "ix_source_audits_source_audited_at",
    source_audits.c.source_id,
    source_audits.c.audited_at,
)
sa.Index(
    "ix_source_audits_decision_audited_at",
    source_audits.c.decision,
    source_audits.c.audited_at,
)

discovery_runs = sa.Table(
    "discovery_runs",
    metadata,
    sa.Column("id", UUID(as_uuid=True), primary_key=True),
    sa.Column("provider", sa.String(64), nullable=False),
    sa.Column("provider_kind", sa.String(32), nullable=False),
    sa.Column("run_key", sa.String(255), nullable=False),
    sa.Column("request", JSONB(), nullable=False),
    sa.Column("status", sa.String(16), nullable=False),
    sa.Column("result_count", sa.Integer(), nullable=False, server_default="0"),
    sa.Column("materialized_count", sa.Integer(), nullable=False, server_default="0"),
    sa.Column("failure_code", sa.String(64)),
    sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("finished_at", sa.DateTime(timezone=True)),
    sa.Column(
        "created_at",
        sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.func.now(),
    ),
    sa.CheckConstraint(
        "provider = lower(provider) "
        "AND provider ~ '^[a-z][a-z0-9_-]{0,63}$'",
        name="provider_valid",
    ),
    sa.CheckConstraint(
        "provider_kind = lower(provider_kind) "
        "AND provider_kind ~ '^[a-z][a-z0-9_-]{0,31}$'",
        name="provider_kind_valid",
    ),
    sa.CheckConstraint(
        "run_key = btrim(run_key) AND run_key <> ''",
        name="run_key_nonempty",
    ),
    sa.CheckConstraint(
        "status IN ('running', 'completed', 'failed')",
        name="status_valid",
    ),
    sa.CheckConstraint(
        "result_count >= 0 AND materialized_count >= 0 "
        "AND materialized_count <= result_count",
        name="counts_valid",
    ),
    sa.CheckConstraint(
        "failure_code IS NULL OR failure_code ~ '^[a-z][a-z0-9_.-]{0,63}$'",
        name="failure_code_safe",
    ),
    sa.CheckConstraint(
        "(status = 'running' AND finished_at IS NULL AND failure_code IS NULL) "
        "OR (status = 'completed' AND finished_at IS NOT NULL "
        "AND failure_code IS NULL AND materialized_count = result_count) "
        "OR (status = 'failed' AND finished_at IS NOT NULL "
        "AND failure_code IS NOT NULL)",
        name="status_fields_consistent",
    ),
    sa.CheckConstraint(
        "finished_at IS NULL OR finished_at >= started_at",
        name="finished_after_started",
    ),
    sa.UniqueConstraint(
        "provider",
        "run_key",
        name="uq_discovery_runs_provider_run_key",
    ),
)

sa.Index(
    "ix_discovery_runs_status_started_at",
    discovery_runs.c.status,
    discovery_runs.c.started_at,
)

web_provider_health = sa.Table(
    "web_provider_health",
    metadata,
    sa.Column("provider", sa.String(64), nullable=False),
    sa.Column("backend", sa.String(64), nullable=False),
    sa.Column("state", sa.String(16), nullable=False),
    sa.Column("successful_searches", sa.Integer(), nullable=False, server_default="0"),
    sa.Column("http_403", sa.Integer(), nullable=False, server_default="0"),
    sa.Column("http_429", sa.Integer(), nullable=False, server_default="0"),
    sa.Column(
        "captcha_or_suspension",
        sa.Integer(),
        nullable=False,
        server_default="0",
    ),
    sa.Column("consecutive_failures", sa.Integer(), nullable=False, server_default="0"),
    sa.Column("last_failure_category", sa.String(64)),
    sa.Column("last_failure_at", sa.DateTime(timezone=True)),
    sa.Column("backoff_until", sa.DateTime(timezone=True)),
    sa.Column("last_success_at", sa.DateTime(timezone=True)),
    sa.Column(
        "updated_at",
        sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.func.now(),
    ),
    sa.CheckConstraint(
        "provider = lower(provider) AND provider ~ '^[a-z][a-z0-9_-]{0,63}$'",
        name="provider_valid",
    ),
    sa.CheckConstraint(
        "backend = lower(backend) AND backend ~ '^[a-z][a-z0-9_-]{0,63}$'",
        name="backend_valid",
    ),
    sa.CheckConstraint(
        "state IN ('READY', 'DEGRADED', 'BACKOFF', 'UNAVAILABLE')",
        name="state_valid",
    ),
    sa.CheckConstraint(
        "successful_searches >= 0 AND http_403 >= 0 AND http_429 >= 0 "
        "AND captcha_or_suspension >= 0 AND consecutive_failures >= 0",
        name="counts_valid",
    ),
    sa.CheckConstraint(
        "last_failure_category IS NULL OR "
        "last_failure_category ~ '^[a-z][a-z0-9_-]{0,63}$'",
        name="last_failure_category_valid",
    ),
    sa.PrimaryKeyConstraint("provider", "backend", name="pk_web_provider_health"),
)

sa.Index(
    "ix_web_provider_health_state_backoff",
    web_provider_health.c.state,
    web_provider_health.c.backoff_until,
)

source_discovery_lineage = sa.Table(
    "source_discovery_lineage",
    metadata,
    sa.Column("id", sa.BigInteger(), sa.Identity(), primary_key=True),
    sa.Column(
        "source_id",
        sa.BigInteger(),
        sa.ForeignKey("sources.id", ondelete="RESTRICT"),
        nullable=False,
    ),
    sa.Column("provider", sa.String(64), nullable=False),
    sa.Column("lineage_key", sa.String(255), nullable=False),
    sa.Column("provider_run_id", sa.String(255)),
    sa.Column(
        "discovery_run_id",
        UUID(as_uuid=True),
        sa.ForeignKey("discovery_runs.id", ondelete="RESTRICT"),
    ),
    sa.Column(
        "seed_source_id",
        sa.BigInteger(),
        sa.ForeignKey("sources.id", ondelete="RESTRICT"),
    ),
    sa.Column("seed_reference", sa.Text()),
    sa.Column(
        "discovered_at",
        sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.func.now(),
    ),
    sa.Column(
        "context",
        JSONB(),
        nullable=False,
        server_default=sa.text("'{}'::jsonb"),
    ),
    sa.Column(
        "created_at",
        sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.func.now(),
    ),
    sa.CheckConstraint(
        "provider = lower(provider) "
        "AND provider ~ '^[a-z][a-z0-9_-]{0,63}$'",
        name="provider_valid",
    ),
    sa.CheckConstraint(
        "lineage_key = btrim(lineage_key) AND lineage_key <> ''",
        name="lineage_key_nonempty",
    ),
    sa.CheckConstraint(
        "provider_run_id IS NULL OR "
        "(provider_run_id = btrim(provider_run_id) AND provider_run_id <> '')",
        name="provider_run_id_valid",
    ),
    sa.CheckConstraint(
        "seed_reference IS NULL OR "
        "(seed_reference = btrim(seed_reference) AND seed_reference <> '')",
        name="seed_reference_valid",
    ),
    sa.CheckConstraint(
        "seed_source_id IS NULL OR seed_source_id <> source_id",
        name="seed_source_not_self",
    ),
    sa.UniqueConstraint(
        "source_id",
        "provider",
        "lineage_key",
        name="uq_source_discovery_lineage_source_provider_key",
    ),
)

sa.Index(
    "ix_source_discovery_lineage_provider_run_id",
    source_discovery_lineage.c.provider,
    source_discovery_lineage.c.provider_run_id,
)
sa.Index(
    "ix_source_discovery_lineage_discovery_run_id",
    source_discovery_lineage.c.discovery_run_id,
)
sa.Index(
    "ix_source_discovery_lineage_source_discovered_at",
    source_discovery_lineage.c.source_id,
    source_discovery_lineage.c.discovered_at,
)

source_lifecycle_events = sa.Table(
    "source_lifecycle_events",
    metadata,
    sa.Column("id", sa.BigInteger(), sa.Identity(), primary_key=True),
    sa.Column(
        "source_id",
        sa.BigInteger(),
        sa.ForeignKey("sources.id", ondelete="RESTRICT"),
        nullable=False,
    ),
    sa.Column("from_status", sa.String(20)),
    sa.Column("to_status", sa.String(20), nullable=False),
    sa.Column("actor_kind", sa.String(16), nullable=False),
    sa.Column("actor_id", sa.String(128)),
    sa.Column("reason", sa.Text(), nullable=False),
    sa.Column("is_override", sa.Boolean(), nullable=False, server_default=sa.false()),
    sa.Column(
        "source_audit_id",
        UUID(as_uuid=True),
        sa.ForeignKey("source_audits.id", ondelete="RESTRICT"),
    ),
    sa.Column(
        "changed_at",
        sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.func.now(),
    ),
    sa.CheckConstraint(
        "from_status IS NULL OR from_status IN "
        "('candidate', 'approved', 'active', 'degraded', 'paused', 'rejected', "
        "'needs_review', 'review_required', 'retired')",
        name="from_status_valid",
    ),
    sa.CheckConstraint(
        "to_status IN "
        "('candidate', 'approved', 'active', 'degraded', 'paused', 'rejected', "
        "'needs_review', 'review_required', 'retired')",
        name="to_status_valid",
    ),
    sa.CheckConstraint(
        "from_status IS NULL OR from_status <> to_status",
        name="status_changed",
    ),
    sa.CheckConstraint(
        "actor_kind IN ('seed', 'system', 'operator')",
        name="actor_kind_valid",
    ),
    sa.CheckConstraint(
        "actor_id IS NULL OR (actor_id = btrim(actor_id) AND actor_id <> '')",
        name="actor_id_valid",
    ),
    sa.CheckConstraint(
        "actor_kind <> 'operator' OR actor_id IS NOT NULL",
        name="operator_actor_present",
    ),
    sa.CheckConstraint(
        "NOT is_override OR actor_kind = 'operator'",
        name="override_is_operator",
    ),
    sa.CheckConstraint(
        "source_audit_id IS NULL OR (actor_kind = 'system' AND NOT is_override)",
        name="audit_is_system",
    ),
    sa.CheckConstraint(
        "reason = btrim(reason) AND reason <> ''",
        name="reason_nonempty",
    ),
)

sa.Index(
    "ix_source_lifecycle_events_source_changed_at",
    source_lifecycle_events.c.source_id,
    source_lifecycle_events.c.changed_at,
)
sa.Index(
    "uq_source_lifecycle_events_source_audit_id",
    source_lifecycle_events.c.source_audit_id,
    unique=True,
    postgresql_where=source_lifecycle_events.c.source_audit_id.is_not(None),
)

discovery_results = sa.Table(
    "discovery_results",
    metadata,
    sa.Column("id", sa.BigInteger(), sa.Identity(), primary_key=True),
    sa.Column(
        "run_id",
        UUID(as_uuid=True),
        sa.ForeignKey("discovery_runs.id", ondelete="RESTRICT"),
        nullable=False,
    ),
    sa.Column("provider_result_key", sa.String(255), nullable=False),
    sa.Column(
        "source_id",
        sa.BigInteger(),
        sa.ForeignKey("sources.id", ondelete="RESTRICT"),
        nullable=False,
    ),
    sa.Column("outcome", sa.String(16), nullable=False),
    sa.Column("platform", sa.String(32), nullable=False),
    sa.Column("external_id", sa.String(255), nullable=False),
    sa.Column("access_type", sa.String(16), nullable=False),
    sa.Column("display_name", sa.Text(), nullable=False),
    sa.Column("handle", sa.String(255)),
    sa.Column("canonical_url", sa.Text()),
    sa.Column("discovered_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column(
        "seed_source_id",
        sa.BigInteger(),
        sa.ForeignKey("sources.id", ondelete="RESTRICT"),
    ),
    sa.Column("seed_reference", sa.Text()),
    sa.Column("context", JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
    sa.Column(
        "created_at",
        sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.func.now(),
    ),
    sa.CheckConstraint(
        "provider_result_key = btrim(provider_result_key) "
        "AND provider_result_key <> ''",
        name="provider_result_key_nonempty",
    ),
    sa.CheckConstraint(
        "outcome IN ('created', 'existing')",
        name="outcome_valid",
    ),
    sa.CheckConstraint(
        "platform = lower(platform) "
        "AND platform ~ '^[a-z][a-z0-9_-]{0,31}$'",
        name="platform_valid",
    ),
    sa.CheckConstraint(
        "external_id = btrim(external_id) AND external_id <> ''",
        name="external_id_nonempty",
    ),
    sa.CheckConstraint(
        "access_type IN ('public', 'private')",
        name="access_type_valid",
    ),
    sa.CheckConstraint(
        "display_name = btrim(display_name) AND display_name <> ''",
        name="display_name_nonempty",
    ),
    sa.CheckConstraint(
        "handle IS NULL OR (handle = lower(handle) AND handle = btrim(handle) "
        "AND handle <> '')",
        name="handle_normalized",
    ),
    sa.CheckConstraint(
        "canonical_url IS NULL OR (canonical_url = btrim(canonical_url) "
        "AND canonical_url <> '')",
        name="canonical_url_valid",
    ),
    sa.CheckConstraint(
        "seed_reference IS NULL OR (seed_reference = btrim(seed_reference) "
        "AND seed_reference <> '')",
        name="seed_reference_valid",
    ),
    sa.UniqueConstraint(
        "run_id",
        "provider_result_key",
        name="uq_discovery_results_run_provider_result_key",
    ),
)

sa.Index(
    "ix_discovery_results_source_id_run_id",
    discovery_results.c.source_id,
    discovery_results.c.run_id,
)

source_taxonomy_terms = sa.Table(
    "source_taxonomy_terms",
    metadata,
    sa.Column("id", sa.BigInteger(), sa.Identity(), primary_key=True),
    sa.Column("dimension", sa.String(32), nullable=False),
    sa.Column("key", sa.String(100), nullable=False),
    sa.Column("display_name", sa.Text(), nullable=False),
    sa.Column(
        "created_at",
        sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.func.now(),
    ),
    sa.Column(
        "updated_at",
        sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.func.now(),
    ),
    sa.CheckConstraint(
        "dimension ~ '^[a-z][a-z0-9_]{0,31}$'",
        name="dimension_valid",
    ),
    sa.CheckConstraint(
        "key ~ '^[a-z0-9][a-z0-9._:-]{0,99}$'",
        name="key_valid",
    ),
    sa.CheckConstraint(
        "display_name = btrim(display_name) AND display_name <> ''",
        name="display_name_nonempty",
    ),
    sa.UniqueConstraint(
        "dimension",
        "key",
        name="uq_source_taxonomy_terms_dimension_key",
    ),
)

source_taxonomy_assignments = sa.Table(
    "source_taxonomy_assignments",
    metadata,
    sa.Column(
        "source_id",
        sa.BigInteger(),
        sa.ForeignKey("sources.id", ondelete="RESTRICT"),
        primary_key=True,
    ),
    sa.Column(
        "term_id",
        sa.BigInteger(),
        sa.ForeignKey("source_taxonomy_terms.id", ondelete="RESTRICT"),
        primary_key=True,
    ),
    sa.Column(
        "created_at",
        sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.func.now(),
    ),
)

sa.Index(
    "ix_source_taxonomy_assignments_term_id_source_id",
    source_taxonomy_assignments.c.term_id,
    source_taxonomy_assignments.c.source_id,
)

match_evaluation_runs = sa.Table(
    "match_evaluation_runs",
    metadata,
    sa.Column("id", UUID(as_uuid=True), primary_key=True),
    sa.Column("idempotency_key", sa.String(64), nullable=False),
    sa.Column("schema_version", sa.String(64), nullable=False),
    sa.Column("algorithm_version", sa.String(64), nullable=False),
    sa.Column("policy_version", sa.String(64), nullable=False),
    sa.Column("policy_config", JSONB(), nullable=False),
    sa.Column("evaluated_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("trace_count", sa.Integer(), nullable=False),
    sa.Column(
        "created_at",
        sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.func.now(),
    ),
    sa.CheckConstraint(
        "idempotency_key ~ '^[0-9a-f]{64}$'",
        name="idempotency_key_sha256",
    ),
    sa.CheckConstraint(
        "schema_version ~ '^[a-z0-9][a-z0-9._-]{0,63}$' "
        "AND algorithm_version ~ '^[a-z0-9][a-z0-9._-]{0,63}$' "
        "AND policy_version ~ '^[a-z0-9][a-z0-9._-]{0,63}$'",
        name="versions_valid",
    ),
    sa.CheckConstraint(
        "jsonb_typeof(policy_config) = 'object'",
        name="policy_config_object",
    ),
    sa.CheckConstraint("trace_count >= 0", name="trace_count_nonnegative"),
    sa.UniqueConstraint(
        "idempotency_key",
        name="uq_match_evaluation_runs_idempotency_key",
    ),
)

sa.Index(
    "ix_match_evaluation_runs_evaluated_at",
    match_evaluation_runs.c.evaluated_at,
    match_evaluation_runs.c.id,
)

match_traces = sa.Table(
    "match_traces",
    metadata,
    sa.Column("id", UUID(as_uuid=True), primary_key=True),
    sa.Column(
        "run_id",
        UUID(as_uuid=True),
        sa.ForeignKey("match_evaluation_runs.id", ondelete="RESTRICT"),
        nullable=False,
    ),
    sa.Column(
        "opportunity_id",
        UUID(as_uuid=True),
        sa.ForeignKey("opportunities.id", ondelete="RESTRICT"),
        nullable=False,
    ),
    sa.Column(
        "search_profile_id",
        UUID(as_uuid=True),
        sa.ForeignKey("search_profiles.id", ondelete="RESTRICT"),
        nullable=False,
    ),
    sa.Column("profile_revision", sa.Integer(), nullable=False),
    sa.Column("profile_schema_version", sa.String(64), nullable=False),
    sa.Column("preferences_schema_version", sa.String(64), nullable=False),
    sa.Column("input_sha256", sa.String(64), nullable=False),
    sa.Column("opportunity_lifecycle_status", sa.String(16), nullable=False),
    sa.Column("opportunity_last_seen_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("filter_version", sa.String(64), nullable=False),
    sa.Column("hard_filter_eligible", sa.Boolean(), nullable=False),
    sa.Column("hard_filter_reasons", JSONB(), nullable=False),
    sa.Column("nonblocking_unknowns", JSONB(), nullable=False),
    sa.Column("structured_scoring_version", sa.String(64)),
    sa.Column("structured_policy_version", sa.String(64)),
    sa.Column("structured_components", JSONB(), nullable=False),
    sa.Column("user_relevance_score", sa.Numeric(6, 5)),
    sa.Column("structured_score", sa.Numeric(6, 5)),
    sa.Column("semantic_matching_version", sa.String(64)),
    sa.Column("semantic_policy_version", sa.String(64)),
    sa.Column("semantic_status", sa.String(24), nullable=False),
    sa.Column("semantic_degraded_reason", sa.String(64)),
    sa.Column("semantic_similarity", sa.Numeric(6, 5)),
    sa.Column("semantic_provider", sa.String(64)),
    sa.Column("semantic_model", sa.String(128)),
    sa.Column("semantic_model_version", sa.String(64)),
    sa.Column("opportunity_representation_sha256", sa.String(64)),
    sa.Column("profile_representation_sha256", sa.String(64)),
    sa.Column("combined_relevance_score", sa.Numeric(6, 5)),
    sa.Column("opportunity_quality_score", sa.Numeric(6, 5)),
    sa.Column("source_quality_score", sa.Numeric(6, 5)),
    sa.Column(
        "source_quality_snapshot_id",
        sa.BigInteger(),
        sa.ForeignKey("source_quality_snapshots.id", ondelete="RESTRICT"),
    ),
    sa.Column("red_flag_penalty", sa.Numeric(6, 5)),
    sa.Column("base_combined_score", sa.Numeric(6, 5)),
    sa.Column("freshness_age_seconds", sa.BigInteger(), nullable=False),
    sa.Column("freshness_score", sa.Numeric(6, 5), nullable=False),
    sa.Column("final_rank_score", sa.Numeric(6, 5)),
    sa.Column("minimum_relevance_threshold", sa.Numeric(6, 5), nullable=False),
    sa.Column("minimum_rank_score_threshold", sa.Numeric(6, 5), nullable=False),
    sa.Column("decision_code", sa.String(40), nullable=False),
    sa.Column("eligible", sa.Boolean(), nullable=False),
    sa.Column("rank", sa.Integer()),
    sa.Column("decision_schema_version", sa.String(64), nullable=False),
    sa.Column("decision_algorithm_version", sa.String(64), nullable=False),
    sa.Column("decision_policy_version", sa.String(64), nullable=False),
    sa.Column("evaluated_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column(
        "created_at",
        sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.func.now(),
    ),
    sa.CheckConstraint("profile_revision >= 1", name="profile_revision_valid"),
    sa.CheckConstraint("input_sha256 ~ '^[0-9a-f]{64}$'", name="input_sha256_valid"),
    sa.CheckConstraint(
        "jsonb_typeof(hard_filter_reasons) = 'array' "
        "AND jsonb_typeof(nonblocking_unknowns) = 'array' "
        "AND jsonb_typeof(structured_components) = 'array'",
        name="trace_arrays_valid",
    ),
    sa.CheckConstraint(
        "semantic_status IN ('available', 'degraded', 'unavailable_input')",
        name="semantic_status_valid",
    ),
    sa.CheckConstraint(
        "decision_code IN ('eligible', 'hard_rejected', 'freshness_expired', "
        "'below_relevance_threshold', 'below_rank_score_threshold')",
        name="decision_code_valid",
    ),
    sa.CheckConstraint(
        "freshness_age_seconds >= 0 AND freshness_score BETWEEN 0 AND 1 "
        "AND minimum_relevance_threshold BETWEEN 0 AND 1 "
        "AND minimum_rank_score_threshold BETWEEN 0 AND 1",
        name="freshness_thresholds_valid",
    ),
    sa.CheckConstraint(
        "(eligible AND decision_code = 'eligible' AND rank IS NOT NULL "
        "AND rank >= 1 AND hard_filter_eligible AND final_rank_score IS NOT NULL) "
        "OR (NOT eligible AND decision_code <> 'eligible' AND rank IS NULL)",
        name="decision_rank_consistent",
    ),
    sa.CheckConstraint(
        "(hard_filter_eligible AND jsonb_array_length(hard_filter_reasons) = 0) "
        "OR (NOT hard_filter_eligible "
        "AND jsonb_array_length(hard_filter_reasons) > 0)",
        name="hard_filter_evidence_consistent",
    ),
    sa.UniqueConstraint(
        "run_id",
        "opportunity_id",
        "search_profile_id",
        "profile_revision",
        name="uq_match_traces_run_opportunity_profile_revision",
    ),
)

sa.Index(
    "ix_match_traces_profile_eligible_rank",
    match_traces.c.search_profile_id,
    match_traces.c.eligible,
    match_traces.c.rank,
)
sa.Index(
    "ix_match_traces_opportunity_profile_evaluated",
    match_traces.c.opportunity_id,
    match_traces.c.search_profile_id,
    match_traces.c.evaluated_at,
)

personalized_deliveries = sa.Table(
    "personalized_deliveries",
    metadata,
    sa.Column("id", UUID(as_uuid=True), primary_key=True),
    sa.Column("idempotency_key", sa.String(64), nullable=False),
    sa.Column("schema_version", sa.String(64), nullable=False),
    sa.Column("renderer_schema_version", sa.String(64), nullable=False),
    sa.Column(
        "match_trace_id",
        UUID(as_uuid=True),
        sa.ForeignKey("match_traces.id", ondelete="RESTRICT"),
        nullable=False,
    ),
    sa.Column(
        "match_run_id",
        UUID(as_uuid=True),
        sa.ForeignKey("match_evaluation_runs.id", ondelete="RESTRICT"),
        nullable=False,
    ),
    sa.Column(
        "opportunity_id",
        UUID(as_uuid=True),
        sa.ForeignKey("opportunities.id", ondelete="RESTRICT"),
        nullable=False,
    ),
    sa.Column(
        "search_profile_id",
        UUID(as_uuid=True),
        sa.ForeignKey("search_profiles.id", ondelete="RESTRICT"),
        nullable=False,
    ),
    sa.Column("profile_revision", sa.Integer(), nullable=False),
    sa.Column(
        "user_id",
        UUID(as_uuid=True),
        sa.ForeignKey("users.id", ondelete="RESTRICT"),
        nullable=False,
    ),
    sa.Column("recipient_platform", sa.String(32), nullable=False),
    sa.Column("recipient_external_user_id", sa.String(255), nullable=False),
    sa.Column(
        "job_id",
        UUID(as_uuid=True),
        sa.ForeignKey("durable_jobs.id", ondelete="RESTRICT"),
        nullable=False,
    ),
    sa.Column("status", sa.String(16), nullable=False, server_default="queued"),
    sa.Column("card_body_html", sa.Text(), nullable=False),
    sa.Column("source_url", sa.Text()),
    sa.Column("parse_mode", sa.String(16), nullable=False),
    sa.Column("link_preview", sa.Boolean(), nullable=False),
    sa.Column("rendered_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("attempt_count", sa.Integer(), nullable=False, server_default="0"),
    sa.Column("last_attempt_at", sa.DateTime(timezone=True)),
    sa.Column("failure_code", sa.String(64)),
    sa.Column("telegram_message_id", sa.BigInteger()),
    sa.Column("sent_at", sa.DateTime(timezone=True)),
    sa.Column(
        "created_at",
        sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.func.now(),
    ),
    sa.Column(
        "updated_at",
        sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.func.now(),
    ),
    sa.CheckConstraint(
        "idempotency_key ~ '^[0-9a-f]{64}$'",
        name="idempotency_key_sha256",
    ),
    sa.CheckConstraint(
        "schema_version ~ '^[a-z0-9][a-z0-9._-]{0,63}$' "
        "AND renderer_schema_version ~ '^[a-z0-9][a-z0-9._-]{0,63}$'",
        name="versions_valid",
    ),
    sa.CheckConstraint("profile_revision >= 1", name="profile_revision_valid"),
    sa.CheckConstraint(
        "recipient_platform = 'telegram' "
        "AND recipient_external_user_id ~ '^[1-9][0-9]{0,19}$'",
        name="recipient_valid",
    ),
    sa.CheckConstraint(
        "length(card_body_html) BETWEEN 1 AND 4096",
        name="card_body_bounded",
    ),
    sa.CheckConstraint(
        "source_url IS NULL OR length(source_url) <= 2048",
        name="source_url_bounded",
    ),
    sa.CheckConstraint("parse_mode = 'html'", name="parse_mode_html"),
    sa.CheckConstraint("attempt_count >= 0", name="attempt_count_nonnegative"),
    sa.CheckConstraint(
        "failure_code IS NULL OR "
        "failure_code ~ '^[A-Za-z][A-Za-z0-9_.-]{0,63}$'",
        name="failure_code_valid",
    ),
    sa.CheckConstraint(
        "(status = 'queued' AND sent_at IS NULL "
        "AND telegram_message_id IS NULL "
        "AND ((attempt_count = 0 AND last_attempt_at IS NULL "
        "AND failure_code IS NULL) OR (attempt_count > 0 "
        "AND last_attempt_at IS NOT NULL AND failure_code IS NOT NULL))) "
        "OR (status = 'sending' AND attempt_count > 0 "
        "AND last_attempt_at IS NOT NULL AND failure_code IS NULL "
        "AND sent_at IS NULL AND telegram_message_id IS NULL) "
        "OR (status = 'sent' AND attempt_count > 0 "
        "AND last_attempt_at IS NOT NULL AND failure_code IS NULL "
        "AND sent_at IS NOT NULL AND telegram_message_id > 0) "
        "OR (status IN ('failed', 'suppressed') AND attempt_count > 0 "
        "AND last_attempt_at IS NOT NULL AND failure_code IS NOT NULL "
        "AND sent_at IS NULL AND telegram_message_id IS NULL)",
        name="state_consistent",
    ),
    sa.UniqueConstraint(
        "idempotency_key",
        name="uq_personalized_deliveries_idempotency_key",
    ),
    sa.UniqueConstraint(
        "match_trace_id",
        "renderer_schema_version",
        name="uq_personalized_deliveries_trace_renderer",
    ),
    sa.UniqueConstraint("job_id", name="uq_personalized_deliveries_job_id"),
)

sa.Index(
    "ix_personalized_deliveries_status_created",
    personalized_deliveries.c.status,
    personalized_deliveries.c.created_at,
    personalized_deliveries.c.id,
)
sa.Index(
    "ix_personalized_deliveries_user_opportunity",
    personalized_deliveries.c.user_id,
    personalized_deliveries.c.opportunity_id,
    personalized_deliveries.c.search_profile_id,
)

delivery_action_events = sa.Table(
    "delivery_action_events",
    metadata,
    sa.Column("id", UUID(as_uuid=True), primary_key=True),
    sa.Column("idempotency_key", sa.String(64), nullable=False),
    sa.Column("schema_version", sa.String(64), nullable=False),
    sa.Column("action_type", sa.String(24), nullable=False),
    sa.Column(
        "delivery_id",
        UUID(as_uuid=True),
        sa.ForeignKey("personalized_deliveries.id", ondelete="RESTRICT"),
        nullable=False,
    ),
    sa.Column(
        "match_trace_id",
        UUID(as_uuid=True),
        sa.ForeignKey("match_traces.id", ondelete="RESTRICT"),
        nullable=False,
    ),
    sa.Column(
        "match_run_id",
        UUID(as_uuid=True),
        sa.ForeignKey("match_evaluation_runs.id", ondelete="RESTRICT"),
        nullable=False,
    ),
    sa.Column(
        "opportunity_id",
        UUID(as_uuid=True),
        sa.ForeignKey("opportunities.id", ondelete="RESTRICT"),
        nullable=False,
    ),
    sa.Column(
        "search_profile_id",
        UUID(as_uuid=True),
        sa.ForeignKey("search_profiles.id", ondelete="RESTRICT"),
        nullable=False,
    ),
    sa.Column("profile_revision", sa.Integer(), nullable=False),
    sa.Column(
        "user_id",
        UUID(as_uuid=True),
        sa.ForeignKey("users.id", ondelete="RESTRICT"),
        nullable=False,
    ),
    sa.Column(
        "source_id",
        sa.BigInteger(),
        sa.ForeignKey("sources.id", ondelete="RESTRICT"),
        nullable=False,
    ),
    sa.Column(
        "source_raw_message_id",
        UUID(as_uuid=True),
        sa.ForeignKey("raw_messages.id", ondelete="RESTRICT"),
        nullable=False,
    ),
    sa.Column("source_url", sa.Text(), nullable=False),
    sa.Column("actor_platform", sa.String(32), nullable=False),
    sa.Column("actor_external_user_id", sa.String(255), nullable=False),
    sa.Column(
        "created_at",
        sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.func.now(),
    ),
    sa.CheckConstraint(
        "idempotency_key ~ '^[0-9a-f]{64}$'",
        name="idempotency_key_sha256",
    ),
    sa.CheckConstraint(
        "schema_version ~ '^[a-z0-9][a-z0-9._-]{0,63}$'",
        name="schema_version_valid",
    ),
    sa.CheckConstraint(
        "action_type IN ('open', 'not_suitable', 'got_job')",
        name="action_type_valid",
    ),
    sa.CheckConstraint("profile_revision >= 1", name="profile_revision_valid"),
    sa.CheckConstraint(
        "length(source_url) BETWEEN 1 AND 2048",
        name="source_url_bounded",
    ),
    sa.CheckConstraint(
        "actor_platform = 'telegram' "
        "AND actor_external_user_id ~ '^[1-9][0-9]{0,19}$'",
        name="actor_valid",
    ),
    sa.UniqueConstraint(
        "idempotency_key",
        name="uq_delivery_action_events_idempotency_key",
    ),
    sa.UniqueConstraint(
        "delivery_id",
        "action_type",
        name="uq_delivery_action_events_delivery_action",
    ),
)

sa.Index(
    "ix_delivery_action_events_user_created",
    delivery_action_events.c.user_id,
    delivery_action_events.c.created_at,
    delivery_action_events.c.id,
)
sa.Index(
    "ix_delivery_action_events_opportunity_action",
    delivery_action_events.c.opportunity_id,
    delivery_action_events.c.action_type,
    delivery_action_events.c.created_at,
)

feedback_events = sa.Table(
    "feedback_events",
    metadata,
    sa.Column("id", UUID(as_uuid=True), primary_key=True),
    sa.Column("schema_version", sa.String(64), nullable=False),
    sa.Column(
        "delivery_action_event_id",
        UUID(as_uuid=True),
        sa.ForeignKey("delivery_action_events.id", ondelete="RESTRICT"),
        nullable=False,
    ),
    sa.Column("feedback_type", sa.String(24), nullable=False),
    sa.Column("signal_scope", sa.String(32), nullable=False),
    sa.Column(
        "delivery_id",
        UUID(as_uuid=True),
        sa.ForeignKey("personalized_deliveries.id", ondelete="RESTRICT"),
        nullable=False,
    ),
    sa.Column(
        "match_trace_id",
        UUID(as_uuid=True),
        sa.ForeignKey("match_traces.id", ondelete="RESTRICT"),
        nullable=False,
    ),
    sa.Column(
        "match_run_id",
        UUID(as_uuid=True),
        sa.ForeignKey("match_evaluation_runs.id", ondelete="RESTRICT"),
        nullable=False,
    ),
    sa.Column(
        "opportunity_id",
        UUID(as_uuid=True),
        sa.ForeignKey("opportunities.id", ondelete="RESTRICT"),
        nullable=False,
    ),
    sa.Column("opportunity_type", sa.String(32), nullable=False),
    sa.Column(
        "search_profile_id",
        UUID(as_uuid=True),
        sa.ForeignKey("search_profiles.id", ondelete="RESTRICT"),
        nullable=False,
    ),
    sa.Column("profile_revision", sa.Integer(), nullable=False),
    sa.Column(
        "user_id",
        UUID(as_uuid=True),
        sa.ForeignKey("users.id", ondelete="RESTRICT"),
        nullable=False,
    ),
    sa.Column(
        "source_id",
        sa.BigInteger(),
        sa.ForeignKey("sources.id", ondelete="RESTRICT"),
        nullable=False,
    ),
    sa.Column(
        "source_raw_message_id",
        UUID(as_uuid=True),
        sa.ForeignKey("raw_messages.id", ondelete="RESTRICT"),
        nullable=False,
    ),
    sa.Column("source_url", sa.Text(), nullable=False),
    sa.Column("match_score", sa.Numeric(6, 5), nullable=False),
    sa.Column("match_score_version", sa.String(64), nullable=False),
    sa.Column("match_policy_version", sa.String(64), nullable=False),
    sa.Column("feedback_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column(
        "created_at",
        sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.func.now(),
    ),
    sa.CheckConstraint(
        "feedback_type IN ('not_suitable', 'got_job')",
        name="feedback_type_valid",
    ),
    sa.CheckConstraint(
        "schema_version ~ '^[a-z0-9][a-z0-9._-]{0,63}$'",
        name="schema_version_valid",
    ),
    sa.CheckConstraint(
        "(feedback_type = 'not_suitable' AND signal_scope = 'personal_match') "
        "OR (feedback_type = 'got_job' AND signal_scope = 'conversion')",
        name="signal_scope_consistent",
    ),
    sa.CheckConstraint(
        "opportunity_type IN ('one_off_order', 'project', 'vacancy', "
        "'part_time_contractor', 'consultation', 'unknown')",
        name="opportunity_type_valid",
    ),
    sa.CheckConstraint("profile_revision >= 1", name="profile_revision_valid"),
    sa.CheckConstraint("match_score BETWEEN 0 AND 1", name="match_score_valid"),
    sa.CheckConstraint(
        "match_score_version ~ '^[a-z0-9][a-z0-9._-]{0,63}$' "
        "AND match_policy_version ~ '^[a-z0-9][a-z0-9._-]{0,63}$'",
        name="versions_valid",
    ),
    sa.CheckConstraint(
        "length(source_url) BETWEEN 1 AND 2048",
        name="source_url_bounded",
    ),
    sa.UniqueConstraint(
        "delivery_action_event_id",
        name="uq_feedback_events_delivery_action_event_id",
    ),
)

sa.Index(
    "ix_feedback_events_source_feedback_at",
    feedback_events.c.source_id,
    feedback_events.c.feedback_at,
    feedback_events.c.id,
)
sa.Index(
    "ix_feedback_events_profile_feedback_at",
    feedback_events.c.search_profile_id,
    feedback_events.c.feedback_at,
    feedback_events.c.id,
)
sa.Index(
    "ix_feedback_events_opportunity_type_feedback_at",
    feedback_events.c.opportunity_type,
    feedback_events.c.feedback_at,
    feedback_events.c.id,
)

source_feedback_signals = sa.Table(
    "source_feedback_signals",
    metadata,
    sa.Column(
        "source_id",
        sa.BigInteger(),
        sa.ForeignKey("sources.id", ondelete="RESTRICT"),
        primary_key=True,
    ),
    sa.Column("signal_version", sa.String(64), nullable=False),
    sa.Column("feedback_count", sa.Integer(), nullable=False, server_default="0"),
    sa.Column(
        "not_suitable_count",
        sa.Integer(),
        nullable=False,
        server_default="0",
    ),
    sa.Column("got_job_count", sa.Integer(), nullable=False, server_default="0"),
    sa.Column("last_feedback_at", sa.DateTime(timezone=True)),
    sa.Column(
        "created_at",
        sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.func.now(),
    ),
    sa.Column(
        "updated_at",
        sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.func.now(),
    ),
    sa.CheckConstraint(
        "signal_version ~ '^[a-z0-9][a-z0-9._-]{0,63}$'",
        name="signal_version_valid",
    ),
    sa.CheckConstraint(
        "feedback_count >= 0 AND not_suitable_count >= 0 "
        "AND got_job_count >= 0",
        name="counts_nonnegative",
    ),
    sa.CheckConstraint(
        "feedback_count = not_suitable_count + got_job_count",
        name="counts_consistent",
    ),
    sa.CheckConstraint(
        "(feedback_count = 0 AND last_feedback_at IS NULL) "
        "OR (feedback_count > 0 AND last_feedback_at IS NOT NULL)",
        name="timestamp_consistent",
    ),
)

# Global Source Library additions.  These tables are intentionally separate
# from user/profile records: a campaign is reusable global work, aliases and
# discovery evidence are historical provenance, and monitoring is assigned per
# source/account rather than copied per SearchProfile.
discovery_campaigns = sa.Table(
    "discovery_campaigns",
    metadata,
    sa.Column("id", UUID(as_uuid=True), primary_key=True),
    sa.Column("campaign_key", sa.String(128), nullable=False),
    sa.Column("campaign_type", sa.String(32), nullable=False),
    sa.Column("status", sa.String(16), nullable=False, server_default="planned"),
    sa.Column("languages", JSONB(), nullable=False),
    sa.Column("geo_constraints", JSONB(), nullable=False),
    sa.Column("specialist_concepts", JSONB(), nullable=False),
    sa.Column("buyer_concepts", JSONB(), nullable=False),
    sa.Column("buyer_habitats", JSONB(), nullable=False),
    sa.Column("industry_contexts", JSONB(), nullable=False),
    sa.Column("query_strategy_version", sa.String(64), nullable=False),
    sa.Column("priority", sa.Integer(), nullable=False, server_default="50"),
    sa.Column("created_from", sa.String(32), nullable=False),
    sa.Column("budget", JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
    sa.Column("progress", JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    sa.Column("last_run_at", sa.DateTime(timezone=True)),
    sa.Column("next_run_at", sa.DateTime(timezone=True)),
    sa.Column("paused_at", sa.DateTime(timezone=True)),
    sa.Column("pause_reason", sa.Text()),
    sa.CheckConstraint(
        "campaign_key = btrim(campaign_key) AND campaign_key <> '' "
        "AND campaign_key ~ '^[a-z0-9][a-z0-9:-]{0,127}$'",
        name="campaign_key_valid",
    ),
    sa.CheckConstraint(
        "campaign_type IN ('bootstrap', 'profile_gap', 'source_graph_expansion', 'manual_operator')",
        name="campaign_type_valid",
    ),
    sa.CheckConstraint("status IN ('planned', 'running', 'paused', 'completed', 'failed')", name="status_valid"),
    sa.CheckConstraint("priority BETWEEN 0 AND 100", name="priority_valid"),
    sa.CheckConstraint("jsonb_typeof(languages) = 'array' AND jsonb_typeof(geo_constraints) = 'array'", name="language_geo_arrays"),
    sa.CheckConstraint(
        "jsonb_typeof(specialist_concepts) = 'array' AND jsonb_typeof(buyer_concepts) = 'array' "
        "AND jsonb_typeof(buyer_habitats) = 'array' AND jsonb_typeof(industry_contexts) = 'array'",
        name="concept_arrays",
    ),
    sa.CheckConstraint("jsonb_typeof(budget) = 'object' AND jsonb_typeof(progress) = 'object'", name="campaign_payloads_object"),
    sa.CheckConstraint(
        "(status = 'paused' AND paused_at IS NOT NULL AND pause_reason IS NOT NULL) "
        "OR (status <> 'paused' AND paused_at IS NULL AND pause_reason IS NULL)",
        name="pause_fields_consistent",
    ),
    sa.UniqueConstraint("campaign_key", name="uq_discovery_campaigns_campaign_key"),
)

sa.Index(
    "ix_discovery_campaigns_status_priority_next_run",
    discovery_campaigns.c.status,
    discovery_campaigns.c.priority,
    discovery_campaigns.c.next_run_at,
)

discovery_campaign_queries = sa.Table(
    "discovery_campaign_queries",
    metadata,
    sa.Column("id", UUID(as_uuid=True), primary_key=True),
    sa.Column("campaign_id", UUID(as_uuid=True), sa.ForeignKey("discovery_campaigns.id", ondelete="RESTRICT"), nullable=False),
    sa.Column("normalized_query_key", sa.String(255), nullable=False),
    sa.Column("query_sha256", sa.String(64), nullable=False),
    sa.Column("query_text", sa.String(2000), nullable=False),
    sa.Column("query_family", sa.String(40), nullable=False),
    sa.Column("language", sa.String(16), nullable=False),
    sa.Column("strategy_version", sa.String(64), nullable=False),
    sa.Column("status", sa.String(16), nullable=False, server_default="queued"),
    sa.Column("last_run_at", sa.DateTime(timezone=True)),
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    sa.CheckConstraint("normalized_query_key = btrim(normalized_query_key) AND normalized_query_key <> ''", name="normalized_key_valid"),
    sa.CheckConstraint("length(query_sha256) = 64 AND query_sha256 ~ '^[0-9a-f]{64}$'", name="query_hash_valid"),
    sa.CheckConstraint("query_text = btrim(query_text) AND query_text <> '' AND length(query_text) <= 2000", name="query_text_valid"),
    sa.CheckConstraint("query_family IN ('DIRECT_TELEGRAM_SOURCE', 'SITE_TELEGRAM', 'COMMUNITY_DIRECTORY', 'BUYER_HABITAT', 'HUB_LISTICLE', 'PROFILE_GAP')", name="query_family_valid"),
    sa.CheckConstraint("language ~ '^[a-z]{2,3}(?:-[a-z]{2})?$'", name="language_valid"),
    sa.CheckConstraint("status IN ('queued', 'running', 'completed', 'failed')", name="status_valid"),
    sa.UniqueConstraint("campaign_id", "normalized_query_key", name="uq_discovery_campaign_queries_campaign_key"),
)

sa.Index("ix_discovery_campaign_queries_status", discovery_campaign_queries.c.status, discovery_campaign_queries.c.updated_at)

discovery_campaign_profiles = sa.Table(
    "discovery_campaign_profiles",
    metadata,
    sa.Column("campaign_id", UUID(as_uuid=True), sa.ForeignKey("discovery_campaigns.id", ondelete="RESTRICT"), primary_key=True),
    sa.Column("search_profile_id", UUID(as_uuid=True), sa.ForeignKey("search_profiles.id", ondelete="RESTRICT"), primary_key=True),
    sa.Column("gap_key", sa.String(64), nullable=False),
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    sa.CheckConstraint("gap_key = btrim(gap_key) AND gap_key <> ''", name="gap_key_valid"),
)

source_reference_aliases = sa.Table(
    "source_reference_aliases",
    metadata,
    sa.Column("id", sa.BigInteger(), sa.Identity(), primary_key=True),
    sa.Column("source_id", sa.BigInteger(), sa.ForeignKey("sources.id", ondelete="RESTRICT"), nullable=False),
    sa.Column("platform", sa.String(32), nullable=False),
    sa.Column("normalized_reference", sa.String(255), nullable=False),
    sa.Column("reference_kind", sa.String(32), nullable=False),
    sa.Column("canonical_peer_identity", sa.String(255)),
    sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    sa.CheckConstraint("platform = lower(platform) AND platform <> ''", name="platform_valid"),
    sa.CheckConstraint("normalized_reference = btrim(normalized_reference) AND normalized_reference <> ''", name="reference_valid"),
    sa.CheckConstraint("reference_kind IN ('source', 'message', 'invite', 'username', 'numeric_peer')", name="reference_kind_valid"),
    sa.UniqueConstraint("platform", "normalized_reference", name="uq_source_reference_aliases_platform_reference"),
)

sa.Index("ix_source_reference_aliases_source", source_reference_aliases.c.source_id, source_reference_aliases.c.last_seen_at)
sa.Index("ix_source_reference_aliases_canonical_peer", source_reference_aliases.c.platform, source_reference_aliases.c.canonical_peer_identity)

source_discovery_evidence = sa.Table(
    "source_discovery_evidence",
    metadata,
    sa.Column("id", UUID(as_uuid=True), primary_key=True),
    sa.Column("source_id", sa.BigInteger(), sa.ForeignKey("sources.id", ondelete="RESTRICT"), nullable=False),
    sa.Column("campaign_id", UUID(as_uuid=True), sa.ForeignKey("discovery_campaigns.id", ondelete="RESTRICT")),
    sa.Column("discovery_run_id", UUID(as_uuid=True), sa.ForeignKey("discovery_runs.id", ondelete="RESTRICT")),
    sa.Column("provider", sa.String(64), nullable=False),
    sa.Column("provider_kind", sa.String(32), nullable=False),
    sa.Column("query_family", sa.String(40)),
    sa.Column("query_key", sa.String(255)),
    sa.Column("query_sha256", sa.String(64)),
    sa.Column("result_domain", sa.String(255)),
    sa.Column("extraction_kind", sa.String(32), nullable=False),
    sa.Column("independent_evidence_key", sa.String(255), nullable=False),
    sa.Column("profile_gap_keys", JSONB(), nullable=False, server_default=sa.text("'[]'::jsonb")),
    sa.Column("source_graph_provenance", JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
    sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    sa.CheckConstraint("provider = lower(provider) AND provider <> ''", name="provider_valid"),
    sa.CheckConstraint("provider_kind = lower(provider_kind) AND provider_kind <> ''", name="provider_kind_valid"),
    sa.CheckConstraint("extraction_kind IN ('direct_result', 'page_extracted', 'source_graph', 'global_search', 'operator')", name="extraction_kind_valid"),
    sa.CheckConstraint("independent_evidence_key = btrim(independent_evidence_key) AND independent_evidence_key <> ''", name="evidence_key_valid"),
    sa.CheckConstraint("query_sha256 IS NULL OR (length(query_sha256) = 64 AND query_sha256 ~ '^[0-9a-f]{64}$')", name="query_hash_valid"),
    sa.CheckConstraint("jsonb_typeof(profile_gap_keys) = 'array' AND jsonb_typeof(source_graph_provenance) = 'object'", name="evidence_payloads_valid"),
    sa.UniqueConstraint("source_id", "independent_evidence_key", name="uq_source_discovery_evidence_independent_key"),
)

sa.Index("ix_source_discovery_evidence_campaign", source_discovery_evidence.c.campaign_id, source_discovery_evidence.c.last_seen_at)
sa.Index("ix_source_discovery_evidence_provider_kind", source_discovery_evidence.c.provider_kind, source_discovery_evidence.c.extraction_kind)

telegram_source_validations = sa.Table(
    "telegram_source_validations",
    metadata,
    sa.Column("source_id", sa.BigInteger(), sa.ForeignKey("sources.id", ondelete="RESTRICT"), primary_key=True),
    sa.Column("collector_account_id", sa.BigInteger(), sa.ForeignKey("collector_accounts.id", ondelete="RESTRICT"), primary_key=True),
    sa.Column("state", sa.String(24), nullable=False, server_default="discovered"),
    sa.Column("access_mode", sa.String(24)),
    sa.Column("canonical_peer_identity", sa.String(255)),
    sa.Column("failure_code", sa.String(64)),
    sa.Column("checked_at", sa.DateTime(timezone=True)),
    sa.Column("checked_by", sa.String(128)),
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    sa.CheckConstraint("state IN ('discovered', 'local_valid', 'validation_pending', 'accessible', 'audit_pending', 'approved', 'rejected', 'needs_review', 'unavailable')", name="state_valid"),
    sa.CheckConstraint("access_mode IS NULL OR access_mode IN ('public_readable', 'joined', 'join_required', 'unavailable')", name="access_mode_valid"),
    sa.CheckConstraint("failure_code IS NULL OR failure_code ~ '^[A-Za-z][A-Za-z0-9_.-]{0,63}$'", name="failure_code_valid"),
)

source_monitoring_assignments = sa.Table(
    "source_monitoring_assignments",
    metadata,
    sa.Column("source_id", sa.BigInteger(), sa.ForeignKey("sources.id", ondelete="RESTRICT"), primary_key=True),
    sa.Column("collector_account_id", sa.BigInteger(), sa.ForeignKey("collector_accounts.id", ondelete="RESTRICT"), nullable=False),
    sa.Column("tier", sa.String(1), nullable=False, server_default="B"),
    sa.Column("state", sa.String(16), nullable=False, server_default="ready"),
    sa.Column("cursor", JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
    sa.Column("next_due_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("last_started_at", sa.DateTime(timezone=True)),
    sa.Column("last_completed_at", sa.DateTime(timezone=True)),
    sa.Column("last_failure_code", sa.String(64)),
    sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    sa.CheckConstraint("tier IN ('A', 'B', 'C', 'D')", name="tier_valid"),
    sa.CheckConstraint("state IN ('ready', 'pacing', 'floodwait', 'paused', 'unavailable')", name="state_valid"),
    sa.CheckConstraint("jsonb_typeof(cursor) = 'object'", name="cursor_object"),
    sa.UniqueConstraint("collector_account_id", "source_id", name="uq_source_monitoring_assignments_account_source"),
)

sa.Index("ix_source_monitoring_assignments_due", source_monitoring_assignments.c.state, source_monitoring_assignments.c.next_due_at)

telegram_chat_discovery_topics = sa.Table(
    "telegram_chat_discovery_topics",
    metadata,
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

sa.Index(
    "ix_telegram_chat_discovery_topics_due",
    telegram_chat_discovery_topics.c.is_active,
    telegram_chat_discovery_topics.c.next_eligible_at,
    telegram_chat_discovery_topics.c.priority,
)

telegram_chat_discovery_search_runs = sa.Table(
    "telegram_chat_discovery_search_runs",
    metadata,
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

sa.Index(
    "ix_telegram_chat_discovery_search_runs_topic_started",
    telegram_chat_discovery_search_runs.c.topic_id,
    telegram_chat_discovery_search_runs.c.started_at,
)

telegram_chat_discovery_peers = sa.Table(
    "telegram_chat_discovery_peers",
    metadata,
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

sa.Index(
    "ix_telegram_chat_discovery_peers_screen_due",
    telegram_chat_discovery_peers.c.screen_status,
    telegram_chat_discovery_peers.c.next_screen_at,
)
sa.Index(
    "ix_telegram_chat_discovery_peers_bucket",
    telegram_chat_discovery_peers.c.dedup_bucket,
    telegram_chat_discovery_peers.c.created_at,
)

telegram_chat_discovery_peer_aliases = sa.Table(
    "telegram_chat_discovery_peer_aliases",
    metadata,
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

sa.Index(
    "ix_telegram_chat_discovery_peer_aliases_peer",
    telegram_chat_discovery_peer_aliases.c.peer_id,
)

telegram_chat_discovery_observations = sa.Table(
    "telegram_chat_discovery_observations",
    metadata,
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

sa.Index(
    "ix_telegram_chat_discovery_observations_topic_seen",
    telegram_chat_discovery_observations.c.topic_id,
    telegram_chat_discovery_observations.c.last_seen_at,
)

telegram_chat_discovery_screen_attempts = sa.Table(
    "telegram_chat_discovery_screen_attempts",
    metadata,
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
    sa.Column("history_request_count", sa.Integer(), nullable=False, server_default="0"),
    sa.Column("ai_call_count", sa.Integer(), nullable=False, server_default="0"),
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
    sa.CheckConstraint("sample_count >= 0 AND useful_count >= 0 AND history_request_count >= 0 AND ai_call_count >= 0", name="counters_nonnegative"),
    sa.CheckConstraint("confidence IS NULL OR confidence BETWEEN 0 AND 1", name="confidence_valid"),
    sa.CheckConstraint("jsonb_typeof(category_counts) = 'object' AND jsonb_typeof(reason_codes) = 'array'", name="payloads_valid"),
    sa.UniqueConstraint("peer_id", "attempt_number", name="uq_telegram_chat_discovery_screen_attempts_peer_attempt"),
)

sa.Index(
    "ix_telegram_chat_discovery_screen_attempts_peer_started",
    telegram_chat_discovery_screen_attempts.c.peer_id,
    telegram_chat_discovery_screen_attempts.c.started_at,
)

discovery_cost_events = sa.Table(
    "discovery_cost_events",
    metadata,
    sa.Column("id", UUID(as_uuid=True), primary_key=True),
    sa.Column("campaign_id", UUID(as_uuid=True), sa.ForeignKey("discovery_campaigns.id", ondelete="RESTRICT"), nullable=False),
    sa.Column("stage", sa.String(32), nullable=False),
    sa.Column("provider", sa.String(64), nullable=False),
    sa.Column("units", sa.Integer(), nullable=False, server_default="1"),
    sa.Column("estimated_cost_usd", sa.Numeric(18, 9), nullable=False, server_default="0"),
    sa.Column("idempotency_key", sa.String(255), nullable=False),
    sa.Column("recorded_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    sa.CheckConstraint("stage IN ('web_search', 'page_fetch', 'telegram_validation', 'source_audit')", name="stage_valid"),
    sa.CheckConstraint("provider = lower(provider) AND provider <> ''", name="provider_valid"),
    sa.CheckConstraint("units > 0 AND estimated_cost_usd >= 0", name="cost_nonnegative"),
    sa.UniqueConstraint("idempotency_key", name="uq_discovery_cost_events_idempotency_key"),
)
