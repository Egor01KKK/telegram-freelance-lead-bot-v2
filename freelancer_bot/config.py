from __future__ import annotations

from datetime import datetime
from enum import Enum
from pathlib import Path
from decimal import Decimal
from typing import Any, Self

from pydantic import (
    AliasChoices,
    Field,
    SecretStr,
    ValidationError,
    field_validator,
    model_validator,
)
from pydantic_settings import BaseSettings, SettingsConfigDict


class RuntimeMode(str, Enum):
    RUN = "run"
    BOT_ONLY = "bot_only"
    COLLECTOR_ONLY = "collector_only"
    CHECK_CONFIG = "check_config"
    CHECK_FILTER = "check_filter"
    CHECK_SOURCES = "check_sources"
    DRAFT_TEXT = "draft_text"
    DATABASE = "database"


class AppEnvironment(str, Enum):
    DEVELOPMENT = "development"
    TEST = "test"
    PRODUCTION = "production"


class AIProvider(str, Enum):
    OPENAI = "openai"


class Sensitivity(str, Enum):
    PUBLIC = "public"
    INTERNAL = "internal"
    SENSITIVE = "sensitive"
    SECRET = "secret"


class ConfigurationError(RuntimeError):
    pass


def classified_field(default: Any, *, sensitivity: Sensitivity, **kwargs: Any) -> Any:
    metadata = dict(kwargs.pop("json_schema_extra", {}) or {})
    metadata["sensitivity"] = sensitivity.value
    return Field(default, json_schema_extra=metadata, **kwargs)


class RuntimeConfig(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_ignore_empty=True,
        extra="ignore",
        frozen=True,
        hide_input_in_errors=True,
        populate_by_name=True,
    )

    app_environment: AppEnvironment = classified_field(
        AppEnvironment.DEVELOPMENT,
        sensitivity=Sensitivity.INTERNAL,
        validation_alias="APP_ENV",
    )
    api_id: int | None = classified_field(
        None,
        sensitivity=Sensitivity.SENSITIVE,
        validation_alias=AliasChoices("TELEGRAM_API_ID", "API_ID"),
        gt=0,
    )
    api_hash: SecretStr | None = classified_field(
        None,
        sensitivity=Sensitivity.SECRET,
        validation_alias=AliasChoices("TELEGRAM_API_HASH", "API_HASH"),
    )
    bot_token: SecretStr | None = classified_field(
        None,
        sensitivity=Sensitivity.SECRET,
        validation_alias=AliasChoices("TELEGRAM_BOT_TOKEN", "BOT_TOKEN"),
    )
    target_chat_id: int | None = classified_field(
        None,
        sensitivity=Sensitivity.SENSITIVE,
        validation_alias=AliasChoices("TELEGRAM_TARGET_CHAT_ID", "TARGET_USER_ID"),
    )
    database_path: Path = classified_field(
        Path("data/leads.sqlite3"),
        sensitivity=Sensitivity.INTERNAL,
        validation_alias="DATABASE_PATH",
    )
    database_url: SecretStr | None = classified_field(
        None,
        sensitivity=Sensitivity.SECRET,
        validation_alias="DATABASE_URL",
    )
    subscription_plan_amount: Decimal = classified_field(
        Decimal("990"),
        sensitivity=Sensitivity.PUBLIC,
        validation_alias=AliasChoices(
            "SUBSCRIPTION_PLAN_AMOUNT",
            "SUBSCRIPTION_PRICE",
            "BILLING_PLAN_AMOUNT",
        ),
        gt=0,
    )
    subscription_plan_currency: str = classified_field(
        "RUB",
        sensitivity=Sensitivity.PUBLIC,
        validation_alias=AliasChoices(
            "SUBSCRIPTION_PLAN_CURRENCY",
            "SUBSCRIPTION_CURRENCY",
            "BILLING_PLAN_CURRENCY",
        ),
        min_length=3,
        max_length=3,
    )
    subscription_plan_interval: str = classified_field(
        "month",
        sensitivity=Sensitivity.PUBLIC,
        validation_alias=AliasChoices(
            "SUBSCRIPTION_PLAN_INTERVAL",
            "SUBSCRIPTION_INTERVAL",
            "BILLING_PLAN_INTERVAL",
        ),
        min_length=1,
        max_length=32,
    )
    sources_path: Path = classified_field(
        Path("config/sources.json"),
        sensitivity=Sensitivity.INTERNAL,
        validation_alias="SOURCES_PATH",
    )
    filters_path: Path = classified_field(
        Path("config/filters.json"),
        sensitivity=Sensitivity.INTERNAL,
        validation_alias="FILTERS_PATH",
    )
    user_session_path: Path = classified_field(
        Path("sessions/freelancer_user"),
        sensitivity=Sensitivity.SENSITIVE,
        validation_alias="USER_SESSION_PATH",
    )
    bot_session_path: Path = classified_field(
        Path("sessions/freelancer_delivery_bot"),
        sensitivity=Sensitivity.SENSITIVE,
        validation_alias="BOT_SESSION_PATH",
    )
    catch_up_limit: int = classified_field(
        25,
        sensitivity=Sensitivity.INTERNAL,
        validation_alias="CATCH_UP_LIMIT",
        ge=0,
    )
    catch_up_source_limit: int = classified_field(
        100,
        sensitivity=Sensitivity.INTERNAL,
        validation_alias="CATCH_UP_SOURCE_LIMIT",
        ge=1,
        le=1000,
    )
    catch_up_newly_approved_sources_only: bool = classified_field(
        False,
        sensitivity=Sensitivity.INTERNAL,
        validation_alias="CATCH_UP_NEWLY_APPROVED_SOURCES_ONLY",
    )
    catch_up_after_source_discovery: bool = classified_field(
        False,
        sensitivity=Sensitivity.INTERNAL,
        validation_alias="CATCH_UP_AFTER_SOURCE_DISCOVERY",
    )
    fresh_run_id: str | None = classified_field(
        None,
        sensitivity=Sensitivity.INTERNAL,
        validation_alias="FRESH_RUN_ID",
        min_length=1,
        max_length=128,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$",
    )
    fresh_run_started_at: datetime | None = classified_field(
        None,
        sensitivity=Sensitivity.INTERNAL,
        validation_alias="FRESH_RUN_STARTED_AT",
    )
    send_catch_up: bool = classified_field(
        False,
        sensitivity=Sensitivity.PUBLIC,
        validation_alias="SEND_CATCH_UP",
    )
    legacy_delivery_enabled: bool = classified_field(
        False,
        sensitivity=Sensitivity.PUBLIC,
        validation_alias="LEGACY_DELIVERY_ENABLED",
    )
    ai_reply_enabled: bool = classified_field(
        False,
        sensitivity=Sensitivity.PUBLIC,
        validation_alias="AI_REPLY_ENABLED",
    )
    ai_provider: AIProvider = classified_field(
        AIProvider.OPENAI,
        sensitivity=Sensitivity.PUBLIC,
        validation_alias="AI_REPLY_PROVIDER",
    )
    openai_api_key: SecretStr | None = classified_field(
        None,
        sensitivity=Sensitivity.SECRET,
        validation_alias="OPENAI_API_KEY",
    )
    deepseek_api_key: SecretStr | None = classified_field(
        None,
        sensitivity=Sensitivity.SECRET,
        validation_alias="DEEPSEEK_API_KEY",
    )
    tokenrouter_api_key: SecretStr | None = classified_field(
        None,
        sensitivity=Sensitivity.SECRET,
        validation_alias="TOKENROUTER_API_KEY",
    )
    tokenrouter_base_url: str = classified_field(
        "https://api.tokenrouter.com/v1",
        sensitivity=Sensitivity.PUBLIC,
        validation_alias="TOKENROUTER_BASE_URL",
        min_length=1,
        max_length=2048,
    )
    openai_model: str = classified_field(
        "gpt-4.1-mini",
        sensitivity=Sensitivity.PUBLIC,
        validation_alias=AliasChoices("AI_REPLY_MODEL", "OPENAI_MODEL"),
        min_length=1,
    )
    ai_reply_temperature: float = classified_field(
        0.3,
        sensitivity=Sensitivity.PUBLIC,
        validation_alias="AI_REPLY_TEMPERATURE",
        ge=0,
        le=2,
    )
    ai_request_timeout_seconds: int = classified_field(
        45,
        sensitivity=Sensitivity.INTERNAL,
        validation_alias="AI_REQUEST_TIMEOUT_SECONDS",
        gt=0,
    )
    source_audit_provider: str = classified_field(
        "openai",
        sensitivity=Sensitivity.PUBLIC,
        validation_alias="SOURCE_AUDIT_PROVIDER",
        min_length=1,
        max_length=64,
    )
    source_audit_model: str = classified_field(
        "gpt-5-nano",
        sensitivity=Sensitivity.PUBLIC,
        validation_alias="SOURCE_AUDIT_MODEL",
        min_length=1,
        max_length=128,
    )
    source_audit_temperature: float = classified_field(
        0.0,
        sensitivity=Sensitivity.PUBLIC,
        validation_alias="SOURCE_AUDIT_TEMPERATURE",
        ge=0,
        le=2,
    )
    source_audit_timeout_seconds: int = classified_field(
        45,
        sensitivity=Sensitivity.INTERNAL,
        validation_alias="SOURCE_AUDIT_TIMEOUT_SECONDS",
        gt=0,
    )
    opportunity_analysis_provider: str = classified_field(
        "openai",
        sensitivity=Sensitivity.PUBLIC,
        validation_alias="OPPORTUNITY_ANALYSIS_PROVIDER",
        min_length=1,
        max_length=64,
    )
    opportunity_analysis_model: str = classified_field(
        "gpt-5-nano",
        sensitivity=Sensitivity.PUBLIC,
        validation_alias="OPPORTUNITY_ANALYSIS_MODEL",
        min_length=1,
        max_length=128,
    )
    opportunity_analysis_temperature: float = classified_field(
        0.0,
        sensitivity=Sensitivity.PUBLIC,
        validation_alias="OPPORTUNITY_ANALYSIS_TEMPERATURE",
        ge=0,
        le=2,
    )
    opportunity_analysis_timeout_seconds: int = classified_field(
        45,
        sensitivity=Sensitivity.INTERNAL,
        validation_alias="OPPORTUNITY_ANALYSIS_TIMEOUT_SECONDS",
        gt=0,
    )
    opportunity_analysis_max_output_attempts: int = classified_field(
        2,
        sensitivity=Sensitivity.INTERNAL,
        validation_alias="OPPORTUNITY_ANALYSIS_MAX_OUTPUT_ATTEMPTS",
        ge=1,
        le=5,
    )
    opportunity_analysis_fallback_enabled: bool = classified_field(
        False,
        sensitivity=Sensitivity.PUBLIC,
        validation_alias="OPPORTUNITY_ANALYSIS_FALLBACK_ENABLED",
    )
    opportunity_analysis_fallback_provider: str = classified_field(
        "openai",
        sensitivity=Sensitivity.PUBLIC,
        validation_alias="OPPORTUNITY_ANALYSIS_FALLBACK_PROVIDER",
        min_length=1,
        max_length=64,
    )
    opportunity_analysis_fallback_model: str = classified_field(
        "gpt-5-mini",
        sensitivity=Sensitivity.PUBLIC,
        validation_alias="OPPORTUNITY_ANALYSIS_FALLBACK_MODEL",
        min_length=1,
        max_length=128,
    )
    opportunity_analysis_confidence_threshold: float = classified_field(
        0.65,
        sensitivity=Sensitivity.PUBLIC,
        validation_alias="OPPORTUNITY_ANALYSIS_CONFIDENCE_THRESHOLD",
        ge=0,
        le=1,
    )
    opportunity_analysis_routing_version: str = classified_field(
        "opportunity-routing.v1",
        sensitivity=Sensitivity.PUBLIC,
        validation_alias="OPPORTUNITY_ANALYSIS_ROUTING_VERSION",
        pattern=r"^[a-z0-9][a-z0-9._-]{0,99}$",
    )
    opportunity_analysis_pricing_version: str = classified_field(
        "openai-gpt5-2025-08-07",
        sensitivity=Sensitivity.PUBLIC,
        validation_alias="OPPORTUNITY_ANALYSIS_PRICING_VERSION",
        pattern=r"^[a-z0-9][a-z0-9._-]{0,99}$",
    )
    opportunity_analysis_input_usd_per_million: Decimal = classified_field(
        Decimal("0.05"),
        sensitivity=Sensitivity.PUBLIC,
        validation_alias="OPPORTUNITY_ANALYSIS_INPUT_USD_PER_MILLION",
        ge=0,
    )
    opportunity_analysis_output_usd_per_million: Decimal = classified_field(
        Decimal("0.40"),
        sensitivity=Sensitivity.PUBLIC,
        validation_alias="OPPORTUNITY_ANALYSIS_OUTPUT_USD_PER_MILLION",
        ge=0,
    )
    opportunity_analysis_fallback_input_usd_per_million: Decimal = classified_field(
        Decimal("0.25"),
        sensitivity=Sensitivity.PUBLIC,
        validation_alias="OPPORTUNITY_ANALYSIS_FALLBACK_INPUT_USD_PER_MILLION",
        ge=0,
    )
    opportunity_analysis_fallback_output_usd_per_million: Decimal = classified_field(
        Decimal("2.00"),
        sensitivity=Sensitivity.PUBLIC,
        validation_alias="OPPORTUNITY_ANALYSIS_FALLBACK_OUTPUT_USD_PER_MILLION",
        ge=0,
    )
    opportunity_analysis_daily_spend_limit_usd: Decimal | None = classified_field(
        Decimal("1.00"),
        sensitivity=Sensitivity.PUBLIC,
        validation_alias="OPPORTUNITY_ANALYSIS_DAILY_SPEND_LIMIT_USD",
        ge=0,
    )
    opportunity_analysis_monthly_spend_limit_usd: Decimal | None = classified_field(
        Decimal("10.00"),
        sensitivity=Sensitivity.PUBLIC,
        validation_alias="OPPORTUNITY_ANALYSIS_MONTHLY_SPEND_LIMIT_USD",
        ge=0,
    )
    opportunity_analysis_budget_reserve_input_tokens: int = classified_field(
        1_000,
        sensitivity=Sensitivity.INTERNAL,
        validation_alias="OPPORTUNITY_ANALYSIS_BUDGET_RESERVE_INPUT_TOKENS",
        ge=0,
    )
    opportunity_analysis_budget_reserve_output_tokens: int = classified_field(
        300,
        sensitivity=Sensitivity.INTERNAL,
        validation_alias="OPPORTUNITY_ANALYSIS_BUDGET_RESERVE_OUTPUT_TOKENS",
        ge=0,
    )
    onboarding_profile_provider: str = classified_field(
        "openai",
        sensitivity=Sensitivity.PUBLIC,
        validation_alias="ONBOARDING_PROFILE_PROVIDER",
        min_length=1,
        max_length=64,
    )
    onboarding_profile_model: str = classified_field(
        "gpt-5-nano",
        sensitivity=Sensitivity.PUBLIC,
        validation_alias="ONBOARDING_PROFILE_MODEL",
        min_length=1,
        max_length=128,
    )
    onboarding_profile_temperature: float = classified_field(
        0.0,
        sensitivity=Sensitivity.PUBLIC,
        validation_alias="ONBOARDING_PROFILE_TEMPERATURE",
        ge=0,
        le=2,
    )
    onboarding_profile_timeout_seconds: int = classified_field(
        45,
        sensitivity=Sensitivity.INTERNAL,
        validation_alias="ONBOARDING_PROFILE_TIMEOUT_SECONDS",
        gt=0,
    )
    onboarding_profile_max_output_attempts: int = classified_field(
        2,
        sensitivity=Sensitivity.INTERNAL,
        validation_alias="ONBOARDING_PROFILE_MAX_OUTPUT_ATTEMPTS",
        ge=1,
        le=5,
    )
    onboarding_profile_max_transport_attempts: int = classified_field(
        2,
        sensitivity=Sensitivity.INTERNAL,
        validation_alias="ONBOARDING_PROFILE_MAX_TRANSPORT_ATTEMPTS",
        ge=1,
        le=5,
    )
    onboarding_profile_transport_retry_backoff_seconds: float = classified_field(
        1.0,
        sensitivity=Sensitivity.INTERNAL,
        validation_alias="ONBOARDING_PROFILE_TRANSPORT_RETRY_BACKOFF_SECONDS",
        ge=0,
        le=30,
    )
    max_ai_calls_per_run: int | None = classified_field(
        10,
        sensitivity=Sensitivity.INTERNAL,
        validation_alias="MAX_AI_CALLS_PER_RUN",
        ge=1,
        le=100,
    )
    matching_decision_policy_version: str = classified_field(
        "matching-decision-policy.v1",
        sensitivity=Sensitivity.PUBLIC,
        validation_alias="MATCHING_DECISION_POLICY_VERSION",
        pattern=r"^[a-z0-9][a-z0-9._-]{0,99}$",
    )
    matching_minimum_relevance_score: Decimal = classified_field(
        Decimal("0.3000"),
        sensitivity=Sensitivity.PUBLIC,
        validation_alias="MATCHING_MINIMUM_RELEVANCE_SCORE",
        ge=0,
        le=1,
    )
    matching_minimum_rank_score: Decimal = classified_field(
        Decimal("0.4000"),
        sensitivity=Sensitivity.PUBLIC,
        validation_alias="MATCHING_MINIMUM_RANK_SCORE",
        ge=0,
        le=1,
    )
    matching_freshness_weight: Decimal = classified_field(
        Decimal("0.1000"),
        sensitivity=Sensitivity.PUBLIC,
        validation_alias="MATCHING_FRESHNESS_WEIGHT",
        ge=0,
        le=1,
    )
    matching_maximum_age_seconds: int = classified_field(
        7 * 24 * 60 * 60,
        sensitivity=Sensitivity.PUBLIC,
        validation_alias="MATCHING_MAXIMUM_AGE_SECONDS",
        ge=60,
    )
    matching_suppress_expired: bool = classified_field(
        True,
        sensitivity=Sensitivity.PUBLIC,
        validation_alias="MATCHING_SUPPRESS_EXPIRED",
    )
    matching_evaluation_bucket_seconds: int = classified_field(
        60 * 60,
        sensitivity=Sensitivity.INTERNAL,
        validation_alias="MATCHING_EVALUATION_BUCKET_SECONDS",
        ge=60,
        le=24 * 60 * 60,
    )
    source_reaudit_degraded_cadence_days: int = classified_field(
        7,
        sensitivity=Sensitivity.PUBLIC,
        validation_alias="SOURCE_REAUDIT_DEGRADED_CADENCE_DAYS",
        ge=7,
        le=30,
    )
    source_reaudit_high_activity_cadence_days: int = classified_field(
        7,
        sensitivity=Sensitivity.PUBLIC,
        validation_alias="SOURCE_REAUDIT_HIGH_ACTIVITY_CADENCE_DAYS",
        ge=7,
        le=30,
    )
    source_reaudit_normal_cadence_days: int = classified_field(
        14,
        sensitivity=Sensitivity.PUBLIC,
        validation_alias="SOURCE_REAUDIT_NORMAL_CADENCE_DAYS",
        ge=7,
        le=30,
    )
    source_reaudit_quiet_cadence_days: int = classified_field(
        30,
        sensitivity=Sensitivity.PUBLIC,
        validation_alias="SOURCE_REAUDIT_QUIET_CADENCE_DAYS",
        ge=7,
        le=30,
    )
    source_reaudit_high_activity_messages_per_day: float = classified_field(
        50.0,
        sensitivity=Sensitivity.INTERNAL,
        validation_alias="SOURCE_REAUDIT_HIGH_ACTIVITY_MESSAGES_PER_DAY",
        gt=0,
    )
    source_reaudit_quiet_activity_messages_per_day: float = classified_field(
        5.0,
        sensitivity=Sensitivity.INTERNAL,
        validation_alias="SOURCE_REAUDIT_QUIET_ACTIVITY_MESSAGES_PER_DAY",
        ge=0,
    )
    source_discovery_enabled: bool = classified_field(
        False,
        sensitivity=Sensitivity.PUBLIC,
        validation_alias="SOURCE_DISCOVERY_ENABLED",
    )
    source_audit_enabled: bool = classified_field(
        False,
        sensitivity=Sensitivity.PUBLIC,
        validation_alias="SOURCE_AUDIT_ENABLED",
    )
    telegram_global_discovery_enabled: bool = classified_field(
        False,
        sensitivity=Sensitivity.PUBLIC,
        validation_alias="TELEGRAM_GLOBAL_DISCOVERY_ENABLED",
    )
    source_graph_discovery_enabled: bool = classified_field(
        False,
        sensitivity=Sensitivity.PUBLIC,
        validation_alias="SOURCE_GRAPH_DISCOVERY_ENABLED",
    )
    source_discovery_audit_new_candidates_only: bool = classified_field(
        False,
        sensitivity=Sensitivity.INTERNAL,
        validation_alias="SOURCE_DISCOVERY_AUDIT_NEW_CANDIDATES_ONLY",
    )
    searxng_url: str | None = classified_field(
        None,
        sensitivity=Sensitivity.INTERNAL,
        validation_alias="SEARXNG_URL",
        max_length=2048,
    )
    primary_web_search_url: str | None = classified_field(
        None,
        sensitivity=Sensitivity.INTERNAL,
        validation_alias="WEB_PRIMARY_SEARCH_URL",
        max_length=2048,
    )
    primary_web_search_api_key: SecretStr | None = classified_field(
        None,
        sensitivity=Sensitivity.SECRET,
        validation_alias="WEB_PRIMARY_SEARCH_API_KEY",
    )
    brave_search_api_key: SecretStr | None = classified_field(
        None,
        sensitivity=Sensitivity.SECRET,
        validation_alias="BRAVE_SEARCH_API_KEY",
    )
    brave_search_timeout_seconds: float = classified_field(
        15.0,
        sensitivity=Sensitivity.INTERNAL,
        validation_alias="BRAVE_SEARCH_TIMEOUT_SECONDS",
        gt=0,
        le=120,
    )
    brave_search_requests_per_day: int = classified_field(
        100,
        sensitivity=Sensitivity.INTERNAL,
        validation_alias="BRAVE_SEARCH_REQUESTS_PER_DAY",
        ge=1,
    )
    brave_search_requests_per_campaign: int = classified_field(
        15,
        sensitivity=Sensitivity.INTERNAL,
        validation_alias="BRAVE_SEARCH_REQUESTS_PER_CAMPAIGN",
        ge=1,
    )
    brave_search_cost_usd_per_request: Decimal = classified_field(
        Decimal("0"),
        sensitivity=Sensitivity.INTERNAL,
        validation_alias="BRAVE_SEARCH_COST_USD_PER_REQUEST",
        ge=0,
    )
    brave_search_pricing_version: str = classified_field(
        "brave-pricing.v1",
        sensitivity=Sensitivity.INTERNAL,
        validation_alias="BRAVE_SEARCH_PRICING_VERSION",
        min_length=1,
        max_length=64,
    )
    web_search_calls_per_day: int = classified_field(
        500,
        sensitivity=Sensitivity.INTERNAL,
        validation_alias="WEB_SEARCH_CALLS_PER_DAY",
        ge=1,
    )
    web_page_fetches_per_day: int = classified_field(
        1000,
        sensitivity=Sensitivity.INTERNAL,
        validation_alias="WEB_PAGE_FETCHES_PER_DAY",
        ge=1,
    )
    telegram_validations_per_hour_per_account: int = classified_field(
        30,
        sensitivity=Sensitivity.INTERNAL,
        validation_alias="TELEGRAM_VALIDATIONS_PER_HOUR_PER_ACCOUNT",
        ge=1,
    )
    telegram_audit_samples_per_hour_per_account: int = classified_field(
        10,
        sensitivity=Sensitivity.INTERNAL,
        validation_alias="TELEGRAM_AUDIT_SAMPLES_PER_HOUR_PER_ACCOUNT",
        ge=1,
    )
    source_audit_calls_per_day: int = classified_field(
        100,
        sensitivity=Sensitivity.INTERNAL,
        validation_alias="SOURCE_AUDIT_CALLS_PER_DAY",
        ge=1,
    )
    opportunity_analysis_backlog_threshold: int = classified_field(
        500,
        sensitivity=Sensitivity.INTERNAL,
        validation_alias="OPPORTUNITY_ANALYSIS_BACKLOG_THRESHOLD",
        ge=1,
    )
    source_discovery_interval_seconds: int = classified_field(
        6 * 60 * 60,
        sensitivity=Sensitivity.INTERNAL,
        validation_alias="SOURCE_DISCOVERY_INTERVAL_SECONDS",
        ge=60,
    )
    source_discovery_seed_limit: int = classified_field(
        3,
        sensitivity=Sensitivity.INTERNAL,
        validation_alias="SOURCE_DISCOVERY_SEED_LIMIT",
        ge=1,
        le=100,
    )
    source_discovery_message_limit_per_seed: int = classified_field(
        150,
        sensitivity=Sensitivity.INTERNAL,
        validation_alias="SOURCE_DISCOVERY_MESSAGE_LIMIT_PER_SEED",
        ge=1,
        le=1000,
    )
    source_discovery_max_candidates: int = classified_field(
        100,
        sensitivity=Sensitivity.INTERNAL,
        validation_alias="SOURCE_DISCOVERY_MAX_CANDIDATES",
        ge=1,
        le=1000,
    )
    source_discovery_max_observations: int = classified_field(
        1000,
        sensitivity=Sensitivity.INTERNAL,
        validation_alias="SOURCE_DISCOVERY_MAX_OBSERVATIONS",
        ge=1,
        le=10_000,
    )
    source_discovery_audit_limit: int = classified_field(
        10,
        sensitivity=Sensitivity.INTERNAL,
        validation_alias="SOURCE_DISCOVERY_AUDIT_LIMIT",
        ge=1,
        le=100,
    )
    source_discovery_reaudit_limit: int = classified_field(
        3,
        sensitivity=Sensitivity.INTERNAL,
        validation_alias="SOURCE_DISCOVERY_REAUDIT_LIMIT",
        ge=1,
        le=100,
    )
    web_discovery_min_delay_seconds: float = classified_field(
        5.0,
        sensitivity=Sensitivity.INTERNAL,
        validation_alias="WEB_DISCOVERY_MIN_DELAY_SECONDS",
        ge=0,
        le=3600,
    )
    web_discovery_max_delay_seconds: float = classified_field(
        10.0,
        sensitivity=Sensitivity.INTERNAL,
        validation_alias="WEB_DISCOVERY_MAX_DELAY_SECONDS",
        ge=0,
        le=3600,
    )
    web_discovery_max_concurrency: int = classified_field(
        1,
        sensitivity=Sensitivity.INTERNAL,
        validation_alias="WEB_DISCOVERY_MAX_CONCURRENCY",
        ge=1,
        le=4,
    )
    web_discovery_base_backoff_seconds: float = classified_field(
        60.0,
        sensitivity=Sensitivity.INTERNAL,
        validation_alias="WEB_DISCOVERY_BASE_BACKOFF_SECONDS",
        gt=0,
        le=86_400,
    )
    web_discovery_max_backoff_seconds: float = classified_field(
        3600.0,
        sensitivity=Sensitivity.INTERNAL,
        validation_alias="WEB_DISCOVERY_MAX_BACKOFF_SECONDS",
        gt=0,
        le=7 * 86_400,
    )
    telegram_crawl_min_delay_seconds: float = classified_field(
        5.0,
        sensitivity=Sensitivity.INTERNAL,
        validation_alias="TELEGRAM_CRAWL_MIN_DELAY_SECONDS",
        ge=0,
        le=300,
    )
    telegram_crawl_max_delay_seconds: float = classified_field(
        10.0,
        sensitivity=Sensitivity.INTERNAL,
        validation_alias="TELEGRAM_CRAWL_MAX_DELAY_SECONDS",
        ge=0,
        le=300,
    )
    telegram_source_cooldown_min_seconds: float = classified_field(
        15.0,
        sensitivity=Sensitivity.INTERNAL,
        validation_alias="TELEGRAM_SOURCE_COOLDOWN_MIN_SECONDS",
        ge=0,
        le=1800,
    )
    telegram_source_cooldown_max_seconds: float = classified_field(
        30.0,
        sensitivity=Sensitivity.INTERNAL,
        validation_alias="TELEGRAM_SOURCE_COOLDOWN_MAX_SECONDS",
        ge=0,
        le=1800,
    )
    telegram_max_history_messages_per_pass: int = classified_field(
        25,
        sensitivity=Sensitivity.INTERNAL,
        validation_alias="TELEGRAM_MAX_HISTORY_MESSAGES_PER_PASS",
        ge=20,
        le=30,
    )
    source_audit_sample_size: int = classified_field(
        60,
        sensitivity=Sensitivity.INTERNAL,
        validation_alias="SOURCE_AUDIT_SAMPLE_SIZE",
        ge=20,
        le=200,
    )
    telegram_max_entity_resolves_per_graph_pass: int = classified_field(
        5,
        sensitivity=Sensitivity.INTERNAL,
        validation_alias="TELEGRAM_MAX_ENTITY_RESOLVES_PER_GRAPH_PASS",
        ge=1,
        le=100,
    )
    telegram_max_audits_per_batch: int = classified_field(
        2,
        sensitivity=Sensitivity.INTERNAL,
        validation_alias="TELEGRAM_MAX_AUDITS_PER_BATCH",
        ge=1,
        le=100,
    )
    telegram_graph_seeds_per_pass: int = classified_field(
        1,
        sensitivity=Sensitivity.INTERNAL,
        validation_alias="TELEGRAM_GRAPH_SEEDS_PER_PASS",
        ge=1,
        le=100,
    )
    telegram_governor_lease_seconds: float = classified_field(
        900.0,
        sensitivity=Sensitivity.INTERNAL,
        validation_alias="TELEGRAM_GOVERNOR_LEASE_SECONDS",
        gt=0,
        le=86400,
    )
    telegram_chat_discovery_enabled: bool = classified_field(
        False,
        sensitivity=Sensitivity.PUBLIC,
        validation_alias="TELEGRAM_CHAT_DISCOVERY_ENABLED",
    )
    telegram_chat_discovery_refresh_interval_seconds: int = classified_field(
        6 * 60 * 60,
        sensitivity=Sensitivity.INTERNAL,
        validation_alias="TELEGRAM_CHAT_DISCOVERY_REFRESH_INTERVAL_SECONDS",
        ge=300,
        le=30 * 24 * 60 * 60,
    )
    telegram_chat_discovery_max_topics_per_cycle: int = classified_field(
        5,
        sensitivity=Sensitivity.INTERNAL,
        validation_alias="TELEGRAM_CHAT_DISCOVERY_MAX_TOPICS_PER_CYCLE",
        ge=1,
        le=100,
    )
    telegram_chat_discovery_max_pending_screens: int = classified_field(
        50,
        sensitivity=Sensitivity.INTERNAL,
        validation_alias="TELEGRAM_CHAT_DISCOVERY_MAX_PENDING_SCREENS",
        ge=1,
        le=10_000,
    )
    telegram_chat_discovery_screen_provider: str | None = classified_field(
        None,
        sensitivity=Sensitivity.PUBLIC,
        validation_alias="TELEGRAM_CHAT_DISCOVERY_SCREEN_PROVIDER",
        min_length=1,
        max_length=64,
    )
    telegram_chat_discovery_screen_model: str | None = classified_field(
        None,
        sensitivity=Sensitivity.PUBLIC,
        validation_alias="TELEGRAM_CHAT_DISCOVERY_SCREEN_MODEL",
        min_length=1,
        max_length=128,
    )
    telegram_chat_discovery_screen_timeout_seconds: int = classified_field(
        45,
        sensitivity=Sensitivity.INTERNAL,
        validation_alias="TELEGRAM_CHAT_DISCOVERY_SCREEN_TIMEOUT_SECONDS",
        ge=5,
        le=120,
    )
    telegram_chat_discovery_screen_retry_interval_seconds: int = classified_field(
        24 * 60 * 60,
        sensitivity=Sensitivity.INTERNAL,
        validation_alias="TELEGRAM_CHAT_DISCOVERY_SCREEN_RETRY_INTERVAL_SECONDS",
        ge=300,
        le=30 * 24 * 60 * 60,
    )
    telegram_chat_discovery_history_limit: int = classified_field(
        25,
        sensitivity=Sensitivity.INTERNAL,
        validation_alias="TELEGRAM_CHAT_DISCOVERY_HISTORY_LIMIT",
        ge=1,
        le=25,
    )
    telegram_chat_discovery_screen_max_message_chars: int = classified_field(
        1000,
        sensitivity=Sensitivity.INTERNAL,
        validation_alias="TELEGRAM_CHAT_DISCOVERY_SCREEN_MAX_MESSAGE_CHARS",
        ge=200,
        le=4000,
    )
    telegram_chat_discovery_screen_max_total_chars: int = classified_field(
        20_000,
        sensitivity=Sensitivity.INTERNAL,
        validation_alias="TELEGRAM_CHAT_DISCOVERY_SCREEN_MAX_TOTAL_CHARS",
        ge=5_000,
        le=50_000,
    )
    telegram_chat_discovery_screen_policy_version: str = classified_field(
        "telegram-chat-screen-policy.v1",
        sensitivity=Sensitivity.INTERNAL,
        validation_alias="TELEGRAM_CHAT_DISCOVERY_SCREEN_POLICY_VERSION",
        min_length=1,
        max_length=64,
    )
    telegram_chat_discovery_screen_min_sample: int = classified_field(
        10,
        sensitivity=Sensitivity.INTERNAL,
        validation_alias="TELEGRAM_CHAT_DISCOVERY_SCREEN_MIN_SAMPLE",
        ge=1,
        le=25,
    )
    telegram_chat_discovery_screen_min_useful_messages: int = classified_field(
        3,
        sensitivity=Sensitivity.INTERNAL,
        validation_alias="TELEGRAM_CHAT_DISCOVERY_SCREEN_MIN_USEFUL_MESSAGES",
        ge=1,
        le=25,
    )
    telegram_chat_discovery_screen_min_useful_ratio: float = classified_field(
        0.12,
        sensitivity=Sensitivity.INTERNAL,
        validation_alias="TELEGRAM_CHAT_DISCOVERY_SCREEN_MIN_USEFUL_RATIO",
        ge=0,
        le=1,
    )
    telegram_chat_discovery_screen_min_confidence: float = classified_field(
        0.65,
        sensitivity=Sensitivity.INTERNAL,
        validation_alias="TELEGRAM_CHAT_DISCOVERY_SCREEN_MIN_CONFIDENCE",
        ge=0,
        le=1,
    )
    telegram_chat_discovery_screen_max_seller_ratio: float = classified_field(
        0.70,
        sensitivity=Sensitivity.INTERNAL,
        validation_alias="TELEGRAM_CHAT_DISCOVERY_SCREEN_MAX_SELLER_RATIO",
        ge=0,
        le=1,
    )
    telegram_chat_discovery_screen_max_spam_ratio: float = classified_field(
        0.70,
        sensitivity=Sensitivity.INTERNAL,
        validation_alias="TELEGRAM_CHAT_DISCOVERY_SCREEN_MAX_SPAM_RATIO",
        ge=0,
        le=1,
    )
    freelancer_profile_path: Path = classified_field(
        Path("freelancer_profile.json"),
        sensitivity=Sensitivity.INTERNAL,
        validation_alias="FREELANCER_PROFILE_PATH",
    )
    log_level: str = classified_field(
        "INFO",
        sensitivity=Sensitivity.INTERNAL,
        validation_alias="LOG_LEVEL",
    )
    worker_poll_interval_seconds: float = classified_field(
        1.0,
        sensitivity=Sensitivity.INTERNAL,
        validation_alias="WORKER_POLL_INTERVAL_SECONDS",
        gt=0,
    )
    worker_lease_seconds: float = classified_field(
        30.0,
        sensitivity=Sensitivity.INTERNAL,
        validation_alias="WORKER_LEASE_SECONDS",
        gt=0,
    )
    worker_heartbeat_seconds: float = classified_field(
        10.0,
        sensitivity=Sensitivity.INTERNAL,
        validation_alias="WORKER_HEARTBEAT_SECONDS",
        gt=0,
    )
    worker_retry_delay_seconds: float = classified_field(
        5.0,
        sensitivity=Sensitivity.INTERNAL,
        validation_alias="WORKER_RETRY_DELAY_SECONDS",
        ge=0,
    )
    worker_shutdown_timeout_seconds: float = classified_field(
        20.0,
        sensitivity=Sensitivity.INTERNAL,
        validation_alias="WORKER_SHUTDOWN_TIMEOUT_SECONDS",
        gt=0,
    )

    @field_validator(
        "openai_model",
        "source_audit_model",
        "opportunity_analysis_model",
        "opportunity_analysis_fallback_model",
        "onboarding_profile_model",
        "telegram_chat_discovery_screen_model",
    )
    @classmethod
    def validate_model_name(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        if not normalized:
            raise ValueError("must not be blank")
        return normalized

    @field_validator(
        "source_audit_provider",
        "opportunity_analysis_provider",
        "opportunity_analysis_fallback_provider",
        "onboarding_profile_provider",
        "telegram_chat_discovery_screen_provider",
    )
    @classmethod
    def validate_source_audit_provider(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip().lower()
        if not normalized or not normalized[0].isalpha() or not all(
            character.isascii() and (character.isalnum() or character in "_-")
            for character in normalized
        ):
            raise ValueError("must be a lowercase-compatible provider identifier")
        return normalized

    @field_validator("subscription_plan_currency")
    @classmethod
    def validate_subscription_plan_currency(cls, value: str) -> str:
        normalized = value.strip().upper()
        if len(normalized) != 3 or not normalized.isascii() or not normalized.isalpha():
            raise ValueError("must be a three-letter currency code")
        return normalized

    @field_validator("subscription_plan_interval")
    @classmethod
    def validate_subscription_plan_interval(cls, value: str) -> str:
        normalized = value.strip().lower()
        if normalized != "month":
            raise ValueError("must be month")
        return normalized

    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, value: str) -> str:
        normalized = value.strip().upper()
        if normalized not in {"CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"}:
            raise ValueError("must be one of CRITICAL, ERROR, WARNING, INFO or DEBUG")
        return normalized

    @model_validator(mode="after")
    def validate_telegram_crawl_ranges(self) -> Self:
        if self.telegram_crawl_max_delay_seconds < self.telegram_crawl_min_delay_seconds:
            raise ValueError(
                "TELEGRAM_CRAWL_MAX_DELAY_SECONDS must be >= "
                "TELEGRAM_CRAWL_MIN_DELAY_SECONDS"
            )
        if (
            self.telegram_source_cooldown_max_seconds
            < self.telegram_source_cooldown_min_seconds
        ):
            raise ValueError(
                "TELEGRAM_SOURCE_COOLDOWN_MAX_SECONDS must be >= "
                "TELEGRAM_SOURCE_COOLDOWN_MIN_SECONDS"
            )
        if self.web_discovery_max_delay_seconds < self.web_discovery_min_delay_seconds:
            raise ValueError(
                "WEB_DISCOVERY_MAX_DELAY_SECONDS must be >= "
                "WEB_DISCOVERY_MIN_DELAY_SECONDS"
            )
        if self.web_discovery_max_backoff_seconds < self.web_discovery_base_backoff_seconds:
            raise ValueError(
                "WEB_DISCOVERY_MAX_BACKOFF_SECONDS must be >= "
                "WEB_DISCOVERY_BASE_BACKOFF_SECONDS"
            )
        # Keep the bounded production audit sampler capable of satisfying the
        # existing Source Audit v1 rejection evidence floor. Import lazily to
        # avoid making the settings module depend on the audit module at import
        # time while still deriving the invariant from the canonical policy.
        from .source_audit import SourceAuditDecisionPolicy

        rejection_evidence_floor = (
            SourceAuditDecisionPolicy().rejection_minimum_evidence_messages
        )
        if self.source_audit_sample_size < rejection_evidence_floor:
            raise ValueError(
                "SOURCE_AUDIT_SAMPLE_SIZE must be >= the Source Audit rejection "
                f"evidence floor ({rejection_evidence_floor})"
            )
        return self

    @classmethod
    def from_env(
        cls,
        *,
        mode: RuntimeMode = RuntimeMode.RUN,
        require_bot_token: bool | None = None,
        env_file: str | Path | None = ".env",
    ) -> Self:
        if require_bot_token is not None:
            mode = RuntimeMode.RUN if require_bot_token else RuntimeMode.CHECK_SOURCES

        try:
            config = cls(_env_file=env_file, _env_file_encoding="utf-8")
        except ValidationError as exc:
            raise ConfigurationError(_safe_validation_message(exc)) from None

        config.validate_for(mode)
        return config

    def validate_for(self, mode: RuntimeMode) -> None:
        required: list[tuple[str, Any]] = []
        if mode is RuntimeMode.RUN:
            required.extend(
                [
                    ("TELEGRAM_API_ID/API_ID", self.api_id),
                    ("TELEGRAM_API_HASH/API_HASH", self.api_hash),
                    ("TELEGRAM_BOT_TOKEN/BOT_TOKEN", self.bot_token),
                    ("DATABASE_URL", self.database_url),
                ]
            )
            if self.ai_reply_enabled:
                required.append(("OPENAI_API_KEY", self.openai_api_key))
        elif mode is RuntimeMode.BOT_ONLY:
            required.extend(
                [
                    ("TELEGRAM_API_ID/API_ID", self.api_id),
                    ("TELEGRAM_API_HASH/API_HASH", self.api_hash),
                    ("TELEGRAM_BOT_TOKEN/BOT_TOKEN", self.bot_token),
                    ("DATABASE_URL", self.database_url),
                ]
            )
        elif mode is RuntimeMode.COLLECTOR_ONLY:
            required.extend(
                [
                    ("TELEGRAM_API_ID/API_ID", self.api_id),
                    ("TELEGRAM_API_HASH/API_HASH", self.api_hash),
                    ("DATABASE_URL", self.database_url),
                ]
            )
        elif mode is RuntimeMode.CHECK_SOURCES:
            required.extend(
                [
                    ("TELEGRAM_API_ID/API_ID", self.api_id),
                    ("TELEGRAM_API_HASH/API_HASH", self.api_hash),
                ]
            )
        elif mode is RuntimeMode.DRAFT_TEXT:
            required.append(("OPENAI_API_KEY", self.openai_api_key))
        elif mode is RuntimeMode.DATABASE:
            required.append(("DATABASE_URL", self.database_url))

        missing = [name for name, value in required if not _configured(value)]
        if missing:
            raise ConfigurationError(
                f"Missing required configuration for {mode.value}: {', '.join(missing)}"
            )

        if mode is RuntimeMode.DATABASE:
            self.postgresql_url()
        elif mode in {
            RuntimeMode.RUN,
            RuntimeMode.BOT_ONLY,
            RuntimeMode.COLLECTOR_ONLY,
        } and self.database_url is not None:
            self.postgresql_url()

    def postgresql_url(self) -> str:
        if self.database_url is None:
            raise ConfigurationError("Missing required configuration for database: DATABASE_URL")

        value = self.database_url.get_secret_value().strip()
        if not value.startswith("postgresql+psycopg://"):
            raise ConfigurationError(
                "Invalid configuration: DATABASE_URL must use postgresql+psycopg://"
            )
        return value


def _configured(value: Any) -> bool:
    if isinstance(value, SecretStr):
        return bool(value.get_secret_value().strip())
    return value is not None and (not isinstance(value, str) or bool(value.strip()))


def _safe_validation_message(error: ValidationError) -> str:
    issues: list[str] = []
    for item in error.errors(include_url=False, include_context=False, include_input=False):
        location = ".".join(str(part) for part in item.get("loc", ())) or "configuration"
        issues.append(f"{location}: {item.get('msg', 'invalid value')}")
    return "Invalid configuration: " + "; ".join(issues)
