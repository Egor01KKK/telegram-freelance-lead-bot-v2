import unittest
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

from freelancer_bot.config import (
    ConfigurationError,
    RuntimeConfig,
    RuntimeMode,
    Sensitivity,
)


class RuntimeConfigTest(unittest.TestCase):
    def test_accepts_legacy_parser_env_names(self):
        env = {
            "API_ID": "12345",
            "API_HASH": "hash",
            "BOT_TOKEN": "token",
            "TARGET_USER_ID": "98765",
            "DATABASE_URL": "postgresql+psycopg://test:test@localhost/test",
        }

        config = self._load(env)

        self.assertEqual(config.api_id, 12345)
        self.assertEqual(config.api_hash.get_secret_value(), "hash")
        self.assertEqual(config.bot_token.get_secret_value(), "token")
        self.assertEqual(config.target_chat_id, 98765)

    def test_prefers_new_env_names_over_legacy_names(self):
        env = {
            "TELEGRAM_API_ID": "111",
            "TELEGRAM_API_HASH": "new_hash",
            "TELEGRAM_BOT_TOKEN": "new_token",
            "TELEGRAM_TARGET_CHAT_ID": "222",
            "API_ID": "333",
            "API_HASH": "old_hash",
            "BOT_TOKEN": "old_token",
            "TARGET_USER_ID": "444",
            "DATABASE_URL": "postgresql+psycopg://test:test@localhost/test",
        }

        config = self._load(env)

        self.assertEqual(config.api_id, 111)
        self.assertEqual(config.api_hash.get_secret_value(), "new_hash")
        self.assertEqual(config.bot_token.get_secret_value(), "new_token")
        self.assertEqual(config.target_chat_id, 222)

    def test_reads_external_source_and_filter_paths(self):
        env = self._telegram_env()
        env.update({"SOURCES_PATH": "custom/sources.json", "FILTERS_PATH": "custom/filters.json"})

        config = self._load(env)

        self.assertEqual(config.sources_path, Path("custom/sources.json"))
        self.assertEqual(config.filters_path, Path("custom/filters.json"))

    def test_subscription_plan_is_configurable_without_payment_credentials(self):
        env = self._telegram_env()
        env.update(
            {
                "SUBSCRIPTION_PLAN_AMOUNT": "1250.50",
                "SUBSCRIPTION_PLAN_CURRENCY": "rub",
                "SUBSCRIPTION_PLAN_INTERVAL": "MONTH",
            }
        )

        config = self._load(env)

        self.assertEqual(config.subscription_plan_amount, Decimal("1250.50"))
        self.assertEqual(config.subscription_plan_currency, "RUB")
        self.assertEqual(config.subscription_plan_interval, "month")

    def test_subscription_plan_rejects_non_monthly_or_invalid_currency(self):
        env = self._telegram_env()
        env["SUBSCRIPTION_PLAN_INTERVAL"] = "year"
        with self.assertRaisesRegex(ConfigurationError, "SUBSCRIPTION_PLAN_INTERVAL"):
            self._load(env)

        env = self._telegram_env()
        env["SUBSCRIPTION_PLAN_CURRENCY"] = "rubles"
        with self.assertRaisesRegex(ConfigurationError, "SUBSCRIPTION_PLAN_CURRENCY"):
            self._load(env)

    def test_check_config_and_filter_require_no_credentials(self):
        for mode in (RuntimeMode.CHECK_CONFIG, RuntimeMode.CHECK_FILTER):
            with self.subTest(mode=mode):
                config = self._load({}, mode=mode)
                self.assertIsNone(config.api_id)
                self.assertIsNone(config.openai_api_key)

    def test_source_check_requires_user_credentials_but_not_bot_token(self):
        config = self._load(
            {"TELEGRAM_API_ID": "111", "TELEGRAM_API_HASH": "hash"},
            mode=RuntimeMode.CHECK_SOURCES,
        )

        self.assertEqual(config.api_id, 111)
        self.assertIsNone(config.bot_token)

        with self.assertRaisesRegex(ConfigurationError, "TELEGRAM_API_HASH"):
            self._load({"TELEGRAM_API_ID": "111"}, mode=RuntimeMode.CHECK_SOURCES)

    def test_bot_only_requires_bot_and_postgresql_but_not_user_session(self):
        config = self._load(
            {
                "TELEGRAM_API_ID": "111",
                "TELEGRAM_API_HASH": "hash",
                "TELEGRAM_BOT_TOKEN": "token",
                "DATABASE_URL": "postgresql+psycopg://test:test@localhost/test",
            },
            mode=RuntimeMode.BOT_ONLY,
        )

        self.assertEqual(config.api_id, 111)
        self.assertEqual(config.bot_token.get_secret_value(), "token")

        with self.assertRaisesRegex(ConfigurationError, "TELEGRAM_BOT_TOKEN"):
            self._load(
                {
                    "TELEGRAM_API_ID": "111",
                    "TELEGRAM_API_HASH": "hash",
                    "DATABASE_URL": "postgresql+psycopg://test:test@localhost/test",
                },
                mode=RuntimeMode.BOT_ONLY,
            )

    def test_default_external_work_is_opt_in_and_cost_bounded(self):
        config = self._load(self._telegram_env())

        self.assertFalse(config.send_catch_up)
        self.assertFalse(config.source_discovery_enabled)
        self.assertFalse(config.source_audit_enabled)
        self.assertFalse(config.source_graph_discovery_enabled)
        self.assertFalse(config.opportunity_analysis_fallback_enabled)
        self.assertEqual(config.max_ai_calls_per_run, 10)
        self.assertEqual(config.opportunity_analysis_daily_spend_limit_usd, Decimal("1.00"))
        self.assertEqual(config.opportunity_analysis_monthly_spend_limit_usd, Decimal("10.00"))

    def test_graph_entity_resolution_budget_is_configurable_and_bounded(self):
        defaults = self._load(self._telegram_env())
        self.assertEqual(defaults.telegram_max_entity_resolves_per_graph_pass, 5)

        env = self._telegram_env()
        env["TELEGRAM_MAX_ENTITY_RESOLVES_PER_GRAPH_PASS"] = "7"
        configured = self._load(env)
        self.assertEqual(configured.telegram_max_entity_resolves_per_graph_pass, 7)

        env["TELEGRAM_MAX_ENTITY_RESOLVES_PER_GRAPH_PASS"] = "0"
        with self.assertRaisesRegex(
            ConfigurationError,
            "TELEGRAM_MAX_ENTITY_RESOLVES_PER_GRAPH_PASS",
        ):
            self._load(env)

    def test_source_audit_capacity_is_separate_and_reaches_policy_floor(self):
        defaults = self._load(self._telegram_env())
        self.assertEqual(defaults.telegram_max_history_messages_per_pass, 25)
        self.assertEqual(defaults.source_audit_sample_size, 60)

        env = self._telegram_env()
        env["SOURCE_AUDIT_SAMPLE_SIZE"] = "80"
        configured = self._load(env)
        self.assertEqual(configured.source_audit_sample_size, 80)

        env["SOURCE_AUDIT_SAMPLE_SIZE"] = "59"
        with self.assertRaisesRegex(
            ConfigurationError,
            r"SOURCE_AUDIT_SAMPLE_SIZE.*rejection evidence floor \(60\)",
        ):
            self._load(env)

    def test_run_requires_telegram_credentials(self):
        with self.assertRaisesRegex(ConfigurationError, "TELEGRAM_API_ID"):
            self._load({}, mode=RuntimeMode.RUN)

    def test_run_requires_postgresql_for_source_selection(self):
        env = self._telegram_env()
        env.pop("DATABASE_URL")
        with self.assertRaisesRegex(ConfigurationError, "DATABASE_URL"):
            self._load(env, mode=RuntimeMode.RUN)

        env["DATABASE_URL"] = "sqlite:///legacy.sqlite3"
        with self.assertRaisesRegex(ConfigurationError, r"postgresql\+psycopg"):
            self._load(env, mode=RuntimeMode.RUN)

    def test_ai_key_is_required_only_when_reply_feature_is_enabled(self):
        config = self._load(self._telegram_env())
        self.assertFalse(config.ai_reply_enabled)
        self.assertIsNone(config.openai_api_key)

        enabled = self._telegram_env()
        enabled["AI_REPLY_ENABLED"] = "true"
        with self.assertRaisesRegex(ConfigurationError, "OPENAI_API_KEY"):
            self._load(enabled)

    def test_core_run_accepts_no_openai_or_billing_provider_credentials(self):
        config = self._load(self._telegram_env(), mode=RuntimeMode.RUN)

        self.assertFalse(config.ai_reply_enabled)
        self.assertIsNone(config.openai_api_key)

    def test_opportunity_analysis_spend_guards_are_configurable_and_optional(self):
        defaults = self._load(self._telegram_env())
        self.assertEqual(defaults.opportunity_analysis_daily_spend_limit_usd, Decimal("1.00"))
        self.assertEqual(defaults.opportunity_analysis_monthly_spend_limit_usd, Decimal("10.00"))

        env = self._telegram_env()
        env.update(
            {
                "OPPORTUNITY_ANALYSIS_DAILY_SPEND_LIMIT_USD": "1.25",
                "OPPORTUNITY_ANALYSIS_MONTHLY_SPEND_LIMIT_USD": "25",
                "OPPORTUNITY_ANALYSIS_BUDGET_RESERVE_INPUT_TOKENS": "700",
                "OPPORTUNITY_ANALYSIS_BUDGET_RESERVE_OUTPUT_TOKENS": "180",
            }
        )
        configured = self._load(env)
        self.assertEqual(configured.opportunity_analysis_daily_spend_limit_usd, Decimal("1.25"))
        self.assertEqual(configured.opportunity_analysis_monthly_spend_limit_usd, Decimal("25"))
        self.assertEqual(configured.opportunity_analysis_budget_reserve_input_tokens, 700)
        self.assertEqual(configured.opportunity_analysis_budget_reserve_output_tokens, 180)

    def test_draft_mode_requires_ai_key_but_not_telegram_credentials(self):
        config = self._load(
            {"OPENAI_API_KEY": "draft-secret"},
            mode=RuntimeMode.DRAFT_TEXT,
        )
        self.assertIsNone(config.api_id)
        self.assertEqual(config.openai_api_key.get_secret_value(), "draft-secret")

        with self.assertRaisesRegex(ConfigurationError, "OPENAI_API_KEY"):
            self._load({}, mode=RuntimeMode.DRAFT_TEXT)

    def test_database_mode_requires_psycopg_postgresql_url_only(self):
        config = self._load(
            {"DATABASE_URL": "postgresql+psycopg://user:short@localhost/database"},
            mode=RuntimeMode.DATABASE,
        )
        self.assertEqual(
            config.postgresql_url(),
            "postgresql+psycopg://user:short@localhost/database",
        )

        with self.assertRaisesRegex(ConfigurationError, "DATABASE_URL"):
            self._load({}, mode=RuntimeMode.DATABASE)
        with self.assertRaisesRegex(ConfigurationError, r"postgresql\+psycopg"):
            self._load(
                {"DATABASE_URL": "sqlite:///data/leads.sqlite3"},
                mode=RuntimeMode.DATABASE,
            )

    def test_secret_values_are_hidden_from_repr_dump_and_validation_errors(self):
        sentinel = "do-not-leak-this-value"
        env = self._telegram_env()
        env.update({"TELEGRAM_API_HASH": sentinel, "TELEGRAM_BOT_TOKEN": sentinel})
        config = self._load(env)

        rendered = " ".join(
            (
                repr(config),
                str(config),
                repr(config.model_dump()),
                repr(config.model_dump(mode="json")),
                config.model_dump_json(),
            )
        )
        self.assertNotIn(sentinel, rendered)

        invalid = self._telegram_env()
        invalid["TELEGRAM_API_ID"] = sentinel
        with self.assertRaises(ConfigurationError) as raised:
            self._load(invalid)
        self.assertNotIn(sentinel, str(raised.exception))

    def test_every_field_has_explicit_sensitivity_metadata(self):
        sensitivity_values = {item.value for item in Sensitivity}
        for name, field in RuntimeConfig.model_fields.items():
            with self.subTest(field=name):
                metadata = field.json_schema_extra or {}
                self.assertIn(metadata.get("sensitivity"), sensitivity_values)

        self.assertEqual(
            RuntimeConfig.model_fields["database_url"].json_schema_extra["sensitivity"],
            Sensitivity.SECRET.value,
        )

    def test_invalid_boolean_is_rejected_without_echoing_value(self):
        env = self._telegram_env()
        env["AI_REPLY_ENABLED"] = "definitely-secret-invalid-value"
        with self.assertRaises(ConfigurationError) as raised:
            self._load(env)

        self.assertIn("AI_REPLY_ENABLED", str(raised.exception))
        self.assertNotIn(env["AI_REPLY_ENABLED"], str(raised.exception))

    def test_programmatic_construction_accepts_field_names(self):
        with patch.dict("os.environ", {}, clear=True):
            config = RuntimeConfig(
                api_id=123,
                api_hash="hash",
                bot_token="token",
                _env_file=None,
            )

        self.assertEqual(config.api_id, 123)
        self.assertEqual(config.api_hash.get_secret_value(), "hash")

    def _load(self, env, mode=RuntimeMode.RUN):
        with patch.dict("os.environ", env, clear=True):
            return RuntimeConfig.from_env(mode=mode, env_file=None)

    @staticmethod
    def _telegram_env():
        return {
            "TELEGRAM_API_ID": "12345",
            "TELEGRAM_API_HASH": "hash",
            "TELEGRAM_BOT_TOKEN": "token",
            "DATABASE_URL": "postgresql+psycopg://test:test@localhost/test",
        }


if __name__ == "__main__":
    unittest.main()
